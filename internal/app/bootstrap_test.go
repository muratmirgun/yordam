package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/cli"
	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/domain"
)

func TestBootstrapComposesCanonicalRedactedRuntime(t *testing.T) {
	const processKey = "bootstrap-process-secret"
	requests := make(chan bootstrapRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var body struct {
			Tools []struct {
				Function struct {
					Name string `json:"name"`
				} `json:"function"`
			} `json:"tools"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		names := make([]string, len(body.Tools))
		for index, tool := range body.Tools {
			names[index] = tool.Function.Name
		}
		requests <- bootstrapRequest{authorization: request.Header.Get("Authorization"), tools: names}
		response.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(response, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", "reply "+processKey)
		fmt.Fprintln(response, "data: [DONE]")
		fmt.Fprintln(response)
	}))
	defer server.Close()

	workspace := t.TempDir()
	workspaceLink := filepath.Join(t.TempDir(), "workspace-link")
	if err := os.Symlink(workspace, workspaceLink); err != nil {
		t.Fatal(err)
	}
	dataDir := t.TempDir()
	debugLog := filepath.Join(t.TempDir(), "debug.jsonl")
	if err := os.WriteFile(debugLog, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SECONDARY_KEY", "secondary-secret")
	t.Setenv("YORDAM_API_KEY", processKey)
	application, snapshot, err := app.Bootstrap(t.Context(), app.BootstrapOptions{
		Config: config.Config{
			ActiveProfile: "primary",
			Profiles: map[string]config.Profile{
				"primary": {
					BaseURL:      server.URL,
					APIKeyEnv:    "PRIMARY_KEY",
					Models:       []string{"model-a", "model-b"},
					DefaultModel: "model-a",
				},
				"secondary": {
					BaseURL:      server.URL,
					APIKeyEnv:    "SECONDARY_KEY",
					Models:       []string{"model-c"},
					DefaultModel: "model-c",
				},
			},
			MaxToolCalls:        32,
			ShellTimeoutSeconds: 120,
		},
		CLI: cli.Options{
			Mode:         domain.ModeAsk,
			Profile:      "primary",
			Model:        "model-a",
			DataDir:      dataDir,
			DebugLog:     debugLog,
			MaxToolCalls: 32,
			ShellTimeout: 2 * time.Second,
		},
		CWD:        workspaceLink,
		HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	canonicalWorkspace, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Workspace.CanonicalPath != canonicalWorkspace || snapshot.Session.Workspace != snapshot.Workspace {
		t.Fatalf("workspace snapshot=%+v session=%+v", snapshot.Workspace, snapshot.Session.Workspace)
	}
	if snapshot.Session.ID == "" || snapshot.Session.Mode != domain.ModeAsk || snapshot.Session.Selection != (domain.ModelSelection{Profile: "primary", Model: "model-a"}) {
		t.Fatalf("session=%+v", snapshot.Session)
	}
	wantModels := []domain.ModelSelection{
		{Profile: "primary", Model: "model-a"},
		{Profile: "primary", Model: "model-b"},
		{Profile: "secondary", Model: "model-c"},
	}
	if !slices.Equal(snapshot.Models, wantModels) || len(snapshot.Sessions) != 1 || snapshot.Sessions[0].ID != snapshot.Session.ID {
		t.Fatalf("models=%v sessions=%v", snapshot.Models, snapshot.Sessions)
	}

	appDone := make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { appDone <- application.Run(ctx) }()
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "do not persist " + processKey}
	var visible strings.Builder
	for {
		event := receiveEvent(t, application.Events())
		visible.WriteString(event.Message)
		visible.WriteString(event.Runtime.Text)
		if event.Kind == app.EventTurnCompleted {
			break
		}
		if event.Kind == app.EventError {
			t.Fatalf("turn failed: %+v", event)
		}
	}
	if strings.Contains(visible.String(), processKey) || !strings.Contains(visible.String(), "[REDACTED]") {
		t.Fatalf("visible output was not redacted: %q", visible.String())
	}
	request := <-requests
	if request.authorization != "Bearer "+processKey || !slices.Equal(request.tools, []string{"read", "search", "edit", "shell"}) {
		t.Fatalf("provider request=%+v", request)
	}
	application.Commands() <- app.Command{Kind: app.CommandShutdown}
	if err := <-appDone; err != nil {
		t.Fatalf("app shutdown: %v", err)
	}

	assertFileMode(t, debugLog, 0o600)
	assertTreeOmits(t, dataDir, processKey)
	assertTreeOmits(t, filepath.Dir(debugLog), processKey)
}

func TestBootstrapStartsWithoutValidConfiguration(t *testing.T) {
	const secret = "sk-malformed-bootstrap-secret"
	configPath := filepath.Join(t.TempDir(), "config.jsonc")
	if err := os.WriteFile(configPath, []byte(`{"model": `+secret), 0o600); err != nil {
		t.Fatal(err)
	}
	debugLog := filepath.Join(t.TempDir(), "debug.jsonl")
	application, snapshot, err := app.Bootstrap(t.Context(), app.BootstrapOptions{
		ConfigPath: configPath,
		CLI: func() cli.Options {
			options := bootstrapCLI(t.TempDir())
			options.DebugLog = debugLog
			return options
		}(),
		CWD: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if application == nil || snapshot.Session.ID == "" || snapshot.Session.Selection != (domain.ModelSelection{}) {
		t.Fatalf("application=%v session=%+v", application != nil, snapshot.Session)
	}
	var typed *domain.TypedError
	if !errors.As(snapshot.ConfigurationError, &typed) || typed.Kind != domain.ErrorConfigurationInvalid || !strings.Contains(typed.Message, configPath) || !strings.Contains(typed.Message, "/reload") || strings.Contains(typed.Message, secret) {
		t.Fatalf("configuration error=%v", snapshot.ConfigurationError)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- application.Run(ctx) }()
	application.Commands() <- app.Command{Kind: app.CommandReloadConfig}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventReloadCompleted || event.Err == nil || strings.Contains(event.Message, secret) || strings.Contains(event.Err.Error(), secret) {
		t.Fatalf("reload event=%+v", event)
	}
	application.Commands() <- app.Command{Kind: app.CommandShutdown}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	assertTreeOmits(t, filepath.Dir(debugLog), secret)
}

func TestBootstrapLoadsModelsWithoutCredential(t *testing.T) {
	configPath := writeBootstrapConfig(t, bootstrapConfig("https://llm.example/v1"))
	application, snapshot, err := app.Bootstrap(t.Context(), app.BootstrapOptions{
		ConfigPath: configPath,
		CLI:        bootstrapCLI(t.TempDir()),
		CWD:        t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	selection := domain.ModelSelection{Profile: "default", Model: "test-model"}
	if application == nil || snapshot.Session.Selection != selection || !slices.Equal(snapshot.Models, []domain.ModelSelection{selection}) {
		t.Fatalf("application=%v snapshot=%+v", application != nil, snapshot)
	}
	var typed *domain.TypedError
	if !errors.As(snapshot.ConfigurationError, &typed) || typed.Kind != domain.ErrorConfigurationInvalid || !strings.Contains(typed.Message, "BOOTSTRAP_KEY") || !strings.Contains(typed.Message, "restart") {
		t.Fatalf("configuration error=%v", snapshot.ConfigurationError)
	}
}

func TestBootstrapContinuesLatestAndRejectsSpecificSessionFromOtherWorkspace(t *testing.T) {
	t.Setenv("BOOTSTRAP_KEY", "bootstrap-key")
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(response, "data: [DONE]\n\n")
	}))
	defer server.Close()
	dataDir := t.TempDir()
	cfg := bootstrapConfig(server.URL)
	firstWorkspace := t.TempDir()
	secondWorkspace := t.TempDir()

	_, first, err := app.Bootstrap(t.Context(), app.BootstrapOptions{
		Config:     cfg,
		CLI:        bootstrapCLI(dataDir),
		CWD:        firstWorkspace,
		HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	continuedCLI := bootstrapCLI(dataDir)
	continuedCLI.Continue = true
	_, continued, err := app.Bootstrap(t.Context(), app.BootstrapOptions{
		Config:     cfg,
		CLI:        continuedCLI,
		CWD:        firstWorkspace,
		HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if continued.Session.ID != first.Session.ID {
		t.Fatalf("continued session=%q want=%q", continued.Session.ID, first.Session.ID)
	}

	_, other, err := app.Bootstrap(t.Context(), app.BootstrapOptions{
		Config:     cfg,
		CLI:        bootstrapCLI(dataDir),
		CWD:        secondWorkspace,
		HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	specificCLI := bootstrapCLI(dataDir)
	specificCLI.Session = other.Session.ID
	if _, _, err := app.Bootstrap(t.Context(), app.BootstrapOptions{
		Config:     cfg,
		CLI:        specificCLI,
		CWD:        firstWorkspace,
		HTTPClient: server.Client(),
	}); err == nil || !strings.Contains(err.Error(), "different workspace") {
		t.Fatalf("workspace mismatch error=%v", err)
	}
}

func TestBootstrapPersistsExplicitOverridesWhenContinuing(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "primary-key")
	t.Setenv("SECONDARY_KEY", "secondary-key")
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(response, "data: [DONE]\n\n")
	}))
	defer server.Close()
	dataDir := t.TempDir()
	workspace := t.TempDir()
	cfg := config.Config{ActiveProfile: "primary", Profiles: map[string]config.Profile{
		"primary":   {BaseURL: server.URL, APIKeyEnv: "PRIMARY_KEY", Models: []string{"m1"}, DefaultModel: "m1"},
		"secondary": {BaseURL: server.URL, APIKeyEnv: "SECONDARY_KEY", Models: []string{"m2"}, DefaultModel: "m2"},
	}, MaxToolCalls: 32, ShellTimeoutSeconds: 120}
	_, first, err := app.Bootstrap(t.Context(), app.BootstrapOptions{Config: cfg, CLI: bootstrapCLI(dataDir), CWD: workspace, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	continued := bootstrapCLI(dataDir)
	continued.Continue = true
	continued.Mode = domain.ModeAuto
	continued.ModeSet = true
	continued.Profile = "secondary"
	continued.ProfileSet = true
	continued.Model = "m2"
	continued.ModelSet = true
	_, snapshot, err := app.Bootstrap(t.Context(), app.BootstrapOptions{Config: cfg, CLI: continued, CWD: workspace, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Session.ID != first.Session.ID || snapshot.Session.Mode != domain.ModeAuto || snapshot.Session.Selection != (domain.ModelSelection{Profile: "secondary", Model: "m2"}) {
		t.Fatalf("continued session=%+v first=%+v", snapshot.Session, first.Session)
	}
	if len(snapshot.Replay.Events) < 3 || snapshot.Replay.Events[len(snapshot.Replay.Events)-2].Kind != domain.EventModeChanged || snapshot.Replay.Events[len(snapshot.Replay.Events)-1].Kind != domain.EventModelChanged {
		t.Fatalf("override events=%v", snapshot.Replay.Events)
	}
}

func TestBootstrapKeepsCredentialsBoundToNamedProfiles(t *testing.T) {
	var primaryAuth, secondaryAuth string
	primary := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		primaryAuth = request.Header.Get("Authorization")
		response.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(response, "data: [DONE]\n\n")
	}))
	defer primary.Close()
	secondary := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		secondaryAuth = request.Header.Get("Authorization")
		response.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(response, "data: [DONE]\n\n")
	}))
	defer secondary.Close()
	t.Setenv("PRIMARY_KEY", "primary-key")
	t.Setenv("SECONDARY_KEY", "secondary-key")
	t.Setenv("YORDAM_API_KEY", "selected-env-key")
	application, snapshot, err := app.Bootstrap(t.Context(), app.BootstrapOptions{
		Config: config.Config{ActiveProfile: "primary", Profiles: map[string]config.Profile{
			"primary":   {BaseURL: primary.URL, APIKeyEnv: "PRIMARY_KEY", Models: []string{"m1"}, DefaultModel: "m1"},
			"secondary": {BaseURL: secondary.URL, APIKeyEnv: "SECONDARY_KEY", Models: []string{"m2"}, DefaultModel: "m2"},
		}, MaxToolCalls: 32, ShellTimeoutSeconds: 120},
		CLI: cli.Options{Mode: domain.ModeAsk, Profile: "primary", Model: "m1", DataDir: t.TempDir(), MaxToolCalls: 32, ShellTimeout: time.Second},
		CWD: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- application.Run(context.Background()) }()
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "primary"}
	for event := range application.Events() {
		if event.Kind == app.EventTurnCompleted {
			break
		}
	}
	application.Commands() <- app.Command{Kind: app.CommandChangeModel, Selection: domain.ModelSelection{Profile: "secondary", Model: "m2"}}
	_ = receiveEvent(t, application.Events())
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "secondary"}
	for event := range application.Events() {
		if event.Kind == app.EventTurnCompleted {
			break
		}
	}
	application.Commands() <- app.Command{Kind: app.CommandShutdown}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if snapshot.Session.Selection.Profile != "primary" || primaryAuth != "Bearer selected-env-key" || secondaryAuth != "Bearer secondary-key" {
		t.Fatalf("selection=%+v primary=%q secondary=%q", snapshot.Session.Selection, primaryAuth, secondaryAuth)
	}
}

func TestBootstrapAppliesEnvironmentAndBaseURLOverridesToResumedSelection(t *testing.T) {
	primaryRequests := make(chan string, 1)
	primary := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		primaryRequests <- request.Header.Get("Authorization")
		response.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(response, "data: [DONE]\n\n")
	}))
	defer primary.Close()
	secondary := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(response, "data: [DONE]\n\n")
	}))
	defer secondary.Close()
	overrideRequests := make(chan string, 1)
	override := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		overrideRequests <- request.Header.Get("Authorization")
		response.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(response, "data: [DONE]\n\n")
	}))
	defer override.Close()
	t.Setenv("PRIMARY_KEY", "primary-key")
	t.Setenv("SECONDARY_KEY", "secondary-key")
	dataDir := t.TempDir()
	workspace := t.TempDir()
	cfg := config.Config{ActiveProfile: "primary", Profiles: map[string]config.Profile{
		"primary":   {BaseURL: primary.URL, APIKeyEnv: "PRIMARY_KEY", Models: []string{"m1"}, DefaultModel: "m1"},
		"secondary": {BaseURL: secondary.URL, APIKeyEnv: "SECONDARY_KEY", Models: []string{"m2"}, DefaultModel: "m2"},
	}, MaxToolCalls: 32, ShellTimeoutSeconds: 120}
	create := bootstrapCLI(dataDir)
	create.Profile, create.Model = "secondary", "m2"
	create.ProfileSet, create.ModelSet = true, true
	_, first, err := app.Bootstrap(t.Context(), app.BootstrapOptions{Config: cfg, CLI: create, CWD: workspace})
	if err != nil {
		t.Fatal(err)
	}
	resume := bootstrapCLI(dataDir)
	resume.Continue = true
	resume.BaseURL, resume.BaseURLSet = override.URL, true
	t.Setenv("YORDAM_API_KEY", "resumed-env-key")
	application, snapshot, err := app.Bootstrap(t.Context(), app.BootstrapOptions{Config: cfg, CLI: resume, CWD: workspace})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Session.ID != first.Session.ID || snapshot.Session.Selection.Profile != "secondary" {
		t.Fatalf("resumed session=%+v first=%+v", snapshot.Session, first.Session)
	}
	done := make(chan error, 1)
	go func() { done <- application.Run(context.Background()) }()
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "secondary"}
	for event := range application.Events() {
		if event.Kind == app.EventTurnCompleted {
			break
		}
		if event.Kind == app.EventError {
			t.Fatalf("turn event=%+v", event)
		}
	}
	select {
	case got := <-overrideRequests:
		if got != "Bearer resumed-env-key" {
			t.Fatalf("override authorization=%q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("resumed selection did not receive process/base-url overrides")
	}
	application.Commands() <- app.Command{Kind: app.CommandChangeModel, Selection: domain.ModelSelection{Profile: "primary", Model: "m1"}}
	_ = receiveEvent(t, application.Events())
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "primary"}
	for event := range application.Events() {
		if event.Kind == app.EventTurnCompleted {
			break
		}
	}
	if got := <-primaryRequests; got != "Bearer primary-key" {
		t.Fatalf("primary authorization=%q", got)
	}
	application.Commands() <- app.Command{Kind: app.CommandShutdown}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestBootstrapRestoresDurableAutoShellAcknowledgement(t *testing.T) {
	var requestCount int
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		requestCount++
		response.Header().Set("Content-Type", "text/event-stream")
		if requestCount == 1 {
			arguments := `{"command":"touch resumed-auto-shell","cwd":"."}`
			fmt.Fprintf(response, "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"shell-1\",\"function\":{\"name\":\"shell\",\"arguments\":%q}}]}}]}\n\n", arguments)
			fmt.Fprint(response, "data: [DONE]\n\n")
			return
		}
		fmt.Fprint(response, "data: {\"choices\":[{\"delta\":{\"content\":\"complete\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer server.Close()
	t.Setenv("BOOTSTRAP_KEY", "bootstrap-key")
	dataDir := t.TempDir()
	workspace := t.TempDir()
	cliOptions := bootstrapCLI(dataDir)
	cliOptions.Mode = domain.ModeAuto
	cliOptions.ModeSet = true
	firstApp, first, err := app.Bootstrap(t.Context(), app.BootstrapOptions{Config: bootstrapConfig(server.URL), CLI: cliOptions, CWD: workspace, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- firstApp.Run(context.Background()) }()
	firstApp.Commands() <- app.Command{Kind: app.CommandAcknowledgeAutoShell}
	if event := receiveEvent(t, firstApp.Events()); event.Kind != app.EventState {
		t.Fatalf("ack event=%+v", event)
	}
	firstApp.Commands() <- app.Command{Kind: app.CommandShutdown}
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}

	resume := bootstrapCLI(dataDir)
	resume.Continue = true
	resumedApp, resumed, err := app.Bootstrap(t.Context(), app.BootstrapOptions{Config: bootstrapConfig(server.URL), CLI: resume, CWD: workspace, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Session.ID != first.Session.ID || resumed.Session.Mode != domain.ModeAuto {
		t.Fatalf("resumed session=%+v first=%+v", resumed.Session, first.Session)
	}
	resumedDone := make(chan error, 1)
	go func() { resumedDone <- resumedApp.Run(context.Background()) }()
	resumedApp.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "run shell"}
	for {
		event := receiveEvent(t, resumedApp.Events())
		if event.Kind == app.EventPermissionRequested {
			t.Fatalf("resumed acknowledged shell asked again: %+v", event)
		}
		if event.Kind == app.EventError {
			t.Fatalf("resumed turn failed: %+v", event)
		}
		if event.Kind == app.EventTurnCompleted {
			break
		}
	}
	if _, err := os.Stat(filepath.Join(workspace, "resumed-auto-shell")); err != nil {
		t.Fatalf("resumed auto shell did not execute: %v", err)
	}
	resumedApp.Commands() <- app.Command{Kind: app.CommandShutdown}
	if err := <-resumedDone; err != nil {
		t.Fatal(err)
	}
}

type bootstrapRequest struct {
	authorization string
	tools         []string
}

func bootstrapConfig(baseURL string) config.Config {
	return config.Config{
		ActiveProfile: "default",
		Profiles: map[string]config.Profile{
			"default": {
				BaseURL:      baseURL,
				APIKeyEnv:    "BOOTSTRAP_KEY",
				Models:       []string{"test-model"},
				DefaultModel: "test-model",
			},
		},
		MaxToolCalls:        32,
		ShellTimeoutSeconds: 120,
	}
}

func writeBootstrapConfig(t *testing.T, cfg config.Config) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.jsonc")
	if err := config.SaveGlobal(path, cfg); err != nil {
		t.Fatal(err)
	}
	return path
}

func bootstrapCLI(dataDir string) cli.Options {
	return cli.Options{
		Mode:         domain.ModeAsk,
		DataDir:      dataDir,
		MaxToolCalls: 32,
		ShellTimeout: 2 * time.Second,
	}
}

func assertFileMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode=%o want=%o", path, got, want)
	}
}

func assertTreeOmits(t *testing.T, root, forbidden string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(raw), forbidden) {
			return fmt.Errorf("%s contains forbidden secret", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
