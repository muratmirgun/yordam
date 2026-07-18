package app

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

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/cli"
	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/permission"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
	"github.com/muratmirgun/yordam/internal/testsupport/agentfixture"
)

func TestNewUsesEmptyRedactorBindingWhenNoneIsSupplied(t *testing.T) {
	application := New(Options{RuntimeSet: RuntimeSet{Redactor: secret.New("candidate-secret")}})
	if got := application.redactors.String("candidate-secret"); got != "candidate-secret" {
		t.Fatalf("implicit binding inherited candidate secrets: %q", got)
	}
}

func TestRuntimeSetReadinessClassifiesModelsAndCredentials(t *testing.T) {
	set := RuntimeSet{
		Models:         []domain.ModelSelection{{Profile: "primary", Model: "a"}, {Profile: "secondary", Model: "b"}},
		CredentialEnvs: map[string]string{"primary": "PRIMARY_KEY", "secondary": "SECONDARY_KEY"},
		Credentials:    map[string]string{"primary": "secret", "secondary": ""},
		configPath:     "/home/user/.config/yordam/config.jsonc",
	}
	if err := set.Ready(domain.ModelSelection{Profile: "primary", Model: "a"}); err != nil {
		t.Fatal(err)
	}
	for selection, want := range map[domain.ModelSelection]string{
		{Profile: "secondary", Model: "b"}: "SECONDARY_KEY",
		{Profile: "missing", Model: "x"}:   "not configured",
	} {
		err := set.Ready(selection)
		var typed *domain.TypedError
		if err == nil || !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), "/home/user/.config/yordam/config.jsonc") || !strings.Contains(err.Error(), "restart") && want == "SECONDARY_KEY" || !asTypedConfiguration(err, &typed) {
			t.Fatalf("selection=%+v error=%v", selection, err)
		}
	}
}

func TestRuntimeSetReadyReturnsConfigurationErrorBeforeCompatibilityBypass(t *testing.T) {
	configuration := configurationError("/home/user/.config/yordam/config.jsonc", "configuration is invalid; edit the file and run /reload", nil)
	set := RuntimeSet{ConfigurationError: configuration, unchecked: true}

	if got := set.Ready(domain.ModelSelection{}); !errors.Is(got, configuration) {
		t.Fatalf("Ready() error = %v, want configuration error %v", got, configuration)
	}
}

func TestRuntimeBuilderBindsProvidersToolsLimitsAndCredentials(t *testing.T) {
	requests := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests <- request.Header.Get("Authorization")
		response.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(response, "data: [DONE]\n\n")
	}))
	defer server.Close()

	t.Setenv("PRIMARY_KEY", "primary-secret")
	t.Setenv("SECONDARY_KEY", "secondary-secret")
	t.Setenv("ORDINARY_VALUE", "preserved")
	cfg := loadRuntimeConfig(t, server.URL+"/v1", 1)
	builder := newRuntimeBuilderForTest(t, server.Client())
	set, err := builder.build(cfg, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	wantModels := []domain.ModelSelection{{Profile: "primary", Model: "a"}, {Profile: "primary", Model: "b"}, {Profile: "secondary", Model: "c"}}
	if !slices.Equal(set.Models, wantModels) || set.DefaultSelection != wantModels[0] {
		t.Fatalf("models=%+v default=%+v", set.Models, set.DefaultSelection)
	}
	if set.Credentials["primary"] != "primary-secret" || set.Credentials["secondary"] != "secondary-secret" {
		t.Fatalf("credentials not bound by provider")
	}
	if summary := fmt.Sprintf("%+v", set); strings.Contains(summary, "primary-secret") || strings.Contains(summary, "secondary-secret") {
		t.Fatalf("runtime set summary exposes credentials: %s", summary)
	}
	serialized, err := json.Marshal(set)
	if err != nil {
		t.Fatalf("serialize runtime set summary: %v", err)
	}
	if strings.Contains(string(serialized), "primary-secret") || strings.Contains(string(serialized), "secondary-secret") {
		t.Fatalf("serialized runtime set summary exposes credentials: %s", serialized)
	}
	runner, ok := set.Runtime.(*agent.Runner)
	if !ok || runner.MaxToolCalls != 7 {
		t.Fatalf("runtime=%T max=%d", set.Runtime, runner.MaxToolCalls)
	}
	for _, selection := range []domain.ModelSelection{{Profile: "primary", Model: "a"}, {Profile: "secondary", Model: "c"}} {
		stream, err := runner.Provider.Stream(t.Context(), domain.ModelRequest{Selection: selection})
		if err != nil {
			t.Fatal(err)
		}
		for range stream {
		}
	}
	if got := []string{<-requests, <-requests}; !slices.Equal(got, []string{"Bearer primary-secret", "Bearer secondary-secret"}) {
		t.Fatalf("authorizations=%q", got)
	}

	shellTool, ok := runner.Tools.Lookup("shell")
	if !ok {
		t.Fatal("shell tool missing")
	}
	prepared, err := shellTool.Prepare(t.Context(), domain.ToolRequest{
		CallID:    "env",
		Name:      "shell",
		Workspace: builder.workspace.CanonicalPath,
		Input:     json.RawMessage(`{"command":"env","cwd":"."}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := prepared.Execute(t.Context())
	if result.Status != domain.ToolSucceeded || strings.Contains(result.Content, "PRIMARY_KEY=") || strings.Contains(result.Content, "SECONDARY_KEY=") || !strings.Contains(result.Content, "ORDINARY_VALUE=preserved") {
		t.Fatalf("shell environment result=%+v", result)
	}

	prepared, err = shellTool.Prepare(t.Context(), domain.ToolRequest{
		CallID:    "timeout",
		Name:      "shell",
		Workspace: builder.workspace.CanonicalPath,
		Input:     json.RawMessage(`{"command":"sleep 5","cwd":"."}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	result = prepared.Execute(t.Context())
	if result.ErrorKind != domain.ErrorToolTimeout || time.Since(started) > 3*time.Second {
		t.Fatalf("timeout result=%+v elapsed=%s", result, time.Since(started))
	}
}

func TestRuntimeGenerationsKeepImmutableRedactors(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "generation-a")
	cfg := loadRuntimeConfig(t, "https://example.invalid/v1", 120)
	builder := newRuntimeBuilderForTest(t, nil)
	first, err := builder.build(cfg, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("PRIMARY_KEY", "generation-b")
	second, err := builder.build(cfg, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	if got := first.Redactor.String("generation-a generation-b"); got != "[REDACTED] generation-b" {
		t.Fatalf("first redactor=%q", got)
	}
	if got := second.Redactor.String("generation-a generation-b"); got != "generation-a [REDACTED]" {
		t.Fatalf("second redactor=%q", got)
	}
	firstRunner, ok := first.Runtime.(*agent.Runner)
	if !ok {
		t.Fatalf("first runtime=%T", first.Runtime)
	}
	if got := firstRunner.Redact("generation-a generation-b"); got != "[REDACTED] generation-b" {
		t.Fatalf("first runner redactor=%q", got)
	}
	shellTool, ok := firstRunner.Tools.Lookup("shell")
	if !ok {
		t.Fatal("shell tool missing")
	}
	prepared, err := shellTool.Prepare(t.Context(), domain.ToolRequest{
		CallID:    "immutable-redaction",
		Name:      "shell",
		Workspace: builder.workspace.CanonicalPath,
		Input:     json.RawMessage(`{"command":"printf 'generation-a generation-b'","cwd":"."}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := prepared.Execute(t.Context()).Content; got != "[REDACTED] generation-b" {
		t.Fatalf("first output redactor=%q", got)
	}
}

func TestRuntimeGenerationBootstrapBindingAdvancesStoreWhileOldOutputIsImmutable(t *testing.T) {
	const generationA = "generation-a"
	const generationB = "generation-b"
	t.Setenv("PRIMARY_KEY", generationA)

	configPath := filepath.Join(t.TempDir(), "config.jsonc")
	if err := os.WriteFile(configPath, []byte(`{
  "model": "primary/a",
  "provider": {
    "primary": {
      "options": {"baseURL": "https://example.invalid/v1", "apiKeyEnv": "PRIMARY_KEY"},
      "models": {"a": {}}
    }
  }
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	application, snapshot, err := Bootstrap(t.Context(), BootstrapOptions{
		ConfigPath: configPath,
		CLI: cli.Options{
			Mode:         domain.ModeAsk,
			DataDir:      t.TempDir(),
			MaxToolCalls: 32,
			ShellTimeout: 120 * time.Second,
		},
		CWD: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	firstRunner, ok := application.runtimeSet.Runtime.(*agent.Runner)
	if !ok {
		t.Fatalf("first runtime=%T", application.runtimeSet.Runtime)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- application.Run(ctx) }()
	t.Setenv("PRIMARY_KEY", generationB)
	application.Commands() <- Command{Kind: CommandReloadConfig}
	select {
	case event := <-application.Events():
		if event.Kind != EventReloadCompleted || !event.Applied || event.Err != nil {
			t.Fatalf("reload event=%+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reload")
	}

	if _, err := application.sessions.Append(t.Context(), snapshot.Session.ID, domain.EventToolResult, domain.ToolResultPayload{
		Result: domain.ToolResult{CallID: "store-generation-b", Status: domain.ToolSucceeded, Content: generationA + " " + generationB},
	}); err != nil {
		t.Fatal(err)
	}
	replay, err := application.sessions.Load(t.Context(), snapshot.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	var persisted domain.ToolResultPayload
	if err := json.Unmarshal(replay.Events[len(replay.Events)-1].Payload, &persisted); err != nil {
		t.Fatal(err)
	}
	if got := persisted.Result.Content; got != generationA+" [REDACTED]" {
		t.Fatalf("production store redactor=%q", got)
	}

	shellTool, ok := firstRunner.Tools.Lookup("shell")
	if !ok {
		t.Fatal("shell tool missing")
	}
	prepared, err := shellTool.Prepare(t.Context(), domain.ToolRequest{
		CallID:    "generation-a-output",
		Name:      "shell",
		Workspace: snapshot.Workspace.CanonicalPath,
		Input:     json.RawMessage(`{"command":"printf 'generation-a generation-b'","cwd":"."}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := prepared.Execute(t.Context()).Content; got != "[REDACTED] generation-b" {
		t.Fatalf("generation-a output redactor=%q", got)
	}

	application.Commands() <- Command{Kind: CommandShutdown}
	if err := <-done; err != nil {
		t.Fatalf("app shutdown: %v", err)
	}
}

func TestRuntimeBuilderRedactsConfiguredAndOverrideCredentials(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "configured-secret")
	t.Setenv("YORDAM_API_KEY", "override-secret")
	cfg := loadRuntimeConfig(t, "https://example.invalid/v1", 120)
	builder := newRuntimeBuilderForTest(t, nil)
	set, err := builder.build(cfg, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	if set.Credentials["primary"] != "override-secret" {
		t.Fatalf("effective credential=%q", set.Credentials["primary"])
	}
	if got := set.Redactor.String("configured-secret override-secret"); got != "[REDACTED] [REDACTED]" {
		t.Fatalf("redacted=%q", got)
	}
	if set.Admission == nil || set.RuntimeGenerationID == "" {
		t.Fatalf("runtime has no generation lease: %+v", set)
	}
	if got := set.Redactor.String("Y29uZmlndXJlZC1zZWNyZXQ="); got != "[REDACTED]" {
		t.Fatalf("encoded redacted=%q", got)
	}
}

func TestRuntimeSetBindsApproverToItsRunner(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "primary-secret")
	set, err := newRuntimeBuilderForTest(t, nil).build(loadRuntimeConfig(t, "https://example.invalid/v1", 120), domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	runner, ok := set.Runtime.(*agent.Runner)
	if !ok {
		t.Fatalf("runtime=%T", set.Runtime)
	}
	approver := &agentfixture.Approver{}
	set.BindApprover(approver)
	if runner.Approver != approver {
		t.Fatalf("runner approver=%T, want %T", runner.Approver, approver)
	}
}

func TestRuntimeBuilderFallsBackToRootWhenCurrentModelWasRemoved(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "primary-secret")
	cfg := loadRuntimeConfig(t, "https://example.invalid/v1", 120)
	builder := newRuntimeBuilderForTest(t, nil)
	set, err := builder.build(cfg, domain.ModelSelection{Profile: "removed", Model: "gone"})
	if err != nil {
		t.Fatal(err)
	}
	if set.DefaultSelection != (domain.ModelSelection{Profile: "primary", Model: "a"}) {
		t.Fatalf("default selection=%+v", set.DefaultSelection)
	}
}

func asTypedConfiguration(err error, target **domain.TypedError) bool {
	return errors.As(err, target) && (*target).Kind == domain.ErrorConfigurationInvalid
}

func loadRuntimeConfig(t *testing.T, baseURL string, timeout int) config.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.jsonc")
	body := fmt.Sprintf(`{
  "model": "primary/a",
  "provider": {
    "primary": {
      "options": {"baseURL": %q, "apiKeyEnv": "PRIMARY_KEY"},
      "models": {"b": {}, "a": {}}
    },
    "secondary": {
      "options": {"baseURL": %q, "apiKeyEnv": "SECONDARY_KEY"},
      "models": {"c": {}}
    }
  },
  "limits": {"maxToolCalls": 7, "shellTimeoutSeconds": %d}
}`, baseURL, baseURL, timeout)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.LoadOptions{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func newRuntimeBuilderForTest(t *testing.T, client *http.Client) runtimeBuilder {
	t.Helper()
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store := jsonl.New(t.TempDir(), jsonl.Options{})
	session, err := store.Create(t.Context(), workspace, domain.ModeAsk, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	return runtimeBuilder{
		configPath:    "/home/user/.config/yordam/config.jsonc",
		cli:           cli.Options{MaxToolCalls: 32, ShellTimeout: 120 * time.Second},
		workspace:     workspace,
		store:         store,
		policy:        newPolicyBinding(permission.NewSession(domain.ModeAsk)),
		activeSession: &sessionBinding{id: session.ID},
		runtimeEvents: make(chan agent.RuntimeEvent, 64),
		httpClient:    client,
	}
}
