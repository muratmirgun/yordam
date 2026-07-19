//go:build acceptance

package acceptance_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/cli"
	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
	"github.com/muratmirgun/yordam/internal/testsupport/ptyfixture"
)

func acceptResume(t *testing.T) {
	t.Setenv("ACCEPTANCE_RESUME_KEY", "resume-key")
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(response, "data: {\"choices\":[{\"delta\":{\"content\":\"durable transcript\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer server.Close()
	workspace := t.TempDir()
	dataDir := t.TempDir()
	cfg := acceptanceConfig(server.URL+"/v1", "ACCEPTANCE_RESUME_KEY", map[string][]string{"resume": {"model-a"}})
	configPath := writeAcceptanceConfig(t, cfg)
	options := acceptanceCLI(dataDir, domain.ModeSafe)
	application, first, err := app.Bootstrap(context.Background(), app.BootstrapOptions{ConfigPath: configPath, CLI: options, CWD: workspace, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	done := runAcceptanceApp(t, application)
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "persist this turn"}
	waitForAppTerminal(t, application.Events())
	application.Commands() <- app.Command{Kind: app.CommandShutdown}
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	continuedOptions := options
	continuedOptions.Continue = true
	continuedApp, continued, err := app.Bootstrap(context.Background(), app.BootstrapOptions{ConfigPath: configPath, CLI: continuedOptions, CWD: workspace, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		done := runAcceptanceApp(t, continuedApp)
		continuedApp.Commands() <- app.Command{Kind: app.CommandShutdown}
		if err := <-done; err != nil {
			t.Errorf("continued app shutdown: %v", err)
		}
	}()
	if continued.Session.ID != first.Session.ID || continued.Workspace != first.Workspace || continued.Session.Workspace != first.Session.Workspace || continued.Session.Mode != first.Session.Mode || continued.Session.Selection != first.Session.Selection {
		t.Fatalf("first=%+v continued=%+v", first, continued)
	}
	var userText, assistantText string
	for _, event := range continued.Replay.Events {
		if event.Kind != domain.EventUserMessage && event.Kind != domain.EventAssistantMessage {
			continue
		}
		var payload domain.MessagePayload
		decodeEvent(t, event, &payload)
		if event.Kind == domain.EventUserMessage {
			userText = payload.Content
		} else if len(payload.ToolCalls) == 0 {
			assistantText = payload.Content
		}
	}
	if userText != "persist this turn" || assistantText != "durable transcript" {
		t.Fatalf("continued transcript user=%q assistant=%q", userText, assistantText)
	}
}

func acceptRecovery(t *testing.T) {
	root := t.TempDir()
	workspace := t.TempDir()
	store := jsonl.New(root, jsonl.Options{Sanitize: func(value any) (json.RawMessage, error) { return json.Marshal(value) }})
	canonical, err := jsonl.WorkspaceFromPath(workspace)
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.Create(context.Background(), canonical, domain.ModeAsk, domain.ModelSelection{Profile: "test", Model: "test"})
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(workspace, "untouched.txt")
	writeAcceptanceFile(t, target, []byte("before"))
	writeLegacyAcceptanceEvent(t, root, canonical, &session, domain.EventToolStarted, map[string]string{"call_id": "unmatched", "path": target, "content": "after"})
	eventsPath := filepath.Join(root, "workspaces", canonical.ID, "sessions", session.ID, "events.jsonl")
	tail := []byte(`{"schema_version":1`)
	file, err := os.OpenFile(eventsPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(tail); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	inspection, err := store.InspectSession(context.Background(), protocol.SessionID(session.ID))
	if err != nil {
		t.Fatal(err)
	}
	var recoveryDetails struct {
		ObservedTailDigest protocol.Digest `json:"observed_tail_digest"`
	}
	for _, diagnostic := range inspection.Journal.Diagnostics {
		if diagnostic.Code == "recovery.available" {
			if err := json.Unmarshal(diagnostic.Details, &recoveryDetails); err != nil {
				t.Fatal(err)
			}
		}
	}
	if inspection.Journal.Writable || recoveryDetails.ObservedTailDigest.IsZero() {
		t.Fatalf("pure inspection did not expose eligible recovery: %+v", inspection.Journal)
	}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(response, "data: [DONE]\n\n")
	}))
	defer server.Close()
	home := t.TempDir()
	configPath := filepath.Join(home, ".config", "yordam", "config.jsonc")
	if err := config.SaveGlobal(configPath, acceptanceConfig(server.URL, "RECOVERY_KEY", map[string][]string{"test": {"test"}})); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RECOVERY_KEY", "recovery-key")
	options := acceptanceCLI(root, domain.ModeAsk)
	options.Continue = true
	application, snapshot, err := app.Bootstrap(context.Background(), app.BootstrapOptions{ConfigPath: configPath, CLI: options, CWD: workspace, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	done := runAcceptanceApp(t, application)
	application.Commands() <- app.Command{Kind: app.CommandShutdown}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	replay := snapshot.Replay
	if replay.ReadOnly || !strings.Contains(replay.RecoveryNote, "recovery.completed") || !strings.Contains(replay.RecoveryNote, "migration.unmatched_activity") {
		t.Fatalf("recovery replay=%+v", replay)
	}
	artifactsDir := filepath.Join(filepath.Dir(eventsPath), "artifacts")
	entries, err := os.ReadDir(artifactsDir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("recovery artifacts=%d err=%v", len(entries), err)
	}
	recovered, err := os.ReadFile(filepath.Join(artifactsDir, entries[0].Name()))
	if err != nil || !bytes.Equal(recovered, tail) {
		t.Fatalf("recovery artifact=%q err=%v", recovered, err)
	}
	if raw, _ := os.ReadFile(target); string(raw) != "before" {
		t.Fatalf("replay executed unmatched tool: %q", raw)
	}
	again, err := store.InspectSession(context.Background(), protocol.SessionID(session.ID))
	if err != nil {
		t.Fatal(err)
	}
	recoveryDiagnostics := 0
	for _, event := range again.Journal.Events {
		if event.Envelope.Kind == protocol.EventRecoveryDiagnostic {
			recoveryDiagnostics++
		}
	}
	if !again.Journal.Writable || recoveryDiagnostics != 1 {
		t.Fatalf("restart recovery diagnostics=%d inspection=%+v", recoveryDiagnostics, again.Journal)
	}
	ptySession := ptyfixture.Start(t, ptyfixture.CachedYordam(t), workspace, cleanPTYEnvironment(home, []string{"RECOVERY_KEY=recovery-key"}), "--continue", "--data-dir", root)
	ptySession.WaitFor(t, "RECOVERY", 3*time.Second)
	ptySession.WaitFor(t, "recovery.completed", 3*time.Second)
	ptySession.Write(t, string([]byte{3}))
	ptySession.WaitForExit(t, 3*time.Second)
	ptySession.AssertRestored(t)
}

func acceptModelChange(t *testing.T) {
	workspace := t.TempDir()
	dataDir := t.TempDir()
	newSelection := domain.ModelSelection{Profile: "new", Model: "model-b"}
	started := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	models := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var body struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode provider request: %v", err)
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		index := len(models)
		models = append(models, body.Model)
		mu.Unlock()
		if index == 0 {
			close(started)
			<-release
		}
		response.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintln(response, `data: {"id":"request","choices":[{"delta":{"content":"done"},"finish_reason":"stop"}]}`)
		fmt.Fprintln(response)
		fmt.Fprintln(response, "data: [DONE]")
		fmt.Fprintln(response)
	}))
	defer server.Close()
	t.Setenv("ACCEPTANCE_OLD_KEY", "old-key")
	t.Setenv("ACCEPTANCE_NEW_KEY", "new-key")
	t.Setenv("YORDAM_PROFILE", "old")
	t.Setenv("YORDAM_MODEL", "model-a")
	cfg := config.Config{
		ActiveProfile: "old",
		Profiles: map[string]config.Profile{
			"old": {BaseURL: server.URL, APIKeyEnv: "ACCEPTANCE_OLD_KEY", Models: []string{"model-a"}, DefaultModel: "model-a"},
			"new": {BaseURL: server.URL, APIKeyEnv: "ACCEPTANCE_NEW_KEY", Models: []string{"model-b"}, DefaultModel: "model-b"},
		},
		MaxToolCalls: 32, ShellTimeoutSeconds: 2,
	}
	application, snapshot, err := app.Bootstrap(context.Background(), app.BootstrapOptions{
		ConfigPath: writeAcceptanceConfig(t, cfg), CWD: workspace, HTTPClient: server.Client(),
		CLI: acceptanceCLI(dataDir, domain.ModeAsk),
	})
	if err != nil {
		t.Fatal(err)
	}
	done := runAcceptanceApp(t, application)
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "first"}
	<-started
	application.Commands() <- app.Command{Kind: app.CommandChangeModel, Selection: newSelection}
	close(release)
	if event := waitForAppTerminal(t, application.Events()); event.Kind != app.EventTurnCompleted {
		t.Fatalf("first terminal=%+v", event)
	}
	if event := <-application.Events(); event.Kind != app.EventState || event.Selection != newSelection {
		t.Fatalf("queued model event=%+v", event)
	}
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "second"}
	waitForAppTerminal(t, application.Events())
	application.Commands() <- app.Command{Kind: app.CommandShutdown}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	gotModels := slices.Clone(models)
	mu.Unlock()
	if !slices.Equal(gotModels, []string{"model-a", "model-b"}) {
		t.Fatalf("provider models=%v", gotModels)
	}
	store := jsonl.New(dataDir, jsonl.Options{})
	inspection, err := store.InspectSession(context.Background(), protocol.SessionID(snapshot.Session.ID))
	if err != nil {
		t.Fatal(err)
	}
	modelChanges := 0
	for _, event := range inspection.Journal.Events {
		if event.Envelope.Kind == protocol.EventModelChanged {
			modelChanges++
		}
	}
	if modelChanges != 1 {
		t.Fatalf("durable model changes=%d", modelChanges)
	}
}

func acceptReleaseTargets(t *testing.T) {
	dist := filepath.Join(repositoryRoot(), "dist")
	archives, err := filepath.Glob(filepath.Join(dist, "*.tar.gz"))
	if err != nil {
		t.Fatal(err)
	}
	sboms, err := filepath.Glob(filepath.Join(dist, "*.spdx.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(archives) != 4 || len(sboms) != 4 {
		t.Fatalf("release artifacts archives=%d sboms=%d; run goreleaser release --snapshot --clean first", len(archives), len(sboms))
	}
	wantTargets := []string{"darwin_amd64", "darwin_arm64", "linux_amd64", "linux_arm64"}
	seen := make([]string, 0, len(archives))
	for _, archive := range archives {
		base := filepath.Base(archive)
		matched := ""
		for _, target := range wantTargets {
			if strings.HasSuffix(base, "_"+target+".tar.gz") {
				matched = target
				break
			}
		}
		if matched == "" {
			t.Fatalf("unexpected archive %s", base)
		}
		seen = append(seen, matched)
		if _, err := os.Stat(archive + ".spdx.json"); err != nil {
			t.Fatalf("matching SBOM for %s: %v", base, err)
		}
	}
	slices.Sort(seen)
	if !slices.Equal(seen, wantTargets) {
		t.Fatalf("release targets=%v want=%v", seen, wantTargets)
	}

	checksumPath := filepath.Join(dist, "checksums.txt")
	raw, err := os.ReadFile(checksumPath)
	if err != nil {
		t.Fatal(err)
	}
	entries := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			t.Fatalf("invalid checksum line %q", line)
		}
		entries[fields[1]] = fields[0]
	}
	if len(entries) != 8 {
		t.Fatalf("checksum entries=%d want 8 (four archives and four SBOMs)", len(entries))
	}
	for _, path := range append(append([]string(nil), archives...), sboms...) {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(content)
		if got, want := entries[filepath.Base(path)], hex.EncodeToString(sum[:]); got != want {
			t.Fatalf("checksum %s=%q want=%q", filepath.Base(path), got, want)
		}
	}
}

func acceptanceConfig(baseURL, keyEnv string, profiles map[string][]string) config.Config {
	configured := make(map[string]config.Profile, len(profiles))
	active := ""
	for name, models := range profiles {
		if active == "" {
			active = name
		}
		configured[name] = config.Profile{BaseURL: baseURL, APIKeyEnv: keyEnv, Models: models, DefaultModel: models[0]}
	}
	return config.Config{ActiveProfile: active, Profiles: configured, MaxToolCalls: 32, ShellTimeoutSeconds: 120}
}

func writeAcceptanceConfig(t *testing.T, cfg config.Config) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.jsonc")
	if err := config.SaveGlobal(path, cfg); err != nil {
		t.Fatal(err)
	}
	return path
}

func acceptanceCLI(dataDir string, mode domain.PermissionMode) cli.Options {
	return cli.Options{Mode: mode, DataDir: dataDir, MaxToolCalls: 32, ShellTimeout: 2 * time.Second}
}

func runAcceptanceApp(t *testing.T, application *app.App) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- application.Run(context.Background()) }()
	return done
}

func waitForAppTerminal(t *testing.T, events <-chan app.Event) app.Event {
	t.Helper()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case event := <-events:
			switch event.Kind {
			case app.EventTurnCompleted, app.EventTurnInterrupted:
				return event
			case app.EventError:
				t.Fatalf("app error: %+v", event)
			}
		case <-deadline.C:
			t.Fatal("timed out waiting for app terminal event")
		}
	}
}

func repositoryRoot() string { return filepath.Clean(filepath.Join("..", "..")) }
