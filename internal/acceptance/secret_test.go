//go:build acceptance

package acceptance_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/cli"
	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/provider/openaicompat"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
	"github.com/muratmirgun/yordam/internal/testsupport/ptyfixture"
	"github.com/muratmirgun/yordam/internal/tools/output"
	shelltool "github.com/muratmirgun/yordam/internal/tools/shell"
)

func acceptSecretHygiene(t *testing.T) {
	sentinels := []string{"v010-secret-hygiene-sentinel-7d5f1a"}
	for _, name := range []string{"YORDAM_API_KEY", "PROFILE_KEY"} {
		if value := os.Getenv(name); value != "" {
			sentinels = append(sentinels, value)
		}
	}
	sentinel := sentinels[0]
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	dataDir := filepath.Join(root, "data")
	outputDir := filepath.Join(root, "output")
	if err := os.MkdirAll(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outputDir, 0o700); err != nil {
		t.Fatal(err)
	}

	redactor := secret.New(sentinels...)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if authorization := request.Header.Get("Authorization"); authorization != "Bearer "+sentinel {
			t.Errorf("provider authorization=%q", authorization)
		}
		if requests.Add(1) == 1 {
			response.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(response, "provider failure "+sentinel)
			return
		}
		response.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(response, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", "streamed "+sentinel)
		fmt.Fprintf(response, "data: %s\n\n", sentinel)
	}))
	defer server.Close()

	client := openaicompat.New(openaicompat.ClientOptions{
		HTTPClient:  server.Client(),
		BaseURL:     server.URL + "/v1",
		APIKey:      sentinel,
		RetryDelays: []time.Duration{},
		Redact:      redactor.String,
	})
	_, providerErr := client.Stream(context.Background(), domain.ModelRequest{})
	if providerErr == nil || strings.Contains(providerErr.Error(), sentinel) || !strings.Contains(providerErr.Error(), "[REDACTED]") {
		t.Fatalf("provider error=%v", providerErr)
	}
	writeAcceptanceFile(t, filepath.Join(outputDir, "provider-error.txt"), []byte(providerErr.Error()))

	store := jsonl.New(dataDir, jsonl.Options{Sanitize: redactor.JSON})
	canonical, err := jsonl.WorkspaceFromPath(workspace)
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.Create(context.Background(), canonical, domain.ModeAsk, domain.ModelSelection{Profile: "secret", Model: "secret-model"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), session.ID, domain.EventUserMessage, domain.MessagePayload{Content: "session " + sentinel}); err != nil {
		t.Fatal(err)
	}
	buffer := output.New(output.Options{SessionID: session.ID, Artifacts: store, Redact: redactor})
	if _, err := fmt.Fprint(buffer, strings.Repeat("artifact-data-", 3000)+sentinel); err != nil {
		t.Fatal(err)
	}
	artifactResult, err := buffer.Result(context.Background())
	if err != nil || len(artifactResult.ArtifactIDs) != 1 || strings.Contains(artifactResult.Content, sentinel) {
		t.Fatalf("artifact result=%+v err=%v", artifactResult, err)
	}

	t.Setenv("ACCEPTANCE_SECRET_KEY", sentinel)
	t.Setenv("ACCEPTANCE_ORDINARY", "preserved")
	shell := shelltool.New(shelltool.Options{
		Workspace:       workspace,
		ShellPath:       "/bin/sh",
		ProviderKeyEnvs: []string{"ACCEPTANCE_SECRET_KEY"},
		Timeout:         2 * time.Second,
		Output:          output.Options{SessionID: session.ID, Artifacts: store, Redact: redactor},
	})
	prepared, err := shell.Prepare(context.Background(), domain.ToolRequest{CallID: "env", Name: "shell", Workspace: workspace, Input: json.RawMessage(`{"command":"env","cwd":"."}`)})
	if err != nil {
		t.Fatal(err)
	}
	shellResult := prepared.Execute(context.Background())
	if shellResult.Status != domain.ToolSucceeded || strings.Contains(shellResult.Content, sentinel) || strings.Contains(shellResult.Content, "ACCEPTANCE_SECRET_KEY=") || !strings.Contains(shellResult.Content, "ACCEPTANCE_ORDINARY=preserved") {
		t.Fatalf("shell env result=%+v", shellResult)
	}
	writeAcceptanceFile(t, filepath.Join(outputDir, "shell.txt"), []byte(shellResult.Content))

	debugLog := filepath.Join(root, "logs", "debug.jsonl")
	t.Setenv("YORDAM_API_KEY", sentinel)
	application, snapshot, err := app.Bootstrap(context.Background(), app.BootstrapOptions{
		Config: config.Config{
			ActiveProfile: "secret",
			Profiles: map[string]config.Profile{
				"secret": {BaseURL: server.URL + "/v1", APIKeyEnv: "ACCEPTANCE_SECRET_KEY", Models: []string{"secret-model"}, DefaultModel: "secret-model"},
			},
			MaxToolCalls:        32,
			ShellTimeoutSeconds: 120,
		},
		CLI:        cli.Options{Mode: domain.ModeAsk, DataDir: dataDir, DebugLog: debugLog, MaxToolCalls: 32, ShellTimeout: 2 * time.Second},
		CWD:        workspace,
		HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Session.ID == session.ID {
		t.Fatal("bootstrap unexpectedly reused the fixture session")
	}
	appDone := runAcceptanceApp(t, application)
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "prompt " + sentinel}
	var visible strings.Builder
	for {
		event := <-application.Events()
		visible.WriteString(event.Message)
		visible.WriteString(event.Runtime.Text)
		if event.Kind == app.EventError {
			break
		}
	}
	if strings.Contains(visible.String(), sentinel) || !strings.Contains(visible.String(), "[REDACTED]") || !strings.Contains(visible.String(), "provider stream interrupted") {
		t.Fatalf("streamed error output=%q", visible.String())
	}
	writeAcceptanceFile(t, filepath.Join(outputDir, "app-visible.txt"), []byte(visible.String()))
	application.Commands() <- app.Command{Kind: app.CommandShutdown}
	if err := <-appDone; err != nil {
		t.Fatal(err)
	}

	ptyRoot := filepath.Join(root, "pty")
	if err := os.MkdirAll(ptyRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	ptyServer := newSSEServer(t, "data: [DONE]\n\n")
	defer ptyServer.Close()
	ptyHome := t.TempDir()
	ptyConfig := filepath.Join(ptyHome, ".config", "yordam", "config.jsonc")
	if err := config.SaveGlobal(ptyConfig, acceptanceConfig(ptyServer.URL+"/v1", "ACCEPTANCE_SECRET_KEY", map[string][]string{"default": {"secret-model"}})); err != nil {
		t.Fatal(err)
	}
	ptySession := ptyfixture.Start(t, ptyfixture.CachedYordam(t), ptyRoot, cleanPTYEnvironment(ptyHome, []string{"ACCEPTANCE_SECRET_KEY=" + sentinel}),
		"--data-dir", filepath.Join(ptyRoot, "data"),
	)
	ptySession.WaitFor(t, "default/secret-model", 3*time.Second)
	ptySession.Write(t, string([]byte{3}))
	ptySession.WaitForExit(t, 3*time.Second)
	ptySession.AssertRestored(t)
	if strings.Contains(ptySession.Output(), sentinel) {
		t.Fatalf("PTY output contains sentinel: %q", ptySession.Output())
	}
	writeAcceptanceFile(t, filepath.Join(outputDir, "pty.txt"), []byte(ptySession.Output()))

	if requests.Load() < 2 {
		t.Fatalf("provider requests=%d want auth error and streamed error", requests.Load())
	}
	assertTreeOmits(t, root, sentinel)
	for _, value := range sentinels[1:] {
		assertTreeOmits(t, root, value)
	}
}
