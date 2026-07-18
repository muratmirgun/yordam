package integration_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/cli"
	"github.com/muratmirgun/yordam/internal/domain"
)

// The detailed provider/tool FIFO and mutation safety semantics live in the
// orchestrator, tooling, authorization, and provider suites. This integration
// test guards the production composition seam that the v0.1 TUI uses.
func TestToolLoopProductionCompositionDelegatesTurnToOrchestrator(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests++
		response.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintln(response, `data: {"id":"request-a","choices":[{"delta":{"content":"done"},"finish_reason":"stop"}]}`)
		fmt.Fprintln(response)
		fmt.Fprintln(response, "data: [DONE]")
		fmt.Fprintln(response)
	}))
	defer server.Close()

	t.Setenv("INTEGRATION_KEY", "integration-secret")
	root := t.TempDir()
	configPath := filepath.Join(root, "config.jsonc")
	config := fmt.Sprintf(`{
  "model": "test/model-a",
  "provider": {
    "test": {
      "options": {"baseURL": %q, "apiKeyEnv": "INTEGRATION_KEY"},
      "models": {"model-a": {}}
    }
  }
}`, server.URL)
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	application, _, err := app.Bootstrap(t.Context(), app.BootstrapOptions{
		ConfigPath: configPath, CWD: root, HTTPClient: server.Client(),
		CLI: cli.Options{Mode: domain.ModeAsk, DataDir: filepath.Join(root, "data"), MaxToolCalls: 32, ShellTimeout: 2 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- application.Run(ctx) }()
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "reply once", DraftID: 1}

	deadline := time.After(5 * time.Second)
	completed := false
	for !completed {
		select {
		case event := <-application.Events():
			t.Logf("application event: kind=%s message=%q", event.Kind, event.Message)
			switch event.Kind {
			case app.EventError:
				cancel()
				t.Fatalf("production turn failed: %+v", event)
			case app.EventTurnCompleted:
				completed = true
			}
		case <-deadline:
			cancel()
			t.Fatal("timed out waiting for orchestrated turn")
		}
	}
	if requests != 1 {
		t.Fatalf("provider requests=%d want 1", requests)
	}
	application.Commands() <- app.Command{Kind: app.CommandShutdown}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
