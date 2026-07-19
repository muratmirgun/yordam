package acceptance_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/cli"
	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
	"github.com/muratmirgun/yordam/internal/tui/components"
)

// TestV030Compaction is intentionally runnable without build tags so release
// operators can execute the command documented in the v0.3 acceptance brief.
// Its subprocesses exercise production packages with deterministic provider
// streams and fault injection; none needs an external credential.
func TestV030Compaction(t *testing.T) {
	t.Run("manual_success_replay_and_immutable_source_prefix", func(t *testing.T) {
		assertV030RealJournalCompactionAndRestart(t)
	})
	t.Run("automatic_threshold_disabled_and_unknown_window", func(t *testing.T) {
		runV030GoTest(t, "./internal/orchestrator", "TestAutomaticCompactionPolicyThresholds")
		runV030GoTest(t, "./internal/compaction", "TestEvaluateThresholdsAndReserves")
		runV030GoTest(t, "./internal/compaction", "TestEvaluateRejectsInvalidBudgets")
	})
	t.Run("prior_summary_suffix_and_large_tool_references", func(t *testing.T) {
		runV030GoTest(t, "./internal/compaction", "TestSelectRetainsSafetyFactsAndBoundsInputDeterministically")
		runV030GoTest(t, "./internal/context", "TestContextPlanReconstructsLatestValidNativeSummary")
		runV030GoTest(t, "./internal/context", "TestContextPlanAdaptsEventTranscriptAndLegacyCompaction")
	})
	t.Run("provider_refusal_and_context_too_large_are_terminal_without_retry", func(t *testing.T) {
		runV030GoTest(t, "./internal/orchestrator", "TestRunCompactionIsIdleOnlyAndIdempotent")
		runV030GoTest(t, "./internal/orchestrator", "TestRunCompactionFaultsDoNotLeaveTheLaneHeldOrRepeatProviderEgress")
		runV030GoTest(t, "./internal/orchestrator", "TestAutomaticCompactionFailuresAreTerminalAndNeverRetry")
		runV030GoTest(t, "./internal/tui/components", "TestContextTooLargeShowsCompactAction")
	})
	t.Run("cancellation_evidence_failure_and_commit_recovery_boundaries", func(t *testing.T) {
		runV030GoTest(t, "./internal/orchestrator", "TestRunCompactionCancellationBeforeAndDuringStream")
		runV030GoTest(t, "./internal/orchestrator", "TestCommitUnknownResolutionAdoptsProvenCommitForEveryCaller")
		runV030GoTest(t, "./internal/orchestrator", "TestCommitUnknownResolutionReturnsTypedUncertaintyWithoutResend")
	})
	t.Run("restart_reconstructs_verified_evidence_and_preserves_suffix", func(t *testing.T) {
		runV030GoTest(t, "./internal/context", "TestEvidenceSummaryResolverFailsClosedToEarlierValidCompaction")
		runV030GoTest(t, "./internal/context", "TestContextPlanKeepsEarlierResolvableSummaryWhenLaterEvidenceIsMissing")
		runV030GoTest(t, "./internal/session/jsonl", "TestExplicitRecoveryPreservesTailAndCommitsDiagnostic")
	})
	t.Run("secret_absence_across_provider_evidence_events_tui_logs_and_public_errors", func(t *testing.T) {
		runV030GoTest(t, "./internal/orchestrator", "TestProviderSplitSecretNeverAppearsInDurableEvents")
		runV030GoTest(t, "./internal/app", "TestPublishRedactsEquivalentTextFieldsWithActiveBinding")
		runV030GoTest(t, "./internal/app", "TestProductionCompactProtocolSerializesCancelledContext")
		runV030GoTest(t, "./internal/tui", "TestConfiguredSecretPromptIsRedactedBeforeConversationRendering")
	})
	t.Run("public_documentation_contract", func(t *testing.T) {
		runV030GoTest(t, "./internal/repolint", "TestV030CompactionDocumentation")
	})
}

func TestV030GoTestInventoryRequiresAnExactTopLevelName(t *testing.T) {
	output := "TestExact\nTestExact/subtest\nok\texample.test\t0.01s\n"
	if got := v030TopLevelInventoryMatches(output, "TestExact"); got != 1 {
		t.Fatalf("exact top-level matches=%d want=1", got)
	}
	if got := v030TopLevelInventoryMatches(output, "TestExact/subtest"); got != 0 {
		t.Fatalf("subtest matches=%d want=0", got)
	}
}

func runV030GoTest(t *testing.T, pkg, testName string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	listed := exec.CommandContext(t.Context(), "go", "test", pkg, "-list", "^"+regexp.QuoteMeta(testName)+"$")
	listed.Dir = root
	output, err := listed.CombinedOutput()
	if err != nil || v030TopLevelInventoryMatches(string(output), testName) != 1 {
		t.Fatalf("package=%s test=%s: %v\n%s", pkg, testName, err, output)
	}
	command := exec.CommandContext(t.Context(), "go", "test", pkg, "-run", "^"+regexp.QuoteMeta(testName)+"$", "-count=1")
	command.Dir = root
	output, err = command.CombinedOutput()
	if err != nil {
		t.Fatalf("package=%s test=%s: %v\n%s", pkg, testName, err, output)
	}
}

func v030TopLevelInventoryMatches(output, want string) int {
	matches := 0
	for _, line := range strings.Split(output, "\n") {
		if line == want {
			matches++
		}
	}
	return matches
}

func assertV030RealJournalCompactionAndRestart(t *testing.T) {
	t.Helper()
	const providerKey = "v030-provider-key-9ccef"
	const sentinel = "v030-summary-sentinel-80bb7"
	t.Setenv("V030_PROVIDER_KEY", providerKey)
	t.Setenv("V030_SENTINEL", sentinel)
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	var requests []v030ProviderRequest
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
			return
		}
		requests = append(requests, v030ProviderRequest{body: string(body), authorization: request.Header.Get("Authorization")})
		response.Header().Set("Content-Type", "text/event-stream")
		if len(requests) == 6 {
			response.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(response, "provider refusal "+sentinel)
			return
		}
		content := "ordinary suffix reply"
		switch len(requests) {
		case 2:
			content = `{"goal":"summary goal","constraints":[],"decisions":[],"files":[],"commands_and_tests":[],"unresolved":[],"children":[],"skills":[],"unknown_effects":[]}`
		case 4:
			content = "restarted normal reply"
		case 5:
			content = "post-restart history reply"
		default:
			if len(requests) > 6 {
				http.Error(response, "unexpected provider request", http.StatusInternalServerError)
				return
			}
		}
		_, _ = fmt.Fprintf(response, "data: {\"id\":\"v030-provider\",\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", content)
		_, _ = io.WriteString(response, "data: {\"id\":\"v030-provider\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")
		_, _ = io.WriteString(response, "data: [DONE]\n\n")
	}))
	defer server.Close()
	configPath := filepath.Join(root, "config.jsonc")
	cfg := config.Config{ActiveProfile: "fixture", Profiles: map[string]config.Profile{
		"fixture":  {BaseURL: server.URL + "/v1", APIKeyEnv: "V030_PROVIDER_KEY", Models: []string{"fixture-model"}, DefaultModel: "fixture-model"},
		"sentinel": {BaseURL: server.URL + "/v1", APIKeyEnv: "V030_SENTINEL", Models: []string{"sentinel-model"}, DefaultModel: "sentinel-model"},
	}, MaxToolCalls: 32, ShellTimeoutSeconds: 2}
	if err := config.SaveGlobal(configPath, cfg); err != nil {
		t.Fatal(err)
	}
	dataDir, debugLog := filepath.Join(root, "data"), filepath.Join(root, "debug", "v030.jsonl")
	application, snapshot, err := app.Bootstrap(t.Context(), app.BootstrapOptions{ConfigPath: configPath, CWD: workspace, HTTPClient: server.Client(), CLI: cli.Options{Mode: domain.ModeAsk, DataDir: dataDir, DebugLog: debugLog, MaxToolCalls: 32, ShellTimeout: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	done := runV030App(t, application)
	visible := sendV030Command(t, application, app.Command{Kind: app.CommandStartTurn, Prompt: "compact this " + sentinel}, app.EventTurnCompleted)
	eventsPath := filepath.Join(dataDir, "workspaces", snapshot.Workspace.ID, "sessions", snapshot.Session.ID, "events.jsonl")
	prefix, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	prefixDigest := sha256.Sum256(prefix)
	visible += sendV030Command(t, application, app.Command{Kind: app.CommandCompact}, app.EventCompactionCompleted)
	visible += sendV030Command(t, application, app.Command{Kind: app.CommandStartTurn, Prompt: "persist uncompacted suffix"}, app.EventTurnCompleted)
	shutdownV030App(t, application, done)
	after, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) <= len(prefix) || !bytes.Equal(after[:len(prefix)], prefix) || sha256.Sum256(after[:len(prefix)]) != prefixDigest {
		t.Fatal("real source journal prefix was rewritten by compaction")
	}
	if len(requests) != 3 || strings.Contains(requests[1].body, sentinel) || strings.Contains(requests[1].body, providerKey) {
		t.Fatalf("admitted summary request=%q requests=%d", requests, len(requests))
	}
	assertV030ProviderRequestBoundary(t, requests, providerKey, sentinel)
	assertV030SecretAbsent(t, visible, sentinel, providerKey)
	assertV030SecretAbsent(t, string(after), sentinel, providerKey)
	assertV030SecretAbsent(t, readV030File(t, debugLog), sentinel, providerKey)

	restarted, restart, err := app.Bootstrap(t.Context(), app.BootstrapOptions{ConfigPath: configPath, CWD: workspace, HTTPClient: server.Client(), CLI: cli.Options{Continue: true, Mode: domain.ModeAsk, DataDir: dataDir, DebugLog: debugLog, MaxToolCalls: 32, ShellTimeout: time.Second}})
	if err != nil || restart.Context == nil || restart.Context.SummaryEvidenceID == "" || restart.Context.LatestRange == nil {
		t.Fatalf("restart compaction projection=%+v err=%v", restart.Context, err)
	}
	if restartedEvents := []byte(readV030File(t, eventsPath)); !bytes.Equal(restartedEvents, after) {
		t.Fatal("restart rewrote compacted journal")
	}
	panel := components.NewContext()
	panel.SetCompactionContext(*restart.Context)
	assertV030SecretAbsent(t, panel.View(), sentinel, providerKey)
	restartedDone := runV030App(t, restarted)
	resumedVisible := sendV030Command(t, restarted, app.Command{Kind: app.CommandStartTurn, Prompt: "use reconstructed context"}, app.EventTurnCompleted)
	if len(requests) != 4 {
		t.Fatalf("restart did not dispatch a normal provider turn: requests=%d", len(requests))
	}
	if !strings.Contains(requests[3].body, "summary goal") || !strings.Contains(requests[3].body, "ordinary suffix reply") {
		t.Fatalf("restart provider context omitted stored summary or exact suffix: %q", requests[3].body)
	}
	visible += resumedVisible
	visible += sendV030Command(t, restarted, app.Command{Kind: app.CommandStartTurn, Prompt: "create post-restart history"}, app.EventTurnCompleted)
	publicFailure := sendV030Failure(t, restarted, app.Command{Kind: app.CommandCompact})
	shutdownV030App(t, restarted, restartedDone)
	if len(requests) != 6 {
		t.Fatalf("provider refusal was not dispatched: requests=%d", len(requests))
	}
	if publicFailure.code != "compaction_failed" {
		t.Fatalf("provider refusal terminal code=%q want compaction_failed", publicFailure.code)
	}
	assertV030ProviderRequestBoundary(t, requests, providerKey, sentinel)
	assertV030SecretAbsent(t, publicFailure.visible, sentinel, providerKey)
	store := jsonl.New(dataDir, jsonl.Options{})
	inspection, err := store.InspectSession(t.Context(), protocol.SessionID(snapshot.Session.ID))
	if err != nil {
		t.Fatal(err)
	}
	if !hasV030Event(inspection.Journal.Events, protocol.EventContextCompacted) {
		t.Fatal("durable journal omitted native context.compacted event")
	}
	assertV030SecretAbsent(t, visible, sentinel, providerKey)
	assertV030TreeOmits(t, root, sentinel)
	assertV030TreeOmits(t, root, providerKey)
}

type v030ProviderRequest struct {
	body          string
	authorization string
}

func assertV030ProviderRequestBoundary(t *testing.T, requests []v030ProviderRequest, providerKey, sentinel string) {
	t.Helper()
	for index, request := range requests {
		if request.authorization != "Bearer "+providerKey {
			t.Fatalf("provider request %d authorization=%q want configured Bearer key", index+1, request.authorization)
		}
		assertV030SecretAbsent(t, request.body, providerKey, sentinel)
	}
}

func runV030App(t *testing.T, application *app.App) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- application.Run(context.Background()) }()
	return done
}

func sendV030Command(t *testing.T, application *app.App, command app.Command, terminal app.EventKind) string {
	t.Helper()
	application.Commands() <- command
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	var visible strings.Builder
	for {
		select {
		case event := <-application.Events():
			raw, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			visible.Write(raw)
			if event.Err != nil {
				visible.WriteString(event.Err.Error())
			}
			if event.Kind == app.EventError || event.Kind == app.EventCompactionFailed {
				t.Fatalf("command %s failed: kind=%s code=%s message=%s", command.Kind, event.Kind, event.Code, event.Message)
			}
			if event.Kind == terminal {
				return visible.String()
			}
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", terminal)
		}
	}
}

func shutdownV030App(t *testing.T, application *app.App, done <-chan error) {
	t.Helper()
	application.Commands() <- app.Command{Kind: app.CommandShutdown}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("application shutdown timed out")
	}
}

type v030PublicFailure struct {
	code    string
	visible string
}

func sendV030Failure(t *testing.T, application *app.App, command app.Command) v030PublicFailure {
	t.Helper()
	application.Commands() <- command
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case event := <-application.Events():
			raw, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			if event.Kind == app.EventError || event.Kind == app.EventCompactionFailed {
				if event.Compaction != nil && event.Compaction.Error != nil {
					return v030PublicFailure{code: event.Compaction.Error.Code, visible: string(raw) + event.Compaction.Error.Message}
				}
				if event.Err != nil {
					return v030PublicFailure{code: event.Code, visible: string(raw) + event.Err.Error()}
				}
				return v030PublicFailure{code: event.Code, visible: string(raw) + event.Message}
			}
		case <-deadline.C:
			t.Fatal("timed out waiting for public provider refusal")
		}
	}
}

func readV030File(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func assertV030SecretAbsent(t *testing.T, value string, secrets ...string) {
	t.Helper()
	for _, secret := range secrets {
		if strings.Contains(value, secret) {
			t.Fatalf("secret %q escaped fixture boundary", secret)
		}
	}
}

func assertV030TreeOmits(t *testing.T, root, secret string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		assertV030SecretAbsent(t, readV030File(t, path), secret)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func hasV030Event(events []protocol.EventRecord, kind string) bool {
	for _, event := range events {
		if event.Envelope.Kind == kind {
			return true
		}
	}
	return false
}
