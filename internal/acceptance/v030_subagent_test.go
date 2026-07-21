package acceptance_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	"github.com/muratmirgun/yordam/internal/tui/components"
)

// TestV030Subagent is the release-facing acceptance matrix for the sequential
// child runtime. Every entry names a production-path test exactly; runV030GoTest
// first checks the package inventory so a renamed, deleted, or accidentally
// broadened test cannot turn this gate into a silent pass.
func TestV030Subagent(t *testing.T) {
	t.Run("real_bootstrap_jsonl_edit_test_failure_restart_and_secret_boundaries", assertV030SubagentEndToEnd)

	t.Run("sequential_child_execution_and_exact_parent_continuation", func(t *testing.T) {
		runV030GoTest(t, "./internal/orchestrator", "TestSequentialChildCoordinatorRunsOrdinaryChildTurnAndReadsReceipt")
		runV030GoTest(t, "./internal/orchestrator", "TestRunTurnSubagentUsesRealSequentialCoordinatorAndContinuesParent")
		runV030GoTest(t, "./internal/orchestrator", "TestStructuredFileChangedEffectBindsCanonicalEditAndFailsClosed")
		runV030GoTest(t, "./internal/orchestrator", "TestFileChangedEffectSharesTheUncertainTerminalTransactionAndIdentity")
		runV030GoTest(t, "./internal/tooling", "TestExecutionResultCarriesOnlyStructuredFileChangeFacts")
		runV030GoTest(t, "./internal/subagent", "TestProjectReceiptUsesOnlyDurableEffectsAndMarksUnmatchedActivityUncertain")
		runV030GoTest(t, "./internal/subagent", "TestProjectReceiptExcludesShellPlansThatNeverStarted")
		runV030GoTest(t, "./internal/subagent", "TestProjectReceiptRetainsStructuredFileEffectWhenActivityIsUncertain")
	})

	t.Run("lane_exclusivity_and_parent_child_ordering", func(t *testing.T) {
		runV030GoTest(t, "./internal/orchestrator", "TestManagedOperationLeaseYieldsAndReacquiresTheIdenticalClaim")
		runV030GoTest(t, "./internal/orchestrator", "TestManagedOperationLeasePreservesFIFOAndReacquiresAfterChildTerminal")
		runV030GoTest(t, "./internal/orchestrator", "TestManagedOperationLeaseReacquiresAfterCancelledChild")
		runV030GoTest(t, "./internal/orchestrator", "TestOperationLaneSerializesTurnsAndControlsAcrossSessions")
	})

	t.Run("permission_mode_shell_and_catalog_isolation", func(t *testing.T) {
		runV030GoTest(t, "./internal/tooling", "TestPlanOrchestratedAuthorizationAcceptsOnlyExactCanonicalMarkerAndBindsRequest")
		runV030GoTest(t, "./internal/orchestrator", "TestRunTurnSubagentInterceptsOnlyTheCanonicalDescriptor")
		runV030GoTest(t, "./internal/orchestrator", "TestSubagentAuthorizationRequestBindsExactCanonicalPlanAndTurnIdentity")
		runV030GoTest(t, "./internal/orchestrator", "TestRunTurnSubagentDerivesChildExposureWithoutSubagent")
		runV030GoTest(t, "./internal/app", "TestAppChildPermissionPromptsRouteSameChildCallIDByDisplayedCorrelation")
		runV030GoTest(t, "./internal/app", "TestAppChildShellAcknowledgementCommandDoesNotMutateParentAutoPolicy")
		runV030GoTest(t, "./internal/tui", "TestChildShellNeverUsesParentAutoAcknowledgement")
		runV030GoTest(t, "./internal/app", "TestRuntimeSetChildSkillCatalogHandoffIsFrozenAndExact")
		runV030GoTest(t, "./internal/app", "TestRuntimeBuilderComposesCompleteImmutableChildRuntimeOnlyWhenEnabled")
	})

	t.Run("depth_active_attempt_tool_and_time_limits", func(t *testing.T) {
		runV030GoTest(t, "./internal/orchestrator", "TestRunTurnSubagentDisabledRejectsBeforeChildDispatch")
		runV030GoTest(t, "./internal/orchestrator", "TestRunTurnSubagentAttemptsOneThroughFourThenRejectsFifth")
		runV030GoTest(t, "./internal/orchestrator", "TestRunTurnSubagentChildDepthRejectsHiddenDelegation")
		runV030GoTest(t, "./internal/orchestrator", "TestRunTurnSubagentChildToolCallLimitTerminalizesReceipt")
		runV030GoTest(t, "./internal/orchestrator", "TestRunTurnSubagentLimitsAndFailuresRejectInvalidCalls")
		runV030GoTest(t, "./internal/orchestrator", "TestSubagentCancelDuringChildProviderStreamCommitsOneCancelledReceipt")
	})

	t.Run("statuses_explicit_retry_and_restart_boundaries", func(t *testing.T) {
		runV030GoTest(t, "./internal/subagent", "TestSubagentProjectorAcceptsEveryTerminalStatus")
		runV030GoTest(t, "./internal/subagent", "TestSubagentRecoverRestartAndCancellationMatrix")
		runV030GoTest(t, "./internal/orchestrator", "TestSubagentRecoveryCreatesReservedChildOnceAndContinuesParent")
		runV030GoTest(t, "./internal/orchestrator", "TestRecoveryReconstructsExactSubagentToolResultContinuation")
		runV030GoTest(t, "./internal/orchestrator", "TestRecoveryParentReconstructionBindsExactFrozenProviderAndModelPair")
		runV030GoTest(t, "./internal/orchestrator", "TestRecoveryTerminalizesUncertainSubagentWithStructuredNonRetryableDiagnostic")
		runV030GoTest(t, "./internal/orchestrator", "TestRecoveryChildInspectionInfrastructureFailureRemainsRetryable")
	})

	t.Run("cancellation_at_provider_approval_and_shell_boundaries", func(t *testing.T) {
		runV030GoTest(t, "./internal/orchestrator", "TestSequentialChildCoordinatorCancellationReadsCancelledReceipt")
		runV030GoTest(t, "./internal/orchestrator", "TestSubagentCancelDuringChildApprovalRemovesPromptWithoutGrantOrRetry")
		runV030GoTest(t, "./internal/orchestrator", "TestSubagentCancelDuringChildShellProcessStopsProcessAndWritesOneReceipt")
	})

	t.Run("identity_receipt_and_secret_surfaces", func(t *testing.T) {
		runV030GoTest(t, "./internal/orchestrator", "TestRecoveryChildReceiptBindsManifestRuntimeAndTerminalCursor")
		runV030GoTest(t, "./internal/orchestrator", "TestProviderSplitSecretNeverAppearsInDurableEvents")
		runV030GoTest(t, "./internal/app", "TestApplicationSubagentReceiptEventCarriesOnlyStableStageIdentity")
		runV030GoTest(t, "./internal/app", "TestProjectSubagentCardsUsesDurableParentAndChildFactsWithRedaction")
		runV030GoTest(t, "./internal/app", "TestAppRedactsConfiguredSecretFromEveryPublishedEventField")
		runV030GoTest(t, "./internal/tui", "TestConfiguredSecretPromptIsRedactedBeforeConversationRendering")
		runV030GoTest(t, "./internal/tui", "TestDurableSubagentSnapshotRendersCardAndNavigatesChildAndParent")
	})

	t.Run("public_documentation_contract", func(t *testing.T) {
		runV030GoTest(t, "./internal/repolint", "TestV030SubagentDocumentation")
	})
}

type v030SubagentProviderRequest struct {
	body          string
	authorization string
	role          string
	ordinal       int
}

func assertV030SubagentEndToEnd(t *testing.T) {
	t.Helper()
	const providerSecret = "V030_SUBAGENT_SECRET_<>&_9f2"
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	if err := os.MkdirAll(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	writeV030SubagentFile(t, filepath.Join(workspace, "target.txt"), "before\n")
	writeV030SubagentFile(t, filepath.Join(workspace, "secret.txt"), providerSecret+"\n")
	canonicalTarget, err := filepath.EvalSymlinks(filepath.Join(workspace, "target.txt"))
	if err != nil {
		t.Fatal(err)
	}
	beforeDigest := fmt.Sprintf("%x", sha256.Sum256([]byte("before\n")))
	childDigest := fmt.Sprintf("%x", sha256.Sum256([]byte("child\n")))

	var mu sync.Mutex
	requests := make([]v030SubagentProviderRequest, 0, 9)
	parentRequests, childRequests := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
			return
		}
		text := string(body)
		role := "child"
		if strings.Contains(text, `"name":"subagent"`) {
			role = "parent"
		}
		mu.Lock()
		if role == "parent" {
			parentRequests++
		} else {
			childRequests++
		}
		ordinal := childRequests
		if role == "parent" {
			ordinal = parentRequests
		}
		requests = append(requests, v030SubagentProviderRequest{body: text, authorization: request.Header.Get("Authorization"), role: role, ordinal: ordinal})
		mu.Unlock()

		currentBytes, readErr := os.ReadFile(filepath.Join(workspace, "target.txt"))
		if readErr != nil {
			t.Errorf("read target during provider request: %v", readErr)
			http.Error(response, "fixture target unavailable", http.StatusInternalServerError)
			return
		}
		current := strings.TrimSpace(string(currentBytes))
		switch role + fmt.Sprint(ordinal) {
		case "parent1":
			if current != "before" {
				t.Errorf("parent dispatched before-state=%q want before", current)
			}
			call := fmt.Sprintf(`{"task":"edit target, run the focused shell test, and do not reveal %s","expected_output":"changed file plus exact test evidence","context":"parent context %s"}`, providerSecret, providerSecret)
			writeV030SubagentToolCall(response, "delegate-success", "subagent", call)
		case "child1":
			if current != "before" {
				t.Errorf("child edit dispatched after state=%q want before", current)
			}
			call := fmt.Sprintf(`{"path":"target.txt","expected_sha256":"%s","replacements":[{"old":"before\n","new":"child\n"}]}`, beforeDigest)
			writeV030SubagentToolCall(response, "edit-child", "edit", call)
		case "child2":
			if current != "child" {
				t.Errorf("child shell dispatched after state=%q want child", current)
			}
			writeV030SubagentToolCall(response, "shell-child", "shell", `{"command":"cat secret.txt; test \"$(cat target.txt)\" = child","cwd":"."}`)
		case "child3":
			if current != "child" {
				t.Errorf("child final dispatched after state=%q want child", current)
			}
			writeV030SubagentText(response, "child completed focused test "+providerSecret)
		case "parent2":
			if current != "child" || !strings.Contains(text, "delegate-success") || !strings.Contains(text, "commands_and_tests") {
				t.Errorf("parent continuation lacks exact child receipt or state: state=%q body=%s", current, text)
			}
			call := fmt.Sprintf(`{"path":"target.txt","expected_sha256":"%s","replacements":[{"old":"child\n","new":"parent\n"}]}`, childDigest)
			writeV030SubagentToolCall(response, "edit-parent", "edit", call)
		case "parent3":
			if current != "parent" {
				t.Errorf("parent final dispatched after state=%q want parent", current)
			}
			writeV030SubagentText(response, "parent continued only after attached child receipt")
		case "parent4":
			if current != "parent" {
				t.Errorf("second delegation changed prior state=%q", current)
			}
			call := fmt.Sprintf(`{"task":"investigate provider failure without revealing %s","expected_output":"bounded failure receipt"}`, providerSecret)
			writeV030SubagentToolCall(response, "delegate-failure", "subagent", call)
		case "child4":
			response.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(response, "provider child failure "+providerSecret)
		case "parent5":
			if !strings.Contains(text, "delegate-failure") || !strings.Contains(text, `\"status\":\"uncertain\"`) {
				t.Errorf("parent failure continuation lacks exact uncertain receipt: %s", text)
			}
			writeV030SubagentText(response, "parent handled bounded child failure")
		default:
			http.Error(response, "unexpected scripted request", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	t.Setenv("V030_SUBAGENT_KEY", providerSecret)
	configPath := filepath.Join(root, "config.jsonc")
	cfg := config.Config{
		ActiveProfile: "fixture",
		Profiles: map[string]config.Profile{"fixture": {
			BaseURL: server.URL + "/v1", APIKeyEnv: "V030_SUBAGENT_KEY", Models: []string{"fixture-model"}, DefaultModel: "fixture-model",
		}},
		MaxToolCalls: 16, ShellTimeoutSeconds: 5,
		Subagents: config.SubagentConfig{Enabled: true, MaxPerTurn: 4, MaxToolCalls: 4, TimeoutSeconds: 30},
	}
	if err := config.SaveGlobal(configPath, cfg); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(root, "data")
	debugLog := filepath.Join(root, "debug", "subagent.jsonl")
	cliOptions := cli.Options{Mode: domain.ModeAuto, ModeSet: true, DataDir: dataDir, DebugLog: debugLog, MaxToolCalls: 16, ShellTimeout: 5 * time.Second}
	application, initial, err := app.Bootstrap(t.Context(), app.BootstrapOptions{ConfigPath: configPath, CWD: workspace, HTTPClient: server.Client(), CLI: cliOptions})
	if err != nil || initial.ConfigurationError != nil {
		t.Fatalf("bootstrap err=%v configuration=%v", err, initial.ConfigurationError)
	}
	done := runV030App(t, application)
	visible := sendV030Command(t, "parent trusted-shell acknowledgement", application, app.Command{Kind: app.CommandAcknowledgeAutoShell}, app.EventState)
	firstVisible, firstChildPrompt := runV030SubagentTurn(t, application, initial.Session.ID, "delegate edit and test "+providerSecret)
	visible += firstVisible
	if firstChildPrompt != 1 {
		t.Fatalf("child-specific prompts=%d want one shell prompt despite parent acknowledgement", firstChildPrompt)
	}
	secondVisible, secondChildPrompt := runV030SubagentTurn(t, application, initial.Session.ID, "delegate failing investigation "+providerSecret)
	visible += secondVisible
	if secondChildPrompt != 0 {
		t.Fatalf("failed child prompts=%d want zero", secondChildPrompt)
	}
	shutdownV030App(t, application, done)

	if got := strings.TrimSpace(readV030File(t, filepath.Join(workspace, "target.txt"))); got != "parent" {
		t.Fatalf("final workspace value=%q want parent", got)
	}
	mu.Lock()
	captured := append([]v030SubagentProviderRequest(nil), requests...)
	mu.Unlock()
	if len(captured) != 9 || parentRequests != 5 || childRequests != 4 {
		t.Fatalf("provider script requests=%d parent=%d child=%d trace=%+v", len(captured), parentRequests, childRequests, captured)
	}
	for _, request := range captured {
		if request.authorization != "Bearer "+providerSecret {
			t.Fatalf("%s provider authorization=%q", request.role, request.authorization)
		}
		assertV030SubagentVariantsAbsent(t, providerSecret, request.body)
	}

	store := jsonl.New(dataDir, jsonl.Options{})
	parent, err := store.InspectSession(t.Context(), protocol.SessionID(initial.Session.ID))
	if err != nil {
		t.Fatal(err)
	}
	manifests := v030SubagentManifests(t, parent.Journal.Events)
	if len(manifests) != 2 || manifests[0].ChildSessionID == manifests[1].ChildSessionID || manifests[0].AttemptID == manifests[1].AttemptID {
		t.Fatalf("delegation identities=%+v", manifests)
	}
	for index, manifest := range manifests {
		child, inspectErr := store.InspectSession(t.Context(), manifest.ChildSessionID)
		if inspectErr != nil {
			t.Fatal(inspectErr)
		}
		receipt, receiptAt := v030SubagentReceipt(t, child.Journal.Events)
		attachment, attachedAt := v030SubagentAttachment(t, parent.Journal.Events, manifest.AttemptID)
		if receipt.Manifest != manifest || attachment.AttemptID != manifest.AttemptID || attachment.ChildSessionID != manifest.ChildSessionID || attachment.TerminalCursor != receipt.TerminalCursor || !attachedAt.After(receiptAt) {
			t.Fatalf("handoff index=%d manifest=%+v receipt=%+v attachment=%+v receipt_at=%s attached_at=%s", index, manifest, receipt, attachment, receiptAt, attachedAt)
		}
		if index == 0 {
			if receipt.Status != "succeeded" || !containsV030String(receipt.ChangedFiles, canonicalTarget) || !containsV030Substring(receipt.CommandsAndTests, "test") {
				t.Fatalf("success receipt lacks durable edit/test evidence: %+v", receipt)
			}
		} else if receipt.Status != "uncertain" || receipt.Error == nil || receipt.Error.Message == "" || len(receipt.UnknownEffects) == 0 {
			t.Fatalf("failure receipt=%+v", receipt)
		}
		assertV030SubagentVariantsAbsent(t, providerSecret, v030JSON(receipt), v030JSON(attachment), v030JSON(child.Journal.Events))
	}
	assertV030SubagentVariantsAbsent(t, providerSecret, visible, v030JSON(parent.Journal.Events), readV030File(t, debugLog))
	assertV030TreeOmits(t, dataDir, providerSecret)

	restartOptions := cliOptions
	restartOptions.Continue = true
	restartOptions.ModeSet = false
	restarted, snapshot, err := app.Bootstrap(t.Context(), app.BootstrapOptions{ConfigPath: configPath, CWD: workspace, HTTPClient: server.Client(), CLI: restartOptions})
	if err != nil || snapshot.Session.ID != initial.Session.ID || snapshot.Durable == nil {
		t.Fatalf("restart err=%v session=%q durable=%+v", err, snapshot.Session.ID, snapshot.Durable)
	}
	cards := v030SubagentCards(t, *snapshot.Durable)
	if len(cards) != 2 || cards[0].State != protocol.SubagentStageSucceeded || cards[1].State != protocol.SubagentStageUncertain {
		t.Fatalf("restart cards=%+v", cards)
	}
	cardView := components.NewChildCards(cards).View(100)
	assertV030SubagentVariantsAbsent(t, providerSecret, cardView, v030JSON(snapshot))
	restartedDone := runV030App(t, restarted)
	shutdownV030App(t, restarted, restartedDone)
}

func runV030SubagentTurn(t *testing.T, application *app.App, parentSessionID, prompt string) (string, int) {
	t.Helper()
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: prompt}
	deadline := time.NewTimer(v030FixtureOperationDeadline)
	defer deadline.Stop()
	var visible strings.Builder
	childPrompts := 0
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
			if event.Kind == app.EventPermissionRequested {
				if event.Permission == nil || event.Permission.ParentSessionID != parentSessionID || event.Permission.DelegationAttemptID == "" {
					t.Fatalf("mutation prompt is not bound to child lineage: %+v", event.Permission)
				}
				childPrompts++
				application.Commands() <- app.Command{Kind: app.CommandResolvePermission, CallID: event.Permission.Call.Request.CallID, Decision: domain.PermissionDecision{Action: domain.PermissionAllow, Lifetime: domain.PermissionOnce, Scope: event.Permission.Call.CanonicalScope, Reason: "acceptance child approval"}}
			}
			if event.Kind == app.EventError || event.Kind == app.EventTurnInterrupted {
				t.Fatalf("subagent turn failed: kind=%s code=%s message=%s err=%v", event.Kind, event.Code, event.Message, event.Err)
			}
			if event.Kind == app.EventTurnCompleted {
				return visible.String(), childPrompts
			}
		case <-deadline.C:
			t.Fatalf("subagent turn timed out after %s", v030FixtureOperationDeadline)
		}
	}
}

func writeV030SubagentToolCall(response http.ResponseWriter, callID, name, arguments string) {
	response.Header().Set("Content-Type", "text/event-stream")
	fmt.Fprintf(response, "data: {\"id\":\"v030-subagent\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":%q,\"function\":{\"name\":%q,\"arguments\":%q}}]}}]}\n\n", callID, name, arguments)
	_, _ = io.WriteString(response, "data: {\"id\":\"v030-subagent\",\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n")
}

func writeV030SubagentText(response http.ResponseWriter, value string) {
	response.Header().Set("Content-Type", "text/event-stream")
	fmt.Fprintf(response, "data: {\"id\":\"v030-subagent\",\"choices\":[{\"delta\":{\"content\":%q},\"finish_reason\":\"stop\"}]}\n\n", value)
	_, _ = io.WriteString(response, "data: [DONE]\n\n")
}

func writeV030SubagentFile(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
}

func v030SubagentManifests(t *testing.T, events []protocol.EventRecord) []protocol.SubagentManifestV1 {
	t.Helper()
	var result []protocol.SubagentManifestV1
	for _, event := range events {
		if event.Envelope.Kind != protocol.EventSubagentRequested {
			continue
		}
		var request protocol.SubagentRequestedV1
		if err := json.Unmarshal(event.Envelope.Payload, &request); err != nil {
			t.Fatal(err)
		}
		result = append(result, request.Manifest)
	}
	return result
}

func v030SubagentReceipt(t *testing.T, events []protocol.EventRecord) (protocol.SubagentReceiptV1, time.Time) {
	t.Helper()
	for _, event := range events {
		if event.Envelope.Kind == protocol.EventSubagentReceipt {
			var receipt protocol.SubagentReceiptV1
			if err := json.Unmarshal(event.Envelope.Payload, &receipt); err != nil {
				t.Fatal(err)
			}
			return receipt, event.Envelope.Time
		}
	}
	t.Fatal("child receipt absent")
	return protocol.SubagentReceiptV1{}, time.Time{}
}

func v030SubagentAttachment(t *testing.T, events []protocol.EventRecord, attemptID protocol.DelegationAttemptID) (protocol.SubagentResultAttachedV1, time.Time) {
	t.Helper()
	for _, event := range events {
		if event.Envelope.Kind != protocol.EventSubagentResultAttached {
			continue
		}
		var attachment protocol.SubagentResultAttachedV1
		if err := json.Unmarshal(event.Envelope.Payload, &attachment); err != nil {
			t.Fatal(err)
		}
		if attachment.AttemptID == attemptID {
			return attachment, event.Envelope.Time
		}
	}
	t.Fatal("parent attachment absent")
	return protocol.SubagentResultAttachedV1{}, time.Time{}
}

func v030SubagentCards(t *testing.T, durable protocol.DurableProjection) []protocol.SubagentCardV1 {
	t.Helper()
	cards := make([]protocol.SubagentCardV1, 0, len(durable.Subagents))
	for _, view := range durable.Subagents {
		var card protocol.SubagentCardV1
		if err := json.Unmarshal(view.Data, &card); err != nil {
			t.Fatal(err)
		}
		cards = append(cards, card)
	}
	return cards
}

func assertV030SubagentVariantsAbsent(t *testing.T, secret string, surfaces ...string) {
	t.Helper()
	variants := v030SecretVariants(secret)
	variants = append(variants, v030JSONEscaped(secret))
	for _, surface := range surfaces {
		for _, variant := range variants {
			if strings.Contains(surface, variant) {
				t.Fatalf("subagent secret variant escaped boundary: variant=%q surface=%s", variant, surface)
			}
		}
	}
}

func containsV030String(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func containsV030Substring(values []string, want string) bool {
	for _, value := range values {
		if strings.Contains(value, want) {
			return true
		}
	}
	return false
}
