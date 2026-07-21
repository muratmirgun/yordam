package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/compaction"
	contextplanner "github.com/muratmirgun/yordam/internal/context"
	"github.com/muratmirgun/yordam/internal/eventcodec"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/provider"
	"github.com/muratmirgun/yordam/internal/tooling"
	toolset "github.com/muratmirgun/yordam/internal/tools"
	edittool "github.com/muratmirgun/yordam/internal/tools/edit"
	"github.com/muratmirgun/yordam/internal/verification"
)

func TestRunTurnProviderLifecycleUsesDurableAuthorizationAndTerminalBarriers(t *testing.T) {
	log := &recordLog{}
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	repository := &recordingRepository{head: request.ExpectedHead, log: log}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{},
		Lane: &loggingLane{delegate: NewOperationLane(), log: log}, Repository: repository,
		TurnLeases: &recordingTurnLeaseManager{log: log}, Context: fakeContextPlanner{log: log},
		Providers: fakeProviderCatalog{log: log}, Provider: fakeProviderService{log: log},
		Tools: noToolService{}, Authorization: &allowingAuthorization{log: log},
		Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{},
		Verification: loggingVerification{log: log, delegate: verification.NewService(func() time.Time {
			return time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
		})},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := service.RunTurn(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.CommandResult.Status != "completed" {
		t.Fatalf("result=%+v", result)
	}
	got := log.snapshot()
	wantPrefix := []string{
		"lane.acquire(turn)", "turn_lease.acquire",
		"append(command.accepted,task.created,user.message,outcome.contract_declared,turn.accepted)",
		"append(outcome.contract_amended,task.status_changed,turn.state_changed)",
		"context.plan", "provider.negotiate", "provider.prepare",
		"append(context.plan_recorded,provider.capability_decided,activity.planned,authorization.requested)",
		"authorization.decide", "append(authorization.decided,activity.authorized)",
		"append(authorization.decision_consumed,activity.started)", "authorization.issue", "provider.stream",
		"append(assistant.message,provider.attempt_terminal,activity.succeeded)",
		"verification.assess",
		"append(verification.receipt_recorded,outcome.criterion_assessed,outcome.final_assessed)",
		"append(turn.completed,task.status_changed,command.completed)",
		"turn_lease.release", "lane.release",
	}
	if !slices.Equal(got, wantPrefix) {
		t.Fatalf("order:\n got=%v\nwant=%v", got, wantPrefix)
	}
}

func TestCollectProviderStreamPreservesCancellationWhenProviderCloses(t *testing.T) {
	service := &Service{}
	for attempt := 0; attempt < 100; attempt++ {
		ctx, cancel := context.WithCancel(context.Background())
		stream := make(chan protocol.ModelEvent)
		cancel()
		close(stream)
		_, _, err := service.collectProviderStream(ctx, "generation", stream)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("attempt=%d error=%v want=%v", attempt, err, context.Canceled)
		}
	}
}

func TestContextMessagesMapsDurableToolSourcesToToolRole(t *testing.T) {
	tool := protocol.ToolResultBlock{CallID: "call-a", Status: "succeeded", Text: "contents"}
	messages := contextMessages(protocol.ContextPlan{Body: protocol.ContextPlanBody{Sources: []protocol.ContentSource{
		{ID: "assistant-event", Kind: "assistant_message", Content: []protocol.ContentBlock{{Kind: protocol.ContentToolUse, ToolUse: &protocol.ToolUseBlock{CallID: tool.CallID, Alias: "read", Arguments: json.RawMessage(`{"path":"README.md"}`)}}}},
		{ID: "tool-event", Kind: "tool_message", Content: []protocol.ContentBlock{{Kind: protocol.ContentToolResult, ToolResult: &tool}}},
	}}})
	if len(messages) != 2 || messages[0].Role != "assistant" || messages[0].Blocks[0].ToolUse == nil || messages[0].Blocks[0].ToolUse.CallID != tool.CallID || messages[1].Role != "tool" || messages[1].Blocks[0].ToolResult == nil || messages[1].Blocks[0].ToolResult.CallID != tool.CallID {
		t.Fatalf("messages=%#v", messages)
	}
}

func TestRunTurnProviderPrepareReceivesProjectedAssistantAndToolMessages(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	tool := protocol.ToolResultBlock{CallID: "call-a", Status: "succeeded", Text: "contents"}
	planner := staticContextPlanner{plan: contextPlanForMessages(t, []protocol.ContentSource{
		{ID: "assistant-event", Kind: "assistant_message", Scope: "session", Provenance: "event_v2", Content: []protocol.ContentBlock{{Kind: protocol.ContentToolUse, ToolUse: &protocol.ToolUseBlock{CallID: tool.CallID, Alias: "read", Arguments: json.RawMessage(`{"path":"README.md"}`)}}}},
		{ID: "tool-event", Kind: "tool_message", Scope: "session", Provenance: "event_v2", Content: []protocol.ContentBlock{{Kind: protocol.ContentToolResult, ToolResult: &tool}}},
	})}
	providerService := &capturingProviderService{}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(),
		Repository: &recordingRepository{head: request.ExpectedHead}, TurnLeases: &recordingTurnLeaseManager{}, Context: planner,
		Providers: fakeProviderCatalog{log: &recordLog{}}, Provider: providerService, Tools: noToolService{},
		Authorization: &allowingAuthorization{log: &recordLog{}}, Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	providerService.mu.Lock()
	requests := protocol.DeepCopy(providerService.requests)
	providerService.mu.Unlock()
	if len(requests) != 1 || len(requests[0].Messages) != 2 || requests[0].Messages[0].Role != "assistant" || requests[0].Messages[0].Blocks[0].ToolUse == nil || requests[0].Messages[0].Blocks[0].ToolUse.CallID != tool.CallID || requests[0].Messages[1].Role != "tool" || requests[0].Messages[1].Blocks[0].ToolResult == nil || requests[0].Messages[1].Blocks[0].ToolResult.CallID != tool.CallID {
		t.Fatalf("prepare requests=%#v", requests)
	}
}

func TestAutomaticCompactionPolicyThresholds(t *testing.T) {
	knownWindow := protocol.ValueInt64{State: protocol.ValueKnown, Value: 10_000, Provenance: "test-window"}
	zero := protocol.ValueInt64{State: protocol.ValueKnown, Value: 0, Provenance: "test-reserve"}
	cases := []struct {
		name            string
		estimated       protocol.ValueInt64
		window          protocol.ValueInt64
		auto            bool
		reserve         protocol.ValueInt64
		wantCompactions int
		wantErr         bool
	}{
		{name: "threshold equality", estimated: protocol.ValueInt64{State: protocol.ValueKnown, Value: 7_952, Provenance: "test-estimate"}, window: knownWindow, auto: true, wantCompactions: 1},
		{name: "below threshold", estimated: protocol.ValueInt64{State: protocol.ValueKnown, Value: 7_951, Provenance: "test-estimate"}, window: knownWindow, auto: true, wantCompactions: 0},
		{name: "disabled", estimated: protocol.ValueInt64{State: protocol.ValueKnown, Value: 9_000, Provenance: "test-estimate"}, window: knownWindow, auto: false, wantCompactions: 0},
		{name: "unknown window", estimated: protocol.ValueInt64{State: protocol.ValueKnown, Value: 9_000, Provenance: "test-estimate"}, window: protocol.ValueInt64{State: protocol.ValueUnknown}, auto: true, wantCompactions: 0},
		{name: "invalid budget", estimated: protocol.ValueInt64{State: protocol.ValueKnown, Value: 9_000, Provenance: "test-estimate"}, window: knownWindow, auto: true, reserve: zero, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := validStartTurnRequest()
			request.ExpectedHead = protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(request.SessionID), CommitSeq: 7, TransactionID: "tx-initial"}
			request.Runtime = validRuntimeManifest(t, "observation")
			request.Runtime.Body.Models[0].ContextWindow = tc.window
			request.Runtime.Body.Limits.AutoCompact = tc.auto
			request.Runtime.Body.Limits.CompactReserveTokens = tc.reserve
			refreshRuntimeDigest(t, &request.Runtime)

			log := &recordLog{}
			repository := newAutomaticCompactionRepository(t, log)
			planner := &automaticCompactionPlanner{estimated: tc.estimated, window: tc.window}
			provider := &automaticCompactionProvider{log: log}
			service, err := NewService(Dependencies{
				Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: &loggingLane{delegate: NewOperationLane(), log: log},
				Repository: repository, TurnLeases: &recordingTurnLeaseManager{log: log}, Context: planner,
				Providers: fakeProviderCatalog{log: log}, Provider: provider, Authorization: &allowingAuthorization{log: log},
				Tools: noToolService{}, Evidence: &compactionEvidence{log: log}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = service.RunTurn(context.Background(), request)
			if tc.wantErr {
				if err == nil {
					t.Fatal("RunTurn accepted an invalid automatic compaction budget")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := provider.compactions.Load(); int(got) != tc.wantCompactions {
				t.Fatalf("compaction requests=%d want=%d", got, tc.wantCompactions)
			}
			if tc.wantCompactions == 1 {
				if planner.calls.Load() != 2 {
					t.Fatalf("context plan calls=%d want rebuild after compaction", planner.calls.Load())
				}
				if got := provider.normalPlanDigest(); got != planner.secondDigest() {
					t.Fatalf("normal provider context digest=%s want rebuilt=%s", got.Value, planner.secondDigest().Value)
				}
				if got := countPrefix(log.snapshot(), "lane.acquire("); got != 1 {
					t.Fatalf("lane acquires=%d want only the owning turn lane", got)
				}
			}
		})
	}
}

func TestAutomaticCompactionFailuresAreTerminalAndNeverRetry(t *testing.T) {
	cases := []struct {
		name       string
		mode       string
		evidence   EvidenceRecorder
		wantNormal int
	}{
		{name: "provider prepare failure", mode: "compact_prepare", evidence: &compactionEvidence{}},
		{name: "provider request uncertain", mode: "compact_stream", evidence: &compactionEvidence{}},
		{name: "cancellation", mode: "cancel", evidence: &compactionEvidence{}},
		{name: "evidence failure", mode: "success", evidence: failingEvidenceRecorder{}},
		{name: "normal context too large", mode: "normal_context_too_large", evidence: &compactionEvidence{}, wantNormal: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := automaticCompactionRequest(t)
			log := &recordLog{}
			repository := newAutomaticCompactionRepository(t, log)
			provider := &automaticFailureProvider{mode: tc.mode, log: log}
			service, err := NewService(Dependencies{
				Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: &loggingLane{delegate: NewOperationLane(), log: log},
				Repository: repository, TurnLeases: &recordingTurnLeaseManager{log: log}, Context: &automaticCompactionPlanner{estimated: request.Runtime.Body.Models[0].MaximumOutput, window: request.Runtime.Body.Models[0].ContextWindow},
				Providers: fakeProviderCatalog{log: log}, Provider: provider, Authorization: &allowingAuthorization{log: log},
				Tools: noToolService{}, Evidence: tc.evidence, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if tc.mode == "cancel" {
				ctx, provider.cancel = context.WithCancel(ctx)
			}
			result, err := service.RunTurn(ctx, request)
			if err == nil {
				t.Fatal("RunTurn unexpectedly succeeded")
			}
			if provider.compactPrepares.Load() != 1 || provider.normalPrepares.Load() != int64(tc.wantNormal) {
				t.Fatalf("compact=%d normal=%d want compact=1 normal=%d", provider.compactPrepares.Load(), provider.normalPrepares.Load(), tc.wantNormal)
			}
			if result.CommandResult.Status == "" || !hasProposedEvent(repository.appendRequests(), protocol.EventCommandCompleted) {
				t.Fatalf("failure was not durably terminalized: result=%+v", result)
			}
			if tc.mode == "normal_context_too_large" && (result.CommandResult.Error == nil || result.CommandResult.Error.Code != "context_too_large") {
				t.Fatalf("context failure is not actionable: result=%+v", result)
			}
		})
	}
}

func TestAutomaticCompactionRebuildsOnceBeforeLaterToolContinuation(t *testing.T) {
	request := automaticCompactionRequest(t)
	log := &recordLog{}
	repository := newAutomaticCompactionRepository(t, log)
	planner := &automaticCompactionPlanner{
		estimated: request.Runtime.Body.Models[0].MaximumOutput, window: request.Runtime.Body.Models[0].ContextWindow,
		estimates: []protocol.ValueInt64{
			request.Runtime.Body.Models[0].MaximumOutput,
			request.Runtime.Body.Models[0].MaximumOutput,
			{State: protocol.ValueKnown, Value: 7_951, Provenance: "test-estimate"},
		},
	}
	provider := &automaticLoopProvider{}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: &loggingLane{delegate: NewOperationLane(), log: log},
		Repository: repository, TurnLeases: &recordingTurnLeaseManager{log: log}, Context: planner,
		Providers: fakeProviderCatalog{log: log}, Provider: provider, Authorization: &allowingAuthorization{log: log},
		Tools: observationToolService{log: log}, Evidence: &compactionEvidence{log: log}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if provider.compactions.Load() != 1 || provider.normals.Load() != 2 {
		t.Fatalf("compactions=%d normal attempts=%d", provider.compactions.Load(), provider.normals.Load())
	}
	if planner.calls.Load() != 3 {
		t.Fatalf("context plan calls=%d want initial, rebuilt, and post-tool", planner.calls.Load())
	}
	if got := countPrefix(log.snapshot(), "lane.acquire("); got != 1 {
		t.Fatalf("lane acquires=%d want one turn lane", got)
	}
}

func TestAutomaticCompactionDeduplicatesAnUnchangedSourceDigest(t *testing.T) {
	request := automaticCompactionRequest(t)
	repository := newAutomaticCompactionRepository(t, &recordLog{})
	history, err := repository.ReadRange(context.Background(), journal.ReadRangeRequest{Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)}})
	if err != nil {
		t.Fatal(err)
	}
	selection, err := compaction.Select(history.Events, request.ExpectedHead, compaction.TriggerAutomatic, 0)
	if err != nil {
		t.Fatal(err)
	}
	provider := &automaticCompactionProvider{}
	service, err := NewService(Dependencies{Repository: repository, Lane: NewOperationLane(), Providers: fakeProviderCatalog{log: &recordLog{}}, Provider: provider})
	if err != nil {
		t.Fatal(err)
	}
	state := newTurnState(request)
	state.head = request.ExpectedHead
	state.compactedSources[selection.SourceDigest] = struct{}{}
	compacted, err := service.compactWithinTurn(context.Background(), nil, request, &state, state.head, history.Events)
	if err != nil || compacted || provider.compactions.Load() != 0 {
		t.Fatalf("compacted=%t requests=%d error=%v", compacted, provider.compactions.Load(), err)
	}
}

func automaticCompactionRequest(t *testing.T) StartTurnRequest {
	t.Helper()
	request := validStartTurnRequest()
	request.ExpectedHead = protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(request.SessionID), CommitSeq: 7, TransactionID: "tx-initial"}
	request.Runtime = validRuntimeManifest(t, "observation")
	request.Runtime.Body.Models[0].ContextWindow = protocol.ValueInt64{State: protocol.ValueKnown, Value: 10_000, Provenance: "test-window"}
	// MaximumOutput is otherwise unused by this test; its value is reused as a
	// known threshold estimate by the specialized test planner.
	request.Runtime.Body.Models[0].MaximumOutput = protocol.ValueInt64{State: protocol.ValueKnown, Value: 7_952, Provenance: "test-estimate"}
	request.Runtime.Body.Limits.AutoCompact = true
	refreshRuntimeDigest(t, &request.Runtime)
	return request
}

func hasProposedEvent(requests []journal.AppendRequest, kind string) bool {
	for _, request := range requests {
		for _, event := range request.Events {
			if event.Kind == kind {
				return true
			}
		}
	}
	return false
}

func TestCollectProviderStreamDoesNotDuplicateFinalizedDeltaBlock(t *testing.T) {
	service := &Service{deps: Dependencies{Admission: passthroughAdmission{}}}
	stream := make(chan protocol.ModelEvent, 3)
	stream <- protocol.ModelEvent{Sequence: 1, Kind: protocol.ModelEventContentDelta, Delta: &protocol.ContentDelta{BlockID: "content-1", Kind: protocol.ContentText, Text: "durable transcript"}}
	stream <- protocol.ModelEvent{Sequence: 2, Kind: protocol.ModelEventContentBlock, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: "durable transcript"}}
	stream <- protocol.ModelEvent{Sequence: 3, Kind: protocol.ModelEventTerminal, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	close(stream)
	message, _, err := service.collectProviderStream(context.Background(), "generation-a", stream)
	if err != nil {
		t.Fatal(err)
	}
	if len(message.Blocks) != 1 || message.Blocks[0].Text != "durable transcript" {
		t.Fatalf("assistant message=%#v", message)
	}
}

func TestDurableOrderingMutationPreviewCheckpointRevalidationExecutionAndContinuation(t *testing.T) {
	log := &recordLog{}
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "mutation")
	repository := &recordingRepository{head: request.ExpectedHead, log: log}
	providerService := &toolThenFinalProvider{log: log}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{},
		Lane: &loggingLane{delegate: NewOperationLane(), log: log}, Repository: repository,
		TurnLeases: &recordingTurnLeaseManager{log: log}, Context: fakeContextPlanner{log: log},
		Providers: fakeProviderCatalog{log: log}, Provider: providerService,
		Tools: mutationToolService{log: log}, Authorization: &allowingAuthorization{log: log},
		Evidence: recordingEvidence{log: log}, Recovery: recordingRecovery{log: log},
		Verification: loggingVerification{log: log, delegate: verification.NewService(time.Now)},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := service.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	providerService.mu.Lock()
	requests := protocol.DeepCopy(providerService.requests)
	providerService.mu.Unlock()
	if len(requests) != 2 || len(requests[1].Messages) == 0 {
		t.Fatalf("provider continuation requests=%#v", requests)
	}
	continuation := requests[1].Messages[len(requests[1].Messages)-1]
	if continuation.Role != "tool" || len(continuation.Blocks) != 1 || continuation.Blocks[0].Kind != protocol.ContentToolResult {
		t.Fatalf("provider continuation=%#v", continuation)
	}
	got := log.snapshot()
	wantSequence := []string{
		"tool.plan_preview",
		"append(activity.planned,execution.plan_declared,authorization.requested)",
		"authorization.decide", "append(authorization.decided,activity.authorized)",
		"append(authorization.decision_consumed,activity.started)", "authorization.issue",
		"tool.prepare_preview", "evidence.put",
		"append(activity.succeeded,evidence.recorded,evidence.linked)",
		"tool.plan_mutation", "append(activity.planned,execution.plan_declared)",
		"append(checkpoint.planned)", "tool.recovery_candidate", "recovery.put", "append(checkpoint.ready)",
		"tool.revalidate", "append(authorization.requested)", "authorization.decide",
		"append(authorization.decided,activity.authorized)", "append(authorization.decision_consumed,activity.started)",
		"authorization.issue", "tool.execute", "evidence.put",
		"append(tool.message,activity.succeeded,evidence.recorded,evidence.linked)",
		"context.plan", "provider.negotiate", "provider.prepare",
	}
	if !containsContiguous(got, wantSequence) {
		t.Fatalf("mutation order missing:\n got=%v\nwant contiguous=%v", got, wantSequence)
	}
}

func TestSequentialToolIntentsExecuteFIFOInProviderOrder(t *testing.T) {
	log := &recordLog{}
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{},
		Lane: NewOperationLane(), Repository: &recordingRepository{head: request.ExpectedHead}, TurnLeases: &recordingTurnLeaseManager{},
		Context: fakeContextPlanner{log: log}, Providers: fakeProviderCatalog{log: log}, Provider: &twoToolThenFinalProvider{log: log},
		Tools: observationToolService{log: log}, Authorization: &allowingAuthorization{log: log}, Evidence: recordingEvidence{log: log},
		Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	executions := make([]string, 0, 2)
	for _, value := range log.snapshot() {
		if strings.HasPrefix(value, "tool.execute(") {
			executions = append(executions, value)
		}
	}
	if !slices.Equal(executions, []string{"tool.execute(call-b)", "tool.execute(call-a)"}) {
		t.Fatalf("execution order=%v", executions)
	}
}

func TestSequentialToolContinuationsRetainEveryPriorResult(t *testing.T) {
	log := &recordLog{}
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	providerService := &threeRoundToolProvider{log: log}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{},
		Lane: NewOperationLane(), Repository: &recordingRepository{head: request.ExpectedHead}, TurnLeases: &recordingTurnLeaseManager{},
		Context: fakeContextPlanner{log: log}, Providers: fakeProviderCatalog{log: log}, Provider: providerService,
		Tools: observationToolService{log: log}, Authorization: &allowingAuthorization{log: log}, Evidence: recordingEvidence{log: log},
		Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	providerService.mu.Lock()
	requests := protocol.DeepCopy(providerService.requests)
	providerService.mu.Unlock()
	if len(requests) != 4 {
		t.Fatalf("provider requests=%d want four", len(requests))
	}
	for requestIndex, modelRequest := range requests {
		var resultIDs []string
		for _, message := range modelRequest.Messages {
			if message.Role != "tool" || len(message.Blocks) != 1 || message.Blocks[0].ToolResult == nil {
				continue
			}
			resultIDs = append(resultIDs, message.Blocks[0].ToolResult.CallID)
		}
		want := []string{"round-call-1", "round-call-2", "round-call-3"}[:requestIndex]
		if !slices.Equal(resultIDs, want) {
			t.Fatalf("request %d tool results=%v want=%v", requestIndex, resultIDs, want)
		}
	}
}

func TestToolResultSharesSuccessfulActivityTransaction(t *testing.T) {
	log := &recordLog{}
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	repository := &recordingRepository{head: request.ExpectedHead}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repository,
		TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: log}, Providers: fakeProviderCatalog{log: log},
		Provider: &toolThenFinalProvider{log: log}, Tools: evidencedObservationToolService{observationToolService{log: log}},
		Authorization: &allowingAuthorization{log: log}, Evidence: recordingEvidence{log: log}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	terminal, result, ok := terminalToolResult(repository.appendRequests(), protocol.EventActivitySucceeded, "call-a")
	if !ok {
		t.Fatalf("successful tool terminal did not atomically persist tool.message: %v", repository.batchKinds())
	}
	if terminal.TransactionID == "" || result.CallID != "call-a" || result.Status != "succeeded" || !slices.Equal(result.EvidenceIDs, []protocol.EvidenceID{"evidence-a", "evidence-z"}) {
		t.Fatalf("terminal=%+v result=%+v", terminal, result)
	}
}

func TestToolResultTerminalPathsPersistConservativeStatus(t *testing.T) {
	tests := []struct {
		name       string
		configure  func(*testing.T, *StartTurnRequest) (ToolService, AuthorizationService)
		wantKind   string
		wantStatus string
		wantErr    bool
	}{
		{
			name: "unknown tool", wantKind: protocol.EventActivityFailed, wantStatus: "failed",
			configure: func(t *testing.T, request *StartTurnRequest) (ToolService, AuthorizationService) {
				request.Runtime.Body.Tools = []protocol.ToolDescriptor{}
				refreshRuntimeDigest(t, &request.Runtime)
				return noToolService{}, &allowingAuthorization{log: &recordLog{}}
			},
		},
		{
			name: "resource drift", wantKind: protocol.EventActivityFailed, wantStatus: "failed",
			configure: func(_ *testing.T, _ *StartTurnRequest) (ToolService, AuthorizationService) {
				return driftingObservationToolService{observationToolService{log: &recordLog{}}}, &allowingAuthorization{log: &recordLog{}}
			},
		},
		{
			name: "execution error", wantKind: protocol.EventActivityUncertain, wantStatus: "uncertain",
			configure: func(_ *testing.T, _ *StartTurnRequest) (ToolService, AuthorizationService) {
				return failingObservationToolService{observationToolService{log: &recordLog{}}}, &allowingAuthorization{log: &recordLog{}}
			},
		},
		{
			name: "cancelled outcome", wantKind: protocol.EventActivityCancelled, wantStatus: "cancelled",
			configure: func(_ *testing.T, _ *StartTurnRequest) (ToolService, AuthorizationService) {
				return cancelledObservationToolService{observationToolService{log: &recordLog{}}}, &allowingAuthorization{log: &recordLog{}}
			},
		},
		{
			name: "authorization denial", wantKind: protocol.EventActivityDenied, wantStatus: "denied", wantErr: true,
			configure: func(_ *testing.T, _ *StartTurnRequest) (ToolService, AuthorizationService) {
				return observationToolService{log: &recordLog{}}, &toolDenyingAuthorization{allowingAuthorization: allowingAuthorization{log: &recordLog{}}}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := validStartTurnRequest()
			request.Runtime = validRuntimeManifest(t, "observation")
			tools, authorizer := test.configure(t, &request)
			repository := &recordingRepository{head: request.ExpectedHead}
			log := &recordLog{}
			service, err := NewService(Dependencies{
				Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repository,
				TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: log}, Providers: fakeProviderCatalog{log: log}, Provider: &toolThenFinalProvider{log: log},
				Tools: tools, Authorization: authorizer, Evidence: recordingEvidence{log: log}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
			})
			if err != nil {
				t.Fatal(err)
			}
			_, runErr := service.RunTurn(context.Background(), request)
			if (runErr != nil) != test.wantErr {
				t.Fatalf("RunTurn error=%v wantErr=%v", runErr, test.wantErr)
			}
			_, result, ok := terminalToolResult(repository.appendRequests(), test.wantKind, "call-a")
			if !ok || result.Status != test.wantStatus {
				t.Fatalf("terminal result=%+v present=%v batches=%v", result, ok, repository.batchKinds())
			}
		})
	}
}

func TestToolMessageExcludesProviderDenialAndSuccessfulMutationPreview(t *testing.T) {
	t.Run("provider denial", func(t *testing.T) {
		request := validStartTurnRequest()
		request.Runtime = validRuntimeManifest(t, "observation")
		repository := &recordingRepository{head: request.ExpectedHead}
		log := &recordLog{}
		service, err := NewService(Dependencies{
			Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repository,
			TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: log}, Providers: fakeProviderCatalog{log: log}, Provider: &toolThenFinalProvider{log: log},
			Tools: observationToolService{log: log}, Authorization: &denyingAuthorization{allowingAuthorization{log: log}}, Evidence: recordingEvidence{log: log}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := service.RunTurn(context.Background(), request); err == nil {
			t.Fatal("provider denial unexpectedly succeeded")
		}
		for _, appendRequest := range repository.appendRequests() {
			if appendHasKinds(appendRequest, protocol.EventActivityDenied, protocol.EventToolMessage) {
				t.Fatalf("provider denial persisted a provider-visible tool result: %+v", appendRequest)
			}
		}
	})

	t.Run("mutation preview", func(t *testing.T) {
		request := validStartTurnRequest()
		request.Runtime = validRuntimeManifest(t, "mutation")
		repository := &recordingRepository{head: request.ExpectedHead}
		log := &recordLog{}
		service, err := NewService(Dependencies{
			Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repository,
			TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: log}, Providers: fakeProviderCatalog{log: log}, Provider: &toolThenFinalProvider{log: log},
			Tools: mutationToolService{log: log}, Authorization: &allowingAuthorization{log: log}, Evidence: recordingEvidence{log: log}, Recovery: recordingRecovery{log: log}, Verification: verification.NewService(time.Now),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := service.RunTurn(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		previewID := protocol.ActivityID(stableID("activity", string(request.Command.CommandID), "preview", "call-a", "0"))
		mutationID := protocol.ActivityID(stableID("activity", string(request.Command.CommandID), "mutation", "call-a", "0"))
		previewToolMessages, mutationToolMessages := 0, 0
		for _, appendRequest := range repository.appendRequests() {
			for _, event := range appendRequest.Events {
				if event.Kind != protocol.EventToolMessage {
					continue
				}
				if event.ActivityID == previewID {
					previewToolMessages++
				}
				if event.ActivityID == mutationID {
					mutationToolMessages++
				}
			}
		}
		if previewToolMessages != 0 || mutationToolMessages != 1 {
			t.Fatalf("preview tool messages=%d mutation tool messages=%d batches=%v", previewToolMessages, mutationToolMessages, repository.batchKinds())
		}
	})
}

func TestLaterTurnRestartReconstructsToolResultFromJournal(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	repository := &recordingRepository{head: request.ExpectedHead}
	firstLog := &recordLog{}
	first, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repository, TurnLeases: &recordingTurnLeaseManager{},
		Context: contextplanner.NewPlanner(request.Runtime.Body.ToolCatalogRevision, nil), Providers: fakeProviderCatalog{log: firstLog}, Provider: &toolThenFinalProvider{log: firstLog},
		Tools: observationToolService{log: firstLog}, Authorization: &allowingAuthorization{log: firstLog}, Evidence: recordingEvidence{log: firstLog}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	later := request
	later.Command.CommandID = "command-b"
	later.Command.IdempotencyKey = "key-b"
	later.Command.RequestDigest = repeatedDigest("9")
	later.ExpectedHead = repository.head
	later.Prompt = "use the durable result"
	provider := &capturingProviderService{}
	second, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repository, TurnLeases: &recordingTurnLeaseManager{},
		Context: contextplanner.NewPlanner(request.Runtime.Body.ToolCatalogRevision, nil), Providers: fakeProviderCatalog{log: &recordLog{}}, Provider: provider,
		Tools: noToolService{}, Authorization: &allowingAuthorization{log: &recordLog{}}, Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.RunTurn(context.Background(), later); err != nil {
		t.Fatal(err)
	}
	provider.mu.Lock()
	requests := protocol.DeepCopy(provider.requests)
	provider.mu.Unlock()
	if len(requests) != 1 {
		t.Fatalf("later provider requests=%d", len(requests))
	}
	result, ok := findToolResult(requests[0], "call-a")
	if !ok || result.Status != "succeeded" {
		t.Fatalf("later request did not reconstruct durable result: %#v", requests[0].Messages)
	}
	assistantFound := false
	for _, message := range requests[0].Messages {
		for _, block := range message.Blocks {
			assistantFound = assistantFound || message.Role == "assistant" && block.ToolUse != nil && block.ToolUse.CallID == "call-a"
		}
	}
	if !assistantFound {
		t.Fatalf("later request did not reconstruct assistant tool call: %#v", requests[0].Messages)
	}
}

func TestToolResultAppendBarrierBlocksProviderContinuation(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	base := &recordingRepository{head: request.ExpectedHead}
	repository := &toolTerminalFailingRepository{recordingRepository: base}
	log := &recordLog{}
	provider := &toolThenFinalProvider{log: log}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repository, TurnLeases: &recordingTurnLeaseManager{},
		Context: fakeContextPlanner{log: log}, Providers: fakeProviderCatalog{log: log}, Provider: provider, Tools: observationToolService{log: log},
		Authorization: &allowingAuthorization{log: log}, Evidence: recordingEvidence{log: log}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err == nil {
		t.Fatal("terminal append failure unexpectedly succeeded")
	}
	provider.mu.Lock()
	prepareCount := len(provider.requests)
	provider.mu.Unlock()
	if prepareCount != 1 || provider.streams.Load() != 1 {
		t.Fatalf("provider crossed failed result barrier: prepares=%d streams=%d", prepareCount, provider.streams.Load())
	}
}

func terminalToolResult(requests []journal.AppendRequest, terminalKind, callID string) (journal.AppendRequest, protocol.ToolResultBlock, bool) {
	for _, request := range requests {
		if !appendHasKinds(request, terminalKind, protocol.EventToolMessage) {
			continue
		}
		var messages []protocol.ToolMessageV1
		for _, event := range request.Events {
			if event.Kind != protocol.EventToolMessage {
				continue
			}
			var message protocol.ToolMessageV1
			if json.Unmarshal(event.Payload, &message) == nil {
				messages = append(messages, message)
			}
		}
		if len(messages) == 1 && len(messages[0].Results) == 1 && messages[0].Results[0].CallID == callID {
			return request, messages[0].Results[0], true
		}
	}
	return journal.AppendRequest{}, protocol.ToolResultBlock{}, false
}

func TestRunTurnEventsValidateFoundationRegistry(t *testing.T) {
	log := &recordLog{}
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "mutation")
	repository := &recordingRepository{head: request.ExpectedHead}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{},
		Lane: NewOperationLane(), Repository: repository, TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: log},
		Providers: fakeProviderCatalog{log: log}, Provider: &toolThenFinalProvider{log: log}, Tools: mutationToolService{log: log},
		Authorization: &allowingAuthorization{log: log}, Evidence: recordingEvidence{log: log}, Recovery: recordingRecovery{log: log},
		Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	validateAppendRequests(t, repository.appendRequests())
}

func TestStructuredFileChangedEffectBindsCanonicalEditAndFailsClosed(t *testing.T) {
	descriptor := edittool.BuiltinDescriptor()
	classification := toolset.EditClassification()
	body := protocol.ActionPlanBody{
		CallID: "edit-call", Tool: descriptor.Body.Identity, SourceRevision: descriptor.Body.SourceRevision, DescriptorDigest: descriptor.DescriptorDigest,
		Action: "edit", Purpose: "mutate", Resources: []protocol.ResourceTarget{{Kind: "file", CanonicalID: "/workspace/target.txt", Digest: strings.Repeat("a", 64)}},
		ExecutionLocus: classification.ExecutionLoci[0], Effect: classification.Effect, Boundary: classification.Boundary, Reversibility: classification.Reversibility,
		VerificationCoverage: classification.VerificationCoverage, RequestedProfile: classification.RequestedProfile, EffectiveProfile: classification.EffectiveProfile, RuntimeGenerationID: "runtime-a",
	}
	planDigest, err := canonicaljson.Digest(body)
	if err != nil {
		t.Fatal(err)
	}
	plan := protocol.ActionPlan{Body: body, Digest: planDigest}
	change := protocol.FileChangedV1{
		CallID: "edit-call", Subject: protocol.SubjectRef{Kind: "file", ID: "/workspace/target.txt"},
		Before: repeatedDigest("a"), After: repeatedDigest("b"), EvidenceIDs: []protocol.EvidenceID{},
	}
	records := []protocol.EvidenceRecord{{Body: protocol.EvidenceRecordBody{ID: "evidence-b"}}, {Body: protocol.EvidenceRecordBody{ID: "evidence-a"}}}

	got, err := structuredFileChangedEffect(plan, protocol.ExecutionResult{FileChange: &change}, records)
	if err != nil || got == nil || !slices.Equal(got.EvidenceIDs, []protocol.EvidenceID{"evidence-a", "evidence-b"}) {
		t.Fatalf("effect=%+v err=%v", got, err)
	}
	if got.Subject != change.Subject || got.Before != change.Before || got.After != change.After {
		t.Fatalf("effect changed structured facts: %+v", got)
	}

	malformed := change
	malformed.After.Value = strings.ToUpper(malformed.After.Value)
	if _, err := structuredFileChangedEffect(plan, protocol.ExecutionResult{FileChange: &malformed}, nil); err == nil {
		t.Fatal("uppercase digest was accepted")
	}
	lookalike := plan
	lookalike.Body.Tool = protocol.ToolIdentity{Source: "mcp", Authority: "attacker", Name: "edit"}
	if _, err := structuredFileChangedEffect(lookalike, protocol.ExecutionResult{FileChange: &change}, nil); err == nil {
		t.Fatal("non-canonical edit synthesized a file change")
	}
	forgedDescriptor := plan
	forgedDescriptor.Body.SourceRevision = "forged-v1"
	forgedDescriptor.Body.DescriptorDigest = repeatedDigest("f")
	forgedDescriptor.Digest, _ = canonicaljson.Digest(forgedDescriptor.Body)
	if _, err := structuredFileChangedEffect(forgedDescriptor, protocol.ExecutionResult{FileChange: &change}, nil); err == nil {
		t.Fatal("forged edit descriptor synthesized a file change")
	}
	wrongPath := change
	wrongPath.Subject.ID = "/workspace/other.txt"
	if _, err := structuredFileChangedEffect(plan, protocol.ExecutionResult{FileChange: &wrongPath}, nil); err == nil {
		t.Fatal("unbound path was accepted")
	}
}

func TestFileChangedEffectSharesTheUncertainTerminalTransactionAndIdentity(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	repository := &recordingRepository{head: request.ExpectedHead}
	service := &Service{repository: repository, probe: NoopBarrierProbe()}
	state := newTurnState(request)
	state.activeActivityID, state.activeStarted, state.activeDispatched = "edit-activity", true, true
	change := protocol.FileChangedV1{
		CallID: "edit-call", Subject: protocol.SubjectRef{Kind: "file", ID: "/workspace/target.txt"},
		Before: repeatedDigest("a"), After: repeatedDigest("b"), EvidenceIDs: []protocol.EvidenceID{},
	}
	if err := service.appendActivityEvidence(context.Background(), request, &state, "edit-activity", "edit-terminal", "uncertain", nil, nil, &change); err != nil {
		t.Fatal(err)
	}
	requests := repository.appendRequests()
	if len(requests) != 1 || !appendHasKinds(requests[0], protocol.EventFileChanged, protocol.EventActivityUncertain) {
		t.Fatalf("terminal append=%+v", requests)
	}
	for _, event := range requests[0].Events {
		if event.SessionID != request.SessionID || event.TaskID != state.taskID || event.TurnID != state.turnID || event.ActivityID != "edit-activity" || event.RuntimeGenerationID != request.Runtime.ID {
			t.Fatalf("effect/terminal identity mismatch: %+v", event)
		}
	}
}

func TestRunTurnConcurrentDuplicateExecutesProviderExactlyOnce(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	repository := &recordingRepository{head: request.ExpectedHead}
	providerService := &countingProviderService{delegate: fakeProviderService{log: &recordLog{}}}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{},
		Lane: NewOperationLane(), Repository: repository, TurnLeases: &recordingTurnLeaseManager{},
		Context: fakeContextPlanner{log: &recordLog{}}, Providers: fakeProviderCatalog{log: &recordLog{}}, Provider: providerService,
		Tools: noToolService{}, Authorization: &allowingAuthorization{log: &recordLog{}}, Evidence: noEvidenceRecorder{},
		Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan RunResult, 2)
	errCh := make(chan error, 2)
	var callers sync.WaitGroup
	callers.Add(2)
	for range 2 {
		go func() {
			defer callers.Done()
			result, runErr := service.RunTurn(context.Background(), request)
			results <- result
			errCh <- runErr
		}()
	}
	callers.Wait()
	close(results)
	close(errCh)
	for runErr := range errCh {
		if runErr != nil {
			t.Fatal(runErr)
		}
	}
	for result := range results {
		if result.Status != "completed" || result.CommandResult.Status != "completed" {
			t.Fatalf("result=%+v", result)
		}
	}
	if got := providerService.streams.Load(); got != 1 {
		t.Fatalf("provider streams=%d want=1", got)
	}

	changed := request
	changed.Command.RequestDigest = repeatedDigest("9")
	if _, err := service.RunTurn(context.Background(), changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed digest error=%v", err)
	}
	if got := providerService.streams.Load(); got != 1 {
		t.Fatalf("provider streams after conflict=%d want=1", got)
	}
}

func validateAppendRequests(t *testing.T, requests []journal.AppendRequest) {
	t.Helper()
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	for _, appendRequest := range requests {
		for index, proposed := range appendRequest.Events {
			envelope := protocol.EventEnvelope{
				SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: proposed.PayloadVersion,
				JournalKind: appendRequest.Journal.Kind, JournalID: appendRequest.Journal.ID, EventID: proposed.EventID,
				SessionID: proposed.SessionID, Seq: appendRequest.ExpectedHead.CommitSeq + uint64(index), Time: proposed.Time, Kind: proposed.Kind,
				TaskID: proposed.TaskID, TurnID: proposed.TurnID, ActivityID: proposed.ActivityID, ParentActivityID: proposed.ParentActivityID,
				CausationEventID: proposed.CausationEventID, Actor: proposed.Actor, RuntimeGenerationID: proposed.RuntimeGenerationID,
				TransactionID: appendRequest.TransactionID, Payload: proposed.Payload,
			}
			raw, marshalErr := canonicaljson.Marshal(envelope)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			record, decodeErr := registry.Decode(raw)
			if decodeErr != nil {
				t.Fatalf("decode %s: %v", proposed.Kind, decodeErr)
			}
			if validateErr := registry.Validate(record); validateErr != nil {
				t.Fatalf("validate %s: %v", proposed.Kind, validateErr)
			}
		}
	}
}

func TestCommitPureCommandReturnsDurableDuplicateAndRejectsChangedDigest(t *testing.T) {
	request := validStartTurnRequest()
	repository := &recordingRepository{head: request.ExpectedHead}
	service, err := NewService(Dependencies{Lane: NewOperationLane(), Repository: repository})
	if err != nil {
		t.Fatal(err)
	}
	completion := PureCommandCompletion{
		Journal:      protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)},
		ExpectedHead: request.ExpectedHead, PayloadVersion: 1, Payload: json.RawMessage(`{"ok":true}`),
	}
	first, err := service.CommitPureCommand(context.Background(), request.Command, completion)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := service.CommitPureCommand(context.Background(), request.Command, completion)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(first.Payload, duplicate.Payload) || first.Cursor.SelectedSession == nil || duplicate.Cursor.SelectedSession == nil || *first.Cursor.SelectedSession != *duplicate.Cursor.SelectedSession {
		t.Fatalf("first=%+v duplicate=%+v", first, duplicate)
	}
	changed := request.Command
	changed.RequestDigest = repeatedDigest("9")
	if _, err := service.CommitPureCommand(context.Background(), changed, completion); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed digest error=%v", err)
	}
	if got := len(repository.batchKinds()); got != 1 {
		t.Fatalf("committed batches=%d want=1", got)
	}
}

func TestConcurrentSameCommandPureAndSessionChangesCommitOneLifecycle(t *testing.T) {
	t.Run("pure", func(t *testing.T) {
		request := validStartTurnRequest()
		repository := &recordingRepository{head: request.ExpectedHead}
		service, err := NewService(Dependencies{Lane: NewOperationLane(), Repository: repository, Admission: passthroughAdmission{}})
		if err != nil {
			t.Fatal(err)
		}
		completion := PureCommandCompletion{
			Journal:      protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)},
			ExpectedHead: request.ExpectedHead, PayloadVersion: 1, Payload: json.RawMessage(`{"ok":true}`),
		}
		runConcurrent(t, func() error {
			_, commitErr := service.CommitPureCommand(context.Background(), request.Command, completion)
			return commitErr
		})
		if got := len(repository.batchKinds()); got != 1 {
			t.Fatalf("committed batches=%d want=1", got)
		}
	})

	t.Run("pure_session_title", func(t *testing.T) {
		request := validStartTurnRequest()
		repository := &recordingRepository{head: request.ExpectedHead}
		service, err := NewService(Dependencies{Lane: NewOperationLane(), Repository: repository, Admission: passthroughAdmission{}})
		if err != nil {
			t.Fatal(err)
		}
		change := SessionChangeRequest{
			Command: request.Command, OperationID: "operation-a",
			Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)}, SessionID: request.SessionID,
			ExpectedHead: request.ExpectedHead, TransactionID: "transaction-session-change", RuntimeGenerationID: "generation-a",
			Event: proposedSessionEvent(protocol.EventSessionTitleChanged, request.SessionID, protocol.SessionTitleChangedV1{Title: "renamed"}),
		}
		runConcurrent(t, func() error {
			_, commitErr := service.CommitSessionChange(context.Background(), change)
			return commitErr
		})
		if got := len(repository.batchKinds()); got != 1 {
			t.Fatalf("committed batches=%d want=1", got)
		}
		changed := change
		changed.Command.RequestDigest = repeatedDigest("9")
		if _, err := service.CommitSessionChange(context.Background(), changed); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("changed digest error=%v", err)
		}
		if got := len(repository.batchKinds()); got != 1 {
			t.Fatalf("committed batches after conflict=%d want=1", got)
		}
	})
}

func runConcurrent(t *testing.T, call func() error) {
	t.Helper()
	errCh := make(chan error, 2)
	var callers sync.WaitGroup
	callers.Add(2)
	for range 2 {
		go func() {
			defer callers.Done()
			errCh <- call()
		}()
	}
	callers.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestCommitSessionChangeCommitsOnlyPureTitleMetadataAndIsIdempotent(t *testing.T) {
	log := &recordLog{}
	request := validStartTurnRequest()
	repository := &recordingRepository{head: request.ExpectedHead, log: log}
	service, err := NewService(Dependencies{Lane: &loggingLane{delegate: NewOperationLane(), log: log}, Repository: repository, Admission: passthroughAdmission{}})
	if err != nil {
		t.Fatal(err)
	}
	change := SessionChangeRequest{
		Command: request.Command, OperationID: "operation-a",
		Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)}, SessionID: request.SessionID,
		ExpectedHead: request.ExpectedHead, TransactionID: "transaction-session-change", RuntimeGenerationID: "generation-a",
		Event: proposedSessionEvent(protocol.EventSessionTitleChanged, request.SessionID, protocol.SessionTitleChangedV1{Title: "renamed"}),
	}
	first, err := service.CommitSessionChange(context.Background(), change)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := service.CommitSessionChange(context.Background(), change)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != "completed" || duplicate.Status != first.Status || len(repository.batchKinds()) != 1 {
		t.Fatalf("first=%+v duplicate=%+v batches=%v", first, duplicate, repository.batchKinds())
	}
	if got := log.snapshot(); !containsContiguous(got, []string{"append(command.accepted,session.title_changed,command.completed)"}) || slices.Contains(got, "lane.acquire(control)") {
		t.Fatalf("pure title ordering=%v", got)
	}

	bypass := change
	bypass.Command.CommandID = "command-b"
	bypass.Command.RequestDigest = repeatedDigest("8")
	bypass.Consequential = true
	if _, err := service.CommitSessionChange(context.Background(), bypass); err == nil {
		t.Fatal("pure title was misclassified as consequential")
	}
}

func TestRunControlCommitsAuthorizedLifecycleAndTerminalCommand(t *testing.T) {
	log := &recordLog{}
	runtime := validRuntimeManifest(t, "observation")
	ref := protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace-control-a"}
	head := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 1, TransactionID: "head-a"}
	repository := &recordingRepository{head: head, log: log}
	authorization := &allowingAuthorization{log: log}
	service, err := NewService(Dependencies{
		Lane: &loggingLane{delegate: NewOperationLane(), log: log}, Repository: repository, Authorization: authorization,
		Admission: passthroughAdmission{},
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := testActionPlan(tooling.PlanRequest{CallID: "reload-a", Alias: "reload", RuntimeGenerationID: runtime.ID}, "mutation", "not_reversible")
	actor := protocol.ActorRef{ID: "user-a", Kind: protocol.ActorUser}
	event := protocol.ProposedEvent{
		EventID: "event-runtime", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventRuntimeGenerationActivated,
		Actor: &actor, RuntimeGenerationID: runtime.ID, Payload: mustCanonical(protocol.RuntimeGenerationActivatedV1{Manifest: runtime}),
	}
	request := ControlRequest{
		Command:     CommandMetadata{CommandID: "control-command-a", IdempotencyKey: "control-key-a", RequestDigest: repeatedDigest("7"), Actor: actor},
		OperationID: "reload-operation-a", Kind: OperationReloadActivation, Journal: ref, ExpectedHead: head,
		TransactionID: "control-terminal-a", Runtime: runtime, Plan: plan, Event: event,
	}
	result, err := service.RunControl(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.CommandResult.Status != "completed" {
		t.Fatalf("result=%+v", result)
	}
	want := []string{
		"lane.acquire(reload_activation)", "append(command.accepted,control_operation.planned,authorization.requested)",
		"authorization.decide", "append(authorization.decided,control_operation.authorized)",
		"append(authorization.decision_consumed,control_operation.started)", "authorization.issue",
		"append(runtime_generation.activated,control_operation.completed,command.completed)", "lane.release",
	}
	if got := log.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("control order got=%v want=%v", got, want)
	}
	validateAppendRequests(t, repository.appendRequests())
}

func TestRunControlConcurrentDuplicateDispatchesExactlyOnce(t *testing.T) {
	runtime := validRuntimeManifest(t, "observation")
	ref := protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace-control-a"}
	head := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 1, TransactionID: "head-a"}
	repository := &recordingRepository{head: head}
	authorizationService := &allowingAuthorization{log: &recordLog{}}
	service, err := NewService(Dependencies{Lane: NewOperationLane(), Repository: repository, Authorization: authorizationService, Admission: passthroughAdmission{}})
	if err != nil {
		t.Fatal(err)
	}
	plan := testActionPlan(tooling.PlanRequest{CallID: "reload-a", Alias: "reload", RuntimeGenerationID: runtime.ID}, "mutation", "not_reversible")
	actor := protocol.ActorRef{ID: "user-a", Kind: protocol.ActorUser}
	request := ControlRequest{
		Command:     CommandMetadata{CommandID: "control-command-a", IdempotencyKey: "control-key-a", RequestDigest: repeatedDigest("7"), Actor: actor},
		OperationID: "reload-operation-a", Kind: OperationReloadActivation, Journal: ref, ExpectedHead: head,
		TransactionID: "control-terminal-a", Runtime: runtime, Plan: plan,
		Event: protocol.ProposedEvent{EventID: "event-runtime", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventRuntimeGenerationActivated, Actor: &actor, RuntimeGenerationID: runtime.ID, Payload: mustCanonical(protocol.RuntimeGenerationActivatedV1{Manifest: runtime})},
	}
	runConcurrent(t, func() error {
		_, runErr := service.RunControl(context.Background(), request)
		return runErr
	})
	if got := authorizationService.dispatches.Load(); got != 1 {
		t.Fatalf("control dispatches=%d want=1", got)
	}

	changed := request
	changed.Command.RequestDigest = repeatedDigest("8")
	if _, err := service.RunControl(context.Background(), changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed digest error=%v", err)
	}
	if got := authorizationService.dispatches.Load(); got != 1 {
		t.Fatalf("control dispatches after conflict=%d want=1", got)
	}
}

func proposedSessionEvent(kind string, sessionID protocol.SessionID, payload any) protocol.ProposedEvent {
	raw, _ := canonicaljson.Marshal(payload)
	actor := protocol.ActorRef{ID: "user-a", Kind: protocol.ActorUser}
	return protocol.ProposedEvent{
		EventID: "event-session-change", Time: time.Now().UTC(), PayloadVersion: 1, Kind: kind, SessionID: sessionID,
		Actor: &actor, RuntimeGenerationID: "generation-a", Payload: raw,
	}
}

func containsContiguous(values, target []string) bool {
	for start := 0; start+len(target) <= len(values); start++ {
		if slices.Equal(values[start:start+len(target)], target) {
			return true
		}
	}
	return false
}

func TestRunTurnCommitsCommandTaskContractAndTurnAcceptanceAtomically(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	repository := &recordingRepository{head: request.ExpectedHead}
	turnLeases := &recordingTurnLeaseManager{}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{},
		Lane: NewOperationLane(), Repository: repository, TurnLeases: turnLeases,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, _ = service.RunTurn(context.Background(), request)
	batches := repository.batchKinds()
	if len(batches) == 0 {
		t.Fatal("no journal batch was committed")
	}
	want := []string{
		protocol.EventCommandAccepted,
		protocol.EventTaskCreated,
		protocol.EventUserMessage,
		protocol.EventOutcomeContractDeclared,
		protocol.EventTurnAccepted,
	}
	if !slices.Equal(batches[0], want) {
		t.Fatalf("first batch=%v want=%v", batches[0], want)
	}
	if got := turnLeases.acquisitions.Load(); got != 1 {
		t.Fatalf("turn lease acquisitions=%d want=1", got)
	}
}

func TestRunTurnValidatesEveryIdentityBeforeApplicationLane(t *testing.T) {
	lane := &countingLane{}
	service, err := NewService(Dependencies{Lane: lane, Repository: inertRepository{}})
	if err != nil {
		t.Fatal(err)
	}

	request := validStartTurnRequest()
	request.Command.CommandID = ""
	if _, err := service.RunTurn(context.Background(), request); err == nil || !strings.Contains(err.Error(), "command") {
		t.Fatalf("invalid command error=%v", err)
	}
	if got := lane.acquisitions.Load(); got != 0 {
		t.Fatalf("lane acquisitions=%d, want 0", got)
	}

	request = validStartTurnRequest()
	request.ExpectedHead.JournalID = "another-session"
	if _, err := service.RunTurn(context.Background(), request); err == nil || !strings.Contains(err.Error(), "cursor") {
		t.Fatalf("invalid cursor error=%v", err)
	}
	if got := lane.acquisitions.Load(); got != 0 {
		t.Fatalf("lane acquisitions=%d, want 0", got)
	}
}

type countingLane struct{ acquisitions atomic.Int64 }

func (l *countingLane) Acquire(context.Context, OperationClaim) (OperationLease, error) {
	l.acquisitions.Add(1)
	return inertOperationLease{}, nil
}

type inertOperationLease struct{}

func (inertOperationLease) Claim() OperationClaim {
	return OperationClaim{Kind: OperationTurn, SessionID: "session-a"}
}
func (inertOperationLease) Release() {}

type inertRepository struct{}

func (inertRepository) Inspect(context.Context, protocol.JournalRef) (journal.Inspection, error) {
	return journal.Inspection{}, nil
}
func (inertRepository) Head(context.Context, protocol.JournalRef) (protocol.CommittedCursor, error) {
	return protocol.CommittedCursor{}, nil
}
func (inertRepository) ReadRange(context.Context, journal.ReadRangeRequest) (journal.EventPage, error) {
	return journal.EventPage{}, nil
}
func (inertRepository) AppendBatch(context.Context, journal.AppendRequest) (journal.AppendResult, error) {
	return journal.AppendResult{}, nil
}
func (inertRepository) LookupTransaction(context.Context, protocol.JournalRef, protocol.TransactionID) (journal.TransactionLookup, error) {
	return journal.TransactionLookup{}, nil
}
func (inertRepository) ReadCommittedTransaction(context.Context, protocol.JournalRef, protocol.TransactionID) (journal.CommittedTransaction, error) {
	return journal.CommittedTransaction{}, nil
}
func (inertRepository) Recover(context.Context, journal.RecoveryRequest) (journal.RecoveryResult, error) {
	return journal.RecoveryResult{}, nil
}

func validStartTurnRequest() StartTurnRequest {
	sessionID := protocol.SessionID("session-a")
	return StartTurnRequest{
		Command: CommandMetadata{
			CommandID:      "command-a",
			IdempotencyKey: "key-a",
			RequestDigest:  repeatedDigest("a"),
			Actor:          protocol.ActorRef{ID: "user-a", Kind: protocol.ActorUser},
		},
		SessionID: sessionID,
		ExpectedHead: protocol.CommittedCursor{
			JournalKind:   protocol.JournalSession,
			JournalID:     protocol.JournalID(sessionID),
			CommitSeq:     1,
			TransactionID: "transaction-head",
		},
		Prompt:     "inspect",
		ProviderID: "provider-a",
		ModelID:    "model-a",
		Runtime: protocol.RuntimeGenerationManifest{
			ID:     "generation-a",
			Digest: repeatedDigest("b"),
		},
	}
}

func repeatedDigest(fill string) protocol.Digest {
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat(fill, 64)}
}

type recordingRepository struct {
	mu       sync.Mutex
	head     protocol.CommittedCursor
	batches  [][]string
	log      *recordLog
	trace    func(journal.AppendRequest)
	events   []protocol.ProposedEvent
	requests []journal.AppendRequest
}

type toolTerminalFailingRepository struct{ *recordingRepository }

func (r *toolTerminalFailingRepository) AppendBatch(ctx context.Context, request journal.AppendRequest) (journal.AppendResult, error) {
	if appendHasKinds(request, protocol.EventToolMessage) {
		return journal.AppendResult{}, errors.New("tool terminal append failed")
	}
	return r.recordingRepository.AppendBatch(ctx, request)
}

func (r *recordingRepository) Inspect(context.Context, protocol.JournalRef) (journal.Inspection, error) {
	return journal.Inspection{Head: r.head, Writable: true}, nil
}
func (r *recordingRepository) Head(context.Context, protocol.JournalRef) (protocol.CommittedCursor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.head, nil
}
func (r *recordingRepository) ReadRange(context.Context, journal.ReadRangeRequest) (journal.EventPage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	records := make([]protocol.EventRecord, len(r.events))
	for index, event := range r.events {
		records[index] = protocol.EventRecord{Envelope: protocol.EventEnvelope{
			JournalKind: r.head.JournalKind, JournalID: r.head.JournalID, SessionID: event.SessionID,
			EventID: event.EventID, Time: event.Time, Kind: event.Kind, PayloadVersion: event.PayloadVersion, Payload: protocol.DeepCopy(event.Payload),
			TaskID: event.TaskID, TurnID: event.TurnID, ActivityID: event.ActivityID, RuntimeGenerationID: event.RuntimeGenerationID,
		}}
	}
	return journal.EventPage{Events: records, Head: r.head, Cursor: r.head}, nil
}
func (r *recordingRepository) AppendBatch(_ context.Context, request journal.AppendRequest) (journal.AppendResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if request.ExpectedHead != r.head {
		return journal.AppendResult{Status: journal.AppendConflict, CurrentHead: r.head}, nil
	}
	kinds := make([]string, len(request.Events))
	for index, event := range request.Events {
		kinds[index] = event.Kind
	}
	r.batches = append(r.batches, kinds)
	r.events = append(r.events, protocol.DeepCopy(request.Events)...)
	r.requests = append(r.requests, protocol.DeepCopy(request))
	if r.log != nil {
		r.log.add("append(" + strings.Join(kinds, ",") + ")")
	}
	r.head = protocol.CommittedCursor{
		JournalKind: request.Journal.Kind, JournalID: request.Journal.ID,
		CommitSeq: r.head.CommitSeq + uint64(len(request.Events)) + 1, TransactionID: request.TransactionID,
	}
	if r.trace != nil {
		r.trace(protocol.DeepCopy(request))
	}
	return journal.AppendResult{Status: journal.AppendCommitted, Cursor: r.head}, nil
}
func (r *recordingRepository) LookupTransaction(context.Context, protocol.JournalRef, protocol.TransactionID) (journal.TransactionLookup, error) {
	return journal.TransactionLookup{State: journal.TransactionNotCommitted}, nil
}
func (r *recordingRepository) ReadCommittedTransaction(context.Context, protocol.JournalRef, protocol.TransactionID) (journal.CommittedTransaction, error) {
	return journal.CommittedTransaction{}, nil
}
func (r *recordingRepository) Recover(context.Context, journal.RecoveryRequest) (journal.RecoveryResult, error) {
	return journal.RecoveryResult{}, nil
}
func (r *recordingRepository) batchKinds() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([][]string, len(r.batches))
	for index := range r.batches {
		result[index] = append([]string(nil), r.batches[index]...)
	}
	return result
}
func (r *recordingRepository) appendRequests() []journal.AppendRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return protocol.DeepCopy(r.requests)
}

type recordingTurnLeaseManager struct {
	acquisitions atomic.Int64
	log          *recordLog
}

func (m *recordingTurnLeaseManager) AcquireTurnLease(_ context.Context, sessionID protocol.SessionID, turnID protocol.TurnID, _ protocol.CommittedCursor) (journal.TurnLease, error) {
	m.acquisitions.Add(1)
	if m.log != nil {
		m.log.add("turn_lease.acquire")
	}
	return recordingTurnLease{sessionID: sessionID, turnID: turnID, log: m.log}, nil
}
func (m *recordingTurnLeaseManager) AcquireTurnRecoveryLease(_ context.Context, sessionID protocol.SessionID, turnID protocol.TurnID, _ protocol.CommittedCursor) (journal.TurnLease, error) {
	m.acquisitions.Add(1)
	if m.log != nil {
		m.log.add("turn_recovery_lease.acquire")
	}
	return recordingTurnLease{sessionID: sessionID, turnID: turnID, log: m.log}, nil
}

type recordingTurnLease struct {
	sessionID protocol.SessionID
	turnID    protocol.TurnID
	log       *recordLog
}

func (l recordingTurnLease) SessionID() protocol.SessionID { return l.sessionID }
func (l recordingTurnLease) TurnID() protocol.TurnID       { return l.turnID }

func (l recordingTurnLease) Release(context.Context, protocol.CommittedCursor) error {
	if l.log != nil {
		l.log.add("turn_lease.release")
	}
	return nil
}

type recordLog struct {
	mu     sync.Mutex
	values []string
}

func (l *recordLog) add(value string) {
	l.mu.Lock()
	l.values = append(l.values, value)
	l.mu.Unlock()
}
func (l *recordLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.values...)
}

type loggingLane struct {
	delegate OperationLane
	log      *recordLog
	name     string
}

func (l *loggingLane) Acquire(ctx context.Context, claim OperationClaim) (OperationLease, error) {
	prefix := ""
	if l.name != "" {
		prefix = l.name + "."
	}
	l.log.add(prefix + "lane.acquire(" + string(claim.Kind) + ")")
	lease, err := l.delegate.Acquire(ctx, claim)
	if err != nil {
		return nil, err
	}
	return loggingOperationLease{OperationLease: lease, log: l.log, prefix: prefix}, nil
}

type loggingOperationLease struct {
	OperationLease
	log    *recordLog
	prefix string
}

func (l loggingOperationLease) Release() {
	l.OperationLease.Release()
	l.log.add(l.prefix + "lane.release")
}

type fakeContextPlanner struct{ log *recordLog }

func (p fakeContextPlanner) Plan(ctx context.Context, request contextplanner.Request) (protocol.ContextPlan, error) {
	p.log.add("context.plan")
	return contextplanner.NewPlanner("tools-a", nil).Plan(ctx, request)
}

type staticContextPlanner struct{ plan protocol.ContextPlan }

func (p staticContextPlanner) Plan(context.Context, contextplanner.Request) (protocol.ContextPlan, error) {
	return protocol.DeepCopy(p.plan), nil
}

func contextPlanForMessages(t *testing.T, sources []protocol.ContentSource) protocol.ContextPlan {
	t.Helper()
	for index := range sources {
		digest, err := canonicaljson.Digest(sources[index].Content)
		if err != nil {
			t.Fatal(err)
		}
		sources[index].Digest = digest
	}
	body := protocol.ContextPlanBody{
		Sources: sources, Excluded: []protocol.ExcludedContentSource{}, EstimatedInputTokens: protocol.ValueInt64{State: protocol.ValueUnknown},
		ContextWindow: protocol.ValueInt64{State: protocol.ValueUnknown}, CompactionRevision: "none", ToolExposureRevision: "tools-a",
	}
	digest, err := canonicaljson.Digest(body)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.ContextPlan{Body: body, Digest: digest}
}

type fakeProviderCatalog struct{ log *recordLog }

func (c fakeProviderCatalog) Resolve(protocol.ProviderID, protocol.ModelID) (protocol.ModelDescriptor, bool) {
	return protocol.ModelDescriptor{}, false
}
func (c fakeProviderCatalog) Negotiate(_ protocol.ProviderID, _ protocol.ModelID, _ []protocol.CapabilityRequirement, _ string) (protocol.NegotiatedProviderPlan, error) {
	c.log.add("provider.negotiate")
	manifest := validModelDescriptor("generation-a")
	body := protocol.NegotiatedProviderPlanBody{Descriptor: manifest, Requirements: []protocol.CapabilityRequirement{}, ToolExposureRevision: "tools-a", Warnings: []string{}}
	digest, _ := canonicaljson.Digest(body)
	return protocol.NegotiatedProviderPlan{Body: body, Digest: digest}, nil
}

type fakeProviderService struct{ log *recordLog }

type capturingProviderService struct {
	mu       sync.Mutex
	requests []protocol.ModelRequest
}

func (p *capturingProviderService) Prepare(_ context.Context, _ protocol.ActivityID, _ string, request protocol.ModelRequest, _ protocol.Digest) (provider.ProviderHandle, error) {
	p.mu.Lock()
	p.requests = append(p.requests, protocol.DeepCopy(request))
	p.mu.Unlock()
	return provider.ProviderHandle{}, nil
}

func (p *capturingProviderService) Stream(context.Context, provider.ProviderHandle, authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	stream := make(chan protocol.ModelEvent, 2)
	stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: "done"}}
	stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	close(stream)
	return stream, nil
}

type countingProviderService struct {
	delegate fakeProviderService
	streams  atomic.Int64
}

func (s *countingProviderService) Prepare(ctx context.Context, activityID protocol.ActivityID, callID string, request protocol.ModelRequest, digest protocol.Digest) (provider.ProviderHandle, error) {
	return s.delegate.Prepare(ctx, activityID, callID, request, digest)
}

func (s *countingProviderService) Stream(ctx context.Context, handle provider.ProviderHandle, token authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	s.streams.Add(1)
	return s.delegate.Stream(ctx, handle, token)
}

func (s fakeProviderService) Prepare(context.Context, protocol.ActivityID, string, protocol.ModelRequest, protocol.Digest) (provider.ProviderHandle, error) {
	s.log.add("provider.prepare")
	return provider.ProviderHandle{}, nil
}

type toolThenFinalProvider struct {
	log      *recordLog
	streams  atomic.Int64
	mu       sync.Mutex
	requests []protocol.ModelRequest
}

type twoToolThenFinalProvider struct {
	log     *recordLog
	streams atomic.Int64
}

type threeRoundToolProvider struct {
	log      *recordLog
	streams  atomic.Int64
	mu       sync.Mutex
	requests []protocol.ModelRequest
}

func (p *threeRoundToolProvider) Prepare(_ context.Context, _ protocol.ActivityID, _ string, request protocol.ModelRequest, _ protocol.Digest) (provider.ProviderHandle, error) {
	p.log.add("provider.prepare")
	p.mu.Lock()
	p.requests = append(p.requests, protocol.DeepCopy(request))
	p.mu.Unlock()
	return provider.ProviderHandle{}, nil
}

func (p *threeRoundToolProvider) Stream(context.Context, provider.ProviderHandle, authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	p.log.add("provider.stream")
	round := int(p.streams.Add(1))
	stream := make(chan protocol.ModelEvent, 2)
	if round <= 3 {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventToolIntent, Sequence: 1, ToolIntent: &protocol.ToolUseBlock{CallID: fmt.Sprintf("round-call-%d", round), Alias: "read", Arguments: json.RawMessage(`{"path":"a.go"}`)}}
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "tool_use"}}
	} else {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: "done"}}
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	}
	close(stream)
	return stream, nil
}

func (s *twoToolThenFinalProvider) Prepare(context.Context, protocol.ActivityID, string, protocol.ModelRequest, protocol.Digest) (provider.ProviderHandle, error) {
	s.log.add("provider.prepare")
	return provider.ProviderHandle{}, nil
}
func (s *twoToolThenFinalProvider) Stream(context.Context, provider.ProviderHandle, authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	s.log.add("provider.stream")
	stream := make(chan protocol.ModelEvent, 3)
	if s.streams.Add(1) == 1 {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventToolIntent, Sequence: 1, ToolIntent: &protocol.ToolUseBlock{CallID: "call-b", Alias: "read", Arguments: json.RawMessage(`{"path":"b.go"}`)}}
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventToolIntent, Sequence: 2, ToolIntent: &protocol.ToolUseBlock{CallID: "call-a", Alias: "read", Arguments: json.RawMessage(`{"path":"a.go"}`)}}
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 3, Terminal: &protocol.ModelTerminal{Reason: "tool_use"}}
	} else {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: "done"}}
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	}
	close(stream)
	return stream, nil
}

func (s *toolThenFinalProvider) Prepare(_ context.Context, _ protocol.ActivityID, _ string, request protocol.ModelRequest, _ protocol.Digest) (provider.ProviderHandle, error) {
	s.log.add("provider.prepare")
	s.mu.Lock()
	s.requests = append(s.requests, protocol.DeepCopy(request))
	s.mu.Unlock()
	return provider.ProviderHandle{}, nil
}
func (s *toolThenFinalProvider) Stream(context.Context, provider.ProviderHandle, authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	s.log.add("provider.stream")
	stream := make(chan protocol.ModelEvent, 2)
	if s.streams.Add(1) == 1 {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventToolIntent, Sequence: 1, ToolIntent: &protocol.ToolUseBlock{CallID: "call-a", Alias: "read", Arguments: json.RawMessage(`{"path":"a.go"}`)}}
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "tool_use"}}
	} else {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: "done"}}
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	}
	close(stream)
	return stream, nil
}
func (s fakeProviderService) Stream(context.Context, provider.ProviderHandle, authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	s.log.add("provider.stream")
	stream := make(chan protocol.ModelEvent, 2)
	stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: "done"}}
	stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	close(stream)
	return stream, nil
}

type allowingAuthorization struct {
	log        *recordLog
	nonce      atomic.Int64
	dispatches atomic.Int64
}

type toolDenyingAuthorization struct{ allowingAuthorization }

func (a *toolDenyingAuthorization) Decide(ctx context.Context, request protocol.AuthorizationRequest) (protocol.AuthorizationDecision, error) {
	decision, err := a.allowingAuthorization.Decide(ctx, request)
	if request.Source.Source != "provider" {
		decision.Action = "deny"
		decision.Reason = "policy denied tool"
	}
	return decision, err
}

func (a *allowingAuthorization) Decide(_ context.Context, request protocol.AuthorizationRequest) (protocol.AuthorizationDecision, error) {
	a.log.add("authorization.decide")
	return protocol.AuthorizationDecision{
		Request: request, Action: "allow", Scope: protocol.CanonicalAuthorizationScope{Capability: request.Action, Source: request.Source, Resources: request.Resources, Constraints: []protocol.AuthorizationConstraint{}},
		Constraints: []protocol.AuthorizationConstraint{},
		Lifetime:    protocol.AuthorizationLifetimeOnce, PolicySource: "test", PolicyGeneration: request.PolicyGeneration,
		Reason: "allowed", DecidedAt: time.Now().UTC(), PlanDigest: request.PlanDigest,
		DecisionNonce: protocol.DecisionNonce(fmt.Sprintf("nonce-%d", a.nonce.Add(1))),
	}, nil
}
func (a *allowingAuthorization) ResolveInteractive(context.Context, protocol.AuthorizationRequest, protocol.AuthorizationDecision, protocol.ApprovalResponse) (protocol.AuthorizationDecision, error) {
	return protocol.AuthorizationDecision{}, fmt.Errorf("unexpected interactive decision")
}
func (a *allowingAuthorization) Issue(context.Context, authorization.CommitReference) (authorization.CommittedToken, error) {
	a.log.add("authorization.issue")
	return authorization.CommittedToken{}, nil
}
func (a *allowingAuthorization) Dispatch(_ context.Context, _ authorization.CommittedToken, _ authorization.DispatchBinding, callback func(context.Context) error) error {
	a.dispatches.Add(1)
	return callback(context.Background())
}

type noToolService struct{}

func (noToolService) Plan(context.Context, tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error) {
	return tooling.ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("unexpected tool plan")
}

type mutationToolService struct{ log *recordLog }

func (s mutationToolService) Plan(context.Context, tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error) {
	return tooling.ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("unexpected direct plan")
}

type observationToolService struct{ log *recordLog }

type evidencedObservationToolService struct{ observationToolService }

func (s evidencedObservationToolService) Execute(_ context.Context, _ tooling.ActionHandle, _ authorization.CommittedToken) (protocol.ExecutionResult, error) {
	s.log.add("tool.execute(call-a)")
	return protocol.ExecutionResult{
		Outcome:    protocol.ActivityOutcomeV1{Status: "succeeded"},
		ToolResult: protocol.ToolResultBlock{CallID: "call-a", Status: "succeeded", Text: "ok"},
		Evidence: []protocol.EvidenceCandidate{
			{ID: "evidence-z", Kind: "tool_output", MediaType: "text/plain", Actor: protocol.ActorRef{ID: "read", Kind: protocol.ActorTool}, Subject: protocol.SubjectRef{Kind: "file", ID: "z.go"}, Content: []byte("z"), Limit: 1024},
			{ID: "evidence-a", Kind: "tool_output", MediaType: "text/plain", Actor: protocol.ActorRef{ID: "read", Kind: protocol.ActorTool}, Subject: protocol.SubjectRef{Kind: "file", ID: "a.go"}, Content: []byte("a"), Limit: 1024},
		},
	}, nil
}

type driftingObservationToolService struct{ observationToolService }

func (driftingObservationToolService) Revalidate(context.Context, tooling.ActionHandle) (protocol.ActionPlan, bool, error) {
	return protocol.ActionPlan{}, true, nil
}

type failingObservationToolService struct{ observationToolService }

func (failingObservationToolService) Execute(context.Context, tooling.ActionHandle, authorization.CommittedToken) (protocol.ExecutionResult, error) {
	return protocol.ExecutionResult{}, errors.New("execution failed with sensitive detail")
}

type cancelledObservationToolService struct{ observationToolService }

func (cancelledObservationToolService) Execute(context.Context, tooling.ActionHandle, authorization.CommittedToken) (protocol.ExecutionResult, error) {
	return protocol.ExecutionResult{Outcome: protocol.ActivityOutcomeV1{Status: "cancelled"}, ToolResult: protocol.ToolResultBlock{CallID: "call-a", Status: "cancelled", Text: "cancelled"}}, nil
}

func (s observationToolService) Plan(_ context.Context, request tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error) {
	s.log.add("tool.plan(" + request.CallID + ")")
	return tooling.ActionHandle{}, testActionPlan(request, "observation", "not_applicable"), nil
}
func (s observationToolService) PlanPreviewInspection(context.Context, tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error) {
	return tooling.ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("unexpected preview")
}
func (s observationToolService) PreparePreview(context.Context, tooling.ActionHandle, authorization.CommittedToken) (tooling.PreviewResult, protocol.ActionPlan, []protocol.EvidenceCandidate, error) {
	return tooling.PreviewResult{}, protocol.ActionPlan{}, nil, fmt.Errorf("unexpected preview")
}
func (s observationToolService) PlanMutation(context.Context, tooling.PreviewResult, tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error) {
	return tooling.ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("unexpected mutation")
}
func (s observationToolService) Revalidate(context.Context, tooling.ActionHandle) (protocol.ActionPlan, bool, error) {
	return protocol.ActionPlan{}, false, nil
}
func (s observationToolService) Execute(_ context.Context, _ tooling.ActionHandle, _ authorization.CommittedToken) (protocol.ExecutionResult, error) {
	// The plan/execute calls are strictly synchronous, so the most recent plan
	// entry identifies this opaque fake handle's call for the FIFO assertion.
	values := s.log.snapshot()
	callID := ""
	for index := len(values) - 1; index >= 0; index-- {
		if strings.HasPrefix(values[index], "tool.plan(") {
			callID = strings.TrimSuffix(strings.TrimPrefix(values[index], "tool.plan("), ")")
			break
		}
	}
	s.log.add("tool.execute(" + callID + ")")
	return protocol.ExecutionResult{Outcome: protocol.ActivityOutcomeV1{Status: "succeeded"}, ToolResult: protocol.ToolResultBlock{CallID: callID, Status: "succeeded", Text: "ok"}}, nil
}
func (s mutationToolService) PlanPreviewInspection(_ context.Context, request tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error) {
	s.log.add("tool.plan_preview")
	return tooling.ActionHandle{}, testActionPlan(request, "observation", "not_applicable"), nil
}
func (s mutationToolService) PreparePreview(_ context.Context, _ tooling.ActionHandle, _ authorization.CommittedToken) (tooling.PreviewResult, protocol.ActionPlan, []protocol.EvidenceCandidate, error) {
	s.log.add("tool.prepare_preview")
	return tooling.PreviewResult{}, protocol.ActionPlan{}, []protocol.EvidenceCandidate{{
		ID: "evidence-preview", Kind: "tool_preview", MediaType: "text/plain", ProducingActivityID: "placeholder",
		Actor: protocol.ActorRef{ID: "read", Kind: protocol.ActorTool}, Subject: protocol.SubjectRef{Kind: "file", ID: "a.go"}, Content: []byte("preview"), Limit: 1024,
	}}, nil
}
func (s mutationToolService) PlanMutation(_ context.Context, _ tooling.PreviewResult, request tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error) {
	s.log.add("tool.plan_mutation")
	return tooling.ActionHandle{}, testActionPlan(request, "mutation", "exact"), nil
}
func (s mutationToolService) Revalidate(_ context.Context, _ tooling.ActionHandle) (protocol.ActionPlan, bool, error) {
	s.log.add("tool.revalidate")
	return protocol.ActionPlan{}, false, nil
}
func (s mutationToolService) Execute(_ context.Context, _ tooling.ActionHandle, _ authorization.CommittedToken) (protocol.ExecutionResult, error) {
	s.log.add("tool.execute")
	return protocol.ExecutionResult{
		Outcome:    protocol.ActivityOutcomeV1{Status: "succeeded"},
		ToolResult: protocol.ToolResultBlock{CallID: "call-a", Status: "succeeded", Text: "edited"},
		Evidence:   []protocol.EvidenceCandidate{{ID: "evidence-result", Kind: "tool_output", MediaType: "text/plain", ProducingActivityID: "placeholder", Actor: protocol.ActorRef{ID: "read", Kind: protocol.ActorTool}, Subject: protocol.SubjectRef{Kind: "file", ID: "a.go"}, Content: []byte("edited"), Limit: 1024}},
	}, nil
}

func testActionPlan(request tooling.PlanRequest, effect, reversibility string) protocol.ActionPlan {
	body := protocol.ActionPlanBody{
		CallID: request.CallID, Tool: protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: request.Alias},
		SourceRevision: "tools-a", DescriptorDigest: repeatedDigest("c"), Action: request.Alias, Purpose: "inspect",
		Resources: []protocol.ResourceTarget{{Kind: "file", CanonicalID: "a.go"}}, ExecutionLocus: "local", Effect: effect,
		Boundary: "workspace", Reversibility: reversibility, VerificationCoverage: "exact", RequestedProfile: "restricted",
		EffectiveProfile: "restricted", RuntimeGenerationID: request.RuntimeGenerationID,
	}
	if effect != "observation" {
		body.Purpose = "mutate"
	}
	digest, _ := canonicaljson.Digest(body)
	return protocol.ActionPlan{Body: body, Digest: digest}
}
func (noToolService) PlanPreviewInspection(context.Context, tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error) {
	return tooling.ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("unexpected preview plan")
}
func (noToolService) PreparePreview(context.Context, tooling.ActionHandle, authorization.CommittedToken) (tooling.PreviewResult, protocol.ActionPlan, []protocol.EvidenceCandidate, error) {
	return tooling.PreviewResult{}, protocol.ActionPlan{}, nil, fmt.Errorf("unexpected preview")
}
func (noToolService) PlanMutation(context.Context, tooling.PreviewResult, tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error) {
	return tooling.ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("unexpected mutation plan")
}
func (noToolService) Revalidate(context.Context, tooling.ActionHandle) (protocol.ActionPlan, bool, error) {
	return protocol.ActionPlan{}, false, fmt.Errorf("unexpected revalidation")
}
func (noToolService) Execute(context.Context, tooling.ActionHandle, authorization.CommittedToken) (protocol.ExecutionResult, error) {
	return protocol.ExecutionResult{}, fmt.Errorf("unexpected execution")
}

type noEvidenceRecorder struct{}

func (noEvidenceRecorder) Put(context.Context, protocol.EvidenceCandidate) (protocol.EvidenceRecord, error) {
	return protocol.EvidenceRecord{}, fmt.Errorf("unexpected evidence")
}

type recordingEvidence struct{ log *recordLog }

func (r recordingEvidence) Put(_ context.Context, candidate protocol.EvidenceCandidate) (protocol.EvidenceRecord, error) {
	r.log.add("evidence.put")
	body := protocol.EvidenceRecordBody{
		ID: candidate.ID, Kind: candidate.Kind, WorkspaceID: candidate.WorkspaceID, SessionID: candidate.SessionID,
		Availability: protocol.ContentWithheldSecret, MediaType: candidate.MediaType, Size: int64(len(candidate.Content)),
		ProducingActivityID: candidate.ProducingActivityID, Actor: candidate.Actor, Subject: candidate.Subject,
		CreatedAt: time.Now().UTC(), Redacted: true,
	}
	digest, _ := canonicaljson.Digest(body)
	return protocol.EvidenceRecord{Body: body, Digest: digest}, nil
}

type noRecoveryRecorder struct{}

func (noRecoveryRecorder) PrepareAndPut(context.Context, tooling.PreviewResult, protocol.ActivityID, protocol.CheckpointBody, protocol.ActionPlan) (protocol.RecoveryMaterialRecord, error) {
	return protocol.RecoveryMaterialRecord{}, fmt.Errorf("unexpected recovery material")
}

type recordingRecovery struct{ log *recordLog }

func (r recordingRecovery) PrepareAndPut(_ context.Context, _ tooling.PreviewResult, activityID protocol.ActivityID, checkpoint protocol.CheckpointBody, plan protocol.ActionPlan) (protocol.RecoveryMaterialRecord, error) {
	r.log.add("tool.recovery_candidate")
	r.log.add("recovery.put")
	return protocol.RecoveryMaterialRecord{
		ID: "recovery-a", WorkspaceID: protocol.WorkspaceID(checkpoint.SessionID), ActivityID: activityID,
		CheckpointID: checkpoint.ID, Subject: checkpoint.Coverage[0].Subject,
		Body:           protocol.RecoveryMaterialBody{PlanDigest: plan.Digest, PreimageDigest: repeatedDigest("d"), ExpectedPostimageDigest: repeatedDigest("e"), Mode: 0o644, CreatedAt: time.Now().UTC()},
		MaterialDigest: repeatedDigest("f"),
	}, nil
}

type loggingVerification struct {
	log      *recordLog
	delegate *verification.Service
}

func (v loggingVerification) Assess(ctx context.Context, request verification.Request) (verification.Result, error) {
	v.log.add("verification.assess")
	return v.delegate.Assess(ctx, request)
}

func validRuntimeManifest(t *testing.T, effect string) protocol.RuntimeGenerationManifest {
	t.Helper()
	descriptor := protocol.ToolDescriptor{Body: protocol.ToolDescriptorBody{
		Identity: protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "read"}, SourceRevision: "tools-a",
		DisplayName: "Read", Description: "read", InputSchema: json.RawMessage(`{"type":"object"}`),
		Effect: effect, Mutation: "none", ExecutionLoci: []string{"local"}, ClassificationSource: "builtin", Idempotency: "safe", Retry: "safe",
	}}
	descriptor.DescriptorDigest, _ = canonicaljson.Digest(descriptor.Body)
	body := protocol.RuntimeGenerationBody{
		ProviderCatalogRevision: "providers-a", Models: []protocol.ModelDescriptor{validModelDescriptor("generation-a")},
		ToolCatalogRevision: "tools-a", Tools: []protocol.ToolDescriptor{descriptor}, InstructionRevision: "instructions-a",
		PolicyGeneration: "policy-a", ExecutionProfiles: []string{"restricted"},
		Limits: protocol.RuntimeLimits{MaxToolCalls: 4, ShellTimeoutNanos: 1, ApplicationQueueCapacity: 8},
	}
	digest, err := canonicaljson.Digest(body)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.RuntimeGenerationManifest{ID: "generation-a", Body: body, Digest: digest}
}

func validModelDescriptor(generation protocol.RuntimeGenerationID) protocol.ModelDescriptor {
	return protocol.ModelDescriptor{
		ProviderID: "provider-a", ModelID: "model-a", AdapterKind: "test", DisplayName: "Test",
		ContextWindow: protocol.ValueInt64{State: protocol.ValueUnknown}, MaximumOutput: protocol.ValueInt64{State: protocol.ValueUnknown},
		Capabilities: []protocol.CapabilityFact{}, UsageCategories: []string{}, Pricing: []protocol.PricingFact{},
		CredentialBindingRef: "credential-a", SourceRevision: "providers-a", RuntimeGenerationID: generation,
	}
}

func refreshRuntimeDigest(t *testing.T, manifest *protocol.RuntimeGenerationManifest) {
	t.Helper()
	digest, err := canonicaljson.Digest(manifest.Body)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Digest = digest
}

func countPrefix(values []string, prefix string) int {
	count := 0
	for _, value := range values {
		if strings.HasPrefix(value, prefix) {
			count++
		}
	}
	return count
}

type automaticCompactionPlanner struct {
	estimated protocol.ValueInt64
	window    protocol.ValueInt64
	estimates []protocol.ValueInt64
	calls     atomic.Int64
	mu        sync.Mutex
	plans     []protocol.ContextPlan
}

func (p *automaticCompactionPlanner) Plan(_ context.Context, _ contextplanner.Request) (protocol.ContextPlan, error) {
	call := p.calls.Add(1)
	estimated := p.estimated
	if index := int(call) - 1; index >= 0 && index < len(p.estimates) {
		estimated = p.estimates[index]
	}
	content := []protocol.ContentBlock{{Kind: protocol.ContentText, Text: fmt.Sprintf("planned-%d", call)}}
	sourceDigest, _ := canonicaljson.Digest(content)
	body := protocol.ContextPlanBody{
		Sources:  []protocol.ContentSource{{ID: fmt.Sprintf("plan-%d", call), Kind: "user_message", Scope: "turn", Provenance: "test", Digest: sourceDigest, Content: content}},
		Excluded: []protocol.ExcludedContentSource{}, EstimatedInputTokens: estimated, OutputReserve: 0,
		ContextWindow: p.window, CompactionRevision: fmt.Sprintf("revision-%d", call), ToolExposureRevision: "tools-a",
	}
	digest, _ := canonicaljson.Digest(body)
	plan := protocol.ContextPlan{Body: body, Digest: digest}
	p.mu.Lock()
	p.plans = append(p.plans, plan)
	p.mu.Unlock()
	return plan, nil
}

func (p *automaticCompactionPlanner) secondDigest() protocol.Digest {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.plans) < 2 {
		return protocol.Digest{}
	}
	return p.plans[1].Digest
}

type automaticCompactionProvider struct {
	log           *recordLog
	mu            sync.Mutex
	prepared      []bool
	normalDigests []protocol.Digest
	compactions   atomic.Int64
}

func (p *automaticCompactionProvider) Prepare(_ context.Context, _ protocol.ActivityID, _ string, request protocol.ModelRequest, contextDigest protocol.Digest) (provider.ProviderHandle, error) {
	if p.log != nil {
		p.log.add("provider.prepare")
	}
	compact := strings.HasPrefix(request.RequestID, "compaction-request-")
	p.mu.Lock()
	p.prepared = append(p.prepared, compact)
	if !compact {
		p.normalDigests = append(p.normalDigests, contextDigest)
	}
	p.mu.Unlock()
	if compact {
		p.compactions.Add(1)
	}
	return provider.ProviderHandle{}, nil
}

func (p *automaticCompactionProvider) Stream(_ context.Context, _ provider.ProviderHandle, _ authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	p.mu.Lock()
	compact := p.prepared[0]
	p.prepared = p.prepared[1:]
	p.mu.Unlock()
	stream := make(chan protocol.ModelEvent, 2)
	if compact {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: string(validCompactionSummary)}}
	} else {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: "done"}}
	}
	stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	close(stream)
	return stream, nil
}

func (p *automaticCompactionProvider) normalPlanDigest() protocol.Digest {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.normalDigests) == 0 {
		return protocol.Digest{}
	}
	return p.normalDigests[len(p.normalDigests)-1]
}

type automaticLoopProvider struct {
	mu          sync.Mutex
	prepared    []bool
	compactions atomic.Int64
	normals     atomic.Int64
}

func (p *automaticLoopProvider) Prepare(_ context.Context, _ protocol.ActivityID, _ string, request protocol.ModelRequest, _ protocol.Digest) (provider.ProviderHandle, error) {
	compact := strings.HasPrefix(request.RequestID, "compaction-request-")
	p.mu.Lock()
	p.prepared = append(p.prepared, compact)
	p.mu.Unlock()
	if compact {
		p.compactions.Add(1)
	} else {
		p.normals.Add(1)
	}
	return provider.ProviderHandle{}, nil
}

func (p *automaticLoopProvider) Stream(_ context.Context, _ provider.ProviderHandle, _ authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	p.mu.Lock()
	compact := p.prepared[0]
	p.prepared = p.prepared[1:]
	p.mu.Unlock()
	stream := make(chan protocol.ModelEvent, 2)
	if compact {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: string(validCompactionSummary)}}
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	} else if p.normals.Load() == 1 {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventToolIntent, Sequence: 1, ToolIntent: &protocol.ToolUseBlock{CallID: "call-a", Alias: "read", Arguments: json.RawMessage(`{"path":"a.go"}`)}}
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "tool_use"}}
	} else {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: "done"}}
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	}
	close(stream)
	return stream, nil
}

type automaticFailureProvider struct {
	mode            string
	log             *recordLog
	cancel          context.CancelFunc
	compactPrepares atomic.Int64
	normalPrepares  atomic.Int64
	mu              sync.Mutex
	prepared        []bool
}

func (p *automaticFailureProvider) Prepare(_ context.Context, _ protocol.ActivityID, _ string, request protocol.ModelRequest, _ protocol.Digest) (provider.ProviderHandle, error) {
	compact := strings.HasPrefix(request.RequestID, "compaction-request-")
	if compact {
		p.compactPrepares.Add(1)
		if p.mode == "compact_prepare" {
			return provider.ProviderHandle{}, errors.New("compaction provider preparation failed")
		}
	} else {
		p.normalPrepares.Add(1)
	}
	p.mu.Lock()
	p.prepared = append(p.prepared, compact)
	p.mu.Unlock()
	return provider.ProviderHandle{}, nil
}

func (p *automaticFailureProvider) Stream(_ context.Context, _ provider.ProviderHandle, _ authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	p.mu.Lock()
	compact := p.prepared[0]
	p.prepared = p.prepared[1:]
	p.mu.Unlock()
	if compact && p.mode == "compact_stream" {
		return nil, errors.New("compaction provider request outcome is uncertain")
	}
	if compact && p.mode == "cancel" {
		p.cancel()
		return make(chan protocol.ModelEvent), nil
	}
	stream := make(chan protocol.ModelEvent, 2)
	if compact {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: string(validCompactionSummary)}}
	} else if p.mode == "normal_context_too_large" {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventError, Sequence: 1, Error: &protocol.ProviderError{Code: "context_too_large", Message: "context too large"}}
	} else {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: "done"}}
	}
	if !(compact && p.mode == "cancel") && !(p.mode == "normal_context_too_large" && !compact) {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	}
	close(stream)
	return stream, nil
}

type failingEvidenceRecorder struct{}

func (failingEvidenceRecorder) Put(context.Context, protocol.EvidenceCandidate) (protocol.EvidenceRecord, error) {
	return protocol.EvidenceRecord{}, errors.New("evidence persistence failed")
}
