package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/eventcodec"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	receiptprojector "github.com/muratmirgun/yordam/internal/subagent"
	"github.com/muratmirgun/yordam/internal/tooling"
	"github.com/muratmirgun/yordam/internal/verification"
)

func TestRecoveryAuthorizedControlTerminalizesCommittedSessionBeforeLeaseRelease(t *testing.T) {
	log := &recordLog{}
	runtime := validRuntimeManifest(t, "observation")
	controlRef := protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace-control-a"}
	sessionRef := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session-a"}
	controlHead := protocol.CommittedCursor{JournalKind: controlRef.Kind, JournalID: controlRef.ID, CommitSeq: 1, TransactionID: "control-head"}
	sessionHead := protocol.CommittedCursor{JournalKind: sessionRef.Kind, JournalID: sessionRef.ID, CommitSeq: 10, TransactionID: "session-head"}
	recoveredHead := protocol.CommittedCursor{JournalKind: sessionRef.Kind, JournalID: sessionRef.ID, CommitSeq: 8, TransactionID: "recovered-head"}
	repository := newRecoveryRepository(log, controlRef, controlHead, sessionRef, sessionHead, recoveredHead)
	projection := recoveryProjectionFake{projection: RecoveryProjection{
		ActiveTurnID: "turn-original", TaskID: "task-original", OriginalCommandID: "command-original",
		OriginalRequestDigest: repeatedDigest("5"), StartedActivities: []protocol.ActivityID{"activity-original"},
	}}
	service, err := NewService(Dependencies{
		Lane: &loggingLane{delegate: NewOperationLane(), log: log}, Repository: repository,
		TurnLeases: &recordingTurnLeaseManager{log: log}, Authorization: &allowingAuthorization{log: log}, Projection: projection,
		Admission: passthroughAdmission{},
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := testActionPlan(tooling.PlanRequest{CallID: "recover-a", Alias: "recover", RuntimeGenerationID: runtime.ID}, "mutation", "not_reversible")
	actor := protocol.ActorRef{ID: "operator-a", Kind: protocol.ActorUser}
	diagnostic := protocol.Diagnostic{Code: "recovery.requested", Message: "recover", Journal: controlRef}
	request := RecoveryControlRequest{
		Control: ControlRequest{
			Command:     CommandMetadata{CommandID: "recovery-command-a", IdempotencyKey: "recovery-key-a", RequestDigest: repeatedDigest("6"), Actor: actor},
			OperationID: "recovery-operation-a", Kind: OperationRecovery, Journal: controlRef, ExpectedHead: controlHead,
			TransactionID: "recovery-control-terminal", Runtime: runtime, Plan: plan,
			Event: protocol.ProposedEvent{EventID: "recovery-event", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventRecoveryDiagnostic, Actor: &actor, RuntimeGenerationID: runtime.ID, Payload: mustCanonical(protocol.DiagnosticV1{Diagnostic: diagnostic})},
		},
		Storage: journal.RecoveryRequest{
			OperationID: "recovery-operation-a", Journal: sessionRef, ExpectedHead: sessionHead, ObservedTailDigest: repeatedDigest("7"),
			TransactionID: "storage-recovery-a", RuntimeGenerationID: runtime.ID,
		},
	}

	result, err := service.RecoverTurn(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Recovery.Cursor != recoveredHead || repository.recoverCount() != 1 || repository.storageRequest() != request.Storage {
		t.Fatalf("result=%+v count=%d storage=%+v", result, repository.recoverCount(), repository.storageRequest())
	}
	want := []string{
		"lane.acquire(recovery)", "turn_recovery_lease.acquire",
		"append_control(command.accepted,control_operation.planned,authorization.requested)",
		"authorization.decide", "append_control(authorization.decided,control_operation.authorized)",
		"append_control(authorization.decision_consumed,control_operation.started)", "authorization.issue",
		"session_repository.recover", "append_session(activity.uncertain,turn.interrupted,task.status_changed,command.completed)",
		"append_control(control_operation.completed,command.completed)", "turn_lease.release", "lane.release",
	}
	if got := log.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("recovery order got=%v want=%v", got, want)
	}
	validateAppendRequests(t, repository.appendRequests())

	runConcurrent(t, func() error {
		duplicate, duplicateErr := service.RecoverTurn(context.Background(), request)
		if duplicateErr == nil && duplicate.Recovery.Cursor != recoveredHead {
			return fmt.Errorf("duplicate recovery cursor=%+v", duplicate.Recovery.Cursor)
		}
		return duplicateErr
	})
	if got := repository.recoverCount(); got != 1 {
		t.Fatalf("repository recovery calls=%d want=1", got)
	}
	changed := request
	changed.Control.Command.RequestDigest = repeatedDigest("8")
	if _, err := service.RecoverTurn(context.Background(), changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed digest error=%v", err)
	}
	if got := repository.recoverCount(); got != 1 {
		t.Fatalf("repository recovery calls after conflict=%d want=1", got)
	}
}

func TestRecoveryConflictTerminalizesControlWithoutSessionTerminal(t *testing.T) {
	log := &recordLog{}
	runtime := validRuntimeManifest(t, "observation")
	controlRef := protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace-control-a"}
	sessionRef := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session-a"}
	controlHead := protocol.CommittedCursor{JournalKind: controlRef.Kind, JournalID: controlRef.ID, CommitSeq: 1, TransactionID: "control-head"}
	sessionHead := protocol.CommittedCursor{JournalKind: sessionRef.Kind, JournalID: sessionRef.ID, CommitSeq: 10, TransactionID: "session-head"}
	repository := newRecoveryRepository(log, controlRef, controlHead, sessionRef, sessionHead, sessionHead)
	repository.result = journal.RecoveryResult{Status: "conflict", Cursor: sessionHead}
	projection := recoveryProjectionFake{projection: RecoveryProjection{ActiveTurnID: "turn-original", TaskID: "task-original", OriginalCommandID: "command-original", OriginalRequestDigest: repeatedDigest("5")}}
	service, err := NewService(Dependencies{Lane: NewOperationLane(), Repository: repository, TurnLeases: &recordingTurnLeaseManager{}, Authorization: &allowingAuthorization{log: &recordLog{}}, Projection: projection, Admission: passthroughAdmission{}})
	if err != nil {
		t.Fatal(err)
	}
	plan := testActionPlan(tooling.PlanRequest{CallID: "recover-a", Alias: "recover", RuntimeGenerationID: runtime.ID}, "mutation", "not_reversible")
	actor := protocol.ActorRef{ID: "operator-a", Kind: protocol.ActorUser}
	request := RecoveryControlRequest{
		Control: ControlRequest{
			Command:     CommandMetadata{CommandID: "recovery-command-a", IdempotencyKey: "recovery-key-a", RequestDigest: repeatedDigest("6"), Actor: actor},
			OperationID: "recovery-operation-a", Kind: OperationRecovery, Journal: controlRef, ExpectedHead: controlHead,
			TransactionID: "recovery-control-terminal", Runtime: runtime, Plan: plan,
			Event: protocol.ProposedEvent{EventID: "recovery-event", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventRecoveryDiagnostic, Actor: &actor, RuntimeGenerationID: runtime.ID, Payload: mustCanonical(protocol.DiagnosticV1{Diagnostic: protocol.Diagnostic{Code: "recovery.requested", Message: "recover", Journal: controlRef}})},
		},
		Storage: journal.RecoveryRequest{OperationID: "recovery-operation-a", Journal: sessionRef, ExpectedHead: sessionHead, ObservedTailDigest: repeatedDigest("7"), TransactionID: "storage-recovery-a", RuntimeGenerationID: runtime.ID},
	}
	if _, err := service.RecoverTurn(context.Background(), request); err == nil {
		t.Fatal("conflicting physical recovery was accepted")
	}
	kinds := flattenAppendKinds(repository.appendRequests())
	if slices.Contains(kinds, protocol.EventTurnInterrupted) || slices.Contains(kinds, protocol.EventActivityUncertain) {
		t.Fatalf("session was terminalized for failed recovery: %v", kinds)
	}
	if !slices.Contains(kinds, protocol.EventControlOperationFailed) || !slices.Contains(kinds, protocol.EventCommandCompleted) {
		t.Fatalf("recovery control was not failed durably: %v", kinds)
	}
	validateAppendRequests(t, repository.appendRequests())
}

func TestDurablePrefixPagesAtThousandRecords(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "child"}
	r := newRecoveryRepository(&recordLog{}, protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "control"}, protocol.CommittedCursor{JournalKind: protocol.JournalWorkspaceControl, JournalID: "control"}, ref, protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child"}, protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child", CommitSeq: 1001, TransactionID: "tail"})
	for i := 0; i < 1001; i++ {
		kind := protocol.EventActivityProgress
		if i == 1000 {
			kind = protocol.EventTransactionCommitted
		}
		r.events[ref] = append(r.events[ref], protocol.ProposedEvent{Kind: kind, SessionID: "child"})
	}
	r.heads[ref] = protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child", CommitSeq: 1001, TransactionID: "tail"}
	service, err := NewService(Dependencies{Repository: r, Lane: NewOperationLane()})
	if err != nil {
		t.Fatal(err)
	}
	events, err := service.durablePrefix(context.Background(), ref, protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child", CommitSeq: 1001, TransactionID: "tail"})
	if err != nil || len(events) != 1001 {
		t.Fatalf("events=%d err=%v", len(events), err)
	}
}

func TestDurablePrefixRejectsWrongCursorIdentity(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "child"}
	r := newRecoveryRepository(&recordLog{}, protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "control"}, protocol.CommittedCursor{}, ref, protocol.CommittedCursor{}, protocol.CommittedCursor{})
	service, _ := NewService(Dependencies{Repository: r, Lane: NewOperationLane()})
	if _, err := service.durablePrefix(context.Background(), ref, protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "other", CommitSeq: 1, TransactionID: "t"}); err == nil {
		t.Fatal("wrong prefix journal accepted")
	}
}

func TestDurablePrefixStopsAtMarkerInsidePageWithLaterTail(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "child"}
	r := newRecoveryRepository(&recordLog{}, protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "control"}, protocol.CommittedCursor{}, ref, protocol.CommittedCursor{}, protocol.CommittedCursor{})
	r.events[ref] = []protocol.ProposedEvent{{Kind: protocol.EventActivityProgress, SessionID: "child"}, {Kind: protocol.EventTransactionCommitted, SessionID: "child"}, {Kind: protocol.EventActivityProgress, SessionID: "child"}}
	r.heads[ref] = protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child", CommitSeq: 3, TransactionID: "tail"}
	service, _ := NewService(Dependencies{Repository: r, Lane: NewOperationLane()})
	events, err := service.durablePrefix(context.Background(), ref, protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child", CommitSeq: 2, TransactionID: "page"})
	if err != nil || len(events) != 2 || events[1].Envelope.Kind != protocol.EventTransactionCommitted {
		t.Fatalf("events=%+v err=%v", events, err)
	}
}

func TestRecoveryChildReceiptBindsManifestRuntimeAndTerminalCursor(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "child"}
	head := protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child", CommitSeq: 3, TransactionID: "child-head"}
	r := newRecoveryRepository(&recordLog{}, protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "control"}, protocol.CommittedCursor{}, ref, head, head)
	manifest := protocol.SubagentManifestV1{AttemptID: "attempt", ParentSessionID: "parent", ParentCursor: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "parent", CommitSeq: 1, TransactionID: "parent-tx"}, ChildSessionID: "child", ChildTaskID: "child-task", ChildTurnID: "child-turn", RuntimeGenerationID: "child-runtime", SkillCatalogRevision: "skills", MaxToolCalls: 1, Deadline: time.Now().Add(time.Minute)}
	r.events[ref] = []protocol.ProposedEvent{{Kind: protocol.EventSubagentManifest, SessionID: "child", TaskID: manifest.ChildTaskID, TurnID: manifest.ChildTurnID, RuntimeGenerationID: manifest.RuntimeGenerationID, Payload: mustCanonical(manifest)}, {Kind: protocol.EventActivityStarted, SessionID: "child", ActivityID: "child-activity"}, {Kind: protocol.EventTransactionCommitted, SessionID: "child", TaskID: manifest.ChildTaskID, TurnID: manifest.ChildTurnID}}
	service, err := NewService(Dependencies{Repository: r, Lane: NewOperationLane()})
	if err != nil {
		t.Fatal(err)
	}
	request := RecoveryControlRequest{Control: ControlRequest{Command: CommandMetadata{CommandID: "recover-command"}, Runtime: protocol.RuntimeGenerationManifest{ID: "current-runtime"}}, Storage: journal.RecoveryRequest{Journal: ref}}
	_, err = service.appendRecoverySessionTerminal(context.Background(), request, RecoveryProjection{ActiveTurnID: manifest.ChildTurnID, TaskID: manifest.ChildTaskID, StartedActivities: []protocol.ActivityID{"child-activity"}, ChildManifest: &manifest}, head)
	if err != nil {
		t.Fatal(err)
	}
	requests := r.appendRequests()
	appended := requests[len(requests)-1]
	var receipt protocol.SubagentReceiptV1
	var event protocol.ProposedEvent
	for _, candidate := range appended.Events {
		if candidate.Kind == protocol.EventSubagentReceipt {
			event = candidate
			_ = json.Unmarshal(candidate.Payload, &receipt)
		}
	}
	if event.RuntimeGenerationID != manifest.RuntimeGenerationID || receipt.Manifest.ChildSessionID != manifest.ChildSessionID || receipt.Manifest.ChildTaskID != manifest.ChildTaskID || receipt.Manifest.ChildTurnID != manifest.ChildTurnID || receipt.Manifest.RuntimeGenerationID != manifest.RuntimeGenerationID || receipt.TerminalCursor.CommitSeq != head.CommitSeq+uint64(len(appended.Events)) {
		t.Fatalf("event=%+v receipt=%+v", event, receipt)
	}
	if err := receipt.Validate(); err != nil {
		t.Fatal(err)
	}
	seq := head.CommitSeq + uint64(len(appended.Events))
	envelope := protocol.EventEnvelope{SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: event.PayloadVersion, JournalKind: ref.Kind, JournalID: ref.ID, EventID: event.EventID, SessionID: event.SessionID, Seq: seq, Time: event.Time, Kind: event.Kind, TaskID: event.TaskID, TurnID: event.TurnID, Actor: event.Actor, RuntimeGenerationID: event.RuntimeGenerationID, TransactionID: appended.TransactionID, Payload: event.Payload}
	raw, _ := canonicaljson.Marshal(envelope)
	registry, _ := eventcodec.New(eventcodec.FoundationDescriptors())
	record, err := registry.Decode(raw)
	if err != nil || registry.Validate(record) != nil {
		t.Fatalf("committed receipt envelope invalid: %v", err)
	}
	store := recoveryChildStore{inspection: journal.Inspection{Journal: ref, Head: receipt.TerminalCursor, Events: []protocol.EventRecord{{Envelope: envelope}}}}
	got, err := committedChildReceipt(context.Background(), store, manifest)
	if err != nil || !reflect.DeepEqual(got, receipt) {
		t.Fatalf("committed receipt=%+v err=%v", got, err)
	}
}

func TestSubagentRecoverNoEffectChildWritesCancelledReceipt(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "child"}
	head := protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child", CommitSeq: 1, TransactionID: "child-head"}
	repository := newRecoveryRepository(&recordLog{}, protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "control"}, protocol.CommittedCursor{}, ref, head, head)
	manifest := protocol.SubagentManifestV1{AttemptID: "attempt", ParentSessionID: "parent", ParentCursor: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "parent", CommitSeq: 1, TransactionID: "parent-tx"}, ChildSessionID: "child", ChildTaskID: "child-task", ChildTurnID: "child-turn", RuntimeGenerationID: "runtime", SkillCatalogRevision: "skills", MaxToolCalls: 1, Deadline: time.Now().Add(time.Minute)}
	repository.events[ref] = []protocol.ProposedEvent{{Kind: protocol.EventSubagentManifest, SessionID: "child", TaskID: manifest.ChildTaskID, TurnID: manifest.ChildTurnID, RuntimeGenerationID: manifest.RuntimeGenerationID, Payload: mustCanonical(manifest)}}
	service, err := NewService(Dependencies{Repository: repository, Lane: NewOperationLane()})
	if err != nil {
		t.Fatal(err)
	}
	request := RecoveryControlRequest{Control: ControlRequest{Command: CommandMetadata{CommandID: "recover-command"}, Runtime: protocol.RuntimeGenerationManifest{ID: manifest.RuntimeGenerationID}}, Storage: journal.RecoveryRequest{Journal: ref}}
	if _, err := service.appendRecoverySessionTerminal(context.Background(), request, RecoveryProjection{ActiveTurnID: manifest.ChildTurnID, TaskID: manifest.ChildTaskID, ChildManifest: &manifest, UnmatchedNoEffect: map[protocol.ActivityID]bool{}}, head); err != nil {
		t.Fatal(err)
	}
	var receipt protocol.SubagentReceiptV1
	for _, event := range repository.appendRequests()[0].Events {
		if event.Kind == protocol.EventSubagentReceipt {
			if err := json.Unmarshal(event.Payload, &receipt); err != nil {
				t.Fatal(err)
			}
		}
	}
	if receipt.Status != "cancelled" || receipt.Error == nil || receipt.Error.Code != "recovery_interrupted" {
		t.Fatalf("receipt=%+v", receipt)
	}
}

func TestSubagentRecoverAlreadyTerminalParentDoesNotResumeAttachedReceipt(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"}
	head := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 9, TransactionID: "parent-terminal"}
	repository := newRecoveryRepository(&recordLog{}, protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "control"}, protocol.CommittedCursor{}, ref, head, head)
	service, err := NewService(Dependencies{Repository: repository, Lane: NewOperationLane()})
	if err != nil {
		t.Fatal(err)
	}
	projection := RecoveryProjection{} // the attached receipt's continuation is terminal
	got, changed, resumed, err := service.reconcileWaitingSubagents(context.Background(), nil, RecoveryControlRequest{Storage: journal.RecoveryRequest{Journal: ref}}, &projection, head)
	if err != nil || got != head || changed || resumed || len(repository.appendRequests()) != 0 {
		t.Fatalf("head=%+v changed=%v resumed=%v requests=%d err=%v", got, changed, resumed, len(repository.appendRequests()), err)
	}
}

func TestRecoveryTerminalizesUncertainSubagentWithStructuredNonRetryableDiagnostic(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"}
	head := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 1, TransactionID: "parent-head"}
	repository := newRecoveryRepository(&recordLog{}, protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "control"}, protocol.CommittedCursor{}, ref, head, head)
	service, err := NewService(Dependencies{Repository: repository, Lane: NewOperationLane()})
	if err != nil {
		t.Fatal(err)
	}
	runtime := validRuntimeManifest(t, "observation")
	request := RecoveryControlRequest{Control: ControlRequest{Command: CommandMetadata{CommandID: "recover-command"}, Runtime: runtime}, Storage: journal.RecoveryRequest{Journal: ref}}
	diagnostic := &SubagentRecoveryDiagnostic{AttemptID: "attempt-uncertain", ChildSessionID: "child-uncertain", ChildStatus: "uncertain", TerminalCursor: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child-uncertain", CommitSeq: 4, TransactionID: "child-commit"}, ReceiptDigest: repeatedDigest("a"), Reason: "child journal commit is unknown"}
	if _, err := service.appendRecoverySessionTerminal(context.Background(), request, RecoveryProjection{ActiveTurnID: "parent-turn", TaskID: "parent-task", OriginalCommandID: "parent-command", OriginalRequestDigest: repeatedDigest("b"), UnmatchedNoEffect: map[protocol.ActivityID]bool{}, SubagentRecoveryDiagnostic: diagnostic}, head); err != nil {
		t.Fatal(err)
	}
	var gotDiagnostic protocol.DiagnosticV1
	var completed protocol.CommandCompletedV1
	for _, event := range repository.appendRequests()[0].Events {
		switch event.Kind {
		case protocol.EventRecoveryDiagnostic:
			if err := json.Unmarshal(event.Payload, &gotDiagnostic); err != nil {
				t.Fatal(err)
			}
		case protocol.EventCommandCompleted:
			if err := json.Unmarshal(event.Payload, &completed); err != nil {
				t.Fatal(err)
			}
		}
	}
	var details SubagentRecoveryDiagnostic
	if err := json.Unmarshal(gotDiagnostic.Diagnostic.Details, &details); err != nil {
		t.Fatal(err)
	}
	if gotDiagnostic.Diagnostic.Code != "subagent.recovery_uncertain" || details != *diagnostic || completed.Error == nil || completed.Error.Code != "subagent_recovery_uncertain" || completed.Error.Retryable {
		t.Fatalf("diagnostic=%+v details=%+v completed=%+v", gotDiagnostic, details, completed)
	}
}

func TestSubagentRecoveryCreatesReservedChildOnceAndContinuesParent(t *testing.T) {
	service, request, repository, recoveryLog := recoveryBarrierFixture(t, NoopBarrierProbe())
	request.Control.Runtime.Body.SkillCatalogRevision = "skills-a"
	request.Control.Runtime.Body.Limits.Subagents = protocol.SubagentLimits{Enabled: true, MaxPerTurn: 4, MaxToolCalls: 1, TimeoutNanos: int64(time.Minute)}
	refreshRuntimeDigest(t, &request.Control.Runtime)
	parentRef := request.Storage.Journal
	manifest := protocol.SubagentManifestV1{
		AttemptID: "restart-attempt", ParentSessionID: "session-a",
		ParentCursor:   protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: parentRef.ID, CommitSeq: 1, TransactionID: "parent-provider"},
		ChildSessionID: "child-reserved", ChildTaskID: "child-task", ChildTurnID: "child-turn",
		RuntimeGenerationID: request.Control.Runtime.ID, SkillCatalogRevision: request.Control.Runtime.Body.SkillCatalogRevision,
		MaxToolCalls: 1, Deadline: time.Now().Add(time.Minute),
	}
	intent := protocol.ToolUseBlock{CallID: "delegate", Alias: "subagent", Arguments: json.RawMessage(`{"task":"durable child task"}`)}
	activityID := protocol.ActivityID(stableID("activity", "command-original", "subagent", intent.CallID))
	previousManifest := protocol.DeepCopy(manifest)
	previousManifest.AttemptID = "completed-attempt"
	previousManifest.ChildSessionID = "child-completed"
	previousManifest.ChildTaskID = "completed-child-task"
	previousManifest.ChildTurnID = "completed-child-turn"
	previousManifest.ParentCursor = protocol.CommittedCursor{JournalKind: parentRef.Kind, JournalID: parentRef.ID, CommitSeq: 4, TransactionID: "previous-parent-provider"}
	previousActivityID := protocol.ActivityID("previous-subagent-activity")
	previousReceipt := childReceipt(previousManifest, "succeeded", "earlier child completed", protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(previousManifest.ChildSessionID), CommitSeq: 2, TransactionID: "previous-child-terminal"}, unknownUsage(), nil, nil)
	previousReceiptDigest, err := canonicaljson.Digest(previousReceipt)
	if err != nil {
		t.Fatal(err)
	}
	recoveredHead := protocol.CommittedCursor{JournalKind: parentRef.Kind, JournalID: parentRef.ID, CommitSeq: 10, TransactionID: "recovered-parent-wait"}
	repository.recovered, repository.result.Cursor, repository.heads[parentRef] = recoveredHead, recoveredHead, recoveredHead
	repository.events[parentRef] = []protocol.ProposedEvent{
		{EventID: "command-original", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventCommandAccepted, SessionID: "session-a", TurnID: "turn-original", Actor: &protocol.ActorRef{ID: "user", Kind: protocol.ActorUser}, RuntimeGenerationID: request.Control.Runtime.ID, Payload: mustCanonical(protocol.CommandAcceptedV1{CommandID: "command-original", RequestDigest: repeatedDigest("5"), IdempotencyKey: "parent-key"})},
		{EventID: "turn-accepted", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventTurnAccepted, SessionID: "session-a", TaskID: "task-original", TurnID: "turn-original", RuntimeGenerationID: request.Control.Runtime.ID, Payload: mustCanonical(protocol.TurnAcceptedV1{CommandID: "command-original", Goal: "durable parent goal"})},
		{EventID: "assistant-tool-use", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventAssistantMessage, SessionID: "session-a", TaskID: "task-original", TurnID: "turn-original", RuntimeGenerationID: request.Control.Runtime.ID, Payload: mustCanonical(protocol.AssistantMessageV1{ToolIntents: []protocol.ToolUseBlock{intent}})},
		{EventID: "provider-terminal", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventProviderAttemptTerminal, SessionID: "session-a", TaskID: "task-original", TurnID: "turn-original", RuntimeGenerationID: request.Control.Runtime.ID, Payload: mustCanonical(protocol.ProviderAttemptTerminalV1{Status: "completed"})},
		{EventID: "previous-subagent-request", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventSubagentRequested, SessionID: "session-a", TaskID: "task-original", TurnID: "turn-original", ActivityID: previousActivityID, RuntimeGenerationID: request.Control.Runtime.ID, Payload: mustCanonical(protocol.SubagentRequestedV1{Call: protocol.SubagentCallV1{Task: "earlier durable child task"}, Manifest: previousManifest})},
		{EventID: "previous-subagent-wait", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventSubagentWaiting, SessionID: "session-a", TaskID: "task-original", TurnID: "turn-original", ActivityID: previousActivityID, RuntimeGenerationID: request.Control.Runtime.ID, Payload: mustCanonical(protocol.SubagentWaitingV1{AttemptID: previousManifest.AttemptID, ChildSessionID: previousManifest.ChildSessionID})},
		{EventID: "previous-subagent-attachment", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventSubagentResultAttached, SessionID: "session-a", TaskID: "task-original", TurnID: "turn-original", ActivityID: previousActivityID, RuntimeGenerationID: request.Control.Runtime.ID, Payload: mustCanonical(protocol.SubagentResultAttachedV1{AttemptID: previousManifest.AttemptID, ChildSessionID: previousManifest.ChildSessionID, TerminalCursor: previousReceipt.TerminalCursor, ReceiptDigest: previousReceiptDigest, ReceiptEvidenceID: "previous-receipt-evidence"})},
		{EventID: "parent-activity", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventActivityStarted, SessionID: "session-a", TaskID: "task-original", TurnID: "turn-original", ActivityID: activityID, RuntimeGenerationID: request.Control.Runtime.ID, Payload: mustCanonical(protocol.ActivityOutcomeV1{Status: "started"})},
		{EventID: "subagent-request", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventSubagentRequested, SessionID: "session-a", TaskID: "task-original", TurnID: "turn-original", ActivityID: activityID, RuntimeGenerationID: request.Control.Runtime.ID, Payload: mustCanonical(protocol.SubagentRequestedV1{Call: protocol.SubagentCallV1{Task: "durable child task"}, Manifest: manifest})},
		{EventID: "subagent-wait", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventSubagentWaiting, SessionID: "session-a", TaskID: "task-original", TurnID: "turn-original", ActivityID: activityID, RuntimeGenerationID: request.Control.Runtime.ID, Payload: mustCanonical(protocol.SubagentWaitingV1{AttemptID: manifest.AttemptID, ChildSessionID: manifest.ChildSessionID})},
	}

	children := &restartRecoveryChildStore{
		workspace: domain.Workspace{ID: "workspace", CanonicalPath: "/workspace"}, selection: domain.ModelSelection{Profile: "provider-a", Model: "model-a"}, log: recoveryLog,
		inspections: map[protocol.SessionID]journal.Inspection{
			previousManifest.ChildSessionID: childRecoveryInspection(previousManifest, previousReceipt),
		},
	}
	var childStart StartTurnRequest
	coordinator, err := NewSequentialChildCoordinator(children, restartRecoveryParent{children}, func(_ context.Context, got StartTurnRequest) (RunResult, error) {
		children.runs++
		childStart = protocol.DeepCopy(got)
		receipt := childReceipt(got.child.manifest, "succeeded", "child completed", protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(got.SessionID), CommitSeq: 2, TransactionID: "child-terminal"}, unknownUsage(), nil, nil)
		raw := mustCanonical(receipt)
		children.inspection = journal.Inspection{Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(got.SessionID)}, Head: receipt.TerminalCursor, Writable: true, Events: []protocol.EventRecord{{Envelope: protocol.EventEnvelope{SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1, JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(got.SessionID), SessionID: got.SessionID, EventID: "child-receipt", Seq: receipt.TerminalCursor.CommitSeq, Time: time.Now().UTC(), Kind: protocol.EventSubagentReceipt, TaskID: got.child.manifest.ChildTaskID, TurnID: got.child.manifest.ChildTurnID, TransactionID: receipt.TerminalCursor.TransactionID, RuntimeGenerationID: got.Runtime.ID, Payload: raw}}}}
		recoveryLog.add("child.receipt.commit")
		return RunResult{Cursor: receipt.TerminalCursor, Status: "completed"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	continuation := &capturingFinalProvider{log: recoveryLog, name: "recovered-parent"}
	service.deps.ChildSessions, service.deps.ParentSessions, service.deps.Children = children, restartRecoveryParent{children}, coordinator
	service.deps.Evidence = recordingEvidence{log: recoveryLog}
	service.deps.Context, service.deps.Providers, service.deps.Provider = fakeContextPlanner{log: &recordLog{}}, fakeProviderCatalog{log: &recordLog{}}, continuation
	service.deps.Instructions, service.deps.Verification = emptyInstructionService{}, verification.NewService(time.Now)
	service.deps.Projection = terminalAwareRecoveryProjection{
		repository: repository,
		active: RecoveryProjection{
			ActiveTurnID: "turn-original", TaskID: "task-original", OriginalCommandID: "command-original",
			OriginalRequestDigest: repeatedDigest("5"), StartedActivities: []protocol.ActivityID{activityID},
		},
	}

	if _, err := service.RecoverTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if children.creates != 1 || children.runs != 1 || children.created != manifest.ChildSessionID {
		t.Fatalf("child create/run=%d/%d identity=%q", children.creates, children.runs, children.created)
	}
	if childStart.SessionID != manifest.ChildSessionID || childStart.ProviderID != "provider-a" || childStart.ModelID != "model-a" || childStart.Runtime.ID != request.Control.Runtime.ID || childStart.child == nil || !sameSubagentManifest(childStart.child.manifest, manifest) {
		t.Fatalf("recovered child request=%+v", childStart)
	}
	if continuation.request.ModelID != "model-a" || continuation.request.ProviderID != "provider-a" {
		t.Fatalf("parent continuation model=%q/%q", continuation.request.ProviderID, continuation.request.ModelID)
	}
	if result, ok := findToolResult(continuation.request, intent.CallID); !ok || result.Status != "succeeded" || len(result.EvidenceIDs) != 1 {
		// Recovery persists the exact canonical receipt result together with the
		// newly recorded parent evidence binding before provider continuation.
		t.Fatalf("continuation result=%+v present=%v", result, ok)
	}
	if !containsBatchKind(flattenBatches(repository.appendRequests()), protocol.EventSubagentResultAttached) || !containsBatchKind(flattenBatches(repository.appendRequests()), protocol.EventTurnCompleted) {
		t.Fatalf("recovery did not attach and terminalize parent: %v", flattenBatches(repository.appendRequests()))
	}
	recoveredAttachmentResult := false
	for _, appendRequest := range repository.appendRequests() {
		if appendHasKinds(appendRequest, protocol.EventActivitySucceeded, protocol.EventToolMessage, protocol.EventSubagentResultAttached) {
			recoveredAttachmentResult = true
		}
	}
	if !recoveredAttachmentResult {
		t.Fatalf("recovered attachment did not atomically persist tool.message: %v", flattenBatches(repository.appendRequests()))
	}
	var attachment protocol.SubagentResultAttachedV1
	attachmentCount := 0
	for _, request := range repository.appendRequests() {
		for _, event := range request.Events {
			if event.Kind == protocol.EventSubagentResultAttached {
				attachmentCount++
				if err := json.Unmarshal(event.Payload, &attachment); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	var committedReceipt protocol.SubagentReceiptV1
	if err := json.Unmarshal(children.inspection.Events[0].Envelope.Payload, &committedReceipt); err != nil {
		t.Fatal(err)
	}
	receiptDigest, err := canonicaljson.Digest(committedReceipt)
	if err != nil {
		t.Fatal(err)
	}
	if attachment.AttemptID != manifest.AttemptID || attachment.ChildSessionID != manifest.ChildSessionID || attachment.TerminalCursor != committedReceipt.TerminalCursor || attachment.ReceiptDigest != receiptDigest || attachment.ReceiptEvidenceID == "" {
		t.Fatalf("attachment=%+v receipt=%+v", attachment, committedReceipt)
	}
	if attachmentCount != 1 || children.inspectionCalls[previousManifest.ChildSessionID] != 1 {
		t.Fatalf("earlier attempt replayed or was not proven: attachments=%d inspection_calls=%v", attachmentCount, children.inspectionCalls)
	}
	assertStrictTrace(t, recoveryLog.snapshot(),
		"child.session.create",
		"child.receipt.commit",
		"evidence.put",
		"append_session(tool.message,activity.succeeded,evidence.recorded,evidence.linked,subagent_result_attached_v1)",
		"recovered-parent.provider.prepare",
		"recovered-parent.provider.stream",
	)
	if _, err := service.RecoverTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if children.creates != 1 || children.runs != 1 || repository.recoverCount() != 1 || countPrefix(recoveryLog.snapshot(), "recovered-parent.provider.prepare") != 1 {
		t.Fatalf("repeated recovery duplicated child create/run=%d/%d repository=%d trace=%v", children.creates, children.runs, repository.recoverCount(), recoveryLog.snapshot())
	}
}

func flattenBatches(requests []journal.AppendRequest) [][]string {
	batches := make([][]string, len(requests))
	for i, request := range requests {
		for _, event := range request.Events {
			batches[i] = append(batches[i], event.Kind)
		}
	}
	return batches
}

type restartRecoveryChildStore struct {
	workspace       domain.Workspace
	selection       domain.ModelSelection
	created         protocol.SessionID
	creates         int
	runs            int
	inspection      journal.Inspection
	inspections     map[protocol.SessionID]journal.Inspection
	inspectionCalls map[protocol.SessionID]int
	log             *recordLog
}

func (s *restartRecoveryChildStore) ReserveSessionID() (protocol.SessionID, error) {
	return "unexpected", errors.New("recovery must use the reserved child identity")
}
func (s *restartRecoveryChildStore) CreateWithIdentity(_ context.Context, id protocol.SessionID, workspace domain.Workspace, _ domain.PermissionMode, selection domain.ModelSelection, _ *journal.SessionLineage) (domain.Session, error) {
	if s.created != "" || id != "child-reserved" || workspace != s.workspace || selection != s.selection {
		return domain.Session{}, fmt.Errorf("unexpected child identity or inherited session binding")
	}
	s.created, s.creates = id, s.creates+1
	if s.log != nil {
		s.log.add("child.session.create")
	}
	s.inspection = journal.Inspection{
		Journal:  protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(id)},
		Head:     protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(id), CommitSeq: 1, TransactionID: "child-created"},
		Writable: true,
	}
	return domain.Session{ID: string(id), Workspace: workspace, Selection: selection}, nil
}
func (s *restartRecoveryChildStore) InspectSession(_ context.Context, id protocol.SessionID) (journal.Inspection, error) {
	if s.inspectionCalls == nil {
		s.inspectionCalls = make(map[protocol.SessionID]int)
	}
	s.inspectionCalls[id]++
	if inspection, ok := s.inspections[id]; ok {
		return protocol.DeepCopy(inspection), nil
	}
	if s.created == "" || id != s.created || s.inspection.Journal.ID == "" {
		return journal.Inspection{}, journal.ErrSessionNotFound
	}
	return protocol.DeepCopy(s.inspection), nil
}

func childRecoveryInspection(manifest protocol.SubagentManifestV1, receipt protocol.SubagentReceiptV1) journal.Inspection {
	return journal.Inspection{
		Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(manifest.ChildSessionID)}, Head: receipt.TerminalCursor, Writable: true,
		Events: []protocol.EventRecord{
			{Envelope: protocol.EventEnvelope{SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1, JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(manifest.ChildSessionID), SessionID: manifest.ChildSessionID, EventID: protocol.EventID("manifest-" + manifest.AttemptID), Seq: 1, Time: time.Now().UTC(), Kind: protocol.EventSubagentManifest, TaskID: manifest.ChildTaskID, TurnID: manifest.ChildTurnID, TransactionID: "child-manifest", RuntimeGenerationID: manifest.RuntimeGenerationID, Payload: mustCanonical(manifest)}},
			{Envelope: protocol.EventEnvelope{SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1, JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(manifest.ChildSessionID), SessionID: manifest.ChildSessionID, EventID: protocol.EventID("receipt-" + manifest.AttemptID), Seq: receipt.TerminalCursor.CommitSeq, Time: time.Now().UTC(), Kind: protocol.EventSubagentReceipt, TaskID: manifest.ChildTaskID, TurnID: manifest.ChildTurnID, TransactionID: receipt.TerminalCursor.TransactionID, RuntimeGenerationID: manifest.RuntimeGenerationID, Payload: mustCanonical(receipt)}},
		},
	}
}

type restartRecoveryParent struct{ store *restartRecoveryChildStore }

func (p restartRecoveryParent) InspectSession(_ context.Context, id protocol.SessionID) (journal.SessionInspection, error) {
	if id != "session-a" {
		return journal.SessionInspection{}, journal.ErrSessionNotFound
	}
	return journal.SessionInspection{Session: domain.Session{ID: string(id), Workspace: p.store.workspace, Mode: domain.ModeAsk, Selection: p.store.selection}}, nil
}

type terminalAwareRecoveryProjection struct {
	repository *recoveryRepository
	active     RecoveryProjection
}

func (p terminalAwareRecoveryProjection) InspectRecovery(_ context.Context, ref protocol.JournalRef, _ protocol.CommittedCursor) (RecoveryProjection, error) {
	if ref != p.repository.sessionRef {
		return RecoveryProjection{}, nil
	}
	for _, request := range p.repository.appendRequests() {
		if request.Journal != ref {
			continue
		}
		for _, event := range request.Events {
			if event.Kind == protocol.EventTurnCompleted || event.Kind == protocol.EventTurnInterrupted {
				return RecoveryProjection{}, nil
			}
		}
	}
	projection := protocol.DeepCopy(p.active)
	for _, request := range p.repository.appendRequests() {
		if request.Journal != ref {
			continue
		}
		for _, event := range request.Events {
			switch event.Kind {
			case protocol.EventActivityStarted:
				if !slices.Contains(projection.StartedActivities, event.ActivityID) {
					projection.StartedActivities = append(projection.StartedActivities, event.ActivityID)
				}
			case protocol.EventActivitySucceeded, protocol.EventActivityFailed, protocol.EventActivityInterruptedNoEffect, protocol.EventActivityUncertain:
				projection.StartedActivities = slices.DeleteFunc(projection.StartedActivities, func(id protocol.ActivityID) bool { return id == event.ActivityID })
			}
		}
	}
	return projection, nil
}

type recoveryChildStore struct{ inspection journal.Inspection }

func (s recoveryChildStore) ReserveSessionID() (protocol.SessionID, error) { return "", nil }
func (s recoveryChildStore) CreateWithIdentity(context.Context, protocol.SessionID, domain.Workspace, domain.PermissionMode, domain.ModelSelection, *journal.SessionLineage) (domain.Session, error) {
	return domain.Session{}, nil
}
func (s recoveryChildStore) InspectSession(context.Context, protocol.SessionID) (journal.Inspection, error) {
	return s.inspection, nil
}

func TestEveryRecoveryDispatchAndTerminalBarrierLeavesDurableControlTerminal(t *testing.T) {
	for _, test := range []struct {
		barrier          Barrier
		phase            string
		wantRecoveryCall bool
		wantSessionTerm  bool
	}{
		{barrier: BarrierAuthorizationCommitted, phase: "before"},
		{barrier: BarrierAuthorizationCommitted, phase: "after"},
		{barrier: BarrierEffectDispatch, phase: "before"},
		{barrier: BarrierEffectDispatch, phase: "after", wantRecoveryCall: true, wantSessionTerm: true},
		{barrier: BarrierRecoveryTurnTerminalCommitted, phase: "before", wantRecoveryCall: true, wantSessionTerm: true},
		{barrier: BarrierRecoveryTurnTerminalCommitted, phase: "after", wantRecoveryCall: true, wantSessionTerm: true},
	} {
		t.Run(string(test.barrier)+"_"+test.phase, func(t *testing.T) {
			service, request, repository, log := recoveryBarrierFixture(t, phaseBarrierProbe{barrier: test.barrier, phase: test.phase})
			if _, err := service.RecoverTurn(context.Background(), request); !errors.Is(err, errInjectedBarrier) {
				t.Fatalf("recovery error=%v", err)
			}
			if got := repository.recoverCount(); (got == 1) != test.wantRecoveryCall {
				t.Fatalf("recovery calls=%d want call=%v", got, test.wantRecoveryCall)
			}
			kinds := flattenAppendKinds(repository.appendRequests())
			if slices.Contains(kinds, protocol.EventTurnInterrupted) != test.wantSessionTerm {
				t.Fatalf("session terminal=%v want=%v kinds=%v", slices.Contains(kinds, protocol.EventTurnInterrupted), test.wantSessionTerm, kinds)
			}
			if !slices.Contains(kinds, protocol.EventControlOperationFailed) || !slices.Contains(kinds, protocol.EventCommandCompleted) {
				t.Fatalf("control terminal missing: %v", kinds)
			}
			entries := log.snapshot()
			terminalIndex := slices.Index(entries, "append_control(control_operation.failed,command.completed)")
			releaseIndex := slices.Index(entries, "turn_lease.release")
			if terminalIndex < 0 || releaseIndex <= terminalIndex {
				t.Fatalf("lease released before control terminal: %v", entries)
			}
			validateAppendRequests(t, repository.appendRequests())
		})
	}
}

func TestRecoveryCommittedControlPublishFailureDoesNotAppendSecondTerminal(t *testing.T) {
	service, request, repository, _ := recoveryBarrierFixture(t, NoopBarrierProbe())
	service.publisher = &nthFailPublisher{failAt: 5}
	if _, err := service.RecoverTurn(context.Background(), request); err == nil {
		t.Fatal("publisher failure was ignored")
	}
	kinds := flattenAppendKinds(repository.appendRequests())
	controlTerminals := 0
	for _, kind := range kinds {
		if kind == protocol.EventControlOperationCompleted || kind == protocol.EventControlOperationFailed || kind == protocol.EventControlOperationInterrupted {
			controlTerminals++
		}
	}
	if controlTerminals != 1 || !slices.Contains(kinds, protocol.EventControlOperationCompleted) {
		t.Fatalf("control terminals=%d kinds=%v", controlTerminals, kinds)
	}
	validateAppendRequests(t, repository.appendRequests())
}

func recoveryBarrierFixture(t *testing.T, probe BarrierProbe) (*Service, RecoveryControlRequest, *recoveryRepository, *recordLog) {
	t.Helper()
	log := &recordLog{}
	runtime := validRuntimeManifest(t, "observation")
	controlRef := protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace-control-a"}
	sessionRef := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session-a"}
	controlHead := protocol.CommittedCursor{JournalKind: controlRef.Kind, JournalID: controlRef.ID, CommitSeq: 1, TransactionID: "control-head"}
	sessionHead := protocol.CommittedCursor{JournalKind: sessionRef.Kind, JournalID: sessionRef.ID, CommitSeq: 10, TransactionID: "session-head"}
	recoveredHead := protocol.CommittedCursor{JournalKind: sessionRef.Kind, JournalID: sessionRef.ID, CommitSeq: 8, TransactionID: "recovered-head"}
	repository := newRecoveryRepository(log, controlRef, controlHead, sessionRef, sessionHead, recoveredHead)
	projection := recoveryProjectionFake{projection: RecoveryProjection{
		ActiveTurnID: "turn-original", TaskID: "task-original", OriginalCommandID: "command-original",
		OriginalRequestDigest: repeatedDigest("5"), StartedActivities: []protocol.ActivityID{"activity-original"},
	}}
	service, err := NewService(Dependencies{
		Lane: NewOperationLane(), Repository: repository, TurnLeases: &recordingTurnLeaseManager{log: log},
		Authorization: &allowingAuthorization{log: &recordLog{}}, Projection: projection, Admission: passthroughAdmission{}, BarrierProbe: probe,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := testActionPlan(tooling.PlanRequest{CallID: "recover-a", Alias: "recover", RuntimeGenerationID: runtime.ID}, "mutation", "not_reversible")
	actor := protocol.ActorRef{ID: "operator-a", Kind: protocol.ActorUser}
	request := RecoveryControlRequest{
		Control: ControlRequest{
			Command:     CommandMetadata{CommandID: "recovery-command-a", IdempotencyKey: "recovery-key-a", RequestDigest: repeatedDigest("6"), Actor: actor},
			OperationID: "recovery-operation-a", Kind: OperationRecovery, Journal: controlRef, ExpectedHead: controlHead,
			TransactionID: "recovery-control-terminal", Runtime: runtime, Plan: plan,
			Event: protocol.ProposedEvent{EventID: "recovery-event", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventRecoveryDiagnostic, Actor: &actor, RuntimeGenerationID: runtime.ID, Payload: mustCanonical(protocol.DiagnosticV1{Diagnostic: protocol.Diagnostic{Code: "recovery.requested", Message: "recover", Journal: controlRef}})},
		},
		Storage: journal.RecoveryRequest{
			OperationID: "recovery-operation-a", Journal: sessionRef, ExpectedHead: sessionHead, ObservedTailDigest: repeatedDigest("7"),
			TransactionID: "storage-recovery-a", RuntimeGenerationID: runtime.ID,
		},
	}
	return service, request, repository, log
}

type recoveryProjectionFake struct{ projection RecoveryProjection }

func (p recoveryProjectionFake) InspectRecovery(context.Context, protocol.JournalRef, protocol.CommittedCursor) (RecoveryProjection, error) {
	return protocol.DeepCopy(p.projection), nil
}

type recoveryRepository struct {
	mu          sync.Mutex
	log         *recordLog
	controlRef  protocol.JournalRef
	sessionRef  protocol.JournalRef
	heads       map[protocol.JournalRef]protocol.CommittedCursor
	events      map[protocol.JournalRef][]protocol.ProposedEvent
	recovered   protocol.CommittedCursor
	recovers    int
	lastStorage journal.RecoveryRequest
	requests    []journal.AppendRequest
	result      journal.RecoveryResult
}

func newRecoveryRepository(log *recordLog, controlRef protocol.JournalRef, controlHead protocol.CommittedCursor, sessionRef protocol.JournalRef, sessionHead, recovered protocol.CommittedCursor) *recoveryRepository {
	return &recoveryRepository{log: log, controlRef: controlRef, sessionRef: sessionRef, heads: map[protocol.JournalRef]protocol.CommittedCursor{controlRef: controlHead, sessionRef: sessionHead}, events: make(map[protocol.JournalRef][]protocol.ProposedEvent), recovered: recovered, result: journal.RecoveryResult{Status: "recovered", Cursor: recovered}}
}
func (r *recoveryRepository) Inspect(context.Context, protocol.JournalRef) (journal.Inspection, error) {
	return journal.Inspection{}, nil
}
func (r *recoveryRepository) Head(_ context.Context, ref protocol.JournalRef) (protocol.CommittedCursor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.heads[ref], nil
}
func (r *recoveryRepository) ReadRange(_ context.Context, request journal.ReadRangeRequest) (journal.EventPage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	all := r.events[request.Journal]
	start := int(request.After.CommitSeq)
	limit := request.Limit
	if limit <= 0 {
		limit = len(all)
	}
	end := start + limit
	if end > len(all) {
		end = len(all)
	}
	records := make([]protocol.EventRecord, end-start)
	for index, event := range all[start:end] {
		txn := protocol.TransactionID("page")
		if start+index+1 == len(all) {
			txn = r.heads[request.Journal].TransactionID
		}
		records[index] = protocol.EventRecord{Envelope: protocol.EventEnvelope{SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: event.PayloadVersion, JournalKind: request.Journal.Kind, JournalID: request.Journal.ID, SessionID: event.SessionID, EventID: event.EventID, Time: event.Time, Kind: event.Kind, Seq: uint64(start + index + 1), TransactionID: txn, Payload: event.Payload, TaskID: event.TaskID, TurnID: event.TurnID, ActivityID: event.ActivityID, Actor: event.Actor, RuntimeGenerationID: event.RuntimeGenerationID}}
	}
	cursor := request.After
	if end > start {
		cursor = protocol.CommittedCursor{JournalKind: request.Journal.Kind, JournalID: request.Journal.ID, CommitSeq: uint64(end), TransactionID: "page"}
		if end == len(all) {
			cursor.TransactionID = r.heads[request.Journal].TransactionID
		}
	}
	return journal.EventPage{Events: records, Head: r.heads[request.Journal], Cursor: cursor, More: end < len(all)}, nil
}
func (r *recoveryRepository) AppendBatch(_ context.Context, request journal.AppendRequest) (journal.AppendResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if request.ExpectedHead != r.heads[request.Journal] {
		return journal.AppendResult{Status: journal.AppendConflict, CurrentHead: r.heads[request.Journal]}, nil
	}
	kinds := make([]string, len(request.Events))
	for index, event := range request.Events {
		kinds[index] = event.Kind
	}
	prefix := "append_control"
	if request.Journal == r.sessionRef {
		prefix = "append_session"
	}
	r.log.add(prefix + "(" + joinKinds(kinds) + ")")
	r.events[request.Journal] = append(r.events[request.Journal], protocol.DeepCopy(request.Events)...)
	// The repository owns the transaction marker. Keep this fake's readable
	// prefix aligned with the committed cursor returned below so recovery can
	// reconstruct data written by an earlier recovery pass.
	r.events[request.Journal] = append(r.events[request.Journal], protocol.ProposedEvent{
		EventID:        protocol.EventID(fmt.Sprintf("%s-marker", request.TransactionID)),
		Time:           time.Now().UTC(),
		PayloadVersion: 1,
		Kind:           protocol.EventTransactionCommitted,
		SessionID:      request.Events[0].SessionID,
		Payload: mustCanonical(protocol.TransactionCommittedV1{
			TransactionID: request.TransactionID,
			FirstSeq:      request.ExpectedHead.CommitSeq + 1,
			LastSeq:       request.ExpectedHead.CommitSeq + uint64(len(request.Events)),
			EventCount:    uint32(len(request.Events)),
			Digest:        repeatedDigest("1"),
		}),
	})
	r.requests = append(r.requests, protocol.DeepCopy(request))
	head := protocol.CommittedCursor{JournalKind: request.Journal.Kind, JournalID: request.Journal.ID, CommitSeq: request.ExpectedHead.CommitSeq + uint64(len(request.Events)) + 1, TransactionID: request.TransactionID}
	r.heads[request.Journal] = head
	return journal.AppendResult{Status: journal.AppendCommitted, Cursor: head}, nil
}
func (r *recoveryRepository) LookupTransaction(context.Context, protocol.JournalRef, protocol.TransactionID) (journal.TransactionLookup, error) {
	return journal.TransactionLookup{}, nil
}
func (r *recoveryRepository) ReadCommittedTransaction(context.Context, protocol.JournalRef, protocol.TransactionID) (journal.CommittedTransaction, error) {
	return journal.CommittedTransaction{}, nil
}
func (r *recoveryRepository) Recover(_ context.Context, request journal.RecoveryRequest) (journal.RecoveryResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.log.add("session_repository.recover")
	r.recovers++
	r.lastStorage = request
	if r.result.Status == "recovered" {
		r.heads[request.Journal] = r.result.Cursor
	}
	return r.result, nil
}
func (r *recoveryRepository) recoverCount() int { r.mu.Lock(); defer r.mu.Unlock(); return r.recovers }
func (r *recoveryRepository) storageRequest() journal.RecoveryRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastStorage
}
func (r *recoveryRepository) appendRequests() []journal.AppendRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return protocol.DeepCopy(r.requests)
}

func joinKinds(kinds []string) string { return strings.Join(kinds, ",") }

var _ journal.Repository = (*recoveryRepository)(nil)

func TestRecoveryReconstructsExactSubagentToolResultContinuation(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"}
	head := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 2, TransactionID: "tail"}
	repository := newRecoveryRepository(&recordLog{}, protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "control"}, protocol.CommittedCursor{}, ref, head, head)
	intent := protocol.ToolUseBlock{CallID: "delegation-call", Alias: "subagent", Arguments: json.RawMessage(`{"task":"exact durable request"}`)}
	commandID := protocol.CommandID("parent-command")
	attempt := receiptprojector.Attempt{ParentTurnID: "parent-turn", ActivityID: protocol.ActivityID(stableID("activity", string(commandID), "subagent", intent.CallID))}
	repository.events[ref] = []protocol.ProposedEvent{
		{Kind: protocol.EventAssistantMessage, SessionID: "parent", TurnID: attempt.ParentTurnID, Payload: mustCanonical(protocol.AssistantMessageV1{ToolIntents: []protocol.ToolUseBlock{intent}})},
		{Kind: protocol.EventProviderAttemptTerminal, SessionID: "parent", TurnID: attempt.ParentTurnID, Payload: mustCanonical(protocol.ProviderAttemptTerminalV1{Status: "completed"})},
	}
	service, err := NewService(Dependencies{Repository: repository, Lane: NewOperationLane()})
	if err != nil {
		t.Fatal(err)
	}
	got, providerAttempt, err := service.recoverSubagentIntent(context.Background(), StartTurnRequest{SessionID: "parent", ExpectedHead: head, Command: CommandMetadata{CommandID: commandID}}, attempt)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, intent) || providerAttempt != 1 {
		t.Fatalf("intent=%+v provider_attempt=%d", got, providerAttempt)
	}
}

func TestRecoveryParentReconstructionBindsExactFrozenProviderAndModelPair(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session-a"}
	head := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 2, TransactionID: "parent-head"}
	repository := newRecoveryRepository(&recordLog{}, protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "control"}, protocol.CommittedCursor{}, ref, head, head)
	runtime := validRuntimeManifest(t, "observation")
	second := protocol.DeepCopy(runtime.Body.Models[0])
	second.ProviderID = "provider-b"
	second.DisplayName = "Provider B Model A"
	second.CredentialBindingRef = "credential-b"
	runtime.Body.Models = append(runtime.Body.Models, second)
	refreshRuntimeDigest(t, &runtime)
	actor := protocol.ActorRef{ID: "user", Kind: protocol.ActorUser}
	events := []protocol.EventRecord{
		{Envelope: protocol.EventEnvelope{Kind: protocol.EventCommandAccepted, TurnID: "parent-turn", RuntimeGenerationID: runtime.ID, Actor: &actor, Payload: mustCanonical(protocol.CommandAcceptedV1{CommandID: "parent-command", RequestDigest: repeatedDigest("4"), IdempotencyKey: "parent-key"})}},
		{Envelope: protocol.EventEnvelope{Kind: protocol.EventTurnAccepted, TurnID: "parent-turn", RuntimeGenerationID: runtime.ID, Payload: mustCanonical(protocol.TurnAcceptedV1{CommandID: "parent-command", Goal: "continue exact parent"})}},
	}
	children := &restartRecoveryChildStore{workspace: domain.Workspace{ID: "workspace", CanonicalPath: "/workspace"}, selection: domain.ModelSelection{Profile: "provider-b", Model: "model-a"}}
	service, err := NewService(Dependencies{Repository: repository, Lane: NewOperationLane(), ParentSessions: restartRecoveryParent{children}})
	if err != nil {
		t.Fatal(err)
	}
	attempt := receiptprojector.Attempt{Manifest: protocol.SubagentManifestV1{ChildSessionID: "child", RuntimeGenerationID: runtime.ID}, ParentTurnID: "parent-turn"}
	request := RecoveryControlRequest{Control: ControlRequest{Runtime: runtime}, Storage: journal.RecoveryRequest{Journal: ref}}
	projection := RecoveryProjection{OriginalCommandID: "parent-command", OriginalRequestDigest: repeatedDigest("4")}

	got, err := service.recoverParentStartRequest(context.Background(), request, projection, attempt, head, events)
	if err != nil {
		t.Fatal(err)
	}
	if got.ProviderID != "provider-b" || got.ModelID != "model-a" {
		t.Fatalf("reconstructed provider/model=%q/%q", got.ProviderID, got.ModelID)
	}
}

func TestRecoveryChildInspectionHealthControlsCommitKnowledge(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "child"}
	for _, test := range []struct {
		name       string
		inspection journal.Inspection
		want       bool
	}{
		{name: "healthy", inspection: journal.Inspection{Journal: ref, Writable: true}, want: true},
		{name: "read only", inspection: journal.Inspection{Journal: ref, Writable: false}},
		{name: "incomplete transaction", inspection: journal.Inspection{Journal: ref, Writable: true, IncompleteTransaction: "child-incomplete"}},
		{name: "recovery available", inspection: journal.Inspection{Journal: ref, Writable: true, Diagnostics: []protocol.Diagnostic{{Code: "recovery.available", Message: "journal has bytes beyond its validated committed prefix", Journal: ref}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := childInspectionCommitKnown(test.inspection); got != test.want {
				t.Fatalf("commit known=%v want=%v inspection=%+v", got, test.want, test.inspection)
			}
		})
	}
}

func TestRecoveryProviderContinuationFailureTerminalizesParentWithSubagentDiagnostic(t *testing.T) {
	service, request, repository, _, manifest := recoveredParentFailureFixture(t, domain.ModelSelection{Profile: "provider-a", Model: "model-a"}, failingProviderService{})

	result, err := service.RecoverTurn(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.CommandResult.Status != "completed" {
		t.Fatalf("recovery control result=%+v", result)
	}
	assertRecoveredParentFailureTerminalized(t, repository, manifest, "resume recovered parent continuation: provider stream fault")
	wantProviderActivity := protocol.ActivityID(stableID("activity", "command-original", "provider", "1"))
	foundUncertain := false
	for _, request := range repository.appendRequests() {
		for _, event := range request.Events {
			foundUncertain = foundUncertain || event.Kind == protocol.EventActivityUncertain && event.ActivityID == wantProviderActivity
		}
	}
	if !foundUncertain {
		t.Fatalf("provider continuation effect was not preserved as uncertain: %v", flattenBatches(repository.appendRequests()))
	}
}

func TestRecoveryParentReconstructionFailureTerminalizesParentWithSubagentDiagnostic(t *testing.T) {
	continuation := &capturingFinalProvider{log: &recordLog{}, name: "must-not-run"}
	service, request, repository, _, manifest := recoveredParentFailureFixture(t, domain.ModelSelection{Profile: "provider-missing", Model: "model-a"}, continuation)

	result, err := service.RecoverTurn(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.CommandResult.Status != "completed" || continuation.request.ModelID != "" {
		t.Fatalf("recovery control result=%+v continuation=%+v", result, continuation.request)
	}
	assertRecoveredParentFailureTerminalized(t, repository, manifest, "reconstruct recovered parent continuation: subagent recovery frozen model is unavailable")
}

func TestRecoveryParentInspectionInfrastructureFailureRemainsRetryable(t *testing.T) {
	continuation := &capturingFinalProvider{log: &recordLog{}, name: "must-not-run"}
	selection := domain.ModelSelection{Profile: "provider-a", Model: "model-a"}
	service, request, repository, _, _ := recoveredParentFailureFixture(t, selection, continuation)
	service.deps.ParentSessions = &failingRecoveryParentInspector{selection: selection}

	if _, err := service.RecoverTurn(context.Background(), request); err == nil || !strings.Contains(err.Error(), "parent inspection fault") {
		t.Fatalf("infrastructure failure=%v", err)
	}
	for _, batch := range flattenBatches(repository.appendRequests()) {
		if slices.Contains(batch, protocol.EventTurnInterrupted) || slices.Contains(batch, protocol.EventRecoveryDiagnostic) {
			t.Fatalf("infrastructure failure was converted to a parent terminal: %v", batch)
		}
	}
}

func TestRecoveryChildInspectionInfrastructureFailureRemainsRetryable(t *testing.T) {
	continuation := &capturingFinalProvider{log: &recordLog{}, name: "must-not-run"}
	service, request, repository, children, currentManifest := recoveredParentFailureFixture(t, domain.ModelSelection{Profile: "provider-a", Model: "model-a"}, continuation)
	previousManifest := protocol.DeepCopy(currentManifest)
	previousManifest.AttemptID = "previous-attempt"
	previousManifest.ChildSessionID = "previous-child"
	previousManifest.ChildTaskID = "previous-child-task"
	previousManifest.ChildTurnID = "previous-child-turn"
	previousManifest.ParentCursor = protocol.CommittedCursor{JournalKind: request.Storage.Journal.Kind, JournalID: request.Storage.Journal.ID, CommitSeq: 4, TransactionID: "previous-parent-provider"}
	previousReceipt := childReceipt(previousManifest, "succeeded", "previous child completed", protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(previousManifest.ChildSessionID), CommitSeq: 2, TransactionID: "previous-child-terminal"}, unknownUsage(), nil, nil)
	previousDigest, err := canonicaljson.Digest(previousReceipt)
	if err != nil {
		t.Fatal(err)
	}
	currentEvents := repository.events[request.Storage.Journal]
	previousActivityID := protocol.ActivityID("previous-subagent-activity")
	repository.events[request.Storage.Journal] = append(append(append([]protocol.ProposedEvent{}, currentEvents[:5]...),
		protocol.ProposedEvent{EventID: "previous-request", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventSubagentRequested, SessionID: "session-a", TaskID: "task-original", TurnID: "turn-original", ActivityID: previousActivityID, RuntimeGenerationID: request.Control.Runtime.ID, Payload: mustCanonical(protocol.SubagentRequestedV1{Call: protocol.SubagentCallV1{Task: "previous child task"}, Manifest: previousManifest})},
		protocol.ProposedEvent{EventID: "previous-wait", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventSubagentWaiting, SessionID: "session-a", TaskID: "task-original", TurnID: "turn-original", ActivityID: previousActivityID, RuntimeGenerationID: request.Control.Runtime.ID, Payload: mustCanonical(protocol.SubagentWaitingV1{AttemptID: previousManifest.AttemptID, ChildSessionID: previousManifest.ChildSessionID})},
		protocol.ProposedEvent{EventID: "previous-attachment", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventSubagentResultAttached, SessionID: "session-a", TaskID: "task-original", TurnID: "turn-original", ActivityID: previousActivityID, RuntimeGenerationID: request.Control.Runtime.ID, Payload: mustCanonical(protocol.SubagentResultAttachedV1{AttemptID: previousManifest.AttemptID, ChildSessionID: previousManifest.ChildSessionID, TerminalCursor: previousReceipt.TerminalCursor, ReceiptDigest: previousDigest, ReceiptEvidenceID: "previous-evidence"})},
	), currentEvents[5:]...)
	recoveredHead := protocol.CommittedCursor{JournalKind: request.Storage.Journal.Kind, JournalID: request.Storage.Journal.ID, CommitSeq: 10, TransactionID: "recovered-multi-attempt-parent"}
	repository.recovered, repository.result.Cursor, repository.heads[request.Storage.Journal] = recoveredHead, recoveredHead, recoveredHead
	inspectionFault := errors.New("child inspection transient fault")
	inspector := &scriptedRecoveryChildInspector{
		restartRecoveryChildStore: children,
		inspections: map[protocol.SessionID]journal.Inspection{
			previousManifest.ChildSessionID: childRecoveryInspection(previousManifest, previousReceipt),
		},
		errors: map[protocol.SessionID]error{currentManifest.ChildSessionID: inspectionFault},
	}
	service.deps.ChildSessions = inspector

	if _, err := service.RecoverTurn(context.Background(), request); !errors.Is(err, inspectionFault) {
		t.Fatalf("infrastructure failure=%v want wrapped %v", err, inspectionFault)
	}
	for _, appendRequest := range repository.appendRequests() {
		if appendRequest.Journal != request.Storage.Journal {
			continue
		}
		if len(appendRequest.Events) != 0 {
			t.Fatalf("child inspection infrastructure failure changed parent journal: %v", flattenAppendKinds([]journal.AppendRequest{appendRequest}))
		}
	}
	projection, err := service.deps.Projection.InspectRecovery(context.Background(), request.Storage.Journal, repository.recovered)
	if err != nil {
		t.Fatal(err)
	}
	if projection.ActiveTurnID != "turn-original" || continuation.request.ModelID != "" {
		t.Fatalf("parent active turn=%q continuation=%+v", projection.ActiveTurnID, continuation.request)
	}
	if !slices.Equal(inspector.calls, []protocol.SessionID{previousManifest.ChildSessionID, currentManifest.ChildSessionID}) {
		t.Fatalf("multi-attempt inspection preflight order=%v", inspector.calls)
	}
}

type scriptedRecoveryChildInspector struct {
	*restartRecoveryChildStore
	inspections map[protocol.SessionID]journal.Inspection
	errors      map[protocol.SessionID]error
	calls       []protocol.SessionID
}

func (s *scriptedRecoveryChildInspector) InspectSession(ctx context.Context, id protocol.SessionID) (journal.Inspection, error) {
	s.calls = append(s.calls, id)
	if err := s.errors[id]; err != nil {
		return journal.Inspection{}, err
	}
	if inspection, ok := s.inspections[id]; ok {
		return protocol.DeepCopy(inspection), nil
	}
	return s.restartRecoveryChildStore.InspectSession(ctx, id)
}

type transientRecoveryChildStore struct {
	*restartRecoveryChildStore
	err error
}

func (s transientRecoveryChildStore) InspectSession(context.Context, protocol.SessionID) (journal.Inspection, error) {
	return journal.Inspection{}, s.err
}

type failingRecoveryParentInspector struct {
	calls     int
	selection domain.ModelSelection
}

func (p *failingRecoveryParentInspector) InspectSession(_ context.Context, id protocol.SessionID) (journal.SessionInspection, error) {
	p.calls++
	if p.calls > 1 {
		return journal.SessionInspection{}, errors.New("parent inspection fault")
	}
	return journal.SessionInspection{Session: domain.Session{ID: string(id), Workspace: domain.Workspace{ID: "workspace", CanonicalPath: "/workspace"}, Mode: domain.ModeAsk, Selection: p.selection}}, nil
}

func TestRecoveryUnhealthyChildInspectionIsUncertainAndNeverResumed(t *testing.T) {
	continuation := &capturingFinalProvider{log: &recordLog{}, name: "must-not-run"}
	service, request, repository, children, manifest := recoveredParentFailureFixture(t, domain.ModelSelection{Profile: "provider-a", Model: "model-a"}, continuation)
	// This mirrors the fail-closed shape returned by the production JSONL
	// inspector when a committed prefix has an incomplete trailing transaction.
	children.inspection.Writable = false
	children.inspection.IncompleteTransaction = "child-incomplete"
	children.inspection.Diagnostics = []protocol.Diagnostic{{
		Code: "recovery.available", Message: "journal has bytes beyond its validated committed prefix",
		Journal: children.inspection.Journal, AtSeq: children.inspection.Head.CommitSeq + 1,
	}}

	result, err := service.RecoverTurn(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.CommandResult.Status != "completed" || continuation.request.ModelID != "" {
		t.Fatalf("recovery control result=%+v continuation=%+v", result, continuation.request)
	}
	assertRecoveredParentFailureTerminalized(t, repository, manifest, "child journal commit is unknown")
}

func recoveredParentFailureFixture(t *testing.T, selection domain.ModelSelection, continuation ProviderService) (*Service, RecoveryControlRequest, *recoveryRepository, *restartRecoveryChildStore, protocol.SubagentManifestV1) {
	t.Helper()
	service, request, repository, recoveryLog := recoveryBarrierFixture(t, NoopBarrierProbe())
	request.Control.Runtime.Body.SkillCatalogRevision = "skills-a"
	request.Control.Runtime.Body.Limits.Subagents = protocol.SubagentLimits{Enabled: true, MaxPerTurn: 4, MaxToolCalls: 1, TimeoutNanos: int64(time.Minute)}
	refreshRuntimeDigest(t, &request.Control.Runtime)
	parentRef := request.Storage.Journal
	manifest := protocol.SubagentManifestV1{
		AttemptID: "restart-attempt", ParentSessionID: "session-a",
		ParentCursor:   protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: parentRef.ID, CommitSeq: 1, TransactionID: "parent-provider"},
		ChildSessionID: "child-reserved", ChildTaskID: "child-task", ChildTurnID: "child-turn",
		RuntimeGenerationID: request.Control.Runtime.ID, SkillCatalogRevision: request.Control.Runtime.Body.SkillCatalogRevision,
		MaxToolCalls: 1, Deadline: time.Now().Add(time.Minute),
	}
	intent := protocol.ToolUseBlock{CallID: "delegate", Alias: "subagent", Arguments: json.RawMessage(`{"task":"durable child task"}`)}
	activityID := protocol.ActivityID(stableID("activity", "command-original", "subagent", intent.CallID))
	recoveredHead := protocol.CommittedCursor{JournalKind: parentRef.Kind, JournalID: parentRef.ID, CommitSeq: 7, TransactionID: "recovered-parent-wait"}
	repository.recovered, repository.result.Cursor, repository.heads[parentRef] = recoveredHead, recoveredHead, recoveredHead
	repository.events[parentRef] = []protocol.ProposedEvent{
		{EventID: "command-original", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventCommandAccepted, SessionID: "session-a", TurnID: "turn-original", Actor: &protocol.ActorRef{ID: "user", Kind: protocol.ActorUser}, RuntimeGenerationID: request.Control.Runtime.ID, Payload: mustCanonical(protocol.CommandAcceptedV1{CommandID: "command-original", RequestDigest: repeatedDigest("5"), IdempotencyKey: "parent-key"})},
		{EventID: "turn-accepted", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventTurnAccepted, SessionID: "session-a", TaskID: "task-original", TurnID: "turn-original", RuntimeGenerationID: request.Control.Runtime.ID, Payload: mustCanonical(protocol.TurnAcceptedV1{CommandID: "command-original", Goal: "durable parent goal"})},
		{EventID: "assistant-tool-use", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventAssistantMessage, SessionID: "session-a", TaskID: "task-original", TurnID: "turn-original", RuntimeGenerationID: request.Control.Runtime.ID, Payload: mustCanonical(protocol.AssistantMessageV1{ToolIntents: []protocol.ToolUseBlock{intent}})},
		{EventID: "provider-terminal", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventProviderAttemptTerminal, SessionID: "session-a", TaskID: "task-original", TurnID: "turn-original", RuntimeGenerationID: request.Control.Runtime.ID, Payload: mustCanonical(protocol.ProviderAttemptTerminalV1{Status: "completed"})},
		{EventID: "parent-activity", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventActivityStarted, SessionID: "session-a", TaskID: "task-original", TurnID: "turn-original", ActivityID: activityID, RuntimeGenerationID: request.Control.Runtime.ID, Payload: mustCanonical(protocol.ActivityOutcomeV1{Status: "started"})},
		{EventID: "subagent-request", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventSubagentRequested, SessionID: "session-a", TaskID: "task-original", TurnID: "turn-original", ActivityID: activityID, RuntimeGenerationID: request.Control.Runtime.ID, Payload: mustCanonical(protocol.SubagentRequestedV1{Call: protocol.SubagentCallV1{Task: "durable child task"}, Manifest: manifest})},
		{EventID: "subagent-wait", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventSubagentWaiting, SessionID: "session-a", TaskID: "task-original", TurnID: "turn-original", ActivityID: activityID, RuntimeGenerationID: request.Control.Runtime.ID, Payload: mustCanonical(protocol.SubagentWaitingV1{AttemptID: manifest.AttemptID, ChildSessionID: manifest.ChildSessionID})},
	}
	receipt := childReceipt(manifest, "succeeded", "child completed", protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(manifest.ChildSessionID), CommitSeq: 2, TransactionID: "child-terminal"}, unknownUsage(), nil, nil)
	children := &restartRecoveryChildStore{
		workspace: domain.Workspace{ID: "workspace", CanonicalPath: "/workspace"}, selection: selection,
		created: manifest.ChildSessionID, log: recoveryLog,
		inspection: journal.Inspection{
			Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(manifest.ChildSessionID)}, Head: receipt.TerminalCursor, Writable: true,
			Events: []protocol.EventRecord{
				{Envelope: protocol.EventEnvelope{SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1, JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(manifest.ChildSessionID), SessionID: manifest.ChildSessionID, EventID: "child-manifest", Seq: 1, Time: time.Now().UTC(), Kind: protocol.EventSubagentManifest, TaskID: manifest.ChildTaskID, TurnID: manifest.ChildTurnID, TransactionID: "child-manifest", RuntimeGenerationID: manifest.RuntimeGenerationID, Payload: mustCanonical(manifest)}},
				{Envelope: protocol.EventEnvelope{SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1, JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(manifest.ChildSessionID), SessionID: manifest.ChildSessionID, EventID: "child-receipt", Seq: receipt.TerminalCursor.CommitSeq, Time: time.Now().UTC(), Kind: protocol.EventSubagentReceipt, TaskID: manifest.ChildTaskID, TurnID: manifest.ChildTurnID, TransactionID: receipt.TerminalCursor.TransactionID, RuntimeGenerationID: manifest.RuntimeGenerationID, Payload: mustCanonical(receipt)}},
			},
		},
	}
	coordinator, err := NewSequentialChildCoordinator(children, restartRecoveryParent{children}, func(context.Context, StartTurnRequest) (RunResult, error) {
		return RunResult{}, errors.New("recovered terminal child must not run again")
	})
	if err != nil {
		t.Fatal(err)
	}
	service.deps.ChildSessions, service.deps.ParentSessions, service.deps.Children = children, restartRecoveryParent{children}, coordinator
	service.deps.Evidence = recordingEvidence{log: recoveryLog}
	service.deps.Context, service.deps.Providers, service.deps.Provider = fakeContextPlanner{log: &recordLog{}}, fakeProviderCatalog{log: &recordLog{}}, continuation
	service.deps.Instructions, service.deps.Verification = emptyInstructionService{}, verification.NewService(time.Now)
	service.deps.Projection = terminalAwareRecoveryProjection{
		repository: repository,
		active:     RecoveryProjection{ActiveTurnID: "turn-original", TaskID: "task-original", OriginalCommandID: "command-original", OriginalRequestDigest: repeatedDigest("5"), StartedActivities: []protocol.ActivityID{activityID}},
	}
	return service, request, repository, children, manifest
}

func assertRecoveredParentFailureTerminalized(t *testing.T, repository *recoveryRepository, manifest protocol.SubagentManifestV1, reason string) {
	t.Helper()
	var diagnostic protocol.DiagnosticV1
	var completed protocol.CommandCompletedV1
	turnInterrupted := 0
	for _, request := range repository.appendRequests() {
		if request.Journal != repository.sessionRef {
			continue
		}
		for _, event := range request.Events {
			switch event.Kind {
			case protocol.EventTurnInterrupted:
				turnInterrupted++
			case protocol.EventRecoveryDiagnostic:
				if err := json.Unmarshal(event.Payload, &diagnostic); err != nil {
					t.Fatal(err)
				}
			case protocol.EventCommandCompleted:
				var candidate protocol.CommandCompletedV1
				if err := json.Unmarshal(event.Payload, &candidate); err != nil {
					t.Fatal(err)
				}
				if candidate.CommandID == "command-original" {
					completed = candidate
				}
			}
		}
	}
	var details SubagentRecoveryDiagnostic
	if err := json.Unmarshal(diagnostic.Diagnostic.Details, &details); err != nil {
		t.Fatal(err)
	}
	wantMessage := "sequential child recovery state is uncertain: " + reason
	if turnInterrupted != 1 || diagnostic.Diagnostic.Code != "subagent.recovery_uncertain" || details.AttemptID != manifest.AttemptID || details.ChildSessionID != manifest.ChildSessionID || details.ChildStatus != "uncertain" || details.TerminalCursor.Validate() != nil || details.TerminalCursor.JournalID != protocol.JournalID(manifest.ChildSessionID) || details.ReceiptDigest.Validate() != nil || details.Reason != reason || completed.Error == nil || completed.Error.Code != "subagent_recovery_uncertain" || completed.Error.Message != wantMessage || completed.Error.Retryable {
		t.Fatalf("turn_interrupted=%d diagnostic=%+v details=%+v completed=%+v", turnInterrupted, diagnostic, details, completed)
	}
}
