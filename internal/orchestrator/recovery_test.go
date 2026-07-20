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
	"github.com/muratmirgun/yordam/internal/tooling"
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
		records[index] = protocol.EventRecord{Envelope: protocol.EventEnvelope{JournalKind: request.Journal.Kind, JournalID: request.Journal.ID, SessionID: event.SessionID, Kind: event.Kind, Seq: uint64(start + index + 1), TransactionID: txn, Payload: event.Payload}}
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
