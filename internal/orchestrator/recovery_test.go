package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

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
		"turn_lease.release", "append_control(control_operation.completed,command.completed)", "lane.release",
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
}

func newRecoveryRepository(log *recordLog, controlRef protocol.JournalRef, controlHead protocol.CommittedCursor, sessionRef protocol.JournalRef, sessionHead, recovered protocol.CommittedCursor) *recoveryRepository {
	return &recoveryRepository{log: log, controlRef: controlRef, sessionRef: sessionRef, heads: map[protocol.JournalRef]protocol.CommittedCursor{controlRef: controlHead, sessionRef: sessionHead}, events: make(map[protocol.JournalRef][]protocol.ProposedEvent), recovered: recovered}
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
	records := make([]protocol.EventRecord, len(r.events[request.Journal]))
	for index, event := range r.events[request.Journal] {
		records[index] = protocol.EventRecord{Envelope: protocol.EventEnvelope{JournalKind: request.Journal.Kind, JournalID: request.Journal.ID, SessionID: event.SessionID, Kind: event.Kind, Payload: event.Payload}}
	}
	return journal.EventPage{Events: records, Head: r.heads[request.Journal], Cursor: r.heads[request.Journal]}, nil
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
	r.heads[request.Journal] = r.recovered
	return journal.RecoveryResult{Status: "recovered", Cursor: r.recovered}, nil
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
