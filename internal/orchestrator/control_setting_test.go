package orchestrator

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/tooling"
)

func TestRunControlCommitsSessionSettingOnlyInsideAuthorizedDispatch(t *testing.T) {
	request := validSessionSettingControlRequest(t)
	repository := newMultiJournalRepository(request.Journal, request.ExpectedHead, request.ConsequentialJournal, request.ConsequentialExpectedHead)
	authorizer := &settingDispatchAuthorization{allowingAuthorization: allowingAuthorization{log: &recordLog{}}, repository: repository, settingKind: request.Event.Kind}
	service, err := NewService(Dependencies{Lane: NewOperationLane(), Repository: repository, Authorization: authorizer, Admission: passthroughAdmission{}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.RunControl(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || !authorizer.observed.Load() {
		t.Fatalf("result=%+v observed=%v", result, authorizer.observed.Load())
	}
	if result.CommandResult.Cursor.WorkspaceControl != repository.head(request.Journal) || result.CommandResult.Cursor.SelectedSession == nil || *result.CommandResult.Cursor.SelectedSession != repository.head(request.ConsequentialJournal) {
		t.Fatalf("result cursor=%+v workspace=%+v session=%+v", result.CommandResult.Cursor, repository.head(request.Journal), repository.head(request.ConsequentialJournal))
	}
	if got := repository.kinds(request.ConsequentialJournal); len(got) != 1 || got[0] != protocol.EventModeChanged {
		t.Fatalf("session events=%v", got)
	}
	for _, required := range []string{
		protocol.EventControlOperationPlanned, protocol.EventAuthorizationRequested, protocol.EventAuthorizationDecided,
		protocol.EventControlOperationAuthorized, protocol.EventAuthorizationDecisionConsumed, protocol.EventControlOperationStarted,
		protocol.EventControlOperationCompleted, protocol.EventCommandCompleted,
	} {
		if countKind(repository.kinds(request.Journal), required) != 1 {
			t.Fatalf("workspace lifecycle=%v missing %s", repository.kinds(request.Journal), required)
		}
	}
}

func TestRunControlCancelledSettingDispatchDoesNotCommitSessionSetting(t *testing.T) {
	request := validSessionSettingControlRequest(t)
	repository := newMultiJournalRepository(request.Journal, request.ExpectedHead, request.ConsequentialJournal, request.ConsequentialExpectedHead)
	service, err := NewService(Dependencies{
		Lane: NewOperationLane(), Repository: repository,
		Authorization: &cancelDispatchAuthorization{allowingAuthorization: allowingAuthorization{log: &recordLog{}}}, Admission: passthroughAdmission{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunControl(t.Context(), request); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if got := repository.kinds(request.ConsequentialJournal); len(got) != 0 {
		t.Fatalf("cancelled control committed session setting: %v", got)
	}
}

func TestRunControlSettingTerminalFailureDoesNotRepeatCommittedSetting(t *testing.T) {
	request := validSessionSettingControlRequest(t)
	base := newMultiJournalRepository(request.Journal, request.ExpectedHead, request.ConsequentialJournal, request.ConsequentialExpectedHead)
	repository := &failControlTerminalOnceRepository{multiJournalRepository: base}
	authorizer := &allowingAuthorization{log: &recordLog{}}
	service, err := NewService(Dependencies{Lane: NewOperationLane(), Repository: repository, Authorization: authorizer, Admission: passthroughAdmission{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunControl(t.Context(), request); !errors.Is(err, errInjectedControlTerminal) {
		t.Fatalf("first error=%v", err)
	}
	result, err := service.RunControl(t.Context(), request)
	if err != nil || result.Status != "failed" || result.CommandResult.Status != "failed" {
		t.Fatalf("duplicate result=%+v err=%v", result, err)
	}
	if got := countKind(repository.kinds(request.ConsequentialJournal), request.Event.Kind); got != 1 {
		t.Fatalf("setting commits=%d events=%v", got, repository.kinds(request.ConsequentialJournal))
	}
	if got := authorizer.dispatches.Load(); got != 1 {
		t.Fatalf("control dispatches=%d want=1", got)
	}
	workspaceKinds := repository.kinds(request.Journal)
	if countKind(workspaceKinds, protocol.EventControlOperationFailed) != 1 || countKind(workspaceKinds, protocol.EventCommandCompleted) != 1 {
		t.Fatalf("terminal recovery lifecycle=%v", workspaceKinds)
	}
}

func TestCommitSessionChangeAllowsOnlyPureTitleMetadata(t *testing.T) {
	request := validStartTurnRequest()
	service, err := NewService(Dependencies{
		Lane: NewOperationLane(), Repository: &recordingRepository{head: request.ExpectedHead}, Admission: passthroughAdmission{},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		kind    string
		payload any
	}{
		{protocol.EventModeChanged, protocol.ModeChangedV1{Mode: "safe"}},
		{protocol.EventModelChanged, protocol.ModelChangedV1{ProviderID: "provider-a", ModelID: "model-a"}},
		{protocol.EventTrustedExecutionAcknowledged, protocol.TrustedExecutionAcknowledgedV1{Enabled: true, Profile: "unsandboxed"}},
	} {
		change := SessionChangeRequest{
			Command: request.Command, OperationID: "operation-" + protocol.ControlOperationID(test.kind),
			Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)}, SessionID: request.SessionID,
			ExpectedHead: request.ExpectedHead, TransactionID: protocol.TransactionID("transaction-" + test.kind), RuntimeGenerationID: "generation-a", Consequential: true,
			Event: proposedSessionEvent(test.kind, request.SessionID, test.payload),
		}
		if _, err := service.CommitSessionChange(t.Context(), change); err == nil {
			t.Fatalf("consequential %s used CommitSessionChange", test.kind)
		}
	}
}

func validSessionSettingControlRequest(t *testing.T) ControlRequest {
	t.Helper()
	runtime := validRuntimeManifest(t, "observation")
	workspace := protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace-control-settings"}
	workspaceHead := protocol.CommittedCursor{JournalKind: workspace.Kind, JournalID: workspace.ID, CommitSeq: 1, TransactionID: "workspace-head"}
	session := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session-settings"}
	sessionHead := protocol.CommittedCursor{JournalKind: session.Kind, JournalID: session.ID, CommitSeq: 1, TransactionID: "session-head"}
	actor := protocol.ActorRef{ID: "user-settings", Kind: protocol.ActorUser}
	plan := testActionPlan(tooling.PlanRequest{CallID: "setting-mode", Alias: "mode", RuntimeGenerationID: runtime.ID}, "mutation", "exact")
	return ControlRequest{
		Command:     CommandMetadata{CommandID: "command-setting", IdempotencyKey: "key-setting", RequestDigest: repeatedDigest("6"), Actor: actor},
		OperationID: "operation-setting", Kind: OperationControl, Journal: workspace, ExpectedHead: workspaceHead, TransactionID: "workspace-setting-terminal",
		ConsequentialJournal: session, ConsequentialExpectedHead: sessionHead,
		Runtime: runtime, Plan: plan,
		Event: proposedSessionEvent(protocol.EventModeChanged, protocol.SessionID(session.ID), protocol.ModeChangedV1{Mode: "safe"}),
	}
}

type settingDispatchAuthorization struct {
	allowingAuthorization
	repository  *multiJournalRepository
	settingKind string
	observed    atomic.Bool
}

func (a *settingDispatchAuthorization) Dispatch(ctx context.Context, _ authorization.CommittedToken, _ authorization.DispatchBinding, callback func(context.Context) error) error {
	before := countKind(a.repository.kindsByObserver(a.settingKind), a.settingKind)
	err := callback(ctx)
	after := countKind(a.repository.kindsByObserver(a.settingKind), a.settingKind)
	if err == nil && before == 0 && after == 1 {
		a.observed.Store(true)
	}
	return err
}

type multiJournalRepository struct {
	mu           sync.Mutex
	heads        map[protocol.JournalRef]protocol.CommittedCursor
	records      map[protocol.JournalRef][]protocol.EventRecord
	transactions map[protocol.TransactionID]journal.CommittedTransaction
	observer     []string
}

func newMultiJournalRepository(refA protocol.JournalRef, headA protocol.CommittedCursor, refB protocol.JournalRef, headB protocol.CommittedCursor) *multiJournalRepository {
	return &multiJournalRepository{
		heads: map[protocol.JournalRef]protocol.CommittedCursor{refA: headA, refB: headB}, records: make(map[protocol.JournalRef][]protocol.EventRecord),
		transactions: make(map[protocol.TransactionID]journal.CommittedTransaction),
	}
}

func (r *multiJournalRepository) Inspect(_ context.Context, ref protocol.JournalRef) (journal.Inspection, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return journal.Inspection{Journal: ref, Head: r.heads[ref], Events: protocol.DeepCopy(r.records[ref]), Writable: true}, nil
}
func (r *multiJournalRepository) Head(_ context.Context, ref protocol.JournalRef) (protocol.CommittedCursor, error) {
	return r.head(ref), nil
}
func (r *multiJournalRepository) head(ref protocol.JournalRef) protocol.CommittedCursor {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.heads[ref]
}
func (r *multiJournalRepository) ReadRange(_ context.Context, request journal.ReadRangeRequest) (journal.EventPage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	all := r.records[request.Journal]
	events := make([]protocol.EventRecord, 0, len(all))
	for _, record := range all {
		if record.Envelope.Seq >= request.After.CommitSeq {
			events = append(events, protocol.CloneEventRecord(record))
		}
	}
	return journal.EventPage{Events: events, Cursor: r.heads[request.Journal], Head: r.heads[request.Journal]}, nil
}
func (r *multiJournalRepository) AppendBatch(_ context.Context, request journal.AppendRequest) (journal.AppendResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.heads[request.Journal] != request.ExpectedHead {
		return journal.AppendResult{Status: journal.AppendConflict, CurrentHead: r.heads[request.Journal]}, nil
	}
	events := make([]protocol.EventEnvelope, len(request.Events))
	for index, proposed := range request.Events {
		envelope := protocol.EventEnvelope{
			SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: proposed.PayloadVersion, JournalKind: request.Journal.Kind, JournalID: request.Journal.ID,
			SessionID: proposed.SessionID, EventID: proposed.EventID, Seq: request.ExpectedHead.CommitSeq + uint64(index), Time: proposed.Time, Kind: proposed.Kind,
			TransactionID: request.TransactionID, Actor: protocol.DeepCopy(proposed.Actor), RuntimeGenerationID: proposed.RuntimeGenerationID, Payload: protocol.CloneRawMessage(proposed.Payload),
		}
		events[index] = envelope
		r.records[request.Journal] = append(r.records[request.Journal], protocol.EventRecord{Envelope: envelope})
		r.observer = append(r.observer, proposed.Kind)
	}
	cursor := protocol.CommittedCursor{JournalKind: request.Journal.Kind, JournalID: request.Journal.ID, CommitSeq: request.ExpectedHead.CommitSeq + uint64(len(events)) + 1, TransactionID: request.TransactionID}
	r.heads[request.Journal] = cursor
	r.transactions[request.TransactionID] = journal.CommittedTransaction{Journal: request.Journal, TransactionID: request.TransactionID, Cursor: cursor, Events: events}
	return journal.AppendResult{Status: journal.AppendCommitted, Cursor: cursor, Events: protocol.DeepCopy(events)}, nil
}
func (r *multiJournalRepository) LookupTransaction(_ context.Context, ref protocol.JournalRef, transactionID protocol.TransactionID) (journal.TransactionLookup, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	transaction, ok := r.transactions[transactionID]
	if !ok || transaction.Journal != ref {
		return journal.TransactionLookup{State: journal.TransactionNotCommitted}, nil
	}
	return journal.TransactionLookup{State: journal.TransactionCommitted, Cursor: transaction.Cursor}, nil
}
func (r *multiJournalRepository) ReadCommittedTransaction(_ context.Context, ref protocol.JournalRef, transactionID protocol.TransactionID) (journal.CommittedTransaction, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	transaction, ok := r.transactions[transactionID]
	if !ok || transaction.Journal != ref {
		return journal.CommittedTransaction{}, errors.New("transaction not committed")
	}
	return protocol.DeepCopy(transaction), nil
}
func (r *multiJournalRepository) Recover(context.Context, journal.RecoveryRequest) (journal.RecoveryResult, error) {
	return journal.RecoveryResult{}, errors.New("unexpected recovery")
}
func (r *multiJournalRepository) kinds(ref protocol.JournalRef) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]string, len(r.records[ref]))
	for index, record := range r.records[ref] {
		result[index] = record.Envelope.Kind
	}
	return result
}
func (r *multiJournalRepository) kindsByObserver(string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.observer...)
}

var _ journal.Repository = (*multiJournalRepository)(nil)

var errInjectedControlTerminal = errors.New("control terminal append failed after consequence")

type failControlTerminalOnceRepository struct {
	*multiJournalRepository
	failed atomic.Bool
}

func (r *failControlTerminalOnceRepository) AppendBatch(ctx context.Context, request journal.AppendRequest) (journal.AppendResult, error) {
	for _, event := range request.Events {
		if event.Kind == protocol.EventControlOperationCompleted && r.failed.CompareAndSwap(false, true) {
			return journal.AppendResult{Status: journal.AppendRecoveryRequired, CurrentHead: request.ExpectedHead}, errInjectedControlTerminal
		}
	}
	return r.multiJournalRepository.AppendBatch(ctx, request)
}

var _ journal.Repository = (*failControlTerminalOnceRepository)(nil)
