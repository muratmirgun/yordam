package orchestrator

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/verification"
)

var errInjectedCommitUnknown = errors.New("append returned commit_unknown")

func TestCommitUnknownResolutionAdoptsProvenCommitForEveryCaller(t *testing.T) {
	t.Run("turn", func(t *testing.T) {
		request := validStartTurnRequest()
		request.Runtime = validRuntimeManifest(t, "observation")
		base := &recordingRepository{head: request.ExpectedHead}
		repository := newCommitUnknownRepository(base, true)
		providerService := &countingProviderService{delegate: fakeProviderService{log: &recordLog{}}}
		publisher := &countingCommittedPublisher{}
		service, err := NewService(Dependencies{
			Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repository,
			TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: &recordLog{}}, Providers: fakeProviderCatalog{log: &recordLog{}},
			Provider: providerService, Tools: noToolService{}, Authorization: &allowingAuthorization{log: &recordLog{}},
			Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now), Publisher: publisher,
		})
		if err != nil {
			t.Fatal(err)
		}
		result, err := service.RunTurn(t.Context(), request)
		if err != nil || result.Status != "completed" {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		if got := providerService.streams.Load(); got != 1 {
			t.Fatalf("provider streams=%d want=1", got)
		}
		assertEveryUnknownCommitWasResolvedAndPublished(t, repository, publisher)
	})

	t.Run("pure", func(t *testing.T) {
		request := validStartTurnRequest()
		base := &recordingRepository{head: request.ExpectedHead}
		repository := newCommitUnknownRepository(base, true)
		publisher := &countingCommittedPublisher{}
		service, err := NewService(Dependencies{Lane: NewOperationLane(), Repository: repository, Publisher: publisher})
		if err != nil {
			t.Fatal(err)
		}
		result, err := service.CommitPureCommand(t.Context(), request.Command, PureCommandCompletion{
			Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)}, ExpectedHead: request.ExpectedHead,
			PayloadVersion: 1, Payload: []byte(`{"snapshot":true}`),
		})
		if err != nil || result.Status != "completed" {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		assertEveryUnknownCommitWasResolvedAndPublished(t, repository, publisher)
	})

	t.Run("pure session title", func(t *testing.T) {
		request := validStartTurnRequest()
		base := &recordingRepository{head: request.ExpectedHead}
		repository := newCommitUnknownRepository(base, true)
		publisher := &countingCommittedPublisher{}
		service, err := NewService(Dependencies{Lane: NewOperationLane(), Repository: repository, Admission: passthroughAdmission{}, Publisher: publisher})
		if err != nil {
			t.Fatal(err)
		}
		change := SessionChangeRequest{
			Command: request.Command, OperationID: "title-operation", Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)},
			SessionID: request.SessionID, ExpectedHead: request.ExpectedHead, TransactionID: "title-transaction", RuntimeGenerationID: "generation-a",
			Event: proposedSessionEvent(protocol.EventSessionTitleChanged, request.SessionID, protocol.SessionTitleChangedV1{Title: "renamed"}),
		}
		result, err := service.CommitSessionChange(t.Context(), change)
		if err != nil || result.Status != "completed" {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		assertEveryUnknownCommitWasResolvedAndPublished(t, repository, publisher)
	})

	t.Run("control", func(t *testing.T) {
		request := validControlRequestForReview(t)
		base := &recordingRepository{head: request.ExpectedHead}
		repository := newCommitUnknownRepository(base, true)
		publisher := &countingCommittedPublisher{}
		authorizer := &allowingAuthorization{log: &recordLog{}}
		service, err := NewService(Dependencies{
			Lane: NewOperationLane(), Repository: repository, Authorization: authorizer, Admission: passthroughAdmission{}, Publisher: publisher,
		})
		if err != nil {
			t.Fatal(err)
		}
		result, err := service.RunControl(t.Context(), request)
		if err != nil || result.Status != "completed" {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		if got := authorizer.dispatches.Load(); got != 1 {
			t.Fatalf("control dispatches=%d want=1", got)
		}
		assertEveryUnknownCommitWasResolvedAndPublished(t, repository, publisher)
	})
}

func TestCommitUnknownResolutionReturnsTypedUncertaintyWithoutResend(t *testing.T) {
	request := validStartTurnRequest()
	base := &recordingRepository{head: request.ExpectedHead}
	repository := newCommitUnknownRepository(base, false)
	service, err := NewService(Dependencies{Lane: NewOperationLane(), Repository: repository})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.CommitPureCommand(t.Context(), request.Command, PureCommandCompletion{
		Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)}, ExpectedHead: request.ExpectedHead,
		PayloadVersion: 1, Payload: []byte(`{"snapshot":true}`),
	})
	var uncertain interface{ UncertainCommit() }
	if !errors.As(err, &uncertain) {
		t.Fatalf("error=%T %v, want typed commit uncertainty", err, err)
	}
	if got := repository.appends.Load(); got != 1 {
		t.Fatalf("append attempts=%d want=1", got)
	}
	if got := repository.lookups.Load(); got != 1 {
		t.Fatalf("transaction lookups=%d want=1", got)
	}
}

func assertEveryUnknownCommitWasResolvedAndPublished(t *testing.T, repository *commitUnknownRepository, publisher *countingCommittedPublisher) {
	t.Helper()
	if repository.appends.Load() == 0 || repository.lookups.Load() != repository.appends.Load() || repository.reads.Load() != repository.appends.Load() {
		t.Fatalf("appends=%d lookups=%d reads=%d", repository.appends.Load(), repository.lookups.Load(), repository.reads.Load())
	}
	if publisher.calls.Load() != repository.appends.Load() {
		t.Fatalf("published=%d appends=%d", publisher.calls.Load(), repository.appends.Load())
	}
}

type countingCommittedPublisher struct{ calls atomic.Int64 }

func (p *countingCommittedPublisher) PublishCommitted(context.Context, protocol.JournalRef, protocol.CommittedCursor, []protocol.EventEnvelope) error {
	p.calls.Add(1)
	return nil
}

type commitUnknownRepository struct {
	*recordingRepository
	proveCommitted bool
	mu             sync.Mutex
	committed      map[protocol.TransactionID]journal.CommittedTransaction
	appends        atomic.Int64
	lookups        atomic.Int64
	reads          atomic.Int64
}

func newCommitUnknownRepository(base *recordingRepository, proveCommitted bool) *commitUnknownRepository {
	return &commitUnknownRepository{recordingRepository: base, proveCommitted: proveCommitted, committed: make(map[protocol.TransactionID]journal.CommittedTransaction)}
}

func (r *commitUnknownRepository) AppendBatch(ctx context.Context, request journal.AppendRequest) (journal.AppendResult, error) {
	r.appends.Add(1)
	if !r.proveCommitted {
		return journal.AppendResult{Status: journal.AppendCommitUnknown, CurrentHead: request.ExpectedHead}, errInjectedCommitUnknown
	}
	result, err := r.recordingRepository.AppendBatch(ctx, request)
	if err != nil || result.Status != journal.AppendCommitted {
		return result, err
	}
	events := make([]protocol.EventEnvelope, len(request.Events))
	for index, proposed := range request.Events {
		events[index] = protocol.EventEnvelope{
			SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: proposed.PayloadVersion, JournalKind: request.Journal.Kind, JournalID: request.Journal.ID,
			SessionID: proposed.SessionID, TaskID: proposed.TaskID, TurnID: proposed.TurnID, ActivityID: proposed.ActivityID, ParentActivityID: proposed.ParentActivityID,
			EventID: proposed.EventID, Seq: request.ExpectedHead.CommitSeq + uint64(index), Time: proposed.Time, Kind: proposed.Kind,
			TransactionID: request.TransactionID, Actor: protocol.DeepCopy(proposed.Actor), RuntimeGenerationID: proposed.RuntimeGenerationID, Payload: protocol.CloneRawMessage(proposed.Payload),
		}
	}
	r.mu.Lock()
	r.committed[request.TransactionID] = journal.CommittedTransaction{Journal: request.Journal, TransactionID: request.TransactionID, Cursor: result.Cursor, Events: events}
	r.mu.Unlock()
	return journal.AppendResult{Status: journal.AppendCommitUnknown, CurrentHead: request.ExpectedHead}, errInjectedCommitUnknown
}

func (r *commitUnknownRepository) LookupTransaction(_ context.Context, ref protocol.JournalRef, transactionID protocol.TransactionID) (journal.TransactionLookup, error) {
	r.lookups.Add(1)
	if !r.proveCommitted {
		return journal.TransactionLookup{State: journal.TransactionUnknown}, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	committed, ok := r.committed[transactionID]
	if !ok || committed.Journal != ref {
		return journal.TransactionLookup{State: journal.TransactionNotCommitted}, nil
	}
	return journal.TransactionLookup{State: journal.TransactionCommitted, Cursor: committed.Cursor}, nil
}

func (r *commitUnknownRepository) ReadCommittedTransaction(_ context.Context, ref protocol.JournalRef, transactionID protocol.TransactionID) (journal.CommittedTransaction, error) {
	r.reads.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	committed, ok := r.committed[transactionID]
	if !ok || committed.Journal != ref {
		return journal.CommittedTransaction{}, errors.New("committed transaction is unavailable")
	}
	return protocol.DeepCopy(committed), nil
}

var _ journal.Repository = (*commitUnknownRepository)(nil)
