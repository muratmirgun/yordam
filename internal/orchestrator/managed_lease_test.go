package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type laneAcquiringApprover struct {
	lane OperationLane
}

func (a laneAcquiringApprover) Approve(ctx context.Context, decision protocol.AuthorizationDecision) (protocol.ApprovalResponse, error) {
	control, err := a.lane.Acquire(ctx, OperationClaim{Kind: OperationControl, SessionID: decision.Request.SessionID, ControlOperationID: "approval-control"})
	if err != nil {
		return protocol.ApprovalResponse{}, err
	}
	control.Release()
	return protocol.ApprovalResponse{Action: "allow", Lifetime: protocol.AuthorizationLifetimeOnce}, nil
}

func TestInteractiveApprovalYieldsTurnLaneForDurableControl(t *testing.T) {
	lane := NewOperationLane()
	turn, err := acquireManagedOperationLease(t.Context(), lane, OperationClaim{Kind: OperationTurn, SessionID: "parent"})
	if err != nil {
		t.Fatal(err)
	}
	defer turn.Release()
	service := &Service{deps: Dependencies{Approver: laneAcquiringApprover{lane: lane}}}
	decision := protocol.AuthorizationDecision{Request: protocol.AuthorizationRequest{SessionID: "parent"}}
	response, err := service.awaitInteractiveApproval(t.Context(), turn, decision)
	if err != nil {
		t.Fatal(err)
	}
	if response.Action != "allow" {
		t.Fatalf("approval action=%q want allow", response.Action)
	}
	if turn.Claim() != (OperationClaim{Kind: OperationTurn, SessionID: "parent"}) {
		t.Fatalf("turn claim was not reacquired: %+v", turn.Claim())
	}
}

type approvalSuffixRepository struct {
	inertRepository
	page journal.EventPage
}

func (r approvalSuffixRepository) ReadRange(context.Context, journal.ReadRangeRequest) (journal.EventPage, error) {
	return r.page, nil
}

func TestInteractiveApprovalReconcilesOnlyExactTrustedShellConsequence(t *testing.T) {
	request := validStartTurnRequest()
	state := newTurnState(request)
	next := protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(request.SessionID), CommitSeq: state.head.CommitSeq + 2, TransactionID: "trusted-shell-control"}
	actor := request.Command.Actor
	payload, err := json.Marshal(protocol.TrustedExecutionAcknowledgedV1{Enabled: true, Profile: "unsandboxed"})
	if err != nil {
		t.Fatal(err)
	}
	event := protocol.EventEnvelope{
		SchemaVersion: protocol.EnvelopeVersion, JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(request.SessionID),
		SessionID: request.SessionID, EventID: "trusted-shell-event", Seq: next.CommitSeq - 1, Time: time.Now().UTC(), PayloadVersion: 1,
		Kind: protocol.EventTrustedExecutionAcknowledged, TransactionID: next.TransactionID, Actor: &actor, RuntimeGenerationID: request.Runtime.ID, Payload: payload,
	}
	markerPayload, err := json.Marshal(protocol.TransactionCommittedV1{TransactionID: next.TransactionID, FirstSeq: event.Seq, LastSeq: event.Seq, EventCount: 1, Digest: repeatedDigest("c")})
	if err != nil {
		t.Fatal(err)
	}
	marker := protocol.EventEnvelope{
		SchemaVersion: protocol.EnvelopeVersion, JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(request.SessionID), SessionID: request.SessionID,
		EventID: "trusted-shell-marker", Seq: next.CommitSeq, Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventTransactionCommitted,
		TransactionID: next.TransactionID, RuntimeGenerationID: request.Runtime.ID, Payload: markerPayload,
	}
	service := &Service{repository: approvalSuffixRepository{page: journal.EventPage{Events: []protocol.EventRecord{{Envelope: event}, {Envelope: marker}}, Cursor: next, Head: next}}}
	if err := service.reconcileInteractiveApprovalJournal(t.Context(), request, &state); err != nil {
		t.Fatal(err)
	}
	if state.head != next {
		t.Fatalf("reconciled head=%+v want=%+v", state.head, next)
	}

	state = newTurnState(request)
	event.Kind = protocol.EventModeChanged
	service.repository = approvalSuffixRepository{page: journal.EventPage{Events: []protocol.EventRecord{{Envelope: event}, {Envelope: marker}}, Cursor: next, Head: next}}
	if err := service.reconcileInteractiveApprovalJournal(t.Context(), request, &state); err == nil {
		t.Fatal("unrelated journal consequence was accepted")
	}
}

func TestManagedOperationLeaseYieldsAndReacquiresTheIdenticalClaim(t *testing.T) {
	lane := NewOperationLane()
	claim := OperationClaim{Kind: OperationTurn, SessionID: "parent"}
	lease, err := acquireManagedOperationLease(context.Background(), lane, claim)
	if err != nil {
		t.Fatal(err)
	}

	blocked, cancel := context.WithCancel(context.Background())
	cancel()
	child, err := lane.Acquire(blocked, OperationClaim{Kind: OperationTurn, SessionID: "child"})
	if err == nil {
		child.Release()
		t.Fatal("child acquired while managed parent was held")
	}

	err = lease.Yield(context.Background(), func(context.Context) error {
		child, err := lane.Acquire(context.Background(), OperationClaim{Kind: OperationTurn, SessionID: "child"})
		if err != nil {
			return err
		}
		child.Release()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := lease.Claim(); got != claim {
		t.Fatalf("reacquired claim=%+v want=%+v", got, claim)
	}
	lease.Release()
}

type failingReacquireLane struct{ calls int }

func (l *failingReacquireLane) Acquire(_ context.Context, claim OperationClaim) (OperationLease, error) {
	l.calls++
	if l.calls > 1 {
		return nil, errReacquire
	}
	return fakeLease{claim: claim}, nil
}

type fakeLease struct{ claim OperationClaim }

func (l fakeLease) Claim() OperationClaim { return l.claim }
func (fakeLease) Release()                {}

var errReacquire = errors.New("reacquire failed")

func TestManagedOperationLeaseJoinsCallbackAndReacquireFailure(t *testing.T) {
	lane := &failingReacquireLane{}
	lease, err := acquireManagedOperationLeaseWithCleanup(context.Background(), lane, OperationClaim{Kind: OperationTurn, SessionID: "parent"}, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	err = lease.Yield(context.Background(), func(context.Context) error { return context.Canceled })
	if !errors.Is(err, context.Canceled) || !errors.Is(err, errReacquire) {
		t.Fatalf("yield error=%v; want joined callback/reacquire failures", err)
	}
}

func TestManagedOperationLeaseReacquiresBeforePanicEscapes(t *testing.T) {
	lane := NewOperationLane()
	lease, err := acquireManagedOperationLease(context.Background(), lane, OperationClaim{Kind: OperationTurn, SessionID: "parent"})
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("panic did not escape callback")
			}
		}()
		_ = lease.Yield(context.Background(), func(context.Context) error { panic("child panic") })
	}()
	lease.Release()
	next, err := lane.Acquire(context.Background(), OperationClaim{Kind: OperationTurn, SessionID: "next"})
	if err != nil {
		t.Fatal(err)
	}
	next.Release()
}

func TestManagedOperationLeasePreservesFIFOAndReacquiresAfterChildTerminal(t *testing.T) {
	lane := NewOperationLane()
	parent, err := acquireManagedOperationLease(context.Background(), lane, OperationClaim{Kind: OperationTurn, SessionID: "parent"})
	if err != nil {
		t.Fatal(err)
	}
	waiterAcquired := make(chan OperationLease, 1)
	go func() {
		lease, acquireErr := lane.Acquire(context.Background(), OperationClaim{Kind: OperationTurn, SessionID: "waiter"})
		if acquireErr == nil {
			waiterAcquired <- lease
		}
	}()
	// Ensure the waiter is queued before the parent yields.
	time.Sleep(time.Millisecond)
	childTerminal := false
	err = parent.Yield(context.Background(), func(context.Context) error {
		select {
		case waiter := <-waiterAcquired:
			if childTerminal {
				t.Fatal("waiter acquired after child terminal marker")
			}
			childTerminal = true
			waiter.Release()
			return nil
		case <-time.After(time.Second):
			return errors.New("FIFO waiter did not acquire yielded lane")
		}
	})
	if err != nil || !childTerminal {
		t.Fatalf("yield err=%v terminal=%v", err, childTerminal)
	}
	parent.Release()
}

func TestManagedOperationLeaseReacquiresAfterCancelledChild(t *testing.T) {
	lane := NewOperationLane()
	claim := OperationClaim{Kind: OperationTurn, SessionID: "parent"}
	lease, err := acquireManagedOperationLease(context.Background(), lane, claim)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	err = lease.Yield(ctx, func(context.Context) error {
		cancel()
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("yield error=%v", err)
	}
	if got := lease.Claim(); got != claim {
		t.Fatalf("claim after cancellation=%+v want=%+v", got, claim)
	}
	lease.Release()
	lease.Release()
	next, err := lane.Acquire(context.Background(), OperationClaim{Kind: OperationTurn, SessionID: "next"})
	if err != nil {
		t.Fatalf("final release did not free lane: %v", err)
	}
	next.Release()
}
