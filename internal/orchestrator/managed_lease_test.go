package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"
)

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
