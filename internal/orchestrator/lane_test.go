package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestOperationLaneSerializesTurnsAndControlsAcrossSessions(t *testing.T) {
	lane := NewOperationLane()
	turn, err := lane.Acquire(context.Background(), OperationClaim{Kind: OperationTurn, SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}

	acquired := make(chan OperationLease, 1)
	go func() {
		lease, acquireErr := lane.Acquire(context.Background(), OperationClaim{
			Kind:               OperationControl,
			ControlOperationID: protocol.ControlOperationID("control-a"),
		})
		if acquireErr == nil {
			acquired <- lease
		}
	}()

	select {
	case lease := <-acquired:
		lease.Release()
		t.Fatal("control acquired while turn still held the application lane")
	case <-time.After(25 * time.Millisecond):
	}
	turn.Release()

	select {
	case lease := <-acquired:
		if lease.Claim().Kind != OperationControl {
			t.Fatalf("claim kind=%q", lease.Claim().Kind)
		}
		lease.Release()
	case <-time.After(time.Second):
		t.Fatal("control did not acquire after turn released the application lane")
	}
}

func TestOperationLaneCancelledWaiterDoesNotBlockFIFO(t *testing.T) {
	lane := NewOperationLane()
	first, err := lane.Acquire(context.Background(), OperationClaim{Kind: OperationTurn, SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}

	cancelledContext, cancel := context.WithCancel(context.Background())
	cancelled := make(chan error, 1)
	go func() {
		_, acquireErr := lane.Acquire(cancelledContext, OperationClaim{Kind: OperationTurn, SessionID: "session-b"})
		cancelled <- acquireErr
	}()
	cancel()
	if err := <-cancelled; err == nil {
		t.Fatal("cancelled acquisition succeeded")
	}

	next := make(chan OperationLease, 1)
	go func() {
		lease, acquireErr := lane.Acquire(context.Background(), OperationClaim{Kind: OperationRecovery, SessionID: "session-a", ControlOperationID: "recovery-a"})
		if acquireErr == nil {
			next <- lease
		}
	}()
	first.Release()
	select {
	case lease := <-next:
		lease.Release()
	case <-time.After(time.Second):
		t.Fatal("cancelled waiter blocked the next operation")
	}
}
