package orchestrator

import (
	"context"
	"errors"
	"sync"
	"time"
)

// managedOperationLease retains a turn's ownership while allowing its global
// lane slot to be lent to one bounded child operation.  Yield always acquires
// the original claim again before returning, including when the child fails.
type managedOperationLease interface {
	Claim() OperationClaim
	Yield(context.Context, func(context.Context) error) error
	Release()
}

type managedLease struct {
	lane  OperationLane
	claim OperationClaim

	mu       sync.Mutex
	lease    OperationLease
	released bool
	cleanup  time.Duration
}

func acquireManagedOperationLease(ctx context.Context, lane OperationLane, claim OperationClaim) (managedOperationLease, error) {
	return acquireManagedOperationLeaseWithCleanup(ctx, lane, claim, 5*time.Second)
}

func acquireManagedOperationLeaseWithCleanup(ctx context.Context, lane OperationLane, claim OperationClaim, cleanup time.Duration) (managedOperationLease, error) {
	if cleanup <= 0 {
		return nil, errors.New("managed lease cleanup timeout must be positive")
	}
	lease, err := lane.Acquire(ctx, claim)
	if err != nil {
		return nil, err
	}
	return &managedLease{lane: lane, claim: claim, lease: lease, cleanup: cleanup}, nil
}

func (l *managedLease) Claim() OperationClaim { return l.claim }

func (l *managedLease) Yield(ctx context.Context, callback func(context.Context) error) (err error) {
	l.mu.Lock()
	if l.released || l.lease == nil {
		l.mu.Unlock()
		return context.Canceled
	}
	current := l.lease
	l.lease = nil
	l.mu.Unlock()
	current.Release()

	defer func() {
		// A cancelled child must not strand the durable parent turn without its
		// operation claim. Cleanup is cancellation-detached but bounded, so a
		// blocked lane cannot wedge a parent turn indefinitely.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), l.cleanup)
		defer cancel()
		reacquired, acquireErr := l.lane.Acquire(cleanupCtx, l.claim)
		l.mu.Lock()
		if acquireErr == nil && !l.released {
			l.lease = reacquired
		} else if reacquired != nil {
			reacquired.Release()
		}
		l.mu.Unlock()
		if acquireErr != nil {
			err = errors.Join(err, acquireErr)
		}
	}()
	return callback(ctx)
}

func (l *managedLease) Release() {
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return
	}
	l.released = true
	lease := l.lease
	l.lease = nil
	l.mu.Unlock()
	if lease != nil {
		lease.Release()
	}
}

var _ managedOperationLease = (*managedLease)(nil)
