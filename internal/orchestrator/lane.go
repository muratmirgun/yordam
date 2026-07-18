package orchestrator

import (
	"context"
	"fmt"
	"sync"

	"github.com/muratmirgun/yordam/internal/protocol"
)

type OperationKind string

const (
	OperationTurn             OperationKind = "turn"
	OperationControl          OperationKind = "control"
	OperationRecovery         OperationKind = "recovery"
	OperationCompaction       OperationKind = "compaction"
	OperationReloadActivation OperationKind = "reload_activation"
)

type OperationClaim struct {
	Kind               OperationKind
	SessionID          protocol.SessionID
	ControlOperationID protocol.ControlOperationID
}

type OperationLease interface {
	Claim() OperationClaim
	Release()
}

type OperationLane interface {
	Acquire(context.Context, OperationClaim) (OperationLease, error)
}

type operationLane struct {
	mu      sync.Mutex
	held    bool
	waiters []*laneWaiter
}

type laneWaiter struct {
	ready   chan struct{}
	granted bool
}

type operationLease struct {
	lane  *operationLane
	claim OperationClaim
	once  sync.Once
}

func NewOperationLane() OperationLane { return &operationLane{} }

func (l *operationLane) Acquire(ctx context.Context, claim OperationClaim) (OperationLease, error) {
	if err := validateOperationClaim(claim); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	waiter := &laneWaiter{ready: make(chan struct{})}
	l.mu.Lock()
	if !l.held && len(l.waiters) == 0 {
		l.held = true
		l.mu.Unlock()
		return &operationLease{lane: l, claim: claim}, nil
	}
	l.waiters = append(l.waiters, waiter)
	l.mu.Unlock()

	select {
	case <-waiter.ready:
		return &operationLease{lane: l, claim: claim}, nil
	case <-ctx.Done():
		l.mu.Lock()
		if waiter.granted {
			l.mu.Unlock()
			return &operationLease{lane: l, claim: claim}, nil
		}
		for index, queued := range l.waiters {
			if queued == waiter {
				l.waiters = append(l.waiters[:index], l.waiters[index+1:]...)
				break
			}
		}
		l.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (l *operationLease) Claim() OperationClaim { return l.claim }

func (l *operationLease) Release() {
	if l == nil || l.lane == nil {
		return
	}
	l.once.Do(func() { l.lane.release() })
}

func (l *operationLane) release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.waiters) == 0 {
		l.held = false
		return
	}
	next := l.waiters[0]
	l.waiters = l.waiters[1:]
	next.granted = true
	close(next.ready)
}

func validateOperationClaim(claim OperationClaim) error {
	switch claim.Kind {
	case OperationTurn:
		if claim.SessionID == "" || claim.ControlOperationID != "" {
			return fmt.Errorf("turn operation requires only a session ID")
		}
	case OperationRecovery:
		if claim.SessionID == "" || claim.ControlOperationID == "" {
			return fmt.Errorf("recovery operation requires session and control operation IDs")
		}
	case OperationControl, OperationCompaction, OperationReloadActivation:
		if claim.ControlOperationID == "" {
			return fmt.Errorf("%s operation requires a control operation ID", claim.Kind)
		}
	default:
		return fmt.Errorf("invalid operation kind %q", claim.Kind)
	}
	return nil
}

var _ OperationLane = (*operationLane)(nil)
var _ OperationLease = (*operationLease)(nil)
