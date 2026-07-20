package permission

import (
	"context"
	"fmt"
	"sync"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
)

// SessionPolicyRegistry resolves authorization against the policy owned by the
// target session. Children are deliberately initialized from a mode only: a
// parent acknowledgement or grant is mutable authority and must not cross a
// session boundary.
type SessionPolicyRegistry struct {
	mu       sync.RWMutex
	policies map[protocol.SessionID]*SessionPolicy
	children map[protocol.SessionID]childLineage
}

type childLineage struct {
	parent  protocol.SessionID
	attempt protocol.DelegationAttemptID
}

func NewSessionPolicyRegistry() *SessionPolicyRegistry {
	return &SessionPolicyRegistry{policies: make(map[protocol.SessionID]*SessionPolicy), children: make(map[protocol.SessionID]childLineage)}
}

func (r *SessionPolicyRegistry) RegisterParent(sessionID protocol.SessionID, policy *SessionPolicy) error {
	if sessionID == "" || policy == nil {
		return fmt.Errorf("session policy registration is incomplete")
	}
	r.mu.Lock()
	r.policies[sessionID] = policy
	r.mu.Unlock()
	return nil
}

func (r *SessionPolicyRegistry) RegisterChild(sessionID protocol.SessionID, mode domain.PermissionMode) error {
	if sessionID == "" {
		return fmt.Errorf("child session ID is required")
	}
	return r.RegisterParent(sessionID, NewSession(mode))
}

// RegisterChildLineage is separate from RegisterChild so the public policy
// interface remains mode-only. It binds only immutable prompt correlation.
func (r *SessionPolicyRegistry) RegisterChildLineage(sessionID, parentSessionID protocol.SessionID, attemptID protocol.DelegationAttemptID) error {
	if sessionID == "" || parentSessionID == "" || attemptID == "" || sessionID == parentSessionID {
		return fmt.Errorf("child prompt lineage is incomplete")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.policies[sessionID] == nil {
		return fmt.Errorf("child session policy is not registered")
	}
	r.children[sessionID] = childLineage{parent: parentSessionID, attempt: attemptID}
	return nil
}

func (r *SessionPolicyRegistry) ChildPrompt(sessionID protocol.SessionID, call domain.PreparedToolRequest) (ports.ChildPrompt, bool) {
	if r == nil {
		return ports.ChildPrompt{}, false
	}
	r.mu.RLock()
	lineage, ok := r.children[sessionID]
	r.mu.RUnlock()
	if !ok {
		return ports.ChildPrompt{}, false
	}
	return ports.ChildPrompt{SessionID: string(sessionID), ParentSessionID: string(lineage.parent), DelegationAttemptID: string(lineage.attempt), Call: call}, true
}

// Restore installs authority reconstructed from this session's own replay.
// It intentionally does not consult a parent replay or the active session.
func (r *SessionPolicyRegistry) Restore(sessionID protocol.SessionID, replay domain.SessionReplay) error {
	if sessionID == "" || replay.Session.ID == "" || protocol.SessionID(replay.Session.ID) != sessionID {
		return fmt.Errorf("session replay does not bind the target session")
	}
	return r.RegisterParent(sessionID, Restore(replay))
}

func (r *SessionPolicyRegistry) Evaluate(ctx context.Context, permissionContext ports.PermissionContext, call domain.PreparedToolRequest) domain.PermissionDecision {
	policy := r.policy(protocol.SessionID(permissionContext.SessionID))
	if policy == nil {
		return domain.PermissionDecision{Action: domain.PermissionDeny, Scope: call.CanonicalScope, Reason: "unknown session policy"}
	}
	return policy.Evaluate(ctx, permissionContext, call)
}

func (r *SessionPolicyRegistry) EvaluateAuthorization(ctx context.Context, input ports.EvaluationInput) (protocol.AuthorizationDecision, error) {
	policy := r.policy(input.Request.SessionID)
	if policy == nil {
		return protocol.AuthorizationDecision{}, fmt.Errorf("authorization policy is not registered for session %q", input.Request.SessionID)
	}
	return policy.EvaluateAuthorization(ctx, input)
}

func (r *SessionPolicyRegistry) GrantAuthorizationSession(request protocol.AuthorizationRequest, constraints []protocol.AuthorizationConstraint) error {
	policy := r.policy(request.SessionID)
	if policy == nil {
		return fmt.Errorf("authorization policy is not registered for session %q", request.SessionID)
	}
	return policy.GrantAuthorizationSession(request, constraints)
}

func (r *SessionPolicyRegistry) policy(sessionID protocol.SessionID) *SessionPolicy {
	if r == nil || sessionID == "" {
		return nil
	}
	r.mu.RLock()
	policy := r.policies[sessionID]
	r.mu.RUnlock()
	return policy
}

var _ interface {
	EvaluateAuthorization(context.Context, ports.EvaluationInput) (protocol.AuthorizationDecision, error)
	GrantAuthorizationSession(protocol.AuthorizationRequest, []protocol.AuthorizationConstraint) error
	RegisterParent(protocol.SessionID, *SessionPolicy) error
	RegisterChild(protocol.SessionID, domain.PermissionMode) error
	Restore(protocol.SessionID, domain.SessionReplay) error
} = (*SessionPolicyRegistry)(nil)
