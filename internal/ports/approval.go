package ports

import (
	"context"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type PermissionPrompt struct {
	SessionID           string
	ParentSessionID     string
	DelegationAttemptID string
	Call                domain.PreparedToolRequest
}

// ChildPrompt preserves the child session's immutable delegation identity in
// prompts presented through the parent application.  Call identity remains
// child-local; the displayed correlation token is minted by the application.
type ChildPrompt struct {
	SessionID           string
	ParentSessionID     string
	DelegationAttemptID string
	Call                domain.PreparedToolRequest
}

type PermissionApprover interface {
	Resolve(context.Context, PermissionPrompt) (domain.PermissionDecision, error)
}

type AuthorizationPrompt struct {
	Request      protocol.AuthorizationRequest
	Scope        protocol.CanonicalAuthorizationScope
	ScopeDigest  protocol.Digest
	Summary      string
	ProposedDiff string
}

type AuthorizationApprover interface {
	ResolveAuthorization(context.Context, AuthorizationPrompt) (protocol.ApprovalResponse, error)
}
