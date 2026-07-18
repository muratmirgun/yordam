package ports

import (
	"context"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type PermissionPrompt struct {
	SessionID string
	Call      domain.PreparedToolRequest
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
