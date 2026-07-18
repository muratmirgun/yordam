package ports

import (
	"context"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type PermissionContext struct {
	SessionID          string
	Mode               domain.PermissionMode
	Workspace          string
	PlatformAction     domain.PermissionAction
	ProjectAction      domain.PermissionAction
	ConfiguredProvider bool
}

type PermissionPolicy interface {
	Evaluate(context.Context, PermissionContext, domain.PreparedToolRequest) domain.PermissionDecision
}

type PermissionGranter interface {
	GrantSession(tool, scope string)
}

type AuthorizationPolicy interface {
	EvaluateAuthorization(context.Context, PermissionContext, protocol.AuthorizationRequest) (protocol.AuthorizationDecision, error)
}

type AuthorizationGranter interface {
	GrantAuthorizationSession(protocol.AuthorizationRequest, []protocol.AuthorizationConstraint) error
}
