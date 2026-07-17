package ports

import (
	"context"

	"github.com/muratmirgun/yordam/internal/domain"
)

type PermissionContext struct {
	SessionID string
	Mode      domain.PermissionMode
	Workspace string
}

type PermissionPolicy interface {
	Evaluate(context.Context, PermissionContext, domain.PreparedToolRequest) domain.PermissionDecision
}

type PermissionGranter interface {
	GrantSession(tool, scope string)
}
