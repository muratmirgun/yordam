package ports

import (
	"context"

	"github.com/muratmirgun/yordam/internal/domain"
)

type PermissionPrompt struct {
	SessionID string
	Call      domain.PreparedToolRequest
}

type PermissionApprover interface {
	Resolve(context.Context, PermissionPrompt) (domain.PermissionDecision, error)
}
