package ports

import (
	"context"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type PermissionContext struct {
	SessionID      string
	Mode           domain.PermissionMode
	Workspace      string
	PlatformAction domain.PermissionAction
	ProjectAction  domain.PermissionAction

	// ConfiguredProvider is the exact provider/model descriptor selected by
	// trusted runtime composition. A request must match every field to inherit
	// the compatibility-policy allow.
	ConfiguredProvider *ConfiguredProviderBinding
}

type ConfiguredProviderBinding struct {
	Identity         protocol.ToolIdentity
	SourceRevision   string
	DescriptorDigest protocol.Digest
}

type EvaluationInput struct {
	Permission  PermissionContext
	Request     protocol.AuthorizationRequest
	Descriptor  protocol.ToolDescriptor
	Constraints []protocol.AuthorizationConstraint
}

type PermissionPolicy interface {
	Evaluate(context.Context, PermissionContext, domain.PreparedToolRequest) domain.PermissionDecision
}

type PermissionGranter interface {
	GrantSession(tool, scope string)
}

type AuthorizationPolicy interface {
	EvaluateAuthorization(context.Context, EvaluationInput) (protocol.AuthorizationDecision, error)
}

type AuthorizationGranter interface {
	GrantAuthorizationSession(protocol.AuthorizationRequest, []protocol.AuthorizationConstraint) error
}
