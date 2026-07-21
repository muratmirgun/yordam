package ports

import (
	"context"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/recovery"
)

type PreparedTool interface {
	Preview() domain.PreparedToolRequest
	Execute(context.Context) domain.ToolResult
}

type PreviewPreparer interface {
	PreparePreview(context.Context) error
}

// ToolPlanner validates arguments and resolves canonical resource names without
// inspecting resource content or dispatching the tool.
type ToolPlanner interface {
	Plan(context.Context, domain.ToolRequest) (PreparedTool, error)
}

type ResourceRevalidator interface {
	Revalidate(context.Context) (domain.PreparedToolRequest, error)
}

type RecoveryMaterialProvider interface {
	RecoveryMaterial(context.Context) (recovery.Candidate, bool, error)
}

type CanonicalDescriptorProvider interface {
	CanonicalDescriptor() protocol.ToolDescriptor
}

type TrustedClassificationProvider interface {
	TrustedClassification() domain.ToolClassification
}

type Tool interface {
	Descriptor() domain.ToolDescriptor
	Prepare(context.Context, domain.ToolRequest) (PreparedTool, error)
}

// OrchestratedTool marks a built-in whose execution is owned by the turn
// orchestrator rather than the ordinary tool dispatcher. Callers must bind the
// marker to the canonical descriptor identity and digest before intercepting.
type OrchestratedTool interface {
	Tool
	OrchestratedKind() string
}

type ToolRegistry interface {
	Descriptors() []domain.ToolDescriptor
	Lookup(name string) (Tool, bool)
}
