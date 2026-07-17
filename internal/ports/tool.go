package ports

import (
	"context"

	"github.com/muratmirgun/yordam/internal/domain"
)

type PreparedTool interface {
	Preview() domain.PreparedToolRequest
	Execute(context.Context) domain.ToolResult
}

type PreviewPreparer interface {
	PreparePreview(context.Context) error
}

type Tool interface {
	Descriptor() domain.ToolDescriptor
	Prepare(context.Context, domain.ToolRequest) (PreparedTool, error)
}

type ToolRegistry interface {
	Descriptors() []domain.ToolDescriptor
	Lookup(name string) (Tool, bool)
}
