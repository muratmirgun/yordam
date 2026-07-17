package agent

import "github.com/muratmirgun/yordam/internal/domain"

type RuntimeEventKind string

const (
	RuntimeStateChanged  RuntimeEventKind = "state_changed"
	RuntimeTextDelta     RuntimeEventKind = "text_delta"
	RuntimeToolStarted   RuntimeEventKind = "tool_started"
	RuntimeToolOutput    RuntimeEventKind = "tool_output"
	RuntimeToolCompleted RuntimeEventKind = "tool_completed"
)

type RuntimeEvent struct {
	Kind     RuntimeEventKind
	State    string
	Text     string
	Progress *domain.ToolProgress
	Result   *domain.ToolResult
}

type Sink func(RuntimeEvent)
