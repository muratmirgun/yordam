package app

import (
	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
)

type EventKind string

const (
	EventState               EventKind = "state"
	EventTextDelta           EventKind = "text_delta"
	EventPermissionRequested EventKind = "permission_requested"
	EventToolStarted         EventKind = "tool_started"
	EventToolOutput          EventKind = "tool_output"
	EventToolCompleted       EventKind = "tool_completed"
	EventTurnCompleted       EventKind = "turn_completed"
	EventTurnAccepted        EventKind = "turn_accepted"
	EventTurnInterrupted     EventKind = "turn_interrupted"
	EventReloadCompleted     EventKind = "reload_completed"
	EventNotice              EventKind = "notice"
	EventError               EventKind = "error"
	EventRejected            EventKind = "rejected"
)

type Event struct {
	Kind        EventKind
	Runtime     agent.RuntimeEvent
	Permission  *ports.PermissionPrompt
	Mode        domain.PermissionMode
	Selection   domain.ModelSelection
	Session     domain.Session
	Replay      domain.SessionReplay
	NonTerminal bool
	Err         error
	Message     string
	Models      []domain.ModelSelection
	Draft       string
	Applied     bool
}
