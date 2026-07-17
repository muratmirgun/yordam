package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

type EventKind string

const (
	EventSessionCreated               EventKind = "session.created"
	EventSessionTitleChanged          EventKind = "session.title_changed"
	EventModeChanged                  EventKind = "mode.changed"
	EventModelChanged                 EventKind = "model.changed"
	EventUserMessage                  EventKind = "user.message"
	EventAssistantMessage             EventKind = "assistant.message"
	EventToolRequested                EventKind = "tool.requested"
	EventPermissionRequested          EventKind = "permission.requested"
	EventPermissionResolved           EventKind = "permission.resolved"
	EventTrustedExecutionAcknowledged EventKind = "trusted_execution.acknowledged"
	EventToolStarted                  EventKind = "tool.started"
	EventToolResult                   EventKind = "tool.result"
	EventFileChangePlanned            EventKind = "file.change_planned"
	EventFileChanged                  EventKind = "file.changed"
	EventContextCompacted             EventKind = "context.compacted"
	EventTurnCompleted                EventKind = "turn.completed"
	EventTurnFailed                   EventKind = "turn.failed"
	EventTurnInterrupted              EventKind = "turn.interrupted"
)

type DurableEvent struct {
	SchemaVersion int             `json:"schema_version"`
	EventID       string          `json:"event_id"`
	SessionID     string          `json:"session_id"`
	Seq           uint64          `json:"seq"`
	Time          time.Time       `json:"time"`
	Kind          EventKind       `json:"kind"`
	Payload       json.RawMessage `json:"payload"`
}

func (e DurableEvent) Validate() error {
	if e.SchemaVersion != 1 || e.EventID == "" || e.SessionID == "" || e.Seq == 0 || e.Time.IsZero() || e.Kind == "" || !json.Valid(e.Payload) {
		return fmt.Errorf("invalid durable event kind=%q seq=%d", e.Kind, e.Seq)
	}
	return nil
}
