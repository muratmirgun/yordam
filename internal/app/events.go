package app

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
)

var errPublishedEventContentUnavailable = errors.New("app event content unavailable")

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
	EventCompactionStarted   EventKind = "compaction_started"
	EventCompactionProgress  EventKind = "compaction_progress"
	EventCompactionCompleted EventKind = "compaction_completed"
	EventCompactionFailed    EventKind = "compaction_failed"
)

type Event struct {
	Kind        EventKind
	Code        string
	DraftID     uint64
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
	Context     *protocol.ContextProjectionV1
	Compaction  *protocol.CompactionEventV1
	Skills      *SkillSnapshot
}

func sanitizePublishedEvent(redactor secret.Redacting, event Event) (Event, error) {
	originalErr := event.Err
	event.Err = nil
	var sanitizedApprovalScope string
	approvalScopeChanged := false
	if event.Permission != nil {
		sanitizedApprovalScope = redactor.String(event.Permission.Call.ApprovalScope)
		approvalScopeChanged = sanitizedApprovalScope != event.Permission.Call.ApprovalScope
	}
	raw, err := redactor.JSON(event)
	if err != nil {
		return eventSanitizationFailure(), errPublishedEventContentUnavailable
	}
	baseline, err := secret.New().JSON(event)
	if err != nil {
		return eventSanitizationFailure(), errPublishedEventContentUnavailable
	}
	sanitizedErr, errChanged := sanitizePublishedError(redactor, originalErr)
	if bytes.Equal(raw, baseline) && !approvalScopeChanged {
		event.Err = originalErr
		if errChanged {
			event.Err = sanitizedErr
		}
		return event, nil
	}
	var sanitized Event
	if err := json.Unmarshal(raw, &sanitized); err != nil {
		return eventSanitizationFailure(), errPublishedEventContentUnavailable
	}
	if sanitized.Permission != nil && event.Permission != nil {
		sanitized.Permission.Call.ApprovalScope = sanitizedApprovalScope
	}
	sanitized.Err = sanitizedErr
	return sanitized, nil
}

func eventSanitizationFailure() Event {
	return Event{
		Kind:    EventError,
		Message: errPublishedEventContentUnavailable.Error(),
		Err:     errPublishedEventContentUnavailable,
	}
}

type sanitizedPublishedError struct {
	message string
	cause   error
}

func (e *sanitizedPublishedError) Error() string { return e.message }
func (e *sanitizedPublishedError) Unwrap() error { return e.cause }

type sanitizedPublishedErrors struct {
	message string
	causes  []error
}

func (e *sanitizedPublishedErrors) Error() string   { return e.message }
func (e *sanitizedPublishedErrors) Unwrap() []error { return e.causes }

func sanitizePublishedError(redactor secret.Redacting, err error) (error, bool) {
	if err == nil {
		return nil, false
	}
	if typed, ok := err.(*domain.TypedError); ok {
		cause, causeChanged := sanitizePublishedError(redactor, typed.Cause)
		message := redactor.String(typed.Message)
		if !causeChanged && message == typed.Message {
			return err, false
		}
		return &domain.TypedError{Kind: typed.Kind, Message: message, Cause: cause}, true
	}
	if wrapped, ok := err.(interface{ Unwrap() []error }); ok {
		causes := wrapped.Unwrap()
		sanitizedCauses := make([]error, len(causes))
		changed := false
		for index, cause := range causes {
			sanitizedCause, causeChanged := sanitizePublishedError(redactor, cause)
			sanitizedCauses[index] = sanitizedCause
			changed = changed || causeChanged
			if !causeChanged {
				sanitizedCauses[index] = cause
			}
		}
		message := redactor.String(err.Error())
		if !changed && message == err.Error() {
			return err, false
		}
		return &sanitizedPublishedErrors{message: message, causes: sanitizedCauses}, true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		cause, causeChanged := sanitizePublishedError(redactor, wrapped.Unwrap())
		message := redactor.String(err.Error())
		if !causeChanged && message == err.Error() {
			return err, false
		}
		return &sanitizedPublishedError{message: message, cause: cause}, true
	}
	message := redactor.String(err.Error())
	if message == err.Error() {
		return err, false
	}
	return &sanitizedPublishedError{message: message}, true
}
