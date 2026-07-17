package app

import (
	"bytes"
	"encoding/json"

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/secret"
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

func sanitizePublishedEvent(redactor secret.Redactor, event Event) Event {
	originalErr := event.Err
	event.Err = nil
	raw, err := redactor.JSON(event)
	if err != nil {
		return eventSanitizationFailure(redactor, event)
	}
	baseline, err := secret.New().JSON(event)
	if err != nil {
		return eventSanitizationFailure(redactor, event)
	}
	sanitizedErr, errChanged := sanitizePublishedError(redactor, originalErr)
	if bytes.Equal(raw, baseline) {
		event.Err = originalErr
		if errChanged {
			event.Err = sanitizedErr
		}
		return event
	}
	var sanitized Event
	if err := json.Unmarshal(raw, &sanitized); err != nil {
		return eventSanitizationFailure(redactor, event)
	}
	if sanitized.Permission != nil && event.Permission != nil {
		sanitized.Permission.Call.ApprovalScope = redactor.String(event.Permission.Call.ApprovalScope)
	}
	sanitized.Err = sanitizedErr
	return sanitized
}

func eventSanitizationFailure(redactor secret.Redactor, event Event) Event {
	return Event{
		Kind:        EventKind(redactor.String(string(event.Kind))),
		Message:     "app event content unavailable",
		NonTerminal: event.NonTerminal,
		Applied:     event.Applied,
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

func sanitizePublishedError(redactor secret.Redactor, err error) (error, bool) {
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
