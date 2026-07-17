package protocol

import (
	"encoding/json"
	"fmt"
	"time"
)

type EventEnvelope struct {
	SchemaVersion       uint32              `json:"schema_version"`
	PayloadVersion      uint32              `json:"payload_version"`
	JournalKind         JournalKind         `json:"journal_kind"`
	JournalID           JournalID           `json:"journal_id"`
	EventID             EventID             `json:"event_id"`
	SessionID           SessionID           `json:"session_id,omitempty"`
	Seq                 uint64              `json:"seq"`
	Time                time.Time           `json:"time"`
	Kind                string              `json:"kind"`
	TaskID              TaskID              `json:"task_id,omitempty"`
	TurnID              TurnID              `json:"turn_id,omitempty"`
	ActivityID          ActivityID          `json:"activity_id,omitempty"`
	ParentActivityID    ActivityID          `json:"parent_activity_id,omitempty"`
	CausationEventID    EventID             `json:"causation_event_id,omitempty"`
	Actor               *ActorRef           `json:"actor,omitempty"`
	RuntimeGenerationID RuntimeGenerationID `json:"runtime_generation_id,omitempty"`
	TransactionID       TransactionID       `json:"transaction_id"`
	Payload             json.RawMessage     `json:"payload"`
}

func (e EventEnvelope) ValidateEnvelope() error {
	if err := ValidateBounds(e); err != nil {
		return fmt.Errorf("event envelope bounds: %w", err)
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event envelope: %w", err)
	}
	if len(raw) > MaxEventBytes {
		return fmt.Errorf("event envelope exceeds %d bytes", MaxEventBytes)
	}
	if e.SchemaVersion != EnvelopeVersion || e.PayloadVersion == 0 || !e.JournalKind.Valid() || e.JournalID == "" || e.EventID == "" || e.Seq == 0 || e.Time.IsZero() || e.Kind == "" || e.TransactionID == "" {
		return fmt.Errorf("invalid v2 event envelope kind=%q seq=%d", e.Kind, e.Seq)
	}
	switch e.JournalKind {
	case JournalSession:
		if e.SessionID == "" || JournalID(e.SessionID) != e.JournalID {
			return fmt.Errorf("session journal identity mismatch")
		}
	case JournalWorkspaceControl:
		if e.SessionID != "" {
			return fmt.Errorf("workspace-control event must not carry session ID")
		}
	}
	if e.Actor != nil {
		if err := e.Actor.Validate(); err != nil {
			return err
		}
	}
	if err := ValidateRawJSON(e.Payload); err != nil {
		return fmt.Errorf("invalid event payload: %w", err)
	}
	return nil
}

func CloneEventEnvelope(event EventEnvelope) EventEnvelope {
	return DeepCopy(event)
}

type ProposedEvent struct {
	EventID             EventID
	Time                time.Time
	PayloadVersion      uint32
	Kind                string
	SessionID           SessionID
	TaskID              TaskID
	TurnID              TurnID
	ActivityID          ActivityID
	ParentActivityID    ActivityID
	CausationEventID    EventID
	Actor               *ActorRef
	RuntimeGenerationID RuntimeGenerationID
	Payload             json.RawMessage
}

func CloneProposedEvent(event ProposedEvent) ProposedEvent {
	return DeepCopy(event)
}

type EventRecord struct {
	Envelope    EventEnvelope
	Decoded     any
	RawEnvelope json.RawMessage
	Legacy      *LegacySource
}

func CloneEventRecord(record EventRecord) EventRecord {
	return DeepCopy(record)
}

type LegacySource struct {
	SchemaVersion uint32          `json:"schema_version"`
	EventID       EventID         `json:"event_id"`
	SessionID     SessionID       `json:"session_id"`
	Seq           uint64          `json:"seq"`
	Time          time.Time       `json:"time"`
	Kind          string          `json:"kind"`
	Payload       json.RawMessage `json:"payload"`
	RawEnvelope   json.RawMessage `json:"raw_envelope"`
}

type Diagnostic struct {
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Journal JournalRef      `json:"journal"`
	AtSeq   uint64          `json:"at_seq,omitempty"`
	EventID EventID         `json:"event_id,omitempty"`
	Details json.RawMessage `json:"details,omitempty"`
}

type TransactionCommittedV1 struct {
	TransactionID TransactionID `json:"transaction_id"`
	FirstSeq      uint64        `json:"first_seq"`
	LastSeq       uint64        `json:"last_seq"`
	EventCount    uint32        `json:"event_count"`
	Digest        Digest        `json:"digest"`
}

type TaskState string

const (
	TaskPending   TaskState = "pending"
	TaskRunning   TaskState = "running"
	TaskCompleted TaskState = "completed"
	TaskFailed    TaskState = "failed"
	TaskCancelled TaskState = "cancelled"
)

type TurnState string

const (
	TurnAccepted    TurnState = "accepted"
	TurnRunning     TurnState = "running"
	TurnCompleted   TurnState = "completed"
	TurnFailed      TurnState = "failed"
	TurnInterrupted TurnState = "interrupted"
)

type ActivityState string

const (
	ActivityPlanned             ActivityState = "planned"
	ActivityAuthorized          ActivityState = "authorized"
	ActivityStarted             ActivityState = "started"
	ActivitySucceeded           ActivityState = "succeeded"
	ActivityFailed              ActivityState = "failed"
	ActivityDenied              ActivityState = "denied"
	ActivityCancelled           ActivityState = "cancelled"
	ActivityInterruptedNoEffect ActivityState = "interrupted_no_effect"
	ActivityUncertain           ActivityState = "uncertain"
)

func validTaskState(state TaskState) bool {
	switch state {
	case TaskPending, TaskRunning, TaskCompleted, TaskFailed, TaskCancelled:
		return true
	default:
		return false
	}
}

func validTurnState(state TurnState) bool {
	switch state {
	case TurnAccepted, TurnRunning, TurnCompleted, TurnFailed, TurnInterrupted:
		return true
	default:
		return false
	}
}

func validActivityState(state ActivityState) bool {
	switch state {
	case ActivityPlanned, ActivityAuthorized, ActivityStarted, ActivitySucceeded, ActivityFailed, ActivityDenied, ActivityCancelled, ActivityInterruptedNoEffect, ActivityUncertain:
		return true
	default:
		return false
	}
}

func terminalTaskState(state TaskState) bool {
	return state == TaskCompleted || state == TaskFailed || state == TaskCancelled
}

func terminalTurnState(state TurnState) bool {
	return state == TurnCompleted || state == TurnFailed || state == TurnInterrupted
}

func terminalActivityState(state ActivityState) bool {
	switch state {
	case ActivitySucceeded, ActivityFailed, ActivityDenied, ActivityCancelled, ActivityInterruptedNoEffect, ActivityUncertain:
		return true
	default:
		return false
	}
}
