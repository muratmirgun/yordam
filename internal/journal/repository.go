package journal

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
)

var (
	ErrLineageParentMissing = errors.New("lineage parent missing")
	ErrLineageCycle         = errors.New("lineage cycle")
	ErrLineageDepthExceeded = errors.New("lineage depth exceeds 64")
	ErrTurnLeaseHeld        = errors.New("turn lease held")
	ErrTurnRecoveryRequired = errors.New("turn recovery required")
	ErrTurnHeadConflict     = errors.New("turn lease expected-head conflict")
	ErrTurnNotTerminal      = errors.New("turn is not terminal at supplied cursor")
	// ErrSessionNotFound is deliberately typed so recovery can distinguish a
	// proven absent pre-reserved child from an unavailable inspection.
	ErrSessionNotFound = errors.New("session not found")
)

type SessionLineage struct {
	Kind LineageKind `json:"kind,omitempty"`

	ParentSessionID protocol.SessionID       `json:"parent_session_id"`
	ParentCursor    protocol.CommittedCursor `json:"parent_cursor"`

	CheckpointDigest    protocol.Digest              `json:"checkpoint_digest,omitempty,omitzero"`
	DelegationAttemptID protocol.DelegationAttemptID `json:"delegation_attempt_id,omitempty"`
	ManifestDigest      protocol.Digest              `json:"manifest_digest,omitempty,omitzero"`
}

type LineageKind string

const (
	LineageCheckpoint LineageKind = "checkpoint"
	LineageSubagent   LineageKind = "subagent"
)

type LineageCursor struct {
	ViewSessionID   protocol.SessionID       `json:"view_session_id"`
	OriginSessionID protocol.SessionID       `json:"origin_session_id"`
	OriginCursor    protocol.CommittedCursor `json:"origin_cursor"`
}

type ComposedReadRequest struct {
	SessionID protocol.SessionID
	After     LineageCursor
	Limit     int
}

type LineageEvent struct {
	Cursor LineageCursor
	Record protocol.EventRecord
}

type ComposedEventPage struct {
	Events []LineageEvent
	Cursor LineageCursor
	Head   LineageCursor
	More   bool
}

type TurnLease interface {
	SessionID() protocol.SessionID
	TurnID() protocol.TurnID
	Release(context.Context, protocol.CommittedCursor) error
}

// UnresolvedTurnLease releases only the in-process/durable lock ownership. It
// deliberately does not write or require a turn terminal; the journal remains
// active so ordinary turns are recovery-gated.
type UnresolvedTurnLease interface {
	TurnLease
	Abandon(context.Context) error
}

type TurnLeaseManager interface {
	AcquireTurnLease(context.Context, protocol.SessionID, protocol.TurnID, protocol.CommittedCursor) (TurnLease, error)
	AcquireTurnRecoveryLease(context.Context, protocol.SessionID, protocol.TurnID, protocol.CommittedCursor) (TurnLease, error)
}

type ActiveTurnInspector interface {
	ActiveTurn(context.Context, protocol.SessionID, protocol.CommittedCursor) (turnID protocol.TurnID, terminal bool, err error)
}

type AppendStatus string

const (
	AppendCommitted        AppendStatus = "committed"
	AppendConflict         AppendStatus = "conflict"
	AppendRecoveryRequired AppendStatus = "recovery_required"
	AppendCommitUnknown    AppendStatus = "commit_unknown"
)

type CompatibilityDeclaration struct {
	ReaderVersion uint32
	WriterVersion uint32
	LegacyHead    protocol.CommittedCursor
}

type AppendRequest struct {
	Journal       protocol.JournalRef
	ExpectedHead  protocol.CommittedCursor
	TransactionID protocol.TransactionID
	Compatibility *CompatibilityDeclaration
	Events        []protocol.ProposedEvent
}

type AppendResult struct {
	Status      AppendStatus
	Cursor      protocol.CommittedCursor
	CurrentHead protocol.CommittedCursor
	Events      []protocol.EventEnvelope
}

type ReadRangeRequest struct {
	Journal protocol.JournalRef
	After   protocol.CommittedCursor
	Limit   int
}

type EventPage struct {
	Events []protocol.EventRecord
	Cursor protocol.CommittedCursor
	Head   protocol.CommittedCursor
	More   bool
}

type TransactionState string

const (
	TransactionCommitted    TransactionState = "committed"
	TransactionNotCommitted TransactionState = "not_committed"
	TransactionUnknown      TransactionState = "unknown"
)

type TransactionLookup struct {
	State  TransactionState
	Cursor protocol.CommittedCursor
}

type CommittedTransaction struct {
	Journal       protocol.JournalRef
	TransactionID protocol.TransactionID
	Cursor        protocol.CommittedCursor
	Events        []protocol.EventEnvelope
}

type Inspection struct {
	Journal               protocol.JournalRef
	Head                  protocol.CommittedCursor
	Events                []protocol.EventRecord
	Writable              bool
	Diagnostics           []protocol.Diagnostic
	IncompleteTransaction protocol.TransactionID
}

type SessionInspection struct {
	Session domain.Session
	Journal Inspection
}

type RecoveryRequest struct {
	OperationID         protocol.ControlOperationID
	Journal             protocol.JournalRef
	ExpectedHead        protocol.CommittedCursor
	ObservedTailDigest  protocol.Digest
	TransactionID       protocol.TransactionID
	RuntimeGenerationID protocol.RuntimeGenerationID
}

type RecoveryResult struct {
	Status           string
	Cursor           protocol.CommittedCursor
	QuarantineDigest protocol.Digest
	Diagnostic       protocol.Diagnostic
}

type Encoder interface {
	EncodeProposed(protocol.ProposedEvent) (json.RawMessage, error)
}

type CommittedTransactionReader interface {
	ReadCommittedTransaction(context.Context, protocol.JournalRef, protocol.TransactionID) (CommittedTransaction, error)
}

type Repository interface {
	Inspect(context.Context, protocol.JournalRef) (Inspection, error)
	Head(context.Context, protocol.JournalRef) (protocol.CommittedCursor, error)
	ReadRange(context.Context, ReadRangeRequest) (EventPage, error)
	AppendBatch(context.Context, AppendRequest) (AppendResult, error)
	LookupTransaction(context.Context, protocol.JournalRef, protocol.TransactionID) (TransactionLookup, error)
	CommittedTransactionReader
	Recover(context.Context, RecoveryRequest) (RecoveryResult, error)
}
