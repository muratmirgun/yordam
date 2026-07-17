package journal

import (
	"context"
	"encoding/json"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
)

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
	OperationID        protocol.ControlOperationID
	Journal            protocol.JournalRef
	ExpectedHead       protocol.CommittedCursor
	ObservedTailDigest protocol.Digest
	TransactionID      protocol.TransactionID
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
