package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/projection"
	"github.com/muratmirgun/yordam/internal/protocol"
)

var ErrSessionStateReadOnly = errors.New("session state source is a read-only validated prefix")

type SessionState struct {
	Mode      domain.PermissionMode
	Selection domain.ModelSelection
}

func ProjectSessionState(replay domain.SessionReplay) SessionState {
	state := SessionState{Mode: replay.Session.Mode, Selection: replay.Session.Selection}
	events := append([]domain.DurableEvent(nil), replay.Events...)
	sort.SliceStable(events, func(left, right int) bool {
		return events[left].Seq < events[right].Seq
	})
	for _, event := range events {
		switch event.Kind {
		case domain.EventModeChanged:
			var payload domain.ModeChangedPayload
			if json.Unmarshal(event.Payload, &payload) == nil && payload.Mode.Validate() == nil {
				state.Mode = payload.Mode
			}
		case domain.EventModelChanged:
			var payload domain.ModelChangedPayload
			if json.Unmarshal(event.Payload, &payload) == nil && payload.Selection.Profile != "" && payload.Selection.Model != "" {
				state.Selection = payload.Selection
			}
		}
	}
	return state
}

type SessionStateJournal interface {
	Inspect(context.Context, protocol.JournalRef) (journal.Inspection, error)
	ReadRange(context.Context, journal.ReadRangeRequest) (journal.EventPage, error)
}

type IncrementalSessionState struct {
	SessionState
	Journal     protocol.JournalRef      `json:"journal"`
	Head        protocol.CommittedCursor `json:"head"`
	Writable    bool                     `json:"writable"`
	Diagnostics []protocol.Diagnostic    `json:"diagnostics,omitempty"`
}

type IncrementalSessionStateProjector struct {
	journal SessionStateJournal
}

// LegacyReplayFromInspection is a presentation-only compatibility projection.
// It never participates in authorization, recovery, or journal decisions.
func LegacyReplayFromInspection(inspection journal.SessionInspection) domain.SessionReplay {
	replay := domain.SessionReplay{Session: inspection.Session, ReadOnly: !inspection.Journal.Writable}
	notes := make([]string, 0, len(inspection.Journal.Diagnostics))
	for _, diagnostic := range inspection.Journal.Diagnostics {
		switch diagnostic.Code {
		case "migration.lossy", "migration.legacy_evidence", "migration.orphan_start", "migration.duplicate_start", "migration.duplicate_terminal", "recovery.available":
			continue
		}
		note := diagnostic.Code
		if diagnostic.Message != "" {
			note += ": " + diagnostic.Message
		}
		notes = append(notes, note)
	}
	for _, record := range inspection.Journal.Events {
		if record.Legacy != nil {
			var event domain.DurableEvent
			if json.Unmarshal(record.Legacy.RawEnvelope, &event) == nil {
				replay.Events = append(replay.Events, event)
			}
			continue
		}
		if record.Envelope.Kind == protocol.EventRecoveryDiagnostic {
			var value protocol.DiagnosticV1
			if json.Unmarshal(record.Envelope.Payload, &value) == nil {
				note := value.Diagnostic.Code
				if value.Diagnostic.Message != "" {
					note += ": " + value.Diagnostic.Message
				}
				notes = append(notes, note)
			}
			continue
		}
		event, ok := legacyPresentationEvent(record)
		if ok {
			replay.Events = append(replay.Events, event)
		}
	}
	replay.RecoveryNote = strings.Join(notes, "; ")
	return replay
}

func legacyPresentationEvent(record protocol.EventRecord) (domain.DurableEvent, bool) {
	envelope := record.Envelope
	legacy := domain.DurableEvent{SchemaVersion: 1, EventID: string(envelope.EventID), SessionID: string(envelope.SessionID), Seq: envelope.Seq, Time: envelope.Time, Kind: domain.EventKind(envelope.Kind)}
	var payload any
	switch envelope.Kind {
	case protocol.EventUserMessage:
		var value protocol.UserMessageV1
		if json.Unmarshal(envelope.Payload, &value) != nil {
			return domain.DurableEvent{}, false
		}
		payload = domain.MessagePayload{Content: value.Content}
	case protocol.EventAssistantMessage:
		var value protocol.AssistantMessageV1
		if json.Unmarshal(envelope.Payload, &value) != nil {
			return domain.DurableEvent{}, false
		}
		message := domain.MessagePayload{}
		for _, block := range value.Blocks {
			if block.Kind == protocol.ContentText {
				message.Content += block.Text
			}
			if block.ToolUse != nil {
				message.ToolCalls = append(message.ToolCalls, domain.ToolCall{ID: block.ToolUse.CallID, Name: block.ToolUse.Alias, Arguments: protocol.CloneRawMessage(block.ToolUse.Arguments)})
			}
		}
		payload = message
	case protocol.EventTurnCompleted, protocol.EventTurnFailed, protocol.EventTurnInterrupted:
		var value protocol.TurnTerminalV1
		if json.Unmarshal(envelope.Payload, &value) != nil {
			return domain.DurableEvent{}, false
		}
		payload = domain.TurnTerminalPayload{Reason: value.Reason}
	case protocol.EventModeChanged:
		var value protocol.ModeChangedV1
		if json.Unmarshal(envelope.Payload, &value) != nil {
			return domain.DurableEvent{}, false
		}
		payload = domain.ModeChangedPayload{Mode: domain.PermissionMode(value.Mode)}
	case protocol.EventModelChanged:
		var value protocol.ModelChangedV1
		if json.Unmarshal(envelope.Payload, &value) != nil {
			return domain.DurableEvent{}, false
		}
		payload = domain.ModelChangedPayload{Selection: domain.ModelSelection{Profile: string(value.ProviderID), Model: string(value.ModelID)}}
	case protocol.EventTrustedExecutionAcknowledged:
		var value protocol.TrustedExecutionAcknowledgedV1
		if json.Unmarshal(envelope.Payload, &value) != nil {
			return domain.DurableEvent{}, false
		}
		payload = domain.TrustedExecutionPayload{Enabled: value.Enabled}
	default:
		return domain.DurableEvent{}, false
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return domain.DurableEvent{}, false
	}
	legacy.Payload = raw
	return legacy, true
}

func NewIncrementalSessionStateProjector(reader SessionStateJournal) *IncrementalSessionStateProjector {
	return &IncrementalSessionStateProjector{journal: reader}
}

func (p *IncrementalSessionStateProjector) Open(
	ctx context.Context,
	ref protocol.JournalRef,
	initial SessionState,
) (IncrementalSessionState, error) {
	state := IncrementalSessionState{SessionState: initial, Journal: ref}
	if p == nil || p.journal == nil {
		return state, fmt.Errorf("session state journal is not configured")
	}
	if err := ref.Validate(); err != nil {
		return state, err
	}
	inspection, err := p.journal.Inspect(ctx, ref)
	if err != nil {
		return state, err
	}
	if inspection.Journal != ref {
		return state, fmt.Errorf("session state inspection journal mismatch")
	}
	state.Diagnostics = protocol.DeepCopy(inspection.Diagnostics)
	candidate, projectedHead, failedRecord, projectionErr := applyCommittedSessionTransactions(ref, state.SessionState, protocol.CommittedCursor{}, inspection.Events)
	if projectionErr != nil {
		state.SessionState, state.Head, state.Writable = candidate, projectedHead, false
		state.Diagnostics = append(state.Diagnostics, sessionStateProjectionDiagnostic(ref, failedRecord, projectionErr))
		return state, errors.Join(ErrSessionStateReadOnly, projectionErr)
	}
	if len(inspection.Events) > 0 && projectedHead != inspection.Head {
		return state, fmt.Errorf("session state inspection head mismatch")
	}
	state.SessionState, state.Head, state.Writable = candidate, inspection.Head, inspection.Writable
	if !inspection.Writable {
		return state, ErrSessionStateReadOnly
	}
	return state, nil
}

func (p *IncrementalSessionStateProjector) ProjectSessionState(
	ctx context.Context,
	current IncrementalSessionState,
) (IncrementalSessionState, error) {
	if p == nil || p.journal == nil {
		return current, fmt.Errorf("session state journal is not configured")
	}
	if !current.Writable {
		return current, ErrSessionStateReadOnly
	}
	if err := current.Journal.Validate(); err != nil {
		return current, err
	}
	if err := current.Head.Validate(); err != nil {
		return current, err
	}
	updated := protocol.DeepCopy(current)
	for {
		page, err := p.journal.ReadRange(ctx, journal.ReadRangeRequest{Journal: updated.Journal, After: updated.Head, Limit: 1000})
		if err != nil {
			return current, err
		}
		candidate, projectedHead, failedRecord, projectionErr := applyCommittedSessionTransactions(updated.Journal, updated.SessionState, updated.Head, page.Events)
		if projectionErr != nil {
			updated.SessionState, updated.Head, updated.Writable = candidate, projectedHead, false
			updated.Diagnostics = append(updated.Diagnostics, sessionStateProjectionDiagnostic(updated.Journal, failedRecord, projectionErr))
			return updated, errors.Join(ErrSessionStateReadOnly, projectionErr)
		}
		if len(page.Events) > 0 {
			if err := page.Cursor.Validate(); err != nil {
				return current, err
			}
			if page.Cursor.JournalKind != updated.Journal.Kind || page.Cursor.JournalID != updated.Journal.ID || page.Cursor.CommitSeq <= updated.Head.CommitSeq || projectedHead != page.Cursor {
				return current, fmt.Errorf("session state cursor did not advance")
			}
			updated.SessionState, updated.Head = candidate, page.Cursor
		}
		if !page.More {
			if page.Head != updated.Head {
				return current, fmt.Errorf("session state final range did not reach reported head")
			}
			return updated, nil
		}
		if len(page.Events) == 0 {
			return current, fmt.Errorf("session state range reported more without advancing")
		}
	}
}

func applyCommittedSessionTransactions(
	ref protocol.JournalRef,
	initial SessionState,
	initialHead protocol.CommittedCursor,
	records []protocol.EventRecord,
) (SessionState, protocol.CommittedCursor, protocol.EventRecord, error) {
	state, head := initial, initialHead
	for start := 0; start < len(records); {
		end := start + 1
		for end < len(records) && sameSessionTransaction(records[start], records[end]) {
			end++
		}
		candidate := state
		for _, record := range records[start:end] {
			var err error
			candidate, err = applyCommittedSessionEvent(candidate, record)
			if err != nil {
				return state, head, record, fmt.Errorf("project session event %q: %w", record.Envelope.Kind, err)
			}
		}
		cursor, err := sessionTransactionCursor(ref, records[start:end])
		if err != nil {
			return state, head, records[start], err
		}
		if head != (protocol.CommittedCursor{}) && cursor.CommitSeq <= head.CommitSeq {
			return state, head, records[start], fmt.Errorf("session state transaction cursor did not advance")
		}
		state, head = candidate, cursor
		start = end
	}
	return state, head, protocol.EventRecord{}, nil
}

func sameSessionTransaction(left, right protocol.EventRecord) bool {
	if left.Legacy != nil || right.Legacy != nil {
		return left.Legacy != nil && right.Legacy != nil && left.Legacy.EventID == right.Legacy.EventID
	}
	return left.Envelope.TransactionID != "" && left.Envelope.TransactionID == right.Envelope.TransactionID
}

func sessionTransactionCursor(ref protocol.JournalRef, records []protocol.EventRecord) (protocol.CommittedCursor, error) {
	if len(records) == 0 {
		return protocol.CommittedCursor{}, fmt.Errorf("session state transaction is empty")
	}
	first, last := records[0], records[len(records)-1]
	if first.Legacy != nil {
		if len(records) != 1 || first.Legacy.EventID == "" || first.Legacy.Seq == 0 {
			return protocol.CommittedCursor{}, fmt.Errorf("invalid legacy session state transaction")
		}
		return protocol.CommittedCursor{
			JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: first.Legacy.Seq,
			TransactionID: protocol.TransactionID("legacy:" + first.Legacy.EventID),
		}, nil
	}
	if first.Envelope.TransactionID == "" || first.Envelope.Seq == 0 || last.Envelope.Seq < first.Envelope.Seq {
		return protocol.CommittedCursor{}, fmt.Errorf("invalid session state transaction identity")
	}
	for index, record := range records {
		if record.Legacy != nil || record.Envelope.TransactionID != first.Envelope.TransactionID || record.Envelope.Seq != first.Envelope.Seq+uint64(index) {
			return protocol.CommittedCursor{}, fmt.Errorf("noncontiguous session state transaction")
		}
	}
	return protocol.CommittedCursor{
		JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: last.Envelope.Seq + 1, TransactionID: first.Envelope.TransactionID,
	}, nil
}

func sessionStateProjectionDiagnostic(ref protocol.JournalRef, record protocol.EventRecord, projectionErr error) protocol.Diagnostic {
	details, _ := json.Marshal(struct {
		Error string `json:"error"`
	}{Error: projectionErr.Error()})
	return protocol.Diagnostic{
		Code: "session_state.projection_failed", Message: "session state projection stopped at the last complete transaction",
		Journal: ref, AtSeq: record.Envelope.Seq, EventID: record.Envelope.EventID, Details: details,
	}
}

func applyCommittedSessionEvent(state SessionState, record protocol.EventRecord) (SessionState, error) {
	if err := projection.ValidateFoundationEvent(record); err != nil {
		return state, err
	}
	switch record.Envelope.Kind {
	case protocol.EventModeChanged:
		payload, ok := record.Decoded.(*protocol.ModeChangedV1)
		if !ok {
			return state, fmt.Errorf("invalid mode projection payload")
		}
		mode := domain.PermissionMode(payload.Mode)
		if err := mode.Validate(); err != nil {
			return state, err
		}
		state.Mode = mode
	case protocol.EventModelChanged:
		payload, ok := record.Decoded.(*protocol.ModelChangedV1)
		if !ok || payload.ProviderID == "" || payload.ModelID == "" {
			return state, fmt.Errorf("invalid model projection payload")
		}
		state.Selection = domain.ModelSelection{Profile: string(payload.ProviderID), Model: string(payload.ModelID)}
	}
	return state, nil
}
