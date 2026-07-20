package subagent

import (
	"fmt"
	"reflect"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/projection"
	"github.com/muratmirgun/yordam/internal/protocol"
)

const projectionVersion uint32 = 1

type State string

const (
	StateIncomplete State = "incomplete"
	StateWaiting    State = "waiting"
	StateTerminal   State = "terminal"
	StateAttached   State = "attached"
	StateConflict   State = "conflict"
)

type Attempt struct {
	AttemptID       protocol.DelegationAttemptID       `json:"attempt_id"`
	ParentTaskID    protocol.TaskID                    `json:"parent_task_id"`
	ParentTurnID    protocol.TurnID                    `json:"parent_turn_id"`
	Call            protocol.SubagentCallV1            `json:"call"`
	Manifest        protocol.SubagentManifestV1        `json:"manifest"`
	State           State                              `json:"state"`
	RequestCursor   protocol.CommittedCursor           `json:"request_cursor"`
	WaitingCursor   *protocol.CommittedCursor          `json:"waiting_cursor,omitempty"`
	ChildManifested bool                               `json:"child_manifested"`
	Receipt         *protocol.SubagentReceiptV1        `json:"receipt,omitempty"`
	ReceiptDigest   protocol.Digest                    `json:"receipt_digest,omitempty"`
	TerminalCursor  protocol.CommittedCursor           `json:"terminal_cursor,omitempty"`
	Attachment      *protocol.SubagentResultAttachedV1 `json:"attachment,omitempty"`
	Conflict        string                             `json:"conflict,omitempty"`
}

type Projection struct {
	Journal  protocol.JournalRef                      `json:"journal"`
	Attempts map[protocol.DelegationAttemptID]Attempt `json:"attempts"`
}

type Projector struct{}

func (Projector) Version() uint32 { return projectionVersion }

func (Projector) Zero(ref protocol.JournalRef) Projection {
	return Projection{Journal: ref, Attempts: make(map[protocol.DelegationAttemptID]Attempt)}
}

func (Projector) Apply(current Projection, event protocol.EventRecord) (Projection, error) {
	if err := projection.ValidateFoundationEvent(event); err != nil {
		return current, err
	}
	if err := validateProjection(current); err != nil {
		return current, err
	}
	next := protocol.DeepCopy(current)
	if next.Attempts == nil {
		next.Attempts = make(map[protocol.DelegationAttemptID]Attempt)
	}
	switch event.Envelope.Kind {
	case protocol.EventSubagentRequested:
		return applyRequest(current, next, event)
	case protocol.EventSubagentWaiting:
		return applyWaiting(current, next, event)
	case protocol.EventSubagentManifest:
		return applyManifest(current, next, event)
	case protocol.EventSubagentReceipt:
		return applyReceipt(current, next, event)
	case protocol.EventSubagentResultAttached:
		return applyAttachment(current, next, event)
	default:
		return next, nil
	}
}

func applyRequest(current, next Projection, event protocol.EventRecord) (Projection, error) {
	payload, ok := event.Decoded.(*protocol.SubagentRequestedV1)
	if !ok || payload.Validate() != nil || payload.Manifest.ParentCursor.CommitSeq >= event.Envelope.Seq || !parentEvent(next.Journal, event, payload.Manifest.ParentSessionID, payload.Manifest.RuntimeGenerationID) {
		return current, fmt.Errorf("invalid subagent request")
	}
	if existing, exists := next.Attempts[payload.Manifest.AttemptID]; exists {
		if existing.State == StateConflict {
			return next, nil
		}
		if existing.Call != payload.Call || !reflect.DeepEqual(existing.Manifest, payload.Manifest) || existing.ParentTaskID != event.Envelope.TaskID || existing.ParentTurnID != event.Envelope.TurnID {
			return conflict(next, payload.Manifest.AttemptID, "request binding changed"), nil
		}
		return next, nil
	}
	if activeAttempt(next) {
		return current, fmt.Errorf("parent already has an active subagent")
	}
	if attemptsForTurn(next, event.Envelope.TurnID) >= protocol.MaxSubagentAttemptsPerTurn {
		return current, fmt.Errorf("subagent attempt limit exceeded")
	}
	next.Attempts[payload.Manifest.AttemptID] = Attempt{AttemptID: payload.Manifest.AttemptID, ParentTaskID: event.Envelope.TaskID, ParentTurnID: event.Envelope.TurnID, Call: protocol.DeepCopy(payload.Call), Manifest: protocol.DeepCopy(payload.Manifest), State: StateIncomplete, RequestCursor: eventCursor(event)}
	return next, nil
}

func applyWaiting(current, next Projection, event protocol.EventRecord) (Projection, error) {
	payload, ok := event.Decoded.(*protocol.SubagentWaitingV1)
	if !ok || payload.Validate() != nil || !parentEvent(next.Journal, event, protocol.SessionID(next.Journal.ID), event.Envelope.RuntimeGenerationID) {
		return current, fmt.Errorf("invalid subagent waiting state")
	}
	attempt, exists := next.Attempts[payload.AttemptID]
	if !exists || attempt.Manifest.ChildSessionID != payload.ChildSessionID || attempt.Manifest.RuntimeGenerationID != event.Envelope.RuntimeGenerationID || attempt.ParentTaskID != event.Envelope.TaskID || attempt.ParentTurnID != event.Envelope.TurnID {
		return current, fmt.Errorf("subagent waiting has no matching request")
	}
	if attempt.State == StateConflict {
		return next, nil
	}
	if attempt.State == StateWaiting {
		return next, nil
	}
	if attempt.State != StateIncomplete {
		return current, fmt.Errorf("subagent waiting is out of order")
	}
	cursor := eventCursor(event)
	attempt.State, attempt.WaitingCursor = StateWaiting, &cursor
	next.Attempts[payload.AttemptID] = attempt
	return next, nil
}

func applyManifest(current, next Projection, event protocol.EventRecord) (Projection, error) {
	payload, ok := event.Decoded.(*protocol.SubagentManifestV1)
	if !ok || payload.Validate() != nil || !childEvent(event, *payload) {
		return current, fmt.Errorf("invalid subagent child manifest")
	}
	attempt, exists := next.Attempts[payload.AttemptID]
	if !exists || !reflect.DeepEqual(attempt.Manifest, *payload) {
		return current, fmt.Errorf("subagent child manifest is out of order")
	}
	if attempt.State == StateConflict {
		return next, nil
	}
	if attempt.State != StateWaiting {
		return current, fmt.Errorf("subagent child manifest is out of order")
	}
	if attempt.ChildManifested {
		return next, nil
	}
	attempt.ChildManifested = true
	next.Attempts[payload.AttemptID] = attempt
	return next, nil
}

func applyReceipt(current, next Projection, event protocol.EventRecord) (Projection, error) {
	payload, ok := event.Decoded.(*protocol.SubagentReceiptV1)
	if !ok || payload.Validate() != nil || !childEvent(event, payload.Manifest) || payload.TerminalCursor != eventCursor(event) {
		return current, fmt.Errorf("invalid subagent receipt")
	}
	attempt, exists := next.Attempts[payload.Manifest.AttemptID]
	if !exists || !reflect.DeepEqual(attempt.Manifest, payload.Manifest) || !attempt.ChildManifested {
		return current, fmt.Errorf("subagent receipt is out of order")
	}
	if attempt.State == StateConflict {
		return next, nil
	}
	digest, err := canonicaljson.Digest(*payload)
	if err != nil {
		return current, err
	}
	if attempt.Receipt != nil {
		if attempt.ReceiptDigest != digest || !reflect.DeepEqual(*attempt.Receipt, *payload) {
			return conflict(next, payload.Manifest.AttemptID, "conflicting child receipt"), nil
		}
		return next, nil
	}
	if attempt.State != StateWaiting {
		return current, fmt.Errorf("subagent receipt is out of order")
	}
	attempt.State, attempt.Receipt, attempt.ReceiptDigest, attempt.TerminalCursor = StateTerminal, protocol.DeepCopy(payload), digest, payload.TerminalCursor
	next.Attempts[payload.Manifest.AttemptID] = attempt
	return next, nil
}

func applyAttachment(current, next Projection, event protocol.EventRecord) (Projection, error) {
	payload, ok := event.Decoded.(*protocol.SubagentResultAttachedV1)
	if !ok || payload.Validate() != nil || !parentEvent(next.Journal, event, protocol.SessionID(next.Journal.ID), event.Envelope.RuntimeGenerationID) {
		return current, fmt.Errorf("invalid subagent result attachment")
	}
	attempt, exists := next.Attempts[payload.AttemptID]
	if !exists || attempt.Manifest.ChildSessionID != payload.ChildSessionID || attempt.Manifest.RuntimeGenerationID != event.Envelope.RuntimeGenerationID || attempt.ParentTaskID != event.Envelope.TaskID || attempt.ParentTurnID != event.Envelope.TurnID || attempt.Receipt == nil {
		return current, fmt.Errorf("subagent attachment is out of order")
	}
	if attempt.State == StateConflict {
		return next, nil
	}
	if payload.TerminalCursor != attempt.TerminalCursor || payload.ReceiptDigest != attempt.ReceiptDigest {
		return conflict(next, payload.AttemptID, "attachment does not match terminal receipt"), nil
	}
	if attempt.Attachment != nil {
		if !reflect.DeepEqual(*attempt.Attachment, *payload) {
			return conflict(next, payload.AttemptID, "conflicting result attachment"), nil
		}
		return next, nil
	}
	attempt.State, attempt.Attachment = StateAttached, protocol.DeepCopy(payload)
	next.Attempts[payload.AttemptID] = attempt
	return next, nil
}

func validateProjection(state Projection) error {
	if state.Journal.Kind != protocol.JournalSession || state.Journal.ID == "" {
		return fmt.Errorf("subagent projection must be bound to a parent session")
	}
	return nil
}

func parentEvent(ref protocol.JournalRef, event protocol.EventRecord, parent protocol.SessionID, runtime protocol.RuntimeGenerationID) bool {
	return parent != "" && protocol.JournalID(parent) == ref.ID && event.Envelope.JournalKind == protocol.JournalSession && event.Envelope.JournalID == ref.ID && event.Envelope.SessionID == parent && event.Envelope.RuntimeGenerationID == runtime && event.Envelope.TaskID != "" && event.Envelope.TurnID != ""
}

func childEvent(event protocol.EventRecord, manifest protocol.SubagentManifestV1) bool {
	return event.Envelope.JournalKind == protocol.JournalSession && event.Envelope.JournalID == protocol.JournalID(manifest.ChildSessionID) && event.Envelope.SessionID == manifest.ChildSessionID && event.Envelope.TaskID == manifest.ChildTaskID && event.Envelope.TurnID == manifest.ChildTurnID && event.Envelope.RuntimeGenerationID == manifest.RuntimeGenerationID
}

func eventCursor(event protocol.EventRecord) protocol.CommittedCursor {
	return protocol.CommittedCursor{JournalKind: event.Envelope.JournalKind, JournalID: event.Envelope.JournalID, CommitSeq: event.Envelope.Seq, TransactionID: event.Envelope.TransactionID}
}

func activeAttempt(state Projection) bool {
	for _, attempt := range state.Attempts {
		if attempt.State == StateIncomplete || attempt.State == StateWaiting {
			return true
		}
	}
	return false
}

func attemptsForTurn(state Projection, turnID protocol.TurnID) int {
	count := 0
	for _, attempt := range state.Attempts {
		if attempt.ParentTurnID == turnID {
			count++
		}
	}
	return count
}

func conflict(state Projection, attemptID protocol.DelegationAttemptID, reason string) Projection {
	attempt := state.Attempts[attemptID]
	if attempt.State == StateConflict {
		return state
	}
	attempt.State, attempt.Conflict = StateConflict, reason
	state.Attempts[attemptID] = attempt
	return state
}
