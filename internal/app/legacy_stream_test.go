package app

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/protocol"
)

type scriptedSubscription struct {
	items  []protocol.SubscriptionItem
	closed bool
}

func (s *scriptedSubscription) Next(context.Context) (protocol.SubscriptionItem, error) {
	if len(s.items) == 0 {
		return protocol.SubscriptionItem{}, io.EOF
	}
	item := s.items[0]
	s.items = s.items[1:]
	return item, nil
}

func (s *scriptedSubscription) Close() error { s.closed = true; return nil }

func streamEvent(kind string, activity protocol.ActivityID, turn protocol.TurnID, payload any) protocol.ApplicationEvent {
	raw, _ := json.Marshal(payload)
	return protocol.ApplicationEvent{
		ProtocolVersion: protocol.ApplicationProtocolVersion,
		StreamEventID:   kind + ":" + string(activity),
		Correlation: protocol.EventCorrelation{
			JournalKind: protocol.JournalSession,
			JournalID:   "session-1",
			SessionID:   "session-1",
			ActivityID:  activity,
			TurnID:      turn,
		},
		Time:           time.Unix(1, 0).UTC(),
		Kind:           kind,
		Classification: "durable",
		PayloadVersion: 1,
		Payload:        raw,
	}
}

func TestConsumeLegacyProtocolEventsAutomaticCompactionContinuesTurnStream(t *testing.T) {
	activity := protocol.ActivityID("auto")
	turn := protocol.TurnID("turn")
	rangeValue := protocol.ContextCompactedV1{
		From:              protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "session-1", CommitSeq: 1, TransactionID: "a"},
		Through:           protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "session-1", CommitSeq: 2, TransactionID: "b"},
		SummaryEvidenceID: "e",
		Revision:          "r",
	}
	sub := &scriptedSubscription{items: []protocol.SubscriptionItem{
		{Event: ptrApplicationEvent(streamEvent(protocol.EventActivityPlanned, activity, turn, map[string]any{
			"kind": "provider", "purpose": "summary", "compaction_trigger": "automatic",
		}))},
		{Event: ptrApplicationEvent(streamEvent(protocol.EventActivityStarted, activity, turn, map[string]any{}))},
		{Event: ptrApplicationEvent(streamEvent(protocol.EventActivitySucceeded, activity, turn, protocol.ActivityOutcomeV1{Status: "succeeded"}))},
		{Event: ptrApplicationEvent(streamEvent(protocol.EventContextCompacted, activity, turn, rangeValue))},
		{Event: ptrApplicationEvent(streamEvent(protocol.EventAssistantMessage, "", turn, protocol.AssistantMessageV1{
			Blocks: []protocol.ContentBlock{{Kind: protocol.ContentText, Text: "after compact"}},
		}))},
		{Event: ptrApplicationEvent(streamEvent(protocol.EventTurnCompleted, "", turn, map[string]any{}))},
	}}
	adapter := NewLegacyAdapter(LegacyAdapterOptions{Actor: protocol.ActorRef{ID: "u", Kind: protocol.ActorUser}})
	destination := make(chan Event, 8)
	consumeLegacyProtocolEvents(context.Background(), sub, adapter, destination, false, nil)
	close(destination)
	var progress, text, terminal int
	var contexts []*protocol.ContextProjectionV1
	stateAt, textAt, terminalAt := -1, -1, -1
	index := 0
	for event := range destination {
		switch event.Kind {
		case EventError:
			t.Fatalf("stream error=%v", event.Message)
		case EventCompactionStarted, EventCompactionProgress:
			progress++
		case EventState:
			if event.Context != nil {
				contexts = append(contexts, event.Context)
				stateAt = index
			}
		case EventTextDelta:
			if event.Runtime.Text == "after compact" {
				text++
				textAt = index
			}
		case EventTurnCompleted:
			terminal++
			terminalAt = index
		case EventCompactionCompleted, EventCompactionFailed:
			t.Fatalf("early automatic terminal=%+v", event)
		}
		index++
	}
	if progress < 2 || len(contexts) != 1 || text != 1 || terminal != 1 || !(stateAt < textAt && textAt < terminalAt) || !sub.closed {
		t.Fatalf("progress=%d contexts=%d text=%d terminal=%d order=%d,%d,%d closed=%t", progress, len(contexts), text, terminal, stateAt, textAt, terminalAt, sub.closed)
	}
	context := contexts[0]
	if context.Revision != rangeValue.Revision || context.SummaryEvidenceID != rangeValue.SummaryEvidenceID || context.LatestRange == nil || context.LatestRange.From != rangeValue.From || context.LatestRange.Through != rangeValue.Through {
		t.Fatalf("context = %+v, want compaction range %+v", context, rangeValue)
	}
}

func ptrApplicationEvent(event protocol.ApplicationEvent) *protocol.ApplicationEvent { return &event }
