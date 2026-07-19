package app

import (
	"testing"

	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestDurableContextFailsClosed(t *testing.T) {
	valid := protocol.ApplicationSnapshot{Durable: protocol.DurableProjection{Context: protocol.ProjectionView{Kind: "context", Status: "ready", State: protocol.ValueKnown, Data: []byte(`{"auto_reason":"below_threshold","estimated_input_tokens":{"state":"known","value":1},"context_window":{"state":"known","value":100},"reserve_tokens":{"state":"known","value":2},"revision":"r","latest_range":{"from":{"journal_kind":"session","journal_id":"s","commit_seq":1,"transaction_id":"a"},"through":{"journal_kind":"session","journal_id":"s","commit_seq":2,"transaction_id":"b"}}}`)}}}
	got, err := DurableContext(valid)
	if err != nil || got == nil || got.Revision != "r" {
		t.Fatalf("%+v %v", got, err)
	}
	if got.AutoReason != "below_threshold" || got.LatestRange == nil {
		t.Fatalf("decoded=%+v", got)
	}
	got.LatestRange.Through.CommitSeq = 99
	second, err := DurableContext(valid)
	if err != nil || second.LatestRange == nil || second.LatestRange.Through.CommitSeq != 2 {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	for _, snapshot := range []protocol.ApplicationSnapshot{
		{Durable: protocol.DurableProjection{Context: protocol.ProjectionView{Kind: "other", Status: "ready", State: protocol.ValueKnown}}},
		{Durable: protocol.DurableProjection{Context: protocol.ProjectionView{Kind: "context", Status: "failed", State: protocol.ValueKnown}}},
	} {
		got, err := DurableContext(snapshot)
		if err == nil || got != nil {
			t.Fatalf("invalid context got=%+v err=%v", got, err)
		}
	}
	unknown := protocol.ApplicationSnapshot{Durable: protocol.DurableProjection{Context: protocol.ProjectionView{Kind: "context", Status: "ready", State: protocol.ValueUnknown}}}
	if got, err := DurableContext(unknown); err != nil || got != nil {
		t.Fatalf("unknown context got=%+v err=%v", got, err)
	}
	malformed := protocol.ApplicationSnapshot{Durable: protocol.DurableProjection{Context: protocol.ProjectionView{Kind: "context", Status: "ready", State: protocol.ValueKnown, Data: []byte(`{"auto_reason":"below_threshold","estimated_input_tokens":{"state":"invalid"}}`)}}}
	if _, err := DurableContext(malformed); err == nil {
		t.Fatal("accepted malformed")
	}
}
