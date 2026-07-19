package app

import (
	"encoding/json"
	"testing"

	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestDurableContextFailsClosed(t *testing.T) {
	validState := protocol.ContextProjectionV1{AutoAvailable: true, AutoReason: "below_threshold", EstimatedInputTokens: protocol.ValueInt64{State: protocol.ValueKnown, Value: 1, Provenance: "estimate"}, ContextWindow: protocol.ValueInt64{State: protocol.ValueKnown, Value: 100, Provenance: "configured"}, ReserveTokens: protocol.ValueInt64{State: protocol.ValueKnown, Value: 2, Provenance: "policy"}, Revision: "r", SummaryEvidenceID: "e", LatestRange: &protocol.CompactionRange{From: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "s", CommitSeq: 1, TransactionID: "a"}, Through: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "s", CommitSeq: 2, TransactionID: "b"}}}
	encode := func(state protocol.ContextProjectionV1) protocol.ApplicationSnapshot {
		t.Helper()
		raw, err := json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		return protocol.ApplicationSnapshot{Durable: protocol.DurableProjection{Context: protocol.ProjectionView{Kind: "context", Status: "ready", State: protocol.ValueKnown, Data: raw}}}
	}
	valid := encode(validState)
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
	for name, mutate := range map[string]func(*protocol.ContextProjectionV1){
		"negative estimate":     func(state *protocol.ContextProjectionV1) { state.EstimatedInputTokens.Value = -1 },
		"missing provenance":    func(state *protocol.ContextProjectionV1) { state.ContextWindow.Provenance = "" },
		"bad reason":            func(state *protocol.ContextProjectionV1) { state.AutoReason = "made_up" },
		"availability mismatch": func(state *protocol.ContextProjectionV1) { state.AutoAvailable = false },
		"reserve contradiction": func(state *protocol.ContextProjectionV1) {
			state.AutoAvailable, state.AutoReason, state.ReserveTokens = false, "disabled", protocol.ValueInt64{State: protocol.ValueKnown, Value: 2, Provenance: "policy"}
		},
		"cross session range": func(state *protocol.ContextProjectionV1) { state.LatestRange.Through.JournalID = "other" },
		"reversed range":      func(state *protocol.ContextProjectionV1) { state.LatestRange.From.CommitSeq = 3 },
	} {
		t.Run(name, func(t *testing.T) {
			state := protocol.DeepCopy(validState)
			mutate(&state)
			if got, err := DurableContext(encode(state)); err == nil || got != nil {
				t.Fatalf("got=%+v err=%v", got, err)
			}
		})
	}
}
