package components

import (
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestContextMergesPlanAndCompletionPartialsWithoutRetainingStaleFullState(t *testing.T) {
	panel := NewContext()
	oldRange := &protocol.CompactionRange{From: protocol.CommittedCursor{JournalID: "old", CommitSeq: 1}, Through: protocol.CommittedCursor{JournalID: "old", CommitSeq: 2}}
	panel.SetCompactionContext(protocol.ContextProjectionV1{
		AutoAvailable: true, AutoReason: "below_threshold",
		EstimatedInputTokens: protocol.ValueInt64{State: protocol.ValueKnown, Value: 10},
		ContextWindow:        protocol.ValueInt64{State: protocol.ValueKnown, Value: 100},
		OutputReserve:        7,
		ReserveTokens:        protocol.ValueInt64{State: protocol.ValueKnown, Value: 20},
		LatestRange:          oldRange, Revision: "old-revision", SummaryEvidenceID: "old-evidence",
	})
	panel.SetCompactionContext(protocol.ContextProjectionV1{
		EstimatedInputTokens: protocol.ValueInt64{State: protocol.ValueKnown, Value: 50},
		ContextWindow:        protocol.ValueInt64{State: protocol.ValueKnown, Value: 120},
		OutputReserve:        0,
		Revision:             "plan-revision",
	})
	newRange := &protocol.CompactionRange{From: protocol.CommittedCursor{JournalID: "new", CommitSeq: 3}, Through: protocol.CommittedCursor{JournalID: "new", CommitSeq: 4}}
	panel.SetCompactionContext(protocol.ContextProjectionV1{LatestRange: newRange, Revision: "completed-revision", SummaryEvidenceID: "new-evidence"})

	got := panel.compaction
	if got == nil || got.AutoAvailable != true || got.AutoReason != "below_threshold" || got.EstimatedInputTokens.Value != 50 || got.ContextWindow.Value != 120 || got.ReserveTokens.Value != 20 || got.OutputReserve != 0 || got.LatestRange == nil || got.LatestRange.Through.CommitSeq != 4 || got.Revision != "completed-revision" || got.SummaryEvidenceID != "new-evidence" {
		t.Fatalf("merged context=%+v", got)
	}
	if view := panel.View(); strings.Contains(view, "provider-body-sentinel") || strings.Contains(view, "summary-body-sentinel") {
		t.Fatalf("context leaked body: %s", view)
	}

	panel.SetCompactionContext(protocol.ContextProjectionV1{
		AutoAvailable: false, AutoReason: "unknown_context_window",
		EstimatedInputTokens: protocol.ValueInt64{State: protocol.ValueUnknown},
		ContextWindow:        protocol.ValueInt64{State: protocol.ValueUnknown},
		OutputReserve:        0,
		ReserveTokens:        protocol.ValueInt64{State: protocol.ValueUnknown},
	})
	got = panel.compaction
	if got.AutoAvailable || got.AutoReason != "unknown_context_window" || got.EstimatedInputTokens.State != protocol.ValueUnknown || got.ContextWindow.State != protocol.ValueUnknown || got.ReserveTokens.State != protocol.ValueUnknown || got.OutputReserve != 0 || got.LatestRange != nil || got.Revision != "" || got.SummaryEvidenceID != "" {
		t.Fatalf("full replacement=%+v", got)
	}
}
