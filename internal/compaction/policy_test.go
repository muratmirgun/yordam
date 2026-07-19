package compaction_test

import (
	"testing"

	"github.com/muratmirgun/yordam/internal/compaction"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestEvaluateThresholdsAndReserves(t *testing.T) {
	t.Parallel()
	explicit := int64(3_000)
	tests := []struct {
		name                string
		estimated, output   int64
		window              protocol.ValueInt64
		policy              compaction.Policy
		wantAvailable, want bool
		wantReserve         int64
		wantReason          string
	}{
		{name: "equality at threshold", estimated: 7_952, output: 0, window: knownWindow(10_000), policy: compaction.Policy{AutoCompact: true}, wantAvailable: true, want: true, wantReserve: 2_048, wantReason: "threshold_reached"},
		{name: "one token below", estimated: 7_951, output: 0, window: knownWindow(10_000), policy: compaction.Policy{AutoCompact: true}, wantAvailable: true, want: false, wantReserve: 2_048, wantReason: "below_threshold"},
		{name: "default ten percent reserve", estimated: 45_000, output: 0, window: knownWindow(50_000), policy: compaction.Policy{AutoCompact: true}, wantAvailable: true, want: true, wantReserve: 5_000, wantReason: "threshold_reached"},
		{name: "explicit reserve", estimated: 7_000, output: 0, window: knownWindow(10_000), policy: compaction.Policy{AutoCompact: true, CompactReserveTokens: &explicit}, wantAvailable: true, want: true, wantReserve: 3_000, wantReason: "threshold_reached"},
		{name: "auto disabled", estimated: 9_000, output: 0, window: knownWindow(10_000), policy: compaction.Policy{AutoCompact: false}, wantReason: "disabled"},
		{name: "unknown context window", estimated: 9_000, output: 0, window: protocol.ValueInt64{State: protocol.ValueUnknown}, policy: compaction.Policy{AutoCompact: true}, wantReason: "unknown_context_window"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := compaction.Evaluate(test.estimated, test.output, test.window, test.policy)
			if err != nil {
				t.Fatal(err)
			}
			if got.Available != test.wantAvailable || got.ShouldCompact != test.want || got.ReserveTokens != test.wantReserve || got.Reason != test.wantReason {
				t.Fatalf("decision=%+v", got)
			}
		})
	}
}

func TestEvaluateRejectsInvalidBudgets(t *testing.T) {
	t.Parallel()
	reserve := int64(9_000)
	tests := []struct {
		name              string
		estimated, output int64
		policy            compaction.Policy
	}{
		{name: "negative estimated input", estimated: -1, output: 0, policy: compaction.Policy{AutoCompact: true}},
		{name: "negative output reserve", estimated: 0, output: -1, policy: compaction.Policy{AutoCompact: true}},
		{name: "reserve and output do not fit", estimated: 0, output: 1_500, policy: compaction.Policy{AutoCompact: true, CompactReserveTokens: &reserve}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := compaction.Evaluate(test.estimated, test.output, knownWindow(10_000), test.policy)
			if err == nil || got.Reason != "invalid_budget" || got.Available || got.ShouldCompact {
				t.Fatalf("decision=%+v err=%v", got, err)
			}
		})
	}
}

func knownWindow(value int64) protocol.ValueInt64 {
	return protocol.ValueInt64{State: protocol.ValueKnown, Value: value, Provenance: "catalog"}
}
