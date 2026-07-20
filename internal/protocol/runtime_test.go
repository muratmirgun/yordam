package protocol_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestRuntimeLimitsPreserveAutomaticCompactionConfiguration(t *testing.T) {
	limits := protocol.RuntimeLimits{
		MaxToolCalls:             4,
		ShellTimeoutNanos:        1,
		ApplicationQueueCapacity: 8,
		AutoCompact:              true,
		CompactReserveTokens: protocol.ValueInt64{
			State: protocol.ValueKnown, Value: 2_048, Provenance: "runtime-policy",
		},
	}
	copy := protocol.DeepCopy(limits)
	if copy != limits {
		t.Fatalf("deep copy=%+v want=%+v", copy, limits)
	}
	raw, err := json.Marshal(limits)
	if err != nil {
		t.Fatal(err)
	}
	var decoded protocol.RuntimeLimits
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != limits {
		t.Fatalf("json round trip=%+v want=%+v", decoded, limits)
	}
}

func TestRuntimeLimitsPreserveSubagentConfiguration(t *testing.T) {
	limits := protocol.RuntimeLimits{
		MaxToolCalls: 4, ShellTimeoutNanos: 1, ApplicationQueueCapacity: 8,
		Subagents: protocol.SubagentLimits{Enabled: true, MaxPerTurn: 4, MaxToolCalls: 16, TimeoutNanos: 600_000_000_000},
	}
	copy := protocol.DeepCopy(limits)
	if copy != limits {
		t.Fatalf("deep copy=%+v want=%+v", copy, limits)
	}
	raw, err := json.Marshal(limits)
	if err != nil {
		t.Fatal(err)
	}
	var decoded protocol.RuntimeLimits
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != limits {
		t.Fatalf("json round trip=%+v want=%+v", decoded, limits)
	}
}

func TestSubagentLimitsValidate(t *testing.T) {
	tests := []struct {
		name   string
		limits protocol.SubagentLimits
		valid  bool
	}{
		{name: "minimum", limits: protocol.SubagentLimits{MaxPerTurn: 1, MaxToolCalls: 1, TimeoutNanos: int64(time.Second)}, valid: true},
		{name: "maximum", limits: protocol.SubagentLimits{MaxPerTurn: 4, MaxToolCalls: 64, TimeoutNanos: int64(1800 * time.Second)}, valid: true},
		{name: "max per turn too low", limits: protocol.SubagentLimits{MaxPerTurn: 0, MaxToolCalls: 1, TimeoutNanos: int64(time.Second)}},
		{name: "max per turn too high", limits: protocol.SubagentLimits{MaxPerTurn: 5, MaxToolCalls: 1, TimeoutNanos: int64(time.Second)}},
		{name: "tool calls too low", limits: protocol.SubagentLimits{MaxPerTurn: 1, MaxToolCalls: 0, TimeoutNanos: int64(time.Second)}},
		{name: "tool calls too high", limits: protocol.SubagentLimits{MaxPerTurn: 1, MaxToolCalls: 65, TimeoutNanos: int64(time.Second)}},
		{name: "one nanosecond", limits: protocol.SubagentLimits{MaxPerTurn: 1, MaxToolCalls: 1, TimeoutNanos: 1}},
		{name: "one nanosecond over maximum", limits: protocol.SubagentLimits{MaxPerTurn: 1, MaxToolCalls: 1, TimeoutNanos: int64(1800*time.Second) + 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.limits.Validate(); (err == nil) != test.valid {
				t.Fatalf("Validate() error=%v want valid=%t", err, test.valid)
			}
		})
	}
}

func TestRuntimeLimitsValidateDelegatesSubagentBounds(t *testing.T) {
	limits := protocol.RuntimeLimits{
		MaxToolCalls: 1, ShellTimeoutNanos: 1, ApplicationQueueCapacity: 1,
		Subagents: protocol.SubagentLimits{MaxPerTurn: 1, MaxToolCalls: 1, TimeoutNanos: 1},
	}
	if err := limits.Validate(); err == nil {
		t.Fatal("runtime limits accepted invalid subagent timeout")
	}
}
