package protocol_test

import (
	"encoding/json"
	"testing"

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
