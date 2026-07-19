// Package compaction provides deterministic, journal-only inputs for context compaction.
package compaction

import (
	"fmt"

	"github.com/muratmirgun/yordam/internal/protocol"
)

const minCompactReserveTokens int64 = 2_048

type Trigger string

const (
	TriggerManual    Trigger = "manual"
	TriggerAutomatic Trigger = "automatic"
)

type Policy struct {
	AutoCompact          bool
	CompactReserveTokens *int64
	ManualInputBytes     int
}

type Decision struct {
	Available     bool
	ShouldCompact bool
	ReserveTokens int64
	Reason        string
}

// Evaluate determines whether automatic compaction must occur before dispatch.
// It consumes only explicit model budget facts and configuration.
func Evaluate(estimatedInput, outputReserve int64, window protocol.ValueInt64, policy Policy) (Decision, error) {
	invalid := func(detail string) (Decision, error) {
		return Decision{Reason: "invalid_budget"}, fmt.Errorf("invalid compaction budget: %s", detail)
	}
	if estimatedInput < 0 || outputReserve < 0 {
		return invalid("negative input or output reserve")
	}
	if policy.CompactReserveTokens != nil && *policy.CompactReserveTokens <= 0 {
		return invalid("non-positive configured reserve")
	}
	if !policy.AutoCompact {
		return Decision{Reason: "disabled"}, nil
	}
	if window.State != protocol.ValueKnown {
		return Decision{Reason: "unknown_context_window"}, nil
	}
	if window.Value <= 0 {
		return invalid("non-positive context window")
	}
	reserve := window.Value / 10
	if reserve < minCompactReserveTokens {
		reserve = minCompactReserveTokens
	}
	if policy.CompactReserveTokens != nil {
		reserve = *policy.CompactReserveTokens
	}
	if reserve+outputReserve >= window.Value {
		return invalid("compact and output reserves do not fit")
	}
	decision := Decision{Available: true, ReserveTokens: reserve, Reason: "below_threshold"}
	if estimatedInput+outputReserve >= window.Value-reserve {
		decision.ShouldCompact = true
		decision.Reason = "threshold_reached"
	}
	return decision, nil
}
