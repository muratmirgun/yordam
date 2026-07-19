package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/muratmirgun/yordam/internal/protocol"
)

func DurableContext(snapshot protocol.ApplicationSnapshot) (*protocol.ContextProjectionV1, error) {
	view := snapshot.Durable.Context
	if view.State == protocol.ValueUnknown || view.State == protocol.ValueUnavailable {
		return nil, nil
	}
	if view.State != protocol.ValueKnown {
		return nil, fmt.Errorf("invalid durable context state %q", view.State)
	}
	if view.Kind != "context" {
		return nil, fmt.Errorf("invalid durable context kind %q", view.Kind)
	}
	if view.Status != "ready" {
		return nil, fmt.Errorf("invalid durable context status %q", view.Status)
	}
	var state protocol.ContextProjectionV1
	decoder := json.NewDecoder(bytes.NewReader(view.Data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return nil, err
	}
	if err := expectEOF(decoder); err != nil {
		return nil, err
	}
	if err := validateDurableContext(state); err != nil {
		return nil, err
	}
	copy := protocol.DeepCopy(state)
	return &copy, nil
}

func validateDurableContext(state protocol.ContextProjectionV1) error {
	if state.AutoReason == "" || state.OutputReserve < 0 {
		return fmt.Errorf("invalid durable context data")
	}
	for _, value := range []protocol.ValueInt64{state.EstimatedInputTokens, state.ContextWindow, state.ReserveTokens} {
		if err := value.Validate(); err != nil {
			return fmt.Errorf("invalid durable context value: %w", err)
		}
	}
	available := state.AutoReason == "below_threshold" || state.AutoReason == "threshold_reached"
	switch state.AutoReason {
	case "disabled", "unknown_context_window", "unknown_generation", "invalid_budget", "below_threshold", "threshold_reached":
	default:
		return fmt.Errorf("invalid durable context reason %q", state.AutoReason)
	}
	if state.AutoAvailable != available {
		return fmt.Errorf("durable context availability contradicts reason %q", state.AutoReason)
	}
	if available && state.ReserveTokens.State != protocol.ValueKnown {
		return fmt.Errorf("available durable context requires known reserve")
	}
	if !available && state.ReserveTokens.State == protocol.ValueKnown {
		return fmt.Errorf("unavailable durable context cannot have known reserve")
	}
	if state.AutoReason == "unknown_context_window" && state.ContextWindow.State == protocol.ValueKnown {
		return fmt.Errorf("unknown context window reason has known window")
	}
	if state.LatestRange != nil {
		from, through := state.LatestRange.From, state.LatestRange.Through
		if from.Validate() != nil || through.Validate() != nil || from.JournalKind != protocol.JournalSession || through.JournalKind != protocol.JournalSession || from.JournalID != through.JournalID || from.CommitSeq > through.CommitSeq || state.SummaryEvidenceID == "" || state.Revision == "" {
			return fmt.Errorf("invalid durable context range")
		}
	}
	return nil
}

func expectEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("unexpected trailing durable context JSON")
}
