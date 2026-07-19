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
	if state.AutoReason == "" || !state.EstimatedInputTokens.State.Valid() || !state.ContextWindow.State.Valid() || !state.ReserveTokens.State.Valid() || state.OutputReserve < 0 {
		return nil, fmt.Errorf("invalid durable context data")
	}
	if state.LatestRange != nil && (state.LatestRange.From.Validate() != nil || state.LatestRange.Through.Validate() != nil) {
		return nil, fmt.Errorf("invalid durable context range")
	}
	copy := protocol.DeepCopy(state)
	return &copy, nil
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
