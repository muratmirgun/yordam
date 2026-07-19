package app

import (
	"encoding/json"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func DurableContext(snapshot protocol.ApplicationSnapshot) (*protocol.ContextProjectionV1, error) {
	view := snapshot.Durable.Context
	if view.State != protocol.ValueKnown || view.Kind != "context" {
		return nil, nil
	}
	var state protocol.ContextProjectionV1
	if err := json.Unmarshal(view.Data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}
