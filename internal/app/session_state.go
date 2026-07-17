package app

import (
	"encoding/json"
	"sort"

	"github.com/muratmirgun/yordam/internal/domain"
)

type SessionState struct {
	Mode      domain.PermissionMode
	Selection domain.ModelSelection
}

func ProjectSessionState(replay domain.SessionReplay) SessionState {
	state := SessionState{Mode: replay.Session.Mode, Selection: replay.Session.Selection}
	events := append([]domain.DurableEvent(nil), replay.Events...)
	sort.SliceStable(events, func(left, right int) bool {
		return events[left].Seq < events[right].Seq
	})
	for _, event := range events {
		switch event.Kind {
		case domain.EventModeChanged:
			var payload domain.ModeChangedPayload
			if json.Unmarshal(event.Payload, &payload) == nil && payload.Mode.Validate() == nil {
				state.Mode = payload.Mode
			}
		case domain.EventModelChanged:
			var payload domain.ModelChangedPayload
			if json.Unmarshal(event.Payload, &payload) == nil && payload.Selection.Profile != "" && payload.Selection.Model != "" {
				state.Selection = payload.Selection
			}
		}
	}
	return state
}
