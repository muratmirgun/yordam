package app_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestApplicationLegacyAdapterPreservesCommandAndRedactedEventSemantics(t *testing.T) {
	adapter := app.NewLegacyAdapter(app.LegacyAdapterOptions{Actor: protocol.ActorRef{ID: "user-1", Kind: protocol.ActorUser}, SelectedSessionID: "session-1"})
	command, err := adapter.Command(app.Command{Kind: app.CommandChangeModel, Selection: domain.ModelSelection{Profile: "openai", Model: "gpt"}})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := app.DecodeCommandPayload(command)
	if err != nil {
		t.Fatal(err)
	}
	payload, ok := decoded.(*protocol.ChangeModelCommandV1)
	if !ok || payload.ProviderID != "openai" || payload.ModelID != "gpt" {
		t.Fatalf("payload=%T %+v", decoded, decoded)
	}

	applicationEvent := protocol.ApplicationEvent{
		ProtocolVersion: 1,
		StreamEventID:   "legacy-error",
		Correlation:     protocol.EventCorrelation{JournalKind: protocol.JournalSession, JournalID: "session-1", SessionID: "session-1"},
		Time:            time.Unix(1, 0).UTC(), Kind: app.ApplicationEventError, Classification: "transient", PayloadVersion: 1,
		Payload: json.RawMessage(`{"message":"safe message"}`), Error: &protocol.PublicError{Code: "runtime_failed", Message: "safe message"},
	}
	legacy, err := adapter.Event(applicationEvent)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Kind != app.EventError || legacy.Message != "safe message" || legacy.Err != nil {
		t.Fatalf("legacy=%+v", legacy)
	}
}
