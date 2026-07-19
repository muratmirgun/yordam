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

func TestApplicationLegacyAdapterCompactIdentityBindsCursorAndGeneration(t *testing.T) {
	cursor := protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "session-1", CommitSeq: 9, TransactionID: "tx-9"}
	adapter := app.NewLegacyAdapter(app.LegacyAdapterOptions{
		Actor: protocol.ActorRef{ID: "user-1", Kind: protocol.ActorUser}, RuntimeGenerationID: "runtime-a",
		Cursor: func() *protocol.CommandExpectation {
			return &protocol.CommandExpectation{SelectedSessionID: "session-1", Session: &cursor}
		},
	})
	first, err := adapter.Command(app.Command{Kind: app.CommandCompact})
	if err != nil {
		t.Fatal(err)
	}
	second, err := adapter.Command(app.Command{Kind: app.CommandCompact})
	if err != nil {
		t.Fatal(err)
	}
	if first.CommandID != second.CommandID || first.IdempotencyKey != second.IdempotencyKey || first.RequestDigest != second.RequestDigest {
		t.Fatalf("same compact cursor/generation identity changed: first=%+v second=%+v", first, second)
	}
	cursor.CommitSeq++
	cursor.TransactionID = "tx-10"
	changedHead, err := adapter.Command(app.Command{Kind: app.CommandCompact})
	if err != nil {
		t.Fatal(err)
	}
	if changedHead.CommandID == first.CommandID {
		t.Fatal("changed compact cursor reused command identity")
	}
	otherGeneration := app.NewLegacyAdapter(app.LegacyAdapterOptions{Actor: protocol.ActorRef{ID: "user-1", Kind: protocol.ActorUser}, RuntimeGenerationID: "runtime-b", Cursor: adapterCursor("session-1", cursor)})
	changedGeneration, err := otherGeneration.Command(app.Command{Kind: app.CommandCompact})
	if err != nil {
		t.Fatal(err)
	}
	if changedGeneration.CommandID == changedHead.CommandID {
		t.Fatal("changed runtime generation reused command identity")
	}
}

func adapterCursor(session protocol.SessionID, cursor protocol.CommittedCursor) func() *protocol.CommandExpectation {
	return func() *protocol.CommandExpectation {
		return &protocol.CommandExpectation{SelectedSessionID: session, Session: &cursor}
	}
}
