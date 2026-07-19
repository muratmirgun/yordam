package app_test

import (
	"encoding/json"
	"strings"
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

func TestApplicationLegacyAdapterProjectsManualCompactionLifecycleWithoutProviderContent(t *testing.T) {
	cursor := protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "session-1", CommitSeq: 9, TransactionID: "tx-9"}
	adapter := app.NewLegacyAdapter(app.LegacyAdapterOptions{Actor: protocol.ActorRef{ID: "user-1", Kind: protocol.ActorUser}, RuntimeGenerationID: "runtime-a", Cursor: adapterCursor("session-1", cursor)})
	if _, err := adapter.Command(app.Command{Kind: app.CommandCompact}); err != nil {
		t.Fatal(err)
	}

	activityID := protocol.ActivityID("compact-activity")
	steps := []struct {
		kind string
		body string
		want app.EventKind
	}{
		{protocol.EventActivityPlanned, `{"kind":"provider","purpose":"summarize stable context"}`, app.EventKind("compaction_started")},
		{protocol.EventActivityStarted, `{}`, app.EventKind("compaction_progress")},
		{protocol.EventActivitySucceeded, `{"status":"succeeded"}`, app.EventKind("compaction_progress")},
		{protocol.EventContextCompacted, `{"from":{"journal_kind":"session","journal_id":"session-1","commit_seq":1,"transaction_id":"tx-1"},"through":{"journal_kind":"session","journal_id":"session-1","commit_seq":4,"transaction_id":"tx-4"},"summary_evidence_id":"summary-1","revision":"r1","summary":"provider-secret-body"}`, app.EventKind("compaction_completed")},
	}
	for _, step := range steps {
		legacy, err := adapter.Event(compactionApplicationEvent(step.kind, activityID, step.body))
		if err != nil {
			t.Fatalf("%s: %v", step.kind, err)
		}
		if legacy.Kind != step.want {
			t.Fatalf("%s legacy kind=%q want=%q", step.kind, legacy.Kind, step.want)
		}
		raw, err := json.Marshal(legacy)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "provider-secret-body") {
			t.Fatalf("%s leaked provider content: %s", step.kind, raw)
		}
	}
}

func TestApplicationLegacyAdapterProjectsAutomaticCompactionCancelledAndUncertain(t *testing.T) {
	for _, terminal := range []struct {
		kind string
		want string
	}{
		{protocol.EventActivityCancelled, "cancelled"},
		{protocol.EventActivityUncertain, "uncertain"},
	} {
		t.Run(terminal.want, func(t *testing.T) {
			adapter := app.NewLegacyAdapter(app.LegacyAdapterOptions{Actor: protocol.ActorRef{ID: "user-1", Kind: protocol.ActorUser}})
			activityID := protocol.ActivityID("auto-" + terminal.want)
			if _, err := adapter.Event(compactionApplicationEvent(protocol.EventActivityPlanned, activityID, `{"kind":"provider","purpose":"summarize stable context"}`)); err != nil {
				t.Fatal(err)
			}
			legacy, err := adapter.Event(compactionApplicationEvent(terminal.kind, activityID, `{}`))
			if err != nil {
				t.Fatal(err)
			}
			if legacy.Kind != app.EventKind("compaction_failed") || !strings.Contains(legacy.Message, terminal.want) {
				t.Fatalf("terminal=%+v", legacy)
			}
		})
	}
}

func compactionApplicationEvent(kind string, activityID protocol.ActivityID, body string) protocol.ApplicationEvent {
	return protocol.ApplicationEvent{
		ProtocolVersion: protocol.ApplicationProtocolVersion,
		StreamEventID:   string(kind) + ":" + string(activityID),
		Correlation:     protocol.EventCorrelation{JournalKind: protocol.JournalSession, JournalID: "session-1", SessionID: "session-1", ActivityID: activityID},
		Time:            time.Unix(1, 0).UTC(), Kind: string(kind), Classification: "durable", PayloadVersion: 1, Payload: json.RawMessage(body),
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
