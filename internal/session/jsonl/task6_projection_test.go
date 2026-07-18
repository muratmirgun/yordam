package jsonl_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	taskprojection "github.com/muratmirgun/yordam/internal/task"
)

func TestTaskSixSessionStateStopsAtRealCommittedTransactionPrefix(t *testing.T) {
	fixture := newV2Journal(t)
	payload, err := canonicaljson.Marshal(protocol.ModeChangedV1{Mode: "safe"})
	if err != nil {
		t.Fatal(err)
	}
	valid, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: fixture.head, TransactionID: "txn-session-state-valid",
		Events: []protocol.ProposedEvent{{
			EventID: "evt-session-state-valid", Time: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC), PayloadVersion: 1,
			Kind: protocol.EventModeChanged, SessionID: protocol.SessionID(fixture.ref.ID), Payload: payload,
		}},
	})
	if err != nil || valid.Status != journal.AppendCommitted {
		t.Fatalf("valid append=%+v err=%v", valid, err)
	}
	fixture.head = valid.Cursor
	appendRawV2Transaction(t, fixture, "txn-session-state-unknown", protocol.EventEnvelope{
		EventID: "evt-session-state-unknown", Time: time.Date(2026, 7, 18, 12, 1, 0, 0, time.UTC),
		Kind: "future.session_policy", Payload: []byte(`{"future":true}`),
	}, nil)

	state, err := app.NewIncrementalSessionStateProjector(fixture.repo).Open(context.Background(), fixture.ref, app.SessionState{Mode: domain.ModeAsk})
	if !errors.Is(err, app.ErrSessionStateReadOnly) {
		t.Fatalf("open error=%v", err)
	}
	if state.Head != valid.Cursor || state.Mode != domain.ModeSafe || state.Writable || len(state.Diagnostics) == 0 {
		t.Fatalf("validated prefix=%+v", state)
	}
}

func TestTaskSixLifecycleRecordsCommitAndProjectFromRepository(t *testing.T) {
	fixture := newV2Journal(t)
	events := []protocol.ProposedEvent{taskSixProposed(t, fixture, "evt-task6-created", protocol.EventTaskCreated, &protocol.TaskCreatedV1{
		Goal: "project committed lifecycle", OutcomeContractID: "contract-task6", ContractVersion: 1,
	})}
	for index, transition := range [][2]string{
		{"draft", "contract_drafting"}, {"contract_drafting", "contract_proposed"}, {"contract_proposed", "contract_frozen"},
		{"contract_frozen", "running"}, {"running", "verifying"}, {"verifying", "partial"},
		{"partial", "reopened"}, {"reopened", "running"},
	} {
		events = append(events, taskSixProposed(t, fixture, protocol.EventID("evt-task6-"+string(rune('a'+index))), protocol.EventTaskStatusChanged, &protocol.TaskStatusChangedV1{
			From: transition[0], To: transition[1],
		}))
	}
	result, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: fixture.head, TransactionID: "txn-task6-lifecycle", Events: events,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != journal.AppendCommitted {
		t.Fatalf("append status=%q", result.Status)
	}
	inspection, err := fixture.repo.Inspect(context.Background(), fixture.ref)
	if err != nil {
		t.Fatal(err)
	}
	projector := taskprojection.Projector{}
	state := projector.Zero(fixture.ref)
	for _, record := range inspection.Events {
		state, err = projector.Apply(state, record)
		if err != nil {
			t.Fatal(err)
		}
	}
	if state.Tasks["task-task6"].State != taskprojection.StateRunning || inspection.Head != result.Cursor {
		t.Fatalf("task=%+v inspection head=%+v result=%+v", state.Tasks["task-task6"], inspection.Head, result.Cursor)
	}
}

func taskSixProposed(t *testing.T, fixture v2JournalFixture, eventID protocol.EventID, kind string, payload any) protocol.ProposedEvent {
	t.Helper()
	raw, err := canonicaljson.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.ProposedEvent{
		EventID: eventID, Time: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC), PayloadVersion: 1,
		Kind: kind, SessionID: protocol.SessionID(fixture.ref.ID), TaskID: "task-task6", Payload: raw,
	}
}
