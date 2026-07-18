package jsonl_test

import (
	"context"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	taskprojection "github.com/muratmirgun/yordam/internal/task"
)

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
