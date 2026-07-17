package jsonl_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestReadRangeStartsStrictlyAfterCursor(t *testing.T) {
	fixture := newV2Journal(t)
	first := appendCommitted(t, fixture, fixture.head, "txn-a", "evt-a")
	second := appendCommitted(t, fixture, first.Cursor, "txn-b", "evt-b")
	page, err := fixture.repo.ReadRange(context.Background(), journal.ReadRangeRequest{
		Journal: fixture.ref, After: first.Cursor, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || page.Events[0].Envelope.EventID != "evt-b" {
		t.Fatalf("events=%+v", page.Events)
	}
	if page.Cursor != second.Cursor || page.Head != second.Cursor || page.More {
		t.Fatalf("page cursor/head=%+v/%+v more=%v", page.Cursor, page.Head, page.More)
	}
}

func TestReadRangeRejectsBoundsAndForeignOrUncommittedCursors(t *testing.T) {
	fixture := newV2Journal(t)
	for _, limit := range []int{0, -1, 1001} {
		if _, err := fixture.repo.ReadRange(context.Background(), journal.ReadRangeRequest{Journal: fixture.ref, Limit: limit}); err == nil {
			t.Fatalf("limit %d accepted", limit)
		}
	}
	foreign := fixture.head
	foreign.JournalID = "other"
	if _, err := fixture.repo.ReadRange(context.Background(), journal.ReadRangeRequest{Journal: fixture.ref, After: foreign, Limit: 1}); err == nil {
		t.Fatal("foreign range cursor accepted")
	}
	uncommitted := fixture.head
	uncommitted.CommitSeq++
	uncommitted.TransactionID = "txn-not-a-marker"
	if _, err := fixture.repo.ReadRange(context.Background(), journal.ReadRangeRequest{Journal: fixture.ref, After: uncommitted, Limit: 1}); err == nil {
		t.Fatal("uncommitted range cursor accepted")
	}
}

func TestRepositoryResultsDoNotExposeMutableAliases(t *testing.T) {
	fixture := newV2Journal(t)
	event := proposed("evt-a", protocol.EventTaskCreated)
	event.SessionID = protocol.SessionID(fixture.ref.ID)
	request := journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: fixture.head,
		TransactionID: "txn-a", Events: []protocol.ProposedEvent{event},
	}
	result, err := fixture.repo.AppendBatch(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	wantPayload := protocol.CloneRawMessage(result.Events[0].Payload)
	request.Events[0].Payload[0] ^= 1
	result.Events[0].Payload[0] ^= 1

	committed, err := fixture.repo.ReadCommittedTransaction(context.Background(), fixture.ref, "txn-a")
	if err != nil {
		t.Fatal(err)
	}
	if string(committed.Events[0].Payload) != string(wantPayload) {
		t.Fatal("append request/result mutation aliased durable transaction")
	}
	committed.Events[0].Payload[0] ^= 1
	again, err := fixture.repo.ReadCommittedTransaction(context.Background(), fixture.ref, "txn-a")
	if err != nil {
		t.Fatal(err)
	}
	if string(again.Events[0].Payload) != string(wantPayload) {
		t.Fatal("committed transaction result aliased a later read")
	}

	page, err := fixture.repo.ReadRange(context.Background(), journal.ReadRangeRequest{Journal: fixture.ref, After: fixture.head, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	decoded, ok := page.Events[0].Decoded.(*protocol.TaskCreatedV1)
	if !ok {
		t.Fatalf("decoded type=%T", page.Events[0].Decoded)
	}
	decoded.Goal = "mutated"
	page.Events[0].RawEnvelope[0] ^= 1
	page.Events[0].Envelope.Payload[0] ^= 1
	inspection, err := fixture.repo.Inspect(context.Background(), fixture.ref)
	if err != nil {
		t.Fatal(err)
	}
	last := inspection.Events[len(inspection.Events)-1]
	decodedAgain, ok := last.Decoded.(*protocol.TaskCreatedV1)
	if !ok || decodedAgain.Goal == "mutated" || !json.Valid(last.RawEnvelope) || string(last.Envelope.Payload) != string(wantPayload) {
		t.Fatalf("range aliases leaked into inspection: %+v", last)
	}
}

func TestReadRangeClearsMoreAfterConsumingAllCommits(t *testing.T) {
	fixture := newV2Journal(t)
	first := appendCommitted(t, fixture, fixture.head, "txn-a", "evt-a")
	second := appendCommitted(t, fixture, first.Cursor, "txn-b", "evt-b")
	page, err := fixture.repo.ReadRange(context.Background(), journal.ReadRangeRequest{
		Journal: fixture.ref, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if page.More || page.Cursor != second.Cursor {
		t.Fatalf("page more=%v cursor=%+v want final cursor=%+v", page.More, page.Cursor, second.Cursor)
	}
}
