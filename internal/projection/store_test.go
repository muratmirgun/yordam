package projection_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/projection"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestRebuildAfterSnapshotDeletionOrVersionMismatchIsCanonicalAndHeadExact(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session"}
	head := cursor(2, "txn-1")
	repository := &fakeRepository{inspection: journal.Inspection{Journal: ref, Head: head, Writable: true, Events: []protocol.EventRecord{numberCommitted(2, 1, "txn-1")}}}
	cache := projection.NewMemoryCache[int]()
	store := projection.New(repository, numberProjector{version: 7}, cache)

	first, err := store.Rebuild(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	firstBytes, err := projection.Canonical(first)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Delete(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	second, err := store.Load(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, _ := projection.Canonical(second)
	if !reflect.DeepEqual(firstBytes, secondBytes) || second.Head != head {
		t.Fatalf("rebuilt snapshot differs\nfirst=%s\nsecond=%s", firstBytes, secondBytes)
	}

	stale := second
	stale.ProjectionVersion = 6
	if err := cache.Store(context.Background(), ref, stale); err != nil {
		t.Fatal(err)
	}
	third, err := store.Load(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	thirdBytes, _ := projection.Canonical(third)
	if !reflect.DeepEqual(firstBytes, thirdBytes) || repository.inspectCalls != 3 {
		t.Fatalf("version mismatch did not rebuild: calls=%d bytes=%s", repository.inspectCalls, thirdBytes)
	}
}

func TestIncrementalProjectionReadsStrictlyAfterSavedCursorWithoutInspect(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session"}
	firstHead := cursor(2, "txn-1")
	secondHead := cursor(4, "txn-2")
	repository := &fakeRepository{inspection: journal.Inspection{Journal: ref, Head: firstHead, Writable: true, Events: []protocol.EventRecord{numberCommitted(2, 1, "txn-1")}}}
	store := projection.New(repository, numberProjector{version: 1}, projection.NewMemoryCache[int]())
	snapshot, err := store.Rebuild(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	repository.pages = []journal.EventPage{{Events: []protocol.EventRecord{numberCommitted(3, 3, "txn-2")}, Cursor: secondHead, Head: secondHead}}
	updated, err := store.Update(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if updated.State != 5 || updated.Head != secondHead || repository.inspectCalls != 1 || len(repository.rangeRequests) != 1 || repository.rangeRequests[0].After != firstHead {
		t.Fatalf("updated=%+v inspect=%d ranges=%+v", updated, repository.inspectCalls, repository.rangeRequests)
	}
}

func TestUnknownStatefulEventStopsAtValidatedPrefixAndIsNotCachedWritable(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session"}
	repository := &fakeRepository{inspection: journal.Inspection{
		Journal: ref, Head: cursor(2, "txn-1"), Writable: false,
		Events: []protocol.EventRecord{numberCommitted(2, 1, "txn-1")}, Diagnostics: []protocol.Diagnostic{{Code: "journal.unknown_event", Message: "future state", Journal: ref}},
	}}
	cache := projection.NewMemoryCache[int]()
	store := projection.New(repository, numberProjector{version: 1}, cache)
	snapshot, err := store.Rebuild(context.Background(), ref)
	if !errors.Is(err, projection.ErrReadOnlyPrefix) || snapshot.State != 2 || snapshot.Head != repository.inspection.Head {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	if _, ok, err := cache.Load(context.Background(), ref); err != nil || ok {
		t.Fatalf("read-only snapshot cached: ok=%v err=%v", ok, err)
	}
}

func TestRebuildRejectsStateThatCannotBeCanonicallyDigested(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session"}
	repository := &fakeRepository{inspection: journal.Inspection{
		Journal: ref, Head: cursor(2, "txn-1"), Writable: true, Events: []protocol.EventRecord{numberCommitted(2, 1, "txn-1")},
	}}
	store := projection.New[json.RawMessage](repository, invalidJSONProjector{}, projection.NewMemoryCache[json.RawMessage]())
	if _, err := store.Rebuild(context.Background(), ref); err == nil {
		t.Fatal("snapshot with noncanonical state was accepted")
	}
}

func TestRebuildStopsAtLastCompleteProjectionTransaction(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session"}
	repository := &fakeRepository{inspection: journal.Inspection{
		Journal: ref, Head: cursor(4, "txn-2"), Writable: true,
		Events: []protocol.EventRecord{numberCommitted(2, 1, "txn-1"), numberCommitted(-1, 3, "txn-2")},
	}}
	store := projection.New(repository, numberProjector{version: 1}, projection.NewMemoryCache[int]())
	snapshot, err := store.Rebuild(context.Background(), ref)
	if !errors.Is(err, projection.ErrReadOnlyPrefix) || snapshot.State != 2 || snapshot.Head != cursor(2, "txn-1") {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
}

func TestIncrementalProjectionStopsAtLastCompleteProjectionTransaction(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session"}
	repository := &fakeRepository{inspection: journal.Inspection{
		Journal: ref, Head: cursor(2, "txn-1"), Writable: true, Events: []protocol.EventRecord{numberCommitted(2, 1, "txn-1")},
	}}
	store := projection.New(repository, numberProjector{version: 1}, projection.NewMemoryCache[int]())
	snapshot, err := store.Rebuild(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	repository.pages = []journal.EventPage{{
		Events: []protocol.EventRecord{numberCommitted(3, 3, "txn-2"), numberCommitted(-1, 5, "txn-3")},
		Cursor: cursor(6, "txn-3"), Head: cursor(6, "txn-3"),
	}}
	updated, err := store.Update(context.Background(), snapshot)
	if !errors.Is(err, projection.ErrReadOnlyPrefix) || updated.State != 5 || updated.Head != cursor(4, "txn-2") {
		t.Fatalf("updated=%+v err=%v", updated, err)
	}
}

func TestIncrementalProjectionRejectsFinalPageThatDoesNotReachReportedHead(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session"}
	repository := &fakeRepository{inspection: journal.Inspection{
		Journal: ref, Head: cursor(2, "txn-1"), Writable: true, Events: []protocol.EventRecord{numberCommitted(2, 1, "txn-1")},
	}}
	store := projection.New(repository, numberProjector{version: 1}, projection.NewMemoryCache[int]())
	snapshot, err := store.Rebuild(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	repository.pages = []journal.EventPage{{
		Events: []protocol.EventRecord{numberCommitted(3, 3, "txn-2")}, Cursor: cursor(4, "txn-2"), Head: cursor(6, "txn-3"),
	}}
	updated, err := store.Update(context.Background(), snapshot)
	if err == nil || updated.Head != snapshot.Head {
		t.Fatalf("updated=%+v err=%v", updated, err)
	}
}

type numberProjector struct{ version uint32 }

func (p numberProjector) Version() uint32              { return p.version }
func (p numberProjector) Zero(protocol.JournalRef) int { return 0 }
func (p numberProjector) Apply(value int, record protocol.EventRecord) (int, error) {
	if record.Decoded == nil {
		return value, errors.New("unknown stateful event")
	}
	number := record.Decoded.(int)
	if number < 0 {
		return value, errors.New("invalid projected transition")
	}
	return value + number, nil
}

type invalidJSONProjector struct{}

func (invalidJSONProjector) Version() uint32 { return 1 }
func (invalidJSONProjector) Zero(protocol.JournalRef) json.RawMessage {
	return json.RawMessage(`{"valid":true}`)
}
func (invalidJSONProjector) Apply(json.RawMessage, protocol.EventRecord) (json.RawMessage, error) {
	return json.RawMessage(`{"unterminated":`), nil
}

type fakeRepository struct {
	inspection    journal.Inspection
	pages         []journal.EventPage
	inspectCalls  int
	rangeRequests []journal.ReadRangeRequest
}

func (r *fakeRepository) Inspect(context.Context, protocol.JournalRef) (journal.Inspection, error) {
	r.inspectCalls++
	return r.inspection, nil
}
func (r *fakeRepository) Head(context.Context, protocol.JournalRef) (protocol.CommittedCursor, error) {
	return r.inspection.Head, nil
}
func (r *fakeRepository) ReadRange(_ context.Context, request journal.ReadRangeRequest) (journal.EventPage, error) {
	r.rangeRequests = append(r.rangeRequests, request)
	if len(r.pages) == 0 {
		return journal.EventPage{Cursor: request.After, Head: request.After}, nil
	}
	page := r.pages[0]
	r.pages = r.pages[1:]
	return page, nil
}
func (r *fakeRepository) AppendBatch(context.Context, journal.AppendRequest) (journal.AppendResult, error) {
	panic("unexpected AppendBatch")
}
func (r *fakeRepository) LookupTransaction(context.Context, protocol.JournalRef, protocol.TransactionID) (journal.TransactionLookup, error) {
	panic("unexpected LookupTransaction")
}
func (r *fakeRepository) ReadCommittedTransaction(context.Context, protocol.JournalRef, protocol.TransactionID) (journal.CommittedTransaction, error) {
	panic("unexpected ReadCommittedTransaction")
}
func (r *fakeRepository) Recover(context.Context, journal.RecoveryRequest) (journal.RecoveryResult, error) {
	panic("unexpected Recover")
}

func numberRecord(value int) protocol.EventRecord {
	return protocol.EventRecord{Envelope: protocol.EventEnvelope{Kind: "number", Time: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)}, Decoded: value}
}

func numberCommitted(value int, seq uint64, transaction protocol.TransactionID) protocol.EventRecord {
	record := numberRecord(value)
	record.Envelope.Seq = seq
	record.Envelope.TransactionID = transaction
	return record
}

func cursor(seq uint64, transaction protocol.TransactionID) protocol.CommittedCursor {
	return protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "session", CommitSeq: seq, TransactionID: transaction}
}
