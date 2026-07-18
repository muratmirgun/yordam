package jsonl_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/eventcodec"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
	"golang.org/x/sys/unix"
)

func TestLookupTransactionSurvivesLaterCommits(t *testing.T) {
	fixture := newV2Journal(t)
	first := appendCommitted(t, fixture, fixture.head, "txn-a", "evt-a")
	_ = appendCommitted(t, fixture, first.Cursor, "txn-b", "evt-b")
	lookup, err := fixture.repo.LookupTransaction(context.Background(), fixture.ref, "txn-a")
	if err != nil {
		t.Fatal(err)
	}
	if lookup.State != journal.TransactionCommitted || lookup.Cursor != first.Cursor {
		t.Fatalf("lookup=%+v want cursor=%+v", lookup, first.Cursor)
	}
}

func TestLookupTransactionDistinguishesCleanAbsentCommittedAndIncompleteUnknown(t *testing.T) {
	fixture := newV2Journal(t)
	lookup, err := fixture.repo.LookupTransaction(context.Background(), fixture.ref, "txn-absent")
	if err != nil {
		t.Fatal(err)
	}
	if lookup.State != journal.TransactionNotCommitted {
		t.Fatalf("clean absent lookup=%+v", lookup)
	}
	committed := appendCommitted(t, fixture, fixture.head, "txn-a", "evt-a")
	file, err := os.OpenFile(fixture.eventsPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte(`{"schema_version":2,"transaction_id":"txn-incomplete"`)); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	lookup, err = fixture.repo.LookupTransaction(context.Background(), fixture.ref, "txn-a")
	if err != nil {
		t.Fatal(err)
	}
	if lookup.State != journal.TransactionCommitted || lookup.Cursor != committed.Cursor {
		t.Fatalf("committed prefix lookup=%+v", lookup)
	}
	lookup, err = fixture.repo.LookupTransaction(context.Background(), fixture.ref, "txn-incomplete")
	if err != nil {
		t.Fatal(err)
	}
	if lookup.State != journal.TransactionUnknown {
		t.Fatalf("incomplete transaction lookup=%+v", lookup)
	}
	lookup, err = fixture.repo.LookupTransaction(context.Background(), fixture.ref, "txn-other")
	if err != nil {
		t.Fatal(err)
	}
	if lookup.State != journal.TransactionUnknown {
		t.Fatalf("absent ID behind uncertain physical tail lookup=%+v", lookup)
	}
}

func TestTransactionIndexIsVersionedBoundToSourceAndNeverAuthoritative(t *testing.T) {
	fixture := newV2Journal(t)
	committed := appendCommitted(t, fixture, fixture.head, "txn-a", "evt-a")
	indexPath := filepath.Join(filepath.Dir(fixture.eventsPath), "journal.index.json")
	rawIndex, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	var index struct {
		Version      uint32             `json:"version"`
		SourceSize   int64              `json:"source_size"`
		SourceDigest protocol.Digest    `json:"source_digest"`
		EventIDs     []protocol.EventID `json:"event_ids"`
		Transactions []struct {
			TransactionID protocol.TransactionID   `json:"transaction_id"`
			Cursor        protocol.CommittedCursor `json:"cursor"`
			FirstOffset   int64                    `json:"first_offset"`
			EndOffset     int64                    `json:"end_offset"`
		} `json:"transactions"`
	}
	if err := json.Unmarshal(rawIndex, &index); err != nil {
		t.Fatal(err)
	}
	rawJournal, err := os.ReadFile(fixture.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(rawJournal)
	wantDigest := protocol.Digest{Algorithm: protocol.DigestSHA256, Value: hex.EncodeToString(sum[:])}
	if index.Version != 1 || index.SourceSize != int64(len(rawJournal)) || index.SourceDigest != wantDigest {
		t.Fatalf("index source binding=%+v size=%d digest=%+v", index, len(rawJournal), wantDigest)
	}
	if len(index.Transactions) == 0 || index.Transactions[len(index.Transactions)-1].TransactionID != "txn-a" || index.Transactions[len(index.Transactions)-1].Cursor != committed.Cursor {
		t.Fatalf("index transactions=%+v", index.Transactions)
	}

	forged := []byte(`{"version":1,"source_size":1,"source_digest":{"algorithm":"sha256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"transactions":[{"transaction_id":"txn-forged","cursor":{"journal_kind":"session","journal_id":"forged","commit_seq":999,"transaction_id":"txn-forged"},"first_offset":0,"end_offset":1}]}`)
	if err := os.WriteFile(indexPath, forged, 0o600); err != nil {
		t.Fatal(err)
	}
	lookup, err := fixture.repo.LookupTransaction(context.Background(), fixture.ref, "txn-forged")
	if err != nil {
		t.Fatal(err)
	}
	if lookup.State != journal.TransactionNotCommitted {
		t.Fatalf("forged disposable index proved transaction: %+v", lookup)
	}
	rebuilt, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(rebuilt, &index); err != nil {
		t.Fatalf("corrupt index was not rebuilt: %v", err)
	}
	if index.SourceSize != int64(len(rawJournal)) {
		t.Fatalf("rebuilt source_size=%d want %d", index.SourceSize, len(rawJournal))
	}
	index.SourceSize--
	stale, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(indexPath, stale, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.repo.LookupTransaction(context.Background(), fixture.ref, "txn-a"); err != nil {
		t.Fatal(err)
	}
	rebuilt, err = os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(rebuilt, &index); err != nil {
		t.Fatal(err)
	}
	if index.SourceSize != int64(len(rawJournal)) {
		t.Fatalf("stale index source_size=%d want rebuilt %d", index.SourceSize, len(rawJournal))
	}
	if err := os.Remove(indexPath); err != nil {
		t.Fatal(err)
	}
	lookup, err = fixture.repo.LookupTransaction(context.Background(), fixture.ref, "txn-a")
	if err != nil {
		t.Fatal(err)
	}
	if lookup.State != journal.TransactionCommitted || lookup.Cursor != committed.Cursor {
		t.Fatalf("missing index changed authoritative lookup: %+v", lookup)
	}
	if _, err := os.Stat(indexPath); err != nil {
		t.Fatalf("missing index was not rebuilt: %v", err)
	}
}

func TestTransactionIndexSymlinkIsNeverFollowedOrReplaced(t *testing.T) {
	fixture := newV2Journal(t)
	committed := appendCommitted(t, fixture, fixture.head, "txn-a", "evt-a")
	indexPath := filepath.Join(filepath.Dir(fixture.eventsPath), "journal.index.json")
	if err := os.Remove(indexPath); err != nil {
		t.Fatal(err)
	}
	outsidePath := filepath.Join(t.TempDir(), "outside-index.json")
	outside := []byte("outside sentinel")
	if err := os.WriteFile(outsidePath, outside, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outsidePath, indexPath); err != nil {
		t.Fatal(err)
	}
	lookup, err := fixture.repo.LookupTransaction(context.Background(), fixture.ref, "txn-a")
	if err != nil {
		t.Fatal(err)
	}
	if lookup.State != journal.TransactionCommitted || lookup.Cursor != committed.Cursor {
		t.Fatalf("lookup=%+v", lookup)
	}
	after, err := os.ReadFile(outsidePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(outside) {
		t.Fatalf("outside index target changed: %q", after)
	}
	info, err := os.Lstat(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("disposable index symlink was replaced")
	}
}

func TestTransactionIndexFIFONeverBlocksAuthoritativeLookup(t *testing.T) {
	fixture := newV2Journal(t)
	committed := appendCommitted(t, fixture, fixture.head, "txn-a", "evt-a")
	indexPath := filepath.Join(filepath.Dir(fixture.eventsPath), "journal.index.json")
	if err := os.Remove(indexPath); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(indexPath, 0o600); err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		lookup journal.TransactionLookup
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		lookup, err := fixture.repo.LookupTransaction(context.Background(), fixture.ref, "txn-a")
		done <- outcome{lookup: lookup, err: err}
	}()
	select {
	case got := <-done:
		if got.err != nil || got.lookup.State != journal.TransactionCommitted || got.lookup.Cursor != committed.Cursor {
			t.Fatalf("lookup=%+v err=%v", got.lookup, got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("LookupTransaction blocked opening a disposable FIFO index")
	}
}

func TestExactSourceDigestForgedIndexCannotChangeDuplicateDecision(t *testing.T) {
	fixture := newV2Journal(t)
	first := appendCommitted(t, fixture, fixture.head, "txn-a", "evt-a")
	indexPath := filepath.Join(filepath.Dir(fixture.eventsPath), "journal.index.json")
	raw, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	var index struct {
		Version      uint32             `json:"version"`
		SourceSize   int64              `json:"source_size"`
		SourceDigest protocol.Digest    `json:"source_digest"`
		EventIDs     []protocol.EventID `json:"event_ids"`
		Transactions []struct {
			TransactionID protocol.TransactionID   `json:"transaction_id"`
			Cursor        protocol.CommittedCursor `json:"cursor"`
			FirstOffset   int64                    `json:"first_offset"`
			EndOffset     int64                    `json:"end_offset"`
		} `json:"transactions"`
	}
	if err := json.Unmarshal(raw, &index); err != nil {
		t.Fatal(err)
	}
	if len(index.Transactions) < 2 || index.Transactions[0].TransactionID != "txn-bootstrap" {
		t.Fatalf("unexpected index=%+v", index)
	}
	index.Transactions[0].TransactionID = "txn-forged-prefix"
	index.Transactions[0].Cursor.TransactionID = "txn-forged-prefix"
	forged, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(indexPath, forged, 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(fixture.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	event := proposed("evt-duplicate-prefix", protocol.EventTaskCreated)
	event.SessionID = protocol.SessionID(fixture.ref.ID)
	if _, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: first.Cursor,
		TransactionID: "txn-bootstrap", Events: []protocol.ProposedEvent{event},
	}); err == nil {
		t.Fatal("exact-source-digest forged index changed duplicate transaction decision")
	}
	after, err := os.ReadFile(fixture.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("forged index caused a partial journal write")
	}

	if err := json.Unmarshal(raw, &index); err != nil {
		t.Fatal(err)
	}
	index.EventIDs[0] = "evt-forged-prefix"
	forged, err = json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(indexPath, forged, 0o600); err != nil {
		t.Fatal(err)
	}
	event = proposed("evt-bootstrap", protocol.EventTaskCreated)
	event.SessionID = protocol.SessionID(fixture.ref.ID)
	if _, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: first.Cursor,
		TransactionID: "txn-unique", Events: []protocol.ProposedEvent{event},
	}); err == nil {
		t.Fatal("exact-source-digest forged event index changed duplicate event decision")
	}
	after, err = os.ReadFile(fixture.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("forged event index caused a partial journal write")
	}
}

type regressionIndexEntry struct {
	transactionID protocol.TransactionID
	cursor        protocol.CommittedCursor
	firstOffset   int64
	endOffset     int64
}

func writeRegressionIndex(t *testing.T, path string, source []byte, eventIDs []protocol.EventID, entries []regressionIndexEntry) {
	t.Helper()
	sum := sha256.Sum256(source)
	transactions := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		transactions = append(transactions, map[string]any{
			"transaction_id": entry.transactionID,
			"cursor":         entry.cursor,
			"first_offset":   entry.firstOffset,
			"end_offset":     entry.endOffset,
		})
	}
	index := map[string]any{
		"version":       uint32(1),
		"source_size":   int64(len(source)),
		"source_digest": protocol.Digest{Algorithm: protocol.DigestSHA256, Value: hex.EncodeToString(sum[:])},
		"event_ids":     eventIDs,
		"transactions":  transactions,
	}
	raw, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestIndexedScannerRegressionEnforcesGlobalV2BeforeV1Invariant(t *testing.T) {
	fixture := newV2Journal(t)
	source, err := os.ReadFile(fixture.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	firstEnd := int64(len(source))
	source = append(source, legacyLineForRegression(fixture.ref, "legacy-after-v2", 3, `{"value":1}`)...)
	if err := os.WriteFile(fixture.eventsPath, source, 0o600); err != nil {
		t.Fatal(err)
	}
	legacyCursor := protocol.CommittedCursor{
		JournalKind: fixture.ref.Kind, JournalID: fixture.ref.ID, CommitSeq: 3,
		TransactionID: "legacy:legacy-after-v2",
	}
	writeRegressionIndex(t, filepath.Join(filepath.Dir(fixture.eventsPath), "journal.index.json"), source,
		[]protocol.EventID{"evt-bootstrap", "evt-bootstrap-marker", "legacy-after-v2"},
		[]regressionIndexEntry{
			{transactionID: fixture.head.TransactionID, cursor: fixture.head, firstOffset: 0, endOffset: firstEnd},
			{transactionID: legacyCursor.TransactionID, cursor: legacyCursor, firstOffset: firstEnd, endOffset: int64(len(source))},
		},
	)
	if _, err := fixture.repo.LookupTransaction(context.Background(), fixture.ref, legacyCursor.TransactionID); err == nil {
		t.Fatal("exact-digest index bypassed the global v2-to-v1 scanner invariant")
	}
}

func TestIndexedScannerRegressionPreservesPureV1TransitionState(t *testing.T) {
	repo, ref, head, path := newLegacyJournalForRegression(t)
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	eventID := protocol.EventID(strings.TrimPrefix(string(head.TransactionID), "legacy:"))
	writeRegressionIndex(t, filepath.Join(filepath.Dir(path), "journal.index.json"), source,
		[]protocol.EventID{eventID},
		[]regressionIndexEntry{{transactionID: head.TransactionID, cursor: head, firstOffset: 0, endOffset: int64(len(source))}},
	)
	event := proposed("evt-v2-regression", protocol.EventTaskCreated)
	event.SessionID = protocol.SessionID(ref.ID)
	before := bytes.Clone(source)
	_, err = repo.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: ref, ExpectedHead: head, TransactionID: "txn-v2-regression", Events: []protocol.ProposedEvent{event},
	})
	if err == nil || !strings.Contains(err.Error(), "legacy") {
		t.Fatalf("verified pure-v1 index lost compatibility state: %v", err)
	}
	after, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("pure-v1 index allowed a first-v2 mutation before Task 3")
	}
}

func TestVerifiedPrefixRegressionSkipsOldDecodeForLookupAndAppend(t *testing.T) {
	var validations atomic.Int64
	descriptors := eventcodec.FoundationDescriptors()
	for index := range descriptors {
		structural := descriptors[index].ValidateStructural
		descriptors[index].ValidateStructural = func(value any) error {
			validations.Add(1)
			if structural != nil {
				return structural(value)
			}
			return nil
		}
	}
	registry, err := eventcodec.New(descriptors)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newV2JournalWithOptions(t, jsonl.Options{Encoder: passthroughEncoder{}, Registry: registry})
	first := appendCommitted(t, fixture, fixture.head, "txn-prefix-a", "evt-prefix-a")
	validations.Store(0)
	for range 3 {
		lookup, err := fixture.repo.LookupTransaction(context.Background(), fixture.ref, "txn-prefix-a")
		if err != nil || lookup.State != journal.TransactionCommitted || lookup.Cursor != first.Cursor {
			t.Fatalf("lookup=%+v err=%v", lookup, err)
		}
	}
	if got := validations.Load(); got != 0 {
		t.Fatalf("unchanged verified prefix decoded %d times", got)
	}
	validations.Store(0)
	second := appendCommitted(t, fixture, first.Cursor, "txn-prefix-b", "evt-prefix-b")
	firstTailValidations := validations.Load()
	validations.Store(0)
	_ = appendCommitted(t, fixture, second.Cursor, "txn-prefix-c", "evt-prefix-c")
	secondTailValidations := validations.Load()
	if firstTailValidations == 0 || secondTailValidations != firstTailValidations {
		t.Fatalf("append tail validations grew with old prefix: first=%d second=%d", firstTailValidations, secondTailValidations)
	}
}

func copyJournalRootForRestart(t *testing.T, source, destination string) {
	t.Helper()
	err := filepath.Walk(source, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, raw, info.Mode().Perm())
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRestartedCommittedViewsRequireSyncAndRootedVerification(t *testing.T) {
	appendFault := errors.New("marker write unknown")
	fixture := newV2JournalWithFault(t, func(point jsonl.FaultPoint) error {
		if point == jsonl.FaultMarkerWrite {
			return appendFault
		}
		return nil
	})
	event := proposed("evt-restart-uncertain", protocol.EventTaskCreated)
	event.SessionID = protocol.SessionID(fixture.ref.ID)
	result, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: fixture.head,
		TransactionID: "txn-restart-uncertain", Events: []protocol.ProposedEvent{event},
	})
	if !errors.Is(err, appendFault) || result.Status != journal.AppendCommitUnknown {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	restartRoot := filepath.Join(t.TempDir(), "restarted-store")
	copyJournalRootForRestart(t, fixture.root, restartRoot)
	viewFault := errors.New("restarted committed view sync unknown")
	restarted := jsonl.New(restartRoot, jsonl.Options{
		Encoder: passthroughEncoder{},
		Fault: func(point jsonl.FaultPoint) error {
			if point == jsonl.FaultCommittedViewSync {
				return viewFault
			}
			return nil
		},
	})
	lookup, err := restarted.LookupTransaction(context.Background(), fixture.ref, "txn-restart-uncertain")
	if !errors.Is(err, viewFault) || lookup.State == journal.TransactionCommitted {
		t.Fatalf("restart lookup returned unverified committed result: lookup=%+v err=%v", lookup, err)
	}
	if inspection, err := restarted.Inspect(context.Background(), fixture.ref); !errors.Is(err, viewFault) {
		t.Fatalf("restart Inspect exposed unverified marker: inspection=%+v err=%v", inspection, err)
	}
	if head, err := restarted.Head(context.Background(), fixture.ref); !errors.Is(err, viewFault) {
		t.Fatalf("restart Head exposed unverified marker: head=%+v err=%v", head, err)
	}
}
