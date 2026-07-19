package jsonl_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
)

func TestConcurrentCreateDoesNotRemoveLiveCreatorStaging(t *testing.T) {
	root := t.TempDir()
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	clock := newCreateGateClock(time.Date(2026, 7, 18, 13, 0, 0, 0, time.UTC))
	firstStore := jsonl.New(root, jsonl.Options{
		Clock:   clock.Now,
		Entropy: strings.NewReader(strings.Repeat("a", 4096)),
	})
	secondStore := jsonl.New(root, jsonl.Options{
		Clock:   func() time.Time { return time.Date(2026, 7, 18, 13, 0, 1, 0, time.UTC) },
		Entropy: strings.NewReader(strings.Repeat("b", 4096)),
	})
	type result struct {
		session domain.Session
		err     error
	}
	firstDone := make(chan result, 1)
	go func() {
		session, err := firstStore.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
		firstDone <- result{session: session, err: err}
	}()
	select {
	case <-clock.eventID:
	case <-time.After(5 * time.Second):
		t.Fatal("first creator did not reach its live staging gate")
	}
	secondDone := make(chan result, 1)
	go func() {
		session, err := secondStore.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
		secondDone <- result{session: session, err: err}
	}()
	select {
	case got := <-secondDone:
		close(clock.release)
		<-firstDone
		t.Fatalf("second creator crossed live staging cleanup boundary: %+v", got)
	case <-time.After(250 * time.Millisecond):
	}
	close(clock.release)
	first := <-firstDone
	second := <-secondDone
	if first.err != nil || second.err != nil {
		t.Fatalf("concurrent creates: first=%+v second=%+v", first, second)
	}
	listed, err := firstStore.List(context.Background(), workspace)
	if err != nil || len(listed) != 2 {
		t.Fatalf("published sessions=%v err=%v", listed, err)
	}
}

func TestInitializedLockSetFailsClosedWhenTurnLockIsMissing(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(session.ID)}
	head, err := store.Head(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	if err := os.Remove(filepath.Join(sessionDir, "turn.lock")); err != nil {
		t.Fatal(err)
	}
	inspection, err := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}}).Inspect(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Writable {
		t.Fatalf("inspection accepted an initialized journal with missing turn.lock: %+v", inspection)
	}
	if _, err := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}}).AppendBatch(
		context.Background(), legacyCompatibilityRequest(t, ref, head, "txn-missing-turn", "evt-missing-turn"),
	); err == nil {
		t.Fatal("journal mutation accepted a missing turn.lock")
	}
	if _, err := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}}).AcquireTurnLease(
		context.Background(), protocol.SessionID(session.ID), "turn-missing-lock", head,
	); err == nil {
		t.Fatal("turn lease acquisition accepted a missing turn.lock")
	}
	if _, err := os.Lstat(filepath.Join(sessionDir, "turn.lock")); !os.IsNotExist(err) {
		t.Fatalf("ordinary admission recreated missing turn.lock: %v", err)
	}
}

func TestInitializedLockSetFailsClosedWhenBothLocksAreMissing(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(session.ID)}
	head, err := store.Head(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	for _, name := range []string{"journal.lock", "turn.lock"} {
		if err := os.Remove(filepath.Join(sessionDir, name)); err != nil {
			t.Fatal(err)
		}
	}
	fresh := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}})
	inspection, err := fresh.Inspect(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Writable {
		t.Fatalf("inspection mistook an initialized journal for legacy: %+v", inspection)
	}
	if _, err := fresh.AppendBatch(context.Background(), legacyCompatibilityRequest(t, ref, head, "txn-missing-both", "evt-missing-both")); err == nil {
		t.Fatal("journal mutation reinitialized a destroyed lock set")
	}
	for _, name := range []string{"journal.lock", "turn.lock"} {
		if _, err := os.Lstat(filepath.Join(sessionDir, name)); !os.IsNotExist(err) {
			t.Fatalf("ordinary admission recreated %s: %v", name, err)
		}
	}
}

func TestFreshProcessRejectsPersistedLockIdentityReplacement(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(session.ID)}
	head, err := store.Head(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	lockPath := filepath.Join(sessionDir, "journal.lock")
	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lockPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	command := legacyAppendHelperCommand(root, protocol.SessionID(session.ID), head, "fresh-replacement")
	raw, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("fresh helper failed: %v output=%q", err, raw)
	}
	if !strings.HasPrefix(strings.TrimSpace(string(raw)), "ERROR:") {
		t.Fatalf("fresh helper accepted replacement lock: %q", raw)
	}
	got, err := store.Head(context.Background(), ref)
	if err != nil || got != head {
		t.Fatalf("replacement advanced head=%+v err=%v want=%+v", got, err, head)
	}
}

func TestLegacyLockInitializationIsExplicitIdempotentAndCrashResumable(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(session.ID)}
	head, err := store.Head(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	for _, name := range []string{"lock-set.json", "journal.lock", "turn.lock"} {
		if err := os.Remove(filepath.Join(sessionDir, name)); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	wantErr := errors.New("simulated initializer crash")
	faulting := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}, Fault: func(point jsonl.FaultPoint) error {
		if point == jsonl.FaultLockInitializationAfterJournalSync {
			return wantErr
		}
		return nil
	}})
	if err := faulting.InitializeJournalLocks(context.Background(), ref); !errors.Is(err, wantErr) {
		t.Fatalf("faulted initialization error=%v want=%v", err, wantErr)
	}
	if _, err := os.Lstat(filepath.Join(sessionDir, "journal.lock")); err != nil {
		t.Fatalf("crash did not leave resumable journal.lock: %v", err)
	}
	for _, name := range []string{"turn.lock", "lock-set.json"} {
		if _, err := os.Lstat(filepath.Join(sessionDir, name)); !os.IsNotExist(err) {
			t.Fatalf("faulted initialization unexpectedly published %s: %v", name, err)
		}
	}
	partial := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}})
	inspection, err := partial.Inspect(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Writable {
		t.Fatalf("partial lock initialization remained writable: %+v", inspection)
	}
	if _, err := partial.AppendBatch(context.Background(), legacyCompatibilityRequest(t, ref, head, "txn-partial-locks", "evt-partial-locks")); err == nil {
		t.Fatal("ordinary append resumed a partial lock initialization")
	}
	if _, err := partial.AcquireTurnLease(context.Background(), protocol.SessionID(session.ID), "turn-partial-locks", head); err == nil {
		t.Fatal("turn lease resumed a partial lock initialization")
	}
	if err := partial.InitializeJournalLocks(context.Background(), ref); err != nil {
		t.Fatalf("resume initialization: %v", err)
	}
	if err := jsonl.New(root, jsonl.Options{}).InitializeJournalLocks(context.Background(), ref); err != nil {
		t.Fatalf("idempotent initialization: %v", err)
	}
	for _, name := range []string{"journal.lock", "turn.lock", "lock-set.json"} {
		info, err := os.Lstat(filepath.Join(sessionDir, name))
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Fatalf("initialized %s info=%v err=%v", name, info, err)
		}
	}
	inspection, err = jsonl.New(root, jsonl.Options{}).Inspect(context.Background(), ref)
	if err != nil || !inspection.Writable {
		t.Fatalf("resumed lock set inspection=%+v err=%v", inspection, err)
	}
}

func TestTurnLeaseReleaseRequiresTerminalEventInSuppliedTransaction(t *testing.T) {
	root := t.TempDir()
	_, _, session := createTestSession(t, root)
	store := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}})
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(session.ID)}
	initial, err := store.Head(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireTurnLease(context.Background(), protocol.SessionID(session.ID), "turn-release", initial)
	if err != nil {
		t.Fatal(err)
	}
	acceptedPayload, err := canonicaljson.Marshal(protocol.TurnAcceptedV1{CommandID: "command-release", Goal: "verify exact terminal cursor", OutcomeContractID: "contract", ContractVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := store.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: ref, ExpectedHead: initial, TransactionID: "txn-release-accepted",
		Compatibility: &journal.CompatibilityDeclaration{ReaderVersion: protocol.EnvelopeVersion, WriterVersion: protocol.EnvelopeVersion, LegacyHead: initial},
		Events:        []protocol.ProposedEvent{{EventID: "evt-release-accepted", Time: time.Date(2026, 7, 18, 13, 1, 0, 0, time.UTC), PayloadVersion: 1, Kind: protocol.EventTurnAccepted, SessionID: protocol.SessionID(session.ID), TurnID: "turn-release", Payload: acceptedPayload}},
	})
	if err != nil || accepted.Status != journal.AppendCommitted {
		t.Fatalf("accepted append=%+v err=%v", accepted, err)
	}
	terminalPayload, err := canonicaljson.Marshal(protocol.TurnTerminalV1{Status: "completed", Reason: "done"})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := store.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: ref, ExpectedHead: accepted.Cursor, TransactionID: "txn-release-terminal",
		Events: []protocol.ProposedEvent{{EventID: "evt-release-terminal", Time: time.Date(2026, 7, 18, 13, 1, 1, 0, time.UTC), PayloadVersion: 1, Kind: protocol.EventTurnCompleted, SessionID: protocol.SessionID(session.ID), TurnID: "turn-release", Payload: terminalPayload}},
	})
	if err != nil || terminal.Status != journal.AppendCommitted {
		t.Fatalf("terminal append=%+v err=%v", terminal, err)
	}
	unrelatedPayload, err := canonicaljson.Marshal(protocol.TaskCreatedV1{Goal: "unrelated", OutcomeContractID: "contract", ContractVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	unrelated, err := store.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: ref, ExpectedHead: terminal.Cursor, TransactionID: "txn-release-unrelated",
		Events: []protocol.ProposedEvent{{EventID: "evt-release-unrelated", Time: time.Date(2026, 7, 18, 13, 1, 2, 0, time.UTC), PayloadVersion: 1, Kind: protocol.EventTaskCreated, SessionID: protocol.SessionID(session.ID), TaskID: "task-unrelated", Payload: unrelatedPayload}},
	})
	if err != nil || unrelated.Status != journal.AppendCommitted {
		t.Fatalf("unrelated append=%+v err=%v", unrelated, err)
	}
	if err := lease.Release(context.Background(), unrelated.Cursor); !errors.Is(err, journal.ErrTurnNotTerminal) {
		t.Fatalf("Release(unrelated cursor) error=%v want ErrTurnNotTerminal", err)
	}
	if err := lease.Release(context.Background(), terminal.Cursor); err != nil {
		t.Fatalf("Release(earlier terminal cursor) error=%v", err)
	}
}

func TestWorkspaceControlRejectsPartialFinalMetadata(t *testing.T) {
	root := t.TempDir()
	store := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnsureWorkspaceControl(context.Background(), workspace); err != nil {
		t.Fatal(err)
	}
	controlDir := filepath.Join(root, "workspaces", workspace.ID, "control")
	if err := os.RemoveAll(controlDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(controlDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(controlDir, "metadata.json"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}}).EnsureWorkspaceControl(context.Background(), workspace); err == nil {
		t.Fatal("EnsureWorkspaceControl accepted a partially published final layout")
	}
}

func TestWorkspaceControlRecoversAbandonedStagingAndConcurrentCreatorsConverge(t *testing.T) {
	root := t.TempDir()
	store := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnsureWorkspaceControl(context.Background(), workspace); err != nil {
		t.Fatal(err)
	}
	workspaceDir := filepath.Join(root, "workspaces", workspace.ID)
	if err := os.RemoveAll(filepath.Join(workspaceDir, "control")); err != nil {
		t.Fatal(err)
	}
	staging := filepath.Join(workspaceDir, ".yordam-control-"+strings.Repeat("a", 32)+".tmp")
	if err := os.Mkdir(staging, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staging, "metadata.json"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	const creators = 8
	start := make(chan struct{})
	errs := make(chan error, creators)
	var wg sync.WaitGroup
	for range creators {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}}).EnsureWorkspaceControl(context.Background(), workspace)
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent control creator: %v", err)
		}
	}
	if _, err := os.Lstat(staging); !os.IsNotExist(err) {
		t.Fatalf("abandoned control staging remained: %v", err)
	}
	ref := protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: protocol.JournalID(workspace.ID)}
	inspection, err := store.Inspect(context.Background(), ref)
	if err != nil || !inspection.Writable {
		t.Fatalf("published control inspection=%+v err=%v", inspection, err)
	}
}

func TestUnrelatedArtifactOperationsProgressConcurrently(t *testing.T) {
	root := t.TempDir()
	store, workspace, first := createTestSession(t, root)
	second, err := store.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	source := newGateReader([]byte("blocked"))
	firstDone := make(chan error, 1)
	go func() {
		_, err := store.Put(context.Background(), first.ID, "text/plain", source, 100)
		firstDone <- err
	}()
	select {
	case <-source.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first artifact put did not reach source gate")
	}
	secondDone := make(chan error, 1)
	go func() {
		_, err := jsonl.New(root, jsonl.Options{}).Put(context.Background(), second.ID, "text/plain", strings.NewReader("unrelated"), 100)
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(500 * time.Millisecond):
		close(source.release)
		<-firstDone
		<-secondDone
		t.Fatal("unrelated artifact operation waited on process-wide root lock")
	}
	close(source.release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}
