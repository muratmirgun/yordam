package jsonl_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
	"github.com/oklog/ulid/v2"
)

func TestWorkspaceFromPathUsesFullSHA256OfCanonicalPath(t *testing.T) {
	realPath := t.TempDir()
	linkPath := filepath.Join(t.TempDir(), "workspace-link")
	if err := os.Symlink(realPath, linkPath); err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.EvalSymlinks(realPath)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err = filepath.Abs(canonical)
	if err != nil {
		t.Fatal(err)
	}

	workspace, err := jsonl.WorkspaceFromPath(linkPath)
	if err != nil {
		t.Fatal(err)
	}
	wantID := fmt.Sprintf("%x", sha256.Sum256([]byte(canonical)))
	if workspace.CanonicalPath != canonical {
		t.Fatalf("canonical path=%q want %q", workspace.CanonicalPath, canonical)
	}
	if workspace.ID != wantID {
		t.Fatalf("workspace ID=%q want full SHA-256 %q", workspace.ID, wantID)
	}
}

func TestCreateAppendAndList(t *testing.T) {
	clock := func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) }
	root := t.TempDir()
	store := jsonl.New(root, jsonl.Options{
		Clock:   clock,
		Entropy: strings.NewReader(strings.Repeat("a", 1024)),
	})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	session, err := store.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{
		Profile: "primary",
		Model:   "model-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ulid.ParseStrict(session.ID); err != nil {
		t.Fatalf("session ID %q is not a ULID: %v", session.ID, err)
	}
	if session.LastSeq != 1 {
		t.Fatalf("created session last seq=%d want 1", session.LastSeq)
	}
	eventsPath := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "events.jsonl")
	createdLog, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	event, err := store.WriteLegacyFixture(context.Background(), session.ID, domain.EventUserMessage, map[string]string{"content": "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if event.Seq != 2 {
		t.Fatalf("seq=%d want 2", event.Seq)
	}
	if !json.Valid(event.Payload) {
		t.Fatal("invalid payload")
	}
	log, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(log, createdLog) {
		t.Fatal("append replaced existing event bytes")
	}
	lines := bytes.Split(bytes.TrimSuffix(log, []byte{'\n'}), []byte{'\n'})
	if len(lines) != 2 {
		t.Fatalf("event lines=%d want 2", len(lines))
	}
	for index, line := range lines {
		var persisted domain.DurableEvent
		if err := json.Unmarshal(line, &persisted); err != nil {
			t.Fatalf("event %d: %v", index, err)
		}
		if err := persisted.Validate(); err != nil {
			t.Fatalf("event %d: %v", index, err)
		}
		if persisted.SchemaVersion != 1 {
			t.Fatalf("event %d schema=%d want 1", index, persisted.SchemaVersion)
		}
		if persisted.Seq != uint64(index+1) {
			t.Fatalf("event %d seq=%d want %d", index, persisted.Seq, index+1)
		}
	}

	sessions, err := store.List(context.Background(), workspace)
	if err != nil || len(sessions) != 1 {
		t.Fatalf("list=%v err=%v", sessions, err)
	}
	metadataPath := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "metadata.json")
	var metadata domain.Session
	rawMetadata, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(rawMetadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.LastSeq != 2 {
		t.Fatalf("metadata last seq=%d want 2", metadata.LastSeq)
	}

	for _, check := range []struct {
		path string
		want os.FileMode
	}{
		{path: filepath.Join(root, "workspaces", workspace.ID), want: 0o700},
		{path: filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID), want: 0o700},
		{path: filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "artifacts"), want: 0o700},
		{path: filepath.Join(root, "workspaces", workspace.ID, "workspace.json"), want: 0o600},
		{path: metadataPath, want: 0o600},
		{path: eventsPath, want: 0o600},
	} {
		info, err := os.Stat(check.path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != check.want {
			t.Fatalf("permissions for %q=%#o want %#o", check.path, got, check.want)
		}
	}
}

func TestReservedSessionIDCreationIsIdempotentAndRejectsCollision(t *testing.T) {
	root := t.TempDir()
	store := jsonl.New(root, jsonl.Options{
		Clock:   func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) },
		Entropy: strings.NewReader(strings.Repeat("a", 1024)),
	})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	id, err := store.ReserveSessionID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "workspaces")); !os.IsNotExist(err) {
		t.Fatalf("reservation wrote storage: %v", err)
	}
	first, err := store.CreateWithIdentity(t.Context(), id, workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.CreateWithIdentity(t.Context(), id, workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"}, nil)
	if err != nil {
		t.Fatalf("idempotent creation failed: %v", err)
	}
	if first != second {
		t.Fatalf("idempotent sessions differ: first=%+v second=%+v", first, second)
	}
	if _, err := store.CreateWithIdentity(t.Context(), id, workspace, domain.ModeAuto, domain.ModelSelection{Profile: "p", Model: "m"}, nil); err == nil || !strings.Contains(err.Error(), "identity collision") {
		t.Fatalf("different content collision err=%v", err)
	}
}

func TestSeparateStoresConcurrentIdentityCreationIsIdempotent(t *testing.T) {
	root := t.TempDir()
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	clock := func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) }
	issuer := jsonl.New(root, jsonl.Options{Clock: clock, Entropy: strings.NewReader(strings.Repeat("a", 1024))})
	id, err := issuer.ReserveSessionID()
	if err != nil {
		t.Fatal(err)
	}
	stores := []*jsonl.Store{
		jsonl.New(root, jsonl.Options{Clock: clock, Entropy: strings.NewReader(strings.Repeat("b", 1024))}),
		jsonl.New(root, jsonl.Options{Clock: clock, Entropy: strings.NewReader(strings.Repeat("c", 1024))}),
	}
	type result struct {
		session domain.Session
		err     error
	}
	results := make(chan result, len(stores))
	start := make(chan struct{})
	var group sync.WaitGroup
	for _, store := range stores {
		group.Add(1)
		go func(store *jsonl.Store) {
			defer group.Done()
			<-start
			session, err := store.CreateWithIdentity(t.Context(), id, workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"}, nil)
			results <- result{session: session, err: err}
		}(store)
	}
	close(start)
	group.Wait()
	close(results)
	var first domain.Session
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if first.ID == "" {
			first = result.session
		} else if result.session != first {
			t.Fatalf("matching identity creation differed: first=%+v got=%+v", first, result.session)
		}
	}
	if first.ID != string(id) {
		t.Fatalf("created ID=%q want %q", first.ID, id)
	}
}

func TestSeparateStoresConcurrentIdentityCollisionCannotOverwrite(t *testing.T) {
	root := t.TempDir()
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	clock := func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) }
	issuer := jsonl.New(root, jsonl.Options{Clock: clock, Entropy: strings.NewReader(strings.Repeat("d", 1024))})
	id, err := issuer.ReserveSessionID()
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		mode    domain.PermissionMode
		session domain.Session
		err     error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	var group sync.WaitGroup
	for index, mode := range []domain.PermissionMode{domain.ModeAsk, domain.ModeAuto} {
		store := jsonl.New(root, jsonl.Options{Clock: clock, Entropy: strings.NewReader(strings.Repeat(string(rune('e'+index)), 1024))})
		group.Add(1)
		go func(store *jsonl.Store, mode domain.PermissionMode) {
			defer group.Done()
			<-start
			session, err := store.CreateWithIdentity(t.Context(), id, workspace, mode, domain.ModelSelection{Profile: "p", Model: "m"}, nil)
			results <- result{mode: mode, session: session, err: err}
		}(store, mode)
	}
	close(start)
	group.Wait()
	close(results)
	var winner result
	failures := 0
	for result := range results {
		if result.err == nil {
			winner = result
			continue
		}
		if !strings.Contains(result.err.Error(), "identity collision") {
			t.Fatalf("unexpected concurrent create error: %v", result.err)
		}
		failures++
	}
	if winner.session.ID != string(id) || failures != 1 {
		t.Fatalf("winner=%+v failures=%d", winner, failures)
	}
	matching, err := issuer.CreateWithIdentity(t.Context(), id, workspace, winner.mode, domain.ModelSelection{Profile: "p", Model: "m"}, nil)
	if err != nil || matching != winner.session {
		t.Fatalf("winning session was not preserved: got=%+v err=%v want=%+v", matching, err, winner.session)
	}
}

func TestIdentityCreationRetriesAfterAbandonedStaging(t *testing.T) {
	root := t.TempDir()
	store := jsonl.New(root, jsonl.Options{
		Clock:   func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) },
		Entropy: strings.NewReader(strings.Repeat("g", 2048)),
	})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(t.Context(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"}); err != nil {
		t.Fatal(err)
	}
	id, err := store.ReserveSessionID()
	if err != nil {
		t.Fatal(err)
	}
	staging := ".yordam-create-" + string(id) + "-" + strings.Repeat("a", 32) + ".tmp"
	stagingPath := filepath.Join(root, "workspaces", workspace.ID, "sessions", staging)
	if err := os.Mkdir(stagingPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stagingPath, "partial"), []byte("interrupted"), 0o600); err != nil {
		t.Fatal(err)
	}
	created, err := store.CreateWithIdentity(t.Context(), id, workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if created.ID != string(id) {
		t.Fatalf("created ID=%q want %q", created.ID, id)
	}
	if _, err := os.Lstat(stagingPath); !os.IsNotExist(err) {
		t.Fatalf("abandoned staging directory remains: %v", err)
	}
	retry, err := store.CreateWithIdentity(t.Context(), id, workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"}, nil)
	if err != nil || retry != created {
		t.Fatalf("retry got=%+v err=%v want=%+v", retry, err, created)
	}
}

func TestWorkspaceIdentityMismatchRejected(t *testing.T) {
	root := t.TempDir()
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	storedWorkspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	workspacePath := filepath.Join(root, "workspaces", workspace.ID, "workspace.json")
	if err := os.MkdirAll(filepath.Dir(workspacePath), 0o700); err != nil {
		t.Fatal(err)
	}
	storedWorkspace.ID = workspace.ID
	original, err := json.Marshal(storedWorkspace)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(workspacePath, original, 0o600); err != nil {
		t.Fatal(err)
	}
	store := jsonl.New(root, jsonl.Options{
		Clock:   func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) },
		Entropy: strings.NewReader(strings.Repeat("a", 1024)),
	})

	_, err = store.Create(
		context.Background(),
		workspace,
		domain.ModeAsk,
		domain.ModelSelection{Profile: "primary", Model: "model-a"},
	)
	if err == nil || !strings.Contains(err.Error(), "workspace identity mismatch") {
		t.Fatalf("error=%v want workspace identity mismatch", err)
	}
	after, readErr := os.ReadFile(workspacePath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(after) != string(original) {
		t.Fatalf("workspace file overwritten: got %q want %q", after, original)
	}
}

func TestCreateAndListRejectInvalidWorkspaceIdentities(t *testing.T) {
	valid, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name      string
		workspace domain.Workspace
	}{
		{name: "traversal", workspace: domain.Workspace{ID: "../escape", CanonicalPath: valid.CanonicalPath}},
		{name: "glob", workspace: domain.Workspace{ID: "*", CanonicalPath: valid.CanonicalPath}},
		{name: "uppercase digest", workspace: domain.Workspace{ID: strings.ToUpper(valid.ID), CanonicalPath: valid.CanonicalPath}},
		{name: "wrong digest", workspace: domain.Workspace{ID: strings.Repeat("0", 64), CanonicalPath: valid.CanonicalPath}},
		{name: "noncanonical path", workspace: func() domain.Workspace {
			noncanonical := valid.CanonicalPath + string(filepath.Separator) + "."
			return domain.Workspace{
				ID:            fmt.Sprintf("%x", sha256.Sum256([]byte(noncanonical))),
				CanonicalPath: noncanonical,
			}
		}()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			store := jsonl.New(root, jsonl.Options{
				Clock:   func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) },
				Entropy: strings.NewReader(strings.Repeat("a", 1024)),
			})
			_, createErr := store.Create(
				context.Background(),
				test.workspace,
				domain.ModeAsk,
				domain.ModelSelection{Profile: "primary", Model: "model-a"},
			)
			if createErr == nil || !strings.Contains(createErr.Error(), "invalid workspace") {
				t.Fatalf("create error=%v want invalid workspace", createErr)
			}
			if _, listErr := store.List(context.Background(), test.workspace); listErr == nil || !strings.Contains(listErr.Error(), "invalid workspace") {
				t.Fatalf("list error=%v want invalid workspace", listErr)
			}
			if _, statErr := os.Stat(filepath.Join(root, "escape")); !os.IsNotExist(statErr) {
				t.Fatalf("workspace traversal created path outside namespace: %v", statErr)
			}
		})
	}
}

func TestAppendRejectsHostileSessionIDs(t *testing.T) {
	tests := []struct {
		name string
		id   func(string) string
	}{
		{name: "glob", id: func(string) string { return "*" }},
		{name: "traversal", id: func(id string) string { return "../" + id }},
		{name: "lowercase", id: strings.ToLower},
		{name: "glob characters", id: func(id string) string { return id[:24] + "[A]" }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, _, session := createTestSession(t, t.TempDir())
			_, err := store.WriteLegacyFixture(
				context.Background(),
				test.id(session.ID),
				domain.EventUserMessage,
				map[string]string{"content": "blocked"},
			)
			if err == nil || !strings.Contains(err.Error(), "invalid session ID") {
				t.Fatalf("error=%v want invalid session ID", err)
			}
		})
	}
}

func TestAppendIgnoresSessionMetadataUnderInvalidWorkspaceDirectory(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	validSessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	hostileSessionDir := filepath.Join(root, "workspaces", "*", "sessions", session.ID)
	if err := os.MkdirAll(hostileSessionDir, 0o700); err != nil {
		t.Fatal(err)
	}
	hostileMetadata, err := os.ReadFile(filepath.Join(validSessionDir, "metadata.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hostileSessionDir, "metadata.json"), hostileMetadata, 0o600); err != nil {
		t.Fatal(err)
	}

	event, err := store.WriteLegacyFixture(
		context.Background(),
		session.ID,
		domain.EventUserMessage,
		map[string]string{"content": "safe"},
	)
	if err != nil {
		t.Fatalf("append rejected valid session because of hostile workspace directory: %v", err)
	}
	if event.Seq != 2 {
		t.Fatalf("seq=%d want 2", event.Seq)
	}
}

func TestAppendRejectsIncompleteTail(t *testing.T) {
	root := t.TempDir()
	store := jsonl.New(root, jsonl.Options{
		Clock:   func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) },
		Entropy: strings.NewReader(strings.Repeat("a", 1024)),
	})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.Create(
		context.Background(),
		workspace,
		domain.ModeAsk,
		domain.ModelSelection{Profile: "primary", Model: "model-a"},
	)
	if err != nil {
		t.Fatal(err)
	}
	eventsPath := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "events.jsonl")
	complete, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	incomplete := complete[:len(complete)-1]
	if err := os.WriteFile(eventsPath, incomplete, 0o600); err != nil {
		t.Fatal(err)
	}

	_, err = store.WriteLegacyFixture(context.Background(), session.ID, domain.EventUserMessage, map[string]string{"content": "hello"})
	if err == nil || !strings.Contains(err.Error(), "incomplete tail") {
		t.Fatalf("error=%v want incomplete tail", err)
	}
	after, readErr := os.ReadFile(eventsPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(after) != string(incomplete) {
		t.Fatal("append changed a log with an incomplete tail")
	}
}

func TestAppendRejectsOversizedEncodedEventWithoutChangingSession(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	eventsPath := filepath.Join(sessionDir, "events.jsonl")
	metadataPath := filepath.Join(sessionDir, "metadata.json")
	beforeLog, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	beforeMetadata, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.WriteLegacyFixture(
		context.Background(),
		session.ID,
		domain.EventUserMessage,
		map[string]string{"content": strings.Repeat("x", 2<<20)},
	)
	if err == nil || !strings.Contains(err.Error(), "exceeds 2 MiB") {
		t.Fatalf("error=%v want event exceeds 2 MiB", err)
	}
	afterLog, readErr := os.ReadFile(eventsPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	afterMetadata, readErr := os.ReadFile(metadataPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(afterLog, beforeLog) || !bytes.Equal(afterMetadata, beforeMetadata) {
		t.Fatal("oversized append changed durable session state")
	}
}

func TestAppendRejectsDurableLogAheadOfMetadata(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	if _, err := store.WriteLegacyFixture(
		context.Background(),
		session.ID,
		domain.EventUserMessage,
		map[string]string{"content": "durable"},
	); err != nil {
		t.Fatal(err)
	}
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	eventsPath := filepath.Join(sessionDir, "events.jsonl")
	metadataPath := filepath.Join(sessionDir, "metadata.json")
	beforeLog, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	var metadata domain.Session
	rawMetadata, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(rawMetadata, &metadata); err != nil {
		t.Fatal(err)
	}
	metadata.LastSeq--
	rolledBackMetadata, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metadataPath, rolledBackMetadata, 0o600); err != nil {
		t.Fatal(err)
	}

	_, err = store.WriteLegacyFixture(context.Background(), session.ID, domain.EventUserMessage, map[string]string{"content": "blocked"})
	if err == nil || !strings.Contains(err.Error(), "session sequence mismatch") {
		t.Fatalf("error=%v want session sequence mismatch", err)
	}
	afterLog, readErr := os.ReadFile(eventsPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	afterMetadata, readErr := os.ReadFile(metadataPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(afterLog, beforeLog) || !bytes.Equal(afterMetadata, rolledBackMetadata) {
		t.Fatal("rejected append changed crash-window state")
	}
}

func TestConcurrentAppendsUseStrictSequence(t *testing.T) {
	root := t.TempDir()
	store := jsonl.New(root, jsonl.Options{
		Clock:   func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) },
		Entropy: strings.NewReader(strings.Repeat("a", 1024)),
	})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.Create(
		context.Background(),
		workspace,
		domain.ModeAsk,
		domain.ModelSelection{Profile: "primary", Model: "model-a"},
	)
	if err != nil {
		t.Fatal(err)
	}

	const appendCount = 8
	type result struct {
		seq uint64
		err error
	}
	results := make(chan result, appendCount)
	start := make(chan struct{})
	var group sync.WaitGroup
	for index := range appendCount {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			event, err := store.WriteLegacyFixture(context.Background(), session.ID, domain.EventUserMessage, map[string]int{"index": index})
			results <- result{seq: event.Seq, err: err}
		}()
	}
	close(start)
	group.Wait()
	close(results)

	sequences := make([]uint64, 0, appendCount)
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		sequences = append(sequences, result.seq)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	for index, sequence := range sequences {
		if want := uint64(index + 2); sequence != want {
			t.Fatalf("sequence[%d]=%d want %d", index, sequence, want)
		}
	}
}

func TestStoresSharingRootSerializeConcurrentAppends(t *testing.T) {
	root := t.TempDir()
	clock := func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) }
	setupStore := jsonl.New(root, jsonl.Options{
		Clock:   clock,
		Entropy: strings.NewReader(strings.Repeat("a", 1024)),
	})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	session, err := setupStore.Create(
		context.Background(),
		workspace,
		domain.ModeAsk,
		domain.ModelSelection{Profile: "primary", Model: "model-a"},
	)
	if err != nil {
		t.Fatal(err)
	}
	stores := []*jsonl.Store{
		jsonl.New(root, jsonl.Options{Clock: clock, Entropy: strings.NewReader(strings.Repeat("b", 1024))}),
		jsonl.New(root, jsonl.Options{Clock: clock, Entropy: strings.NewReader(strings.Repeat("c", 1024))}),
	}

	const appendCount = 32
	type result struct {
		seq uint64
		err error
	}
	results := make(chan result, appendCount)
	start := make(chan struct{})
	var group sync.WaitGroup
	for index := range appendCount {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			event, err := stores[index%len(stores)].WriteLegacyFixture(
				context.Background(),
				session.ID,
				domain.EventUserMessage,
				map[string]int{"index": index},
			)
			results <- result{seq: event.Seq, err: err}
		}()
	}
	close(start)
	group.Wait()
	close(results)

	sequences := make([]uint64, 0, appendCount)
	for result := range results {
		if result.err != nil {
			t.Fatalf("append: %v", result.err)
		}
		sequences = append(sequences, result.seq)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	for index, sequence := range sequences {
		if want := uint64(index + 2); sequence != want {
			t.Fatalf("sequence[%d]=%d want %d", index, sequence, want)
		}
	}
}

func TestStoresShareStateWhenMissingRootIsBelowSymlink(t *testing.T) {
	physicalParent := t.TempDir()
	aliasParent := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(physicalParent, aliasParent); err != nil {
		t.Fatal(err)
	}
	aliasRoot := filepath.Join(aliasParent, "missing", "root")
	physicalRoot := filepath.Join(physicalParent, "missing", "root")
	clock := func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) }
	newStore := func(root string) *jsonl.Store {
		return jsonl.New(root, jsonl.Options{
			Clock:   clock,
			Entropy: strings.NewReader(strings.Repeat("a", 1024)),
		})
	}
	beforeCreation := newStore(aliasRoot)
	if err := os.MkdirAll(physicalRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	afterCreation := newStore(aliasRoot)
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	first, err := beforeCreation.Create(
		context.Background(),
		workspace,
		domain.ModeAsk,
		domain.ModelSelection{Profile: "primary", Model: "model-a"},
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := afterCreation.Create(
		context.Background(),
		workspace,
		domain.ModeAsk,
		domain.ModelSelection{Profile: "primary", Model: "model-a"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID >= second.ID {
		t.Fatalf("session IDs are not unique and sortable: first=%q second=%q", first.ID, second.ID)
	}

	stores := []*jsonl.Store{beforeCreation, afterCreation}
	const appendCount = 16
	type result struct {
		seq uint64
		err error
	}
	results := make(chan result, appendCount)
	start := make(chan struct{})
	var group sync.WaitGroup
	for index := range appendCount {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			event, err := stores[index%len(stores)].WriteLegacyFixture(
				context.Background(),
				first.ID,
				domain.EventUserMessage,
				map[string]int{"index": index},
			)
			results <- result{seq: event.Seq, err: err}
		}()
	}
	close(start)
	group.Wait()
	close(results)

	sequences := make([]uint64, 0, appendCount)
	for result := range results {
		if result.err != nil {
			t.Fatalf("append: %v", result.err)
		}
		sequences = append(sequences, result.seq)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	for index, sequence := range sequences {
		if want := uint64(index + 2); sequence != want {
			t.Fatalf("sequence[%d]=%d want %d", index, sequence, want)
		}
	}
}

func TestCreateGeneratesUniqueSortableSessionsWithDeterministicInputs(t *testing.T) {
	root := t.TempDir()
	clock := func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) }
	newStore := func() *jsonl.Store {
		return jsonl.New(root, jsonl.Options{
			Clock:   clock,
			Entropy: strings.NewReader(strings.Repeat("a", 1024)),
		})
	}
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	first, err := newStore().Create(
		context.Background(),
		workspace,
		domain.ModeAsk,
		domain.ModelSelection{Profile: "primary", Model: "model-a"},
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := newStore().Create(
		context.Background(),
		workspace,
		domain.ModeAsk,
		domain.ModelSelection{Profile: "primary", Model: "model-a"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID {
		t.Fatalf("duplicate session ID %q", first.ID)
	}
	if first.ID >= second.ID {
		t.Fatalf("session IDs are not sortable: first=%q second=%q", first.ID, second.ID)
	}
	for _, session := range []domain.Session{first, second} {
		metadataPath := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "metadata.json")
		if _, err := os.Stat(metadataPath); err != nil {
			t.Fatalf("session %q metadata: %v", session.ID, err)
		}
	}
}

func TestCreateReturnsEventUpdatedSessionWithoutStaleMetadata(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time {
		current := now
		now = now.Add(time.Second)
		return current
	}
	store := jsonl.New(root, jsonl.Options{
		Clock:   clock,
		Entropy: strings.NewReader(strings.Repeat("a", 1024)),
	})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	session, err := store.Create(
		context.Background(),
		workspace,
		domain.ModeAsk,
		domain.ModelSelection{Profile: "primary", Model: "model-a"},
	)
	if err != nil {
		t.Fatal(err)
	}
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	events := readEvents(t, filepath.Join(sessionDir, "events.jsonl"))
	if len(events) != 1 {
		t.Fatalf("events=%d want 1", len(events))
	}
	if session.UpdatedAt != events[0].Time {
		t.Fatalf("returned updated_at=%v want creation event time %v", session.UpdatedAt, events[0].Time)
	}
	var persisted domain.Session
	raw, err := os.ReadFile(filepath.Join(sessionDir, "metadata.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted != session {
		t.Fatalf("persisted session=%+v want returned session=%+v", persisted, session)
	}
}

func TestListWaitsForCreateMetadata(t *testing.T) {
	root := t.TempDir()
	clock := newCreateGateClock(time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC))
	store := jsonl.New(root, jsonl.Options{
		Clock:   clock.Now,
		Entropy: strings.NewReader(strings.Repeat("a", 1024)),
	})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	type createResult struct {
		session domain.Session
		err     error
	}
	createDone := make(chan createResult, 1)
	go func() {
		session, err := store.Create(
			context.Background(),
			workspace,
			domain.ModeAsk,
			domain.ModelSelection{Profile: "primary", Model: "model-a"},
		)
		createDone <- createResult{session: session, err: err}
	}()

	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(clock.release) }) }
	defer release()
	select {
	case <-clock.eventID:
	case <-time.After(5 * time.Second):
		t.Fatal("Create did not reach the pre-publication gate")
	}

	type listResult struct {
		sessions []domain.SessionSummary
		err      error
	}
	listDone := make(chan listResult, 1)
	listStarted := make(chan struct{})
	go func() {
		close(listStarted)
		sessions, err := store.List(context.Background(), workspace)
		listDone <- listResult{sessions: sessions, err: err}
	}()
	<-listStarted
	select {
	case result := <-listDone:
		t.Fatalf("List returned before Create completed: sessions=%v err=%v", result.sessions, result.err)
	case <-time.After(time.Second):
	}
	release()

	var created createResult
	select {
	case created = <-createDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Create did not finish")
	}
	if created.err != nil {
		t.Fatal(created.err)
	}
	select {
	case listed := <-listDone:
		if listed.err != nil {
			t.Fatal(listed.err)
		}
		if len(listed.sessions) != 1 || listed.sessions[0].ID != created.session.ID {
			t.Fatalf("sessions=%v want created session %q", listed.sessions, created.session.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("List did not finish")
	}
}

func TestAppendRejectsCorruptEarlierEvents(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]domain.DurableEvent)
	}{
		{
			name: "invalid envelope",
			mutate: func(events []domain.DurableEvent) {
				events[0].SchemaVersion = 2
			},
		},
		{
			name: "duplicate sequence",
			mutate: func(events []domain.DurableEvent) {
				events[1].Seq = events[0].Seq
			},
		},
		{
			name: "out of order sequence",
			mutate: func(events []domain.DurableEvent) {
				events[1].Seq, events[2].Seq = events[2].Seq, events[1].Seq
			},
		},
		{
			name: "wrong session",
			mutate: func(events []domain.DurableEvent) {
				events[0].SessionID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			store, workspace, session := createTestSession(t, root)
			for index := range 3 {
				if _, err := store.WriteLegacyFixture(
					context.Background(),
					session.ID,
					domain.EventUserMessage,
					map[string]int{"index": index},
				); err != nil {
					t.Fatal(err)
				}
			}
			eventsPath := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "events.jsonl")
			metadataPath := filepath.Join(filepath.Dir(eventsPath), "metadata.json")
			events := readEvents(t, eventsPath)
			test.mutate(events)
			writeEvents(t, eventsPath, events)
			beforeLog, err := os.ReadFile(eventsPath)
			if err != nil {
				t.Fatal(err)
			}
			beforeMetadata, err := os.ReadFile(metadataPath)
			if err != nil {
				t.Fatal(err)
			}

			_, err = store.WriteLegacyFixture(context.Background(), session.ID, domain.EventUserMessage, map[string]string{"content": "blocked"})
			if err == nil {
				t.Fatal("append accepted a corrupt earlier event")
			}
			afterLog, readErr := os.ReadFile(eventsPath)
			if readErr != nil {
				t.Fatal(readErr)
			}
			afterMetadata, readErr := os.ReadFile(metadataPath)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if !bytes.Equal(afterLog, beforeLog) || !bytes.Equal(afterMetadata, beforeMetadata) {
				t.Fatal("rejected append changed durable session state")
			}
		})
	}
}

func createTestSession(t *testing.T, root string) (*jsonl.Store, domain.Workspace, domain.Session) {
	t.Helper()
	store := jsonl.New(root, jsonl.Options{
		Clock:   func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) },
		Entropy: strings.NewReader(strings.Repeat("a", 1024)),
	})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.Create(
		context.Background(),
		workspace,
		domain.ModeAsk,
		domain.ModelSelection{Profile: "primary", Model: "model-a"},
	)
	if err != nil {
		t.Fatal(err)
	}
	return store, workspace, session
}

func readEvents(t *testing.T, path string) []domain.DurableEvent {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSuffix(raw, []byte{'\n'}), []byte{'\n'})
	events := make([]domain.DurableEvent, len(lines))
	for index, line := range lines {
		if err := json.Unmarshal(line, &events[index]); err != nil {
			t.Fatal(err)
		}
	}
	return events
}

func writeEvents(t *testing.T, path string, events []domain.DurableEvent) {
	t.Helper()
	var contents bytes.Buffer
	for _, event := range events {
		encoded, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		contents.Write(encoded)
		contents.WriteByte('\n')
	}
	if err := os.WriteFile(path, contents.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
}
