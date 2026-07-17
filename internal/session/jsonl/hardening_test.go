package jsonl_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
)

func TestLoadRecoveryIsRestartIdempotentAcrossCrashWindows(t *testing.T) {
	tests := []struct {
		name        string
		restoreTail bool
	}{
		{name: "artifact persisted before log truncation", restoreTail: true},
		{name: "log truncated before restart", restoreTail: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			store, workspace, session := createTestSession(t, root)
			eventsPath := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "events.jsonl")
			artifactsDir := filepath.Join(filepath.Dir(eventsPath), "artifacts")
			tail := []byte(`{"schema_version":1`)
			appendFile(t, eventsPath, tail)

			first, err := store.Load(context.Background(), session.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(first.RecoveryNote, "incomplete final line") {
				t.Fatalf("first recovery note=%q", first.RecoveryNote)
			}
			beforeArtifacts := directoryNames(t, artifactsDir)
			if len(beforeArtifacts) != 1 {
				t.Fatalf("initial recovery artifacts=%v want one", beforeArtifacts)
			}
			beforeInfo, err := os.Stat(filepath.Join(artifactsDir, beforeArtifacts[0]))
			if err != nil {
				t.Fatal(err)
			}
			if test.restoreTail {
				appendFile(t, eventsPath, tail)
			}

			restarted := jsonl.New(root, jsonl.Options{
				Clock:   func() time.Time { return time.Date(2026, 7, 13, 12, 0, 1, 0, time.UTC) },
				Entropy: strings.NewReader(strings.Repeat("r", 1024)),
			})
			replay, err := restarted.Load(context.Background(), session.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(replay.RecoveryNote, "incomplete final line") {
				t.Fatalf("restart recovery note=%q", replay.RecoveryNote)
			}
			afterArtifacts := directoryNames(t, artifactsDir)
			if strings.Join(afterArtifacts, "\x00") != strings.Join(beforeArtifacts, "\x00") {
				t.Fatalf("restart artifacts=%v want reused %v", afterArtifacts, beforeArtifacts)
			}
			afterPath := filepath.Join(artifactsDir, afterArtifacts[0])
			afterInfo, err := os.Stat(afterPath)
			if err != nil {
				t.Fatal(err)
			}
			if !os.SameFile(beforeInfo, afterInfo) {
				t.Fatal("restart replaced rather than reused the recovery artifact")
			}
			artifactBytes, err := os.ReadFile(afterPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(artifactBytes, tail) {
				t.Fatalf("recovery artifact=%q want %q", artifactBytes, tail)
			}
		})
	}
}

func TestLoadRejectsSubstitutedEventsLeaf(t *testing.T) {
	tests := []struct {
		name       string
		targetPath func(t *testing.T, root, sessionDir string) string
		linkTarget func(target, sessionDir string) string
	}{
		{
			name: "outside store",
			targetPath: func(t *testing.T, _, _ string) string {
				t.Helper()
				return filepath.Join(t.TempDir(), "outside.jsonl")
			},
			linkTarget: func(target, _ string) string { return target },
		},
		{
			name: "inside session directory",
			targetPath: func(t *testing.T, _, sessionDir string) string {
				t.Helper()
				return filepath.Join(sessionDir, "substituted.jsonl")
			},
			linkTarget: func(target, sessionDir string) string {
				relative, err := filepath.Rel(sessionDir, target)
				if err != nil {
					panic(err)
				}
				return relative
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			store, workspace, session := createTestSession(t, root)
			sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
			eventsPath := filepath.Join(sessionDir, "events.jsonl")
			complete, err := os.ReadFile(eventsPath)
			if err != nil {
				t.Fatal(err)
			}
			target := test.targetPath(t, root, sessionDir)
			targetBytes := append(append([]byte(nil), complete...), []byte(`{"schema_version":1`)...)
			if err := os.WriteFile(target, targetBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(eventsPath); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(test.linkTarget(target, sessionDir), eventsPath); err != nil {
				t.Fatal(err)
			}

			if _, err := store.Load(context.Background(), session.ID); err == nil {
				t.Fatal("Load followed a substituted events.jsonl leaf")
			}
			after, err := os.ReadFile(target)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(after, targetBytes) {
				t.Fatal("Load truncated the substituted events target")
			}
		})
	}
}

func TestAppendRejectsSubstitutedEventsLeaf(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	eventsPath := filepath.Join(sessionDir, "events.jsonl")
	target := filepath.Join(sessionDir, "substituted.jsonl")
	targetBytes, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, targetBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(eventsPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Base(target), eventsPath); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Append(context.Background(), session.ID, domain.EventUserMessage, map[string]string{"content": "blocked"}); err == nil {
		t.Fatal("Append followed a substituted events.jsonl leaf")
	}
	after, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, targetBytes) {
		t.Fatal("Append wrote to the substituted events target")
	}
}

func TestPutRejectsArtifactsDirectorySwapDuringCopy(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	artifactsDir := filepath.Join(sessionDir, "artifacts")
	movedDir := filepath.Join(sessionDir, "artifacts-moved")
	source := &callbackReader{
		data: []byte("must not be published"),
		callback: func() error {
			if err := os.Rename(artifactsDir, movedDir); err != nil {
				return err
			}
			return os.Mkdir(artifactsDir, 0o700)
		},
	}

	if _, err := store.Put(context.Background(), session.ID, "text/plain", source, 100); err == nil {
		t.Fatal("Put succeeded after its artifacts directory was substituted")
	}
	if names := directoryNames(t, artifactsDir); len(names) != 0 {
		t.Fatalf("substituted artifacts directory contains files: %v", names)
	}
	if names := directoryNames(t, movedDir); len(names) != 0 {
		t.Fatalf("original artifacts directory retained partial files: %v", names)
	}
}

func TestPutRejectsTemporaryLeafSwapDuringCopy(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	artifactsDir := filepath.Join(sessionDir, "artifacts")
	var temporaryPath string
	movedPath := filepath.Join(sessionDir, "moved-artifact-temp")
	source := &callbackReader{
		data: []byte("must stay on the opened descriptor"),
		callback: func() error {
			entries, err := os.ReadDir(artifactsDir)
			if err != nil {
				return err
			}
			if len(entries) != 1 || !strings.HasSuffix(entries[0].Name(), ".tmp") {
				return errors.New("transactional temporary file not found")
			}
			temporaryPath = filepath.Join(artifactsDir, entries[0].Name())
			if err := os.Rename(temporaryPath, movedPath); err != nil {
				return err
			}
			return os.WriteFile(temporaryPath, []byte("substitute"), 0o600)
		},
	}

	if _, err := store.Put(context.Background(), session.ID, "text/plain", source, 100); err == nil {
		t.Fatal("Put published a substituted temporary leaf")
	}
	for _, name := range directoryNames(t, artifactsDir) {
		if strings.HasSuffix(name, ".bin") {
			t.Fatalf("substituted temporary leaf was published as %q", name)
		}
	}
	substitute, err := os.ReadFile(temporaryPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(substitute) != "substitute" {
		t.Fatalf("substituted temporary leaf was modified: %q", substitute)
	}
}

func TestOpenRejectsInRootArtifactLeafSubstitution(t *testing.T) {
	root := t.TempDir()
	store, _, session := createTestSession(t, root)
	first, err := store.Put(context.Background(), session.ID, "text/plain", strings.NewReader("first"), 100)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Put(context.Background(), session.ID, "text/plain", strings.NewReader("second"), 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(first.Path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Base(second.Path), first.Path); err != nil {
		t.Fatal(err)
	}
	if reader, err := store.Open(context.Background(), first); err == nil {
		reader.Close()
		t.Fatal("Open followed an in-root artifact leaf substitution")
	}
}

func TestOpenRejectsArtifactLeafSwapBetweenCheckAndOpen(t *testing.T) {
	root := t.TempDir()
	store, _, session := createTestSession(t, root)
	first, err := store.Put(context.Background(), session.ID, "text/plain", strings.NewReader("first"), 100)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Put(context.Background(), session.ID, "text/plain", strings.NewReader("second"), 100)
	if err != nil {
		t.Fatal(err)
	}
	ctx := &nthErrContext{
		Context: context.Background(),
		at:      3,
		callback: func() {
			if err := os.Remove(first.Path); err != nil {
				t.Error(err)
				return
			}
			if err := os.Symlink(filepath.Base(second.Path), first.Path); err != nil {
				t.Error(err)
			}
		},
	}

	if reader, err := store.Open(ctx, first); err == nil {
		reader.Close()
		t.Fatal("Open accepted an artifact swapped between identity check and descriptor open")
	}
	if !ctx.triggered {
		t.Fatal("artifact leaf swap was not injected at the open boundary")
	}
}

func TestOpenRejectsArtifactsDirectorySwapDuringOpen(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	artifact, err := store.Put(context.Background(), session.ID, "text/plain", strings.NewReader("original"), 100)
	if err != nil {
		t.Fatal(err)
	}
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	artifactsDir := filepath.Join(sessionDir, "artifacts")
	movedDir := filepath.Join(sessionDir, "artifacts-moved")
	ctx := &nthErrContext{
		Context: context.Background(),
		at:      3,
		callback: func() {
			if err := os.Rename(artifactsDir, movedDir); err != nil {
				t.Error(err)
				return
			}
			if err := os.Mkdir(artifactsDir, 0o700); err != nil {
				t.Error(err)
			}
		},
	}

	if reader, err := store.Open(ctx, artifact); err == nil {
		reader.Close()
		t.Fatal("Open accepted an artifact from a substituted directory")
	}
	if !ctx.triggered {
		t.Fatal("artifacts directory swap was not injected after descriptor open")
	}
}

func TestPutPublishesFinalNameOnlyAfterCopyCompletes(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	artifactsDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "artifacts")
	source := newGateReader([]byte("complete artifact"))
	type result struct {
		artifact domain.Artifact
		err      error
	}
	done := make(chan result, 1)
	go func() {
		artifact, err := store.Put(context.Background(), session.ID, "text/plain", source, 100)
		done <- result{artifact: artifact, err: err}
	}()
	<-source.entered

	visibleFinal := false
	for _, name := range directoryNames(t, artifactsDir) {
		if strings.HasSuffix(name, ".bin") {
			visibleFinal = true
		}
	}
	close(source.release)
	put := <-done
	if put.err != nil {
		t.Fatal(put.err)
	}
	if visibleFinal {
		t.Fatal("final artifact name was visible before copying completed")
	}
	if names := directoryNames(t, artifactsDir); len(names) != 1 || names[0] != filepath.Base(put.artifact.Path) {
		t.Fatalf("published artifact entries=%v path=%q", names, put.artifact.Path)
	}
}

func TestPutDoesNotReplaceExistingFinalArtifact(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	artifactsDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "artifacts")
	var finalPath string
	source := &callbackReader{
		data: []byte("new artifact"),
		callback: func() error {
			entries, err := os.ReadDir(artifactsDir)
			if err != nil {
				return err
			}
			if len(entries) != 1 || !strings.HasSuffix(entries[0].Name(), ".tmp") {
				return errors.New("transactional temporary file not found")
			}
			id := strings.TrimSuffix(strings.TrimPrefix(entries[0].Name(), "."), ".tmp")
			finalPath = filepath.Join(artifactsDir, id+".bin")
			return os.WriteFile(finalPath, []byte("existing artifact"), 0o600)
		},
	}

	if _, err := store.Put(context.Background(), session.ID, "text/plain", source, 100); err == nil {
		t.Fatal("Put replaced an existing final artifact")
	}
	contents, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "existing artifact" {
		t.Fatalf("existing final artifact=%q", contents)
	}
	if names := directoryNames(t, artifactsDir); len(names) != 1 || names[0] != filepath.Base(finalPath) {
		t.Fatalf("failed publication entries=%v want only existing final", names)
	}
}

func TestPutCleansTemporaryStateAfterReaderFailure(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	artifactsDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "artifacts")
	wantErr := errors.New("source failed")

	_, err := store.Put(context.Background(), session.ID, "text/plain", &failingReader{err: wantErr}, 100)
	if !errors.Is(err, wantErr) {
		t.Fatalf("Put error=%v want source failure", err)
	}
	if names := directoryNames(t, artifactsDir); len(names) != 0 {
		t.Fatalf("failed artifact left state behind: %v", names)
	}
}

func TestPutSurfacesDurableCleanupFailure(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	artifactsDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "artifacts")
	wantErr := errors.New("source failed")
	source := &cleanupFailReader{directory: artifactsDir, err: wantErr}

	_, err := store.Put(context.Background(), session.ID, "text/plain", source, 100)
	if chmodErr := os.Chmod(artifactsDir, 0o700); chmodErr != nil {
		t.Fatal(chmodErr)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("Put error=%v want source failure", err)
	}
	if err == nil || !strings.Contains(err.Error(), "cleanup artifact") {
		t.Fatalf("Put error=%v does not surface cleanup failure", err)
	}
}

func TestContextCancellationWhileWaitingForSharedRootLock(t *testing.T) {
	root := t.TempDir()
	holder, _, session := createTestSession(t, root)
	waiter := jsonl.New(root, jsonl.Options{
		Clock:   func() time.Time { return time.Date(2026, 7, 13, 12, 0, 1, 0, time.UTC) },
		Entropy: strings.NewReader(strings.Repeat("w", 1024)),
	})
	source := newGateReader([]byte("holder"))
	holderDone := make(chan error, 1)
	go func() {
		_, err := holder.Put(context.Background(), session.ID, "text/plain", source, 100)
		holderDone <- err
	}()
	<-source.entered

	parent, cancel := context.WithCancel(context.Background())
	ctx := newObservedContext(parent)
	waiterDone := make(chan error, 1)
	go func() {
		_, err := waiter.Append(ctx, session.ID, domain.EventUserMessage, map[string]string{"content": "blocked"})
		waiterDone <- err
	}()

	select {
	case <-ctx.observed:
		cancel()
	case <-time.After(500 * time.Millisecond):
		cancel()
		close(source.release)
		<-holderDone
		<-waiterDone
		t.Fatal("waiting API did not observe context before blocking on the shared lock")
	}
	select {
	case err := <-waiterDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting Append error=%v want context canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		close(source.release)
		<-holderDone
		<-waiterDone
		t.Fatal("waiting Append did not return after context cancellation")
	}
	close(source.release)
	if err := <-holderDone; err != nil {
		t.Fatal(err)
	}
}

func TestPutObservesCancellationBetweenSourceReads(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	artifactsDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "artifacts")
	ctx, cancel := context.WithCancel(context.Background())
	source := &cancelingReader{cancel: cancel, data: []byte("not durable")}

	_, err := store.Put(ctx, session.ID, "text/plain", source, 100)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Put error=%v want context canceled", err)
	}
	if source.reads != 1 {
		t.Fatalf("source reads=%d want cancellation after first read", source.reads)
	}
	if names := directoryNames(t, artifactsDir); len(names) != 0 {
		t.Fatalf("canceled artifact left state behind: %v", names)
	}
}

func TestPutCleansTemporaryStateWhenCanceledDuringTempOpen(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	artifactsDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "artifacts")
	parent, cancel := context.WithCancel(context.Background())
	ctx := &nthErrContext{
		Context:  parent,
		at:       4,
		callback: cancel,
	}

	_, err := store.Put(ctx, session.ID, "text/plain", strings.NewReader("not durable"), 100)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Put error=%v want context canceled", err)
	}
	if !ctx.triggered {
		t.Fatal("cancellation was not injected after the temporary file opened")
	}
	if names := directoryNames(t, artifactsDir); len(names) != 0 {
		t.Fatalf("canceled temporary open left state behind: %v", names)
	}
}

func TestLoadPairsToolCallsByMultiplicityAndPayloadShape(t *testing.T) {
	type eventSpec struct {
		kind    domain.EventKind
		payload any
	}
	tests := []struct {
		name            string
		events          []eventSpec
		wantInterrupted bool
		wantCallCount   int
	}{
		{
			name: "one result closes only one duplicate start",
			events: []eventSpec{
				{kind: domain.EventToolStarted, payload: map[string]any{"call_id": "same"}},
				{kind: domain.EventToolStarted, payload: map[string]any{"call_id": "same"}},
				{kind: domain.EventToolResult, payload: map[string]any{"call_id": "same"}},
			},
			wantInterrupted: true,
			wantCallCount:   1,
		},
		{
			name: "two results close two duplicate starts",
			events: []eventSpec{
				{kind: domain.EventToolStarted, payload: map[string]any{"call_id": "same"}},
				{kind: domain.EventToolStarted, payload: map[string]any{"call_id": "same"}},
				{kind: domain.EventToolResult, payload: map[string]any{"call_id": "same"}},
				{kind: domain.EventToolResult, payload: map[string]any{"call_id": "same"}},
			},
		},
		{
			name: "nested runner result closes flat start",
			events: []eventSpec{
				{kind: domain.EventToolStarted, payload: map[string]any{"call_id": "nested"}},
				{kind: domain.EventToolResult, payload: map[string]any{"result": map[string]any{"call_id": "nested"}}},
			},
		},
		{
			name: "malformed starts remain independently unmatched",
			events: []eventSpec{
				{kind: domain.EventToolStarted, payload: map[string]any{}},
				{kind: domain.EventToolStarted, payload: map[string]any{"call_id": ""}},
				{kind: domain.EventToolStarted, payload: map[string]any{"call_id": 42}},
				{kind: domain.EventToolResult, payload: map[string]any{"call_id": ""}},
				{kind: domain.EventToolResult, payload: map[string]any{"result": map[string]any{"call_id": 42}}},
			},
			wantInterrupted: true,
			wantCallCount:   3,
		},
		{
			name: "prior interruption does not close future start",
			events: []eventSpec{
				{kind: domain.EventToolStarted, payload: map[string]any{"call_id": "old"}},
				{kind: domain.EventTurnInterrupted, payload: map[string]any{"reason": "prior restart"}},
				{kind: domain.EventToolStarted, payload: map[string]any{"call_id": "future"}},
			},
			wantInterrupted: true,
			wantCallCount:   1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, _, session := createTestSession(t, t.TempDir())
			for _, spec := range test.events {
				if _, err := store.Append(context.Background(), session.ID, spec.kind, spec.payload); err != nil {
					t.Fatal(err)
				}
			}
			before := 1 + len(test.events)
			replay, err := store.Load(context.Background(), session.ID)
			if err != nil {
				t.Fatal(err)
			}
			if replay.ReadOnly {
				t.Fatal("valid tool events replayed read-only")
			}
			if !test.wantInterrupted {
				if len(replay.Events) != before || replay.RecoveryNote != "" {
					t.Fatalf("events=%d note=%q want fully matched", len(replay.Events), replay.RecoveryNote)
				}
				return
			}
			if len(replay.Events) != before+1 {
				t.Fatalf("events=%d want interruption after %d durable events", len(replay.Events), before)
			}
			last := replay.Events[len(replay.Events)-1]
			if last.Kind != domain.EventTurnInterrupted {
				t.Fatalf("last kind=%q want turn.interrupted", last.Kind)
			}
			var payload struct {
				CallCount int `json:"call_count"`
			}
			if err := json.Unmarshal(last.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if payload.CallCount != test.wantCallCount {
				t.Fatalf("interrupted call_count=%d want %d", payload.CallCount, test.wantCallCount)
			}
		})
	}
}

func TestLoadDoesNotRewriteCurrentMetadataForTimestampDifference(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	metadataPath := filepath.Join(sessionDir, "metadata.json")
	metadata := readSessionMetadata(t, metadataPath)
	metadata.UpdatedAt = metadata.UpdatedAt.Add(24 * time.Hour)
	writeJSONFile(t, metadataPath, metadata)
	before, err := snapshotTree(sessionDir)
	if err != nil {
		t.Fatal(err)
	}

	replay, err := store.Load(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replay.ReadOnly || replay.Session.LastSeq != metadata.LastSeq || !replay.Session.UpdatedAt.Equal(metadata.UpdatedAt) {
		t.Fatalf("replay=%+v want current metadata unchanged", replay)
	}
	assertSessionTreeUnchanged(t, sessionDir, before)
}

type gateReader struct {
	data    []byte
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	done    bool
}

func newGateReader(data []byte) *gateReader {
	return &gateReader{data: data, entered: make(chan struct{}), release: make(chan struct{})}
}

func (r *gateReader) Read(buffer []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.once.Do(func() { close(r.entered) })
	<-r.release
	r.done = true
	return copy(buffer, r.data), io.EOF
}

type callbackReader struct {
	data     []byte
	callback func() error
	done     bool
}

func (r *callbackReader) Read(buffer []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	if err := r.callback(); err != nil {
		return 0, err
	}
	return copy(buffer, r.data), io.EOF
}

type failingReader struct {
	err  error
	sent bool
}

func (r *failingReader) Read(buffer []byte) (int, error) {
	if !r.sent {
		r.sent = true
		return copy(buffer, "partial"), nil
	}
	return 0, r.err
}

type cancelingReader struct {
	cancel context.CancelFunc
	data   []byte
	reads  int
}

type cleanupFailReader struct {
	directory string
	err       error
	reads     int
}

func (r *cleanupFailReader) Read(buffer []byte) (int, error) {
	r.reads++
	if r.reads == 1 {
		return copy(buffer, "partial"), nil
	}
	if err := os.Chmod(r.directory, 0o500); err != nil {
		return 0, err
	}
	return 0, r.err
}

func (r *cancelingReader) Read(buffer []byte) (int, error) {
	r.reads++
	r.cancel()
	return copy(buffer, r.data), io.EOF
}

type observedContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

type nthErrContext struct {
	context.Context
	at        int
	count     int
	callback  func()
	triggered bool
}

func (c *nthErrContext) Err() error {
	c.count++
	if c.count == c.at {
		c.triggered = true
		c.callback()
	}
	return c.Context.Err()
}

func newObservedContext(parent context.Context) *observedContext {
	return &observedContext{Context: parent, observed: make(chan struct{})}
}

func (c *observedContext) Err() error {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Err()
}

func appendFile(t *testing.T, path string, contents []byte) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(contents); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func directoryNames(t *testing.T, path string) []string {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(entries))
	for index, entry := range entries {
		names[index] = entry.Name()
	}
	return names
}

func writeJSONFile(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}
