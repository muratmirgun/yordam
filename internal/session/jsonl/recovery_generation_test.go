package jsonl_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
)

func TestExplicitRecoveryPreservesDistinctTailGenerations(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	eventsPath := filepath.Join(sessionDir, "events.jsonl")
	artifactsDir := filepath.Join(sessionDir, "artifacts")
	tails := [][]byte{[]byte(`{"first":`), []byte(`{"second":`)}

	for index, tail := range tails {
		appendFile(t, eventsPath, tail)
		request := recoveryRequestFromInspection(t, store, session.ID,
			protocol.ControlOperationID(fmt.Sprintf("operation-generation-%d", index)),
			protocol.TransactionID(fmt.Sprintf("txn-generation-%d", index)))
		result, err := store.RecoverSession(context.Background(), request)
		if err != nil || result.Status != "recovered" {
			t.Fatalf("generation %d result=%+v err=%v", index, result, err)
		}
	}

	var recovered [][]byte
	entries, err := os.ReadDir(artifactsDir)
	if err != nil {
		t.Fatal(err)
	}
	var wantNames []string
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "recovery-tail-") {
			continue
		}
		wantNames = append(wantNames, entry.Name())
		contents, err := os.ReadFile(filepath.Join(artifactsDir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		recovered = append(recovered, contents)
	}
	if len(wantNames) != len(tails) {
		t.Fatalf("recovery generations=%v want %d", wantNames, len(tails))
	}
	for _, tail := range tails {
		found := false
		for _, contents := range recovered {
			found = found || bytes.Equal(contents, tail)
		}
		if !found {
			t.Fatalf("tail %q was not preserved exactly", tail)
		}
	}
}

func TestExplicitRecoveryPreservesTailsThatDifferBeyondLegacyRetentionCap(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	eventsPath := filepath.Join(sessionDir, "events.jsonl")
	artifactsDir := filepath.Join(sessionDir, "artifacts")
	prefix := bytes.Repeat([]byte("x"), int(jsonl.MaxArtifactBytes))
	tails := [][]byte{
		append(append([]byte(nil), prefix...), 'a'),
		append(append([]byte(nil), prefix...), 'b'),
	}

	for index, tail := range tails {
		appendFile(t, eventsPath, tail)
		request := recoveryRequestFromInspection(t, store, session.ID,
			protocol.ControlOperationID(fmt.Sprintf("operation-large-generation-%d", index)),
			protocol.TransactionID(fmt.Sprintf("txn-large-generation-%d", index)))
		result, err := store.RecoverSession(context.Background(), request)
		if err != nil || result.Status != "recovered" {
			t.Fatalf("generation %d result=%+v err=%v", index, result, err)
		}
	}
	var names []string
	for _, name := range directoryNames(t, artifactsDir) {
		if strings.HasPrefix(name, "recovery-tail-") {
			names = append(names, name)
		}
	}
	if len(names) != 2 || names[0] == names[1] {
		t.Fatalf("recovery generations=%v want two distinct capped generations", names)
	}
	for _, name := range names {
		info, err := os.Stat(filepath.Join(artifactsDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != jsonl.MaxArtifactBytes+1 {
			t.Fatalf("generation %q size=%d want %d", name, info.Size(), jsonl.MaxArtifactBytes+1)
		}
	}
}

func TestLoadLeavesStaleLegacyRecoveryTemporaryUnchanged(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	eventsPath := filepath.Join(sessionDir, "events.jsonl")
	artifactsDir := filepath.Join(sessionDir, "artifacts")
	tail := []byte(`{"stale-temp":`)
	_, temporary := recoveryGenerationNames(tail)
	if err := os.WriteFile(filepath.Join(artifactsDir, temporary), []byte("partial stale bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	appendFile(t, eventsPath, tail)

	replay, err := store.Load(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(replay.RecoveryNote, "incomplete_final_fragment") {
		t.Fatalf("recovery note=%q", replay.RecoveryNote)
	}
	if got := directoryNames(t, artifactsDir); len(got) != 1 || got[0] != temporary {
		t.Fatalf("pure Load changed artifacts=%v want [%s]", got, temporary)
	}
	contents, err := os.ReadFile(filepath.Join(artifactsDir, temporary))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "partial stale bytes" {
		t.Fatalf("pure Load changed temporary contents=%q", contents)
	}
}

func TestLoadLeavesLegacyRecoveryFinalAndTemporaryUnchanged(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	eventsPath := filepath.Join(sessionDir, "events.jsonl")
	artifactsDir := filepath.Join(sessionDir, "artifacts")
	tail := []byte(`{"linked-temp":`)
	final, temporary := recoveryGenerationNames(tail)
	temporaryPath := filepath.Join(artifactsDir, temporary)
	finalPath := filepath.Join(artifactsDir, final)
	if err := os.WriteFile(temporaryPath, tail, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(temporaryPath, finalPath); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(finalPath)
	if err != nil {
		t.Fatal(err)
	}
	appendFile(t, eventsPath, tail)

	if _, err := store.Load(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	if got := directoryNames(t, artifactsDir); len(got) != 2 || got[0] != temporary || got[1] != final {
		t.Fatalf("pure Load changed artifacts=%v want [%s %s]", got, temporary, final)
	}
	after, err := os.Stat(finalPath)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("reconciliation replaced the durable recovery generation")
	}
}

func TestLoadIgnoresLegacyRecoveryArtifactsWithoutMutatingThem(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	artifactsDir := filepath.Join(sessionDir, "artifacts")
	if _, err := store.Put(context.Background(), session.ID, "text/plain", strings.NewReader("ordinary"), 100); err != nil {
		t.Fatal(err)
	}
	forged := "recovery-" + strings.Repeat("0", sha256.Size*2) + "-" + strings.Repeat("0", sha256.Size*2) + ".bin"
	if err := os.WriteFile(filepath.Join(artifactsDir, forged), []byte("not the named digest"), 0o600); err != nil {
		t.Fatal(err)
	}
	restarted := jsonl.New(root, jsonl.Options{
		Clock:   func() time.Time { return time.Date(2026, 7, 13, 12, 0, 1, 0, time.UTC) },
		Entropy: strings.NewReader(strings.Repeat("r", 1024)),
	})
	replay, err := restarted.Load(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replay.RecoveryNote != "" {
		t.Fatalf("ordinary or forged artifact produced recovery note %q", replay.RecoveryNote)
	}

	tail := []byte(`{"durable":`)
	final, _ := recoveryGenerationNames(tail)
	if err := os.WriteFile(filepath.Join(artifactsDir, final), tail, 0o600); err != nil {
		t.Fatal(err)
	}
	replay, err = restarted.Load(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replay.RecoveryNote != "" {
		t.Fatalf("legacy recovery artifact changed pure inspection note=%q", replay.RecoveryNote)
	}
}

func TestAppendKeepsEventAndMetadataOnOneSessionRoot(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	movedDir := sessionDir + "-opened"
	var replacementEvents, replacementMetadata []byte
	ctx := &conditionalContext{
		Context: context.Background(),
		condition: func() bool {
			events := readEventsIfValid(filepath.Join(sessionDir, "events.jsonl"))
			return len(events) == 2 && metadataTemporaryExists(sessionDir)
		},
		callback: func() error {
			var err error
			replacementEvents, err = os.ReadFile(filepath.Join(sessionDir, "events.jsonl"))
			if err != nil {
				return err
			}
			replacementMetadata, err = os.ReadFile(filepath.Join(sessionDir, "metadata.json"))
			if err != nil {
				return err
			}
			if err := os.Rename(sessionDir, movedDir); err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Join(sessionDir, "artifacts"), 0o700); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(sessionDir, "events.jsonl"), replacementEvents, 0o600); err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(sessionDir, "metadata.json"), replacementMetadata, 0o600)
		},
	}

	if _, err := store.Append(ctx, session.ID, domain.EventUserMessage, map[string]string{"content": "anchored"}); err == nil {
		t.Fatal("Append advanced metadata through a substituted session path")
	}
	if !ctx.triggered {
		t.Fatal("event-to-metadata transaction boundary was not reached")
	}
	if ctx.callbackErr != nil {
		t.Fatal(ctx.callbackErr)
	}
	afterEvents, err := os.ReadFile(filepath.Join(sessionDir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	afterMetadata, err := os.ReadFile(filepath.Join(sessionDir, "metadata.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(afterEvents, replacementEvents) || !bytes.Equal(afterMetadata, replacementMetadata) {
		t.Fatal("Append mutated the substituted session directory")
	}
}

func TestPutJoinsSourceAndTruncateErrors(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	artifactsDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "artifacts")
	sourceErr := errors.New("source failed after overflow")
	ctx := &conditionalContext{
		Context: context.Background(),
		condition: func() bool {
			entries, err := os.ReadDir(artifactsDir)
			if err != nil {
				return false
			}
			for _, entry := range entries {
				if strings.HasSuffix(entry.Name(), ".tmp") {
					path := filepath.Join(artifactsDir, entry.Name())
					info, statErr := os.Stat(path)
					return statErr == nil && info.Size() > 8 && descriptorForPathIsOpen(path)
				}
			}
			return false
		},
		callback: func() error {
			for _, name := range directoryNames(t, artifactsDir) {
				if strings.HasSuffix(name, ".tmp") {
					return closeDescriptorForPath(filepath.Join(artifactsDir, name))
				}
			}
			return errors.New("artifact temporary was not open")
		},
	}

	_, err := store.Put(ctx, session.ID, "application/octet-stream", &overflowErrorReader{err: sourceErr}, 8)
	if !ctx.triggered {
		t.Fatal("post-write truncate fault boundary was not reached")
	}
	if !errors.Is(err, sourceErr) || !errors.Is(err, syscall.EBADF) {
		t.Fatalf("Put error=%v want joined source and truncate errors", err)
	}
}

func TestOpenJoinsReturnedFileCloseErrorOnVerificationFailure(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	artifact, err := store.Put(context.Background(), session.ID, "text/plain", strings.NewReader("artifact"), 100)
	if err != nil {
		t.Fatal(err)
	}
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	artifactsDir := filepath.Join(sessionDir, "artifacts")
	ctx := &conditionalContext{
		Context: context.Background(),
		condition: func() bool {
			return descriptorForPathIsOpen(artifact.Path)
		},
		callback: func() error {
			if err := closeDescriptorForPath(artifact.Path); err != nil {
				return err
			}
			if err := os.Rename(artifactsDir, filepath.Join(sessionDir, "artifacts-opened")); err != nil {
				return err
			}
			return os.Mkdir(artifactsDir, 0o700)
		},
	}

	reader, err := store.Open(ctx, artifact)
	if reader != nil {
		reader.Close()
	}
	if !ctx.triggered {
		t.Fatal("artifact verification fault was not injected")
	}
	if err == nil || !strings.Contains(err.Error(), "opened directory") || !errors.Is(err, syscall.EBADF) {
		t.Fatalf("Open error=%v want verification and returned-file close errors", err)
	}
}

func recoveryGenerationNames(tail []byte) (string, string) {
	retained := tail
	if int64(len(retained)) > jsonl.MaxArtifactBytes {
		retained = retained[:jsonl.MaxArtifactBytes]
	}
	digest := fmt.Sprintf("%x-%x", sha256.Sum256(tail), sha256.Sum256(retained))
	return "recovery-" + digest + ".bin", ".recovery-" + digest + ".tmp"
}

func metadataTemporaryExists(sessionDir string) bool {
	entries, err := os.ReadDir(sessionDir)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".metadata-") && strings.HasSuffix(entry.Name(), ".tmp") {
			return true
		}
	}
	return false
}

func readEventsIfValid(path string) []domain.DurableEvent {
	raw, err := os.ReadFile(path)
	if err != nil || len(raw) == 0 || raw[len(raw)-1] != '\n' {
		return nil
	}
	lines := bytes.Split(raw[:len(raw)-1], []byte{'\n'})
	events := make([]domain.DurableEvent, len(lines))
	for index, line := range lines {
		if json.Unmarshal(line, &events[index]) != nil {
			return nil
		}
	}
	return events
}

type conditionalContext struct {
	context.Context
	checks      int
	minChecks   int
	condition   func() bool
	callback    func() error
	triggered   bool
	callbackErr error
}

func (c *conditionalContext) Err() error {
	c.checks++
	if !c.triggered && c.checks >= c.minChecks && c.condition() {
		c.triggered = true
		c.callbackErr = c.callback()
	}
	return c.Context.Err()
}

type overflowErrorReader struct {
	err  error
	done bool
}

func (r *overflowErrorReader) Read(buffer []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	for index := range buffer {
		buffer[index] = 'x'
	}
	return len(buffer), r.err
}

func closeDescriptorForPath(target string) error {
	info, err := os.Stat(target)
	if err != nil {
		return err
	}
	want, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("stat identity for %q is unavailable", target)
	}
	for fd := 3; fd < 256; fd++ {
		var got syscall.Stat_t
		if syscall.Fstat(fd, &got) == nil && got.Dev == want.Dev && got.Ino == want.Ino {
			return syscall.Close(fd)
		}
	}
	return fmt.Errorf("open descriptor for %q not found", target)
}

func descriptorForPathIsOpen(target string) bool {
	info, err := os.Stat(target)
	if err != nil {
		return false
	}
	want, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	for fd := 3; fd < 256; fd++ {
		var got syscall.Stat_t
		if syscall.Fstat(fd, &got) == nil && got.Dev == want.Dev && got.Ino == want.Ino {
			return true
		}
	}
	return false
}
