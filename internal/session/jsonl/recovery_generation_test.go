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
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
)

func TestLoadRecoversDistinctTailGenerations(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	eventsPath := filepath.Join(sessionDir, "events.jsonl")
	artifactsDir := filepath.Join(sessionDir, "artifacts")
	tails := [][]byte{[]byte(`{"first":`), []byte(`{"second":`)}

	for _, tail := range tails {
		appendFile(t, eventsPath, tail)
		replay, err := store.Load(context.Background(), session.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(replay.RecoveryNote, "incomplete final line") {
			t.Fatalf("recovery note=%q", replay.RecoveryNote)
		}
	}

	wantNames := make([]string, 0, len(tails))
	for _, tail := range tails {
		final, _ := recoveryGenerationNames(tail)
		wantNames = append(wantNames, final)
		contents, err := os.ReadFile(filepath.Join(artifactsDir, final))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(contents, tail) {
			t.Fatalf("generation %q=%q want %q", final, contents, tail)
		}
	}
	sort.Strings(wantNames)
	if got := directoryNames(t, artifactsDir); strings.Join(got, "\x00") != strings.Join(wantNames, "\x00") {
		t.Fatalf("recovery generations=%v want %v", got, wantNames)
	}
}

func TestLoadDistinguishesTailsThatDifferBeyondRetentionCap(t *testing.T) {
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

	for _, tail := range tails {
		appendFile(t, eventsPath, tail)
		if _, err := store.Load(context.Background(), session.ID); err != nil {
			t.Fatal(err)
		}
	}
	names := directoryNames(t, artifactsDir)
	if len(names) != 2 || names[0] == names[1] {
		t.Fatalf("recovery generations=%v want two distinct capped generations", names)
	}
	for _, name := range names {
		info, err := os.Stat(filepath.Join(artifactsDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != jsonl.MaxArtifactBytes {
			t.Fatalf("generation %q size=%d want %d", name, info.Size(), jsonl.MaxArtifactBytes)
		}
	}
}

func TestLoadReconcilesStaleRecoveryTemporary(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	eventsPath := filepath.Join(sessionDir, "events.jsonl")
	artifactsDir := filepath.Join(sessionDir, "artifacts")
	tail := []byte(`{"stale-temp":`)
	final, temporary := recoveryGenerationNames(tail)
	if err := os.WriteFile(filepath.Join(artifactsDir, temporary), []byte("partial stale bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	appendFile(t, eventsPath, tail)

	replay, err := store.Load(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(replay.RecoveryNote, "incomplete final line") {
		t.Fatalf("recovery note=%q", replay.RecoveryNote)
	}
	if got := directoryNames(t, artifactsDir); len(got) != 1 || got[0] != final {
		t.Fatalf("reconciled artifacts=%v want [%s]", got, final)
	}
	contents, err := os.ReadFile(filepath.Join(artifactsDir, final))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(contents, tail) {
		t.Fatalf("recovered contents=%q want %q", contents, tail)
	}
}

func TestLoadReconcilesRecoveryFinalAndTemporary(t *testing.T) {
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
	if got := directoryNames(t, artifactsDir); len(got) != 1 || got[0] != final {
		t.Fatalf("reconciled artifacts=%v want [%s]", got, final)
	}
	after, err := os.Stat(finalPath)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("reconciliation replaced the durable recovery generation")
	}
}

func TestLoadReportsOnlyValidDurableRecoveryGenerations(t *testing.T) {
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
	if !strings.Contains(replay.RecoveryNote, "earlier restart") {
		t.Fatalf("durable recovery generation note=%q", replay.RecoveryNote)
	}
}

func TestLoadKeepsRecoveryTransactionOnOpenedSessionRoot(t *testing.T) {
	tests := []struct {
		name      string
		prepare   func(t *testing.T, root string, store *jsonl.Store, workspace domain.Workspace, session domain.Session)
		condition func(sessionDir string) bool
		minChecks int
	}{
		{
			name: "before recovery truncation",
			prepare: func(t *testing.T, root string, _ *jsonl.Store, workspace domain.Workspace, session domain.Session) {
				t.Helper()
				appendFile(t, filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "events.jsonl"), []byte(`{"tail":`))
			},
			condition: func(sessionDir string) bool {
				entries, _ := os.ReadDir(filepath.Join(sessionDir, "artifacts"))
				for _, entry := range entries {
					if strings.HasPrefix(entry.Name(), "recovery-") && strings.HasSuffix(entry.Name(), ".bin") {
						return true
					}
				}
				return false
			},
		},
		{
			name: "before rooted metadata replacement",
			prepare: func(t *testing.T, root string, store *jsonl.Store, workspace domain.Workspace, session domain.Session) {
				t.Helper()
				if _, err := store.Append(context.Background(), session.ID, domain.EventUserMessage, map[string]string{"content": "durable"}); err != nil {
					t.Fatal(err)
				}
				metadataPath := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "metadata.json")
				var metadata domain.Session
				raw, err := os.ReadFile(metadataPath)
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(raw, &metadata); err != nil {
					t.Fatal(err)
				}
				metadata.LastSeq--
				writeJSONFile(t, metadataPath, metadata)
			},
			condition: metadataTemporaryExists,
		},
		{
			name: "before interruption append",
			prepare: func(t *testing.T, _ string, store *jsonl.Store, _ domain.Workspace, session domain.Session) {
				t.Helper()
				if _, err := store.Append(context.Background(), session.ID, domain.EventToolStarted, map[string]string{"call_id": "open"}); err != nil {
					t.Fatal(err)
				}
			},
			condition: func(string) bool { return true },
			minChecks: 4,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			store, workspace, session := createTestSession(t, root)
			test.prepare(t, root, store, workspace, session)
			sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
			movedDir := sessionDir + "-opened"
			var replacementEvents, replacementMetadata []byte
			ctx := &conditionalContext{
				Context:   context.Background(),
				minChecks: test.minChecks,
				condition: func() bool { return test.condition(sessionDir) },
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

			if _, err := store.Load(ctx, session.ID); err == nil {
				t.Fatal("Load continued after the opened session directory was substituted")
			}
			if !ctx.triggered {
				t.Fatal("session substitution boundary was not reached")
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
				t.Fatal("Load mutated the substituted session directory")
			}
		})
	}
}

func TestLoadReverifiesRecoveryLeavesImmediatelyBeforeTruncate(t *testing.T) {
	tests := []struct {
		name string
		swap func(sessionDir, final string) error
	}{
		{
			name: "artifacts directory",
			swap: func(sessionDir, _ string) error {
				artifactsDir := filepath.Join(sessionDir, "artifacts")
				if err := os.Rename(artifactsDir, filepath.Join(sessionDir, "artifacts-opened")); err != nil {
					return err
				}
				return os.Mkdir(artifactsDir, 0o700)
			},
		},
		{
			name: "recovery generation leaf",
			swap: func(sessionDir, final string) error {
				path := filepath.Join(sessionDir, "artifacts", final)
				if err := os.Rename(path, path+"-opened"); err != nil {
					return err
				}
				return os.WriteFile(path, []byte("substitute"), 0o600)
			},
		},
		{
			name: "events leaf",
			swap: func(sessionDir, _ string) error {
				path := filepath.Join(sessionDir, "events.jsonl")
				if err := os.Rename(path, path+"-opened"); err != nil {
					return err
				}
				return os.WriteFile(path, []byte("substitute events"), 0o600)
			},
		},
		{
			name: "metadata leaf",
			swap: func(sessionDir, _ string) error {
				path := filepath.Join(sessionDir, "metadata.json")
				if err := os.Rename(path, path+"-opened"); err != nil {
					return err
				}
				return os.WriteFile(path, []byte("substitute metadata"), 0o600)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			store, workspace, session := createTestSession(t, root)
			sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
			eventsPath := filepath.Join(sessionDir, "events.jsonl")
			tail := []byte(`{"boundary":`)
			final, _ := recoveryGenerationNames(tail)
			appendFile(t, eventsPath, tail)
			ctx := &conditionalContext{
				Context: context.Background(),
				condition: func() bool {
					_, err := os.Stat(filepath.Join(sessionDir, "artifacts", final))
					return err == nil
				},
				callback: func() error { return test.swap(sessionDir, final) },
			}

			if _, err := store.Load(ctx, session.ID); err == nil {
				t.Fatal("Load truncated after a recovery transaction leaf was substituted")
			}
			if !ctx.triggered {
				t.Fatal("pre-truncate verification boundary was not reached")
			}
			if ctx.callbackErr != nil {
				t.Fatal(ctx.callbackErr)
			}
			openedEvents := eventsPath
			if test.name == "events leaf" {
				openedEvents += "-opened"
			}
			after, err := os.ReadFile(openedEvents)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.HasSuffix(after, tail) {
				t.Fatal("Load truncated the opened event descriptor after substitution")
			}
		})
	}
}

func TestLoadRootedMetadataReplacementRejectsLeafSubstitution(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	metadataPath := filepath.Join(sessionDir, "metadata.json")
	if _, err := store.Append(context.Background(), session.ID, domain.EventUserMessage, map[string]string{"content": "durable"}); err != nil {
		t.Fatal(err)
	}
	var metadata domain.Session
	raw, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &metadata); err != nil {
		t.Fatal(err)
	}
	metadata.LastSeq--
	writeJSONFile(t, metadataPath, metadata)
	substitute := []byte(`{"substitute":true}`)
	ctx := &conditionalContext{
		Context:   context.Background(),
		condition: func() bool { return metadataTemporaryExists(sessionDir) },
		callback: func() error {
			if err := os.Rename(metadataPath, metadataPath+"-opened"); err != nil {
				return err
			}
			return os.WriteFile(metadataPath, substitute, 0o600)
		},
	}

	if _, err := store.Load(ctx, session.ID); err == nil {
		t.Fatal("Load replaced a substituted metadata leaf")
	}
	if !ctx.triggered {
		t.Fatal("rooted metadata replacement boundary was not reached")
	}
	if ctx.callbackErr != nil {
		t.Fatal(ctx.callbackErr)
	}
	after, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, substitute) {
		t.Fatalf("substituted metadata=%q want %q", after, substitute)
	}
	for _, name := range directoryNames(t, sessionDir) {
		if strings.HasPrefix(name, ".metadata-") && strings.HasSuffix(name, ".tmp") {
			t.Fatalf("failed metadata replacement left temporary %q", name)
		}
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
