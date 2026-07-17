package jsonl_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
	"github.com/oklog/ulid/v2"
)

func TestCreatePublishesOnlyCompleteSequenceOneSession(t *testing.T) {
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
	type result struct {
		session domain.Session
		err     error
	}
	done := make(chan result, 1)
	go func() {
		session, err := store.Create(
			context.Background(),
			workspace,
			domain.ModeAsk,
			domain.ModelSelection{Profile: "primary", Model: "model-a"},
		)
		done <- result{session: session, err: err}
	}()

	select {
	case <-clock.eventID:
	case <-time.After(5 * time.Second):
		close(clock.release)
		t.Fatal("Create did not reach sequence-1 event construction")
	}
	sessionsDir := filepath.Join(root, "workspaces", workspace.ID, "sessions")
	entries, err := os.ReadDir(sessionsDir)
	if err != nil {
		close(clock.release)
		t.Fatal(err)
	}
	for _, entry := range entries {
		if _, parseErr := ulid.ParseStrict(entry.Name()); parseErr == nil {
			close(clock.release)
			t.Fatalf("final session %q was visible before sequence 1 was durable", entry.Name())
		}
	}
	close(clock.release)
	created := <-done
	if created.err != nil {
		t.Fatal(created.err)
	}
	replay, err := store.Load(context.Background(), created.session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replay.ReadOnly || len(replay.Events) != 1 || replay.Events[0].Seq != 1 || replay.Events[0].Kind != domain.EventSessionCreated {
		t.Fatalf("published replay=%+v want complete sequence-1 session", replay)
	}
}

func TestCreateReconcilesOnlyStrictStaleStagingDirectories(t *testing.T) {
	root := t.TempDir()
	store, workspace, _ := createTestSession(t, root)
	sessionsDir := filepath.Join(root, "workspaces", workspace.ID, "sessions")
	stale := ".yordam-create-01ARZ3NDEKTSV4RRFFQ69G5FAV-" + strings.Repeat("a", 32) + ".tmp"
	stalePath := filepath.Join(sessionsDir, stale)
	if err := os.Mkdir(stalePath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stalePath, "partial"), []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	lookalike := ".yordam-create-01ARZ3NDEKTSV4RRFFQ69G5FAW-" + strings.Repeat("b", 32) + ".tmp"
	lookalikePath := filepath.Join(sessionsDir, lookalike)
	if err := os.Symlink(outside, lookalikePath); err != nil {
		t.Fatal(err)
	}

	listed, err := store.List(context.Background(), workspace)
	if err != nil || len(listed) != 1 {
		t.Fatalf("List with stale staging state=%v err=%v", listed, err)
	}
	if _, err := store.Create(
		context.Background(),
		workspace,
		domain.ModeAsk,
		domain.ModelSelection{Profile: "primary", Model: "model-a"},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(stalePath); !os.IsNotExist(err) {
		t.Fatalf("strict stale staging directory was not reconciled: %v", err)
	}
	if info, err := os.Lstat(lookalikePath); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("staging-shaped symlink was removed or followed: info=%v err=%v", info, err)
	}
	if entries, err := os.ReadDir(outside); err != nil || len(entries) != 0 {
		t.Fatalf("staging cleanup escaped through symlink: entries=%v err=%v", entries, err)
	}
}

func TestCreateRejectsPreexistingLayoutSymlinks(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(t *testing.T, root, outside string, workspace domain.Workspace)
	}{
		{
			name: "workspaces",
			prepare: func(t *testing.T, root, outside string, _ domain.Workspace) {
				t.Helper()
				if err := os.Symlink(outside, filepath.Join(root, "workspaces")); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "workspace ID",
			prepare: func(t *testing.T, root, outside string, workspace domain.Workspace) {
				t.Helper()
				if err := os.Mkdir(filepath.Join(root, "workspaces"), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, filepath.Join(root, "workspaces", workspace.ID)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "sessions",
			prepare: func(t *testing.T, root, outside string, workspace domain.Workspace) {
				t.Helper()
				workspaceDir := filepath.Join(root, "workspaces", workspace.ID)
				if err := os.MkdirAll(workspaceDir, 0o700); err != nil {
					t.Fatal(err)
				}
				writeJSONFile(t, filepath.Join(workspaceDir, "workspace.json"), workspace)
				if err := os.Symlink(outside, filepath.Join(workspaceDir, "sessions")); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "workspace metadata",
			prepare: func(t *testing.T, root, outside string, workspace domain.Workspace) {
				t.Helper()
				workspaceDir := filepath.Join(root, "workspaces", workspace.ID)
				if err := os.MkdirAll(workspaceDir, 0o700); err != nil {
					t.Fatal(err)
				}
				target := filepath.Join(outside, "workspace.json")
				writeJSONFile(t, target, workspace)
				if err := os.Symlink(target, filepath.Join(workspaceDir, "workspace.json")); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			outside := t.TempDir()
			workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			test.prepare(t, root, outside, workspace)
			before, err := snapshotTree(outside)
			if err != nil {
				t.Fatal(err)
			}
			store := jsonl.New(root, jsonl.Options{
				Clock:   func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) },
				Entropy: strings.NewReader(strings.Repeat("a", 1024)),
			})
			if _, err := store.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"}); err == nil {
				t.Fatal("Create accepted a pre-existing layout symlink")
			}
			after, err := snapshotTree(outside)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatalf("Create changed symlink target tree: before=%q after=%q", before, after)
			}
		})
	}
}

func TestCreateRejectsPreexistingFinalSessionSymlink(t *testing.T) {
	root := t.TempDir()
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	expected, err := ulid.New(ulid.Timestamp(now), strings.NewReader(strings.Repeat("a", 1024)))
	if err != nil {
		t.Fatal(err)
	}
	workspaceDir := filepath.Join(root, "workspaces", workspace.ID)
	sessionsDir := filepath.Join(workspaceDir, "sessions")
	if err := os.MkdirAll(sessionsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeJSONFile(t, filepath.Join(workspaceDir, "workspace.json"), workspace)
	outside := t.TempDir()
	finalPath := filepath.Join(sessionsDir, expected.String())
	if err := os.Symlink(outside, finalPath); err != nil {
		t.Fatal(err)
	}
	store := jsonl.New(root, jsonl.Options{
		Clock:   func() time.Time { return now },
		Entropy: strings.NewReader(strings.Repeat("a", 1024)),
	})

	if _, err := store.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"}); err == nil {
		t.Fatal("Create replaced a pre-existing final-session symlink")
	}
	if info, err := os.Lstat(finalPath); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("final-session symlink was replaced: info=%v err=%v", info, err)
	}
	if entries, err := os.ReadDir(outside); err != nil || len(entries) != 0 {
		t.Fatalf("Create wrote through final-session symlink: entries=%v err=%v", entries, err)
	}
}

func TestListRejectsPreexistingSessionAndMetadataSymlinks(t *testing.T) {
	t.Run("session directory", func(t *testing.T) {
		root := t.TempDir()
		store, workspace, session := createTestSession(t, root)
		sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
		outside := t.TempDir()
		if err := os.Rename(sessionDir, filepath.Join(outside, "session")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join(outside, "session"), sessionDir); err != nil {
			t.Fatal(err)
		}
		if _, err := store.List(context.Background(), workspace); err == nil {
			t.Fatal("List followed a session directory symlink")
		}
	})

	t.Run("metadata leaf", func(t *testing.T) {
		root := t.TempDir()
		store, workspace, session := createTestSession(t, root)
		metadataPath := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "metadata.json")
		outside := filepath.Join(t.TempDir(), "metadata.json")
		if err := os.Rename(metadataPath, outside); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, metadataPath); err != nil {
			t.Fatal(err)
		}
		if _, err := store.List(context.Background(), workspace); err == nil {
			t.Fatal("List followed a metadata symlink")
		}
	})
}

func TestListRejectsWorkspaceIdentityMismatch(t *testing.T) {
	root := t.TempDir()
	store, workspace, _ := createTestSession(t, root)
	workspacePath := filepath.Join(root, "workspaces", workspace.ID, "workspace.json")
	stored, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stored.ID = workspace.ID
	writeJSONFile(t, workspacePath, stored)

	if _, err := store.List(context.Background(), workspace); err == nil || !strings.Contains(err.Error(), "workspace identity mismatch") {
		t.Fatalf("List error=%v want workspace identity mismatch", err)
	}
}

func TestPersistentJSONReadsAreBounded(t *testing.T) {
	t.Run("session metadata", func(t *testing.T) {
		root := t.TempDir()
		store, workspace, session := createTestSession(t, root)
		metadataPath := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "metadata.json")
		session.Title = strings.Repeat("x", 128<<10)
		writeJSONFile(t, metadataPath, session)
		if _, err := store.List(context.Background(), workspace); err == nil || !strings.Contains(err.Error(), "exceeds") {
			t.Fatalf("List error=%v want bounded metadata error", err)
		}
	})

	t.Run("workspace metadata", func(t *testing.T) {
		root := t.TempDir()
		workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		workspaceDir := filepath.Join(root, "workspaces", workspace.ID)
		if err := os.MkdirAll(workspaceDir, 0o700); err != nil {
			t.Fatal(err)
		}
		raw := []byte(fmt.Sprintf(`{"id":%q,"canonical_path":%q}`, workspace.ID, workspace.CanonicalPath))
		raw = append(raw, bytes.Repeat([]byte(" "), 128<<10)...)
		if err := os.WriteFile(filepath.Join(workspaceDir, "workspace.json"), raw, 0o600); err != nil {
			t.Fatal(err)
		}
		store := jsonl.New(root, jsonl.Options{})
		if _, err := store.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"}); err == nil || !strings.Contains(err.Error(), "exceeds") {
			t.Fatalf("Create error=%v want bounded workspace metadata error", err)
		}
	})

	t.Run("recovery artifact", func(t *testing.T) {
		root := t.TempDir()
		store, workspace, session := createTestSession(t, root)
		contents := bytes.Repeat([]byte("r"), int(jsonl.MaxArtifactBytes)+1)
		digest := fmt.Sprintf("%x", sha256.Sum256(contents[:jsonl.MaxArtifactBytes]))
		name := "recovery-" + strings.Repeat("0", sha256.Size*2) + "-" + digest + ".bin"
		path := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "artifacts", name)
		if err := os.WriteFile(path, contents, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Load(context.Background(), session.ID); err == nil || !strings.Contains(err.Error(), "exceeds") {
			t.Fatalf("Load error=%v want bounded recovery artifact error", err)
		}
	})
}

type createGateClock struct {
	mu      sync.Mutex
	now     time.Time
	calls   int
	eventID chan struct{}
	release chan struct{}
	once    sync.Once
}

func newCreateGateClock(now time.Time) *createGateClock {
	return &createGateClock{now: now, eventID: make(chan struct{}), release: make(chan struct{})}
}

func (c *createGateClock) Now() time.Time {
	c.mu.Lock()
	c.calls++
	call := c.calls
	c.mu.Unlock()
	if call == 3 {
		c.once.Do(func() { close(c.eventID) })
		<-c.release
	}
	return c.now
}

func snapshotTree(root string) ([]byte, error) {
	var snapshot bytes.Buffer
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		fmt.Fprintf(&snapshot, "%s:%s\n", relative, info.Mode())
		if info.Mode().IsRegular() {
			contents, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			snapshot.Write(contents)
			snapshot.WriteByte('\n')
		}
		return nil
	})
	return snapshot.Bytes(), err
}
