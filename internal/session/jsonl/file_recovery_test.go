package jsonl_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
	"golang.org/x/sys/unix"
)

func TestLoadReportsPlannedFileStateWithoutReplayingMutation(t *testing.T) {
	tests := []struct {
		name        string
		plannedBody string
		wantNote    string
	}{
		{name: "after state present", plannedBody: "after\n", wantNote: "planned after-state present"},
		{name: "change not confirmed", plannedBody: "different\n", wantNote: "planned change not confirmed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, _, session := createTestSession(t, t.TempDir())
			parent, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(parent, "target.txt")
			contents := []byte("after\n")
			if err := os.WriteFile(target, contents, 0o640); err != nil {
				t.Fatal(err)
			}
			before, err := os.Stat(target)
			if err != nil {
				t.Fatal(err)
			}
			plannedHash := sha256.Sum256([]byte(test.plannedBody))
			if _, err := store.Append(context.Background(), session.ID, domain.EventFileChangePlanned, domain.FileChangePlan{
				CallID:         "call-1",
				Path:           target,
				ExpectedSHA256: "before",
				PlannedSHA256:  fmt.Sprintf("%x", plannedHash),
				Diff:           "diff",
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Append(context.Background(), session.ID, domain.EventToolStarted, map[string]string{"call_id": "call-1"}); err != nil {
				t.Fatal(err)
			}

			replay, err := store.Load(context.Background(), session.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(replay.RecoveryNote, test.wantNote) {
				t.Fatalf("recovery note=%q want %q", replay.RecoveryNote, test.wantNote)
			}

			after, err := os.Stat(target)
			if err != nil {
				t.Fatal(err)
			}
			if !os.SameFile(before, after) {
				t.Fatal("replay replaced the target")
			}
			if before.Size() != after.Size() || before.Mode() != after.Mode() || !before.ModTime().Equal(after.ModTime()) {
				t.Fatalf("target metadata changed: before=%#v after=%#v", before, after)
			}
			got, err := os.ReadFile(target)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != string(contents) {
				t.Fatalf("target contents=%q want=%q", got, contents)
			}
		})
	}
}

func TestLoadRecoveryHashRejectsSymlinksAndNeverBlocksOnFIFO(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*testing.T, string, []byte)
	}{
		{
			name: "symlink",
			setup: func(t *testing.T, target string, content []byte) {
				outside := filepath.Join(t.TempDir(), "outside.txt")
				if err := os.WriteFile(outside, content, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, target); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "fifo",
			setup: func(t *testing.T, target string, _ []byte) {
				if err := unix.Mkfifo(target, 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, _, session := createTestSession(t, t.TempDir())
			parent, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(parent, "target")
			contents := []byte("planned\n")
			test.setup(t, target, contents)
			digest := sha256.Sum256(contents)
			if _, err := store.Append(context.Background(), session.ID, domain.EventFileChangePlanned, domain.FileChangePlan{
				CallID:        "call-1",
				Path:          target,
				PlannedSHA256: fmt.Sprintf("%x", digest),
			}); err != nil {
				t.Fatal(err)
			}

			done := make(chan domain.SessionReplay, 1)
			errs := make(chan error, 1)
			go func() {
				replay, err := store.Load(context.Background(), session.ID)
				if err != nil {
					errs <- err
					return
				}
				done <- replay
			}()
			select {
			case err := <-errs:
				t.Fatal(err)
			case replay := <-done:
				if !strings.Contains(replay.RecoveryNote, "planned change not confirmed") || strings.Contains(replay.RecoveryNote, "after-state present") {
					t.Fatalf("recovery note=%q", replay.RecoveryNote)
				}
			case <-time.After(time.Second):
				t.Fatal("recovery hash check blocked on a special file")
			}
		})
	}
}

func TestLoadDoesNotReportCompletedFilePlan(t *testing.T) {
	store, _, session := createTestSession(t, t.TempDir())
	target := filepath.Join(t.TempDir(), "target.txt")
	if err := os.WriteFile(target, []byte("after\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	plan := domain.FileChangePlan{CallID: "call-1", Path: target, PlannedSHA256: strings.Repeat("0", 64)}
	if _, err := store.Append(context.Background(), session.ID, domain.EventFileChangePlanned, plan); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), session.ID, domain.EventFileChanged, domain.FileChange{CallID: "call-1", Path: target}); err != nil {
		t.Fatal(err)
	}

	replay, err := store.Load(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(replay.RecoveryNote, "planned") {
		t.Fatalf("completed plan reported as incomplete: %q", replay.RecoveryNote)
	}
}

func TestLoadCleansRecognizedAbandonedTemporaries(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	workspaceDir := filepath.Join(root, "workspaces", workspace.ID)
	sessionDir := filepath.Join(workspaceDir, "sessions", session.ID)
	artifactsDir := filepath.Join(sessionDir, "artifacts")
	paths := []string{
		filepath.Join(workspaceDir, ".workspace-"+strings.Repeat("a", 32)+".tmp"),
		filepath.Join(sessionDir, ".metadata-"+strings.Repeat("b", 32)+".tmp"),
		filepath.Join(artifactsDir, "."+session.ID+".tmp"),
	}
	for _, path := range paths {
		if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	lookalike := filepath.Join(sessionDir, ".metadata-keep.tmp")
	if err := os.WriteFile(lookalike, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Load(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("abandoned temporary remains at %s: %v", path, err)
		}
	}
	if _, err := os.Stat(lookalike); err != nil {
		t.Fatalf("lookalike removed: %v", err)
	}
}
