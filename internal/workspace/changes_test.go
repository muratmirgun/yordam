package workspace_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/tools/output"
	"github.com/muratmirgun/yordam/internal/workspace"
)

func TestInspectReturnsBoundedRedactedGitStatusAndDiff(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init")
	runGit(t, root, "config", "user.email", "test@example.com")
	runGit(t, root, "config", "user.name", "Test User")
	path := filepath.Join(root, "tracked.txt")
	if err := os.WriteFile(path, []byte("before\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, root, "add", "tracked.txt")
	runGit(t, root, "commit", "-m", "initial")
	const sentinel = "git-secret"
	after := strings.Repeat("changed line\n", 4000) + sentinel + "\n"
	if err := os.WriteFile(path, []byte(after), 0o600); err != nil {
		t.Fatal(err)
	}
	store := &artifactStore{}

	changes, err := workspace.Inspect(context.Background(), root, output.Options{
		SessionID: "session-1",
		Artifacts: store,
		Redact:    secret.New(sentinel),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !changes.IsGit || changes.Notice != "" || changes.Status != " M tracked.txt\n" {
		t.Fatalf("changes=%#v", changes)
	}
	if len(changes.Diff) > output.ModelExcerptBytes || strings.Contains(changes.Diff, sentinel) || !strings.Contains(changes.Diff, "[REDACTED]") {
		t.Fatalf("unsafe diff bytes=%d: %s", len(changes.Diff), changes.Diff)
	}
	if fmt.Sprint(changes.ArtifactIDs) != "[artifact-1]" {
		t.Fatalf("artifacts=%v", changes.ArtifactIDs)
	}
	contents := store.contents()
	if len(contents) != 1 || bytes.Contains(contents[0], []byte(sentinel)) {
		t.Fatalf("unsafe artifacts=%d", len(contents))
	}
}

func TestInspectReturnsExactNoticeForNonGitDirectory(t *testing.T) {
	changes, err := workspace.Inspect(context.Background(), t.TempDir(), output.Options{Artifacts: &artifactStore{}})
	if err != nil {
		t.Fatal(err)
	}
	if changes.IsGit || changes.Status != "" || changes.Diff != "" || len(changes.ArtifactIDs) != 0 || changes.Notice != workspace.NonGitNotice {
		t.Fatalf("changes=%#v", changes)
	}
}

func runGit(t *testing.T, root string, args ...string) {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", root}, args...)...)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
}

type artifactStore struct {
	mu        sync.Mutex
	artifacts [][]byte
}

func (s *artifactStore) Put(_ context.Context, sessionID, mediaType string, source io.Reader, limit int64) (domain.Artifact, error) {
	raw, err := io.ReadAll(io.LimitReader(source, limit+1))
	if err != nil {
		return domain.Artifact{}, err
	}
	if int64(len(raw)) > limit {
		return domain.Artifact{}, fmt.Errorf("artifact exceeded limit")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.artifacts = append(s.artifacts, bytes.Clone(raw))
	return domain.Artifact{ID: fmt.Sprintf("artifact-%d", len(s.artifacts)), SessionID: sessionID, MediaType: mediaType, Size: int64(len(raw))}, nil
}

func (*artifactStore) Open(context.Context, domain.Artifact) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *artifactStore) contents() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([][]byte, len(s.artifacts))
	for index, artifact := range s.artifacts {
		result[index] = bytes.Clone(artifact)
	}
	return result
}
