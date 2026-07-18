package search

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/tools/output"
)

func TestSearchFailsClosedWhenRootBecomesSymlinkBeforeOpen(t *testing.T) {
	workspace := t.TempDir()
	target := filepath.Join(workspace, "target")
	outside := t.TempDir()
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("outside secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tool := New(Options{Workspace: workspace, LookPath: func(string) (string, error) { return "", exec.ErrNotFound }, Output: output.Options{SessionID: "s", Artifacts: searchDiscardStore{}}})
	preparedTool, err := tool.Prepare(context.Background(), domain.ToolRequest{CallID: "c", Name: "search", Workspace: workspace, Input: json.RawMessage(`{"query":"secret","path":"target"}`)})
	if err != nil {
		t.Fatal(err)
	}
	prepared := preparedTool.(*prepared)
	prepared.beforeOpen = func() error {
		if err := os.Remove(target); err != nil {
			return err
		}
		return os.Symlink(outside, target)
	}

	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolFailed || !strings.Contains(result.Content, "open search root") || strings.Contains(result.Content, "outside secret") {
		t.Fatalf("result=%#v", result)
	}
}

func TestDescriptorPlanningProducesCanonicalSearchResource(t *testing.T) {
	workspace := t.TempDir()
	root := filepath.Join(workspace, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	tool := New(Options{Workspace: workspace, LookPath: func(string) (string, error) { return "", exec.ErrNotFound }, Output: output.Options{SessionID: "s", Artifacts: searchDiscardStore{}}})
	planned, err := tool.Plan(context.Background(), domain.ToolRequest{CallID: "c", Name: "search", Workspace: workspace, Input: json.RawMessage(`{"query":"needle","path":"root"}`)})
	if err != nil {
		t.Fatal(err)
	}
	preview := planned.Preview()
	if len(preview.Resources) != 1 || preview.Resources[0].Kind != "directory" || preview.Resources[0].CanonicalID == "" {
		t.Fatalf("preview=%#v", preview)
	}
}

type searchDiscardStore struct{}

func (searchDiscardStore) Put(_ context.Context, sessionID, mediaType string, source io.Reader, limit int64) (domain.Artifact, error) {
	written, err := io.Copy(io.Discard, io.LimitReader(source, limit+1))
	return domain.Artifact{ID: "a", SessionID: sessionID, MediaType: mediaType, Size: written}, err
}

func (searchDiscardStore) Open(context.Context, domain.Artifact) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}
