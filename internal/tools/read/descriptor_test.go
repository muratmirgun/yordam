package read

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/tools/output"
)

func TestReadFailsClosedWhenTargetBecomesSymlinkBeforeOpen(t *testing.T) {
	workspace := t.TempDir()
	target := filepath.Join(workspace, "target.txt")
	outside := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(target, []byte("inside\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outside, []byte("outside secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tool := New(Options{Workspace: workspace, Output: output.Options{SessionID: "s", Artifacts: readDiscardStore{}}})
	preparedTool, err := tool.Prepare(context.Background(), domain.ToolRequest{CallID: "c", Name: "read", Workspace: workspace, Input: json.RawMessage(`{"path":"target.txt"}`)})
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
	if result.Status != domain.ToolFailed || !strings.Contains(result.Content, "open read path") || strings.Contains(result.Content, "outside secret") {
		t.Fatalf("result=%#v", result)
	}
}

func TestDescriptorPlanningDoesNotOpenReadContent(t *testing.T) {
	workspace := t.TempDir()
	target := filepath.Join(workspace, "target.txt")
	if err := os.WriteFile(target, []byte("not opened while planning\n"), 0o000); err != nil {
		t.Fatal(err)
	}
	tool := New(Options{Workspace: workspace, Output: output.Options{SessionID: "s", Artifacts: readDiscardStore{}}})
	planned, err := tool.Plan(context.Background(), domain.ToolRequest{CallID: "c", Name: "read", Workspace: workspace, Input: json.RawMessage(`{"path":"target.txt"}`)})
	if err != nil {
		t.Fatal(err)
	}
	preview := planned.Preview()
	if len(preview.Resources) != 1 || preview.Resources[0].Kind != "file" || preview.Resources[0].CanonicalID == "" {
		t.Fatalf("preview=%#v", preview)
	}
}

type readDiscardStore struct{}

func (readDiscardStore) Put(_ context.Context, sessionID, mediaType string, source io.Reader, limit int64) (domain.Artifact, error) {
	written, err := io.Copy(io.Discard, io.LimitReader(source, limit+1))
	return domain.Artifact{ID: "a", SessionID: sessionID, MediaType: mediaType, Size: written}, err
}

func (readDiscardStore) Open(context.Context, domain.Artifact) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}
