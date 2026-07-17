package read_test

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
	readtool "github.com/muratmirgun/yordam/internal/tools/read"
)

func TestReadDescriptorHasExactSchema(t *testing.T) {
	tool := newTool(t, t.TempDir())
	descriptor := tool.Descriptor()
	if descriptor.Name != "read" || descriptor.Mutation != domain.MutationReadOnly {
		t.Fatalf("descriptor=%#v", descriptor)
	}
	if err := descriptor.Validate(); err != nil {
		t.Fatal(err)
	}

	var schema struct {
		Type                 string `json:"type"`
		AdditionalProperties bool   `json:"additionalProperties"`
		Required             []string
		Properties           map[string]struct {
			Type    string `json:"type"`
			Default *int   `json:"default"`
			Minimum *int   `json:"minimum"`
			Maximum *int   `json:"maximum"`
		}
	}
	if err := json.Unmarshal(descriptor.InputSchema, &schema); err != nil {
		t.Fatal(err)
	}
	if schema.Type != "object" || schema.AdditionalProperties || fmt.Sprint(schema.Required) != "[path]" {
		t.Fatalf("schema=%s", descriptor.InputSchema)
	}
	if len(schema.Properties) != 3 || schema.Properties["path"].Type != "string" {
		t.Fatalf("properties=%#v", schema.Properties)
	}
	offset, limit := schema.Properties["offset"], schema.Properties["limit"]
	if offset.Type != "integer" || offset.Default == nil || *offset.Default != 1 || offset.Minimum == nil || *offset.Minimum != 1 || offset.Maximum != nil {
		t.Fatalf("offset schema=%#v", offset)
	}
	if limit.Type != "integer" || limit.Default == nil || *limit.Default != 200 || limit.Minimum == nil || *limit.Minimum != 1 || limit.Maximum == nil || *limit.Maximum != 2000 {
		t.Fatalf("limit schema=%#v", limit)
	}
}

func TestReadNumbersSelectedLinesAndUsesDefaults(t *testing.T) {
	root := t.TempDir()
	lines := make([]string, 205)
	for index := range lines {
		lines[index] = fmt.Sprintf("line-%d", index+1)
	}
	writeFile(t, filepath.Join(root, "a.txt"), strings.Join(lines, "\n")+"\n")
	tool := newTool(t, root)

	prepared, err := tool.Prepare(context.Background(), request("read", `{"path":"a.txt","offset":2,"limit":1}`, root))
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.EvalSymlinks(filepath.Join(root, "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	preview := prepared.Preview()
	if preview.CanonicalScope != canonical || !preview.InsideWorkspace || preview.Request.CallID != "call-1" {
		t.Fatalf("preview=%#v", preview)
	}
	got := prepared.Execute(context.Background())
	if got.Status != domain.ToolSucceeded || got.Content != "2: line-2" || got.CallID != "call-1" {
		t.Fatalf("result=%#v", got)
	}

	prepared, err = tool.Prepare(context.Background(), request("read", `{"path":"a.txt"}`, root))
	if err != nil {
		t.Fatal(err)
	}
	got = prepared.Execute(context.Background())
	if got.Status != domain.ToolSucceeded || !strings.HasPrefix(got.Content, "1: line-1\n2: line-2") || strings.Contains(got.Content, "201: line-201") {
		t.Fatalf("default result=%#v", got)
	}
}

func TestReadRejectsInvalidInputsDirectoriesAndBinary(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "binary.bin"), "a\x00b")
	if err := os.WriteFile(filepath.Join(root, "invalid.txt"), []byte{0xff, '\n'}, 0o600); err != nil {
		t.Fatal(err)
	}
	tool := newTool(t, root)

	tests := []string{
		`{}`,
		`{"path":"."}`,
		`{"path":"binary.bin"}`,
		`{"path":"invalid.txt"}`,
		`{"path":"binary.bin","offset":0}`,
		`{"path":"binary.bin","limit":2001}`,
		`{"path":"binary.bin","extra":true}`,
	}
	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			if _, err := tool.Prepare(context.Background(), request("read", input, root)); err == nil {
				t.Fatal("Prepare accepted invalid input")
			}
		})
	}
}

func TestReadReportsOutsideScopeAndRejectsChangedCanonicalPath(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	writeFile(t, filepath.Join(outside, "first.txt"), "first\n")
	writeFile(t, filepath.Join(outside, "second.txt"), "second\n")
	link := filepath.Join(root, "target")
	if err := os.Symlink(filepath.Join(outside, "first.txt"), link); err != nil {
		t.Fatal(err)
	}
	tool := newTool(t, root)
	prepared, err := tool.Prepare(context.Background(), request("read", `{"path":"target"}`, root))
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Preview().InsideWorkspace {
		t.Fatalf("escaped preview=%#v", prepared.Preview())
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "second.txt"), link); err != nil {
		t.Fatal(err)
	}

	got := prepared.Execute(context.Background())
	if got.Status != domain.ToolFailed || got.ErrorKind != domain.ErrorToolFailed || !strings.Contains(got.Content, "canonical scope changed") {
		t.Fatalf("result=%#v", got)
	}
}

func TestReadOutsidePrepareDoesNotOpenFileBeforeAuthorization(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.txt")
	writeFile(t, outside, "secret\n")
	tool := newTool(t, root)
	prepared, err := tool.Prepare(context.Background(), request("read", fmt.Sprintf(`{"path":%q}`, outside), root))
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Preview().InsideWorkspace {
		t.Fatalf("outside preview=%#v", prepared.Preview())
	}
	if err := os.Chmod(outside, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(outside, 0o600) })
	got := prepared.Execute(context.Background())
	if got.Status != domain.ToolFailed || !strings.Contains(got.Content, "open read path") {
		t.Fatalf("outside content was opened during Prepare: %#v", got)
	}
}

func TestReadConstructorRequiresArtifactStore(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("New did not reject a missing artifact store")
		}
	}()
	readtool.New(readtool.Options{Workspace: t.TempDir()})
}

func newTool(t *testing.T, workspace string) *readtool.Tool {
	t.Helper()
	return readtool.New(readtool.Options{
		Workspace: workspace,
		Output:    output.Options{SessionID: "session-1", Artifacts: discardArtifactStore{}},
	})
}

func request(name, input, workspace string) domain.ToolRequest {
	return domain.ToolRequest{CallID: "call-1", Name: name, Input: json.RawMessage(input), Workspace: workspace}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

type discardArtifactStore struct{}

func (discardArtifactStore) Put(_ context.Context, sessionID, mediaType string, source io.Reader, limit int64) (domain.Artifact, error) {
	written, err := io.Copy(io.Discard, io.LimitReader(source, limit+1))
	if err != nil {
		return domain.Artifact{}, err
	}
	return domain.Artifact{ID: "artifact-1", SessionID: sessionID, MediaType: mediaType, Size: written}, nil
}

func (discardArtifactStore) Open(context.Context, domain.Artifact) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}
