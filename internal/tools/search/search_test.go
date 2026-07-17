package search_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/tools/output"
	searchtool "github.com/muratmirgun/yordam/internal/tools/search"
)

func TestSearchDescriptorHasExactSchema(t *testing.T) {
	tool := newFallbackTool(t, t.TempDir(), 0)
	descriptor := tool.Descriptor()
	if descriptor.Name != "search" || descriptor.Mutation != domain.MutationReadOnly {
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
			Type    any    `json:"type"`
			Default string `json:"default"`
			Items   *struct {
				Type string `json:"type"`
			} `json:"items"`
		}
	}
	if err := json.Unmarshal(descriptor.InputSchema, &schema); err != nil {
		t.Fatal(err)
	}
	if schema.Type != "object" || schema.AdditionalProperties || fmt.Sprint(schema.Required) != "[query]" || len(schema.Properties) != 4 {
		t.Fatalf("schema=%s", descriptor.InputSchema)
	}
	if schema.Properties["query"].Type != "string" || schema.Properties["path"].Type != "string" || schema.Properties["path"].Default != "." {
		t.Fatalf("properties=%#v", schema.Properties)
	}
	for _, name := range []string{"include", "exclude"} {
		property := schema.Properties[name]
		if property.Type != "array" || property.Items == nil || property.Items.Type != "string" {
			t.Fatalf("%s schema=%#v", name, property)
		}
	}
}

func TestSearchFallbackCapsAndOrdersMatches(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "b.txt"), "needle\n")
	writeFile(t, filepath.Join(root, "a.txt"), "first\nneedle\n")
	tool := newFallbackTool(t, root, 1)

	prepared, err := tool.Prepare(context.Background(), request("search", `{"query":"needle"}`, root))
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	preview := prepared.Preview()
	if preview.CanonicalScope != canonical || !preview.InsideWorkspace {
		t.Fatalf("preview=%#v", preview)
	}
	got := prepared.Execute(context.Background())
	if got.Status != domain.ToolSucceeded || got.Content != "a.txt:2:needle" || !got.Truncated || got.CallID != "call-1" {
		t.Fatalf("result=%#v", got)
	}
}

func TestSearchRipgrepAndFallbackParityWithGlobsAndSymlinkEscapes(t *testing.T) {
	rg, err := exec.LookPath("rg")
	if err != nil {
		t.Skip("rg is not installed")
	}
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(root, "b.txt"), "needle b\n")
	writeFile(t, filepath.Join(root, "a.txt"), "needle a\n")
	writeFile(t, filepath.Join(root, "colon:name.txt"), "needle colon\n")
	writeFile(t, filepath.Join(root, "skip.log"), "needle log\n")
	writeFile(t, filepath.Join(root, "nested", "keep.txt"), "needle nested\n")
	writeFile(t, filepath.Join(root, "nested", "skip.txt"), "needle excluded\n")
	if err := os.WriteFile(filepath.Join(root, "binary.bin"), []byte("needle\x00binary\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(outside, "sentinel.txt"), "OUTSIDE_SENTINEL\nneedle outside\n")
	if err := os.Symlink(filepath.Join(outside, "sentinel.txt"), filepath.Join(root, "escaped-file.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "escaped-dir")); err != nil {
		t.Fatal(err)
	}

	input := `{"query":"needle","include":["*.txt"],"exclude":["nested/skip.txt"]}`
	fallback := execute(t, newFallbackTool(t, root, 500), input, root)
	native := execute(t, newRGTool(t, root, rg, 500), input, root)
	if fallback.Status != domain.ToolSucceeded || native.Status != domain.ToolSucceeded {
		t.Fatalf("fallback=%#v native=%#v", fallback, native)
	}
	if fallback.Content != native.Content || fallback.Truncated != native.Truncated {
		t.Fatalf("fallback=%q truncated=%v\nnative=%q truncated=%v", fallback.Content, fallback.Truncated, native.Content, native.Truncated)
	}
	if strings.Contains(fallback.Content, "OUTSIDE_SENTINEL") || strings.Contains(fallback.Content, "escaped-") {
		t.Fatalf("symlink escape searched: %q", fallback.Content)
	}
	want := "a.txt:1:needle a\nb.txt:1:needle b\ncolon:name.txt:1:needle colon\nnested/keep.txt:1:needle nested"
	if fallback.Content != want {
		t.Fatalf("content=%q want=%q", fallback.Content, want)
	}
	binaryInput := `{"query":"needle","include":["*.bin"]}`
	fallbackBinary := execute(t, newFallbackTool(t, root, 500), binaryInput, root)
	nativeBinary := execute(t, newRGTool(t, root, rg, 500), binaryInput, root)
	if fallbackBinary.Status != domain.ToolSucceeded || nativeBinary.Status != domain.ToolSucceeded || fallbackBinary.Content != nativeBinary.Content || fallbackBinary.Content != "" {
		t.Fatalf("binary fallback=%#v native=%#v", fallbackBinary, nativeBinary)
	}
	for name, tool := range map[string]*searchtool.Tool{
		"fallback": newFallbackTool(t, root, 500),
		"rg":       newRGTool(t, root, rg, 500),
	} {
		t.Run(name+" skips outside sentinel", func(t *testing.T) {
			got := execute(t, tool, `{"query":"OUTSIDE_SENTINEL"}`, root)
			if got.Status != domain.ToolSucceeded || got.Content != "" {
				t.Fatalf("result=%#v", got)
			}
		})
	}
}

func TestSearchRipgrepUsesExactArgumentsAndExitCodes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell fixture is Unix-only")
	}
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a.go"), "literal.*\n")
	script := filepath.Join(t.TempDir(), "fake-rg")
	writeExecutable(t, script, "#!/bin/sh\nexit \"${RG_EXIT:-1}\"\n")
	t.Setenv("RG_EXIT", "1")
	tool := newRGTool(t, root, script, 500)
	got := execute(t, tool, `{"query":"literal.*","include":["*.go"],"exclude":["vendor/**"]}`, root)
	if got.Status != domain.ToolSucceeded || got.Content != "" {
		t.Fatalf("zero-match result=%#v", got)
	}
	t.Setenv("RG_EXIT", "2")
	got = execute(t, tool, `{"query":"literal.*"}`, root)
	if got.Status != domain.ToolFailed || got.ErrorKind != domain.ErrorToolFailed {
		t.Fatalf("failure result=%#v", got)
	}
}

func TestSearchNativeAndFallbackStopAtCapAndShareInvalidUTF8Policy(t *testing.T) {
	rg, err := exec.LookPath("rg")
	if err != nil {
		t.Skip("rg is not installed")
	}
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a.txt"), "needle one\nneedle two\nneedle three\n")
	if err := os.WriteFile(filepath.Join(root, "invalid.txt"), []byte("needle valid prefix\nneedle \xff\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	for name, tool := range map[string]*searchtool.Tool{
		"fallback": newFallbackTool(t, root, 2),
		"native":   newRGTool(t, root, rg, 2),
	} {
		t.Run(name, func(t *testing.T) {
			result := execute(t, tool, `{"query":"needle"}`, root)
			if result.Status != domain.ToolSucceeded || !result.Truncated || result.Content != "a.txt:1:needle one\na.txt:2:needle two" {
				t.Fatalf("result=%#v", result)
			}
		})
	}
}

func TestSearchRejectsInvalidInputAndChangedCanonicalScope(t *testing.T) {
	root := t.TempDir()
	first := t.TempDir()
	second := t.TempDir()
	link := filepath.Join(root, "target")
	if err := os.Symlink(first, link); err != nil {
		t.Fatal(err)
	}
	tool := newFallbackTool(t, root, 0)
	for _, input := range []string{`{}`, `{"query":""}`, `{"query":"x","extra":true}`, `{"query":"x","include":[""]}`} {
		if _, err := tool.Prepare(context.Background(), request("search", input, root)); err == nil {
			t.Fatalf("Prepare accepted %s", input)
		}
	}

	prepared, err := tool.Prepare(context.Background(), request("search", `{"query":"needle","path":"target"}`, root))
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Preview().InsideWorkspace {
		t.Fatalf("escaped preview=%#v", prepared.Preview())
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(second, link); err != nil {
		t.Fatal(err)
	}
	got := prepared.Execute(context.Background())
	if got.Status != domain.ToolFailed || !strings.Contains(got.Content, "canonical scope changed") {
		t.Fatalf("result=%#v", got)
	}
}

func TestSearchConstructorRequiresArtifactStore(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("New did not reject a missing artifact store")
		}
	}()
	searchtool.New(searchtool.Options{Workspace: t.TempDir()})
}

func execute(t *testing.T, tool *searchtool.Tool, input, workspace string) domain.ToolResult {
	t.Helper()
	prepared, err := tool.Prepare(context.Background(), request("search", input, workspace))
	if err != nil {
		t.Fatal(err)
	}
	return prepared.Execute(context.Background())
}

func newFallbackTool(t *testing.T, workspace string, maxMatches int) *searchtool.Tool {
	t.Helper()
	return searchtool.New(searchtool.Options{
		Workspace:  workspace,
		LookPath:   func(string) (string, error) { return "", exec.ErrNotFound },
		MaxMatches: maxMatches,
		Output:     output.Options{SessionID: "session-1", Artifacts: discardArtifactStore{}},
	})
}

func newRGTool(t *testing.T, workspace, executable string, maxMatches int) *searchtool.Tool {
	t.Helper()
	return searchtool.New(searchtool.Options{
		Workspace:  workspace,
		LookPath:   func(string) (string, error) { return executable, nil },
		MaxMatches: maxMatches,
		Output:     output.Options{SessionID: "session-1", Artifacts: discardArtifactStore{}},
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

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
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
