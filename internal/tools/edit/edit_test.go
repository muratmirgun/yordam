package edit

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
	"sync"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/tools/output"
)

func TestEditDescriptorHasExactSchema(t *testing.T) {
	tool := newTool(t, t.TempDir(), output.Options{})
	descriptor := tool.Descriptor()
	if descriptor.Name != "edit" || descriptor.Mutation != domain.MutationFile {
		t.Fatalf("descriptor=%#v", descriptor)
	}
	if err := descriptor.Validate(); err != nil {
		t.Fatal(err)
	}

	var schema struct {
		Type                 string `json:"type"`
		AdditionalProperties bool   `json:"additionalProperties"`
		Required             []string
		Properties           map[string]json.RawMessage
	}
	if err := json.Unmarshal(descriptor.InputSchema, &schema); err != nil {
		t.Fatal(err)
	}
	wantProperties := []string{"path", "expected_sha256", "create", "new_content", "replacements"}
	if schema.Type != "object" || schema.AdditionalProperties || fmt.Sprint(schema.Required) != "[path]" || len(schema.Properties) != len(wantProperties) {
		t.Fatalf("schema=%s", descriptor.InputSchema)
	}
	for _, name := range wantProperties {
		if _, ok := schema.Properties[name]; !ok {
			t.Fatalf("schema missing property %q: %s", name, descriptor.InputSchema)
		}
	}
	var replacements struct {
		Type  string
		Items struct {
			Type                 string
			AdditionalProperties bool `json:"additionalProperties"`
			Required             []string
			Properties           map[string]json.RawMessage
		}
	}
	if err := json.Unmarshal(schema.Properties["replacements"], &replacements); err != nil {
		t.Fatal(err)
	}
	if replacements.Type != "array" || replacements.Items.Type != "object" || replacements.Items.AdditionalProperties || fmt.Sprint(replacements.Items.Required) != "[old new]" || len(replacements.Items.Properties) != 3 {
		t.Fatalf("replacements schema=%s", schema.Properties["replacements"])
	}
}

func TestEditPrepareDefersFileInspectionUntilAuthorizedPreview(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "a.txt")
	writeFile(t, path, "old\n", 0o600)
	tool := newTool(t, root, output.Options{})
	preparedTool, err := tool.Prepare(context.Background(), request(fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%s","replacements":[{"old":"old","new":"new"}]}`, strings.Repeat("0", 64)), root))
	if err != nil {
		t.Fatalf("Prepare inspected existing file: %v", err)
	}
	preparedCall := preparedTool.(*prepared)
	if preview := preparedCall.Preview(); preview.FilePlan != nil || preview.ProposedDiff != "" {
		t.Fatalf("Prepare generated preview before authorization: %#v", preview)
	}
	if err := preparedCall.PreparePreview(context.Background()); err == nil || !strings.Contains(err.Error(), "stale preimage") {
		t.Fatalf("PreparePreview error=%v", err)
	}

	createdTool, err := tool.Prepare(context.Background(), request(`{"path":"a.txt","create":true,"new_content":"new"}`, root))
	if err != nil {
		t.Fatalf("Prepare inspected create target: %v", err)
	}
	if err := createdTool.(*prepared).PreparePreview(context.Background()); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("create PreparePreview error=%v", err)
	}
}

func TestPlanDigestResourceCarriesExpectedPreimage(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "target.txt")
	writeFile(t, path, "before\n", 0o600)
	tool := newTool(t, root, output.Options{})
	expected := hashString([]byte("before\n"))
	prepared, err := tool.Plan(context.Background(), request(fmt.Sprintf(`{"path":"target.txt","expected_sha256":"%s","replacements":[{"old":"before","new":"after"}]}`, expected), root))
	if err != nil {
		t.Fatal(err)
	}
	preview := prepared.Preview()
	if len(preview.Resources) != 1 || preview.Resources[0].Digest != expected || preview.FilePlan != nil {
		t.Fatalf("planning read content or omitted preimage identity: %#v", preview)
	}
}

func TestRevalidateRejectsPreviewedExistingFilePreimageDrift(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(string) error
		want   string
	}{
		{name: "content changed at same path", change: func(path string) error { return os.WriteFile(path, []byte("changed\n"), 0o600) }, want: "stale preimage"},
		{name: "target deleted", change: os.Remove, want: "re-resolve edit path"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "target.txt")
			writeFile(t, path, "before\n", 0o600)
			tool := newTool(t, root, output.Options{})
			expected := hashString([]byte("before\n"))
			preparedTool, err := tool.Prepare(context.Background(), request(fmt.Sprintf(`{"path":"target.txt","expected_sha256":"%s","replacements":[{"old":"before","new":"after"}]}`, expected), root))
			if err != nil {
				t.Fatal(err)
			}
			if err := preparedTool.(ports.PreviewPreparer).PreparePreview(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := test.change(path); err != nil {
				t.Fatal(err)
			}
			_, err = preparedTool.(ports.ResourceRevalidator).Revalidate(context.Background())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Revalidate error=%v want containing %q", err, test.want)
			}
		})
	}
}

func TestRevalidateRejectsPreviewedCreateTargetAppearance(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "target.txt")
	tool := newTool(t, root, output.Options{})
	preparedTool, err := tool.Prepare(context.Background(), request(`{"path":"target.txt","create":true,"new_content":"created\n"}`, root))
	if err != nil {
		t.Fatal(err)
	}
	if err := preparedTool.(ports.PreviewPreparer).PreparePreview(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("appeared\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = preparedTool.(ports.ResourceRevalidator).Revalidate(context.Background())
	if err == nil || !strings.Contains(err.Error(), "appeared") {
		t.Fatalf("Revalidate error=%v", err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "appeared\n" {
		t.Fatalf("revalidation mutated appeared target: %q %v", got, err)
	}
}

func TestEditPreviewsAndRejectsStalePreimage(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "a.txt")
	writeFile(t, path, "old\n", 0o640)
	input := fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%s","replacements":[{"old":"old","new":"new","all":false}]}`, hashString([]byte("old\n")))
	tool := newTool(t, root, output.Options{})

	prepared, err := tool.Prepare(context.Background(), request(input, root))
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.(ports.PreviewPreparer).PreparePreview(context.Background()); err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	wantDiff := "--- a/a.txt\n+++ b/a.txt\n@@ -1,2 +1,2 @@\n-old\n+new\n \n"
	preview := prepared.Preview()
	if preview.CanonicalScope != canonical || !preview.InsideWorkspace || preview.ProposedDiff != wantDiff {
		t.Fatalf("preview=%#v", preview)
	}
	if preview.FilePlan == nil || preview.FilePlan.CallID != "call-1" || preview.FilePlan.Path != canonical || preview.FilePlan.ExpectedSHA256 != hashString([]byte("old\n")) || preview.FilePlan.PlannedSHA256 != hashString([]byte("new\n")) || preview.FilePlan.Diff != wantDiff || len(preview.FilePlan.ArtifactIDs) != 0 {
		t.Fatalf("file plan=%#v", preview.FilePlan)
	}
	if got := readFile(t, path); got != "old\n" {
		t.Fatalf("Prepare mutated target: %q", got)
	}

	writeFile(t, path, "changed elsewhere\n", 0o640)
	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolFailed || result.ErrorKind != domain.ErrorToolFailed || !strings.Contains(result.Content, "stale preimage") || result.FileChange != nil {
		t.Fatalf("result=%#v", result)
	}
	if got := readFile(t, path); got != "changed elsewhere\n" {
		t.Fatalf("stale edit mutated target: %q", got)
	}
}

func TestEditReplacesOneOrAllAndReturnsExactChange(t *testing.T) {
	tests := []struct {
		name string
		all  bool
		old  string
		new  string
		want string
		diff string
	}{
		{name: "one", old: "one", new: "ONE", want: "ONE two\n", diff: "--- a/a.txt\n+++ b/a.txt\n@@ -1,2 +1,2 @@\n-one two\n+ONE two\n \n"},
		{name: "all", all: true, old: "o", new: "O", want: "One twO\n", diff: "--- a/a.txt\n+++ b/a.txt\n@@ -1,2 +1,2 @@\n-one two\n+One twO\n \n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "a.txt")
			before := []byte("one two\n")
			writeFile(t, path, string(before), 0o640)
			input := fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%s","replacements":[{"old":%q,"new":%q,"all":%t}]}`, hashString(before), test.old, test.new, test.all)
			prepared, err := newTool(t, root, output.Options{}).Prepare(context.Background(), request(input, root))
			if err != nil {
				t.Fatal(err)
			}

			result := prepared.Execute(context.Background())
			if result.Status != domain.ToolSucceeded || result.CallID != "call-1" || result.ErrorKind != "" || result.Content != test.diff || result.Truncated || len(result.ArtifactIDs) != 0 {
				t.Fatalf("result=%#v", result)
			}
			canonical, err := filepath.EvalSymlinks(path)
			if err != nil {
				t.Fatal(err)
			}
			if result.FileChange == nil || result.FileChange.CallID != "call-1" || result.FileChange.Path != canonical || result.FileChange.BeforeSHA256 != hashString(before) || result.FileChange.AfterSHA256 != hashString([]byte(test.want)) || result.FileChange.Diff != test.diff || len(result.FileChange.ArtifactIDs) != 0 {
				t.Fatalf("file change=%#v", result.FileChange)
			}
			if got := readFile(t, path); got != test.want {
				t.Fatalf("content=%q want=%q", got, test.want)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if got := info.Mode().Perm(); got != 0o640 {
				t.Fatalf("mode=%#o want=%#o", got, 0o640)
			}
		})
	}
}

func TestEditCreateRequiresAbsentTargetAndCreatesMode0600(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "new.txt")
	tool := newTool(t, root, output.Options{})
	prepared, err := tool.Prepare(context.Background(), request(`{"path":"new.txt","create":true,"new_content":"created\n"}`, root))
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.(ports.PreviewPreparer).PreparePreview(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantDiff := "--- a/new.txt\n+++ b/new.txt\n@@ -1 +1,2 @@\n+created\n \n"
	preview := prepared.Preview()
	if preview.ProposedDiff != wantDiff || preview.FilePlan == nil || preview.FilePlan.ExpectedSHA256 != "" || preview.FilePlan.PlannedSHA256 != hashString([]byte("created\n")) {
		t.Fatalf("preview=%#v", preview)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("Prepare created target: %v", err)
	}

	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolSucceeded || result.Content != wantDiff || result.FileChange == nil || result.FileChange.BeforeSHA256 != "" || result.FileChange.AfterSHA256 != hashString([]byte("created\n")) {
		t.Fatalf("result=%#v", result)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("mode=%#o want=%#o", got, 0o600)
	}

	existing, err := tool.Prepare(context.Background(), request(`{"path":"new.txt","create":true,"new_content":"other"}`, root))
	if err != nil {
		t.Fatal(err)
	}
	if err := existing.(ports.PreviewPreparer).PreparePreview(context.Background()); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("create accepted existing target: %v", err)
	}
}

func TestEditRestoresPreparedModeWhenPermissionsChangeBeforeExecute(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "a.txt")
	before := []byte("old\n")
	writeFile(t, path, string(before), 0o640)
	input := fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%s","replacements":[{"old":"old","new":"new"}]}`, hashString(before))
	prepared, err := newTool(t, root, output.Options{}).Prepare(context.Background(), request(input, root))
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.(ports.PreviewPreparer).PreparePreview(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}

	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolSucceeded {
		t.Fatalf("result=%#v", result)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o640 {
		t.Fatalf("mode=%#o want prepared mode %#o", got, 0o640)
	}
}

func TestEditValidatesCreateAndExistingRules(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a.txt"), "one one\n", 0o600)
	hash := hashString([]byte("one one\n"))
	tool := newTool(t, root, output.Options{})
	tests := []struct {
		name    string
		input   string
		want    string
		preview bool
	}{
		{name: "unknown field", input: `{"path":"a.txt","extra":true}`, want: "unknown field"},
		{name: "empty path", input: `{"path":"","create":true}`, want: "path is required"},
		{name: "create hash", input: fmt.Sprintf(`{"path":"new.txt","create":true,"expected_sha256":"%s"}`, hash), want: "expected_sha256 must be empty"},
		{name: "create replacements", input: `{"path":"new.txt","create":true,"replacements":[{"old":"a","new":"b"}]}`, want: "replacements must be empty"},
		{name: "create existing", input: `{"path":"a.txt","create":true}`, want: "already exists", preview: true},
		{name: "missing existing", input: fmt.Sprintf(`{"path":"missing.txt","expected_sha256":"%s","replacements":[{"old":"one","new":"two"}]}`, hash), want: "no such file"},
		{name: "missing hash", input: `{"path":"a.txt","replacements":[{"old":"one","new":"two"}]}`, want: "lowercase 64-character SHA-256"},
		{name: "uppercase hash", input: fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%s","replacements":[{"old":"one","new":"two"}]}`, strings.ToUpper(hash)), want: "lowercase 64-character SHA-256"},
		{name: "short hash", input: `{"path":"a.txt","expected_sha256":"abcd","replacements":[{"old":"one","new":"two"}]}`, want: "lowercase 64-character SHA-256"},
		{name: "no replacements", input: fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%s"}`, hash), want: "at least one replacement"},
		{name: "missing old", input: fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%s","replacements":[{"new":"two","all":true}]}`, hash), want: "old is required"},
		{name: "missing new", input: fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%s","replacements":[{"old":"one one\\n"}]}`, hash), want: "new is required"},
		{name: "one requires exact", input: fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%s","replacements":[{"old":"one","new":"two","all":false}]}`, hash), want: "exactly one occurrence", preview: true},
		{name: "all requires one", input: fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%s","replacements":[{"old":"absent","new":"two","all":true}]}`, hash), want: "at least one occurrence", preview: true},
		{name: "hash mismatch", input: fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%s","replacements":[{"old":"one one","new":"two"}]}`, strings.Repeat("0", 64)), want: "stale preimage", preview: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			preparedTool, err := tool.Prepare(context.Background(), request(test.input, root))
			if err == nil && test.preview {
				err = preparedTool.(ports.PreviewPreparer).PreparePreview(context.Background())
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Prepare error=%v want containing %q", err, test.want)
			}
		})
	}
}

func TestEditUsesExactOccurrenceSemanticsForEmptyOld(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "a.txt")
	writeFile(t, path, "", 0o600)
	input := fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%s","replacements":[{"old":"","new":"content","all":false}]}`, hashString(nil))
	prepared, err := newTool(t, root, output.Options{}).Prepare(context.Background(), request(input, root))
	if err != nil {
		t.Fatal(err)
	}
	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolSucceeded || readFile(t, path) != "content" {
		t.Fatalf("result=%#v content=%q", result, readFile(t, path))
	}
}

func TestEditAppliesReplacementsInOrder(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "a.txt")
	before := []byte("a\n")
	writeFile(t, path, string(before), 0o600)
	input := fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%s","replacements":[{"old":"a","new":"aa"},{"old":"aa","new":"b"}]}`, hashString(before))
	prepared, err := newTool(t, root, output.Options{}).Prepare(context.Background(), request(input, root))
	if err != nil {
		t.Fatal(err)
	}
	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolSucceeded || readFile(t, path) != "b\n" {
		t.Fatalf("result=%#v content=%q", result, readFile(t, path))
	}
}

func TestEditRejectsSymlinkSwapBeforeMutation(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	path := filepath.Join(root, "a.txt")
	outsidePath := filepath.Join(outside, "outside.txt")
	before := []byte("old\n")
	writeFile(t, path, string(before), 0o600)
	writeFile(t, outsidePath, "outside\n", 0o600)
	input := fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%s","replacements":[{"old":"old","new":"new"}]}`, hashString(before))
	prepared, err := newTool(t, root, output.Options{}).Prepare(context.Background(), request(input, root))
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.(ports.PreviewPreparer).PreparePreview(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outsidePath, path); err != nil {
		t.Fatal(err)
	}

	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolFailed || (!strings.Contains(result.Content, "canonical scope changed") && !strings.Contains(result.Content, "open edit parent")) {
		t.Fatalf("result=%#v", result)
	}
	if got := readFile(t, outsidePath); got != "outside\n" {
		t.Fatalf("outside target mutated: %q", got)
	}
}

func TestEditRollsBackTargetSwapDuringAtomicReplace(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	path := filepath.Join(root, "a.txt")
	outsidePath := filepath.Join(outside, "outside.txt")
	before := []byte("old\n")
	writeFile(t, path, string(before), 0o600)
	writeFile(t, outsidePath, "outside\n", 0o600)
	input := fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%s","replacements":[{"old":"old","new":"new"}]}`, hashString(before))
	preparedTool, err := newTool(t, root, output.Options{}).Prepare(context.Background(), request(input, root))
	if err != nil {
		t.Fatal(err)
	}
	prepared := preparedTool.(*prepared)
	prepared.beforeRename = func(_, target string) error {
		if err := os.Remove(target); err != nil {
			return err
		}
		return os.Symlink(outsidePath, target)
	}

	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolFailed || result.FileChange != nil {
		t.Fatalf("result=%#v", result)
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("target swap was not restored: info=%v err=%v", info, err)
	}
	if got := readFile(t, outsidePath); got != "outside\n" {
		t.Fatalf("outside target mutated: %q", got)
	}
}

func TestEditCreateRejectsParentSymlinkSwapBeforeMutation(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	directory := filepath.Join(root, "dir")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	prepared, err := newTool(t, root, output.Options{}).Prepare(context.Background(), request(`{"path":"dir/new.txt","create":true,"new_content":"new"}`, root))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(directory); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, directory); err != nil {
		t.Fatal(err)
	}

	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolFailed || (!strings.Contains(result.Content, "canonical scope changed") && !strings.Contains(result.Content, "open edit parent")) {
		t.Fatalf("result=%#v", result)
	}
	if _, err := os.Lstat(filepath.Join(outside, "new.txt")); !os.IsNotExist(err) {
		t.Fatalf("outside target created: %v", err)
	}
}

func TestEditFailsClosedWhenParentBecomesSymlinkBeforeDescriptorOpen(t *testing.T) {
	workspace := t.TempDir()
	parent := filepath.Join(workspace, "parent")
	moved := filepath.Join(workspace, "moved")
	outside := t.TempDir()
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(parent, "a.txt")
	before := []byte("old\n")
	writeFile(t, path, string(before), 0o600)
	outsidePath := filepath.Join(outside, "a.txt")
	writeFile(t, outsidePath, string(before), 0o600)
	input := fmt.Sprintf(`{"path":"parent/a.txt","expected_sha256":"%s","replacements":[{"old":"old","new":"new"}]}`, hashString(before))
	preparedTool, err := newTool(t, workspace, output.Options{}).Prepare(context.Background(), request(input, workspace))
	if err != nil {
		t.Fatal(err)
	}
	prepared := preparedTool.(*prepared)
	prepared.beforeOpenDirectory = func() error {
		if err := os.Rename(parent, moved); err != nil {
			return err
		}
		return os.Symlink(outside, parent)
	}

	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolFailed || result.FileChange != nil || !strings.Contains(result.Content, "open edit directory") {
		t.Fatalf("result=%#v", result)
	}
	if got := readFile(t, outsidePath); got != string(before) {
		t.Fatalf("outside target mutated: %q", got)
	}
}

func TestEditCreateRejectsTargetAppearingAfterPrepare(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "new.txt")
	prepared, err := newTool(t, root, output.Options{}).Prepare(context.Background(), request(`{"path":"new.txt","create":true,"new_content":"new"}`, root))
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.(ports.PreviewPreparer).PreparePreview(context.Background()); err != nil {
		t.Fatal(err)
	}
	writeFile(t, path, "appeared", 0o640)

	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolFailed || !strings.Contains(result.Content, "target appeared") {
		t.Fatalf("result=%#v", result)
	}
	if got := readFile(t, path); got != "appeared" {
		t.Fatalf("appeared target mutated: %q", got)
	}
}

func TestEditCleansTemporaryFileAfterAtomicWriteFailure(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "a.txt")
	before := []byte("old\n")
	writeFile(t, path, string(before), 0o640)
	input := fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%s","replacements":[{"old":"old","new":"new"}]}`, hashString(before))
	preparedTool, err := newTool(t, root, output.Options{}).Prepare(context.Background(), request(input, root))
	if err != nil {
		t.Fatal(err)
	}
	prepared := preparedTool.(*prepared)
	prepared.beforeRename = func(string, string) error { return errors.New("injected rename failure") }
	syncCalls := 0
	prepared.syncDirectory = func(*os.File) error {
		syncCalls++
		return nil
	}

	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolFailed || !strings.Contains(result.Content, "injected rename failure") {
		t.Fatalf("result=%#v", result)
	}
	if got := readFile(t, path); got != string(before) {
		t.Fatalf("target mutated after failure: %q", got)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "a.txt" {
		t.Fatalf("temporary file leaked: %v", entryNames(entries))
	}
	if syncCalls != 1 {
		t.Fatalf("temporary cleanup directory syncs=%d want=1", syncCalls)
	}
}

func TestEditReportsUncertainMutationWhenRollbackSyncFails(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	path := filepath.Join(root, "a.txt")
	outsidePath := filepath.Join(outside, "outside.txt")
	before := []byte("old\n")
	writeFile(t, path, string(before), 0o600)
	writeFile(t, outsidePath, "outside\n", 0o600)
	input := fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%s","replacements":[{"old":"old","new":"new"}]}`, hashString(before))
	preparedTool, err := newTool(t, root, output.Options{}).Prepare(context.Background(), request(input, root))
	if err != nil {
		t.Fatal(err)
	}
	prepared := preparedTool.(*prepared)
	prepared.beforeRename = func(_, target string) error {
		if err := os.Remove(target); err != nil {
			return err
		}
		return os.Symlink(outsidePath, target)
	}
	prepared.syncDirectory = func(*os.File) error { return errors.New("injected rollback sync failure") }

	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolFailed || result.FileChange == nil || !strings.Contains(result.Content, "durability is uncertain") || !strings.Contains(result.Content, "rollback") {
		t.Fatalf("result=%#v", result)
	}
}

func TestEditBoundsAndRedactsDiffOnce(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "a.txt")
	before := []byte(strings.Repeat("before-line\n", 4000) + "secret\n")
	writeFile(t, path, string(before), 0o600)
	store := &artifactStore{}
	options := output.Options{SessionID: "session-1", Artifacts: store, Redact: secret.New("secret")}
	input := fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%s","replacements":[{"old":"before-line","new":"after-line","all":true},{"old":"secret","new":"revealed"}]}`, hashString(before))
	prepared, err := newTool(t, root, options).Prepare(context.Background(), request(input, root))
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.(ports.PreviewPreparer).PreparePreview(context.Background()); err != nil {
		t.Fatal(err)
	}
	preview := prepared.Preview()
	if preview.FilePlan == nil || len(preview.ProposedDiff) > output.ModelExcerptBytes || strings.Contains(preview.ProposedDiff, "secret") || preview.FilePlan.Diff != preview.ProposedDiff || fmt.Sprint(preview.FilePlan.ArtifactIDs) != "[artifact-1]" {
		t.Fatalf("preview=%#v", preview)
	}
	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolSucceeded || result.Content != preview.ProposedDiff || result.FileChange == nil || result.FileChange.Diff != preview.ProposedDiff || fmt.Sprint(result.ArtifactIDs) != "[artifact-1]" || fmt.Sprint(result.FileChange.ArtifactIDs) != "[artifact-1]" {
		t.Fatalf("result=%#v", result)
	}
	if got := store.count(); got != 1 {
		t.Fatalf("artifact writes=%d want=1", got)
	}
}

func TestEditConcurrentExecuteOnlyOnePreparedEditWins(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "a.txt")
	before := []byte("old\n")
	writeFile(t, path, string(before), 0o600)
	tool := newTool(t, root, output.Options{})
	prepare := func(replacement string) interface {
		Execute(context.Context) domain.ToolResult
	} {
		t.Helper()
		input := fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%s","replacements":[{"old":"old","new":%q}]}`, hashString(before), replacement)
		prepared, err := tool.Prepare(context.Background(), request(input, root))
		if err != nil {
			t.Fatal(err)
		}
		return prepared
	}
	preparedA := prepare("first")
	preparedB := prepare("second")
	start := make(chan struct{})
	results := make(chan domain.ToolResult, 2)
	var wait sync.WaitGroup
	for _, candidate := range []interface {
		Execute(context.Context) domain.ToolResult
	}{preparedA, preparedB} {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			results <- candidate.Execute(context.Background())
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	succeeded := 0
	failed := 0
	for result := range results {
		switch result.Status {
		case domain.ToolSucceeded:
			succeeded++
		case domain.ToolFailed:
			failed++
		default:
			t.Fatalf("result=%#v", result)
		}
	}
	if succeeded != 1 || failed != 1 {
		t.Fatalf("succeeded=%d failed=%d content=%q", succeeded, failed, readFile(t, path))
	}
	if got := readFile(t, path); got != "first\n" && got != "second\n" {
		t.Fatalf("content=%q", got)
	}
}

func TestEditReturnsFileChangeWhenDirectorySyncFailsAfterRename(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "a.txt")
	before := []byte("old\n")
	writeFile(t, path, string(before), 0o600)
	input := fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%s","replacements":[{"old":"old","new":"new"}]}`, hashString(before))
	preparedTool, err := newTool(t, root, output.Options{}).Prepare(context.Background(), request(input, root))
	if err != nil {
		t.Fatal(err)
	}
	prepared := preparedTool.(*prepared)
	prepared.syncDirectory = func(*os.File) error { return errors.New("injected directory sync failure") }

	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolFailed || result.FileChange == nil || result.FileChange.AfterSHA256 != hashString([]byte("new\n")) {
		t.Fatalf("result=%#v", result)
	}
	if !strings.Contains(result.Content, "durability is uncertain") || !strings.Contains(result.Content, "injected directory sync failure") {
		t.Fatalf("content=%q", result.Content)
	}
	if got := readFile(t, path); got != "new\n" {
		t.Fatalf("mutated content=%q", got)
	}
}

func TestEditConstructorRequiresArtifactStore(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("New did not reject a missing artifact store")
		}
	}()
	New(Options{Workspace: t.TempDir()})
}

func newTool(t *testing.T, workspace string, options output.Options) *Tool {
	t.Helper()
	if options.Artifacts == nil {
		options.SessionID = "session-1"
		options.Artifacts = &artifactStore{}
	}
	return New(Options{Workspace: workspace, Output: options})
}

func request(input, workspace string) domain.ToolRequest {
	return domain.ToolRequest{CallID: "call-1", Name: "edit", Input: json.RawMessage(input), Workspace: workspace}
}

func writeFile(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

func hashString(content []byte) string {
	sum := sha256.Sum256(content)
	return fmt.Sprintf("%x", sum)
}

func entryNames(entries []os.DirEntry) []string {
	names := make([]string, len(entries))
	for index, entry := range entries {
		names[index] = entry.Name()
	}
	return names
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

func (s *artifactStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.artifacts)
}
