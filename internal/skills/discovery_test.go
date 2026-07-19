package skills

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
	"golang.org/x/sys/unix"
)

var discoveryHookMu sync.Mutex

func canonicalTempDir(t *testing.T) string {
	t.Helper()
	path, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func skillOptions(t *testing.T, global string) DiscoveryOptions {
	t.Helper()
	workspace := canonicalTempDir(t)
	project := filepath.Join(workspace, ".yordam", "skills")
	return DiscoveryOptions{
		Workspace:  domain.Workspace{ID: "workspace", CanonicalPath: workspace},
		GlobalRoot: global, ProjectRoot: project, GenerationID: "generation",
	}
}

func writeSkill(t *testing.T, root, name, description, body string) string {
	t.Helper()
	directory := filepath.Join(root, name)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "SKILL.md")
	if err := os.WriteFile(path, validSkill(name, description, body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDiscoverMissingRootsAreEmpty(t *testing.T) {
	options := skillOptions(t, filepath.Join(canonicalTempDir(t), "missing-global"))
	result, err := Discover(context.Background(), options)
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(result.Candidates) != 0 || len(result.Diagnostics) != 0 {
		t.Fatalf("Discover() = %#v", result)
	}
}

func TestDiscoverFindsGlobalAndProjectSkillsWithBoundIdentity(t *testing.T) {
	global := canonicalTempDir(t)
	options := skillOptions(t, global)
	globalPath := writeSkill(t, global, "global-skill", "global description", "global body\n")
	projectPath := writeSkill(t, options.ProjectRoot, "project-skill", "project description", "project body\n")
	result, err := Discover(context.Background(), options)
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(result.Candidates) != 2 {
		t.Fatalf("candidate count = %d, diagnostics = %#v", len(result.Candidates), result.Diagnostics)
	}
	globalCandidate, projectCandidate := result.Candidates[0], result.Candidates[1]
	if globalCandidate.Source != protocol.SkillSourceGlobal || globalCandidate.CanonicalPath != globalPath || globalCandidate.WorkspaceID != "" {
		t.Fatalf("global candidate = %#v", globalCandidate)
	}
	if projectCandidate.Source != protocol.SkillSourceProject || projectCandidate.CanonicalPath != projectPath || projectCandidate.WorkspaceID != "workspace" {
		t.Fatalf("project candidate = %#v", projectCandidate)
	}
	for _, candidate := range result.Candidates {
		if candidate.ContentDigest != digest(candidate.Content) {
			t.Fatalf("content digest = %#v for %q", candidate.ContentDigest, candidate.Name)
		}
	}
}

func TestDiscoverRejectsLinksAndSpecialFilesWithoutLeakingContent(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(t *testing.T, options DiscoveryOptions, global string)
	}{
		{"global root", func(t *testing.T, options DiscoveryOptions, global string) {
			mustSymlink(t, canonicalTempDir(t), global)
		}},
		{"project root", func(t *testing.T, options DiscoveryOptions, global string) {
			if err := os.MkdirAll(filepath.Dir(options.ProjectRoot), 0o700); err != nil {
				t.Fatal(err)
			}
			mustSymlink(t, canonicalTempDir(t), options.ProjectRoot)
		}},
		{"skill directory", func(t *testing.T, options DiscoveryOptions, global string) {
			if err := os.MkdirAll(global, 0o700); err != nil {
				t.Fatal(err)
			}
			mustSymlink(t, canonicalTempDir(t), filepath.Join(global, "bad-skill"))
		}},
		{"skill file", func(t *testing.T, options DiscoveryOptions, global string) {
			if err := os.MkdirAll(filepath.Join(global, "bad-skill"), 0o700); err != nil {
				t.Fatal(err)
			}
			mustSymlink(t, filepath.Join(canonicalTempDir(t), "secret"), filepath.Join(global, "bad-skill", "SKILL.md"))
		}},
		{"fifo", func(t *testing.T, options DiscoveryOptions, global string) {
			path := filepath.Join(global, "bad-skill", "SKILL.md")
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := unix.Mkfifo(path, 0o600); err != nil {
				t.Skipf("Mkfifo: %v", err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			global := filepath.Join(canonicalTempDir(t), "global")
			options := skillOptions(t, global)
			test.setup(t, options, global)
			result, err := Discover(context.Background(), options)
			if err != nil {
				t.Fatalf("Discover() error = %v", err)
			}
			if len(result.Candidates) != 0 || len(result.Diagnostics) == 0 {
				t.Fatalf("Discover() = %#v", result)
			}
			for _, diagnostic := range result.Diagnostics {
				if strings.Contains(diagnostic.Message, "secret") {
					t.Fatalf("diagnostic leaked secret: %#v", diagnostic)
				}
			}
		})
	}
}

func TestDiscoverRejectsRootWithSymlinkedAncestor(t *testing.T) {
	base := canonicalTempDir(t)
	actual := filepath.Join(base, "actual")
	if err := os.Mkdir(actual, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "linked")
	mustSymlink(t, actual, link)
	options := skillOptions(t, filepath.Join(link, "skills"))
	if err := os.Mkdir(filepath.Join(actual, "skills"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeSkill(t, filepath.Join(actual, "skills"), "go-testing", "description", "body\n")
	result, err := Discover(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Candidates) != 0 || len(result.Diagnostics) != 1 {
		t.Fatalf("Discover() = %#v", result)
	}
}

func TestDiscoverKeepsDistinctSafeDiagnosticsWithoutSecretContent(t *testing.T) {
	global := canonicalTempDir(t)
	options := skillOptions(t, global)
	for _, name := range []string{"Bad", "AlsoBad"} {
		if err := os.Mkdir(filepath.Join(global, name), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(global, name, "SKILL.md"), []byte("body-secret-should-not-appear"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	result, err := Discover(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Candidates) != 0 || len(result.Diagnostics) != 2 || diagnosticKey(result.Diagnostics[0]) == diagnosticKey(result.Diagnostics[1]) {
		t.Fatalf("Discover() = %#v", result)
	}
	encoded, err := json.Marshal(result.Diagnostics)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "body-secret-should-not-appear") {
		t.Fatalf("diagnostics leaked rejected content: %s", encoded)
	}
}

func TestDiscoverWalksExactlyOneLevelAndRejectsInvalidAndCaseCollidingNames(t *testing.T) {
	global := canonicalTempDir(t)
	options := skillOptions(t, global)
	if err := os.MkdirAll(filepath.Join(global, "valid", "extra"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(global, "valid", "extra", "SKILL.md"), validSkill("valid", "description", "secret nested\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	writeSkill(t, global, "Good", "description", "body\n")
	writeSkill(t, global, "good", "description", "body\n")
	if err := os.MkdirAll(filepath.Join(global, "..bad"), 0o700); err != nil {
		t.Fatal(err)
	}
	result, err := Discover(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Candidates) != 0 || len(result.Diagnostics) < 2 {
		t.Fatalf("Discover() = %#v", result)
	}
	for _, diagnostic := range result.Diagnostics {
		if strings.Contains(diagnostic.Message, "secret nested") {
			t.Fatal("nested content leaked")
		}
	}
}

func TestDiscoverRejectsDuplicateIdentityAndReplacementRaces(t *testing.T) {
	global := canonicalTempDir(t)
	options := skillOptions(t, global)
	first := writeSkill(t, global, "first", "description", "first body\n")
	if err := os.MkdirAll(filepath.Join(global, "second"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(first, filepath.Join(global, "second", "SKILL.md")); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	duplicate, err := Discover(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if len(duplicate.Candidates) != 0 || len(duplicate.Diagnostics) == 0 {
		t.Fatalf("duplicate result = %#v", duplicate)
	}

	discoveryHookMu.Lock()
	defer discoveryHookMu.Unlock()
	global = canonicalTempDir(t)
	options = skillOptions(t, global)
	path := writeSkill(t, global, "go-testing", "description", "first body\n")
	discoveryHook = func(stage, _ string) {
		if stage == "after-file-open" {
			_ = os.Remove(path)
			_ = os.WriteFile(path, validSkill("go-testing", "description", "replacement body\n"), 0o600)
		}
	}
	defer func() { discoveryHook = nil }()
	result, err := Discover(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Candidates) != 0 || len(result.Diagnostics) == 0 {
		t.Fatalf("Discover() = %#v", result)
	}
}

func TestDiscoverRejectsDirectoryInspectOpenReplacement(t *testing.T) {
	discoveryHookMu.Lock()
	defer discoveryHookMu.Unlock()
	global := canonicalTempDir(t)
	options := skillOptions(t, global)
	directory := filepath.Join(global, "go-testing")
	writeSkill(t, global, "go-testing", "description", "first body\n")
	discoveryHook = func(stage, _ string) {
		if stage == "after-directory-inspect" {
			_ = os.Rename(directory, filepath.Join(global, "old"))
			_ = os.Mkdir(directory, 0o700)
			_ = os.WriteFile(filepath.Join(directory, "SKILL.md"), validSkill("go-testing", "description", "replacement body\n"), 0o600)
		}
	}
	defer func() { discoveryHook = nil }()
	result, err := Discover(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Candidates) != 0 || len(result.Diagnostics) == 0 {
		t.Fatalf("Discover() = %#v", result)
	}
}

func TestDiscoverRejectsRootReplacementAndCancellation(t *testing.T) {
	discoveryHookMu.Lock()
	defer discoveryHookMu.Unlock()
	globalParent := canonicalTempDir(t)
	global := filepath.Join(globalParent, "global")
	options := skillOptions(t, global)
	writeSkill(t, global, "go-testing", "description", "body\n")
	discoveryHook = func(stage, _ string) {
		if stage == "after-root-open" {
			_ = os.Rename(global, filepath.Join(globalParent, "old-global"))
			_ = os.Mkdir(global, 0o700)
		}
	}
	result, err := Discover(context.Background(), options)
	discoveryHook = nil
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Candidates) != 0 || len(result.Diagnostics) == 0 {
		t.Fatalf("Discover() = %#v", result)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Discover(ctx, options); err == nil {
		t.Fatal("Discover(cancelled) error = nil")
	}
}

func TestDiscoverOrderingDiagnosticsAndLargeInputAreDeterministic(t *testing.T) {
	global := canonicalTempDir(t)
	options := skillOptions(t, global)
	if err := os.Mkdir(filepath.Join(global, "Bad"), 0o700); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 129; index++ {
		writeSkill(t, global, "skill-"+pad(index), "description", "body\n")
	}
	first, err := Discover(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Discover(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Candidates) != 129 || len(second.Candidates) != 129 {
		t.Fatalf("candidate counts = %d, %d", len(first.Candidates), len(second.Candidates))
	}
	if !sameCandidates(first.Candidates, second.Candidates) {
		t.Fatal("candidate ordering is nondeterministic")
	}
	if !reflect.DeepEqual(first.Diagnostics, second.Diagnostics) {
		t.Fatalf("diagnostics are nondeterministic: %#v / %#v", first.Diagnostics, second.Diagnostics)
	}
	if !sort.SliceIsSorted(first.Candidates, func(i, j int) bool { return candidateKey(first.Candidates[i]) < candidateKey(first.Candidates[j]) }) {
		t.Fatal("candidates are not sorted")
	}
}

func mustSymlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
}
func pad(value int) string { return fmt.Sprintf("%03d", value) }
func digest(content []byte) protocol.Digest {
	sum := sha256.Sum256(content)
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: fmt.Sprintf("%x", sum)}
}
func candidateKey(candidate Candidate) string {
	return string(candidate.Source) + "\x00" + candidate.Name + "\x00" + candidate.ContentDigest.Value
}
func sameCandidates(left, right []Candidate) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].Name != right[i].Name || left[i].Source != right[i].Source || !bytes.Equal(left[i].Content, right[i].Content) {
			return false
		}
	}
	return true
}
