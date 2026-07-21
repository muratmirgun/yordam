package repolint_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/skills"
)

func TestCheckScriptKeepsV030AcceptanceOptIn(t *testing.T) {
	raw, err := os.ReadFile("../../scripts/check.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	const acceptance = `if [[ "${YORDAM_ACCEPTANCE:-0}" == "1" ]]; then
  go test -tags acceptance ./internal/acceptance -run '^TestV030SelfHostedRuntime$' -count=1 -v
fi`
	if strings.Count(text, acceptance) != 1 {
		t.Fatalf("check script must contain one exact opt-in v0.3 acceptance branch")
	}
	if strings.Index(text, acceptance) < strings.Index(text, "git diff --check") {
		t.Fatal("optional acceptance must extend, not replace, the fast default gate")
	}
}

func TestProductionCapabilityPackagesContainNoExecutableSkillOrHookLoader(t *testing.T) {
	root := repositoryRoot(t)
	for _, directory := range []string{
		filepath.Join("internal", "skills"),
		filepath.Join("internal", "subagent"),
		filepath.Join("internal", "app"),
		filepath.Join("internal", "tui"),
	} {
		err := filepath.WalkDir(filepath.Join(root, directory), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, forbidden := range []string{`"os/exec"`, `"plugin"`, "exec.Command(", "syscall.Exec(", "LoadHook(", "RunHook("} {
				if strings.Contains(string(raw), forbidden) {
					relative, _ := filepath.Rel(root, path)
					t.Errorf("production capability package contains executable skill/hook loader %q in %s", forbidden, filepath.ToSlash(relative))
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	skill := filepath.Join(root, ".yordam", "skills", "go-development", "SKILL.md")
	info, err := os.Stat(skill)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o111 != 0 {
		t.Fatalf("repository skill is executable: mode=%#o", info.Mode().Perm())
	}
}

func TestSelfHostingGoDevelopmentSkillIsStrictAndNonAuthoritative(t *testing.T) {
	path := "../../.yordam/skills/go-development/SKILL.md"
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	metadata, normalized, err := skills.Parse("go-development", raw)
	if err != nil {
		t.Fatalf("parse repository skill: %v", err)
	}
	if metadata.Description != "Make bounded Go changes with focused and complete verification." || string(normalized) != string(raw) {
		t.Fatalf("repository skill metadata=%+v or normalization changed bytes", metadata)
	}
	content := string(raw)
	for _, required := range []string{"Inspect", "exact bounded changes", "focused verification", "complete project verification", "Report every failure", "Never bypass or broaden permissions"} {
		if !strings.Contains(content, required) {
			t.Errorf("repository skill missing %q", required)
		}
	}
	for _, forbidden := range []string{"api_key", "apiKey", "Bearer ", "sudo ", "chmod 777", "curl ", "http://", "https://", "/Users/", "/home/", "hook:"} {
		if strings.Contains(content, forbidden) {
			t.Errorf("repository skill contains forbidden authority, secret, hook, or absolute-path text %q", forbidden)
		}
	}
}

func TestV030CompactionDocumentation(t *testing.T) {
	for path, required := range map[string][]string{
		"README.md": {
			"## Context compaction",
			"/compact",
			"automatic compaction",
			"context too large",
			"summary evidence",
		},
		"SECURITY.md": {
			"Context compaction summaries",
			"not original evidence",
			"provider credentials",
		},
		"docs/releases/v0.3.0-compaction-acceptance.md": {
			"# Yordam v0.3.0 Context Compaction Acceptance",
			"Evidence provenance",
			"Exact verification commands",
		},
	} {
		raw, err := os.ReadFile("../../" + path)
		if err != nil {
			t.Errorf("%s: %v", path, err)
			continue
		}
		for _, needle := range required {
			if !strings.Contains(string(raw), needle) {
				t.Errorf("%s missing %q", path, needle)
			}
		}
	}
}

func TestV030SkillsDocumentation(t *testing.T) {
	for path, required := range map[string][]string{
		"README.md":   {"## Filesystem skills", "~/.config/yordam/skills/<name>/SKILL.md", ".yordam/skills/<name>/SKILL.md", "/skills", "metadata only", "reload"},
		"SECURITY.md": {"Filesystem skills", "untrusted context", "permission", "symlink"},
		"docs/releases/v0.3.0-skills-acceptance.md": {"# Yordam v0.3.0 Filesystem Skills Acceptance", "Metadata versus loaded body", "Exact verification commands"},
	} {
		raw, err := os.ReadFile("../../" + path)
		if err != nil {
			t.Errorf("%s: %v", path, err)
			continue
		}
		for _, needle := range required {
			if !strings.Contains(string(raw), needle) {
				t.Errorf("%s missing %q", path, needle)
			}
		}
	}
}

func TestV030SubagentDocumentation(t *testing.T) {
	for path, required := range map[string][]string{
		"README.md": {
			"## Sequential subagents",
			"depth one",
			"same provider and model",
			"does not verify the parent task",
		},
		"SECURITY.md": {
			"Sequential subagents",
			"mutable permission grants",
			"trusted-shell acknowledgement",
			"uncertain",
		},
		"docs/releases/v0.3.0-subagent-acceptance.md": {
			"# Yordam v0.3.0 Sequential Subagent Acceptance",
			"Sequential execution contract",
			"Secret and authority boundaries",
			"Exact verification commands",
			"Fixture digests",
		},
	} {
		raw, err := os.ReadFile("../../" + path)
		if err != nil {
			t.Errorf("%s: %v", path, err)
			continue
		}
		for _, needle := range required {
			if !strings.Contains(string(raw), needle) {
				t.Errorf("%s missing %q", path, needle)
			}
		}
	}

	for path, forbidden := range map[string][]string{
		"README.md": {"parallel subagents", "remote subagents", "independent model selection"},
		"docs/releases/v0.3.0-subagent-acceptance.md": {"parallel subagents", "remote subagents", "independent model selection"},
	} {
		raw, err := os.ReadFile("../../" + path)
		if err != nil {
			continue
		}
		for _, needle := range forbidden {
			if strings.Contains(strings.ToLower(string(raw)), needle) {
				t.Errorf("%s makes unsupported claim %q", path, needle)
			}
		}
	}
}
