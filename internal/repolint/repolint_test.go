package repolint_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/skills"
)

func TestV030AcceptanceCIJobContract(t *testing.T) {
	workflow := readRepositoryFile(t, ".github/workflows/ci.yml")
	job := workflowJob(t, workflow, "v030-acceptance")

	for _, required := range []string{
		"timeout-minutes: 75",
		"os: [ubuntu-24.04, macos-15]",
		"runs-on: ${{ matrix.os }}",
		"uses: actions/checkout@v7",
		"fetch-depth: 0",
		"uses: actions/setup-go@v6",
		"go-version-file: go.mod",
		"go test -tags acceptance ./internal/acceptance -run TestV030SelfHostedRuntime -count=1 -v",
		"-timeout 60m",
	} {
		if !strings.Contains(job, required) {
			t.Errorf("v0.3 acceptance CI job missing %q", required)
		}
	}
	if strings.Count(job, "go test -tags acceptance ./internal/acceptance -run TestV030SelfHostedRuntime -count=1 -v") != 1 {
		t.Fatal("v0.3 acceptance CI job must run the cumulative umbrella exactly once")
	}
	for _, forbidden := range []string{"${{ secrets.", "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "YORDAM_API_KEY", "PROFILE_KEY"} {
		if strings.Contains(job, forbidden) {
			t.Errorf("v0.3 acceptance CI job contains provider secret wiring %q", forbidden)
		}
	}
}

func TestV030ReleaseCandidateWorkflowContract(t *testing.T) {
	workflow := readRepositoryFile(t, ".github/workflows/release.yml")
	packageJob := workflowJob(t, workflow, "package")
	for _, required := range []string{
		"needs: verify",
		"timeout-minutes: 120",
		"uses: actions/checkout@v7",
		"fetch-depth: 0",
		"go-version-file: go.mod",
		`test "$(git rev-parse HEAD)" = "$GITHUB_SHA"`,
		"goreleaser release --snapshot --clean --skip=publish",
		"goreleaser release --clean --skip=publish",
		"go test -tags acceptance ./internal/acceptance -run TestV030SelfHostedRuntime -count=1 -v",
		"-timeout 60m",
		"archives=(dist/*.tar.gz)",
		"sboms=(dist/*.spdx.json)",
		"[[ ${#archives[@]} -eq 4 ]]",
		"[[ ${#sboms[@]} -eq 4 ]]",
		"[[ -f dist/checksums.txt ]]",
		"uses: actions/upload-artifact@v4",
	} {
		if !strings.Contains(packageJob, required) {
			t.Errorf("v0.3 release package job missing %q", required)
		}
	}
	if strings.Count(packageJob, "go test -tags acceptance ./internal/acceptance -run TestV030SelfHostedRuntime -count=1 -v") != 1 {
		t.Fatal("release package job must run the cumulative v0.3 umbrella exactly once")
	}
	packageIndex := strings.Index(packageJob, "goreleaser release --snapshot --clean --skip=publish")
	acceptanceIndex := strings.Index(packageJob, "go test -tags acceptance ./internal/acceptance -run TestV030SelfHostedRuntime -count=1 -v")
	verifyIndex := strings.Index(packageJob, "name: Verify packaged files")
	uploadIndex := strings.Index(packageJob, "uses: actions/upload-artifact@v4")
	if packageIndex < 0 || acceptanceIndex < packageIndex || verifyIndex < acceptanceIndex || uploadIndex < verifyIndex {
		t.Fatal("release package job must package, run v0.3 acceptance, verify, then upload")
	}
	for _, forbidden := range []string{"${{ secrets.", "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "YORDAM_API_KEY", "PROFILE_KEY"} {
		if strings.Contains(packageJob, forbidden) {
			t.Errorf("release package job contains provider secret wiring %q", forbidden)
		}
	}

	smokeJob := workflowJob(t, workflow, "smoke")
	for _, required := range []string{
		"{runner: ubuntu-24.04, os: linux, arch: amd64}",
		"{runner: ubuntu-24.04-arm, os: linux, arch: arm64}",
		"{runner: macos-15-intel, os: darwin, arch: amd64}",
		"{runner: macos-15, os: darwin, arch: arm64}",
		`./scripts/smoke-release.sh "$archive" "$GITHUB_SHA"`,
	} {
		if !strings.Contains(smokeJob, required) {
			t.Errorf("v0.3 release smoke job missing %q", required)
		}
	}
	if strings.Count(smokeJob, "{runner:") != 4 {
		t.Fatal("release smoke job must contain exactly four native targets")
	}

	publishJob := workflowJob(t, workflow, "publish")
	for _, required := range []string{"needs: [verify, package, smoke]", "contents: write", "--verify-tag"} {
		if !strings.Contains(publishJob, required) {
			t.Errorf("release publish dependency or signing contract missing %q", required)
		}
	}
}

func TestReleaseSmokeRequiresPackagedVersionAndCommit(t *testing.T) {
	script := readRepositoryFile(t, "scripts/smoke-release.sh")
	for _, required := range []string{
		"expected_commit=${2:?expected commit required}",
		`^yordam_([^_]+)_(darwin|linux)_(amd64|arm64)\.tar\.gz$`,
		`^[0-9]+\.[0-9]+\.[0-9]+$`,
		`^0\.0\.0-SNAPSHOT-[0-9a-f]+$`,
		`expected_output="yordam $expected_version ($expected_commit, `,
		`[[ "$output" == "$expected_output" ]]`,
	} {
		if !strings.Contains(script, required) {
			t.Errorf("release smoke script missing %q", required)
		}
	}
}

func readRepositoryFile(t *testing.T, relative string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repositoryRoot(t), filepath.FromSlash(relative)))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func workflowJob(t *testing.T, workflow, name string) string {
	t.Helper()
	lines := strings.Split(workflow, "\n")
	start := -1
	prefix := "  " + name + ":"
	for index, line := range lines {
		if line == prefix {
			if start >= 0 {
				t.Fatalf("workflow contains duplicate %q job", name)
			}
			start = index
		}
	}
	if start < 0 {
		t.Fatalf("workflow missing %q job", name)
	}
	end := len(lines)
	for index := start + 1; index < len(lines); index++ {
		line := lines[index]
		if strings.HasPrefix(line, "  ") && !strings.HasPrefix(line, "    ") && strings.HasSuffix(line, ":") {
			end = index
			break
		}
	}
	return strings.Join(lines[start:end], "\n")
}

func TestCheckScriptKeepsV030AcceptanceOptIn(t *testing.T) {
	raw, err := os.ReadFile("../../scripts/check.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	const acceptance = `if [[ "${YORDAM_ACCEPTANCE:-0}" == "1" ]]; then
  go test -tags acceptance ./internal/acceptance -run '^TestV030SelfHostedRuntime$' -count=1 -v -timeout 60m
fi`
	if strings.Count(text, acceptance) != 1 {
		t.Fatalf("check script must contain one exact opt-in v0.3 acceptance branch")
	}
	if strings.Index(text, acceptance) < strings.Index(text, "git diff --check") {
		t.Fatal("optional acceptance must extend, not replace, the fast default gate")
	}
}

func TestV030ProductDocumentationMatchesShippedSurface(t *testing.T) {
	for path, required := range map[string][]string{
		"README.md": {
			`"contextWindow": 128000`,
			`"autoCompact": true`,
			`"compactReserveTokens": 8192`,
			`"projectPolicy": "ask"`,
			"`/compact` manually summarizes",
			"`/skills` records an allow/deny decision",
			"same provider and model",
			"Only one child can be active",
			"does not automatically retry an uncertain effect",
			"## Explicit v0.3 non-goals",
			"no parallel children, executable skills or hooks, permission-bypass mode, or journal rewriting",
			"macOS and Linux",
		},
		"SECURITY.md": {
			"`safe`, `ask`, and `auto` are permission modes, not sandboxes",
			"trusted-shell acknowledgement",
			"Project trust does not grant file, shell, provider, child, or permission authority",
			"does not automatically retry an uncertain effect",
			"Yordam has no permission-bypass mode",
			"Skills are instruction-only; Yordam does not execute skills or load skill hooks",
			"Compaction appends derived evidence and never deletes or rewrites the source journal",
		},
		"CHANGELOG.md": {
			"## [0.3.0] - Unreleased",
			"Context compaction",
			"Filesystem skills",
			"Sequential depth-one subagents",
			"macOS and Linux",
			"Uncertain effects are never retried automatically",
		},
		"docs/superpowers/specs/2026-07-19-yordam-v0.3-self-hosted-agent-runtime-design.md": {
			"Status: implemented and release-gated",
			"Implementation completed: 2026-07-21",
			"TestV030SelfHostedRuntime",
			"does not assert that v0.3.0 has been published",
		},
	} {
		text := readRepositoryFile(t, path)
		normalized := strings.Join(strings.Fields(text), " ")
		for _, needle := range required {
			if !strings.Contains(normalized, strings.Join(strings.Fields(needle), " ")) {
				t.Errorf("%s missing shipped v0.3 contract %q", path, needle)
			}
		}
	}

	for _, path := range []string{"README.md", "SECURITY.md", "CHANGELOG.md"} {
		text := strings.ToLower(strings.Join(strings.Fields(readRepositoryFile(t, path)), " "))
		for _, unsupported := range []string{
			"coming soon",
			"planned for v0.3",
			"will support parallel",
			"supports parallel children",
			"executes skill files",
			"installs skill hooks",
			"full access mode",
			"automatically retries uncertain",
			"yordam rewrites the source journal",
		} {
			if strings.Contains(text, unsupported) {
				t.Errorf("%s contains future or unsupported v0.3 claim %q", path, unsupported)
			}
		}
	}

	design := readRepositoryFile(t, "docs/superpowers/specs/2026-07-19-yordam-v0.3-self-hosted-agent-runtime-design.md")
	for _, stale := range []string{"Status: approved for implementation planning", "Production implementation begins from"} {
		if strings.Contains(design, stale) {
			t.Errorf("implemented design retains stale planning claim %q", stale)
		}
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
