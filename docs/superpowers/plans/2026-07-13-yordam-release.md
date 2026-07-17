# Yordam OSS Release and Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the completed Yordam binary into a documented Apache-2.0 project with enforced CI, reproducible macOS/Linux artifacts, checksums, SBOMs, and recorded v0.1.0 acceptance evidence.

**Architecture:** GitHub Actions runs formatting, vet, unit/integration/PTY tests, race checks, and four-target builds. A signed semantic-version tag triggers GoReleaser v2; Syft emits SPDX JSON SBOMs and smoke jobs execute the native artifact before release publication.

**Tech Stack:** Go 1.26.4, GitHub Actions `actions/checkout@v7`, `actions/setup-go@v6`, GoReleaser v2.17.0, Syft v1.46.0, shell smoke scripts.

## Global Constraints

- Complete Phases 1-4 and Gates 1-4 first.
- License is Apache-2.0.
- Release targets are exactly `darwin/linux` × `amd64/arm64`; no Windows artifacts.
- Every release includes archives, `checksums.txt`, and SPDX JSON SBOMs.
- The tag is an annotated, maintainer-signed SemVer tag.
- Release jobs do not receive model API keys.
- CI must prove secret hygiene using synthetic keys.
- No release occurs with a dirty worktree, failing tests, vet findings, race failures, or missing acceptance evidence.

---

### Task 1: Add the OSS project documents

**Files:**
- Create: `LICENSE`
- Create: `README.md`
- Create: `CONTRIBUTING.md`
- Create: `SECURITY.md`
- Create: `CHANGELOG.md`
- Test: `internal/repolint/docs_test.go`

**Interfaces:**
- Consumes: approved design and actual CLI output.
- Produces: contributor-facing project identity, build/test commands, security channel, and changelog policy.

- [ ] **Step 1: Write repository-document tests**

```go
package repolint_test

import (
    "os"
    "strings"
    "testing"
)

func TestRequiredRepositoryDocuments(t *testing.T) {
    required:=map[string][]string{
        "LICENSE":{"Apache License","Version 2.0, January 2004"},
        "README.md":{"# Yordam","a small, hackable agent for your terminal","Shell execution is not sandboxed"},
        "CONTRIBUTING.md":{"go test ./...","go test -race ./...","go vet ./..."},
        "SECURITY.md":{"Private vulnerability reporting","Do not open a public issue"},
        "CHANGELOG.md":{"## [Unreleased]","Semantic Versioning"},
    }
    for path,needles:=range required{raw,err:=os.ReadFile("../../../"+path);if err!=nil{t.Fatalf("%s: %v",path,err)};for _,needle:=range needles{if !strings.Contains(string(raw),needle){t.Errorf("%s missing %q",path,needle)}}}
}
```

- [ ] **Step 2: Run repository-document test**

Run: `go test ./internal/repolint -run TestRequiredRepositoryDocuments -v`

Expected: FAIL because the root documents are missing.

- [ ] **Step 3: Add canonical license and exact document sections**

Create `LICENSE` from the canonical Apache Software Foundation text:

```bash
curl -fsSL https://www.apache.org/licenses/LICENSE-2.0.txt -o /tmp/yordam-apache-license
test "$(shasum -a 256 /tmp/yordam-apache-license | awk '{print $1}')" = "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30"
```

Apply the verified `/tmp/yordam-apache-license` content to `LICENSE` with the file-editing tool; do not leave the temporary file in the repository.

`README.md` must contain these sections in order: tagline, status warning (`v0.x`), features, security model, install from GitHub release, first-run profile TOML, `YORDAM_API_KEY` example, usage, commands/keys, session locations, build/test, roadmap, contributing, license. The security section must state that file tools enforce canonical permission scopes while shell is trusted and unsandboxed.

`CONTRIBUTING.md` must require one focused change per commit, TDD, `gofmt`, `go test ./...`, `go test -race ./...`, `go vet ./...`, and an updated changelog for user-visible behavior.

`SECURITY.md` must direct reporters to GitHub's private vulnerability reporting feature, say not to open a public issue, document supported version `0.1.x`, and promise acknowledgement without inventing a response-time SLA.

`CHANGELOG.md` starts with `# Changelog`, states Semantic Versioning, and contains `## [Unreleased]` with `Added`, `Changed`, `Fixed`, and `Security` subsections.

- [ ] **Step 4: Run doc lint and link-free local checks**

Run: `go test ./internal/repolint -v && git diff --check`

Expected: PASS.

- [ ] **Step 5: Commit OSS documents**

```bash
git add LICENSE README.md CONTRIBUTING.md SECURITY.md CHANGELOG.md internal/repolint
git commit -m "docs: add Yordam OSS project guides"
```

### Task 2: Enforce CI tests, race checks, and cross-builds

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `scripts/check.sh`
- Test: `internal/repolint/ci_test.go`

**Interfaces:**
- Consumes: all Go packages and supported target matrix.
- Produces: local check script and pull-request CI gates.

- [ ] **Step 1: Write a workflow structure test**

```go
func TestCIWorkflowContainsRequiredGates(t *testing.T) {
    raw,err:=os.ReadFile("../../../.github/workflows/ci.yml");if err!=nil{t.Fatal(err)}
    text:=string(raw)
    for _,needle:=range []string{"ubuntu-latest","macos-latest","go test ./...","go test -race ./...","go vet ./...","GOOS: darwin","GOOS: linux","GOARCH: amd64","GOARCH: arm64"}{if !strings.Contains(text,needle){t.Errorf("workflow missing %q",needle)}}
}
```

- [ ] **Step 2: Run CI structure test**

Run: `go test ./internal/repolint -run TestCIWorkflowContainsRequiredGates -v`

Expected: FAIL because CI workflow is missing.

- [ ] **Step 3: Add exact local and GitHub CI commands**

```bash
#!/usr/bin/env bash
# scripts/check.sh
set -euo pipefail
test -z "$(gofmt -l .)"
go test ./...
go test -race ./...
go vet ./...
git diff --check
```

`.github/workflows/ci.yml` has three jobs:

1. `test` matrix on `ubuntu-latest` and `macos-latest`: checkout v7, setup-go v6 with `go-version-file: go.mod` and cache, then `./scripts/check.sh`.
2. `cross-build` matrix of four explicit `GOOS`/`GOARCH` pairs: `CGO_ENABLED=0 go build -trimpath -o dist/yordam-${GOOS}-${GOARCH} ./cmd/yordam`.
3. `secret-hygiene`: set `YORDAM_API_KEY=synthetic-ci-secret` and `PROFILE_KEY=synthetic-profile-secret`, run the secret/integration tests, then fail if `rg -uuu 'synthetic-(ci|profile)-secret'` finds a match under the test data directory.

The workflow triggers on pull requests and pushes to `main`, uses read-only contents permission, and uploads no session data.

- [ ] **Step 4: Run local CI and workflow tests**

Run: `chmod +x scripts/check.sh && ./scripts/check.sh && go test ./internal/repolint -v`

Expected: PASS; four cross-build commands can be reproduced locally.

- [ ] **Step 5: Commit CI gates**

```bash
git add .github/workflows/ci.yml scripts/check.sh internal/repolint/ci_test.go
git commit -m "ci: enforce tests race vet and cross-builds"
```

### Task 3: Configure GoReleaser, checksums, and SBOMs

**Files:**
- Create: `.goreleaser.yml`
- Create: `.github/workflows/release.yml`
- Create: `scripts/smoke-release.sh`
- Test: `internal/repolint/release_test.go`

**Interfaces:**
- Consumes: signed `v*` tag and completed CI.
- Produces: four target archives, `checksums.txt`, SPDX JSON SBOM per archive, and native smoke verification on all four OS/architecture pairs.

- [ ] **Step 1: Write release config tests**

```go
func TestReleaseConfigTargetsOnlyApprovedPlatforms(t *testing.T) {
    raw,err:=os.ReadFile("../../../.goreleaser.yml");if err!=nil{t.Fatal(err)}
    text:=string(raw)
    for _,needle:=range []string{"version: 2","project_name: yordam","darwin","linux","amd64","arm64","checksums.txt","spdx-json"}{if !strings.Contains(text,needle){t.Errorf("missing %q",needle)}}
    if strings.Contains(text,"windows"){t.Fatal("Windows target present")}
    workflow,err:=os.ReadFile("../../../.github/workflows/release.yml");if err!=nil{t.Fatal(err)}
    for _,runner:=range []string{"ubuntu-24.04","ubuntu-24.04-arm","macos-15-intel","macos-15"}{if !strings.Contains(string(workflow),runner){t.Errorf("release workflow missing native runner %q",runner)}}
}
```

- [ ] **Step 2: Run release config test**

Run: `go test ./internal/repolint -run TestReleaseConfigTargetsOnlyApprovedPlatforms -v`

Expected: FAIL because release config is missing.

- [ ] **Step 3: Add GoReleaser v2 config and native smoke script**

```yaml
# .goreleaser.yml
version: 2
project_name: yordam
builds:
  - id: yordam
    main: ./cmd/yordam
    binary: yordam
    env: [CGO_ENABLED=0]
    goos: [darwin, linux]
    goarch: [amd64, arm64]
    flags: [-trimpath]
    ldflags:
      - -s -w -X github.com/muratmirgun/yordam/internal/buildinfo.Version={{.Version}} -X github.com/muratmirgun/yordam/internal/buildinfo.Commit={{.Commit}} -X github.com/muratmirgun/yordam/internal/buildinfo.Date={{.Date}}
archives:
  - id: default
    formats: [tar.gz]
    name_template: "{{ .ProjectName }}_{{ .Version }}_{{ .Os }}_{{ .Arch }}"
checksum:
  name_template: checksums.txt
sboms:
  - id: archives
    artifacts: archive
    documents: ["{{ .ArtifactName }}.spdx.json"]
    cmd: syft
    args: ["$artifact", "--output", "spdx-json=$document"]
changelog:
  use: git
release:
  draft: false
```

```bash
#!/usr/bin/env bash
# scripts/smoke-release.sh
set -euo pipefail
archive=${1:?archive path required}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
tar -xzf "$archive" -C "$work"
output=$($work/yordam --version)
case "$output" in
  "yordam "*) ;;
  *) echo "unexpected version output: $output" >&2; exit 1 ;;
esac
```

The release workflow triggers on signed tags `v*` and has four ordered jobs:

1. `verify` runs the complete Gate 5 test/race/vet suite and queries GitHub's annotated-tag object, failing unless `verification.verified` is true and the tag name matches `v[0-9]+.[0-9]+.[0-9]+`.
2. `package` on `ubuntu-24.04` installs pinned GoReleaser/Syft, runs `goreleaser release --clean --skip=publish`, verifies four archives/checksums/SBOMs, and uploads `dist` as a workflow artifact.
3. `smoke` downloads that exact artifact with a matrix: `{runner:ubuntu-24.04, os:linux, arch:amd64}`, `{runner:ubuntu-24.04-arm, os:linux, arch:arm64}`, `{runner:macos-15-intel, os:darwin, arch:amd64}`, and `{runner:macos-15, os:darwin, arch:arm64}`. Each row verifies `go env GOOS/GOARCH`, checksum and matching SBOM, then runs `scripts/smoke-release.sh` on its native archive.
4. `publish` needs every smoke row, downloads the unchanged artifact, grants `contents: write`, and creates the GitHub release with the four archives, four SBOMs, and checksum file. No release is published before all native smoke jobs pass.

All earlier jobs use read-only contents permissions; the workflow uploads no session data or credentials.

- [ ] **Step 4: Test snapshot packaging and SBOM presence**

Run:

```bash
go install github.com/goreleaser/goreleaser/v2@v2.17.0
go install github.com/anchore/syft/cmd/syft@v1.46.0
goreleaser release --snapshot --clean
test "$(find dist -name '*.tar.gz' | wc -l | tr -d ' ')" = 4
test -f dist/checksums.txt
test "$(find dist -name '*.spdx.json' | wc -l | tr -d ' ')" = 4
./scripts/smoke-release.sh "$(find dist -name "*$(go env GOOS)*$(go env GOARCH)*.tar.gz" -print -quit)"
```

Expected: four archives, four SPDX JSON documents, one checksum file, local native smoke PASS; CI provides native evidence for the other three rows.

- [ ] **Step 5: Commit release automation**

```bash
git add .goreleaser.yml .github/workflows/release.yml scripts/smoke-release.sh internal/repolint/release_test.go
git commit -m "ci: package signed Yordam releases"
```

### Task 4: Execute and record the fourteen acceptance criteria

**Files:**
- Create: `docs/releases/v0.1.0-acceptance.md`
- Create: `internal/acceptance/v010_test.go`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: complete binary, scripted provider fixtures, temp workspaces, release snapshot.
- Produces: machine-checked acceptance suite and human-readable command/result record.

- [ ] **Step 1: Write acceptance test names matching the design**

```go
package acceptance_test

import "testing"

func TestV010Acceptance(t *testing.T) {
    tests:=[]struct{name string;run func(*testing.T)}{
        {"single_binary_startup",acceptSingleBinaryStartup},
        {"secret_free_first_run_setup",acceptSecretFreeSetup},
        {"scripted_four_tool_turn",acceptFourToolTurn},
        {"ask_allow_and_deny",acceptAskPaths},
        {"safe_blocks_mutations",acceptSafeMode},
        {"auto_scope_and_shell_ack",acceptAutoMode},
        {"exact_diff_and_stale_preimage",acceptEditSafety},
        {"cancel_kills_process_group",acceptShellCancellation},
        {"continue_reconstructs_session",acceptResume},
        {"truncated_tail_recovery",acceptRecovery},
        {"model_change_next_turn",acceptModelChange},
        {"git_and_non_git",acceptWorkspaceKinds},
        {"four_release_targets",acceptReleaseTargets},
        {"secret_hygiene",acceptSecretHygiene},
    }
    for _,tc:=range tests{t.Run(tc.name,tc.run)}
}
```

Implement the helpers with this fixed evidence map:

| Helper | Fixture/action | Required assertion |
|---|---|---|
| `acceptSingleBinaryStartup` | build `./cmd/yordam` into `t.TempDir`, run `--version`, then open it through PTY | one executable starts, prints version, reaches setup/conversation, and exits with terminal restoration |
| `acceptSecretFreeSetup` | PTY first-run setup with a sentinel API key in environment and temp config/data roots | config persists base URL/profile/model/key variable name; sentinel value is absent from every persisted file |
| `acceptFourToolTurn` | reuse the deterministic SSE and temp-workspace fixture from `internal/integration` | read/search/edit/shell calls complete in order and final assistant text is durable |
| `acceptAskPaths` | ask-mode scripted edit allow-once followed by shell deny | edit occurs once, shell side effect is absent, both decisions are durable |
| `acceptSafeMode` | safe-mode edit and shell requests | both are denied before execute; read inside workspace succeeds |
| `acceptAutoMode` | auto-mode inside edit, outside read, and shell before/after acknowledgement | inside edit runs, outside access asks, shell asks before acknowledgement and runs after it |
| `acceptEditSafety` | real edit preview, then mutate preimage before execute | unified diff is exact and stale preimage prevents the write |
| `acceptShellCancellation` | start `sleep 30 & wait`, cancel after process start | process group exits within three seconds and result is cancelled |
| `acceptResume` | complete a turn, close runtime, bootstrap with `--continue` | same canonical workspace/session/model/mode and transcript are reconstructed |
| `acceptRecovery` | append an incomplete final JSON fragment and an unmatched `tool.started` fixture | tail is preserved as recovery artifact and replay appends one interrupted terminal event without executing the tool |
| `acceptModelChange` | queue profile/model change during a blocked turn, release it, start next turn | first request uses old selection; second uses new profile and model |
| `acceptWorkspaceKinds` | run identical read turns in a temp Git repository and a plain temp directory | both complete without Git being required by core runtime |
| `acceptReleaseTargets` | inspect GoReleaser snapshot directory and checksums | exactly darwin/linux × amd64/arm64 archives and four matching SPDX SBOMs exist |
| `acceptSecretHygiene` | use a unique sentinel in provider auth, streamed error, shell environment, session, artifact, and PTY fixtures | recursive scan of temp config/data/log/output roots finds zero sentinel occurrences and shell `env` output omits it |

No helper may skip based on developer machine state. `acceptReleaseTargets` reads snapshot artifacts rather than executing foreign-architecture binaries; the native archive is executed by `scripts/smoke-release.sh`.

- [ ] **Step 2: Run acceptance suite and observe missing helper failures**

Run: `go test ./internal/acceptance -run TestV010Acceptance -v`

Expected: FAIL until all fourteen helpers and fixtures are implemented.

- [ ] **Step 3: Implement helpers and write the evidence document**

`docs/releases/v0.1.0-acceptance.md` has one numbered section per design criterion with:

```markdown
### 1. Single binary startup

Command: `go test ./internal/acceptance -run 'TestV010Acceptance/single_binary_startup' -v`

Expected: the release binary starts in a fresh directory and reaches the setup or conversation screen.

Result: PASS
```

Repeat the same complete four-line structure for all fourteen named subtests. The document begins with commit SHA, Go version, date, macOS runner, Linux runner, and snapshot checksum values captured by the release candidate workflow.

Move changelog `Unreleased` additions into `## [0.1.0] - 2026-07-13`, then recreate an empty `Unreleased` section above it.

- [ ] **Step 4: Run the full release candidate gate**

```bash
./scripts/check.sh
go test ./internal/acceptance -run TestV010Acceptance -v
goreleaser release --snapshot --clean
go test ./internal/repolint -v
git diff --check
```

Expected: PASS; acceptance document has fourteen `Result: PASS` entries.

- [ ] **Step 5: Commit acceptance evidence**

```bash
git add docs/releases/v0.1.0-acceptance.md internal/acceptance CHANGELOG.md
git commit -m "test: record Yordam v0.1.0 acceptance"
```

### Task 5: Complete Gate 5 and prepare the signed v0.1.0 tag

**Files:**
- Modify: `docs/superpowers/plans/2026-07-13-yordam-v0.1-roadmap.md`

**Interfaces:**
- Consumes: clean release candidate and all recorded evidence.
- Produces: Gate 5 completion and locally verified signed tag ready to push.

- [ ] **Step 1: Verify clean, reproducible release state**

```bash
test -z "$(git status --short)"
./scripts/check.sh
goreleaser release --snapshot --clean
sha256sum dist/*.tar.gz > /tmp/yordam-snapshot-checksums.txt 2>/dev/null || shasum -a 256 dist/*.tar.gz > /tmp/yordam-snapshot-checksums.txt
test "$(wc -l < /tmp/yordam-snapshot-checksums.txt | tr -d ' ')" = 4
```

Expected: all commands exit 0 and checksum file has four lines.

- [ ] **Step 2: Mark only Gate 5 complete and commit**

Change Gate 5 to `[x]`, then run:

```bash
git add docs/superpowers/plans/2026-07-13-yordam-v0.1-roadmap.md
git commit -m "chore: complete Yordam v0.1 release gate"
```

- [ ] **Step 3: Create and locally verify the signed tag**

```bash
git tag -s v0.1.0 -m "Yordam v0.1.0"
git verify-tag v0.1.0
```

Expected: `git verify-tag` reports a good signature for the maintainer identity.

- [ ] **Step 4: Inspect tag and commit before any push**

```bash
git show --stat --decorate v0.1.0
git status --short --branch
```

Expected: tag points to the Gate 5 commit and worktree is clean. Pushing the tag is an external publication action and requires the user's explicit instruction in the execution session.

- [ ] **Step 5: Preserve handoff information without publishing**

Record this exact handoff in the execution summary:

```text
Release candidate: v0.1.0
Local tag verification: PASS
Remote publication: NOT PERFORMED
Next authorized command: git push origin main v0.1.0
```
