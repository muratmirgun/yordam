# Test Fixture Materialization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove every repository `testdata` directory while preserving exact Foundation compatibility, recovery, TUI golden, and PTY fixture coverage.

**Architecture:** `internal/testsupport/foundationfixture` owns literal, serializer-independent Foundation bytes and materializes selected scenarios beneath caller-owned temporary roots. JSONL and acceptance tests share it; TUI golden text and scripted SSE bytes move into test helpers. Repository lint and dependency gates prevent `testdata` paths or test-support code from entering production again.

**Tech Stack:** Go 1.25, `testing`, `crypto/sha256`, filesystem APIs, acceptance build tags, Git/GPG-signed commits.

## Global Constraints

- The final tree contains no path component named `testdata`.
- Preserve all 14 Foundation scenario names and exact source bytes.
- Never generate compatibility literals with production journal encoders.
- Materialization writes only below an absolute caller-owned root with file mode `0600` and directory mode `0700`.
- Do not introduce `go:embed`, binary archives, Git LFS, downloads, or network-dependent tests.
- Preserve migration, corruption, recovery, TUI, PTY, Foundation, and all 14 v0.1 acceptance scenarios.
- `cmd/yordam` must not depend on `internal/testsupport/foundationfixture`.
- Sign every commit with `git commit -S`.
- Do not request another code review; the user requested one consolidated v0.2 review only.

---

### Task 1: Add the Foundation fixture materializer

**Files:**
- Create: `internal/testsupport/foundationfixture/fixture.go`
- Create: `internal/testsupport/foundationfixture/definitions.go`
- Create: `internal/testsupport/foundationfixture/fixture_test.go`
- Read-only conversion source: `internal/acceptance/testdata/foundation/**`

**Interfaces:**
- Produces: `Names() []string`
- Produces: `Digest() string`
- Produces: `Read(name, relative string) ([]byte, bool)`
- Produces: `Materialize(root string, names ...string) error`

- [ ] **Step 1: Write failing inventory, digest, and safety tests**

Create `fixture_test.go`. Assert the sorted inventory is exactly:

```go
var expectedNames = []string{
    "evidence-digest-mismatch", "incomplete-batch", "invalid-known-payload",
    "invalid-sequence", "lineage-cycle", "missing-evidence-blob", "mixed-v1-v2",
    "unknown-future-kind", "unsupported-envelope-version", "unsupported-payload-version",
    "v1-stale-edit-recovery", "v1-truncated-final", "v1-unmatched-tool-start", "v1-valid",
}
```

Assert `Digest()` equals `c20a9601d4a87b7105c2938d57abd9591bef932bd157d563f3cc5d5dd39ccaeb`. Assert `Materialize(t.TempDir(), "v1-valid")` writes byte-identical `events.jsonl`, omits unselected scenarios, rejects unknown/duplicate names, and rejects definitions containing `../escape`, `/absolute`, `safe/../../escape`, an empty path, or duplicate paths.

- [ ] **Step 2: Run the focused test and verify RED**

Run: `go test ./internal/testsupport/foundationfixture -count=1`

Expected: FAIL because the package API and definition types do not exist.

- [ ] **Step 3: Implement the minimal materializer**

Use these internal types and contracts:

```go
type fileDefinition struct { path, data string }
type definition struct { name string; files []fileDefinition }

func Names() []string
func Digest() string
func Read(name, relative string) ([]byte, bool)
func Materialize(root string, names ...string) error
func definitionByName(name string) definition
func validateDefinitions(values []definition) error
```

`Names` returns a sorted copy. `Digest` iterates sorted `name/path` values and hashes `name/path`, a NUL byte, the SHA-256 of exact data, and a newline. `Read` returns a defensive copy. `Materialize` requires an absolute root, validates every definition before writing, rejects unknown/duplicate names, and writes only validated relative paths below the root.

- [ ] **Step 4: Convert exact fixture bytes into definitions**

Mechanically convert every non-manifest file under `internal/acceptance/testdata/foundation` into `definitions.go`. Preserve exact bytes and terminating newlines. Use raw string literals when possible and quoted literals when a file contains a backtick. Before deleting any source, byte-compare every `Read(name, path)` result against its source and verify the stable bundle digest.

- [ ] **Step 5: Verify and commit**

Run:

```bash
gofmt -w internal/testsupport/foundationfixture/*.go
go test ./internal/testsupport/foundationfixture -count=1
git add -- internal/testsupport/foundationfixture
git commit -S -m "test: add foundation fixture materializer"
```

Expected: PASS; the signed commit contains only the new helper package.

---

### Task 2: Migrate JSONL migration and recovery tests

**Files:**
- Modify: `internal/session/jsonl/migration_test.go`
- Modify: `internal/session/jsonl/explicit_recovery_test.go`
- Delete: `internal/session/jsonl/testdata/**`

**Interfaces:**
- Consumes: all four `foundationfixture` exports from Task 1
- Produces: JSONL compatibility tests with no repository-relative fixture reads

- [ ] **Step 1: Write the failing temporary-materialization assertion**

Change `TestFoundationFixtureInventory` to materialize the existing 11-name JSONL subset into `t.TempDir()`, list the resulting directories, and compare them with the sorted subset. Assert the shared digest constant before inspecting files.

Run: `go test ./internal/session/jsonl -run '^TestFoundationFixtureInventory$' -count=1`

Expected: FAIL until the new package is imported and the old manifest parser is removed.

- [ ] **Step 2: Migrate fixture copying and immutability checks**

Add `sourceRoot string` to `fixtureMaterialization`. In `copyFixture`, materialize only the named scenario beneath a new temporary source root, then perform the existing workspace placeholder replacement from that temporary source. Return `sourceRoot`; change explicit recovery to snapshot `fixture.sourceRoot` before and after recovery. Remove obsolete manifest parsing imports while retaining `snapshotTree` for immutability assertions.

- [ ] **Step 3: Verify before and after deleting the JSONL tree**

Run:

```bash
gofmt -w internal/session/jsonl/migration_test.go internal/session/jsonl/explicit_recovery_test.go
go test ./internal/session/jsonl -run 'Test(FoundationFixture|InspectSessionNeverMutatesFixture|LoadNeverMutatesFixture|V1Upcast|MixedV1V2|ExplicitRecovery)' -count=1
```

Delete exactly `internal/session/jsonl/testdata`, then run `go test ./internal/session/jsonl -count=1`.

Expected: both runs PASS and the deleted directory is never read.

- [ ] **Step 4: Commit**

```bash
git add -- internal/session/jsonl internal/testsupport/foundationfixture
git commit -S -m "test: materialize jsonl compatibility fixtures"
```

---

### Task 3: Migrate Foundation acceptance fixtures

**Files:**
- Modify: `internal/acceptance/foundation_protocol_test.go`
- Delete: `internal/acceptance/testdata/**`

**Interfaces:**
- Consumes: complete 14-scenario shared inventory and literal bytes
- Produces: acceptance-tagged migration/evidence checks without checked-in fixture paths

- [ ] **Step 1: Replace local inventory and manifest behavior**

Change `validateFoundationFixtures` to assert `len(foundationfixture.Names()) == 14`, verify the stable digest, materialize all names into `t.TempDir()`, snapshot that generated tree before/after validation, and log the count/digest. Change evidence and lineage expectation reads to `foundationfixture.Read(name, "want.json")`; fail when `ok` is false.

- [ ] **Step 2: Run focused acceptance before deletion**

Run:

```bash
gofmt -w internal/acceptance/foundation_protocol_test.go
go test -tags=acceptance ./internal/acceptance -run '^TestV020Foundation/(migration|evidence)' -count=1 -v
```

Expected: PASS with `fixtures=14` and the stable digest in logs.

- [ ] **Step 3: Delete and verify**

Delete exactly `internal/acceptance/testdata`, then rerun the same command.

Expected: PASS without any repository fixture directory.

- [ ] **Step 4: Commit**

```bash
git add -- internal/acceptance internal/testsupport/foundationfixture
git commit -S -m "test: materialize foundation acceptance fixtures"
```

---

### Task 4: Inline TUI goldens and PTY SSE bytes

**Files:**
- Create: `internal/tui/golden_views_test.go`
- Modify: `internal/tui/model_test.go`
- Modify: `internal/testsupport/ptyfixture/pty_unix.go`
- Modify: `internal/testsupport/ptyfixture/pty_unix_test.go`
- Modify: `internal/ptytest/yordam_test.go`
- Delete: `internal/tui/testdata/**`

**Interfaces:**
- Produces: test-local `goldenView(width int) (string, bool)`
- Produces: `ptyfixture.ScriptedSSE() string`

- [ ] **Step 1: Write failing exact-byte tests**

Assert widths 80, 120, and 160 exist and hash respectively to:

```text
d17a5cdf3802eaa46ebb55d89b8f84c718f3c6d3e733e723bc7e491a5d52ce7b
9cc5a002c6d54c846bc0276dc0a015fd90f87c860c689ff0e03f30c527d0ad35
c2ae70b230c41db36e6a3a0b601c5add6c75a5cb70b05c2ca71fbb6543f53a03
```

Assert width 100 is absent. Assert `ptyfixture.ScriptedSSE()` hashes to `c2cabffc9c925fe2d55c53bb62e4586f63462c40d8d961402bca801342a2de56`.

Run: `go test ./internal/tui ./internal/testsupport/ptyfixture -run 'Test(GoldenViewFixturesAreStable|ScriptedSSEFixtureIsStable)' -count=1`

Expected: FAIL because both helpers are absent.

- [ ] **Step 2: Move exact bytes into helpers**

Create a `map[int]string` containing the exact current golden bytes and implement a defensive lookup. Add this exact SSE literal:

```go
const scriptedSSE = "data: {\"choices\":[{\"delta\":{\"content\":\"scripted PTY response\"}}]}\n\ndata: [DONE]\n\n"
func ScriptedSSE() string { return scriptedSSE }
```

Change `model_test.go` to compare with `goldenView(test.width)`. Change `readSSEFixture` to return `ptyfixture.ScriptedSSE()` without filesystem access.

- [ ] **Step 3: Verify before and after deleting the TUI tree**

Run:

```bash
gofmt -w internal/tui/golden_views_test.go internal/tui/model_test.go internal/testsupport/ptyfixture/pty_unix.go internal/testsupport/ptyfixture/pty_unix_test.go internal/ptytest/yordam_test.go
go test ./internal/tui ./internal/testsupport/ptyfixture ./internal/ptytest -count=1
```

Delete exactly `internal/tui/testdata`, then rerun the same command.

Expected: PASS with all four byte digests unchanged.

- [ ] **Step 4: Commit**

```bash
git add -- internal/tui internal/ptytest internal/testsupport/ptyfixture
git commit -S -m "test: inline tui and pty fixtures"
```

---

### Task 5: Enforce the invariant and renew release evidence

**Files:**
- Create: `internal/repolint/testdata_test.go`
- Modify: `internal/acceptance/foundation_architecture_test.go`
- Modify: `docs/releases/v0.2.0-foundation-acceptance.md`

**Interfaces:**
- Produces: permanent no-`testdata` lint gate
- Produces: production dependency isolation gate
- Produces: renewed report-only signed Gate 1 evidence

- [ ] **Step 1: Write and observe the repository lint RED**

Before deleting the final fixture directory, add `TestRepositoryContainsNoTestdataDirectories`. Resolve the repository root, walk directories while skipping `.git`, `.worktrees`, and `dist`, and fail on a directory whose base name is `testdata`.

Run: `go test ./internal/repolint -run '^TestRepositoryContainsNoTestdataDirectories$' -count=1`

Expected: FAIL listing the remaining TUI fixture path. After Task 4 deletion, rerun and expect PASS.

- [ ] **Step 2: Add the production dependency isolation gate**

In the acceptance architecture test, run `go list -deps ./cmd/yordam` from the repository root and fail if any output line contains `/internal/testsupport/`. This is test-only architecture behavior and must not alter runtime code.

- [ ] **Step 3: Run exact invariant checks**

```bash
go test ./internal/repolint -count=1
go test -tags=acceptance ./internal/acceptance -run '^TestV020Foundation/application_and_architecture$' -count=1 -v
! git ls-files | rg '(^|/)testdata/'
! find . -type d -name testdata -not -path './.git/*' -not -path './.worktrees/*'
! go list -deps ./cmd/yordam | rg '/internal/testsupport/'
```

Expected: all commands exit zero with no path/dependency matches.

- [ ] **Step 4: Run complete verification**

```bash
go test ./... -count=1 -timeout=120s
go test -race ./... -count=1 -timeout=240s
go vet ./...
go test -tags=acceptance ./internal/acceptance -run '^TestV020Foundation$' -count=1 -v -timeout=240s
go test -race -tags=acceptance ./internal/acceptance -run '^TestV020Foundation$' -count=1 -v -timeout=300s
go test -tags=acceptance ./internal/acceptance -run '^TestV010Acceptance$' -count=1 -v
git diff --check
```

Expected: PASS; Foundation retains 14 fixtures and all 14 v0.1 scenarios pass.

- [ ] **Step 5: Commit the clean source checkpoint**

```bash
git add -- internal/repolint/testdata_test.go internal/acceptance/foundation_architecture_test.go
git commit -S -m "test: enforce generated fixture policy"
```

- [ ] **Step 6: Renew evidence and make a report-only commit**

Record the new signed source SHA, exact suite statuses/timings, bundle digest, zero tracked `testdata` paths, production dependency isolation, and unchanged Foundation/v0.1 scenario counts in the existing release report. Rehash evidence logs and release artifacts using its established format.

```bash
git add -- docs/releases/v0.2.0-foundation-acceptance.md
git commit -S -m "test: renew generated fixture evidence"
git verify-commit HEAD
git verify-commit HEAD^
test "$(git diff --name-only HEAD^ HEAD)" = "docs/releases/v0.2.0-foundation-acceptance.md"
git status --short
```

Expected: both signatures are good, the source-to-report delta contains only the report, and the final worktree is clean.
