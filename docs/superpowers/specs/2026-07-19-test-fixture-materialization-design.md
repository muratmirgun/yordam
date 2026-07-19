# Test Fixture Materialization Design

**Date:** 2026-07-19  
**Status:** Approved for implementation planning

## Goal

Remove every tracked `testdata` directory from the repository without weakening
Foundation migration, corruption, recovery, TUI rendering, or PTY compatibility
coverage.

The finished tree contains no path component named `testdata`. Tests that need
filesystem fixtures create them under `t.TempDir()` from centralized,
test-only definitions.

## Current State

The repository tracks three fixture trees:

- `internal/acceptance/testdata`: 40 files, about 160 KiB;
- `internal/session/jsonl/testdata`: 34 files, about 136 KiB;
- `internal/tui/testdata`: 4 files, about 16 KiB.

Most Foundation journal fixtures are duplicated between acceptance and JSONL
migration tests. The TUI tree contains three golden views and one scripted SSE
stream used by PTY tests.

## Chosen Approach

Introduce shared fixture materializers under `internal/testsupport`. Foundation
fixtures remain independent, literal compatibility data rather than being
serialized by production code. Tests materialize the requested scenario into a
fresh temporary directory and then exercise the existing filesystem APIs.

This approach was selected over:

- a compressed archive, which would leave an opaque binary test asset in the
  repository; and
- production-generated fixtures, which would make compatibility tests validate
  the current serializer against its own output.

## Architecture

### Foundation fixture package

Add `internal/testsupport/foundationfixture` with:

- one canonical inventory of all 14 scenario names;
- immutable raw file definitions for each scenario;
- a `Materialize` helper that writes a requested inventory beneath a caller's
  `t.TempDir()` using explicit file modes;
- a stable bundle digest computed from sorted relative paths and exact bytes;
- validation that rejects duplicate, absolute, escaping, or unlisted paths.

The package must not import production journal encoders. Raw v1/v2 envelopes,
malformed payloads, truncated tails, digest mismatches, and expected results
remain literal bytes so future protocol changes cannot silently rewrite the
compatibility baseline.

The JSONL migration and recovery tests materialize the shared journal subset.
Foundation acceptance materializes the full inventory, including evidence and
lineage-only cases. Existing immutability checks snapshot the temporary source
tree before and after inspection/recovery.

### TUI and PTY fixtures

Move the three small TUI golden views into a named map in a `_test.go` helper in
`internal/tui`. Existing comparison behavior and update guidance remain, but
tests no longer read or rewrite repository files.

Move the scripted SSE response into the existing `internal/testsupport/ptyfixture`
package as immutable bytes. PTY tests request the fixture through that package
instead of resolving a repository path.

### Production isolation

Only tests may import the new helpers. The `cmd/yordam` dependency graph must
not include `internal/testsupport/foundationfixture` or the PTY fixture symbol.
No `go:embed` directive is introduced.

## Data Flow

1. A test selects a named scenario or fixture subset.
2. The materializer validates the static inventory and bundle digest.
3. It creates a fresh directory beneath `t.TempDir()` and writes exact bytes.
4. The existing journal/TUI/PTY code reads the generated input through its
   normal public boundary.
5. Tests compare semantic projections or exact golden output as before.
6. The temporary tree is removed automatically by the Go test framework.

## Error Handling

Fixture construction fails the calling test immediately on an unknown scenario,
unsafe path, duplicate path, digest mismatch, directory creation failure, or
short/failed write. Materialization never writes outside the caller-owned root.

Golden mismatches print the same actionable expected/actual diff but do not
offer a repository-file update mode. Updating a golden requires changing the
named literal in code, making the change visible in ordinary code review.

## Test-Driven Migration

The first failing guard asserts that no tracked or filesystem path component
named `testdata` remains after the migration. Additional failing tests establish
the materializer API, inventory, stable digest, path-safety behavior, and exact
TUI/PTY byte output before existing consumers are switched.

Implementation then proceeds in small slices:

1. add and verify the Foundation materializer;
2. migrate JSONL tests and remove their fixture tree;
3. migrate Foundation acceptance and remove its fixture tree;
4. inline TUI goldens and PTY SSE bytes, then remove the final fixture tree;
5. enforce the repository-wide no-`testdata` invariant.

## Verification

Required checks:

- focused materializer, JSONL migration/recovery, TUI, and PTY tests;
- `go test ./...`;
- `go test -race ./...`;
- Foundation acceptance regular and race runs;
- all 14 v0.1 acceptance scenarios;
- architecture/application acceptance gates;
- `go list -deps ./cmd/yordam` production-isolation assertion;
- `git ls-files | rg '(^|/)testdata/'` returns no matches;
- `find . -type d -name testdata` returns no matches;
- `git diff --check`.

The acceptance evidence report must be renewed from the final clean source
commit because fixture provenance and evidence-manifest hashes change.

## Non-Goals

- Removing or reducing migration, corruption, recovery, TUI, or PTY coverage;
- generating compatibility fixtures from production serializers;
- changing protocol semantics or runtime behavior;
- adding binary archives, Git LFS, external downloads, or network-dependent
  tests.
