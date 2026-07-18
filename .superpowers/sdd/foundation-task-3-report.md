# Foundation Gate 1 Task 3 Report

## Result

Task 3 is implemented from signed base `45b656d2256c604cbacfce9feeb5704578c195e1`.

- Pure `InspectSession` and legacy `Load` no longer truncate journals, repair metadata, clean temporaries, or append interruption events.
- Explicit `RecoverSession` persists the complete request, preserves the exact invalid tail, builds and validates replacement candidates, activates by rename, synchronizes durability boundaries, and commits one typed recovery diagnostic.
- All ten named recovery probes are injected before and after their physical action and are restart/idempotency tested.
- v0.1 events are deterministically upcast in memory with original envelope evidence preserved and missing v2 facts left unknown.
- The first v2 append to a v1 journal atomically prepends the required compatibility declaration; later declarations are rejected.
- The exact 11-case fixture matrix contains 33 fixture files plus one sorted lowercase SHA-256 manifest.
- No config, schema, reload, Task 4+, or progress-ledger semantics were changed.

## Base verification

Before implementation:

```text
HEAD 45b656d2256c604cbacfce9feeb5704578c195e1
git status --short: empty
git verify-commit HEAD: Good signature from Murat (key C333...)
```

## TDD evidence

The exact migration command was run before production implementation and failed to compile because `InspectSession`, `UpcastState`, and `UpcastV1` did not exist:

```bash
go test ./internal/session/jsonl -run 'Test(FoundationFixture|InspectSession|V1Upcast|MixedV1V2|UnknownFuture|UnsupportedPayload|InvalidKnown)' -count=1
```

Explicit recovery was then stubbed and the recovery suite failed every scenario with `explicit recovery is not implemented`, including exact-tail preservation, both occurrences of every named fault probe, persisted request identity, conflicts, and repository/session idempotency.

The pure-Load correction received a separate RED source snapshot test: the old implementation truncated an incomplete tail and appended a recovery interruption. After the correction, both `Load` and `InspectSession` leave the complete tree unchanged.

A final crash-window RED test truncated each operation-owned quarantine/journal-candidate/metadata-candidate file between write and sync. Retry initially failed with a permanent conflict; recovery now reconstructs only manifest-owned candidates and resumes. The active journal is never truncated in place.

## Implementation and compatibility

### Inspection and upcasting

- `InspectSession` opens the journal read-only, returns the validated committed prefix, exposes stable diagnostics and exact observed-tail digest/byte bounds, and does not mutate filesystem state.
- `Load` delegates to inspection, reconstructs the compatible v0.1 replay in memory, and reports read-only diagnostics without hidden recovery.
- Original v1 event ID, session ID, order, sequence, timestamp, kind/raw payload, and raw envelope are retained in `Legacy` evidence.
- Synthetic IDs use the exact specified SHA-256 byte sequence and `derived:` prefix.
- Call identity is turn-scoped; flat and nested historical tool-result shapes are supported. Duplicate starts/terminals remain uncertain, and unmatched calls never become success.
- Historical output/diffs remain legacy evidence, not verification receipts. Usage, cost, profiles, verification, and restore guarantees are not invented.

### Explicit recovery

- Request identity is durably persisted before the first named recovery action.
- The exact tail is retained under an operation/tail-digest-derived artifact name, including tails larger than the legacy 10 MiB retention cap (bounded by the 64 MiB recovery-journal limit).
- Journal and metadata candidates are separately written, synced, rooted-identity checked, content validated, activated by replacement, and followed by directory sync.
- Same-request restart returns `recovered` or `already_recovered`; changed request/head/tail returns `conflict` without mutation.
- Partial operation-owned files resume safely. Candidate/session-root substitution tests fail closed without changing the active/replacement source.
- Recovery appends `recovery.diagnostic` in a new committed transaction and automatically includes the first-v2 compatibility declaration for a legacy-only prefix.

## Test migrations outside the nominal brief list

The following existing Task 2/v0.1 tests encoded the removed automatic-Load mutation path and therefore required narrow semantic migration:

- `batch_test.go`: pure Load no longer enters mutation sanitization or temporary cleanup.
- `file_recovery_test.go`: pure Load preserves abandoned mutation temporaries.
- `hardening_test.go`: restart recovery now uses the complete explicit request; unmatched tool calls are diagnostics without appended events.
- `transaction_test.go`: pure Load does not scan or mutate unrelated legacy recovery artifacts.

`recovery_test.go` and `recovery_generation_test.go` were migrated from automatic truncation/metadata repair/interruption expectations to unchanged-source inspection plus explicit recovery. Superseded mutation-only helper coverage was replaced by explicit fault, partial-write, candidate-substitution, and opened-root tests.

## Verification

Passed:

```bash
go test ./internal/session/jsonl -run 'Test(FoundationFixture|InspectSession|V1Upcast|MixedV1V2|UnknownFuture|UnsupportedPayload|InvalidKnown)' -count=1
go test ./internal/session/jsonl -run 'Test(FoundationFixture|InspectSession|ExplicitRecovery|V1Upcast|MixedV1V2|ValidatedPrefix)' -count=1
go test -race ./internal/session/jsonl -run 'Test(InspectSession|ExplicitRecovery)' -count=1
go test ./internal/session/jsonl -count=1
go vet ./...
git diff --check
gofmt -l internal/session/jsonl
```

Latest package results:

```text
jsonl package: PASS (15.196s final gate; 15.598s after dead-path cleanup)
focused race: PASS (5.728s)
go vet ./...: PASS
diff/gofmt checks: PASS
```

Repository-wide `go test ./... -count=1` passed every package except one pre-existing PTY raw-output assertion:

```text
internal/ptytest/TestEditAndReloadPreservesSessionAndSecretBoundaries
timed out waiting for "assistant response after reload"
```

The output contains the complete rendered response, but secret streaming intentionally buffers the suffix `reload` (a prefix of `reload-secret`) until close, causing two alternate-screen redraws and escape bytes between `assistant response after` and `reload`. Isolated `-count=3` reproduced the test-harness defect. No PTY/TUI files are part of this Task 3 change; the parent task is handling that test-infrastructure correction separately before final review.

## Scope and concerns

- `.superpowers/sdd/progress.md` was not modified.
- The source fixture directory is immutable in tests; recovery operates only on copied trees.
- Workspace-control journals remain the Task 4 boundary.
- Production composition migration remains Task 12.
