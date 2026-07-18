# Foundation Gate 1 Task 3 Report

## Result

Task 3 is implemented from signed base `45b656d2256c604cbacfce9feeb5704578c195e1`.

- Pure `InspectSession` and legacy `Load` no longer truncate journals, repair metadata, clean temporaries, or append interruption events.
- Explicit `RecoverSession` persists the complete request, preserves the exact invalid tail, builds and validates replacement candidates, activates by rename, synchronizes durability boundaries, and commits one typed recovery diagnostic.
- All ten named recovery probes are injected before and after their physical action and are restart/idempotency tested.
- v0.1 events are deterministically upcast in memory with original envelope evidence preserved and missing v2 facts left unknown.
- The first v2 append to a v1 journal atomically prepends the required compatibility declaration; later declarations are rejected.
- Scanning now validates the semantics of every recognized v1 payload before advancing the committed prefix and validates the exact durable v1-to-v2 compatibility transition at the commit marker.
- Recovery is advertised and accepted only for eligible incomplete final fragments or incomplete known transactions; unsupported, malformed, and invalid-transition data stays read-only and non-discardable.
- Persisted recovery manifests, activation candidates, metadata, quarantine artifacts, and their rooted identities are revalidated immediately before mutation. Recovery-diagnostic append retries cover every Task 2 append crash window.
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

### Independent review correction batch

The independent Task 3 review returned seven Important findings. They were reproduced together in one RED batch before production changes:

1. A recognized v1 `mode.changed` record with `{}` advanced the durable head and left append possible. GREEN: the mapped v2 payload's semantic validator runs during the scan (and index verification), the prefix stops before the malformed event, `invalid_known_payload` is emitted, no recovery request is exposed, and append is blocked.
2. `tool.started -> succeeded result` without a request and `tool.requested -> succeeded result` without a start both became `activity.succeeded`. GREEN: requested and started phases are tracked independently; missing, orphaned, duplicate, and out-of-order phases remain `activity.uncertain`.
3. Recovery-diagnostic faults before the marker left operation-owned event bytes that conflicted forever; faults after the marker could return `already_recovered` while metadata lagged. GREEN: partial diagnostic transactions are recognized by the retained transaction/operation identity, preserved under a deterministic diagnostic-tail artifact, reset to the validated prefix, and retried. A committed marker repairs and syncs metadata before success. The test matrix covers `event_write`, `event_sync`, `marker_write`, `marker_sync`, `metadata_write`, `metadata_rename`, and `directory_sync` inside `recovery_diagnostic_commit`.
4. Persisted manifest filenames and observation fields were trusted. GREEN: names are reconstructed from request/operation identity, reserved leaves are rejected, and prefix/source sizes and digests are rebound to the active source or preserved quarantine before any action. Eight forged-field cases plus changed-request identity fail with zero mutation.
5. Candidate validation had a validate-to-activate substitution window. GREEN: candidate, metadata, artifacts directory, and quarantine identities are retained and reopened/recompared at activation; quarantine bytes are rechecked. Substitution at `candidate_activate` fails before either active file is renamed.
6. Unsupported envelope, payload, and unknown stateful records could expose discard-style recovery details. GREEN: only supported incomplete fragments/transactions are eligible; unsupported/malformed/invalid-transition scans never emit `recovery.available`, and `RecoverSession` returns conflict without mutation.
7. Mixed durable journals did not validate the compatibility transition. GREEN: the first committed v2 transaction after legacy data requires exactly one declaration as its first event with reader/writer `2`, the exact legacy head, and `v0.1_read_only_after_v2`; missing, later, duplicate, wrong-field, and repeated declarations stop at `invalid_transition`.

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
- Partial operation-owned files and diagnostic append bytes resume safely. Candidate/session/artifacts/quarantine substitution tests fail closed without changing the active source.
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
go test -race ./internal/session/jsonl -count=1
go test ./... -count=1
go vet ./...
git diff --check
gofmt -l internal/session/jsonl
```

Latest package results:

```text
seven-finding focused regression batch: PASS
jsonl package: PASS (17.028s final package rerun)
jsonl race package: PASS (37.541s final rerun)
repository-wide go test ./...: PASS (including internal/ptytest)
go vet ./...: PASS
diff/gofmt checks: PASS
```

## Scope and concerns

- `.superpowers/sdd/progress.md` was not modified.
- The source fixture directory is immutable in tests; recovery operates only on copied trees.
- The exact fixture inventory remains 11 directories/33 files; the malformed-known fixture is now a recognized malformed v1 source, while unknown/unsupported v2 fixtures carry the required compatibility declaration.
- Workspace-control journals remain the Task 4 boundary.
- Production composition migration remains Task 12.
