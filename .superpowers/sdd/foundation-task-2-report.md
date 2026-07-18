# Foundation Task 2 Implementation Report

## Status

Implemented and verified against accepted Task 1 base
`7a3dc40b5d46f89372c10514341dd2773780dbb8`.

## Delivered Contracts

- Added the exact `journal.Repository`, request/result, compatibility, recovery,
  range, lookup, inspection, encoder, and independently verified committed
  transaction contracts.
- Added mixed v1/v2 journal scanning. Every v1 line is an implicit committed
  transaction; every v2 transaction becomes visible only after a validated
  `transaction.committed` marker.
- Added expected-head batch append with per-journal in-process serialization,
  contiguous sequence assignment, canonical payload/envelope encoding,
  Task 1 registry validation, one durable marker, and metadata/index updates
  strictly after the marker.
- Added committed-only range reads, transaction lookup, independent committed
  transaction reads, inspection/head, and the explicit Task 3 recovery boundary.
- Added a versioned disposable journal index bound to source size and SHA-256.
  Index entries, offsets, cursors, transaction IDs, and event IDs are physically
  revalidated against journal bytes before use. Missing, stale, corrupt,
  substituted, or unprovable indexes fall back to an authoritative scan and
  safe rebuild.
- Preserved exact raw envelopes for unknown kinds and unsupported payload
  versions while making the validated prefix read-only. Invalid known payloads
  and invalid marker bounds/count/digest/sequence fail closed as corruption.
- Kept the existing JSON redactor as the temporary mandatory Task 2 encoder
  adapter. An absent admission/encoding boundary rejects before writing.

## RED / GREEN Evidence

The initial named test command was run before production contracts existed:

```text
go test ./internal/session/jsonl -run 'Test(AppendBatch|IncompleteBatch|LookupTransaction|ReadRange)' -count=1
```

It failed first because `internal/journal` did not exist, then compiled far
enough to fail on the missing `Options.Encoder`, `AppendBatch`, `ReadRange`, and
`LookupTransaction` APIs. The same exact command passed after the first coherent
implementation.

Focused negative RED/GREEN cycles then covered:

- transaction sizes above 1000 (RED: the commit could not be represented by the
  bounded atomic range API; GREEN: rejection occurs before any write);
- stale `EventPage.More` state after the last commit;
- clean absent, committed-prefix, and incomplete-tail lookup states;
- post-marker faults resolving as committed through lookup and pre-marker
  faults remaining unknown/recovery-required;
- missing, stale, malformed, symlinked, source-mismatched, and exact-source-SHA
  forged indexes;
- a forged prefix transaction ID or event ID changing duplicate decisions
  (RED), followed by physical verification of every indexed segment (GREEN);
- missing encoder, secret-redacting encoder adaptation, opened events-leaf
  substitution, wrong cursor identity, duplicate IDs, mixed v1/v2 input,
  unknown kind/version raw preservation, invalid known payloads, marker
  corruption, and mutable return-value aliases.

## Append and Fault Ordering

| Stage / fault point | Durable interpretation | Returned status | Lookup result |
| --- | --- | --- | --- |
| expected-head mismatch | no bytes written | `conflict` | unchanged |
| incomplete physical tail before append | no new bytes written | `recovery_required` | target/absent state remains `unknown` |
| `event_write` | event bytes may exist without marker | `recovery_required` | `unknown` |
| `event_sync` | events may be durable without marker | `recovery_required` | `unknown` |
| `marker_write` | marker may be partially or fully present and is tracked as not yet durable | `commit_unknown` | a complete marker is committed only after `Sync` plus rooted identity re-verification; partial/substituted data remains unknown |
| `marker_sync` | marker is present but durability result is ambiguous | `commit_unknown` | committed when marker validates |
| `metadata_write` | marker already durable; metadata may lag | `commit_unknown` | committed |
| `metadata_rename` | marker already durable; metadata result ambiguous | `commit_unknown` | committed |
| `directory_sync` | marker and cache writes completed; directory durability result ambiguous | `commit_unknown` | committed |
| all stages complete | marker, metadata, index attempt, and root sync complete | `committed` | committed |

Caller-provided transaction IDs are never replaced or blindly retried after
`commit_unknown`.

## Index Authority and Alias Review

- The journal is the only authority. Source size/digest alone is not treated as
  authentication of index fields: all indexed ranges are re-read, decoded,
  registry-validated, sequence-checked, marker-checked, and compared with the
  index before a seed is accepted.
- A store-local in-memory verified prefix is bound to the opened rooted events
  identity, verified byte count, and SHA-256 source digest. Repeated operations
  hash the unchanged prefix and decode only its tail. Identity, size, digest,
  tail, or validation failure discards the acceleration and performs a full
  authoritative scan. The cache is never shared across registries and never
  remembers read-only, incomplete, or marker-uncertain state.
- Lookup uses a fully physically verified index when possible. It performs an
  authoritative scan and rebuild when the cache cannot prove the complete
  source. `ReadCommittedTransaction` intentionally performs an independent full
  scan on every call.
- Index symlinks/FIFOs/substituted leaves are neither followed nor replaced.
  Read correctness remains available through the journal scan.
- Append requests are cloned before admission. Append results, range pages,
  inspections, diagnostics, raw envelopes, decoded payloads, and independently
  read committed envelopes are returned as immutable copies.

## Rooted I/O and Security Review

- Event identity is verified before each physical phase and after the marker.
  Opened-leaf substitution cannot redirect or mutate an outside/replacement
  file.
- Atomic index replacement uses a rooted 0600 temporary, sync, rooted identity
  verification, rename, root-directory sync, and final regular-file identity
  verification. Existing non-regular leaves are rejected.
- Event lines remain capped at 2 MiB. Append count and range limits are bounded
  to 1..1000. Task 1 canonical JSON and payload bounds remain in force.
- Registry decoding distinguishes unsupported future data (exact raw retained,
  prefix read-only) from invalid known data (corruption).
- Task 3 quarantine/replacement and Task 4 cross-process flock/workspace-control
  behavior were not implemented early.

## Final Verification

The following exact Task 2 gates passed:

```text
go test ./internal/session/jsonl -run 'Test(AppendBatch|ExpectedHead|CommittedBatch|IncompleteBatch|LookupTransaction|ReadRange)' -count=1
go test -race ./internal/session/jsonl -run 'Test(AppendBatch|ExpectedHead|ReadRange)' -count=10
go test ./internal/session/jsonl -run 'Test.*(Substitut|Symlink|Durable|Metadata)' -count=1
go test ./internal/journal ./internal/session/jsonl -count=1
go test ./...
go test -race ./...
go vet ./...
git diff --check
```

All exited 0. The independently repeated broad focused command also passed all
append/head/incomplete/lookup/range/alias/index/forgery/mixed/unknown/invalid
tests. The full race run included every repository package and reported no race.
`go vet` and `git diff --check` produced no output.

## Files

- Created `internal/journal/repository.go`.
- Created `internal/session/jsonl/journal.go`.
- Created `internal/session/jsonl/batch.go`.
- Created `internal/session/jsonl/range.go`.
- Created `internal/session/jsonl/faults.go`.
- Created `internal/session/jsonl/batch_test.go`.
- Created `internal/session/jsonl/range_test.go`.
- Created `internal/session/jsonl/transaction_lookup_test.go`.
- Modified `internal/session/jsonl/store.go`.
- Modified `internal/session/jsonl/rootfs.go`.
- Modified `internal/session/jsonl/conformance_test.go`.
- Created this report.

No config/schema/reload semantics, Task 3 recovery behavior, Task 4 workspace
control/flock behavior, or SDD progress ledger was changed.

## Remaining Boundary

`Recover` deliberately returns the explicit Task 3 boundary error. First-v2
legacy compatibility declaration insertion and deterministic v1 upcasting also
remain Task 3 work. Cross-process journal locks remain Task 4 work.

## Independent Review-Fix Closure (2026-07-18)

The review-fix base was the clean signed Task 2 commit
`2e394b37848e13baeb22097bd8d0586ac7f9d952`; its GPG signature was verified as
good before editing. The independent review supplied ten focused regressions
for nine findings (indexed scanning had separate mixed-order and pure-v1
cases). Before production edits, all ten failed for their intended reasons:

- legacy float/exponent and valid 1-2 MiB lines hit the v2 canonical-number
  validator;
- exact-source indexes bypassed the global v2-to-v1 rule and erased pure-v1
  transition state;
- three unchanged lookups decoded the verified prefix 24 times;
- an unsupported-version commit marker became domain data in a later commit;
- a physical 1001-event transaction was accepted;
- a generated marker-ID collision wrote bytes;
- substituted bytes proved an unsynced complete marker;
- a nil-returning directory-sync hook substituted `events.jsonl` before a
  committed return; and
- a read open deleted live metadata/index writer temporaries.

### Finding-to-fix and permanent evidence

| Finding | Correction | Permanent regression |
| --- | --- | --- |
| Indexed scanner equivalence | Index verification carries private `hasLegacy`/`hasV2` state across segments, rejects v1 after v2, preserves pure-v1 transition state, and mirrors full-scan session/legacy validation. | `TestIndexedScannerRegressionEnforcesGlobalV2BeforeV1Invariant`, `TestIndexedScannerRegressionPreservesPureV1TransitionState` |
| Verified-prefix acceleration | A deep-cloned store-local prefix is bound to rooted file identity, exact byte count, and SHA-256; unchanged bytes are hashed and only the tail is decoded. Any mismatch falls back to verified index/full scan. Memory hits still inspect/rebuild the disposable disk index. | `TestVerifiedPrefixRegressionSkipsOldDecodeForLookupAndAppend` plus existing stale/corrupt/exact-source index tests |
| Exact v1 compatibility | Full and indexed v1 branches use legacy `json.Unmarshal` and `domain.DurableEvent.Validate`; only the 2 MiB physical-line bound applies. | `TestLegacyJournalRegressionAcceptsV01NumbersAndTwoMiBPhysicalLimit` |
| Unsupported commit-marker version | `transaction.committed` remains structural at unsupported versions, makes the transaction ambiguous/read-only, and can never be accumulated or committed by a later marker. | `TestUnsupportedCommitMarkerRegressionRemainsStructuralAndAmbiguous` |
| Physical transaction bound | Full and indexed scanners reject the 1001st domain event before pending-slice accumulation. | `TestPhysicalTransactionRegressionRejectsMoreThanOneThousandEvents` |
| Generated marker collision | The generated marker ID is checked against all durable and proposed IDs before the first event write. | `TestGeneratedMarkerIDRegressionRejectsProposedCollisionBeforeWrite`, `TestGeneratedMarkerIDRegressionRejectsDurableCollisionBeforeWrite` |
| Marker durability uncertainty | Uncertainty is registered before marker write and cleared only after successful `Sync`. Lookup can prove a complete marker only on the original rooted identity after another `Sync` and rooted re-verification; partial/substituted data remains unknown and cannot publish an index. | `TestUnsyncedMarkerRegressionRequiresOriginalRootedIdentity` plus existing pre/post-marker fault tests |
| Directory-sync substitution | After the fault hook, append checks context and rooted events identity again before returning committed. | `TestDirectorySyncHookRegressionReverifiesEventsBeforeCommitted` |
| Read/write temporary ownership | Read-only opens never reconcile writer temporaries. Range/lookup index maintenance is serialized by the keyed journal lock; independent committed reads do not write indexes. | `TestConcurrentReadRegressionDoesNotDeleteLiveWriterTemporaries` |

Each finding was run individually RED then GREEN. The complete temporary review
matrix subsequently passed, was removed, and its durable equivalents remained
in Task 2-owned test files. A fresh permanent package gate passed:

```text
go test ./internal/session/jsonl -count=1
ok github.com/muratmirgun/yordam/internal/session/jsonl 12.644s
```

The final review-fix checkpoint and delivery gates were then rerun from the
still-unstaged diff and all exited 0:

```text
TASK_GO_FILES=<the exact eleven Task 2-owned Go paths> <mandatory format/test/diff/status checkpoint>
go test ./internal/session/jsonl -run 'Test(AppendBatch|ExpectedHead|CommittedBatch|IncompleteBatch|LookupTransaction|ReadRange)' -count=1
go test -race ./internal/session/jsonl -run 'Test(AppendBatch|ExpectedHead|ReadRange)' -count=10
go test ./internal/session/jsonl -run 'Test.*(Substitut|Symlink|Durable|Metadata)' -count=1
go test ./internal/journal ./internal/session/jsonl -count=1
go test ./...
go test -race ./...
go vet ./...
git diff --check
```

The focused race repetition completed with no race report, the repository-wide
race run covered every package, `go vet` and `git diff --check` were silent,
and `gofmt -l` returned no Task 2-owned path.

No Task 3 recovery/upcasting behavior, Task 4 `flock`, config/schema/reload
semantics, or `.superpowers/sdd/progress.md` content was changed.
