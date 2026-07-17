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
| `marker_write` | marker may be partially or fully present | `commit_unknown` | physical rescan decides committed/unknown |
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
