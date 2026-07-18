# Foundation Gate 1 Task 5 Report

Date: 2026-07-18

Accepted signed base before Task 5: `1ad106038d6d90651895f022bd4729cd4ad119c3`

Rejected Task 5 commit: `9187376587a493223b20c4ea69bd34e11f699b07` (`feat: add content addressed evidence`)

First rejection remediation commit: `853c435448b07ca1838f76fd7441bacab8326cc5` (`fix: close evidence admission gaps`)

## Preconditions and protected scope

- The rejection remediation began from clean `9187376587a493223b20c4ea69bd34e11f699b07`.
- `git verify-commit 9187376587a493223b20c4ea69bd34e11f699b07` reported a good signature from Murat Mirgün Ercan using key `C333D69A035F6B952A9AC361E1E0E3973D0B6F43`.
- The second re-review remediation began from clean `853c435448b07ca1838f76fd7441bacab8326cc5`; its sole parent is `9187376587a493223b20c4ea69bd34e11f699b07`.
- `git verify-commit 853c435448b07ca1838f76fd7441bacab8326cc5` reported a good signature using key `C333D69A035F6B952A9AC361E1E0E3973D0B6F43`.
- `internal/protocol/evidence.go` remains byte-for-byte unchanged by this remediation.
- `.superpowers/sdd/progress.md` remains unchanged.
- No configuration schema, configuration persistence, or reload-policy contract was added or changed.

## Rejection RED evidence

Regression tests were written before each remediation group.

Lease revocation and producer ownership:

```text
go test ./internal/secret -run 'TestClosedLease|TestRetiredGenerationKeeps' -count=1
```

Result: exit 1. `*secret.Lease` had no `Derive` method; closed leases still retained usable scanner/redactor state.

Mandatory model-context admission:

```text
go test ./internal/agent -run 'TestBuildContextAdmitsEvery|TestModelContextRejects' -count=1
```

Result: exit 1. `BuildContext` accepted no lease and returned no error; `ToolResultContentLeased` still had a nil-lease compatibility fallback.

Pinned recovery generation:

```text
go test ./internal/session/jsonl -run TestRecoveryPinsGenerationAcrossFaultAndRestartAdmission -count=1
```

Result: exit 1. `journal.RecoveryRequest` had no `RuntimeGenerationID` field. After adding the field, the first deterministic-replay attempt remained RED because the prepared marker bytes omitted the pinned generation; the marker was then fixed and the fault/restart test passed.

Full envelope metadata admission:

```text
go test ./internal/session/jsonl -run TestAppendBatchRejectsSecretInFullEnvelopeMetadataBeforeWrite -count=1
```

Result: exit 1. Secret event kind reached codec validation and secret actor/event identity metadata was not rejected by admission before journal mutation.

Production composition:

```text
go test ./internal/app ./internal/session/jsonl ./internal/secret -run 'TestRuntimeBuilderRegistersGeneration|TestNewAdmitted|TestBindingAcquires' -count=1
```

Result: exit 1. The runtime builder had no shared registry field, JSONL had no strict `NewAdmitted` constructor/generation binding, and bindings could not acquire independently owned leases.

Evidence bounds, metadata, and rooted identity:

```text
go test ./internal/evidence -run 'TestEvidenceRejectsSecretMetadata|TestEvidenceFailsAfterPinned' -count=1
```

Result: exit 1. Evidence still accepted arbitrary scanners, had no closeable owned lease, created/published before complete metadata admission, and did not pin root/ancestor identities.

Rooted legacy migration:

```text
go test ./internal/evidence -run TestLegacy -count=1
```

Result: exit 1. `LegacyResolver` and `WithLegacyResolver` did not exist; the implementation still enumerated arbitrary workspace directories and trusted undigested aliases.

Atomic recovery publication:

```text
go test ./internal/recovery -run 'TestRecoveryRejectsSecretMetadata|TestRecoverySingleContainer' -count=1
```

Result: exit 1. Recovery still accepted an arbitrary scanner, exposed no closeable owned lease, and had no fault points or crash-reconcilable single-container publication.

Generation-bound production logging:

```text
go test ./internal/logging -run TestGenerationBound -count=1
```

Result: exit 1 because `logging.NewGenerationBound` did not exist.

## Remediation of the nine Important findings

### 1. No production admission bypass

- Bootstrap now creates one shared generation registry before the logger/store/runtime composition.
- `jsonl.NewAdmitted` requires both that registry and a live generation binding. Bootstrap uses only this strict constructor.
- Runtime generations register in the shared registry rather than private per-build registries.
- Debug logging uses `NewGenerationBound`; every event acquires an owned lease from the active binding and rejects a missing generation.
- Artifact writes in the strict store acquire a live generation lease before reading or creating a temporary.
- Compatibility constructors remain available for tests and v0.1 adapters, but are not reachable from production bootstrap.

### 2. Recovery generation survives fault/restart

- `journal.RecoveryRequest` now carries `RuntimeGenerationID`.
- Recovery request manifests pin that generation because they persist the complete request.
- Compatibility, recovery-diagnostic, and transaction-marker envelopes all carry the same generation.
- Deterministic replay reconstructs the exact admitted bytes, including the generation.
- Recovery request IDs/journal/head/transaction/generation metadata are admitted before manifest persistence.
- An enabled-registry fault/restart test proves the same generation bytes survive retry.

### 3. Every model-visible context field is leased

- `BuildContext` now requires a live lease and returns an error.
- System prompts, historical compaction summaries, user/assistant/tool messages, tool call IDs/names/arguments, tool-result IDs/content/artifact IDs, model selection, and tool descriptor names/descriptions/scopes/schemas are admitted.
- `ToolResultContentLeased` rejects nil/closed leases; its permissive fallback was removed.
- Runner and compaction both use independently owned generation leases.
- Registered encoded variants in historical/compatibility replay cannot reach the provider.

### 4. Registry and stream lifecycle is fail closed

- `Lease.Derive` creates an independently owned producer lease.
- Retirement blocks new producers while already-derived producers and buffers pin the generation.
- Closing a lease revokes its scanner, redactor, admission streams, and redaction streams; lease-local variants and pending stream bytes are cleared.
- Closed `String`/`Bytes` return only `[REDACTED]`; `JSON`, `Admit`, `Derive`, and stream acquisition return `ErrLeaseClosed`.
- Provider clients, runner, compaction, output buffers, logger events, journal/artifact writes, evidence, and recovery each own or derive their producer lifetime.
- Snapshot-only and error paths in read/search/edit/shell/workspace inspection now close buffers.
- Focused and full race runs cover close/retire/reload/old-buffer behavior.

### 5. Bounded evidence retention and pre-publication validation

- Evidence clones only `candidate.Content[:min(limit,len)]`; it never clones the full oversized candidate before truncation.
- Candidate non-content bounds and complete record-body bounds are validated before blob publication.
- Secret/invalid metadata tests use multi-megabyte content and prove the storage root remains absent.

### 6. Complete metadata/envelope admission

- Evidence and recovery constructors require generation leases, not arbitrary scanners.
- Evidence admits canonical ID/kind/workspace/session/media/activity/actor/subject/alias metadata before filesystem access.
- Recovery admits canonical workspace/activity/checkpoint/subject/digest/mode metadata and material before filesystem access.
- Journal append admits journal/head/transaction and every proposed non-payload envelope field before mutation; payload admission still redacts through the event encoder.
- Explicit recovery admits the full request before persisting a manifest.
- Encoded identity/path/actor/subject variants are rejected and leave no new durable bytes.

### 7. Evidence/recovery root identity is pinned

- Both stores normalize through the nearest existing canonical ancestor, retain that ancestor's `os.Root` and inode chain at construction, and create no missing root component until validation/admission succeeds.
- Missing components are created one at a time beneath retained `os.Root.OpenRoot` handles; the original absolute path is never reaccepted as creation authority.
- Root, creation-chain, and dynamically discovered descendant directory handles/identities are retained for the store lifetime and verified before later use.
- Fresh access also checks every relative directory component, so in-root symlinks cannot be accepted merely because the store was constructed after substitution.
- Tests cover root/deepest-ancestor replacement before first `Put`, root non-creation on rejection, and existing/fresh substitutions of materials, evidence records, blobs, aliases, and dynamic workspace paths.

### 8. Recovery publication is one crash-reconcilable unit

- Recovery ID is deterministic from admitted candidate metadata.
- Metadata and material are encoded into one versioned `.recovery` container, eliminating split material/metadata finals.
- Publication uses a synced regular temporary, hard-link no-replace, regular/SameFile final verification, temporary removal, and directory sync.
- Startup/retry reconciles only verified regular recovery temporaries and validates final container digest/provenance before returning it.
- Fault tests cover temporary-synced, before-publish, after-publish, and directory-synced windows and prove retry leaves exactly one final container.
- A stable rooted `.recovery.lock` is verified as a regular same-file leaf, forced to mode `0600`, synced when created, and protected with context-aware `flock` on Darwin/Linux.
- All temporary reconciliation and publication occurs under that cross-process lock. Live contenders fail with typed `ErrRecoveryBusy` plus their context error; owner death releases the kernel lock and permits abandoned temporary cleanup.

### 9. Legacy migration is rooted and verifiable

- Evidence no longer enumerates `workspaces/` or opens caller-derived arbitrary workspace paths.
- Migration requires an injected `LegacyResolver`; `jsonl.Store` implements it by opening one identified session/artifact through its store-owned rooted transaction.
- Alias metadata contains workspace/session/artifact/target identity and a canonical digest.
- Target evidence ID is deterministic, and every alias hit/`ErrEvidenceExists` path verifies workspace/session/kind/media/activity/actor/subject/alias provenance.
- Alias hits return without reopening or rehashing the legacy source; tampered alias/record, ambiguous resolver, traversal, symlink, and substitution paths fail closed.

## Second re-review RED evidence and closure

### A. Final durable bytes and one journal generation

Tests registered generated-only values from evidence/recovery timestamps and digests, legacy alias digests, journal envelope/marker fields, and complete recovery transaction bytes. Before the fix, every final-byte case returned a nil error, and a two-event transaction with different runtime generations was accepted:

```text
go test ./internal/evidence ./internal/recovery ./internal/session/jsonl -run 'Test(EvidenceAdmitsFinal|LegacyAliasAdmitsFinal|RecoveryAdmitsFinalCanonical|AppendBatchAdmitsComplete|AppendBatchRequiresOnePinned|RecoveryAdmitsComplete)' -count=1
```

The final canonical evidence record is now admitted before blob/record publication, the complete recovery container before root creation, and the complete canonical legacy alias before evidence/alias publication. Journal append acquires exactly one generation lease, rejects mixed generations, uses that lease for every proposed payload/envelope, and admits event lines plus the generated marker as one deterministic byte sequence before the first journal write. Explicit recovery admits those exact transaction bytes before manifest/candidate persistence and rechecks the persisted bytes on diagnostic commit/restart.

### B. Root identity for the entire store lifetime

The new construction and descendant tests failed before implementation: both stores accepted replacement of an existing root or deepest existing ancestor after construction but before first `Put`; evidence reads followed in-root symlink replacements of records and workspace/blob descendants.

```text
go test ./internal/evidence ./internal/recovery -run 'Test(EvidencePinsExistingRootOrDeepestAncestorAtConstruction|EvidenceRejectsInRootSymlinkSwapOfPinnedDescendant|RecoveryPinsExistingRootOrDeepestAncestorAtConstruction|RecoveryRejectsInRootSymlinkSwapOfPinnedMaterials)' -count=1
```

The shared retained-anchor implementation described in finding 7 made these cases green. Alias and explicit blob-directory variants cover both already-open and fresh stores.

### C. Cross-instance and cross-process recovery reconciliation

The gated two-store test initially showed the contender returning success, deleting the live writer's synced temporary, and publishing its own final. The live subprocess case failed identically, and the crash case proved no stable lock existed:

```text
go test ./internal/recovery -run '^TestRecoveryCoordinationPreventsSecondStoreFromDeletingLiveTemporary$' -count=1
go test ./internal/recovery -run '^TestRecoveryCoordinationAcrossProcessesAndOwnerDeath$' -count=1
```

The rooted lock described in finding 8 made both cases green. After release, both stores resolve the same record with one final and no temporary; after subprocess owner death, the next owner removes the abandoned verified temporary and completes publication.

## Verification evidence

Focused journal/evidence/recovery verification:

```text
go test ./internal/session/jsonl ./internal/recovery ./internal/evidence -count=1
ok github.com/muratmirgun/yordam/internal/session/jsonl 29.823s
ok github.com/muratmirgun/yordam/internal/recovery 0.627s
ok github.com/muratmirgun/yordam/internal/evidence 1.711s

go test -race ./internal/recovery ./internal/evidence -count=1
ok github.com/muratmirgun/yordam/internal/recovery 1.615s
ok github.com/muratmirgun/yordam/internal/evidence 2.463s
```

Lifecycle race verification:

```text
go test -race ./internal/secret ./internal/tools/output ./internal/logging ./internal/app -count=1
ok github.com/muratmirgun/yordam/internal/secret 1.423s
ok github.com/muratmirgun/yordam/internal/tools/output 1.797s
ok github.com/muratmirgun/yordam/internal/logging 1.374s
ok github.com/muratmirgun/yordam/internal/app 4.996s
```

Repository verification:

```text
go test ./... -count=1
```

Result: exit 0 in the final second re-review run. Notable uncached results: `internal/app 8.278s`, `internal/evidence 5.710s`, `internal/recovery 2.428s`, `internal/session/jsonl 38.552s`, `internal/ptytest 10.489s`.

```text
go test -race ./... -count=1
```

Result: exit 0. All packages passed in the final second re-review run. Notable results: `internal/app 9.676s`, `internal/evidence 7.009s`, `internal/recovery 4.681s`, `internal/secret 1.248s`, `internal/session/jsonl 73.550s`, `internal/tools/output 1.552s`.

Repeated and focused race verification:

```text
# generated/final-byte admissions: count=20
# root/descendant substitutions: count=10
# recovery coordination and fault windows: count=5
# focused evidence/recovery/journal race runs: exit 0
```

All repeated and focused race commands passed.

```text
go vet ./...
git diff --check
```

Result: both exited 0 with no output.
