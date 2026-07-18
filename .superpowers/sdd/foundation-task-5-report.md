# Foundation Gate 1 Task 5 Report

Date: 2026-07-18

Accepted signed base before Task 5: `1ad106038d6d90651895f022bd4729cd4ad119c3`

Rejected Task 5 commit: `9187376587a493223b20c4ea69bd34e11f699b07` (`feat: add content addressed evidence`)

## Preconditions and protected scope

- The rejection remediation began from clean `9187376587a493223b20c4ea69bd34e11f699b07`.
- `git verify-commit 9187376587a493223b20c4ea69bd34e11f699b07` reported a good signature from Murat Mirgün Ercan using key `C333D69A035F6B952A9AC361E1E0E3973D0B6F43`.
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

- Both stores normalize through the nearest existing canonical ancestor but create no root until validation/admission succeeds.
- On first valid write they lazily create/open one `os.Root`, retain it for the store lifetime, and pin every root/ancestor inode.
- Every later operation verifies the complete identity chain and uses only rooted relative operations.
- Root replacement, ancestor replacement, symlinks, and non-regular substitutions fail closed.

### 8. Recovery publication is one crash-reconcilable unit

- Recovery ID is deterministic from admitted candidate metadata.
- Metadata and material are encoded into one versioned `.recovery` container, eliminating split material/metadata finals.
- Publication uses a synced regular temporary, hard-link no-replace, regular/SameFile final verification, temporary removal, and directory sync.
- Startup/retry reconciles only verified regular recovery temporaries and validates final container digest/provenance before returning it.
- Fault tests cover temporary-synced, before-publish, after-publish, and directory-synced windows and prove retry leaves exactly one final container.

### 9. Legacy migration is rooted and verifiable

- Evidence no longer enumerates `workspaces/` or opens caller-derived arbitrary workspace paths.
- Migration requires an injected `LegacyResolver`; `jsonl.Store` implements it by opening one identified session/artifact through its store-owned rooted transaction.
- Alias metadata contains workspace/session/artifact/target identity and a canonical digest.
- Target evidence ID is deterministic, and every alias hit/`ErrEvidenceExists` path verifies workspace/session/kind/media/activity/actor/subject/alias provenance.
- Alias hits return without reopening or rehashing the legacy source; tampered alias/record, ambiguous resolver, traversal, symlink, and substitution paths fail closed.

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

Result: exit 0. All packages passed. Notable uncached results: `internal/app 5.169s`, `internal/evidence 2.399s`, `internal/integration 1.608s`, `internal/recovery 0.410s`, `internal/session/jsonl 31.559s`, `internal/tools/shell 1.053s`.

```text
go test -race ./... -count=1
```

Result: exit 0. All packages passed in the final post-review run. Notable results: `internal/app 6.003s`, `internal/evidence 2.830s`, `internal/recovery 1.564s`, `internal/secret 1.281s`, `internal/session/jsonl 65.706s`, `internal/tools/output 1.586s`.

```text
go vet ./...
git diff --check
```

Result: both exited 0 with no output.
