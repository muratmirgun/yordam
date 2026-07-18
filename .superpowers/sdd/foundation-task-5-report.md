# Foundation Gate 1 Task 5 Report

Date: 2026-07-18

Accepted base/head: `1ad106038d6d90651895f022bd4729cd4ad119c3`

## Precondition evidence

- `git status --short` produced no output before the first edit.
- `git rev-parse HEAD` returned `1ad106038d6d90651895f022bd4729cd4ad119c3`.
- `git verify-commit HEAD` returned a good signature from `Murat Mirgün Ercan (github) <muratmirgunercan225@gmail.com>` using RSA key `C333D69A035F6B952A9AC361E1E0E3973D0B6F43`.
- `internal/protocol/evidence.go` was consumed unchanged.
- `.superpowers/sdd/progress.md` was not modified.

## RED evidence

The initial focused tests were written before the Task 5 production packages and APIs.

Command:

```text
go test ./internal/secret ./internal/evidence ./internal/recovery -count=1
```

Result: exit 1.

```text
github.com/muratmirgun/yordam/internal/evidence: no non-test Go files in .../internal/evidence
github.com/muratmirgun/yordam/internal/recovery: no non-test Go files in .../internal/recovery
internal/secret/admission_test.go:13:20: undefined: secret.NewAdmissionScanner
internal/secret/registry_test.go:12:21: undefined: secret.NewRegistry
FAIL github.com/muratmirgun/yordam/internal/secret [build failed]
FAIL github.com/muratmirgun/yordam/internal/evidence [build failed]
FAIL github.com/muratmirgun/yordam/internal/recovery [build failed]
```

The currently reachable sink adapters were then tested before their leased APIs were added.

Command:

```text
go test ./internal/logging ./internal/tools/output ./internal/session/jsonl -run 'Test(LeasedLogger|BufferLease|AppendBatchUsesGenerationLease|ArtifactAdmission)' -count=1
```

Result: exit 1. Failures were the missing `logging.NewLeased`, `output.Options.Admission`, `jsonl.Options.Secrets`, `jsonl.Options.ArtifactAdmission`, and `secret.ErrSecretDetected` contracts.

The Step 4 sink audit added provider, model-result, application-publishing, and runtime-lease tests before wiring.

Command:

```text
go test ./internal/app ./internal/provider/openaicompat ./internal/agent -run 'Test(PublishUsesGenerationLease|ProviderNormalizationUsesLease|ToolResultContentUsesGenerationLease|RuntimeBuilderRedactsConfigured)' -count=1
```

Result: exit 1. Failures were the missing `ToolResultContentLeased`, provider `Admission`, lease-capable binding, and runtime `Admission`/`RuntimeGenerationID` contracts.

A final focused diagnostic test was also observed RED before the fix.

Command:

```text
go test ./internal/agent -run TestRunnerAdmitsDiagnosticReasonBeforePersistence -count=1
```

Result: exit 1 because the persisted terminal reason still contained `ZGlhZ25vc3RpYy1zZWNyZXQ=`. After admitting terminal reasons through the lease, the same command passed.

## Implemented contracts and design notes

### Generation-leased secret admission

- `AdmissionScanner` expands every non-empty raw value to raw bytes, padded and unpadded standard Base64, padded and unpadded URL-safe Base64, lowercase hex, and uppercase hex. Exact duplicate variants are removed.
- Chunked scanning retains exactly `longestVariant-1` bytes of overlap, so variants split at arbitrary write boundaries are detected.
- `Registry.Acquire` pins an immutable scanner/redactor to a runtime generation. `Retire` rejects new acquisitions. Existing producer leases and leased redaction/admission streams retain their variants until their own lifetime closes.
- Lease JSON admission redacts values while preserving ordinary schema keys. If a registered variant survives in a structural key, admission fails closed to a minimal redacted JSON object instead of writing the key.

### Immutable evidence

- The retained limit is 10 MiB. Truncated records retain the original size and hash only the retained immutable content.
- Blobs live at `workspaces/<workspace>/evidence/blobs/sha256/<digest>` and deduplicate across sessions in the same workspace.
- Blob and metadata publication use same-directory temporary regular files, file sync, hard-link create-no-replace publication, identity verification, temporary removal, and directory sync.
- Metadata lives at `evidence/records/<evidence-id>.json`, is canonicalized, body-digested, immutable, and never replaced.
- Registered secret content produces `ContentWithheldSecret`, `Redacted=true`, and a nil blob. No candidate content is written.
- `Open` safely resolves a regular file, reads it before returning, and verifies both size and SHA-256. Missing, digest-mismatched, and unsafe substituted blobs cannot verify.
- `Get` leaves immutable metadata untouched and returns a freshly body-digested missing/corrupt projection. The store records typed recovery diagnostics, and `VerifyReceipt` returns `ErrEvidenceUnavailable` when any dependent evidence cannot verify.

### Confidential recovery material

- Candidate validation, preimage digest comparison, and exact admission scanning all happen before the recovery root, a temporary file, or a final file is created.
- Recovery directories are forced to `0700`; material and metadata files are forced to `0600`.
- An injected optional `Sealer` runs in memory before publication, and the protocol record states whether sealing was used.
- `Candidate` has no JSON tags, rejects JSON marshaling, and implements redacted `String`/`GoString`; the store exposes no material read/display method.

### Legacy artifact migration

- Migration resolves a legacy artifact ID beneath the store-owned workspace/session/artifact hierarchy; caller paths are never accepted as identity.
- Traversal, non-regular files, symlinks, ambiguous session identities, and substitutions are rejected.
- Content is lazily hashed and published through the evidence store. A durable alias maps `(session ID, legacy artifact ID)` to the immutable evidence ID, so subsequent resolution does not reopen or rehash the legacy source.

### Step 4 sink audit

- Journal encoding and recovery-diagnostic admission: `jsonl.Store` optionally acquires the event generation for every encoded proposal; compatibility events inherit the caller generation. Persisted recovery admission reuses the same entry point and therefore fails closed without a generation instead of bypassing scanning.
- Legacy artifact output: a configured artifact lease scans the complete bounded candidate before any temporary/final artifact write.
- Provider normalization: the OpenAI-compatible client holds the runtime lease, performs boundary-safe SSE text redaction, and re-admits completed tool-call IDs, names, and JSON arguments before emission.
- Tool normalization and model-visible construction: `Runner` re-admits prompts, deltas, tool calls, previews, results, and terminal diagnostic reasons. `ToolResultContentLeased` performs a final model-result admission step.
- Evidence and recovery: both require a non-nil scanner at construction and scan before persistence.
- Logging: `NewLeased` requires a producer lease. The bootstrap logger's binding holds the active runtime lease and therefore receives the same exact encoded variants.
- Application publishing: the active binding accepts a generation lease, and all app events/errors are sanitized before they enter the published queue or logger.
- TUI: there is no separate production ingress or sink. `tui.Run` and `tui.Model` consume only `application.Events()`, which is admitted before queue publication. TUI tests that inject raw events are test harnesses, not a production bypass.
- Fake/headless: there is no production headless adapter in this repository. Acceptance and agent fixtures consume the same admitted app/runtime paths; direct fixture providers are test-only.

No config schema, config persistence, or reload policy contract was added or changed. Runtime reload only retires the previous secret generation after successful activation; an already-leased producer keeps its immutable variants for its remaining lifetime.

## GREEN evidence

Focused admission/storage command:

```text
go test ./internal/secret ./internal/evidence ./internal/recovery -count=1
ok github.com/muratmirgun/yordam/internal/secret 0.356s
ok github.com/muratmirgun/yordam/internal/evidence 1.034s
ok github.com/muratmirgun/yordam/internal/recovery 0.295s
```

Focused reachable-sink command:

```text
go test ./internal/logging ./internal/tools/output ./internal/session/jsonl ./internal/provider/openaicompat ./internal/agent ./internal/app -count=1
ok github.com/muratmirgun/yordam/internal/logging 0.214s
ok github.com/muratmirgun/yordam/internal/tools/output 0.283s
ok github.com/muratmirgun/yordam/internal/session/jsonl 33.896s
ok github.com/muratmirgun/yordam/internal/provider/openaicompat 3.238s
ok github.com/muratmirgun/yordam/internal/agent 0.249s
ok github.com/muratmirgun/yordam/internal/app 4.421s
```

Required security/race command:

```text
go test -race ./internal/secret ./internal/evidence ./internal/recovery ./internal/tools/output ./internal/session/jsonl -count=1
ok github.com/muratmirgun/yordam/internal/secret 1.322s
ok github.com/muratmirgun/yordam/internal/evidence 2.003s
ok github.com/muratmirgun/yordam/internal/recovery 1.279s
ok github.com/muratmirgun/yordam/internal/tools/output 1.586s
ok github.com/muratmirgun/yordam/internal/session/jsonl 61.876s
```

Repository verification:

```text
go test ./...
```

Result: exit 0. All packages passed; packages without tests reported `[no test files]`. Notable uncached results included `internal/app 4.921s`, `internal/evidence 1.622s`, `internal/integration 1.492s`, `internal/recovery 0.151s`, `internal/session/jsonl 36.793s`, and `internal/tui 0.344s`.

```text
go vet ./...
```

Result: exit 0 with no output.

```text
gofmt -w <all Task 5 Go files>
git diff --check
```

Result: exit 0 with no output.

## Required security cases

- Digest mismatch: covered for recovery candidate admission and corrupted evidence blobs.
- Missing blob: covered with missing projection, diagnostic, and failed verification.
- Truncation: covered at the 10 MiB boundary with retained content verification.
- Symlink/substitution: covered for evidence blobs and legacy artifact sources.
- Provenance: covered for workspace, session, activity, actor, subject, kind, and media type.
- No-replace races: covered for concurrent immutable evidence metadata publication.
- Encoded-secret zero on disk: covered for evidence, recovery, artifacts, journal records, logs, and model/tool artifacts.
- Streaming boundary: covered for scanners and provider SSE normalization.
- Registry retirement: covered for rejected new producers plus buffered stream lifetime after retirement.
- Legacy alias: covered across store restart and legacy source removal.
