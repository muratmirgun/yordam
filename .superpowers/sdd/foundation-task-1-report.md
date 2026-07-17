# Foundation Task 1 Implementation Report

## Status

Implemented and verified.

Approved baseline: `b0748bbb728e0c8a9d23b08d6baabffe7ffa4f75`.

## Contract Coverage Matrix

| Contract family | Implementation | Validation and tests |
| --- | --- | --- |
| Opaque IDs, digests, actors, journal references, committed cursors | `internal/protocol/ids.go` | Typed opaque strings, SHA-256 syntax, actor/journal enums, nonzero cursor identity |
| Durable v2 envelope, proposals, decoded records, legacy source, diagnostics, transaction marker, typed lifecycle states | `internal/protocol/events.go` | Envelope v2 identity, session/workspace-control union, payload version, whole-line 2 MiB cap, valid bounded JSON, state-transition terminal immutability |
| All 61 Foundation event kinds and exact v1 payload DTOs | `internal/protocol/payloads.go` | Required fields are checked from raw JSON; semantic validators cover goals, purposes, reasons, criteria, status/transition enums, bindings, sorted IDs, and terminal-kind agreement |
| Provider-neutral model/content/capability/usage protocol | `internal/protocol/model.go` | Content and model-event one-ofs, matching kind/value, capability and usage enums, JSON validity, sorted evidence IDs, string/list bounds |
| Tool identities, descriptors, exposure, resources, action plans | `internal/protocol/tool.go` | Complete identities/plans, sorted unique resources/attributes, schemas, digest syntax and body-digest recomputation |
| Authorization request/decision/approval protocol | `internal/protocol/authorization.go` | Exactly one session/control correlation, actors, resources, action enum, digest bindings, expiry ordering |
| Evidence, checkpoints, receipts, recovery material | `internal/protocol/evidence.go` | Availability/blob/redaction/truncation invariants, sorted IDs/aliases, nonzero digests, canonical body-only digest support |
| Runtime-generation manifest | `internal/protocol/runtime.go` | Manifest body/digest shape and registry recomputation |
| Application command/result/event/snapshot/subscription protocol | `internal/protocol/application.go` | Protocol v1, command 2 MiB cap, event correlation union, application JSON bounds, cancel/subscription one-ofs, deep-copy helpers |
| Canonical JSON profile | `internal/canonicaljson/canonical.go` | UTF-8 byte key ordering, `encoding/json` string escaping, array order, integer-only grammar, duplicate/trailing rejection, depth/string/member/byte bounds |
| Canonical digests | `internal/canonicaljson/canonical.go` | SHA-256 `Digest`, body-only `ValidateDigest`, transaction envelope + newline digest |
| Immutable payload registry | `internal/eventcodec/registry.go` | Duplicate-registration rejection, immutable descriptor metadata, exact raw preservation, strict envelope/payload decoding, required/null checks, unknown kind/version errors |
| Foundation descriptors and semantic validation | `internal/eventcodec/foundation.go` | All 61 `kind@1` descriptors, journal-family rules, envelope/payload identity, all body digests, decision-consumption/start cross-record binding validator |
| Legacy compatibility | `internal/domain/events_test.go` | Confirms the v1 `domain.DurableEvent` wire object remains exactly the original seven keys; production v1 type is unchanged |

## Public APIs Added

- `protocol.EventEnvelope`, `ValidateEnvelope`, `ProposedEvent`, `EventRecord`, `LegacySource`, `CommittedCursor`, `TransactionCommittedV1`, `Diagnostic`, and every requested opaque ID.
- Every provider, content, tool, authorization, evidence, checkpoint, recovery, runtime-generation, command, application-event, snapshot, and subscription DTO listed in the brief.
- All 61 event-kind constants and version-1 payload types.
- Typed `TaskState`, `TurnState`, and `ActivityState` constants and transition validation.
- `protocol.ValidateRawJSON`, `protocol.ValidateBounds`, `protocol.DeepCopy`, `CloneRawMessage`, `CloneEventEnvelope`, `CloneProposedEvent`, `CloneEventRecord`, `CloneCommand`, `CloneApplicationEvent`, and `CloneApplicationSnapshot`.
- `canonicaljson.Marshal`, `Digest`, `ValidateDigest`, and `TransactionDigest`.
- `eventcodec.Descriptor`, `Registry`, `New`, `Decode`, `Validate`, `Descriptor`, `FoundationDescriptors`, `UnknownKindError`, `UnsupportedPayloadVersionError`, and `ValidateAuthorizationConsumption`.

## RED Evidence

### Initial missing-package RED

Exact command:

```text
go test ./internal/canonicaljson ./internal/protocol ./internal/eventcodec -count=1
```

Exact relevant output before any production file existed:

```text
github.com/muratmirgun/yordam/internal/protocol: no non-test Go files in .../internal/protocol
github.com/muratmirgun/yordam/internal/canonicaljson: no non-test Go files in .../internal/canonicaljson
github.com/muratmirgun/yordam/internal/eventcodec: no non-test Go files in .../internal/eventcodec
FAIL github.com/muratmirgun/yordam/internal/canonicaljson [build failed]
FAIL github.com/muratmirgun/yordam/internal/protocol [build failed]
FAIL github.com/muratmirgun/yordam/internal/eventcodec [build failed]
FAIL
```

This proved the canonical JSON, protocol, and codec tests were compiled before their production packages/APIs existed.

### Body digest and cross-record binding RED

Command:

```text
go test ./internal/canonicaljson ./internal/eventcodec -run 'TestValidateDigest|TestFoundationRegistryRejectsMissing|TestValidateAuthorizationConsumption' -count=1
```

Relevant output:

```text
internal/eventcodec/registry_test.go:201:23: undefined: eventcodec.ValidateAuthorizationConsumption
internal/canonicaljson/canonical_test.go:81:26: undefined: canonicaljson.ValidateDigest
FAIL github.com/muratmirgun/yordam/internal/canonicaljson [build failed]
FAIL github.com/muratmirgun/yordam/internal/eventcodec [build failed]
```

### Persisted digest and decoded/envelope identity RED

Command:

```text
go test ./internal/protocol ./internal/eventcodec -run 'TestProtocolOneOf|TestRegistryValidateRejects' -count=1
```

Relevant output:

```text
--- FAIL: TestProtocolOneOfAndContentAvailabilityValidation
    protocol_test.go:91: evidence record without digest accepted
--- FAIL: TestRegistryValidateRejectsDecodedPayloadThatDiffersFromEnvelope
    registry_test.go:200: decoded payload/envelope mismatch accepted
FAIL
```

### Raw JSON versus byte-field bound RED

Command:

```text
go test ./internal/canonicaljson -run TestMarshalDoesNotTreatTopLevelRawJSONAsAByteField -count=1
```

Output:

```text
--- FAIL: TestMarshalDoesNotTreatTopLevelRawJSONAsAByteField
    canonical_test.go:44: byte field exceeds 1048576 bytes
FAIL
```

### Whole-wire bound RED

Command:

```text
go test ./internal/protocol -run TestEnvelopeAndCommandEnforceWholeWireSize -count=1
```

Output:

```text
--- FAIL: TestEnvelopeAndCommandEnforceWholeWireSize
    protocol_test.go:157: event line larger than 2 MiB accepted
FAIL
```

## GREEN Evidence

Initial three-package GREEN after the first coherent implementation slices:

```text
ok github.com/muratmirgun/yordam/internal/canonicaljson 0.370s
ok github.com/muratmirgun/yordam/internal/protocol      0.369s
ok github.com/muratmirgun/yordam/internal/eventcodec    0.652s
```

Audit APIs GREEN:

```text
ok github.com/muratmirgun/yordam/internal/canonicaljson 0.328s
ok github.com/muratmirgun/yordam/internal/eventcodec    0.330s
```

Persisted-digest and decoded/envelope identity GREEN:

```text
ok github.com/muratmirgun/yordam/internal/protocol   0.312s
ok github.com/muratmirgun/yordam/internal/eventcodec 0.313s
```

Large canonical JSON and whole-wire bound GREEN:

```text
ok github.com/muratmirgun/yordam/internal/canonicaljson 0.432s
ok github.com/muratmirgun/yordam/internal/protocol      0.243s
```

## Final Verification

Exact focused gate:

```text
go test ./internal/canonicaljson ./internal/protocol ./internal/eventcodec ./internal/domain -count=1
```

Result:

```text
ok github.com/muratmirgun/yordam/internal/canonicaljson 0.307s
ok github.com/muratmirgun/yordam/internal/protocol      0.137s
ok github.com/muratmirgun/yordam/internal/eventcodec    0.291s
ok github.com/muratmirgun/yordam/internal/domain        0.122s
```

Exact focused race gate:

```text
go test -race ./internal/canonicaljson ./internal/protocol ./internal/eventcodec ./internal/domain -count=1
```

Result:

```text
ok github.com/muratmirgun/yordam/internal/canonicaljson 1.388s
ok github.com/muratmirgun/yordam/internal/protocol      1.309s
ok github.com/muratmirgun/yordam/internal/eventcodec    1.273s
ok github.com/muratmirgun/yordam/internal/domain        1.260s
```

Repository-wide verification:

```text
go test ./...
go test -race ./...
go vet ./...
git diff --check
```

All commands exited 0. The full race run included `internal/app`, `internal/ptytest`, persistence, provider, tools, TUI, and all new protocol packages. `go vet ./...` and `git diff --check` produced no output.

## Validation and Bounds Self-Review

- Event envelopes require schema `2`, nonzero payload version/sequence/transaction identity, valid journal kind/ID, valid time/kind/payload, session ID exactly for session journals, and no session ID for workspace-control journals.
- Registry decoding checks the 2 MiB line cap before JSON work, preserves an independent exact raw copy on every return path, rejects duplicate keys, non-integer/exponent numbers, unknown fields, missing required fields, explicit nulls, trailing values, unknown kinds, and unsupported versions.
- Canonical JSON permits only object, array, string, boolean, null, and grammar `-?(0|[1-9][0-9]*)`; object keys sort by UTF-8 bytes and arrays retain order.
- Canonical/application JSON rejects depth above 64, strings or actual byte-valued DTO fields above 1 MiB, and collections above 4096 members. Event lines and whole commands reject sizes above 2 MiB.
- Top-level raw JSON input is not misclassified as a byte-valued DTO field; this permits an overall JSON document over 1 MiB when every contained field remains within its own limit.
- Content blocks, model events, cancellation, subscription items, authorization requests, authorization consumption, and application correlations enforce their one-of/union contracts.
- Task/turn/activity states are typed; validators reject invalid transitions and transitions out of terminal states. Shared terminal payloads must match their concrete event kind.
- Frozen outcome contracts require at least one required criterion; criterion IDs are unique. Goals, purposes, reasons, and durable purpose actors are nonempty where required.
- Resource targets and resource attributes are sorted and unique. Evidence, receipt, recovery-material, criterion, and other canonical ID lists are checked where order is contractual.
- Available evidence requires a valid blob and cannot be redacted. Withheld-secret evidence requires redaction and no blob. Missing/corrupt evidence cannot carry a blob. Truncation requires a larger original size.
- Body digests are recomputed from only the canonical `Body` for action plans, negotiated provider/context plans, tool descriptors, runtime manifests, checkpoints, evidence records, and receipts. `canonicaljson.ValidateDigest` provides the same body-only contract for recovery-material records.
- `ValidateAuthorizationConsumption` recomputes the committed decision digest and requires exact nonce, event, request, plan, request-input, dispatch, generation, target, call, and started-payload bindings.

## Deep-Copy and Mutable-Alias Self-Review

- `protocol.DeepCopy` recursively creates independent slices, maps, pointers, interfaces, arrays, and exported DTO fields; immutable scalars and `time.Time` values are copied by value.
- All `json.RawMessage`, `[]byte`, typed-ID slices, projection slices, content slices, pointer fields, nested evidence/tool structures, and legacy raw envelopes in the shared DTO graph are covered by the generic copier.
- Explicit clone helpers cover the envelope, proposed event, decoded record, command, application event, and application snapshot boundaries.
- Registry construction clones descriptor metadata; descriptor lookup returns a cloned domain list; decode clones caller raw bytes and the complete returned record. Tests mutate nested raw JSON, actor pointers, and typed-ID slices and confirm the source remains unchanged.

## Files

- Created: `internal/protocol/ids.go`
- Created: `internal/protocol/events.go`
- Created: `internal/protocol/payloads.go`
- Created: `internal/protocol/runtime.go`
- Created: `internal/protocol/model.go`
- Created: `internal/protocol/tool.go`
- Created: `internal/protocol/authorization.go`
- Created: `internal/protocol/evidence.go`
- Created: `internal/protocol/application.go`
- Created: `internal/protocol/protocol_test.go`
- Created: `internal/canonicaljson/canonical.go`
- Created: `internal/canonicaljson/canonical_test.go`
- Created: `internal/eventcodec/registry.go`
- Created: `internal/eventcodec/foundation.go`
- Created: `internal/eventcodec/registry_test.go`
- Modified: `internal/domain/events_test.go` (v1 compatibility assertion only)
- Created: `.superpowers/sdd/foundation-task-1-report.md`

No config/runtime composition, schema, reload, or SDD progress-ledger file was modified. `internal/domain/events.go` remains byte-for-byte unchanged.

## Concerns

- No known Task 1 blocker remains.
- Cross-record authorization binding necessarily requires the referenced committed decision and start payload; Task 1 exposes and tests `eventcodec.ValidateAuthorizationConsumption`. The journal/orchestrator tasks must call this helper when those records become available rather than relying on single-record `Registry.Validate` alone.
