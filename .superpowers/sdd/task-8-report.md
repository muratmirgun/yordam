# Task 8 Report: Production Composition, Child Cards, and Session Navigation

## Delivered

- Added one config-gated immutable `ChildRuntimeView` containing the exact
  child session store, session policy registry, frozen skill catalog, derived
  child-safe exposure, reconciler, generation identity, and runtime lease.
  Disabled runtimes omit this surface. Production protocol-dispatched parent
  turns and ordinary child turns now hold independent leases; retirement waits
  for both.
- Added snapshot-authoritative subagent cards reconstructed from committed
  parent requests and exact child journals. Cards cover requested, waiting,
  running, succeeded, failed, cancelled, and uncertain states with attempt,
  elapsed/timeout, tool budget, bounded redacted summary, changed files, tests,
  and an explicit uncertain-effect warning.
- Application events project subagent journal payloads to stable identity and
  stage only. Child journal stages are fanned out transiently to the selected
  parent without advancing its durable journal cursor; the app then refreshes
  the authoritative durable snapshot. Raw calls and receipts never cross the
  event boundary.
- Added narrow/wide child cards, durable session lineage labels, child open
  navigation (`Alt+Enter`), parent backlink/return (`Alt+Left`), child-card
  selection, child permission lineage preservation, and existing `Esc` parent
  cancellation. A fresh TUI model reconstructs the same cards/navigation from
  the restart snapshot without composing parent messages into child context.

## TDD evidence

- RED: stage event and child-card tests initially failed because public stage,
  card, and component APIs did not exist. GREEN: all lifecycle/layout/security
  assertions pass.
- RED: durable card projection and TUI navigation failed on absent projector
  and `Event.Durable`. GREEN: committed parent/child projection, redaction,
  restart reconstruction, child open, and parent return pass.
- RED: provider activity incorrectly consumed child tool budget (`2/4`).
  GREEN: only committed `activity_planned.kind == "tool"` counts (`1/4`).
- RED: selected-parent subscriptions timed out waiting for child manifest
  stages. GREEN: child stages fan out as identity-only transient events while
  the selected parent cursor remains unchanged.
- RED: nil decoded lifecycle records panicked. GREEN: malformed nil records are
  ignored and redaction-collapsed list values are deduplicated.

## Existing proof reused

- `TestRuntimeSetChildSkillCatalogHandoffIsFrozenAndExact`: active parent and
  child retain the old exact catalog; a future build sees filesystem changes.
- `TestRuntimeGenerationBootstrapBindingAdvancesStoreWhileOldOutputIsImmutable`:
  reload activates only a future generation while the active old producer and
  redactor remain valid until lease release.
- Existing Task 6 permission tests prove child modal session, parent, attempt,
  and displayed call identity and prevent parent auto-shell authority leakage.

## Verification

```text
go test ./internal/app ./internal/tui/... ./internal/session/jsonl -count=1  PASS
go test -race ./internal/app ./internal/tui/... -run 'Test.*Subagent|Test.*Child|TestRuntimeBuilderComposesCompleteImmutableChildRuntimeOnlyWhenEnabled' -count=1  PASS
go test ./... -count=1  PASS
go vet ./internal/app ./internal/tui/... ./internal/session/jsonl ./internal/protocol  PASS
git diff --check  PASS
```

Residual: elapsed time is frozen at the snapshot boundary (or the durable
receipt time for terminal cards); it does not animate between stage/snapshot
refreshes. This keeps reconnect rendering deterministic and avoids a second
transient timer authority.

## Review-fix addendum

- Delegation ordinals are now derived per durable parent turn rather than per
  session. Exact duplicate requests retain their original ordinal, more than
  four distinct requests in one turn fail closed, and later turns restart at
  ordinal one. The existing subagent projector remains the durable protocol
  authority for the per-turn maximum.
- Snapshot provenance now includes a sorted, unique, deep-copied list of exact
  related-child committed cursors. The broker captures the selected parent
  head, derives child identities from that exact prefix, captures every present
  child head, and only then projects. Parent and child card inputs are read with
  paged `ReadRange` calls that stop at the requested committed transaction;
  card projection never calls latest-head `Inspect`.
- A real JSONL/runtime-source adversarial fixture commits a second parent
  request and a child receipt after vector capture. The first snapshot excludes
  both commits and reports the captured parent/child cursors; the following
  snapshot includes both. Subscription cursor advancement preserves and
  deep-copies the captured related-child provenance.
- Receipts now populate public cards only when the full manifest and the child
  journal/session/task/turn/runtime envelope identity match the request. A
  mismatched receipt cannot surface its summary, changed files, commands, or
  tests.
- Child rendering now shows one selected card with an index/total indicator,
  real navigation controls, at most two changed-file and two command/test rows,
  at most twelve card lines, and display-width truncation on every line. A real
  TUI model fixture covers 1,000 history lines, 20 cards, long task/evidence
  values, and narrow/wide windows. Session lineage replacement clears stale
  relation labels.

Review-fix verification:

```text
go test ./internal/app ./internal/protocol ./internal/tui/... ./internal/subagent ./internal/session/jsonl -count=1  PASS
go test -race ./internal/app ./internal/tui/... ./internal/subagent -run 'Test.*(Subagent|Child|Snapshot|ApplicationCursor|Session.*Lineage|Related|InspectionAt|Attempt)' -count=1  PASS
go test ./... -count=1  PASS
go vet ./...  PASS
git diff --check  PASS
```
