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
