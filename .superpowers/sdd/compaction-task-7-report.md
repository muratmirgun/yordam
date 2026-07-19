# Compaction Task 7 Report

Commit: `35d051b354203db3d033c7b392d4a9b037357981`

## Delivered

- Stable public compaction lifecycle DTOs and legacy-adapter mapping for manual and automatic preparing, summarizing, persisting, completed, cancelled, uncertain, and failed states.
- Context panel metadata and `/compact` guidance with exact committed ranges, revisions, evidence IDs, usage state, and no summary/provider body rendering.
- Durable broker context projection reconstructed from committed context-plan and context-compacted records; transient animation is intentionally excluded.
- Updated context-panel golden fixtures for narrow and split layouts.

## TDD evidence

- Initial RED: `go test ./internal/tui/... ./internal/app -run 'Test.*Compact|Test.*Context' -count=1` failed because the context panel had no compaction hint.
- Lifecycle RED: the same command failed with `activity.planned legacy kind="" want="compaction_started"` and empty cancelled/uncertain mappings.
- Snapshot RED: `go test ./internal/app -run 'TestRuntimeBrokerSnapshotReconstructsDurableCompactionContext' -count=1` failed with every durable context field at its zero value.
- GREEN: `go test ./internal/tui/... ./internal/app ./internal/protocol -count=1` passed; `git diff --check` passed.

## Files

- Task files: `internal/protocol/application.go`, `internal/app/events.go`, `internal/app/legacy_adapter.go`, `internal/tui/components/context.go`, `internal/tui/update.go`, `internal/tui/view.go`, and their focused tests/golden fixture.
- Approved minimal scope expansion: `internal/app/runtime.go`, `internal/app/runtime_support.go`, and `internal/app/runtime_test.go` to reconstruct durable snapshot state from committed history.

## Deviations and risks

- The pre-existing palette already contained the exact `/compact` command and required no behavioral change.
- The broker projection uses the immutable runtime manifest when available; its zero-value compatibility path retains the configured-default automatic policy for direct legacy-source tests.

## Fix Review

Fix commit: `55fef4a6921ad35ecf8bd5011368930f2b066ef7`

- Durable compaction attribution now comes exclusively from the optional validated `compaction_trigger` on the committed activity plan. Legacy command ordering and purpose strings no longer influence it.
- Committed successful activity outcomes carry optional sanitized usage and admitted output-byte facts; the adapter uses those facts for completed UI events.
- Snapshot context policy resolves generation manifests from committed workspace activation history, with an exact-ID runtime bootstrap fallback and fail-closed `unknown_generation` state.
- Terminal and ordinary-turn reducer transitions clear transient progress.
- GREEN: focused app/orchestrator/eventcodec/tui/protocol tests passed, plus `git diff --check`. A `go test ./... -count=1` invocation was started twice; the runner returned only a successful partial package stream, so this environment did not provide a full-suite completion record.

### Group A evidence

Commit: `61fffbedbefe048adf98f79d1d53e0670fe56951`

- `go test ./internal/app -run 'TestApplicationLegacyAdapter.*Compaction|TestApplicationLegacyAdapter.*ContextPlan' -count=1` — PASS
- `go test -race ./internal/app -run 'TestApplicationLegacyAdapter' -count=1` — PASS
- Covers interleaved manual/automatic ActivityIDs with distinct durable usage/byte/range/revision facts, ignores an untagged ordinary provider activity, and proves live context-plan adaptation does not guess policy.

### Group B evidence

Commit: `f73e860db7aeae8ccc93c96b36b81d39277228ea`

- `go test ./internal/tui/... -run 'Test.*Compaction.*(Terminal|Reset|Resume)|Test.*Context.*Resume' -count=1` — PASS
- `go test -race ./internal/tui/... -run 'Test.*Compaction' -count=1` — PASS
- Covers completed/failed/cancelled/uncertain terminal progress clearing, next-turn reset, and durable context resume retaining revision while clearing transient progress.

### Group C evidence

Commit: `a0c5f38cf7794c1ff9ca3f3ccb14ed8fa8aeb0a4`

- `go test ./internal/tui/... -run 'Test.*(Compaction|Context).*' -count=1` — PASS
- `go test -race ./internal/tui/... -run 'Test.*(Compaction|Context).*' -count=1` — PASS
- Covers stable manual/automatic lifecycle labels and all durable policy reason labels with known estimate/window/reserve facts. The setter stores value copies directly so context/progress payloads remain visible without pointer deep-copy loss.

### Group C matrix completion

Commit: `2044d1fafdff3fadeea8c7f89c25dc3843f9c96f`

- Focused and race TUI compaction/context commands — PASS.
- Covers manual and automatic preparing/summarizing/persisting labels; 80-column context-only and 120-column split durable renders with `session-range:17–42`, `revision-7`, `/compact`, and provider/summary sentinel absence.

### Group D evidence

Commit: `f684d33bef554e8a615aa1d7986b5b49f745bf25`

- `go test ./internal/app -run 'TestContextProjection|TestRuntimeBrokerSnapshotReconstructsDurableCompactionContext' -count=1` — PASS
- Matching `-race` command — PASS
- Covers immutable generation limits for disabled/below/threshold/invalid reasons, old-generation precedence, and missing-generation fail-closed reserve state.

### Group D matrix completion

Commit: `13233429e172b8365a9d30132ae62cf8af5b8ecc`

- Focused context projection and matching race commands — PASS.
- Adds unknown-window, default/explicit reserve provenance, exact-ID bootstrap fallback, and sequential durable compaction range/revision/evidence assertions.

### Group E evidence

Commit: `76a696aa0a7fa158d9bf579f6189953a32d33387`

- Requested protocol/eventcodec/app/TUI focused command — PASS.
- Requested app/TUI race subset — PASS.
- Public DTO JSON boundary test asserts summary/provider/prompt/internal-error body keys are absent.

## Fix Review 2

Fix commit: `23e93a7d1581ec31af2535a700a57818659d27fb`

- `App.openSession` is covered through the production `Broker` and `ProtocolService` seam: opening a no-range/unknown session and then a compacted session refreshes exact context facts without cross-session residue.
- Durable snapshots now require a known, ready `context` view, strict valid context data, and return a deep copy. Bootstrap uses non-subscribing `Snapshot` and returns malformed known context errors instead of discarding them.
- The context reducer treats a non-empty policy reason as a complete durable snapshot replacement. Typed plan facts establish plan-partial presence, so an explicit `OutputReserve: 0` overwrites an older value while completion partials retain plan facts.
- The compact subscription race test now has one two-minute sequence deadline; its immediate next turn detects leaked terminal events without a per-operation timing allowance.

### Verification

- RED: `go test ./internal/app ./internal/tui/components -run 'TestDurableContextFailsClosed|TestContextMergesPlanAndCompletionPartialsWithoutRetainingStaleFullState' -count=1` failed for accepted wrong known context and stale output reserve `7` after explicit `0`.
- GREEN focused closure: `go test ./internal/app ./internal/tui/components -run 'TestDurableContextFailsClosed|TestAppOpenSessionRefreshesDurableContextFromNonSubscribingSnapshot|TestBootstrapComposesCanonicalRedactedRuntime|TestContextMergesPlanAndCompletionPartialsWithoutRetainingStaleFullState' -count=1` — PASS.
- Focused packages: `go test ./internal/app ./internal/tui/... ./internal/eventcodec ./internal/protocol ./internal/compaction ./internal/orchestrator -count=1` — PASS.
- Race: `go test -race ./internal/app -run TestAppRunCompactProtocolSubscriptionDoesNotLeakTerminalEvents -count=1` — PASS in `73.348s`, explicit `RACE_EXIT:0`.
- Race context/TUI subset: `go test -race ./internal/app -run 'TestDurableContextFailsClosed|TestAppOpenSessionRefreshesDurableContextFromNonSubscribingSnapshot' -count=1` and `go test -race ./internal/tui/... -run 'Test.*(Context|Compaction).*' -count=1` — PASS.
- `git diff --check` — PASS before the fix commit.
- The desktop foreground runner ended `go test ./... -count=1` after only a partial successful package stream and left no process or exit status; full-suite completion is delegated to the controller and is not claimed here.
