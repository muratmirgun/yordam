# Task 7 Report: Cancellation and Restart Reconciliation

## Delivered

- Added `internal/subagent` reconciliation classification with the requested
  `ReconcileRequest` and `ReconcileResult` boundaries.  It binds the exact
  parent session/cursor, runtime generation, child identity, receipt terminal
  cursor, and receipt digest before a parent can be resumable.
- The restart matrix is fail-closed: missing/commit-unknown/conflicting or
  unproven child state is `uncertain` and is never retried; a proven
  no-effect child becomes `cancelled`; a terminal receipt remains
  non-resumable until the matching parent attachment is present.
- Recovery now projects the parent request/wait and the exact child manifest,
  receipt, and attachment before generic active-turn terminalization.  It
  writes a bounded child recovery receipt before attaching its canonical
  evidence to the parent.  Existing matching attachments remain idempotent
  through the projector's cursor/digest binding.
- Recovery probes unmatched child activities.  If every started activity is
  proven no-effect it produces a cancelled receipt; otherwise the existing
  terminalization path marks the child uncertain rather than replaying it.
- Bootstrap continues to enter recovery through one `RecoverTurn` lane; its
  startup boundary documents that sequential-child reconciliation occurs
  inside that lane before normal turn terminalization, preventing a second
  cleanup worker from racing child provider/process/approval cancellation.

## TDD evidence

- RED: `go test ./internal/subagent -run 'TestSubagentRecover' -count=1`
  initially failed because `Reconcile`, `ReconcileRequest`, and
  `ChildRecoveryState` did not exist.
- GREEN: the same focused test passes after the minimal classification layer.
- Added recovery coverage proving a child without unmatched effects records a
  `cancelled` receipt with the recovery interruption diagnostic.
- Follow-up matrix coverage names the parent-before-create, pre-provider,
  provider, pending-approval, shell, receipt-before-attachment, parent
  reacquire, and commit-unknown cases.  The concrete child provider-stream
  test blocks a real child `RunTurn`, cancels it, verifies the provider context
  stops, exactly one terminal receipt is committed, and the receipt is
  non-retryable `uncertain` (the provider activity crossed its start boundary).
- RED: `TestRecoveryChildInspectionInfrastructureFailureRemainsRetryable`
  returned nil because an arbitrary child-inspection error was classified as
  semantic uncertainty and terminalized the parent. GREEN: the same full
  `RecoverTurn` regression now receives the wrapped sentinel error, observes
  no parent-journal append, and still projects the original active turn.

## Verification

```text
go test ./internal/subagent ./internal/orchestrator ./internal/app -run 'Test.*Subagent.*Recover|Test.*Subagent.*Cancel|TestRecovery' -count=1  PASS
go test ./internal/subagent ./internal/orchestrator ./internal/app -count=1  PASS
go test -race ./internal/subagent ./internal/orchestrator ./internal/app -run 'Test.*Subagent.*Recover|Test.*Subagent.*Cancel' -count=1  PASS
go vet ./internal/subagent ./internal/orchestrator ./internal/app  PASS
git diff --check  PASS
```

Follow-up commits: `5d2d4c8 test: cover child recovery cancellation matrix`;
`9b89c59 test: stop child provider stream on cancellation`.

Existing cancellation coverage exercised alongside this change:

- `TestAppCancelRemovesPendingPermissionsAndEmitsOneTerminalEvent` proves an
  active application cancellation removes the pending approval and rejects the
  stale resolve command.
- `TestSequentialChildCoordinatorCancellationReadsCancelledReceipt` proves the
  child coordinator uses its cancellation-detached receipt read.

Follow-up fixture coverage:

- `TestSubagentCancelDuringChildApprovalRemovesPromptWithoutGrantOrRetry`
  drives a real child `RunTurn` through interactive authorization. Cancellation
  removes its pending child prompt, writes one non-retryable `cancelled`
  receipt, and never resolves, issues, dispatches, resumes, or retries the
  authorization.
- `TestSubagentCancelDuringChildShellProcessStopsProcessAndWritesOneReceipt`
  drives a real child mutation path into an OS `sleep` process. Cancellation
  waits for that process to exit, writes exactly one non-retryable `uncertain`
  receipt, and performs no duplicate execution.
- Bootstrap continues to use the sole `RecoverTurn` entry point; the existing
  recovery ordering places sequential-child reconciliation inside that single
  global lane before generic terminalization. Its terminal command diagnostic
  is explicitly `Retryable: false` when child state cannot be proven.

The full `go test ./... -count=1` invocation was started; this foreground
runner returned only its first successful package line and did not provide a
complete package stream, so it is not claimed as a full-suite completion.

## Review follow-up

- Recovery now uses a managed recovery-lane lease. A proven pre-reserved,
  typed-not-found child identity is created exactly once by the production
  coordinator under `Yield`; inspection errors remain uncertain and are never
  treated as absence.
- A terminal child receipt is attached first, then the parent request, actor,
  turn/task/contract identity, frozen runtime and selected model are rebuilt
  from durable records. Recovery resumes the normal authorized
  provider/tool loop with the exact canonical receipt `ToolResultBlock` rather
  than terminalizing a resumable parent.
- Runtime/model ambiguity and malformed historical bindings fail closed before
  any new provider or tool activity. New provider/tool work remains inside the
  existing managed lane and receives ordinary fresh authorization.
- Added a JSONL regression test proving session inspection returns the typed
  `journal.ErrSessionNotFound` signal required for create-once recovery.

Verification: `go test ./internal/session/jsonl ./internal/subagent
./internal/orchestrator ./internal/app -run 'Test(InspectMissingSessionReturnsTypedNotFound|.*Subagent.*Recover|.*Subagent.*Cancel|Recovery)' -count=1` — PASS;
`git diff --check` — PASS.

- `TestRecoveryReconstructsExactSubagentToolResultContinuation` verifies that
  recovery finds the original assistant subagent tool use by its deterministic
  activity binding and counts the already-terminal provider activity before
  starting the authorized continuation (so the continuation cannot collide
  with the prior provider activity ID).

## Acceptance closure

- `TestSubagentRecoveryCreatesReservedChildOnceAndContinuesParent` now drives
  the production recovery path from a durable parent wait and typed child
  absence. It proves the exact reserved identity is created and run once, the
  durable receipt cursor/digest is attached, and the reconstructed frozen
  provider/model continuation runs only after that attachment. Repeating the
  same recovery command does not re-run repository recovery, child creation,
  child execution, or parent provider continuation.
- A parent whose attached continuation is already terminal returns without
  reconciliation work, preventing a restart from dispatching a second provider
  activity.
- Uncertain/conflicting child reconciliation now persists a structured
  `subagent.recovery_uncertain` diagnostic containing attempt ID, child session
  ID, child status, terminal cursor, receipt digest, and reason. The parent
  command completes with a non-retryable `subagent_recovery_uncertain` public
  error instead of silently degrading to a generic recovery interruption.
- Cancellation after a mutation process starts now stops the provider loop
  after its detached durable uncertain/no-effect terminal write. This closes a
  race where the cancelled child could otherwise enter another provider round
  and report a tool-limit error. The repository test double now preserves
  durable activity identity, so the terminal child receipt is derived as
  `uncertain` from the committed activity event rather than test-local state.

Final verification:

```text
go test ./internal/subagent ./internal/orchestrator ./internal/app -run 'Test.*Subagent.*Recover|Test.*Subagent.*Cancel|TestRecovery' -count=1  PASS
go test ./internal/subagent ./internal/orchestrator ./internal/app -count=1  PASS
go test -race ./internal/subagent ./internal/orchestrator ./internal/app -run 'Test.*Subagent.*Recover|Test.*Subagent.*Cancel|TestRecovery' -count=1  PASS
go test ./internal/orchestrator -run '^TestSubagentCancelDuringChildShellProcessStopsProcessAndWritesOneReceipt$' -count=5  PASS
go test -race ./internal/orchestrator -run '^TestSubagentCancelDuringChildShellProcessStopsProcessAndWritesOneReceipt$' -count=5  PASS
go vet ./internal/subagent ./internal/orchestrator ./internal/app  PASS
git diff --check  PASS
```

Residual risk: bootstrap still enters recovery through the single production
`RecoverTurn` boundary, and the orchestrator tests prove reconciliation occurs
before generic terminalization. There is not yet a real-JSONL application
bootstrap fixture that corrupts a parent tail and observes the structured child
diagnostic through an application subscription; that integration assertion is
left explicit for review rather than represented by a mock-only startup test.

## Final review closure

- Recovered-parent reconstruction and continuation failures no longer escape
  with the parent active. Semantic model/reconstruction failures and provider,
  authorization, or tool continuation failures persist one structured
  `subagent.recovery_uncertain` diagnostic, interrupt the parent with a
  non-retryable `subagent_recovery_uncertain` public error, and let the recovery
  control command complete consistently. The latest continuation cursor is
  carried into terminalization, so a dispatched provider activity remains
  durably `uncertain` instead of being lost behind the earlier attachment head.
- Parent- and child-inspection infrastructure failures remain ordinary wrapped
  recovery errors rather than being misrepresented as semantic child failures.
  Recovery preflights every candidate child before mutation, so a transient
  inspection error leaves all parent and child journals untouched for a safe
  retry. Typed `ErrSessionNotFound` alone proves absence/create-once, while a
  successful but non-writable, incomplete, or `recovery.available` inspection
  remains semantic commit-unknown and terminalizes the parent with the
  structured non-retryable uncertain diagnostic.
- Child inspection success is no longer sufficient proof of a known commit.
  Read-only JSONL views, incomplete transactions, and `recovery.available`
  diagnostics classify the exact child as `uncertain`, are never resumed, and
  persist the child identity, status, terminal cursor, and receipt digest in the
  parent diagnostic.
- Parent reconstruction now selects the exact frozen provider/model pair from
  the durable session selection. Duplicate model IDs across providers cannot
  switch the provider.
- `Reconcile` returns the bounded attempt/child identity and `uncertain` status
  even when binding validation fails, preventing an empty `child_status` in the
  structured diagnostic.

Final review verification:

```text
go test ./internal/subagent ./internal/orchestrator ./internal/app -count=1  PASS
go test ./internal/orchestrator -run 'TestRecovery(ChildInspectionInfrastructureFailure|UnhealthyChildInspection|ParentInspectionInfrastructureFailure)|TestSubagentRecoveryCreatesReservedChildOnceAndContinuesParent' -count=1  PASS
go test -race ./internal/orchestrator -run 'TestRecovery(ChildInspectionInfrastructureFailure|UnhealthyChildInspection|ParentInspectionInfrastructureFailure)|TestSubagentRecoveryCreatesReservedChildOnceAndContinuesParent' -count=1  PASS
go test -json ./... -count=1  PASS
go vet ./internal/subagent ./internal/orchestrator ./internal/app  PASS
git diff --check  PASS
```
