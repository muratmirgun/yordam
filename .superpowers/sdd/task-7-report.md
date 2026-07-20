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
