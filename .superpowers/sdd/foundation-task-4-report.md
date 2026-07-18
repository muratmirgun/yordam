# Foundation Gate 1 Task 4 Report

## Baseline

- Accepted base: `aee6f6c542653c5308e43095db197319458d61ff`.
- The worktree was clean before implementation.
- `git verify-commit HEAD` reported a good signature from the expected repository signer.

## TDD evidence

The first focused run failed only because the Task 4 APIs and storage members did not exist: workspace control creation, lineage DTOs/composed reads, turn leases, and typed lease errors were undefined. Subsequent focused RED cases demonstrated:

- an empty control journal could not accept its first expected-head append;
- a child bootstrap omitted the inherited nonterminal-turn interruption;
- concurrent legacy initialization observed a stale pre-lock metadata handle;
- an active subprocess writer did not yet reject lock unlink/recreate;
- pure inspection left a missing initialized lock writable.

Each case was rerun GREEN after its minimum production change. No production turn-recovery shortcut was added: the process test acquires the recovery lease, then the storage test fixture commits a typed `turn.interrupted` batch through `journal.Repository`, standing in for the future sole orchestrator committer.

## Delivered contracts

- One workspace-control journal at `workspaces/<canonical-workspace-id>/control/`, with durable mode-`0600` metadata, event, and journal-lock members.
- Immutable `SessionLineage` storage, exact parent-cursor anchoring, composed origin cursors, ancestry-cycle rejection, and a maximum lineage depth of 64.
- One committed child bootstrap transaction containing session creation/fork identity, inherited historical turn interruptions, trust reset, and idle state.
- Nonblocking Unix `flock` primitives for per-journal mutation locks and per-session full-turn leases.
- Durable active-turn inspection from committed typed codec records; ordinary acquisition rejects a nonterminal durable turn, recovery acquisition requires the exact active turn, and normal release requires a terminal event at the supplied committed cursor.
- Create-once legacy lock initialization, concurrent-creator convergence, symlink/missing/inode-substitution fail-closed behavior, and pre-Task4 recovery admission that remains mutation-free until validation/admission succeeds.
- Per-journal in-process and cross-process serialization without holding the legacy process-wide admission lock across journal I/O; unrelated session appends remain concurrent.

## Verification

- Required focused RED command: observed expected compile failures for absent Task 4 contracts.
- Required focused GREEN command: PASS.
- Required focused command with `-count=10`: PASS (`28.296s`).
- Lock initialization/substitution subprocess suite with `-count=10`: PASS (`4.145s`).
- Required race command: PASS with no race report (`4.188s`).
- `go test ./internal/session/jsonl -count=1`: PASS (`27.267s` in the final fresh storage-package checkpoint).
- `go test ./...`: PASS.
- `go vet ./...`: PASS.
- Owned-file `gofmt -l`: no output.
- `git diff --check`: PASS.

The progress ledger was not edited.
