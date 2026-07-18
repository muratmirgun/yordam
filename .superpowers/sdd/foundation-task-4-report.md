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

## Rejection remediation

The first Task 4 checkpoint was rejected at `4e011874a29451c5ca817cdd1b017d361ee8669b`. Investigation traced the five findings to separate admission and publication defects:

- session staging reconciliation had no cross-process ownership boundary;
- lock identity existed only in process memory and initialization had no durable completion record;
- lease release delegated to a head-only active-turn projection;
- control members were published directly into the final directory one at a time;
- artifact `Put` and `Open` still held the process-wide root lock across filesystem verification and I/O.

Strict regression RED evidence:

- the first combined rejection run failed the live-creator staging gate, missing-turn and missing-both lock-set checks, fresh-subprocess lock replacement, partial final control acceptance, abandoned control staging cleanup, and unrelated artifact progress (`go test` package time `1.598s`);
- after correcting only the release test fixture's compatibility declaration, the exact-cursor test failed because `Release` accepted a later unrelated transaction (`0.448s`);
- the explicit legacy initializer test then failed to compile solely because `FaultLockInitializationAfterJournalSync` and `InitializeJournalLocks` did not exist.

The remediation adds a mode-`0600` per-workspace `flock` boundary around layout initialization and session staging cleanup, atomically publishes a fully synced control staging directory, and validates an existing final control layout before accepting it. Each session now persists `lock-set.json` with the journal identity and the device/inode identities of both `journal.lock` and `turn.lock`; control journals persist the corresponding journal-only set. Inspection, mutation, and lease admission open and validate the complete persisted set. Legacy lock initialization is an explicit, events-locked, idempotent operation that resumes a crash after the journal lock sync but never repairs a persisted initialized set. Process-local inode memory was removed.

`TurnLease.Release` now finds the exact committed transaction named by the supplied cursor and requires a typed terminal event for the leased turn in that transaction. Later unrelated commits do not invalidate an earlier terminal cursor, and a later unrelated cursor cannot release the lease. Artifact `Put`/`Open` now use session-keyed coordination; the root-wide I/O lock was removed, leaving only the short monotonic-ID mutex as process-wide mutex coordination.

Remediation GREEN evidence:

- all original Task 4 and rejection regressions together: PASS (`5.124s`);
- the combined focused subprocess/concurrency group with `-count=10`: PASS (`48.616s`);
- the expanded focused race run: PASS with no race report (`10.564s`);
- `go test ./internal/session/jsonl -count=1`: PASS (`32.426s`);
- final `go test ./...`: PASS (storage package `32.559s` in the repository-wide run);
- `go vet ./...`: PASS;
- owned-file `gofmt -l`: no output;
- `git diff --check`: PASS.

The progress ledger remains untouched.
