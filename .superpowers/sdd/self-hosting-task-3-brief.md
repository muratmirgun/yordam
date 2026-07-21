# Self-hosting/release Task 3 brief

Implement only Task 3, “Combined Failure, Cancellation, and Crash Matrix,” from `docs/superpowers/plans/2026-07-19-yordam-v0.3-self-hosting-release.md` in `/Users/murat/oss/tui-yordam-v0.1/.worktrees/codex-v0.3-compaction`.

## Current baseline

- Branch: `codex/v0.3-compaction`
- Start commit: `2799d1d`
- Task 2 real PTY normal/race acceptance is green.
- Exactly one implementation agent is active. Do not create a reviewer or another subagent.
- Do not begin Tasks 4–7.

## Required scope

- Create `internal/acceptance/v030_failure_matrix_test.go`.
- Modify only the Task 3 acceptance files and owning production packages needed to close a demonstrated integration defect.
- Cover the named plan cases: trust denial; hostile project skills; compaction provider/context/evidence failures; child timeout; Esc cancellation; the specified parent/child crash boundaries; attachment commit-unknown; and changed skill digest on restart.
- Every case must use isolated state, bind exact provider/effect counts, prove user-visible and durable terminal state, and prove no retry of an uncertain mutation.
- Reuse existing deterministic fixtures and production fault/recovery mechanisms. Do not add production-only fault flags, sleeps as synchronization, or broad retry behavior.
- Preserve secret boundaries and parent writability rules.

## Workflow

1. Inventory existing v0.3 acceptance cases and helpers first; reuse/prove them through a named matrix instead of duplicating expensive mechanics where the same production boundary is already exercised.
2. Add the matrix/inventory guard red test.
3. Run the focused normal matrix and close only real production gaps.
4. Run focused normal and race matrix, affected package tests, vet, and diff.
5. Commit owning production fixes separately, then commit the Task 3 acceptance/report.
6. Write `.superpowers/sdd/self-hosting-task-3-report.md` with exact commands/results and update `.superpowers/sdd/progress.md` to Task 3 complete / Task 4 ready.

## Required gates

```text
go test -tags acceptance ./internal/acceptance -run '^TestV030FailureMatrix$' -count=1 -v
go test -race -tags acceptance ./internal/acceptance -run '^TestV030FailureMatrix$' -count=1 -v
go test ./... -count=1
go vet ./...
git diff --check
```

Report exact failures immediately if blocked. No review pass is requested.
