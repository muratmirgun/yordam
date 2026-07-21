# Self-hosting Task 4 report

## Scope

Added the cumulative v0.3 regression umbrella, architecture source/dependency gates, repository policy checks, and the opt-in release acceptance branch in `scripts/check.sh`.

## Regression inventory

`TestV030SelfHostedRuntime` now has one exact guarded inventory containing:

- compaction;
- skills;
- sequential subagent;
- real PTY self-hosting;
- the combined failure matrix;
- Foundation (`TestV020Foundation`).

The Foundation entry parses the v0.2 test definition and proves that it invokes `TestV010Acceptance` exactly once, so cumulative v0.1 coverage cannot disappear silently.

## Architecture and repository boundaries

- Journal `AppendBatch` calls are restricted to `internal/orchestrator`; the implementation remains restricted to `internal/session/jsonl`.
- Provider/tool dispatch is restricted to the existing orchestrator, provider-router, and tooling services, including the v0.3 compaction service.
- Production dependencies and direct production imports remain free of `internal/testsupport`.
- Skills, subagent, app, and TUI production packages are checked for executable skill/hook loaders; the repository skill is non-executable.
- The existing repository-wide testdata guard remains active.
- The default check script remains unit/race/vet/diff only. The cumulative v0.3 umbrella runs only when `YORDAM_ACCEPTANCE=1`.

## Production defect closed separately

The full race gate exposed a provider-cancellation classification race: when a provider closed its stream in response to context cancellation, the stream and `ctx.Done()` select cases could both be ready and the close path returned a malformed-stream error. Commit `a631c35` adds a deterministic RED regression and makes the closed-stream path preserve `ctx.Err()` before classifying a missing terminal. The new regression passed normal x1000 and race x100; the original child-provider cancellation test passed race x100 and the complete orchestrator race suite passed.

## Verification

```text
RED umbrella inventory:
FAIL: inventory=[self_hosting_success], wanted [compaction skills subagent self_hosting failure_matrix Foundation]

RED architecture:
FAIL: provider Stream outside dispatch services at internal/orchestrator/compaction.go:113 and :232

RED check-script contract:
FAIL: missing exact YORDAM_ACCEPTANCE=1 branch

go test ./internal/repolint -count=1
PASS

go test -tags acceptance ./internal/acceptance -run '^TestFoundationArchitectureBoundaries$' -count=1 -v
go test -race -tags acceptance ./internal/acceptance -run '^TestFoundationArchitectureBoundaries$' -count=1 -v
PASS

goreleaser release --snapshot --clean --skip=publish
./scripts/check.sh
YORDAM_ACCEPTANCE=1 ./scripts/check.sh
go vet ./...
git diff --check
PASS on the clean exact Task 4 commit
```

Local evidence is Darwin/arm64 only. Task 5 owns the macOS/Linux CI and release-candidate matrix.
