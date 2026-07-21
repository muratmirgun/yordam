# Task 9 Report: Sequential Subagent Acceptance, Security, and Documentation

## Delivered

- Added a release-facing acceptance matrix plus a real `app.Bootstrap` and
  JSONL fixture. The scripted OpenAI-compatible provider drives parent and
  child independently through child edit, child-specific shell approval and
  focused test, exact receipt continuation, a provider-failure `uncertain`
  attempt, shutdown, restart, durable cards, and TUI projection.
- Added durable ordering and identity assertions for parent request/wait,
  child receipt, parent attachment, unique child/attempt identities, canonical
  authorization, same provider/model, frozen skill exposure, lane ownership,
  limits, cancellation, recovery, and every terminal status.
- Seeded one synthetic configured secret through parent and child prompts,
  provider errors, shell output, and summaries. Raw, encoded, escaped, durable,
  application, debug-log, restart, and TUI surfaces are checked while the
  provider authorization header remains exact.
- Added public README, security-boundary, and release-acceptance documentation
  for the exact sequential/depth-one scope and unsupported capabilities.

## TDD evidence and production gaps

- RED: the first real Bootstrap fixture failed before child creation because
  the canonical orchestration descriptor had no production planning seam.
  GREEN: `PlanOrchestratedAuthorization` now validates the exact catalog marker,
  produces an orchestration authorization plan, and destroys its temporary
  handle so ordinary tool dispatch is impossible.
- RED: child lifecycle fanout reached `LegacyAdapter` as typed transient stage
  data but was decoded as a private legacy payload. GREEN: typed subagent
  lifecycle stages are validated before the legacy envelope path.
- RED: the child edit and shell test succeeded, but its receipt reported no
  changed files. The edit tool already produced structured pre/postimage facts;
  the tooling boundary dropped them and no `file.changed` fact was committed.
  GREEN: only exact canonical built-in edit output can produce the structured
  fact, bound to descriptor revision/digest, classification, plan digest,
  workspace resource, call/path, and lowercase SHA-256 values. The effect and
  terminal share one transaction. Possible post-dispatch file effects map to
  `uncertain`, retain the affected path, and are never inferred from diff text.
- RED fixture bug: JSON encoded literal `\\n` rather than newline and caused an
  edit preview to terminalize uncertain. This was corrected to the canonical
  JSON newline representation before evaluating production behavior.

## Focused verification

```text
go test ./internal/tooling ./internal/orchestrator -run 'TestExecutionResultCarriesOnlyStructuredFileChangeFacts|TestStructuredFileChangedEffectBindsCanonicalEditAndFailsClosed' -count=1  PASS
go test ./internal/acceptance -run '^TestV030Subagent/real_bootstrap_jsonl_edit_test_failure_restart_and_secret_boundaries$' -count=1 -v  PASS (15.82s)
go test ./internal/acceptance -run '^TestV030Subagent$' -count=1 -v  PASS (53.36s)
go test ./internal/repolint -run '^TestV030SubagentDocumentation$' -count=1  PASS
```

The exact-source full/race/vet/diff release evidence is intentionally recorded
only after the source commit and clean gate. It will be added to the release
acceptance document in a child evidence commit.

The first exact-source full race attempt found no data-race report, but the real
E2E fixture exceeded its inherited 45-second event watchdog. A focused race run
with a two-minute watchdog then proved the deeper bound: the fixture's
30-second child manifest deadline interrupted the three-request success child
under instrumentation. Timeout behavior already has focused acceptance tests,
so the composition fixture now uses a 90-second child deadline and a separate
two-minute harness watchdog. This test-only timing fix is committed separately;
the complete clean gate is rerun against that final source.

Focused verification of the timing fix:

```text
go test ./internal/acceptance -run '^TestV030Subagent/real_bootstrap_jsonl_edit_test_failure_restart_and_secret_boundaries$' -count=1 -v  PASS (16.60s)
go test -race ./internal/acceptance -run '^TestV030Subagent/real_bootstrap_jsonl_edit_test_failure_restart_and_secret_boundaries$' -count=1 -v  PASS, no race report (202.16s)
```

## Residual scope

This task does not add concurrent child scheduling, worktree isolation,
cross-host execution, free-form child messaging, or per-child provider/model
selection. A receipt proves only evidence projected from one child attempt; it
does not verify the parent goal.

## Final exact-source release gate

Exact tested source:
`6fb8bf83d55413246e112a76433fd93443421e9b`

```text
go test ./internal/subagent ./internal/orchestrator ./internal/app ./internal/tui/... ./internal/acceptance -count=1  PASS (109.77s)
go test ./... -count=1  PASS (123.12s)
go test -race ./... -count=1  PASS, no race report (568.47s)
go vet ./...  PASS (0.88s)
git diff --check  PASS (0.01s)
```

Host: `go version go1.26.4 darwin/arm64`; `Darwin 27.0.0 arm64`.
Fixture SHA-256:
`3025639c84e32fa887dfaebc69a26be7ec10209fc41b168d3d6ff1178c444ad6`.
Linux was not run locally and is not claimed.
