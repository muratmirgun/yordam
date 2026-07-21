# Self-hosting Task 2 report

## Scope

Implemented and proved the complete real-PTY self-hosting success scenario: explicit project-skill trust, one sequential child, exact README edit, focused and full verification, compaction, post-compaction continuation, shutdown, and `--continue` recovery.

## Production defects closed

- Deduplicated dual-represented durable tool intents and retained prior tool results across provider continuations.
- Allowed the active turn lane to yield only while an interactive approval is resolved, then reconciled the exact trusted-shell consequence transaction before continuing.
- Bound turn, compaction, child, recovery, and evidence provenance to the real workspace identity instead of conflating workspace and session IDs.
- Preserved exact receipt JSON in the parent provider continuation while keeping receipt evidence identity in canonical storage.

## Acceptance contract covered

- Builds and runs the exact clean source commit in a no-hardlink disposable clone.
- Activates the trusted `go-development` project skill and exposes one depth-one child on the same model.
- Child reads, searches, exact-edits only `README.md`, and passes `git diff --check -- README.md`.
- Parent receives the canonical child receipt and passes `go test ./...`.
- Manual compaction records verified workspace/session-bound evidence and the TUI exposes every stage plus durable range/revision in the context panel.
- A real post-compaction turn proves summary-plus-suffix reconstruction.
- Restart restores the child card, lineage, terminal receipt, compaction state, post-compaction continuation, changed workspace, and exact provider request count without effect retry.
- All pre-compaction journal bytes remain exact prefixes.

## Verification

```text
go test -tags acceptance ./internal/acceptance -run '^TestV030SelfHosting$' -count=1 -v
PASS (166.97s)

go test -race -tags acceptance ./internal/acceptance -run '^TestV030SelfHosting$' -count=1 -v
PASS (166.08s test; 167.508s package)

go test -race ./internal/testsupport/ptyfixture -count=1
go test ./internal/testsupport/ptyfixture ./internal/orchestrator ./internal/app -count=1
go vet ./...
git diff --check
PASS
```

Local evidence is Darwin/arm64 only; Linux is reserved for the CI matrix task and is not claimed here.
