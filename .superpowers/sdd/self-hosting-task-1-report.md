# Self-hosting Task 1 report

## Scope

Implemented the deterministic v0.3 self-hosting fixture only: exact-commit checkout/build isolation, local scripted provider validation, isolated PTY startup, and the repository Go-development skill.

## TDD evidence

- RED: the focused acceptance package failed to compile because `CleanHead`, `CloneExact`, `BuildExact`, `TreeDigest`, `providerStep`, and `newScriptedProvider` did not exist.
- GREEN: the focused harness/provider tests and the PTY fixture/repository lint packages pass after the bounded implementation.

## Contract covered

- Rejects dirty release input and binds clean `HEAD` exactly.
- Creates detached local `--no-hardlinks` build and workspace clones and checks clean status and distinct tracked-file identities.
- Hashes all primary worktree paths and bytes, including untracked content, before and after the fixture.
- Builds with exact version, commit, and commit-date ldflags, then verifies `yordam --version`.
- Keeps HOME, config, data, debug log, temporary files, and provider credential isolated; permits only a loopback HTTP provider.
- Starts the real binary in `auto` mode through the redacting PTY fixture.
- Validates the deterministic parent, child, receipt, full-verification, and compaction request sequence without fallback responses.
- Adds a strict metadata/body `go-development` project skill that never grants authority or embeds executable hooks/secrets.

## Verification

```text
go test -tags acceptance ./internal/acceptance -run 'TestSelfHostHarness|TestScriptedProvider' -count=1
go test ./internal/testsupport/ptyfixture ./internal/repolint -count=1
git diff --check
```
