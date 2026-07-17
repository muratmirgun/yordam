# Contributing to Yordam

Thank you for improving Yordam. Keep contributions narrow, testable, and explicit about their security implications.

## Before you start

- Search existing issues and changes before duplicating work.
- Discuss broad design changes before investing in an implementation.
- Use the Go version and toolchain declared in `go.mod`.
- Never commit provider keys, session data, debug logs, or other secrets.
- Report vulnerabilities through the private process in [SECURITY.md](SECURITY.md), not through a public issue.

## Development workflow

Use test-driven development for behavior changes:

1. Add the smallest test that describes the intended behavior.
2. Run it and confirm it fails for the expected reason.
3. Implement the smallest change that makes it pass.
4. Refactor while keeping the focused and full suites green.

Keep one focused change per commit. Avoid mixing drive-by cleanup with the behavior or documentation being changed.

Format changed Go files with `gofmt`:

```sh
gofmt -w path/to/changed.go path/to/changed_test.go
```

## Required checks

Run the focused tests while developing, then run all checks before submitting:

```sh
go test ./...
go test -race ./...
go vet ./...
git diff --check
```

Do not hide or skip a failing check. If a platform-specific check cannot run, describe the reason and the unverified risk in the contribution.

## Security-sensitive changes

Changes to path resolution, permissions, shell execution, environment handling, redaction, session storage, provider requests, or artifact handling require adversarial tests. Preserve these boundaries:

- File-tool decisions use canonical scopes and distinguish inside from outside the workspace.
- Shell is trusted and unsandboxed; documentation and UI must never imply otherwise.
- Provider credentials must not be persisted in configuration or inherited by shell commands.
- Session and debug output must pass through existing redaction and bounded-output paths.

## Documentation and changelog

Update documentation when commands, configuration, storage, security boundaries, or contributor workflows change. Add an entry under `CHANGELOG.md`'s `[Unreleased]` section for every user-visible behavior change, including security-relevant fixes.

## Commit and submission notes

- Write an imperative commit subject that describes the focused change.
- Explain the observed RED failure and the GREEN/full-suite evidence.
- Call out compatibility, migration, and security consequences.
- Keep generated files and unrelated local artifacts out of the commit.
