# Self-hosting Task 5 report

## Scope

Added a dedicated bounded macOS/Linux v0.3 acceptance matrix, made the release candidate run the cumulative v0.3 umbrella against the exact checked-out commit before artifact verification/upload, and strengthened native archive smoke checks to bind the packaged version and full commit SHA.

`.goreleaser.yml` did not require a change: the existing configuration already produces the approved Darwin/Linux amd64/arm64 archives, one SPDX document per archive, checksums, and embedded version/commit/date metadata.

## CI and release contracts

- CI has one dedicated `v030-acceptance` job with the exact `ubuntu-24.04` and `macos-15` matrix, full Git history, Go from `go.mod`, a 75-minute job bound, and a 60-minute Go test bound.
- The dedicated job has no provider credential or GitHub secret wiring.
- The release package job checks `HEAD == GITHUB_SHA`, preserves pinned GoReleaser and Syft installation, packages first, runs `TestV030SelfHostedRuntime`, verifies exactly four archives/four SPDX documents/checksums, and only then uploads the immutable candidate.
- Signed-tag verification, the four native smoke runners, checksum/SBOM verification, and the existing verify/package/smoke publication dependencies remain intact.
- `scripts/smoke-release.sh` accepts only a release semver or explicit GoReleaser snapshot version from the archive name and requires `yordam --version` to report that exact version and the supplied full commit SHA.

## TDD evidence

```text
RED:
- CI workflow missing the v030-acceptance job.
- Release package job missing bounded timeout, exact-source guard, and v0.3 umbrella.
- Smoke script missing packaged version/full-commit validation.

GREEN:
go test ./internal/repolint -run 'TestV030AcceptanceCIJobContract|TestV030ReleaseCandidateWorkflowContract|TestReleaseSmokeRequiresPackagedVersionAndCommit' -count=1 -v
go test ./internal/repolint -count=1
PASS
```

The pre-existing release workflow guard still required the v0.1-only command after the new contract passed. It was updated to require the cumulative v0.3 umbrella, which already proves the v0.2-to-v0.1 nesting edge.

## Packaging verification

```text
goreleaser release --snapshot --clean --skip=publish
PASS

Artifacts:
- exactly four non-empty tar.gz archives;
- exactly four non-empty archive SPDX JSON documents;
- checksums.txt contains every archive;
- current native Darwin/arm64 archive passes version + exact commit smoke;
- a deliberately wrong commit is rejected.

go vet ./...
git diff --check
PASS
```

Local native evidence is Darwin/arm64. The dedicated CI matrix owns the corresponding Linux and macOS cumulative acceptance evidence, and the release smoke matrix owns all four native target pairs.
