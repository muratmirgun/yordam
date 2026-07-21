#!/usr/bin/env bash
set -euo pipefail

test -z "$(gofmt -l .)"
go test ./...
go test -race ./...
go vet ./...
git diff --check

# The real PTY self-hosting umbrella is a release gate, not part of the fast
# edit loop. Opt in explicitly when validating a v0.3 release candidate.
if [[ "${YORDAM_ACCEPTANCE:-0}" == "1" ]]; then
  go test -tags acceptance ./internal/acceptance -run '^TestV030SelfHostedRuntime$' -count=1 -v
fi
