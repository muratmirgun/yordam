# Self-hosting Task 3 report

## Scope

Added the named v0.3 failure, cancellation, and crash release matrix. The gate inventories every required case, verifies that each referenced production test still exists as one exact top-level test, and runs the smallest deterministic owner tests for each boundary.

## Coverage

- Project trust denial and changed-digest restart remain digest-bound; stale trust cannot activate changed project bytes.
- Malformed, oversized, symlinked, FIFO, and replacement-raced project skills fail closed without leaking rejected content.
- Compaction refusal, context-too-large, evidence failure, orphan evidence, and commit-unknown paths terminalize or remain visibly uncertain without provider resend.
- Child provider cancellation/timeout and Esc shell cancellation commit one conservative receipt and stop the real shell process group.
- Every named parent/child crash boundary combines a real killed subprocess plus durable JSONL turn-lease recovery with the exact subagent reconciliation state: create-once, proven no-effect cancellation, unproven mutation uncertainty, receipt attachment, and exact parent continuation.
- Parent attachment commit-unknown adopts only a proven commit and never resends an unresolved transaction.

The matrix exposed no production defect, so Task 3 changes only acceptance and SDD evidence files. No production-only fault flag, broad retry, or sleep-based acceptance synchronization was added.

## Verification

```text
RED inventory guard:
go test -tags acceptance ./internal/acceptance -run '^TestV030FailureMatrix$' -count=1 -v
FAIL: inventory=[]; wanted the 13 named release cases

go test -tags acceptance ./internal/acceptance -run '^TestV030FailureMatrix$' -count=1 -v
PASS (33.67s test; 34.068s package)

go test -race -tags acceptance ./internal/acceptance -run '^TestV030FailureMatrix$' -count=1 -v
PASS (33.88s test; 35.268s package)

go test ./... -count=1
PASS (acceptance package 115.034s; all packages passed)

go vet ./...
git diff --check
PASS
```

Local process-signal evidence is Darwin/arm64 only. Linux remains reserved for the CI matrix task and is not claimed here.
