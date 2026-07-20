# Task 6 report

Implemented session-scoped child permission isolation and durable child receipt projection.

- Added `permission.SessionPolicyRegistry`, keyed by `AuthorizationRequest.SessionID`.
  Child registration starts a new policy with only the inherited mode; session grants,
  authorization grants, and trusted-shell acknowledgement are never copied.
- Wired the registry through bootstrap/runtime authorization and the production
  sequential child coordinator. Child prompts retain child-session, parent-session,
  and delegation-attempt identity, while TUI correlations remain opaque per prompt.
- Changed pending approval bookkeeping to key internal call identity by session and
  call ID, preventing equal child-local call IDs from colliding.
- Added a durable-event receipt projector. It derives changed files, shell commands,
  evidence, provider usage, verification evidence, and unmatched-effect uncertainty
  from the child journal; assistant prose is only a bounded summary.

Verification passed:

```text
go test ./internal/permission ./internal/subagent ./internal/app ./internal/orchestrator -run 'Test.*Child|Test.*Subagent.*Permission|Test.*Receipt' -count=1
go test ./internal/permission ./internal/subagent ./internal/app ./internal/orchestrator -count=1
go test -race ./internal/permission ./internal/app ./internal/orchestrator -run 'Test.*Child|Test.*Subagent' -count=1
go vet ./internal/permission ./internal/subagent ./internal/app ./internal/orchestrator
git diff --check
```

Review follow-up: child shell prompts now always resolve through the exact child
correlation (including a malformed acknowledgement command), never mutate parent
trusted-shell state, and render child/parent/attempt lineage. Receipt projection
now ignores foreign/out-of-cursor records, admits shell commands only after their
activity started and terminalized, forces unmatched effects to `uncertain`, and
recovery emits a projected child receipt when it finds a durable child manifest.
