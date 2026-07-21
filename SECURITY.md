# Security policy

## Supported versions

The `0.1.x` line is the supported version line. Support applies to versions that have actually been published; this policy does not assert that a GitHub release is currently available.

| Version | Supported |
| --- | --- |
| `0.1.x` | Yes |
| Earlier or unreleased development snapshots | No guaranteed support |

## Private vulnerability reporting

Use GitHub's **Private vulnerability reporting** feature for this repository: open the repository's Security tab, choose **Report a vulnerability**, and submit the advisory privately.

**Do not open a public issue** for a suspected vulnerability. Do not include exploit details, credentials, private session data, or affected-user information in public discussions.

Include, when available:

- the affected version, commit, operating system, and architecture;
- the security impact and expected trust boundary;
- minimal reproduction steps or a proof of concept;
- relevant logs with credentials and personal data removed; and
- any mitigation or fix you have already tested.

Maintainers will acknowledge receipt and coordinate validation, remediation, and disclosure through the private advisory. No fixed response-time or resolution-time SLA is promised.

## Security boundaries

Yordam runs with the permissions of the local user who starts it. Model output and tool requests should be treated as untrusted until their effects are understood.

`safe`, `ask`, and `auto` are permission modes, not sandboxes. `safe` denies
mutation and shell, `ask` requires approval for mutation and shell, and `auto`
allows eligible inside-workspace file effects while retaining the explicit
trusted-shell acknowledgement and outside-workspace prompts.

- File tools canonicalize requested paths, classify them as inside or outside the workspace, present exact scopes to the permission policy, and re-resolve before execution.
- `safe` mode denies edits and shell. `ask` mode prompts for edits, outside reads, and shell. `auto` mode permits inside-workspace file operations but still requires acknowledgement before trusted shell execution and prompts for outside file access.
- Shell execution is not OS-sandboxed. Approved commands can access files outside the workspace, use the network, launch processes, and otherwise act with the user's privileges.
- Shell commands do not inherit `YORDAM_API_KEY` or configured provider-key environment variables, but they do inherit other environment variables.
- Configuration stores provider credential environment-variable names with `apiKeyEnv`, not credential values. Set those variables before starting Yordam. Protect `~/.config/yordam/config.jsonc`; Yordam creates its parent directory with mode `0700` and the file with mode `0600`.
- Sessions are persisted locally and can contain prompts, responses, tool metadata, diffs, command summaries, and bounded output artifacts. Debug logs are opt-in and redacted, but users should still protect and review all persisted data.
- Context compaction summaries are derived context aids, not original evidence. Compaction appends derived evidence and never deletes or rewrites the source journal. Treat both the journal and summary evidence as sensitive local session data.
- Configured provider credentials are intentionally sent in the `Authorization` header to the configured provider. They and registered secret values are redacted from model and summary request bodies, stored summary evidence, durable events, application events, TUI output, debug logs, and public errors. Redaction reduces accidental disclosure; it cannot make arbitrary prompt or provider content safe to share.
- Filesystem skills are untrusted context. Only bounded UTF-8 `SKILL.md` files under `~/.config/yordam/skills/<name>/` or `.yordam/skills/<name>/` below the canonical workspace are considered; symlinked roots, directories, and files are rejected. Metadata is visible before an explicit catalog-bound read-only load. Project trust does not grant file, shell, provider, child, or permission authority. Loaded text cannot bypass a normal permission prompt or disclose configured secrets.

Permission prompts reduce accidental execution; they are not a sandbox or a guarantee that a command is safe. Use a separate OS account, container, or virtual machine when stronger isolation is required.

## Sequential subagents

A sequential child is another local turn under the same configured provider,
model, operating-system user, workspace, and immutable runtime generation. It
does not form a new security sandbox. The child receives a depth-one tool
catalog without `subagent`, an attempt-specific session identity, a bounded
deadline and tool budget, and the parent's current permission mode as an upper
authority limit.

The handoff deliberately excludes parent mutable permission grants and the
parent trusted-shell acknowledgement. File and shell effects are planned and
authorized in the child session with exact child/parent/attempt correlation.
The built-in `subagent` marker itself authorizes only local orchestration: it
has no file resource, shell capability, or ordinary tool-dispatch handle.
Provider credentials are used only in the configured provider authorization
header. Registered secret values are redacted from parent and child model
bodies, receipts, durable and application events, TUI output, debug logs, and
public errors.

The parent releases its operation lane only after committing its request and
wait state. The child commits a terminal receipt before the parent can attach
it and continue. Changed files and commands/tests in that receipt come only
from structured durable effect events, not from assistant claims. A receipt is
evidence about the child attempt; it is not proof that the broader parent goal
is correct or complete.

Cancellation is propagated to provider, approval, and shell boundaries. On
restart, an exact committed receipt can be recovered and attached once. If the
journal cannot prove whether a dispatched effect completed, the attempt is
`uncertain`; Yordam does not automatically retry that effect. A later explicit
delegation receives a new child session and attempt identity.

## Explicit capability boundaries

Yordam has no permission-bypass mode. Skills are instruction-only; Yordam does
not execute skills or load skill hooks. Project trust activates an exact catalog
digest but grants no tool authority. Sequential children are depth one, use the
parent's provider and model, and never run in parallel with the parent or another
child. Compaction never rewrites history, and Yordam does not automatically
retry an uncertain effect.
