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

- File tools canonicalize requested paths, classify them as inside or outside the workspace, present exact scopes to the permission policy, and re-resolve before execution.
- `safe` mode denies edits and shell. `ask` mode prompts for edits, outside reads, and shell. `auto` mode permits inside-workspace file operations but still requires acknowledgement before trusted shell execution and prompts for outside file access.
- Shell execution is not OS-sandboxed. Approved commands can access files outside the workspace, use the network, launch processes, and otherwise act with the user's privileges.
- Shell commands do not inherit `YORDAM_API_KEY` or configured provider-key environment variables, but they do inherit other environment variables.
- Configuration stores provider credential environment-variable names with `apiKeyEnv`, not credential values. Set those variables before starting Yordam. Protect `~/.config/yordam/config.jsonc`; Yordam creates its parent directory with mode `0700` and the file with mode `0600`.
- Sessions are persisted locally and can contain prompts, responses, tool metadata, diffs, command summaries, and bounded output artifacts. Debug logs are opt-in and redacted, but users should still protect and review all persisted data.

Permission prompts reduce accidental execution; they are not a sandbox or a guarantee that a command is safe. Use a separate OS account, container, or virtual machine when stronger isolation is required.
