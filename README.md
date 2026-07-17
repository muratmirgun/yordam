# Yordam

Yordam is a small, hackable agent for your terminal. It combines an adaptive terminal UI, OpenAI-compatible model profiles, durable local sessions, and explicit tool permissions.

> [!WARNING]
> Yordam is `v0.x` pre-release software. Interfaces, configuration, and stored session formats may change before `v1.0`; review changes before relying on it for important work.

## Features

- Streaming terminal conversations with persistent, workspace-scoped sessions.
- OpenAI-compatible provider profiles and model switching.
- Bounded `read` and fixed-string `search` tools.
- Atomic text edits with a proposed diff and preimage verification.
- Cancellable shell commands with bounded output and configurable timeouts.
- `safe`, `ask`, and `auto` permission modes.
- Session compaction, a command palette, and an adaptive context panel.

## Security model

Yordam separates file-tool policy from shell execution:

- File tools resolve paths to canonical permission scopes and re-resolve them before execution. Permission decisions distinguish paths inside and outside the current workspace.
- In `safe` mode, only reads and searches inside the workspace are allowed; edits and shell commands are denied.
- In `ask` mode, reads inside the workspace are allowed, while outside reads, edits, and shell commands require visible approval.
- In `auto` mode, file operations inside the workspace can run automatically. Outside file access still asks, and shell use requires a session acknowledgement.

**Shell execution is not sandboxed.** Shell is a trusted, unsandboxed tool that invokes your shell with `-lc`. An approved command can read or write outside the workspace, access the network, and start other programs with your user privileges. Yordam removes `YORDAM_API_KEY` and configured provider-key variables from the child environment, but other environment variables are inherited. Read every command and scope before approving it; use `safe` mode when shell access is not appropriate.

See [SECURITY.md](SECURITY.md) for private vulnerability reporting and the complete trust-boundary summary.

## Install from a GitHub release

The documentation does not assume that a release is currently published. When a release exists:

1. Open the repository's [GitHub Releases](https://github.com/muratmirgun/yordam/releases) page.
2. Download the asset matching your operating system and architecture.
3. Follow any verification instructions attached to that release, then place the `yordam` binary on your `PATH`.

For example, after extracting a verified release asset:

```sh
install -m 0755 ./yordam "$HOME/.local/bin/yordam"
```

If no suitable release asset is published, build from source as described below. Yordam currently supports macOS and Linux.

## Configuration

Yordam always opens its normal conversation TUI. On first run it creates `~/.config/yordam/config.jsonc` on both macOS and Linux. This is the only supported config location; old TOML files are not loaded or migrated.

The generated JSONC template is intentionally incomplete. Replace both `your-model-id` occurrences and `your-provider-api-key`, then run `/reload` in Yordam:

```jsonc
{
  "$schema": "https://raw.githubusercontent.com/muratmirgun/yordam/main/schema/config.json",

  // Format: provider/model
  "model": "openai/your-model-id",
  "provider": {
    "openai": {
      "name": "OpenAI",
      "options": {
        "baseURL": "https://api.openai.com/v1",
        "apiKey": "your-provider-api-key"
      },
      "models": {
        "your-model-id": {"name": "Your model"}
      }
    }
  },
  "limits": {
    "maxToolCalls": 32,
    "shellTimeoutSeconds": 120
  }
}
```

JSONC comments and trailing commas are supported. The schema enables editor validation and autocomplete. Yordam validates locally and does not fetch the schema at runtime.

You can alternatively keep the API key in an environment variable by replacing `apiKey` with:

```jsonc
"apiKeyEnv": "YORDAM_API_KEY"
```

Then start Yordam with the variable set:

```sh
export YORDAM_API_KEY='your-provider-api-key'
yordam
```

`/reload` applies file changes without replacing the current session. Environment variables are inherited when Yordam starts, so setting a missing API key in another terminal requires restarting Yordam.

## Usage

Run `yordam` from the workspace the agent should use:

```sh
cd path/to/project
yordam
```

Each run creates a session by default. Common startup options are:

```text
--continue                 Resume the latest session for this workspace
--session <id>             Resume a specific session ID
--mode safe|ask|auto       Select the initial permission mode (default: ask)
--profile <name>           Select a configured provider profile
--model <name>             Select a model configured for that profile
--base-url <url>           Override the OpenAI-compatible base URL
--data-dir <path>          Use another session data directory
--debug-log <path>         Write an opt-in redacted JSON log
--max-tool-calls <1..128>  Limit completed tool calls per turn
--shell-timeout <1s..30m>  Limit shell command duration
--version                  Print build version information
```

`--continue` and `--session` are mutually exclusive. CLI profile, model, and base URL settings can also be supplied through `YORDAM_PROFILE`, `YORDAM_MODEL`, and `YORDAM_BASE_URL`.

## Commands and keys

| Command | Action |
| --- | --- |
| `/new` | Create a new session. |
| `/sessions` | Filter and open a session for the current workspace. |
| `/mode` | Choose `safe`, `ask`, or `auto`. |
| `/model` | Choose a configured profile and model. |
| `/reload` | Validate and apply `~/.config/yordam/config.jsonc`. |
| `/compact` | Compact older conversation context. |
| `/help` | Show commands and keybindings. |
| `/quit` | Exit, confirming first if a turn is active. |

| Key | Action |
| --- | --- |
| `Ctrl+P` | Open the command palette. |
| `Ctrl+O` | Toggle the context panel. |
| `Esc` | Close the current surface or cancel an active turn. |
| `Ctrl+C` | Exit, confirming first if a turn is active. |
| `y` / `s` / `n` | At a permission prompt, allow once, allow for the exact session scope, or deny. |

## Session locations

Sessions are local JSONL data grouped by a canonical workspace identity:

- macOS: `~/Library/Application Support/yordam/data/workspaces/<workspace-id>/sessions/<session-id>/`
- Linux: `${XDG_DATA_HOME:-$HOME/.local/share}/yordam/workspaces/<workspace-id>/sessions/<session-id>/`
- Override: `--data-dir <path>` stores sessions under `<path>/workspaces/<workspace-id>/sessions/<session-id>/`.

Session directories contain metadata, durable events, and bounded output artifacts. They can include prompts, model responses, tool requests, command summaries, diffs, and tool output. Protect the data directory accordingly. Debug logging is disabled unless `--debug-log` is set.

## Build and test

Yordam's module declares Go 1.26 and toolchain Go 1.26.4.

```sh
go build -o yordam ./cmd/yordam
go test ./...
go test -race ./...
go vet ./...
```

## Roadmap

Current directions, not release commitments:

- Broaden provider and platform testing.
- Strengthen recovery and inspectability for long-running sessions.
- Refine tool permissions and security documentation as the interface evolves.
- Improve packaging after repeatable release automation is in place.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before sending a change. Security reports belong in the private process described in [SECURITY.md](SECURITY.md), not in a public issue.

## License

Yordam is licensed under the [Apache License 2.0](LICENSE).
