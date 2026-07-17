# Yordam v0.1 Design

Status: approved design  
Date: 2026-07-13  
Product and binary name: `yordam`  
Implementation language: Go  
License: Apache-2.0

## 1. Summary

Yordam is a small, hackable, open-source terminal agent. Its core is general-purpose, while its first polished workflow is software development inside a local working directory. The product combines a Pi-like minimal core with selected OpenCode-like capabilities: persistent sessions, readable diffs, runtime model selection, and explicit tool permissions.

Version 0.1 is a single Go process and a single distributable binary for macOS and Linux. It uses an adaptive hybrid TUI: the conversation remains primary, while a contextual panel appears only for diffs, permission requests, errors, or details.

The core is separated from the TUI, model transport, tools, permission policy, and session storage through small internal interfaces. These boundaries must allow a later daemon/client split and out-of-process extensions without rewriting the agent loop or TUI.

## 2. Product principles

1. **Small core, strong boundaries.** The v0.1 implementation stays in one process, but UI, orchestration, and adapters do not reach across their interfaces.
2. **Inspectability over hidden machinery.** Sessions use readable append-only files. Tool requests, permission decisions, mutations, and failures are visible.
3. **Honest security.** Yordam distinguishes path enforcement from process isolation. It never calls a working directory a shell sandbox.
4. **Conversation first.** The main screen is an agent stream, not a permanent IDE dashboard.
5. **Deterministic recovery.** A session can be replayed after interruption without silently repeating a mutation.
6. **Provider portability through one contract.** v0.1 implements one OpenAI-compatible adapter while keeping the provider port independent.
7. **Extensions later, extension boundaries now.** v0.1 defines internal tool and provider contracts but ships no external plugin protocol or package manager.

## 3. Goals

Yordam v0.1 must:

- install as one `yordam` binary on macOS and Linux;
- start in any local directory, whether or not it is a Git repository;
- connect to configured OpenAI-compatible Chat Completions endpoints with streaming and tool calls;
- support multiple named endpoint/model profiles through the same adapter;
- run a multi-turn agent loop with built-in `read`, `search`, `edit`, and `shell` tools;
- support `safe`, `ask`, and `auto` permission modes;
- canonicalize every built-in file-tool path and block workspace escape unless an explicit permission event authorizes the resolved outside scope;
- show an explicit trust warning before unsandboxed shell execution becomes automatic;
- preview edits in `ask` mode and show applied diffs in `auto` mode;
- store, list, resume, and recover local sessions;
- let the user change profile/model and permission mode between turns;
- cancel an active model request or tool process;
- publish reproducible release artifacts under Apache-2.0.

## 4. Non-goals for v0.1

The following are deliberately deferred:

- web search, HTTP fetch, and browser automation tools;
- Windows support;
- a background daemon or remote client;
- a public extension protocol, extension marketplace, or package manager;
- native Go dynamic plugins;
- hard OS-level shell sandboxing;
- session branches or forks;
- SQLite indexing or full-text session search;
- permanent file-tree and multi-pane IDE layouts;
- multiple provider-specific API adapters;
- automatic loading of repository-owned configuration;
- parallel tool execution;
- a headless automation command intended for CI.

## 5. User experience

### 5.1 Startup

- `yordam` opens a new session rooted at the canonical current working directory.
- `yordam --continue` resumes the most recently updated session for that working directory.
- `yordam --session <id>` opens a specific session if it belongs to the current working directory.
- The default permission mode is `ask` unless `--mode` explicitly selects another mode.
- If no usable model profile exists, Yordam opens a first-run setup screen for a profile name, base URL, model name, and API-key environment variable.
- A key may also be entered for the current process. A process-only key is held in memory and is never written to config, events, artifacts, or debug logs.

### 5.2 Primary workflow

1. The user enters a request in the composer.
2. The request is appended to the session log.
3. The agent constructs context and starts a streaming model request.
4. Assistant deltas appear in the conversation.
5. A completed tool call is persisted, authorized, executed, and returned to the model.
6. An edit opens the context panel with a preview or applied diff, depending on mode.
7. The loop continues until the model returns a final response, the user cancels, an unrecoverable error occurs, or the per-turn tool-call limit is reached.
8. The completed response and terminal turn state are persisted.

Only one turn may be active in a session. Additional user submissions are disabled while a turn is active.

### 5.3 TUI layout

The TUI has four regions:

- a top status bar showing session title, permission mode, profile/model, and workspace;
- the primary conversation/tool stream;
- a multi-line composer at the bottom;
- a contextual right panel that appears for diffs, permissions, errors, and expanded tool details.

The context panel is hidden during ordinary conversation. On terminals narrower than 100 columns, contextual content replaces the stream temporarily instead of creating a cramped split. Closing it restores the stream and focus.

### 5.4 Keyboard and commands

Required keyboard behavior:

| Input | Behavior |
|---|---|
| `Enter` | Submit the composer when no modal is open |
| `Ctrl+J` | Insert a newline |
| `Esc` | Close the topmost modal; otherwise close the context panel; otherwise cancel the active turn |
| `Ctrl+P` | Open the command palette |
| `Ctrl+O` | Toggle the context panel |
| `Ctrl+C` | Request application exit; confirm if a turn is active |

Required slash commands:

- `/new` creates a new session for the current workspace;
- `/sessions` opens the session picker;
- `/mode` selects `safe`, `ask`, or `auto`;
- `/model` selects a configured profile/model pair for the next turn;
- `/compact` asks the active model to summarize older context and appends the summary as an event;
- `/help` opens command and keybinding help;
- `/quit` exits after the same active-turn check as `Ctrl+C`.

Mode or model changes during an active turn are queued and become effective only after that turn reaches a terminal state.

`Esc` follows the priority shown in the table. Closing a permission modal resolves the pending request as `deny`; it never leaves a hidden unresolved request.

## 6. Architecture

### 6.1 Process model

Yordam v0.1 runs as one process:

```text
cmd/yordam
    |
    v
app composition root and lifecycle
    |
    +--> Bubble Tea TUI <---- AppCommand / AppEvent ----> agent runtime
    |                                                   |
    |                                                   +--> ModelProvider port
    |                                                   +--> Tool registry
    |                                                   +--> PermissionPolicy port
    |                                                   +--> SessionStore port
    |
    +--> concrete v0.1 adapters
         - OpenAI-compatible provider
         - local read/search/edit/shell tools
         - mode-based permission policy
         - append-only local session store
```

The TUI never calls provider or tool adapters directly. It sends typed commands to the app layer and renders typed events. The agent runtime does not import Bubble Tea or TUI packages.

### 6.2 Package boundaries

The implementation plan should preserve these responsibilities:

```text
cmd/yordam/                     executable entry point
internal/app/                   composition root, lifecycle, command/event bridge
internal/agent/                 turn state machine and context construction
internal/domain/                shared value types and typed events
internal/ports/                 provider, tool, policy, and session interfaces
internal/provider/openaicompat/ Chat Completions streaming adapter
internal/tools/                 built-in tool implementations and registry
internal/permission/            safe/ask/auto policy evaluation
internal/session/jsonl/         event log, metadata, recovery, artifacts
internal/tui/                   Bubble Tea models, views, commands, key bindings
```

Files may be split further by responsibility. Packages must not collapse into a single application model or a global mutable service container.

### 6.3 Core ports

The exact Go syntax may evolve during implementation, but the contracts must preserve these semantics:

```go
type ModelProvider interface {
    Stream(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error)
}

type Tool interface {
    Descriptor() ToolDescriptor
    Execute(ctx context.Context, req ToolRequest) ToolResult
}

type PermissionPolicy interface {
    Evaluate(ctx PermissionContext, req ToolRequest) PermissionDecision
}

type SessionStore interface {
    Create(ctx context.Context, workspace Workspace) (Session, error)
    Append(ctx context.Context, sessionID string, event DurableEvent) error
    Load(ctx context.Context, sessionID string) (SessionReplay, error)
    List(ctx context.Context, workspace Workspace) ([]SessionSummary, error)
}
```

`ModelEvent` includes transient stream deltas and completed model objects. Only events explicitly represented as `DurableEvent` are written to disk.

### 6.4 Concurrency

- Bubble Tea owns UI state on its update loop.
- The app layer owns one cancellable turn context per session.
- Provider streaming happens outside the UI loop and is forwarded as typed events.
- Tool calls execute sequentially in model-provided order.
- Session appends are serialized per session.
- Cancellation propagates through `context.Context` to provider and tool adapters.
- No adapter may mutate TUI state or append session data behind the app layer.

### 6.5 Future migration boundaries

- A daemon/client split replaces the in-memory `AppCommand`/`AppEvent` bridge with a versioned transport. Agent and TUI behavior remain unchanged.
- Out-of-process extensions add proxy implementations of `Tool` and `ModelProvider`. The v0.1 interfaces are internal design boundaries, not a promised public Go API.

## 7. Agent runtime

### 7.1 Turn state machine

A turn moves through these states:

```text
idle -> building_context -> streaming_model
     -> awaiting_permission -> running_tool -> streaming_model
     -> completed | failed | interrupted
```

The app persists meaningful transitions. Streaming text deltas are transient; a completed assistant message is durable.

The default maximum is 32 completed tool calls per turn. Hitting the limit stops the loop with a visible `turn.failed` event and a user-facing explanation. The limit is configurable globally and through a CLI flag, with a minimum of 1 and a maximum of 128.

### 7.2 Context construction

Context is built from:

1. the Yordam system prompt and permission-mode description;
2. the latest durable compaction summary, if present;
3. durable messages and tool results after that summary;
4. current built-in tool descriptors.

Large tool results contribute a bounded excerpt rather than their full artifact. v0.1 does not depend on a provider-specific tokenizer. When a provider reports that context is too large, Yordam surfaces the error and offers `/compact`; it does not silently discard history.

### 7.3 Provider profiles

Yordam implements one OpenAI-compatible Chat Completions adapter with SSE streaming and tool calls. The adapter appends `/chat/completions` to a normalized profile base URL. It is configured through named profiles, so users can switch endpoints and models without adding provider-specific code.

An illustrative global config is:

```toml
active_profile = "primary"

[profiles.primary]
base_url = "https://example.invalid/v1"
api_key_env = "PRIMARY_LLM_API_KEY"
models = ["model-a", "model-b"]
default_model = "model-a"

[profiles.local]
base_url = "http://127.0.0.1:11434/v1"
api_key_env = "LOCAL_LLM_API_KEY"
models = ["local-model"]
default_model = "local-model"
```

The `example.invalid` value is documentation-only and is never generated as a usable profile. First-run setup requires the user to provide a real base URL and model.

Profile resolution order is:

1. CLI flags;
2. `YORDAM_PROFILE`, `YORDAM_MODEL`, `YORDAM_BASE_URL`, and `YORDAM_API_KEY` environment overrides;
3. the selected global profile.

Global config lives under the platform configuration directory:

- macOS: `~/Library/Application Support/yordam/config.toml`;
- Linux: `${XDG_CONFIG_HOME:-~/.config}/yordam/config.toml`.

Repository-owned config is not loaded in v0.1. The config subsystem may reserve a project layer internally, but adding trusted project config is a future feature with a separate security design.

The `/model` picker shows configured profile/model pairs. A change applies to the next model request and is recorded in the session log. Raw API keys must not appear in model requests except in the HTTP authorization mechanism required by the configured endpoint.

## 8. Built-in tools

### 8.1 Shared rules

Every tool descriptor has a stable name, human-readable description, JSON input schema, mutation classification, and scope description. Every tool result includes status, bounded model-facing content, timing, and artifact references when needed.

Tool output limits:

- at most 10 MiB of combined stdout/stderr or equivalent tool content is retained;
- at most 32 KiB is included directly in model context;
- retained content above the context excerpt is written to a session artifact;
- content above 10 MiB is truncated and marked as truncated in both the artifact and event.

The default shell timeout is 120 seconds. A global config value and CLI flag may raise it to at most 30 minutes.

### 8.2 `read`

- Reads a UTF-8 text file within the authorized scope.
- Supports line offset and line limit.
- Rejects directories and files detected as binary.
- Canonicalizes the target before authorization.
- Returns a bounded excerpt with stable line numbers.

### 8.3 `search`

- Searches text under an authorized directory.
- Supports a query, root path, and include/exclude globs.
- Uses `rg` when available and a deterministic Go fallback when it is not.
- Canonicalizes the root before authorization.
- Caps results and reports omitted matches.

### 8.4 `edit`

- Accepts a file path, expected preimage SHA-256, and a structured patch.
- Canonicalizes existing path components and rejects symlink escapes.
- Verifies the preimage hash before applying the patch.
- Applies the patch in memory and produces a unified before/after diff.
- In `ask` mode, persists the intended change and displays its diff before writing.
- Writes through a temporary file in the same directory and atomically renames it over the target.
- Preserves existing file permissions.
- Appends the resulting hashes and diff reference after a successful write.
- Never retries automatically after a write attempt.

Creating a new file is permitted only when its canonical parent is within the authorized scope. Deleting and renaming files are not built-in v0.1 edit operations.

### 8.5 `shell`

- Accepts a command string and working directory.
- Canonicalizes the working directory and displays both command and directory in permission UI.
- Uses the configured shell, defaulting to executable `$SHELL` and falling back to `/bin/sh`, and invokes it with `-lc`.
- Inherits the process environment except `YORDAM_API_KEY` and every environment variable named by a configured profile's `api_key_env`.
- Starts the child in a separate process group where supported.
- Streams bounded output to the TUI and stores large output as an artifact.
- On cancellation, sends graceful termination, waits up to two seconds, then force-kills the remaining process group.
- Records exit code, duration, timeout, cancellation, and truncation.

Setting a child process working directory does not confine its filesystem or network access. v0.1 makes no hard sandbox claim for shell execution.

After a shell command in a Git repository, Yordam refreshes Git status and diff information for display. Outside Git, arbitrary shell mutations cannot be exhaustively detected; the UI must state this limitation.

## 9. Permission model

### 9.1 Mode matrix

| Mode | `read` / `search` | `edit` | `shell` |
|---|---|---|---|
| `safe` | allow inside workspace; deny outside | deny | deny |
| `ask` | allow inside; ask outside | ask with diff preview | ask with command and cwd |
| `auto` | allow inside; ask outside | allow inside; ask outside | allow only after explicit trusted-execution acknowledgement |

The workspace is the canonical current working directory captured when the session is created. Built-in file tools resolve `..`, absolute paths, and symlinks before comparing canonical paths. A lexical prefix check alone is insufficient.

### 9.2 Permission decisions

When a request requires approval, v0.1 offers:

- `allow once`;
- `allow session`;
- `deny`.

A session-scoped allowance is keyed by tool plus normalized scope. For `read` and `search`, the scope is the exact canonical file or search root; for `edit`, it is the exact canonical file; for `shell`, it is the normalized command plus canonical working directory. It never becomes a global policy. Mode changes and permission decisions are durable events.

### 9.3 Auto-mode shell acknowledgement

The first attempted shell call after entering `auto` mode opens a blocking warning that states:

- shell execution is not OS-sandboxed;
- the command may read or write outside the workspace and may access the network;
- built-in file tools enforce canonical scopes and cannot cross them without a visible permission decision;
- accepting enables shell calls for the current session only.

Declining leaves shell requests in `ask` behavior for that session. Accepting is valid only while that session remains in `auto`; leaving `auto` invalidates it, so re-entering `auto` requires a new acknowledgement. The acknowledgement is recorded without recording secrets from the environment and survives resuming a session that never left `auto`.

## 10. Sessions and event storage

### 10.1 Storage location

Durable data lives under:

- macOS: `~/Library/Application Support/yordam/data`;
- Linux: `${XDG_DATA_HOME:-~/.local/share}/yordam`.

The structure is:

```text
workspaces/<full-sha256-of-canonical-cwd>/
  workspace.json
  sessions/<ulid>/
    metadata.json
    events.jsonl
    artifacts/
```

`workspace.json` stores the canonical path so a hash collision or moved directory is never silently accepted. Session IDs are lexicographically sortable ULIDs.

### 10.2 Event envelope

Every durable event is one JSON object on one line:

```json
{
  "schema_version": 1,
  "event_id": "01J...",
  "session_id": "01J...",
  "seq": 42,
  "time": "2026-07-13T12:00:00Z",
  "kind": "tool.result",
  "payload": {}
}
```

`seq` is strictly increasing within a session. The store rejects duplicate or out-of-order appends.

Required event kinds include:

- `session.created`;
- `session.title_changed`;
- `mode.changed`;
- `model.changed`;
- `user.message`;
- `assistant.message`;
- `tool.requested`;
- `permission.requested`;
- `permission.resolved`;
- `tool.started`;
- `tool.result`;
- `file.change_planned`;
- `file.changed`;
- `context.compacted`;
- `turn.completed`;
- `turn.failed`;
- `turn.interrupted`.

### 10.3 Durability and recovery

- In this section, "flushed" means the append has been written and the event file has been synchronized to durable storage. New directories and atomic file replacements also synchronize the parent directory where the operating system supports it.
- A user message is flushed before its model request starts.
- A tool request and `tool.started` event are flushed before execution.
- An intended edit and its preimage hash are flushed before the filesystem mutation.
- A terminal tool result is flushed before another model request starts.
- A completed assistant message is flushed before `turn.completed`.
- A final incomplete JSONL line caused by process termination is copied to a recovery artifact, then removed by truncating `events.jsonl` to its last valid newline before the session becomes writable.
- Invalid JSON or a sequence violation before the final line opens the session read-only and surfaces a corruption error; Yordam does not guess past it.
- Any `tool.started` without a terminal result becomes interrupted during replay and is never re-executed automatically.
- If an interrupted edit has a planned patch, replay compares current hashes and reports whether the planned after-state appears to have been written.
- Transient assistant deltas from an interrupted stream are not reconstructed; replay shows the durable interruption at that point instead.

Resume reconstructs session state by replaying durable events in order. It does not automatically continue an interrupted turn.

### 10.4 Context compaction

`/compact` sends a bounded summarization request using the currently selected profile/model. On success, it appends `context.compacted` with the source sequence range and summary. Old events and artifacts are never deleted by compaction. Future context uses the latest valid summary plus subsequent events.

## 11. Error handling

Errors are typed and converted into durable events when they affect session state. Required categories are:

- `cancelled`;
- `permission_denied`;
- `tool_failed`;
- `tool_timeout`;
- `provider_retryable`;
- `provider_interrupted`;
- `provider_fatal`;
- `context_too_large`;
- `storage_corrupt`;
- `configuration_invalid`.

Provider retry behavior:

- retry connection failures, rate-limit responses, and server failures at most three times with capped exponential backoff and jitter;
- retry only if the stream has emitted no content or tool-call delta;
- once any stream delta has been emitted, persist an interrupted turn and require an explicit user retry;
- never retry a mutating tool call automatically.

A tool failure is returned to the model as a structured result unless the failure makes continued execution unsafe. It does not crash the TUI. Storage errors that prevent durable ordering stop the active turn before further provider or tool activity.

## 12. Security and privacy

- API keys come from named environment variables, CLI environment overrides, or process-memory input.
- Config stores the environment variable name, never the key value.
- Events, artifacts, UI error messages, and debug logs pass through secret redaction.
- HTTP error bodies are bounded and redacted before display or persistence.
- Built-in file tools authorize canonical paths and defend against symlink escape.
- Shell execution is labeled trusted, unsandboxed execution.
- Shell inherits the current process environment except provider-key variables, which are always removed. The permission warning states that other inherited variables may still contain sensitive data.
- Yordam has no telemetry. Its own networking code contacts only configured model endpoints; user- or model-requested shell processes are outside this guarantee.
- Debug logging is opt-in. It omits request authorization headers and redacts configured secret values.

## 13. Testing strategy

### 13.1 Unit tests

Unit coverage focuses on behavior and risk:

- every agent state transition, including limit and cancellation paths;
- the complete permission matrix;
- canonical path, `..`, absolute path, and symlink escape cases;
- patch application, stale preimage, atomic write, and permission preservation;
- JSONL ordering, final-line truncation, corruption, and replay;
- provider SSE parsing and fragmented tool-call assembly;
- output truncation and artifact references;
- secret redaction.

### 13.2 Contract tests

- Provider contract tests use scripted HTTP/SSE fixtures for text, tool calls, rate limits, dropped streams, and invalid payloads.
- Tool contract tests run every built-in tool against temporary workspaces.
- Session contract tests replay golden event logs and assert the same reconstructed state.
- App command/event tests use fake providers, tools, policies, and stores.

### 13.3 Integration tests

Required scenarios include:

- prompt -> read/search -> edit approval -> edit -> shell approval -> final response;
- denial returned to the model without session failure;
- auto-mode acknowledgement and session-scoped behavior;
- cancellation during provider streaming and shell execution;
- stale edit preimage rejection;
- interrupted event log recovery and resume;
- model/profile change taking effect on the next turn;
- Git and non-Git workspaces;
- API keys absent from all persisted output.

### 13.4 TUI and PTY tests

- Bubble Tea update logic is tested with message sequences.
- Golden views cover 80, 120, and 160-column terminals.
- Focus, modal, permission, context-panel, and session-picker flows have keybinding tests.
- PTY tests verify startup, resize, `Esc` cancellation, process-group termination, and clean terminal restoration.

### 13.5 Merge and release gates

Every merge must pass formatting, `go vet`, unit/contract/integration tests, race detection on supported CI runners, and clean macOS/Linux builds.

Every release must additionally pass install-and-run smoke tests on macOS and Linux for `amd64` and `arm64`, verify checksums, and verify that the SBOM matches the release artifacts.

## 14. Distribution and open-source project

- License: Apache-2.0.
- Versioning: semantic versions; the public API remains unstable throughout `0.x`.
- Targets: macOS and Linux on `amd64` and `arm64`.
- Artifacts: one binary per target, checksums, and an SBOM.
- Release automation: a signed version tag triggers CI, tests, builds, artifact verification, and release publication.
- Initial repository documents: `README.md`, `LICENSE`, `CONTRIBUTING.md`, `SECURITY.md`, and `CHANGELOG.md`.
- Security reports use a private contact documented in `SECURITY.md`, not public issues.

The repository path may remain `/Users/murat/oss/tui` locally; the product, command, module identity, release artifacts, and eventual remote repository name use `yordam`.

## 15. Acceptance criteria

Yordam v0.1 is complete only when all of the following are demonstrated:

1. A user installs one release binary on macOS or Linux and runs `yordam` in a fresh directory.
2. First-run setup creates a usable OpenAI-compatible profile without persisting the API key.
3. A scripted model completes a turn using `read`, `search`, `edit`, and `shell`.
4. `ask` mode previews an edit and command, and both allow and deny paths work.
5. `safe` mode blocks edit and shell.
6. `auto` mode blocks unapproved out-of-workspace file-tool access and requires the unsandboxed-shell acknowledgement.
7. An edit produces an exact diff and rejects a stale preimage.
8. Cancelling a shell tool terminates its process group and leaves the terminal usable.
9. Exiting and running `yordam --continue` reconstructs the same completed session.
10. A session with a truncated final event line opens with a visible recovery notice and no silent mutation replay.
11. Changing profile/model affects the next turn and is recorded.
12. Git and non-Git workspaces both function, with the documented shell-diff limitation visible outside Git.
13. macOS/Linux `amd64` and `arm64` release artifacts pass smoke tests.
14. Automated secret-hygiene tests find no configured API key in events, artifacts, errors, or debug logs.

## 16. Planned evolution after v0.1

The intended order is:

1. define a versioned JSON-RPC stdio extension protocol and proxy the existing tool/provider ports;
2. add an extension installer and discovery mechanism only after real extension examples validate the protocol;
3. add web/HTTP tools as optional packages;
4. add session branch/fork and indexed search;
5. add optional file-tree, pinned inspector, and IDE-oriented layouts;
6. evaluate a daemon/client split using the existing command/event boundary;
7. design platform-specific hard sandbox adapters separately for Linux and macOS;
8. evaluate Windows after process, path, and PTY semantics have dedicated tests.

These are directional commitments, not v0.1 requirements.
