# Yordam JSONC Configuration and Reload Design

Status: approved design
Date: 2026-07-16

## 1. Summary

Yordam will always open its normal conversation TUI, including on first run and when its configuration is missing or invalid. The first-run setup screen will be removed.

Yordam will use one global configuration file on macOS and Linux:

```text
~/.config/yordam/config.jsonc
```

The file will use an OpenCode-inspired JSONC structure with a root `$schema`, a `provider/model` default model identifier, and provider-specific options and model maps. Users will edit this file with their own editor. The `/reload` command will validate and apply file changes without discarding the current TUI, session, or conversation.

## 2. Goals

- Open the normal conversation TUI on every launch.
- Use `~/.config/yordam/config.jsonc` as the only supported config location.
- Create an editable OpenAI-oriented template when the config file does not exist.
- Support JSON comments and trailing commas.
- Publish a JSON Schema for editor validation and autocomplete.
- Keep API key values out of configuration, events, artifacts, and logs.
- Let users apply configuration file changes with `/reload`.
- Apply reloads atomically without changing an active turn midway.
- Preserve the last working runtime when a reload fails.
- Surface configuration errors inside the normal TUI.

## 3. Non-goals

- Migrating or reading the old TOML configuration.
- Reading the old macOS Application Support configuration location.
- Supporting `--config`, an environment override, project config, or any alternate config path.
- Watching the config file for automatic changes.
- Editing configuration through an in-app form.
- Persisting raw API keys.
- Detecting environment variables exported in another process after Yordam starts.
- Matching every OpenCode configuration field or using OpenCode's schema directly.

## 4. Configuration Location and Lifecycle

`DefaultConfigPath` will derive the home directory with the operating-system home-directory API and append `.config/yordam/config.jsonc`. It will not use the macOS Application Support directory or `XDG_CONFIG_HOME`. This preserves the single-location invariant requested for both supported platforms.

The `--config` CLI option and its public documentation will be removed. A TOML file at any previous location will be ignored without migration, copying, or deletion.

When `config.jsonc` is absent, Yordam will:

1. create `~/.config/yordam` with mode `0700`;
2. atomically create `config.jsonc` with mode `0600`;
3. write the default template defined below;
4. continue into the normal conversation TUI.

Yordam will not overwrite an existing file, including an empty or invalid file. Temporary-file replacement will synchronize the file and parent directory using the existing durability pattern in `config.SaveGlobal`.

## 5. JSONC Format

The generated template will be:

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
        "apiKeyEnv": "YORDAM_API_KEY"
      },
      "models": {
        "your-model-id": {
          "name": "Your model"
        }
      }
    }
  },

  "limits": {
    "maxToolCalls": 32,
    "shellTimeoutSeconds": 120
  }
}
```

The supported structure is intentionally smaller than OpenCode's:

```text
Config
  $schema?: string
  model: string                    // provider/model
  provider: map<string, Provider>
  limits?: Limits

Provider
  name?: string
  options: ProviderOptions
  models: map<string, Model>

ProviderOptions
  baseURL: string
  apiKeyEnv: string

Model
  name?: string

Limits
  maxToolCalls?: integer           // default 32, range 1..128
  shellTimeoutSeconds?: integer    // default 120, range 1..1800
```

Provider IDs and model IDs must be non-empty. `model` must contain exactly one provider/model split with both parts non-empty, and both identifiers must exist in `provider` and its `models` map. `baseURL` must use HTTP or HTTPS, include a host, and contain no user information. `apiKeyEnv` must match `^[A-Z_][A-Z0-9_]*$`.

The generated `your-model-id` value is a reserved setup sentinel, not a usable model ID. Runtime validation and the published schema will reject `openai/your-model-id` as the root model and `your-model-id` as a configured model key. This intentionally leaves the generated template incomplete until the user replaces both occurrences with a real model ID. No provider request can be sent with the placeholder.

Unknown fields will be rejected at every object level. `$schema` is metadata only and will not cause Yordam to fetch remote content at runtime.

The parser will accept standard JSON plus line comments, block comments, and trailing commas. Parse errors should retain line and column information when the parser provides it.

## 6. Published Schema

The repository will contain `schema/config.json`, using JSON Schema Draft 2020-12. It will describe the exact supported subset, set `additionalProperties: false` at every fixed object level, and apply the same ranges and string constraints as runtime validation.

The generated config will reference:

```text
https://raw.githubusercontent.com/muratmirgun/yordam/main/schema/config.json
```

Runtime Go validation remains authoritative. The schema improves editor validation and autocomplete but is not downloaded or evaluated by Yordam.

Tests will use representative valid and invalid documents to keep schema constraints and Go validation aligned.

## 7. Startup Architecture

Startup will separate session/TUI bootstrap from provider runtime construction.

The app will always initialize the following config-independent state:

- canonical workspace;
- data directory and session store;
- selected or newly created session;
- restored permission state;
- command and event channels;
- normal TUI model.

Provider-dependent state will live behind a replaceable runtime binding:

- provider router and clients;
- configured model list and selected default;
- configured API-key environment names;
- secret redactor values;
- shell timeout;
- maximum tool calls.

The binding can be in one of three states:

- `config_invalid`: the file cannot be parsed or validated;
- `credential_missing`: config is valid, but the selected provider key is absent from the process environment;
- `ready`: config and the selected provider credential are usable.

Missing credentials do not prevent normal TUI startup. A provider client may be prepared from valid public configuration, but no model request may start until credential resolution succeeds.

If startup creates a session before a usable model exists, the session may temporarily have an empty model selection. The first successful runtime activation will persist the configured root `model` as `model.changed` before the first model turn. Existing session formats remain readable because an empty selection is a transient pre-turn state, not a fabricated provider/model pair.

## 8. Normal TUI Behavior

The old `FIRST-RUN SETUP` stage and process-only API-key input will be removed.

When Yordam creates a template or starts with invalid configuration, the conversation will show a notice containing:

- the exact config path;
- the current parse, validation, or credential error;
- an instruction to edit the file and run `/reload`;
- for a missing API key, an instruction to export the named variable and restart Yordam.

The composer remains usable. If the user submits a normal message while the runtime is not ready:

- no `user.message` or turn event is persisted;
- no provider request starts;
- the composer text remains available for correction or retry;
- the conversation shows a `configuration_invalid` error with the actionable instruction.

Slash commands that do not require a provider may continue to work. Provider-dependent commands such as `/compact` will return the same configuration error while the runtime is not ready.

## 9. `/reload` Command

`/reload` will be available through direct composer input, the command palette, and the help screen.

Reload is allowed only while no turn or compaction operation is active. If an operation is active, Yordam will reject it with the existing `an operation is already active` notice.

Reload follows a prepare-then-swap transaction:

1. Read `~/.config/yordam/config.jsonc`.
2. Parse JSONC and reject unknown fields.
3. Validate all structural and semantic constraints.
4. Resolve the root default model and all provider API-key environment names.
5. Build an isolated candidate containing provider clients/router, model list, redactor, tool registry and shell filtering, limits, and runtime runner.
6. Classify the candidate as `ready` or `credential_missing` from the environment inherited at process startup.
7. If parsing, validation, or construction fails, discard the candidate and retain the complete previous binding.
8. If construction succeeds, atomically replace the active binding and update the TUI model list and status.

Every turn captures one runtime-binding snapshot when it begins. Reload cannot replace a snapshot used by an active turn, and a later reload cannot alter that turn midway.

Credential lookup reads the environment inherited by the Yordam process. `/reload` cannot observe an `export` performed in another terminal after startup. If the configured key is missing, the user must set it before launching Yordam and restart. Raw key values must never appear in reload events or error messages.

A missing credential does not make `/reload` fail. Structurally valid configuration is activated in `credential_missing` state, and reload reports that the file was loaded but Yordam must be restarted with the named environment variable set. A later normal submission is rejected before persistence or provider activity. This activation may replace an older ready binding because the newly edited file is authoritative; rollback preservation applies to parse, validation, construction, and required-persistence failures, not to missing process credentials.

## 10. Model Selection on Reload

If the current session model still exists in the candidate config, reload preserves it.

If it no longer exists, reload selects the root `model` value and appends a durable `model.changed` event before exposing the new ready binding. If persisting this event fails, the candidate is not activated and the previous binding remains active.

The same fallback applies to a session that had no model because it was created while configuration was invalid.

A successful reload emits a non-terminal app state event that updates the TUI's configured models and current selection, followed by a concise success notice. A failed reload emits a non-terminal configuration error and does not change the displayed active model or runtime state.

## 11. Atomicity and Ownership

The replaceable binding will own all config-sensitive resources together. In particular, provider credentials, the redactor, shell environment filtering, and provider clients must never come from different config generations.

Replacing a binding transfers ownership only after all candidate construction and required durable events succeed. Resources owned by an unsuccessful candidate are closed immediately. Resources owned by the previous binding are closed only after no turn can reference that binding.

Session store, current session identity, replayed conversation, permission grants, and TUI channels are outside the replaceable binding and survive reload unchanged.

## 12. Error Handling

Configuration failures use `configuration_invalid` and include the config path plus a redacted, actionable reason. Categories include:

- file read failure;
- JSONC syntax error;
- unknown field;
- schema-equivalent validation failure;
- invalid provider or model reference;
- missing API-key environment variable;
- candidate provider/runtime construction failure;
- model fallback persistence failure.

A startup configuration failure is non-fatal. A reload configuration failure is non-terminal and leaves the previous runtime usable. Storage failures that prevent required `model.changed` persistence still follow the existing storage-safety rules and prevent activation of the candidate.

## 13. Security and Privacy

- Config stores only an API-key environment-variable name, never its value.
- JSONC comments are treated as potentially sensitive input and are not copied into events or logs.
- Errors are passed through the active or candidate redactor before display or persistence.
- Candidate redaction is established before any candidate provider error can be surfaced.
- Shell child environments remove `YORDAM_API_KEY` and every `apiKeyEnv` named in the active config.
- Config directories and files use modes `0700` and `0600` respectively.
- The `$schema` URL is for the editor only; Yordam performs no schema network request.
- Failed reloads cannot partially update provider keys, redaction, shell filtering, limits, or models.

## 14. Documentation Changes

The README and CLI help will:

- replace TOML examples with the JSONC template;
- document `~/.config/yordam/config.jsonc` as the only location on macOS and Linux;
- remove `--config` and process-only key setup instructions;
- explain first-run template creation;
- document `/reload` and its active-operation restriction;
- explain that environment changes require restarting Yordam;
- state that old TOML files are not migrated or loaded.

The original v0.1 design's first-run setup and platform-specific config-location statements are superseded by this document. Its prohibition on repository-owned config remains unchanged.

## 15. Testing Strategy

### 15.1 Configuration unit tests

- The default path is `$HOME/.config/yordam/config.jsonc` on macOS and Linux and ignores `XDG_CONFIG_HOME`.
- Missing config creates exactly one template with correct directory and file modes.
- Existing config is never overwritten.
- JSON, line comments, block comments, and trailing commas parse correctly.
- Syntax errors retain useful location information.
- Unknown fields at each level are rejected.
- Provider IDs, model IDs, root selection, URLs, environment names, and limits are validated.
- No raw API-key field can be decoded or persisted.
- Representative documents produce equivalent schema and runtime-validation outcomes.

### 15.2 App and runtime tests

- Invalid or missing config still bootstraps session and app state.
- A submission without a ready runtime starts no turn and persists no user message.
- Composer contents survive configuration rejection.
- Successful reload replaces all config-sensitive components together.
- Failed reload preserves the previous provider, model list, redactor, shell filtering, and limits.
- Reload is rejected while a turn or compaction operation is active.
- A turn keeps its original binding snapshot throughout execution.
- Removed current models fall back to root `model` and persist `model.changed`.
- Failure to persist fallback prevents candidate activation.
- Structurally valid reload with a missing credential activates `credential_missing`, requires restart guidance, and exposes no secret value.

### 15.3 TUI and acceptance tests

- First launch displays the normal conversation screen, not setup.
- The generated config path and edit/reload instruction are visible.
- `/reload` appears in the palette and help screen.
- Reload success and failure notices are visible and accurate.
- A message submitted before readiness remains in the composer.
- Editing a valid config and running `/reload` enables a model turn without losing the current session.
- PTY and terminal-restoration tests no longer depend on the setup stage.
- Config, events, artifacts, debug logs, and terminal output remain free of API-key values.

## 16. Acceptance Criteria

1. With no config file, running `yordam` creates `~/.config/yordam/config.jsonc` and opens the normal TUI.
2. The generated file is valid JSONC, references Yordam's schema, and contains no secret value.
3. Yordam reads no alternate config path and exposes no `--config` option.
4. Invalid config does not terminate startup.
5. Submitting a message with invalid config performs no durable turn mutation and preserves the draft.
6. After editing a valid config, `/reload` activates it without replacing the TUI, session, or conversation.
7. A failed reload leaves the last working runtime unchanged.
8. Reload cannot alter an active turn.
9. Model removal falls back durably to the configured root model.
10. API-key values never enter configuration, events, artifacts, debug logs, or user-facing errors.
