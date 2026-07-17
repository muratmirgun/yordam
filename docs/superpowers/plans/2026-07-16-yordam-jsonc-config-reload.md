# Yordam JSONC Configuration and Reload Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace first-run TOML setup with one OpenCode-inspired JSONC file at `~/.config/yordam/config.jsonc`, always open the normal TUI, and atomically apply edits with `/reload`.

**Architecture:** The config package parses JSONC into a strict Go model and publishes the matching Draft 2020-12 schema. Bootstrap always creates workspace/session/TUI state, while provider clients, tools, limits, model choices, credentials, and a fixed redactor are grouped into one replaceable `RuntimeSet`; the app command loop is the sole owner of runtime activation and never swaps a set during an active operation. A thread-safe redactor binding protects the long-lived session store and debug logger while each turn keeps the immutable redactor and runtime snapshot that were active when it started.

**Tech Stack:** Go 1.26.4, Bubble Tea v2, `github.com/tailscale/hujson`, `github.com/santhosh-tekuri/jsonschema/v6` for schema conformance tests, standard `encoding/json`, existing JSONL session store and OpenAI-compatible provider.

## Global Constraints

- The only supported config path is `~/.config/yordam/config.jsonc` on macOS and Linux.
- Ignore `XDG_CONFIG_HOME`, old TOML files, old macOS Application Support config, project config, and alternate path environment variables.
- Remove `--config`; retain existing profile, model, base URL, data directory, debug log, tool-call limit, and shell-timeout overrides.
- Generated config directory mode is `0700`; generated config file mode is `0600`.
- Accept line comments, block comments, and trailing commas; reject unknown fields.
- Generated `your-model-id` is a reserved invalid sentinel until both occurrences are edited.
- `$schema` is editor metadata only; Yordam must not fetch it at runtime.
- API key values come only from the process environment and never enter config, events, artifacts, logs, or user-facing errors.
- `/reload` cannot observe environment changes made in another process; a missing key requires restart.
- Reload must not replace any part of a working runtime when parsing, validation, construction, or required model-event persistence fails.
- A structurally valid config with a missing selected credential is activated as `credential_missing` and replaces the previous runtime.
- Reload is rejected while a turn, compaction, or another reload is active.
- Old persisted sessions and `domain.ModelSelection{Profile, Model}` event payloads remain compatible; “profile” in durable state continues to store the provider ID.

## File Structure

- Create `schema/config.json`: editor-facing Draft 2020-12 schema.
- Rewrite `internal/config/config.go`: strict JSONC document decode into the existing normalized `Config`/`Profile` runtime model, semantic validation, resolution, and model/key helpers.
- Rewrite `internal/config/config_test.go`: JSONC parsing, validation, selection, and fixed-path tests.
- Create `internal/config/schema_test.go`: compile the published schema and compare representative cases with Go validation.
- Rewrite `internal/config/save.go`: first-run template and atomic create-only persistence.
- Rewrite `internal/config/save_test.go`: template contents, permissions, no overwrite, and temporary cleanup.
- Delete `internal/config/save_internal_test.go`: TOML reflection guard is replaced by a JSON model with no raw secret field.
- Modify `internal/config/paths.go`: fixed home-relative JSONC path.
- Modify `internal/cli/options.go` and `internal/cli/options_test.go`: remove `--config` and `ConfigPath`.
- Delete `internal/tui/components/setup.go` and `internal/tui/components/setup_test.go` together with the startup rewrite: remove the separate first-run UI and process-only secret without leaving an intermediate broken build.
- Create `internal/secret/binding.go` and `internal/secret/binding_test.go`: thread-safe replaceable redactor for long-lived sinks.
- Modify `internal/logging/logger.go` and tests: consume a redaction interface rather than a fixed concrete value.
- Modify `internal/tools/output/buffer.go` and tests: consume the same interface while runtime generations pass immutable redactors.
- Create `internal/app/runtime.go` and `internal/app/runtime_test.go`: runtime generation type, readiness checks, and candidate builder.
- Rewrite the composition section of `internal/app/bootstrap.go` and `internal/app/bootstrap_test.go`: config-independent startup and initial candidate activation.
- Modify `internal/app/commands.go`, `internal/app/events.go`, `internal/app/app.go`, and `internal/app/app_test.go`: reload transaction, runtime snapshots, fallback persistence, and pre-turn rejection.
- Modify `internal/tui/run.go` and `internal/tui/run_test.go`: ensure template, remove setup stage, always start normal TUI.
- Modify `internal/tui/model.go`, `internal/tui/update.go`, `internal/tui/view.go`, `internal/tui/keys.go`, `internal/tui/export_test.go`, `internal/tui/commands_test.go`, and `internal/tui/model_test.go`: `/reload`, model-list refresh, startup errors, and draft restoration.
- Modify `internal/tui/components/palette.go` and tests: advertise `/reload`.
- Modify PTY and acceptance tests under `internal/ptytest/` and `internal/acceptance/`: isolated `HOME`, JSONC fixtures, normal first launch, reload, and secret checks.
- Modify `README.md` and `docs/releases/v0.1.0-acceptance.md`: new config and command behavior.
- Modify `go.mod` and `go.sum`: remove BurntSushi TOML and add JSONC/schema dependencies.

Do not create commits while executing this plan unless the user explicitly requests commits. The end of each task uses a diff/status checkpoint instead.

---

### Task 1: Strict JSONC Model and Published Schema

**Files:**
- Create: `schema/config.json`
- Rewrite: `internal/config/config.go`
- Rewrite: `internal/config/config_test.go`
- Create: `internal/config/schema_test.go`
- Rewrite: `internal/config/save.go`
- Rewrite: `internal/config/save_test.go`
- Delete: `internal/config/save_internal_test.go`
- Modify: `internal/ptytest/yordam_test.go`
- Modify: `go.mod`
- Modify: `go.sum`

**Interfaces:**
- Produces: `config.Load(config.LoadOptions) (config.Config, error)`.
- Preserves: the existing normalized `config.Profile`, `config.Config`, and `config.ResolvedProfile` types used by app composition and tests.
- Produces: `config.Config.Resolve(config.ResolveOptions) (config.ResolvedProfile, error)` without treating a missing credential as structural invalidity.
- Produces: `config.Config.Models() []domain.ModelSelection`, `Config.DefaultSelection() domain.ModelSelection`, `Config.ProviderKeyEnvironmentNames() []string`, and `Config.APIKeys() map[string]string`.
- Preserves: `ResolveOptions.Profile`, `Model`, `BaseURL`, `DefaultProfile`, and `DefaultModel` so existing CLI/session precedence remains explicit.

- [ ] **Step 1: Add the JSONC and schema test dependencies**

Run:

```bash
go get github.com/tailscale/hujson@v0.0.0-20260302212456-ecc657c15afd
go get github.com/santhosh-tekuri/jsonschema/v6@v6.0.2
```

Expected: `go.mod` directly requires HuJSON and jsonschema v6. BurntSushi TOML remains until Step 7 because the pre-implementation source still imports it.

- [ ] **Step 2: Write failing JSONC parser and semantic-validation tests**

Replace TOML fixtures in `internal/config/config_test.go` with table-driven JSONC fixtures using this valid base document:

```go
const validConfig = `{
  "$schema": "https://raw.githubusercontent.com/muratmirgun/yordam/main/schema/config.json",
  // comments and trailing commas are accepted
  "model": "primary/model-a",
  "provider": {
    "primary": {
      "name": "Primary",
      "options": {
        "baseURL": "https://llm.example/v1",
        "apiKeyEnv": "PRIMARY_KEY",
      },
      "models": {
        "model-a": {"name": "Model A"},
        "model-b": {},
      },
    },
  },
  "limits": {"maxToolCalls": 32, "shellTimeoutSeconds": 120},
}`
```

Add explicit tests for:

```go
func TestLoadJSONCAndResolveProvider(t *testing.T) {
    path := writeConfig(t, validConfig)
    t.Setenv("PRIMARY_KEY", "secret-value")

    cfg, err := config.Load(config.LoadOptions{ConfigPath: path, LookupEnv: os.LookupEnv})
    if err != nil {
        t.Fatal(err)
    }
    if got := cfg.DefaultSelection(); got != (domain.ModelSelection{Profile: "primary", Model: "model-a"}) {
        t.Fatalf("default selection=%+v", got)
    }
    resolved, err := cfg.Resolve(config.ResolveOptions{Model: "model-b"})
    if err != nil {
        t.Fatal(err)
    }
    if resolved.Name != "primary" || resolved.Model != "model-b" || resolved.BaseURL != "https://llm.example/v1" || resolved.APIKey != "secret-value" {
        t.Fatalf("resolved=%+v", resolved)
    }
    if strings.Contains(string(cfg.RawForTest()), "secret-value") {
        t.Fatal("secret copied into config")
    }
}

func TestLoadReportsJSONCLineAndColumn(t *testing.T) {
    _, err := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, "{\n  \"model\":,\n}")})
    if err == nil || !strings.Contains(err.Error(), "line 2") || !strings.Contains(err.Error(), "column") {
        t.Fatalf("syntax error=%v", err)
    }
}

func TestLoadAcceptsMissingCredential(t *testing.T) {
    cfg, err := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, validConfig), LookupEnv: func(string) (string, bool) { return "", false }})
    if err != nil {
        t.Fatal(err)
    }
    resolved, err := cfg.Resolve(config.ResolveOptions{})
    if err != nil || resolved.APIKey != "" || resolved.APIKeyEnv != "PRIMARY_KEY" {
        t.Fatalf("resolved=%+v err=%v", resolved, err)
    }
}
```

The invalid-case table must assert these exact categories: top-level unknown field, provider unknown field, options unknown field, model unknown field, limits unknown field, absent provider, malformed root model, missing referenced provider, missing referenced model, reserved `your-model-id`, user-info URL, non-HTTP URL, invalid environment name, zero/negative/129 tool-call limit, zero/negative/1801 shell timeout, empty provider ID, and empty model ID.

- [ ] **Step 3: Run config tests and verify the TOML implementation fails**

Run: `go test ./internal/config -run 'TestLoad|TestResolve' -count=1`

Expected: FAIL because TOML decoding cannot parse JSONC and the new fields/methods are undefined.

- [ ] **Step 4: Implement the strict JSONC config types and decoder**

Keep these normalized public runtime types in `internal/config/config.go`:

```go
const SchemaURL = "https://raw.githubusercontent.com/muratmirgun/yordam/main/schema/config.json"

const (
    defaultMaxToolCalls        = 32
    defaultShellTimeoutSeconds = 120
    reservedModelID            = "your-model-id"
)

type Profile struct {
    Name         string
    BaseURL      string
    APIKeyEnv    string
    Models       []string
    DefaultModel string
}

type Config struct {
    ActiveProfile       string
    Profiles            map[string]Profile
    MaxToolCalls        int
    ShellTimeoutSeconds int
    raw                 []byte
    lookupEnv           func(string) (string, bool)
}

type LoadOptions struct {
    ConfigPath string
    LookupEnv  func(string) (string, bool)
}

type ResolveOptions struct {
    Profile        string
    Model          string
    BaseURL        string
    ProcessAPIKey  string // removed with the setup stage in Task 5
    DefaultProfile string
    DefaultModel   string
}

type ResolvedProfile struct {
    Name      string
    Label     string
    BaseURL   string
    APIKey    string
    APIKeyEnv string
    Model     string
}
```

Use private document types only for the on-disk JSONC shape and omitted-value detection:

```go
type document struct {
    Schema   string                      `json:"$schema,omitempty"`
    Model    string                      `json:"model"`
    Provider map[string]documentProvider `json:"provider"`
    Limits   documentLimits              `json:"limits,omitempty"`
}

type documentProvider struct {
    Name    string                    `json:"name,omitempty"`
    Options documentProviderOptions   `json:"options"`
    Models  map[string]documentModel  `json:"models"`
}

type documentProviderOptions struct {
    BaseURL   string `json:"baseURL"`
    APIKeyEnv string `json:"apiKeyEnv"`
}

type documentModel struct {
    Name string `json:"name,omitempty"`
}

type documentLimits struct {
    MaxToolCalls        *int `json:"maxToolCalls,omitempty"`
    ShellTimeoutSeconds *int `json:"shellTimeoutSeconds,omitempty"`
}
```

Implement `Load` by reading bytes, calling `hujson.Parse`, calling `Standardize` to preserve line/column offsets, and decoding a `document` with `json.Decoder.DisallowUnknownFields()`. Decode one value and require EOF. Convert `document.model` into `Config.ActiveProfile` plus the matching profile's `DefaultModel`; convert each model-map key into a sorted `Profile.Models` slice. Apply defaults only when the pointer limit field is nil, so explicit zero remains invalid. Then call `Validate`.

Implement `DefaultSelection` from `ActiveProfile` and that profile's `DefaultModel`. Implement `Resolve` precedence as CLI option, `YORDAM_PROFILE`/`YORDAM_MODEL`/`YORDAM_BASE_URL`, resumed-session defaults, then normalized config defaults; only the selected provider may use `YORDAM_API_KEY`. Retain `ProcessAPIKey` precedence temporarily so the still-compiling setup path works until Task 5 removes it. Missing keys produce `APIKey == ""`, not an error.

Implement deterministic helpers:

```go
func (c Config) Models() []domain.ModelSelection
func (c Config) ProviderKeyEnvironmentNames() []string
func (c Config) APIKeys() map[string]string
func (c Config) RawForTest() []byte
```

Sort provider IDs and model IDs in returned slices. `APIKeys` reads each provider's configured environment name. The runtime builder replaces the selected provider's map value with `ResolvedProfile.APIKey`, which is where selected-provider CLI/environment credential precedence is applied.

- [ ] **Step 5: Rewrite `SaveGlobal` as strict JSON output while preserving its temporary internal API**

Convert normalized `Config` back into the OpenCode-inspired document shape, marshal it with indentation, and keep the existing atomic replace/sync behavior and `0600` mode. This temporary API keeps setup and test fixtures compiling until Task 5 removes setup; it does not create a second product config location because no CLI path is added.

Update `save_test.go` to assert JSON field names (`model`, `provider`, `baseURL`, `apiKeyEnv`, `models`, `limits`), round-trip through `Load`, atomic non-replacement on invalid config, abandoned-temporary cleanup, and absence of the environment's actual key value. Delete the TOML-reflection-only `save_internal_test.go`; strict decode tests now reject raw `apiKey` fields.

- [ ] **Step 6: Create the exact published Draft 2020-12 schema**

Create `schema/config.json` with this structure:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://raw.githubusercontent.com/muratmirgun/yordam/main/schema/config.json",
  "title": "Yordam configuration",
  "type": "object",
  "required": ["model", "provider"],
  "additionalProperties": false,
  "properties": {
    "$schema": {"type": "string"},
    "model": {
      "type": "string",
      "pattern": "^[^/]+/[^/]+$",
      "not": {"pattern": "/your-model-id$"},
      "description": "Default model in provider/model format."
    },
    "provider": {
      "type": "object",
      "minProperties": 1,
      "propertyNames": {"minLength": 1},
      "additionalProperties": {"$ref": "#/$defs/provider"}
    },
    "limits": {"$ref": "#/$defs/limits"}
  },
  "$defs": {
    "provider": {
      "type": "object",
      "required": ["options", "models"],
      "additionalProperties": false,
      "properties": {
        "name": {"type": "string"},
        "options": {"$ref": "#/$defs/providerOptions"},
        "models": {
          "type": "object",
          "minProperties": 1,
          "propertyNames": {
            "allOf": [
              {"minLength": 1},
              {"not": {"const": "your-model-id"}}
            ]
          },
          "additionalProperties": {"$ref": "#/$defs/model"}
        }
      }
    },
    "providerOptions": {
      "type": "object",
      "required": ["baseURL", "apiKeyEnv"],
      "additionalProperties": false,
      "properties": {
        "baseURL": {"type": "string", "format": "uri", "pattern": "^https?://"},
        "apiKeyEnv": {"type": "string", "pattern": "^[A-Z_][A-Z0-9_]*$"}
      }
    },
    "model": {
      "type": "object",
      "additionalProperties": false,
      "properties": {"name": {"type": "string"}}
    },
    "limits": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "maxToolCalls": {"type": "integer", "minimum": 1, "maximum": 128, "default": 32},
        "shellTimeoutSeconds": {"type": "integer", "minimum": 1, "maximum": 1800, "default": 120}
      }
    }
  }
}
```

- [ ] **Step 7: Add schema/runtime conformance tests**

In `internal/config/schema_test.go`, load `../../schema/config.json`, compile it with `jsonschema.NewCompiler()`, call `AssertFormat()`, standardize each JSONC fixture with HuJSON, decode to `any`, then compare schema validity with `config.Load` validity.

Use a table containing: `validConfig`, every unknown-field location, both limit boundaries, both out-of-range limits, invalid URL, invalid environment name, malformed root model, reserved sentinel, and a root model referencing an absent provider/model. For cross-reference cases the schema validates shape but cannot prove map membership; mark those cases `schemaValid: true, runtimeValid: false` and assert that difference explicitly rather than weakening runtime validation.

- [ ] **Step 8: Remove TOML and run config plus repository tests**

Before the commands, rewrite the existing `internal/ptytest/yordam_test.go` `writeConfig` helper to emit the Task 1 JSONC provider/model structure and update its persisted setup assertion from `api_key_env` to `apiKeyEnv`, while retaining the temporary `--config` test argument and existing setup-screen expectations. This is only an intermediate fixture conversion; Task 7 removes the override and setup expectations.

Run:

```bash
go mod edit -droprequire github.com/BurntSushi/toml
go mod tidy
go test ./internal/config -count=1
go test ./...
```

Expected: BurntSushi TOML is absent from `go.mod`/`go.sum`, `go mod tidy` exits 0, and both test commands PASS. Update all config serialization assertions in `internal/config`, `internal/app/bootstrap_test.go`, and `internal/acceptance` from TOML field spelling to the JSONC field names defined in this task; keep the setup screen itself until Task 5.

- [ ] **Step 9: Inspect the JSONC model checkpoint**

```bash
git status --short
git diff --check
git diff -- go.mod go.sum schema/config.json internal/config internal/ptytest/yordam_test.go
```

Expected: only intended config/schema/dependency changes are present and `git diff --check` exits 0.

---

### Task 2: Fixed Global Path and First-Run Template

**Files:**
- Modify: `internal/config/paths.go`
- Modify: `internal/config/save.go`
- Modify: `internal/config/save_test.go`

**Interfaces:**
- Consumes: `config.SchemaURL` from Task 1.
- Produces: `config.DefaultConfigPath() (string, error)` returning `$HOME/.config/yordam/config.jsonc`.
- Produces: `config.EnsureGlobal() (path string, created bool, err error)` with create-only semantics.

- [ ] **Step 1: Write failing fixed-path and template tests**

Update `TestDefaultPaths` to set `HOME` and `XDG_CONFIG_HOME` to different directories and assert:

```go
wantConfig := filepath.Join(home, ".config", "yordam", "config.jsonc")
```

Keep existing platform-specific data-directory assertions unchanged.

Replace `save_test.go` with tests that call `EnsureGlobal()` after setting `HOME` and assert:

```go
path == filepath.Join(home, ".config", "yordam", "config.jsonc")
created == true
directory mode == 0o700
file mode == 0o600
strings.Contains(raw, `"$schema": "`+config.SchemaURL+`"`)
strings.Contains(raw, `"model": "openai/your-model-id"`)
!strings.Contains(raw, "actual-secret")
```

Call `EnsureGlobal()` again after replacing the file contents with `unchanged`; assert `created == false` and the bytes remain exactly `unchanged`. Retain the abandoned `.config-*.tmp` cleanup case.

- [ ] **Step 2: Run focused tests and verify failure**

Run: `go test ./internal/config -run 'TestDefaultPaths|TestEnsureGlobal' -count=1`

Expected: FAIL because the old path and TOML writer still exist.

- [ ] **Step 3: Implement fixed-path resolution and atomic template creation**

Implement `DefaultConfigPath` as:

```go
func DefaultConfigPath() (string, error) {
    if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
        return "", fmt.Errorf("unsupported operating system %s", runtime.GOOS)
    }
    home, err := os.UserHomeDir()
    if err != nil {
        return "", err
    }
    return filepath.Join(home, ".config", "yordam", "config.jsonc"), nil
}
```

In `save.go`, add a `defaultTemplate` byte slice containing the approved JSONC example. `EnsureGlobal` must check the target with `os.Lstat`, return without writing when it exists, create the directory and enforce mode `0700`, remove only `.config-*.tmp` regular files, create a same-directory temporary with `0600`, write/sync/close it, then install it without replacing a concurrently created target. Use `os.Link(temporaryPath, path)` followed by temporary removal: hard linking is supported on the target macOS/Linux filesystems and fails with `os.ErrExist` rather than replacing a concurrent winner. Sync the directory after installation or cleanup. Do not use `os.Rename` alone because it would overwrite a concurrently created config.

- [ ] **Step 4: Run config tests**

Run: `go test ./internal/config -count=1`

Expected: PASS.

- [ ] **Step 5: Inspect the fixed-path/template checkpoint**

```bash
git status --short
git diff --check
git diff -- internal/config/paths.go internal/config/save.go internal/config/save_test.go
```

Expected: only intended fixed-path/template changes are present and `git diff --check` exits 0.

---

### Task 3: Replaceable Redaction for Long-Lived Sinks

**Files:**
- Create: `internal/secret/binding.go`
- Create: `internal/secret/binding_test.go`
- Modify: `internal/logging/logger.go`
- Modify: `internal/logging/logger_test.go`
- Modify: `internal/tools/output/buffer.go`
- Modify: `internal/tools/output/buffer_test.go`

**Interfaces:**
- Produces: `secret.Redacting` interface with `String`, `Bytes`, and `JSON`.
- Produces: `secret.Binding` with `Replace(secret.Redactor)` and `Snapshot() secret.Redactor`.
- Long-lived JSONL sanitization and logging consume `*secret.Binding`; generation-specific tools consume `secret.Binding.Snapshot()`.

- [ ] **Step 1: Write failing atomic-binding tests**

Create tests that instantiate `binding := secret.NewBinding(secret.New("old-secret"))`, assert all three redaction methods hide the old value, call `binding.Replace(secret.New("new-secret"))`, and assert the new value is hidden. Capture `snapshot := binding.Snapshot()` before replacement and assert it continues to hide only `old-secret` afterward.

Add a concurrency test with 100 goroutines repeatedly calling `String`, `Bytes`, and `JSON` while another goroutine alternates two redactors; run it under `go test -race`.

- [ ] **Step 2: Run secret tests and verify missing API failure**

Run: `go test ./internal/secret -run TestBinding -count=1`

Expected: FAIL because `NewBinding` does not exist.

- [ ] **Step 3: Implement the redaction interface and binding**

Create `internal/secret/binding.go`:

```go
package secret

import (
    "encoding/json"
    "sync"
)

type Redacting interface {
    String(string) string
    Bytes([]byte) []byte
    JSON(any) (json.RawMessage, error)
}

type Binding struct {
    mu      sync.RWMutex
    current Redactor
}

func NewBinding(initial Redactor) *Binding { return &Binding{current: initial} }

func (b *Binding) Replace(next Redactor) {
    b.mu.Lock()
    b.current = next
    b.mu.Unlock()
}

func (b *Binding) Snapshot() Redactor {
    b.mu.RLock()
    defer b.mu.RUnlock()
    return b.current
}

func (b *Binding) String(value string) string { return b.Snapshot().String(value) }
func (b *Binding) Bytes(value []byte) []byte  { return b.Snapshot().Bytes(value) }
func (b *Binding) JSON(value any) (json.RawMessage, error) {
    return b.Snapshot().JSON(value)
}
```

Change `logging.Logger.redactor`, `logging.New`, and `output.Options.Redact` from concrete `secret.Redactor` to `secret.Redacting`. In `output.New`, replace a nil redactor with `secret.New()` so zero-value test options remain safe. Production logger and JSONL store receive the dynamic binding; runtime tools receive only `binding.Snapshot()` from their generation.

- [ ] **Step 4: Add logger/output replacement tests**

Pass one binding to a logger, log old-secret content, replace the binding, then log new-secret content; assert each log write is redacted by the binding active at that call. For output buffers, capture snapshot A before replacement and snapshot B after replacement, pass each snapshot to a separate buffer, and prove buffer A continues to redact only A while buffer B redacts only B. Never pass the dynamic binding to a production output buffer because buffered raw bytes must stay paired with one immutable secret generation.

- [ ] **Step 5: Run race-enabled redaction tests**

Run: `go test -race ./internal/secret ./internal/logging ./internal/tools/output -count=1`

Expected: PASS with no race report.

- [ ] **Step 6: Inspect the redactor-binding checkpoint**

```bash
git status --short
git diff --check
git diff -- internal/secret internal/logging internal/tools/output
```

Expected: only intended redaction changes are present and `git diff --check` exits 0.

---

### Task 4: Runtime Generation Builder

**Files:**
- Create: `internal/app/runtime.go`
- Create: `internal/app/runtime_test.go`
- Modify: `internal/app/bootstrap.go:266-385`

**Interfaces:**
- Consumes: Task 1 `config.Config` helpers and Task 3 `secret.Redactor` snapshots.
- Produces: `app.RuntimeSet` and `app.ReloadRuntime` used by the app loop in Task 5.
- Produces: one candidate containing runner, compactor, model list, default selection, credentials, config-sensitive tools, and immutable redactor.

- [ ] **Step 1: Write failing runtime-set readiness tests**

Define tests for a set with models `primary/a` and `secondary/b`, credentials only for primary, and assert:

```go
set.Ready(domain.ModelSelection{Profile: "primary", Model: "a"}) == nil
set.Ready(domain.ModelSelection{Profile: "secondary", Model: "b"})
// typed configuration_invalid naming SECONDARY_KEY and restart
set.Ready(domain.ModelSelection{Profile: "missing", Model: "x"})
// typed configuration_invalid naming the unconfigured provider/model
```

Build a candidate from a two-provider config and fake HTTP servers. Assert deterministic models, root default selection, per-provider Authorization headers, shell key-environment list, max tool calls, and timeout. Ensure the key values do not appear in errors or a serialized candidate summary.

- [ ] **Step 2: Run runtime tests and verify missing type failure**

Run: `go test ./internal/app -run 'TestRuntimeSet|TestBuildRuntime' -count=1`

Expected: FAIL because `RuntimeSet` and the builder do not exist.

- [ ] **Step 3: Define the runtime generation interfaces**

Create `internal/app/runtime.go` with:

```go
type RuntimeSet struct {
    Runtime          Runtime
    CompactSession   CompactSession
    Models           []domain.ModelSelection
    DefaultSelection domain.ModelSelection
    CredentialEnvs   map[string]string
    Credentials      map[string]string
    Redactor         secret.Redactor
    ConfigurationError error
}

type ReloadRuntime func(context.Context, domain.ModelSelection) (RuntimeSet, error)

func (s RuntimeSet) Ready(selection domain.ModelSelection) error
func (s RuntimeSet) BindApprover(approver ports.PermissionApprover)
```

`Ready` must first return `ConfigurationError` when non-nil, then confirm `selection` exists in `Models`, then require a non-empty credential for its provider. Return `*domain.TypedError{Kind: domain.ErrorConfigurationInvalid}` with the config path supplied by the builder, the environment-variable name, and restart guidance. Add an unexported `configPath string` field for this message.

Add an unexported `bindApprover func(ports.PermissionApprover)` field to `RuntimeSet`; `BindApprover` calls it when non-nil. Define an unexported `runtimeBuilder` that holds CLI overrides, workspace, store, policy binding, active-session binding, runtime-events channel, HTTP client, and config path. Its `build(config.Config, domain.ModelSelection) (RuntimeSet, error)` method must use the supplied current selection as `ResolveOptions.DefaultProfile/DefaultModel`, then construct all clients, tools, runner limits, shell timeout, key filtering, compact function, immutable redactor, and `bindApprover` closure together.

- [ ] **Step 4: Implement config-sensitive composition in the builder**

Move existing config-sensitive helper functions from `bootstrap.go` into `runtime.go` and make them `runtimeBuilder` methods where they need builder state. Implement `profileKeys`, `profileKeyValues`, client/router creation, tool registry construction, effective max-tool calculation, effective shell timeout, and compaction there; use Task 1's `Models`, `ProviderKeyEnvironmentNames`, and `APIKeys` helpers rather than duplicating sorting and environment lookup. Keep signatures compatible with the old bootstrap call sites until Task 5 replaces those call sites.

The builder must configure every provider client. Apply CLI/environment base URL and model overrides only to the selected provider returned by `Config.Resolve`; other clients use their provider options and receive their own configured environment key.

Set `Runner.Policy` to the stable policy binding, `Runner.Sessions` to the long-lived store, and `Runner.Sink` to the shared runtime event channel. Set `RuntimeSet.bindApprover` to `func(approver ports.PermissionApprover) { runner.Approver = approver }`; bootstrap and each successful reload must call `BindApprover(application)` before exposing the set.

- [ ] **Step 5: Test generation isolation**

Build generation A with key A, timeout 1 second, and model A; build generation B with key B, timeout 2 seconds, and model B. Replace the long-lived binding with B's redactor. Assert generation A's runner/output still redacts A while the store/logger binding now redacts B. This proves a turn snapshot cannot change midway.

- [ ] **Step 6: Run runtime builder tests**

Run: `go test -race ./internal/app -run 'TestRuntimeSet|TestBuildRuntime|TestRuntimeGeneration' -count=1`

Expected: PASS.

- [ ] **Step 7: Inspect the runtime-generation checkpoint**

```bash
git status --short
git diff --check
git diff -- internal/app/runtime.go internal/app/runtime_test.go internal/app/bootstrap.go
```

Expected: only intended runtime-generation changes are present and `git diff --check` exits 0.

---

### Task 5: App-Owned Runtime Lifecycle and Config-Independent Bootstrap

**Files:**
- Modify: `internal/app/bootstrap.go:33-238`
- Rewrite: `internal/app/bootstrap_test.go`
- Modify: `internal/app/commands.go`
- Modify: `internal/app/events.go`
- Modify: `internal/app/app.go`
- Modify: `internal/app/app_test.go`
- Modify: `internal/app/session_state_test.go`
- Modify: `internal/tui/run.go:25-110`
- Modify: `internal/tui/run_test.go`
- Modify: `internal/tui/model.go`
- Modify: `internal/tui/view.go`
- Modify: `internal/cli/options.go`
- Modify: `internal/cli/options_test.go`
- Delete: `internal/tui/components/setup.go`
- Delete: `internal/tui/components/setup_test.go`

**Interfaces:**
- Consumes: `config.EnsureGlobal`, `config.Load`, and `runtimeBuilder`.
- Produces: `BootstrapOptions.ConfigPath string` and removes `Config`/`ProcessAPIKey`.
- Produces: `Snapshot.ConfigurationError error` for initial TUI notice.
- Produces: an `App` even when config parsing/validation/credentials are unavailable.
- Produces: `CommandReloadConfig`, `EventTurnAccepted`, and `EventReloadCompleted`.
- Guarantees: the app command loop is the only writer of the active runtime set and redactor binding.

- [ ] **Step 1: Write failing bootstrap tests for invalid and missing credentials**

Add tests that use an isolated config path and assert:

```go
application != nil
snapshot.Session.ID != ""
snapshot.Session.Selection == (domain.ModelSelection{}) // structurally invalid config
snapshot.ConfigurationError is configuration_invalid
```

For structurally valid config with no selected key, assert the default selection and model list are present, `ConfigurationError` instructs restart with the named variable, and bootstrap still succeeds.

For a valid keyed config, preserve all current canonical workspace, resume, mode/model override, provider routing, permissions, storage, and redaction assertions, rewritten for JSONC fixtures and no process-only key.

- [ ] **Step 2: Write failing app readiness and reload transaction tests**

Create an app with a `RuntimeSet` whose `Ready` returns a typed configuration error. Send `CommandStartTurn{Prompt: "draft"}` and assert one `EventError` with `Draft == "draft"`, no `EventTurnAccepted`, no runtime call, and no session append. Create a ready set and assert `EventTurnAccepted` precedes runtime events and the runtime is called exactly once.

Add tests for:

- active turn or compaction rejects reload with `an operation is already active`;
- successful reload swaps runtime/models/default/redactor, emits `EventReloadCompleted{Applied:true}`, and the next turn uses the new runtime;
- parse/construction failure emits `Applied:false` and preserves the old runtime/redactor/models;
- missing credential candidate emits `Applied:true` with a typed error and rejects the next draft before persistence;
- candidate removing the current model appends one `model.changed` fallback before activation;
- fallback append failure leaves the old runtime active;
- a blocked turn keeps generation A; after it completes, reload and the next turn use B;
- opening a session whose model is absent from the active set durably falls back before publishing state.

- [ ] **Step 3: Write failing TUI-run first-launch tests**

Add package variables `ensureGlobal = config.EnsureGlobal`, `bootstrap = app.Bootstrap`, and `newProgram = func(model tea.Model) programRunner { return tea.NewProgram(model) }` in `internal/tui/run.go`. Override and restore them with `t.Cleanup` in `run_test.go` to assert that a created template still calls bootstrap and constructs `NewModel` rather than `NewSetupStage`.

Add a model assertion that `ScreenSetup` no longer exists and the initial screen is always `ScreenConversation`.

- [ ] **Step 4: Run app/bootstrap/run tests and verify failure**

Run: `go test ./internal/app ./internal/tui -run 'Test(App.*(Runtime|Reload|Draft|Fallback)|Bootstrap|Run)' -count=1`

Expected: FAIL because reload contracts are absent, bootstrap still requires valid config, and `tui.Run` still launches setup.

- [ ] **Step 5: Add command, event, and app runtime contracts**

Add:

```go
const CommandReloadConfig CommandKind = "reload_config"

const (
    EventTurnAccepted    EventKind = "turn_accepted"
    EventReloadCompleted EventKind = "reload_completed"
    EventNotice          EventKind = "notice"
)

type Event struct {
    // existing fields remain
    Models  []domain.ModelSelection
    Draft   string
    Applied bool
}
```

Extend `Options`/`App` with `RuntimeSet`, `ReloadRuntime`, and `*secret.Binding`. `New` must create `secret.NewBinding(secret.New())` when no binding is supplied so zero-option lifecycle tests remain safe. Replace standalone mutable `runtime`, `compactSession`, and `configuredModels` access with the active set. Preserve the old `Options.Runtime`, `Options.CompactSession`, and `Options.ConfiguredModels` fields only long enough to adapt existing unit fixtures in this same step; update every `app.New(app.Options{...})` test fixture under `internal/app` to construct `RuntimeSet`, then remove those old fields before running Step 10.

- [ ] **Step 6: Implement serialized runtime activation and fallback**

Add `operationReload`. On start-turn and compaction, copy `activeSet := a.runtimeSet`, call `activeSet.Ready(a.session.Selection)`, and reject with `EventError{Draft: command.Prompt}` before setting an operation or spawning a goroutine. Publish `EventTurnAccepted` only after readiness/input checks succeed and immediately before starting the turn goroutine.

On reload command, reject if any operation is active; otherwise call `ReloadRuntime(ctx, a.session.Selection)` in a goroutine and return the candidate through:

```go
type operationResult struct {
    kind       operationKind
    err        error
    runtimeSet RuntimeSet
}
```

When reload completes:

1. if builder error is non-nil, publish `EventReloadCompleted{Applied:false}` and retain all old fields;
2. if current selection is absent, append `model.changed` to candidate default and abort activation on append failure;
3. call `candidate.BindApprover(a)`;
4. assign the complete candidate set;
5. call `redactors.Replace(candidate.Redactor)`;
6. publish `EventReloadCompleted{Applied:true, Models:candidate.Models, Selection:a.session.Selection}`;
7. attach `candidate.Ready(current)` as a non-terminal event error when the credential is missing.

Never replace fields before required durable fallback succeeds. Reload starts only while idle, so every turn owns its copied `RuntimeSet` and no successful swap overlaps it.

Use candidate default selection when `/new` starts from an empty/unconfigured session. After `/sessions` loads a replay, preserve a configured selection or append fallback `model.changed`; if persistence fails, restore the prior in-memory session/replay and emit a non-terminal error. A `/model` change to a provider with a missing key remains durable, but the next turn/compaction readiness check returns restart guidance.

- [ ] **Step 7: Split bootstrap into long-lived and generation state**

Change `BootstrapOptions` to:

```go
type BootstrapOptions struct {
    ConfigPath string
    CLI        cli.Options
    CWD        string
    HTTPClient *http.Client
}
```

Bootstrap order must be:

1. resolve workspace and data directory;
2. create `redactors := secret.NewBinding(secret.New())`;
3. open the debug logger with the binding;
4. create the JSONL store with `Sanitize: redactors.JSON`;
5. load config without returning config errors;
6. derive a default selection only when structural config is valid;
7. create/resume the session, allowing an empty selection;
8. restore the policy and active-session binding;
9. create a reload closure that loads the fixed config path and calls `builder.build(cfg, currentSelection)`;
10. build the initial runtime candidate when config is valid;
11. reconcile an absent/unconfigured session selection to the candidate default with a durable `model.changed` event;
12. create `App` with the candidate, reload closure, redactor binding, and configuration error;
13. call `candidate.BindApprover(application)` and return snapshot/app; only workspace, storage, logger-open, session, or required fallback-persistence errors are fatal.

Wrap every config-facing error with a helper that returns `*domain.TypedError{Kind: domain.ErrorConfigurationInvalid}` and includes the exact config path plus either `edit the file and run /reload` or missing-key restart guidance. Never include raw config bytes/comments.

- [ ] **Step 8: Remove `--config`, process-only key, and setup from startup**

Delete `ConfigPath string` and its flag registration from `cli.Options`/`Parse`. Remove `ProcessAPIKey` from `config.ResolveOptions` and all remaining resolution branches/tests. Add:

```go
func TestParseRejectsConfigOverride(t *testing.T) {
    if _, err := cli.Parse([]string{"--config", "/tmp/config.jsonc"}); err == nil {
        t.Fatal("--config override accepted")
    }
}
```

Delete `internal/tui/components/setup.go` and `setup_test.go` in the same change as the following `tui.Run` rewrite, so no intermediate checked state imports a deleted component.

Replace `resolveConfigPath`/`loadOrSetup` with:

```go
configPath, created, err := config.EnsureGlobal()
if err != nil { return fmt.Errorf("ensure config: %w", err) }
application, snapshot, err := app.Bootstrap(ctx, app.BootstrapOptions{
    ConfigPath: configPath,
    CLI: options,
})
```

Always build `NewModel`. Apply the durable state event first. If `created`, append a notice that the template was created at the exact path and must be edited before `/reload`; then apply a non-terminal `EventError` for `snapshot.ConfigurationError` so neither message is erased by conversation replay.

Delete `ScreenSetup`, its view branch, `resolveConfigPath`, and `loadOrSetup`. Rename `firstRunError` to `firstSignificantError` because `runApplication` still uses it to combine program/app shutdown errors; update its tests and call sites.

- [ ] **Step 9: Update all bootstrap and direct app fixtures**

Rewrite `internal/app/bootstrap_test.go` to create JSONC files and pass `BootstrapOptions.ConfigPath`. Rewrite direct `app.New` fixtures in `internal/app/app_test.go` and `internal/app/session_state_test.go` to use `RuntimeSet`. Remove process-only key assertions. Keep tests for canonical workspace, resume selection, CLI overrides, per-provider credentials, auto-shell acknowledgement, and redaction.

- [ ] **Step 10: Run app, bootstrap, CLI, and TUI-run tests**

Run:

```bash
go test -race ./internal/app ./internal/cli ./internal/tui -count=1
go test ./...
```

Expected: PASS with no race report.

- [ ] **Step 11: Inspect the runtime/bootstrap checkpoint**

```bash
git status --short
git diff --check
git diff -- internal/app internal/cli internal/tui/run.go internal/tui/run_test.go internal/tui/model.go internal/tui/view.go internal/tui/components/setup.go internal/tui/components/setup_test.go
```

Expected: intended app/bootstrap/startup removals are present, deleted setup files are visible, and `git diff --check` exits 0.

---

### Task 6: TUI Reload Command, Model Refresh, and Draft Restoration

**Files:**
- Modify: `internal/tui/model.go`
- Modify: `internal/tui/update.go`
- Modify: `internal/tui/view.go`
- Modify: `internal/tui/keys.go`
- Modify: `internal/tui/export_test.go`
- Modify: `internal/tui/commands_test.go`
- Modify: `internal/tui/model_test.go`
- Modify: `internal/tui/components/palette.go`
- Modify: `internal/tui/components/palette_test.go`

**Interfaces:**
- Consumes: Task 5 commands/events.
- Produces: `/reload` direct command, palette item, help entry, completion feedback, and model-list refresh.
- Guarantees: an unaccepted normal prompt is restored to the composer and is not rendered as a user conversation block.

- [ ] **Step 1: Write failing command/palette/help tests**

Add `/reload` to `TestSlashCommandTable` expecting `CommandReloadConfig`. Add it to palette expected items and assert `renderScreen(ScreenHelp)` contains `/reload`.

Add a local active-operation test matching `/compact`:

```go
model = tui.SetTurnActiveForTest(model, true)
model = tui.SubmitForTest(model, "/reload")
// no command, turn remains active, notice is "an operation is already active"
```

- [ ] **Step 2: Write failing draft lifecycle tests**

Submit `draft prompt` and assert the TUI sends one `CommandStartTurn` but does not append a user block yet. Apply `EventTurnAccepted` and assert the user block appears once.

In a separate model, submit the same draft then apply:

```go
app.Event{
    Kind:  app.EventError,
    Err:   &domain.TypedError{Kind: domain.ErrorConfigurationInvalid, Message: "edit config and run /reload"},
    Draft: "draft prompt",
}
```

Assert the composer value is restored, active state is false, no user block exists, and the error block/context action is visible.

- [ ] **Step 3: Write failing reload-completion/model-refresh tests**

Submit `/reload`, assert the composer locks, then apply an `EventReloadCompleted` with models `new/a`, `new/b`, current selection `new/b`, and `Applied:true`. Assert unlock, status `new/b`, picker entries replaced rather than appended, and a success notice.

Apply `Applied:false` with an error and assert old status/models remain while the composer unlocks. Apply `Applied:true` plus a missing-credential error and assert new models/status apply while restart guidance is rendered.

- [ ] **Step 4: Run TUI tests and verify failure**

Run: `go test ./internal/tui/... -run 'Test.*(Reload|Draft|Palette|SlashCommand)' -count=1`

Expected: FAIL because the command/event handling is absent.

- [ ] **Step 5: Implement pending-draft and reload state**

Add `pendingDraft string` to `Model`. For a non-slash submission, store the draft, mark active, and send the command without appending a user block. On `EventTurnAccepted`, append the pending draft once and clear it. On an error carrying `Draft`, restore the composer, clear pending draft, and unlock.

Route `/reload` like `/compact`: reject locally while active; otherwise mark active and send `CommandReloadConfig`.

Handle `EventReloadCompleted` by unlocking first. If `Applied`, replace `model.models`, update selection/cursor, and append the success message. If `Err` is present, route it through the existing typed-error rendering after applying fields only when `Applied` is true.

Handle `EventNotice` by appending `components.BlockNotice` without changing turn state. This path renders the first-run template-created message after the initial replay and is also available for future non-error lifecycle notices.

When any `EventState` carries a non-nil `Models` slice, replace the model list and recompute the cursor. Distinguish nil (no update) from an empty non-nil slice (clear picker).

- [ ] **Step 6: Update palette and help copy**

Add:

```go
{Name: "/reload", Description: "Reload ~/.config/yordam/config.jsonc", Command: app.CommandReloadConfig},
```

Render help as:

```text
HELP
/new /sessions /mode /model /reload /compact /help /quit
Ctrl+P: commands | Ctrl+O: context | Ctrl+C: quit | Esc: back
```

- [ ] **Step 7: Run all TUI tests**

Run: `go test -race ./internal/tui/... -count=1`

Expected: PASS.

- [ ] **Step 8: Inspect the TUI reload checkpoint**

```bash
git status --short
git diff --check
git diff -- internal/tui
```

Expected: only intended TUI reload/draft changes are present and `git diff --check` exits 0.

---

### Task 7: PTY Acceptance, Security Regression, and Documentation

**Files:**
- Modify: `internal/ptytest/yordam_test.go`
- Modify: `internal/acceptance/pty_test.go`
- Modify: `internal/acceptance/secret_test.go`
- Modify: `internal/acceptance/session_release_test.go`
- Modify: any remaining Go tests found by the removal searches below
- Modify: `README.md`
- Modify: `SECURITY.md`
- Modify: `docs/releases/v0.1.0-acceptance.md`

**Interfaces:**
- Consumes: completed fixed-path startup and reload behavior.
- Produces: end-to-end evidence that first launch, edit/reload, resume, model fallback, terminal restoration, and secret boundaries work through the binary.

- [ ] **Step 1: Replace PTY config overrides with isolated homes**

Change PTY helpers to create a home per test and place fixtures at:

```go
configPath := filepath.Join(home, ".config", "yordam", "config.jsonc")
```

Pass `HOME=<home>` in the child environment. Remove every `--config` argument. Rewrite fixture bodies to JSONC using provider/model structure. Keep `--data-dir` where a test needs direct artifact inspection.

Run removal searches:

```bash
rg -n -- '--config|config\.toml|FIRST-RUN SETUP|active_profile|profiles\.' internal --glob '*.go'
```

Expected after updates: no production or test match, except historical text deliberately quoted in a migration/non-support assertion.

- [ ] **Step 2: Replace first-run setup PTY test with normal-TUI/template test**

The new PTY test must start with an empty isolated home, wait for `Conversation`, `.config/yordam/config.jsonc`, and `/reload`, then exit with Ctrl+C and assert terminal restoration. Read the generated file and assert schema URL, OpenAI URL, reserved model sentinel, mode `0600`, parent mode `0700`, and absence of secret values.

- [ ] **Step 3: Add edit-and-reload PTY test**

Start Yordam with an empty home but include `YORDAM_API_KEY=reload-secret` in its initial environment. After the normal TUI appears, atomically replace the generated JSONC from the test process with a valid config pointing to the local SSE server. Type `/reload`, wait for `openai/test-model` and a success notice, submit `hello after reload`, wait for the scripted assistant response, then exit.

Assert the session ID/data remains the same across reload and `reload-secret` is absent from PTY output, config, data tree, artifacts, and debug log.

- [ ] **Step 4: Add failed reload and restart-guidance PTY coverage**

With a working runtime, write invalid JSONC and type `/reload`; assert the old model remains in status and a subsequent prompt still reaches the old SSE server. Then write structurally valid config naming `MISSING_RELOAD_KEY`, reload, and assert the new model appears but submission is restored with restart guidance and no provider request occurs.

- [ ] **Step 5: Update acceptance helpers and release evidence**

Rewrite `acceptSingleBinaryStartup` and `acceptSecretFreeSetup` as normal-startup/template/reload checks. Update recovery resume to place JSONC in the isolated home rather than passing a path. Remove all process-only key byte assertions; preserve environment-secret redaction, shell stripping, HTTP authorization, cancellation, and recovery assertions.

- [ ] **Step 6: Rewrite README configuration and command sections**

Document only:

```text
~/.config/yordam/config.jsonc
```

Include the approved JSONC example, explain that first run creates an intentionally incomplete template and still opens normal TUI, document editing plus `/reload`, state that missing environment keys require restart, add `/reload` to command tables, remove `--config` and setup-form/process-only key text, and state old TOML files are ignored without migration.

Update `SECURITY.md` to remove the optional process-only setup-key claim and retain the environment-only credential boundary. Update `docs/releases/v0.1.0-acceptance.md` commands/evidence so it does not claim the removed setup flow or config path.

- [ ] **Step 7: Run focused PTY and acceptance tests**

Run:

```bash
go test ./internal/ptytest -count=1
go test -tags=acceptance ./internal/acceptance -count=1
```

Expected: PASS.

- [ ] **Step 8: Run full verification**

Run:

```bash
gofmt -w cmd internal
go test ./...
go test -race ./...
go vet ./...
go build -o /tmp/yordam-jsonc-config ./cmd/yordam
/tmp/yordam-jsonc-config --version
git diff --check
```

Expected: every command exits 0; version output starts with `yordam`; no race or vet diagnostics; diff check is empty.

- [ ] **Step 9: Re-run secret and legacy-reference scans**

Run:

```bash
rg -n -- '--config|config\.toml|FIRST-RUN SETUP|active_profile|profiles\.' cmd internal README.md SECURITY.md docs/releases
rg -n 'apiKey|api_key' schema internal/config README.md
```

Expected: the first scan finds no live code/current-doc references; the second finds only `apiKeyEnv`/environment-name documentation and explicit tests rejecting raw key fields, never a persisted raw-key property.

- [ ] **Step 10: Inspect the final acceptance/documentation checkpoint**

```bash
git status --short
git diff --check
git diff -- internal/ptytest internal/acceptance README.md SECURITY.md docs/releases/v0.1.0-acceptance.md
```

Expected: only intended acceptance/current-documentation changes are present and `git diff --check` exits 0.

---

## Plan Self-Review

- Spec coverage: fixed path, JSONC syntax, schema, invalid template sentinel, normal startup, config-independent sessions, missing credentials, immutable turn snapshots, dynamic sink redaction, atomic reload, model fallback, draft restoration, command/help/palette updates, security, PTY acceptance, and documentation each map to a task.
- Placeholder scan: implementation steps specify concrete paths, types, commands, expected outcomes, and exact behavioral cases; the literal `your-model-id` is the product's required reserved sentinel rather than an unfinished plan item.
- Type consistency: JSONC documents normalize into the existing `Config/Profile` runtime model; provider IDs remain stored in durable `ModelSelection.Profile`; `RuntimeSet` is produced in Task 4 and consumed by Tasks 5-6; `secret.Redacting` is introduced before logger/output/runtime use; `CommandReloadConfig`, `EventTurnAccepted`, and `EventReloadCompleted` are introduced before TUI routing.
