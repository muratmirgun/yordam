# Yordam Foundation and Session Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create Yordam's compileable Go module, stable internal domain/port contracts, validated named-profile configuration, and crash-recoverable append-only session storage.

**Architecture:** Domain packages contain data and validation only. Ports describe provider, tool, permission, session, and artifact dependencies. Config and JSONL storage are concrete adapters with injected clock/entropy for deterministic tests.

**Tech Stack:** Go 1.26.4, standard library, `github.com/BurntSushi/toml` v1.6.0, `github.com/oklog/ulid/v2` v2.1.1.

## Global Constraints

- Module path: `github.com/muratmirgun/yordam`.
- Product and binary name: `yordam`.
- macOS and Linux only; no Windows code in this phase.
- Session events are schema version 1, append-only JSONL, strictly increasing by `seq`.
- A final incomplete line is recoverable; earlier corruption is read-only failure.
- API key values are never written to config or session data.
- Config reads global config only; repository-owned config is forbidden.
- All public-in-package behavior begins with a focused failing test.

---

### Task 1: Bootstrap the module and domain value types

**Files:**
- Create: `go.mod`
- Create: `internal/domain/permission.go`
- Create: `internal/domain/tool.go`
- Create: `internal/domain/model.go`
- Create: `internal/domain/errors.go`
- Test: `internal/domain/domain_test.go`

**Interfaces:**
- Produces: `domain.PermissionMode`, `domain.ToolDescriptor`, `domain.ToolRequest`, `domain.ToolResult`, `domain.ModelRequest`, `domain.ModelEvent`, and `domain.ErrorKind`.
- Consumes: nothing; this is the root type layer.

- [ ] **Step 1: Write the failing domain validation test**

```go
package domain_test

import (
    "encoding/json"
    "testing"

    "github.com/muratmirgun/yordam/internal/domain"
)

func TestPermissionModeValidate(t *testing.T) {
    t.Parallel()
    for _, mode := range []domain.PermissionMode{domain.ModeSafe, domain.ModeAsk, domain.ModeAuto} {
        if err := mode.Validate(); err != nil {
            t.Fatalf("%q: %v", mode, err)
        }
    }
    if err := domain.PermissionMode("root").Validate(); err == nil {
        t.Fatal("invalid mode accepted")
    }
}

func TestToolDescriptorValidate(t *testing.T) {
    t.Parallel()
    d := domain.ToolDescriptor{
        Name: "read",
        Description: "Read UTF-8 text",
        InputSchema: json.RawMessage(`{"type":"object"}`),
        Mutation: domain.MutationReadOnly,
    }
    if err := d.Validate(); err != nil {
        t.Fatal(err)
    }
    d.Name = "Read File"
    if err := d.Validate(); err == nil {
        t.Fatal("invalid tool name accepted")
    }
}
```

- [ ] **Step 2: Run the test and observe the missing package failure**

Run: `go test ./internal/domain -run 'TestPermissionModeValidate|TestToolDescriptorValidate' -v`

Expected: FAIL because `go.mod` and `internal/domain` do not exist.

- [ ] **Step 3: Create the module and exact root types**

```go
// go.mod
module github.com/muratmirgun/yordam

go 1.26.0

toolchain go1.26.4

require (
    github.com/BurntSushi/toml v1.6.0
    github.com/oklog/ulid/v2 v2.1.1
)
```

```go
// internal/domain/permission.go
package domain

import "fmt"

type PermissionMode string

const (
    ModeSafe PermissionMode = "safe"
    ModeAsk  PermissionMode = "ask"
    ModeAuto PermissionMode = "auto"
)

func (m PermissionMode) Validate() error {
    switch m {
    case ModeSafe, ModeAsk, ModeAuto:
        return nil
    default:
        return fmt.Errorf("invalid permission mode %q", m)
    }
}

type PermissionAction string

const (
    PermissionAllow PermissionAction = "allow"
    PermissionAsk   PermissionAction = "ask"
    PermissionDeny  PermissionAction = "deny"
)

type PermissionLifetime string

const (
    PermissionOnce    PermissionLifetime = "once"
    PermissionSession PermissionLifetime = "session"
)

type PermissionDecision struct {
    Action   PermissionAction   `json:"action"`
    Lifetime PermissionLifetime `json:"lifetime"`
    Scope    string             `json:"scope"`
    Reason   string             `json:"reason"`
}
```

```go
// internal/domain/tool.go
package domain

import (
    "encoding/json"
    "fmt"
    "regexp"
    "time"
)

type MutationKind string

const (
    MutationReadOnly MutationKind = "read_only"
    MutationFile     MutationKind = "file"
    MutationProcess  MutationKind = "process"
)

var toolNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

type ToolDescriptor struct {
    Name        string          `json:"name"`
    Description string          `json:"description"`
    InputSchema json.RawMessage `json:"input_schema"`
    Mutation    MutationKind    `json:"mutation"`
}

func (d ToolDescriptor) Validate() error {
    if !toolNamePattern.MatchString(d.Name) {
        return fmt.Errorf("invalid tool name %q", d.Name)
    }
    if d.Description == "" || !json.Valid(d.InputSchema) {
        return fmt.Errorf("invalid descriptor for %q", d.Name)
    }
    switch d.Mutation {
    case MutationReadOnly, MutationFile, MutationProcess:
        return nil
    default:
        return fmt.Errorf("invalid mutation kind %q", d.Mutation)
    }
}

type ToolRequest struct {
    CallID   string          `json:"call_id"`
    Name     string          `json:"name"`
    Input    json.RawMessage `json:"input"`
    Workspace string         `json:"workspace"`
}

type ToolStatus string

const (
    ToolSucceeded ToolStatus = "succeeded"
    ToolFailed    ToolStatus = "failed"
    ToolDenied    ToolStatus = "denied"
    ToolCancelled ToolStatus = "cancelled"
)

type ToolResult struct {
    CallID      string     `json:"call_id"`
    Status      ToolStatus `json:"status"`
    ErrorKind   ErrorKind  `json:"error_kind,omitempty"`
    Content     string     `json:"content"`
    ArtifactIDs []string   `json:"artifact_ids,omitempty"`
    ExitCode    *int       `json:"exit_code,omitempty"`
    Duration    time.Duration `json:"duration"`
    Truncated   bool       `json:"truncated"`
}

type ToolProgress struct {
    CallID string `json:"call_id"`
    Text string `json:"text"`
    Truncated bool `json:"truncated"`
}
```

```go
// internal/domain/model.go
package domain

import "encoding/json"

type Role string

const (
    RoleSystem    Role = "system"
    RoleUser      Role = "user"
    RoleAssistant Role = "assistant"
    RoleTool      Role = "tool"
)

type Message struct {
    Role       Role       `json:"role"`
    Content    string     `json:"content"`
    ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
    ToolCallID string     `json:"tool_call_id,omitempty"`
}

type ToolCall struct {
    ID        string          `json:"id"`
    Name      string          `json:"name"`
    Arguments json.RawMessage `json:"arguments"`
}

type ModelSelection struct {
    Profile string `json:"profile"`
    Model   string `json:"model"`
}

type ModelRequest struct {
    Selection ModelSelection  `json:"selection"`
    Messages  []Message       `json:"messages"`
    Tools     []ToolDescriptor `json:"tools"`
}

type ModelEventKind string

const (
    ModelTextDelta ModelEventKind = "text_delta"
    ModelToolCall  ModelEventKind = "tool_call"
    ModelDone      ModelEventKind = "done"
    ModelStreamError ModelEventKind = "error"
)

type ModelEvent struct {
    Kind     ModelEventKind `json:"kind"`
    Text     string         `json:"text,omitempty"`
    ToolCall *ToolCall      `json:"tool_call,omitempty"`
    Err      error          `json:"-"`
}
```

```go
// internal/domain/errors.go
package domain

type ErrorKind string

const (
    ErrorCancelled            ErrorKind = "cancelled"
    ErrorPermissionDenied     ErrorKind = "permission_denied"
    ErrorToolFailed           ErrorKind = "tool_failed"
    ErrorToolTimeout          ErrorKind = "tool_timeout"
    ErrorProviderRetryable    ErrorKind = "provider_retryable"
    ErrorProviderInterrupted  ErrorKind = "provider_interrupted"
    ErrorProviderFatal        ErrorKind = "provider_fatal"
    ErrorContextTooLarge      ErrorKind = "context_too_large"
    ErrorStorageCorrupt       ErrorKind = "storage_corrupt"
    ErrorConfigurationInvalid ErrorKind = "configuration_invalid"
)

type TypedError struct {
    Kind    ErrorKind
    Message string
    Cause   error
}

func (e *TypedError) Error() string { return e.Message }
func (e *TypedError) Unwrap() error { return e.Cause }
```

- [ ] **Step 4: Format, resolve modules, and run the tests**

Run: `gofmt -w internal/domain && go mod tidy && go test ./internal/domain -v`

Expected: PASS; `go.sum` is created.

- [ ] **Step 5: Commit the root domain contract**

```bash
git add go.mod go.sum internal/domain
git commit -m "feat: define Yordam domain contracts"
```

### Task 2: Define durable events, sessions, and adapter ports

**Files:**
- Create: `internal/domain/events.go`
- Create: `internal/domain/session.go`
- Create: `internal/ports/provider.go`
- Create: `internal/ports/tool.go`
- Create: `internal/ports/permission.go`
- Create: `internal/ports/session.go`
- Test: `internal/domain/events_test.go`

**Interfaces:**
- Consumes: Task 1 domain model/tool/permission types.
- Produces: `domain.DurableEvent`, `domain.Session`, `ports.ModelProvider`, `ports.Tool`, `ports.PermissionPolicy`, `ports.SessionStore`, and `ports.ArtifactStore`.

- [ ] **Step 1: Write event envelope validation tests**

```go
package domain_test

import (
    "encoding/json"
    "testing"
    "time"

    "github.com/muratmirgun/yordam/internal/domain"
)

func TestDurableEventValidate(t *testing.T) {
    t.Parallel()
    event := domain.DurableEvent{
        SchemaVersion: 1,
        EventID: "01J00000000000000000000000",
        SessionID: "01J00000000000000000000001",
        Seq: 1,
        Time: time.Unix(0, 0).UTC(),
        Kind: domain.EventSessionCreated,
        Payload: json.RawMessage(`{"workspace":"/tmp/app"}`),
    }
    if err := event.Validate(); err != nil {
        t.Fatal(err)
    }
    event.Seq = 0
    if err := event.Validate(); err == nil {
        t.Fatal("zero sequence accepted")
    }
}
```

- [ ] **Step 2: Run the focused test and verify missing symbols**

Run: `go test ./internal/domain -run TestDurableEventValidate -v`

Expected: FAIL with undefined `domain.DurableEvent`.

- [ ] **Step 3: Add exact event/session types and ports**

```go
// internal/domain/events.go
package domain

import (
    "encoding/json"
    "fmt"
    "time"
)

type EventKind string

const (
    EventSessionCreated     EventKind = "session.created"
    EventSessionTitleChanged EventKind = "session.title_changed"
    EventModeChanged        EventKind = "mode.changed"
    EventModelChanged       EventKind = "model.changed"
    EventUserMessage        EventKind = "user.message"
    EventAssistantMessage   EventKind = "assistant.message"
    EventToolRequested      EventKind = "tool.requested"
    EventPermissionRequested EventKind = "permission.requested"
    EventPermissionResolved EventKind = "permission.resolved"
    EventTrustedExecutionAcknowledged EventKind = "trusted_execution.acknowledged"
    EventToolStarted        EventKind = "tool.started"
    EventToolResult         EventKind = "tool.result"
    EventFileChangePlanned  EventKind = "file.change_planned"
    EventFileChanged        EventKind = "file.changed"
    EventContextCompacted   EventKind = "context.compacted"
    EventTurnCompleted      EventKind = "turn.completed"
    EventTurnFailed         EventKind = "turn.failed"
    EventTurnInterrupted    EventKind = "turn.interrupted"
)

type DurableEvent struct {
    SchemaVersion int             `json:"schema_version"`
    EventID       string          `json:"event_id"`
    SessionID     string          `json:"session_id"`
    Seq           uint64          `json:"seq"`
    Time          time.Time       `json:"time"`
    Kind          EventKind       `json:"kind"`
    Payload       json.RawMessage `json:"payload"`
}

func (e DurableEvent) Validate() error {
    if e.SchemaVersion != 1 || e.EventID == "" || e.SessionID == "" || e.Seq == 0 || e.Time.IsZero() || e.Kind == "" || !json.Valid(e.Payload) {
        return fmt.Errorf("invalid durable event kind=%q seq=%d", e.Kind, e.Seq)
    }
    return nil
}
```

```go
// internal/domain/session.go
package domain

import "time"

type Workspace struct {
    ID            string `json:"id"`
    CanonicalPath string `json:"canonical_path"`
}

type Session struct {
    ID        string         `json:"id"`
    Workspace Workspace      `json:"workspace"`
    Title     string         `json:"title"`
    Mode      PermissionMode `json:"mode"`
    Selection ModelSelection `json:"selection"`
    CreatedAt time.Time      `json:"created_at"`
    UpdatedAt time.Time      `json:"updated_at"`
    LastSeq   uint64         `json:"last_seq"`
}

type SessionSummary struct {
    ID        string         `json:"id"`
    Title     string         `json:"title"`
    Mode      PermissionMode `json:"mode"`
    UpdatedAt time.Time      `json:"updated_at"`
}

type SessionReplay struct {
    Session      Session
    Events       []DurableEvent
    RecoveryNote string
    ReadOnly     bool
}

type Artifact struct {
    ID        string `json:"id"`
    SessionID string `json:"session_id"`
    MediaType string `json:"media_type"`
    Path      string `json:"path"`
    Size      int64  `json:"size"`
    Truncated bool   `json:"truncated"`
}
```

```go
// internal/ports/provider.go
package ports

import (
    "context"
    "github.com/muratmirgun/yordam/internal/domain"
)

type ModelProvider interface {
    Stream(context.Context, domain.ModelRequest) (<-chan domain.ModelEvent, error)
}
```

```go
// internal/ports/tool.go
package ports

import (
    "context"
    "github.com/muratmirgun/yordam/internal/domain"
)

type Tool interface {
    Descriptor() domain.ToolDescriptor
    Execute(context.Context, domain.ToolRequest) domain.ToolResult
}

type ToolRegistry interface {
    Descriptors() []domain.ToolDescriptor
    Lookup(name string) (Tool, bool)
}
```

```go
// internal/ports/permission.go
package ports

import (
    "context"
    "github.com/muratmirgun/yordam/internal/domain"
)

type PermissionContext struct {
    SessionID string
    Mode      domain.PermissionMode
    Workspace string
}

type PermissionPolicy interface {
    Evaluate(context.Context, PermissionContext, domain.ToolRequest) domain.PermissionDecision
}
```

```go
// internal/ports/session.go
package ports

import (
    "context"
    "io"
    "github.com/muratmirgun/yordam/internal/domain"
)

type SessionStore interface {
    Create(context.Context, domain.Workspace, domain.PermissionMode, domain.ModelSelection) (domain.Session, error)
    Append(context.Context, string, domain.EventKind, any) (domain.DurableEvent, error)
    Load(context.Context, string) (domain.SessionReplay, error)
    List(context.Context, domain.Workspace) ([]domain.SessionSummary, error)
}

type ArtifactStore interface {
    Put(context.Context, string, string, io.Reader, int64) (domain.Artifact, error)
    Open(context.Context, domain.Artifact) (io.ReadCloser, error)
}
```

- [ ] **Step 4: Run domain and port tests**

Run: `gofmt -w internal/domain internal/ports && go test ./internal/domain ./internal/ports -v`

Expected: PASS; `internal/ports` reports no test files and compiles.

- [ ] **Step 5: Commit the durable contracts**

```bash
git add internal/domain internal/ports
git commit -m "feat: define session and adapter ports"
```

### Task 3: Implement global profile configuration

**Files:**
- Create: `internal/config/paths.go`
- Create: `internal/config/config.go`
- Test: `internal/config/config_test.go`

**Interfaces:**
- Consumes: `domain.ModelSelection`.
- Produces: `config.Load(LoadOptions) (Config, error)`, `config.Config.Resolve(ResolveOptions)`, and platform config/data paths.

- [ ] **Step 1: Write config precedence, validation, and secret tests**

```go
package config_test

import (
    "os"
    "path/filepath"
    "strings"
    "testing"

    "github.com/muratmirgun/yordam/internal/config"
)

func TestLoadAndResolveProfile(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "config.toml")
    body := `active_profile = "primary"
[profiles.primary]
base_url = "https://llm.example/v1"
api_key_env = "PRIMARY_KEY"
models = ["model-a", "model-b"]
default_model = "model-a"
`
    if err := os.WriteFile(path, []byte(body), 0o600); err != nil { t.Fatal(err) }
    t.Setenv("PRIMARY_KEY", "secret-value")

    cfg, err := config.Load(config.LoadOptions{ConfigPath: path, LookupEnv: os.LookupEnv})
    if err != nil { t.Fatal(err) }
    resolved, err := cfg.Resolve(config.ResolveOptions{Model:"model-b"})
    if err != nil { t.Fatal(err) }
    if resolved.BaseURL != "https://llm.example/v1" || resolved.APIKey != "secret-value" || resolved.Model != "model-b" {
        t.Fatalf("unexpected resolved profile: %#v", resolved)
    }
    if strings.Contains(string(cfg.RawForTest()), "secret-value") {
        t.Fatal("secret was copied into config")
    }
}

func TestLoadRejectsEmbeddedCredentialAndUnknownModel(t *testing.T) {
    cfg := config.Config{ActiveProfile: "bad", Profiles: map[string]config.Profile{
        "bad": {BaseURL: "https://user:pass@example.com/v1", APIKeyEnv: "KEY", Models: []string{"a"}, DefaultModel: "a"},
    }}
    if err := cfg.Validate(); err == nil { t.Fatal("credentialed URL accepted") }
}

func TestResolveCLIOverridesEnvironment(t *testing.T) {
    env:=map[string]string{"YORDAM_PROFILE":"env","YORDAM_MODEL":"env-model","YORDAM_BASE_URL":"https://env.example/v1","YORDAM_API_KEY":"env-key","CLI_KEY":"configured-key"}
    lookup:=func(name string)(string,bool){value,ok:=env[name];return value,ok}
    path:=filepath.Join(t.TempDir(),"config.toml")
    body:=`active_profile = "configured"
[profiles.configured]
base_url = "https://configured.example/v1"
api_key_env = "CLI_KEY"
models = ["cli-model"]
default_model = "cli-model"
[profiles.env]
base_url = "https://env.example/v1"
api_key_env = "ENV_KEY"
models = ["env-model"]
default_model = "env-model"
`
    if err:=os.WriteFile(path,[]byte(body),0o600);err!=nil{t.Fatal(err)}
    cfg,err:=config.Load(config.LoadOptions{ConfigPath:path,LookupEnv:lookup});if err!=nil{t.Fatal(err)}
    got,err:=cfg.Resolve(config.ResolveOptions{Profile:"configured",Model:"cli-model",BaseURL:"https://cli.example/v1",ProcessAPIKey:"process-key"})
    if err!=nil{t.Fatal(err)}
    if got.Name!="configured"||got.Model!="cli-model"||got.BaseURL!="https://cli.example/v1"||got.APIKey!="process-key"{t.Fatalf("resolved=%#v",got)}
}
```

- [ ] **Step 2: Run the focused config tests**

Run: `go test ./internal/config -run 'TestLoad' -v`

Expected: FAIL because the config package does not exist.

- [ ] **Step 3: Implement config paths, validation, resolution, and env overrides**

```go
// internal/config/paths.go
package config

import (
    "fmt"
    "os"
    "path/filepath"
    "runtime"
)

func DefaultConfigPath() (string, error) {
    dir, err := os.UserConfigDir()
    if err != nil { return "", err }
    return filepath.Join(dir, "yordam", "config.toml"), nil
}

func DefaultDataDir() (string, error) {
    home, err := os.UserHomeDir()
    if err != nil { return "", err }
    switch runtime.GOOS {
    case "darwin":
        return filepath.Join(home, "Library", "Application Support", "yordam", "data"), nil
    case "linux":
        if dir := os.Getenv("XDG_DATA_HOME"); dir != "" { return filepath.Join(dir, "yordam"), nil }
        return filepath.Join(home, ".local", "share", "yordam"), nil
    default:
        return "", fmt.Errorf("unsupported operating system %s", runtime.GOOS)
    }
}
```

```go
// internal/config/config.go
package config

import (
    "fmt"
    "net/url"
    "os"
    "regexp"
    "slices"
    "strings"

    "github.com/BurntSushi/toml"
)

type Profile struct {
    BaseURL      string   `toml:"base_url"`
    APIKeyEnv    string   `toml:"api_key_env"`
    Models       []string `toml:"models"`
    DefaultModel string   `toml:"default_model"`
}

type Config struct {
    ActiveProfile      string             `toml:"active_profile"`
    Profiles           map[string]Profile `toml:"profiles"`
    MaxToolCalls       int                `toml:"max_tool_calls"`
    ShellTimeoutSeconds int               `toml:"shell_timeout_seconds"`
    raw                []byte
    lookupEnv          func(string) (string, bool)
}

type LoadOptions struct {
    ConfigPath string
    LookupEnv  func(string) (string, bool)
}

type ResolveOptions struct { Profile, Model, BaseURL, ProcessAPIKey string }
type ResolvedProfile struct { Name, BaseURL, APIKey, APIKeyEnv, Model string }

var envName = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

func Load(opts LoadOptions) (Config, error) {
    if opts.LookupEnv == nil { opts.LookupEnv = os.LookupEnv }
    raw, err := os.ReadFile(opts.ConfigPath)
    if err != nil { return Config{}, err }
    var cfg Config
    if _, err := toml.Decode(string(raw), &cfg); err != nil { return Config{}, err }
    cfg.raw = append([]byte(nil), raw...)
    cfg.lookupEnv = opts.LookupEnv
    if cfg.MaxToolCalls == 0 { cfg.MaxToolCalls = 32 }
    if cfg.ShellTimeoutSeconds == 0 { cfg.ShellTimeoutSeconds = 120 }
    if err := cfg.Validate(); err != nil { return Config{}, err }
    return cfg, nil
}

func (c Config) Validate() error {
    if c.MaxToolCalls < 1 || c.MaxToolCalls > 128 { return fmt.Errorf("max_tool_calls must be 1..128") }
    if c.ShellTimeoutSeconds < 1 || c.ShellTimeoutSeconds > 1800 { return fmt.Errorf("shell_timeout_seconds must be 1..1800") }
    if _, ok := c.Profiles[c.ActiveProfile]; !ok { return fmt.Errorf("active profile %q not found", c.ActiveProfile) }
    for name, p := range c.Profiles {
        u, err := url.Parse(p.BaseURL)
        if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil { return fmt.Errorf("profile %q has invalid base_url", name) }
        if !envName.MatchString(p.APIKeyEnv) { return fmt.Errorf("profile %q has invalid api_key_env", name) }
        if len(p.Models) == 0 || !slices.Contains(p.Models, p.DefaultModel) { return fmt.Errorf("profile %q has invalid models", name) }
    }
    return nil
}

func (c Config) Resolve(opts ResolveOptions) (ResolvedProfile, error) {
    lookup := c.lookupEnv
    if lookup == nil { lookup = os.LookupEnv }
    profileName := opts.Profile
    if profileName == "" { if value, ok := lookup("YORDAM_PROFILE"); ok { profileName = value } }
    if profileName == "" { profileName = c.ActiveProfile }
    p, ok := c.Profiles[profileName]
    if !ok { return ResolvedProfile{}, fmt.Errorf("profile %q not found", profileName) }
    model := opts.Model
    if model == "" { if value, ok := lookup("YORDAM_MODEL"); ok { model = value } }
    if model == "" { model = p.DefaultModel }
    if !slices.Contains(p.Models, model) { return ResolvedProfile{}, fmt.Errorf("model %q not configured for %q", model, profileName) }
    key := opts.ProcessAPIKey
    if key == "" { if value, ok := lookup("YORDAM_API_KEY"); ok { key = value } }
    if key == "" { key, _ = lookup(p.APIKeyEnv) }
    base := opts.BaseURL
    if base == "" { if value, ok := lookup("YORDAM_BASE_URL"); ok { base = value } }
    if base == "" { base = p.BaseURL }
    base = strings.TrimRight(base, "/")
    u, err := url.Parse(base)
    if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil { return ResolvedProfile{}, fmt.Errorf("resolved base_url is invalid") }
    if key == "" { return ResolvedProfile{}, fmt.Errorf("API key environment variable %q is empty", p.APIKeyEnv) }
    return ResolvedProfile{Name: profileName, BaseURL: base, APIKey:key, APIKeyEnv:p.APIKeyEnv, Model:model}, nil
}

func (c Config) RawForTest() []byte { return append([]byte(nil), c.raw...) }
```

- [ ] **Step 4: Run config tests and the full current suite**

Run: `gofmt -w internal/config && go test ./internal/config ./internal/domain ./internal/ports -v`

Expected: PASS.

- [ ] **Step 5: Commit validated global config**

```bash
git add internal/config go.mod go.sum
git commit -m "feat: load validated model profiles"
```

### Task 4: Implement session layout, create, append, and list

**Files:**
- Create: `internal/session/jsonl/layout.go`
- Create: `internal/session/jsonl/store.go`
- Test: `internal/session/jsonl/store_test.go`

**Interfaces:**
- Consumes: `ports.SessionStore`, `domain.Workspace`, `domain.DurableEvent`.
- Produces: `jsonl.New(root, Options) *Store` implementing create/append/list with durable sequence ordering.

- [ ] **Step 1: Write create/append/list tests**

```go
package jsonl_test

import (
    "context"
    "encoding/json"
    "strings"
    "testing"
    "time"

    "github.com/muratmirgun/yordam/internal/domain"
    "github.com/muratmirgun/yordam/internal/session/jsonl"
)

func TestCreateAppendAndList(t *testing.T) {
    clock := func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) }
    store := jsonl.New(t.TempDir(), jsonl.Options{Clock: clock, Entropy: strings.NewReader(strings.Repeat("a", 1024))})
    workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
    if err != nil { t.Fatal(err) }
    session, err := store.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "primary", Model: "model-a"})
    if err != nil { t.Fatal(err) }
    event, err := store.Append(context.Background(), session.ID, domain.EventUserMessage, map[string]string{"content":"hello"})
    if err != nil { t.Fatal(err) }
    if event.Seq != 2 { t.Fatalf("seq=%d want 2", event.Seq) }
    if !json.Valid(event.Payload) { t.Fatal("invalid payload") }
    sessions, err := store.List(context.Background(), workspace)
    if err != nil || len(sessions) != 1 { t.Fatalf("list=%v err=%v", sessions, err) }
}
```

- [ ] **Step 2: Run the test and verify package failure**

Run: `go test ./internal/session/jsonl -run TestCreateAppendAndList -v`

Expected: FAIL because `jsonl` does not exist.

- [ ] **Step 3: Implement deterministic layout and durable append**

Use these exact types and ordering rules:

```go
// internal/session/jsonl/layout.go
package jsonl

import (
    "crypto/sha256"
    "encoding/hex"
    "fmt"
    "os"
    "path/filepath"

    "github.com/muratmirgun/yordam/internal/domain"
)

func WorkspaceFromPath(path string) (domain.Workspace, error) {
    canonical, err := filepath.EvalSymlinks(path)
    if err != nil { return domain.Workspace{}, err }
    canonical, err = filepath.Abs(canonical)
    if err != nil { return domain.Workspace{}, err }
    sum := sha256.Sum256([]byte(canonical))
    return domain.Workspace{ID: hex.EncodeToString(sum[:]), CanonicalPath: canonical}, nil
}

func ensureDir(path string) error {
    clean:=filepath.Clean(path)
    missing:=[]string{}
    for cursor:=clean;;cursor=filepath.Dir(cursor){
        if info,err:=os.Stat(cursor);err==nil{if !info.IsDir(){return fmt.Errorf("%q is not a directory",cursor)};break}else if !os.IsNotExist(err){return err}
        missing=append(missing,cursor)
        if filepath.Dir(cursor)==cursor{return fmt.Errorf("no existing ancestor for %q",clean)}
    }
    for index:=len(missing)-1;index>=0;index--{
        if err:=os.Mkdir(missing[index],0o700);err!=nil{return err}
        if err:=syncDir(filepath.Dir(missing[index]));err!=nil{return err}
    }
    return nil
}

func syncDir(path string) error { directory,err:=os.Open(path);if err!=nil{return err};defer directory.Close();return directory.Sync() }
```

```go
// internal/session/jsonl/store.go
package jsonl

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "os"
    "path/filepath"
    "sort"
    "sync"
    "time"

    "github.com/oklog/ulid/v2"
    "github.com/muratmirgun/yordam/internal/domain"
)

type Options struct { Clock func() time.Time; Entropy io.Reader }

type Store struct {
    root string
    clock func() time.Time
    entropy io.Reader
    mu sync.Mutex
}

func New(root string, opts Options) *Store {
    if opts.Clock == nil { opts.Clock = time.Now }
    if opts.Entropy == nil { opts.Entropy = ulid.DefaultEntropy() }
    return &Store{root: root, clock: opts.Clock, entropy: opts.Entropy}
}

func (s *Store) nextID() (string, error) { id, err := ulid.New(ulid.Timestamp(s.clock()), s.entropy); return id.String(), err }
func (s *Store) workspaceDir(id string) string { return filepath.Join(s.root, "workspaces", id) }
func (s *Store) sessionDir(workspaceID, sessionID string) string { return filepath.Join(s.workspaceDir(workspaceID), "sessions", sessionID) }

func (s *Store) Create(ctx context.Context, workspace domain.Workspace, mode domain.PermissionMode, selection domain.ModelSelection) (domain.Session, error) {
    s.mu.Lock(); defer s.mu.Unlock()
    if err := ctx.Err(); err != nil { return domain.Session{}, err }
    if err := mode.Validate(); err != nil { return domain.Session{}, err }
    id, err := s.nextID(); if err != nil { return domain.Session{}, err }
    now := s.clock().UTC()
    session := domain.Session{ID:id, Workspace:workspace, Title:"New session", Mode:mode, Selection:selection, CreatedAt:now, UpdatedAt:now}
    dir := s.sessionDir(workspace.ID, id)
    if err := ensureDir(filepath.Join(dir, "artifacts")); err != nil { return domain.Session{}, err }
    if err := s.ensureWorkspace(workspace); err != nil { return domain.Session{}, err }
    if err := writeJSONAtomic(filepath.Join(dir, "metadata.json"), session); err != nil { return domain.Session{}, err }
    event, err := s.appendLocked(session, domain.EventSessionCreated, session)
    if err != nil { return domain.Session{}, err }
    session.LastSeq = event.Seq
    if err := writeJSONAtomic(filepath.Join(dir, "metadata.json"), session); err != nil { return domain.Session{}, err }
    return session, nil
}

func (s *Store) Append(ctx context.Context, sessionID string, kind domain.EventKind, payload any) (domain.DurableEvent, error) {
    s.mu.Lock(); defer s.mu.Unlock()
    if err := ctx.Err(); err != nil { return domain.DurableEvent{}, err }
    session, err := s.findSession(sessionID); if err != nil { return domain.DurableEvent{}, err }
    return s.appendLocked(session, kind, payload)
}

func (s *Store) appendLocked(session domain.Session, kind domain.EventKind, payload any) (domain.DurableEvent, error) {
    raw, err := json.Marshal(payload); if err != nil { return domain.DurableEvent{}, err }
    id, err := s.nextID(); if err != nil { return domain.DurableEvent{}, err }
    event := domain.DurableEvent{SchemaVersion:1, EventID:id, SessionID:session.ID, Seq:session.LastSeq+1, Time:s.clock().UTC(), Kind:kind, Payload:raw}
    if err := event.Validate(); err != nil { return domain.DurableEvent{}, err }
    path := filepath.Join(s.sessionDir(session.Workspace.ID, session.ID), "events.jsonl")
    durableSeq, err := lastCompleteSequence(path)
    if err != nil { return domain.DurableEvent{}, err }
    if durableSeq != session.LastSeq { return domain.DurableEvent{}, fmt.Errorf("session sequence mismatch: metadata=%d log=%d", session.LastSeq, durableSeq) }
    file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600); if err != nil { return domain.DurableEvent{}, err }
    encoded, err := json.Marshal(event); if err == nil { _, err = file.Write(append(encoded, '\n')) }
    if err == nil { err = file.Sync() }
    closeErr := file.Close(); if err == nil { err = closeErr }
    if err != nil { return domain.DurableEvent{}, err }
    session.LastSeq, session.UpdatedAt = event.Seq, event.Time
    if err := writeJSONAtomic(filepath.Join(s.sessionDir(session.Workspace.ID, session.ID), "metadata.json"), session); err != nil { return domain.DurableEvent{}, err }
    return event, nil
}

// lastCompleteSequence returns zero for a missing/empty log, validates the
// final complete envelope, and never accepts an incomplete tail.
func lastCompleteSequence(path string) (uint64, error) {
    raw, err := os.ReadFile(path)
    if os.IsNotExist(err) { return 0, nil }
    if err != nil { return 0, err }
    if len(raw) == 0 { return 0, nil }
    if raw[len(raw)-1] != '\n' { return 0, fmt.Errorf("session log has incomplete tail") }
    trimmed := raw[:len(raw)-1]
    start := bytes.LastIndexByte(trimmed, '\n') + 1
    if len(trimmed)-start > 2<<20 { return 0, fmt.Errorf("session event exceeds 2 MiB") }
    var event domain.DurableEvent
    if err := json.Unmarshal(trimmed[start:], &event); err != nil { return 0, err }
    if err := event.Validate(); err != nil { return 0, err }
    return event.Seq, nil
}

func (s *Store) findSession(id string) (domain.Session, error) {
    pattern := filepath.Join(s.root, "workspaces", "*", "sessions", id, "metadata.json")
    matches, err := filepath.Glob(pattern); if err != nil || len(matches) != 1 { return domain.Session{}, fmt.Errorf("session %q not found", id) }
    var session domain.Session
    raw, err := os.ReadFile(matches[0]); if err != nil { return session, err }
    return session, json.Unmarshal(raw, &session)
}

func (s *Store) ensureWorkspace(workspace domain.Workspace) error {
    path := filepath.Join(s.workspaceDir(workspace.ID), "workspace.json")
    raw, err := os.ReadFile(path)
    if err == nil {
        var stored domain.Workspace
        if json.Unmarshal(raw, &stored) != nil || stored.ID != workspace.ID || stored.CanonicalPath != workspace.CanonicalPath { return fmt.Errorf("workspace identity mismatch for %q", workspace.ID) }
        return nil
    }
    if !os.IsNotExist(err) { return err }
    return writeJSONAtomic(path, workspace)
}

func (s *Store) List(ctx context.Context, workspace domain.Workspace) ([]domain.SessionSummary, error) {
    if err := ctx.Err(); err != nil { return nil, err }
    paths, err := filepath.Glob(filepath.Join(s.workspaceDir(workspace.ID), "sessions", "*", "metadata.json")); if err != nil { return nil, err }
    out := make([]domain.SessionSummary, 0, len(paths))
    for _, path := range paths {
        var session domain.Session
        raw, err := os.ReadFile(path); if err != nil { return nil, err }
        if err := json.Unmarshal(raw, &session); err != nil { return nil, err }
        out = append(out, domain.SessionSummary{ID:session.ID, Title:session.Title, Mode:session.Mode, UpdatedAt:session.UpdatedAt})
    }
    sort.Slice(out, func(i,j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
    return out, nil
}

func writeJSONAtomic(path string, value any) error {
    if err := ensureDir(filepath.Dir(path)); err != nil { return err }
    raw, err := json.MarshalIndent(value, "", "  "); if err != nil { return err }
    tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*"); if err != nil { return err }
    name := tmp.Name()
    defer os.Remove(name)
    if err := tmp.Chmod(0o600); err != nil { tmp.Close(); return err }
    if _, err := tmp.Write(raw); err != nil { tmp.Close(); return err }
    if err := tmp.Sync(); err != nil { tmp.Close(); return err }
    if err := tmp.Close(); err != nil { return err }
    if err := os.Rename(name, path); err != nil { return err }
    dir, err := os.Open(filepath.Dir(path)); if err != nil { return err }
    defer dir.Close()
    return dir.Sync()
}

```

- [ ] **Step 4: Run storage tests and inspect persisted permissions**

Run: `gofmt -w internal/session/jsonl && go test ./internal/session/jsonl -run TestCreateAppendAndList -v`

Expected: PASS. Then run `go test ./...` and expect PASS.

Add `TestWorkspaceIdentityMismatchRejected`: precreate `workspace.json` under a chosen ID with a different canonical path, call `Create` with that same ID, and assert the mismatch error occurs without overwriting the file.

- [ ] **Step 5: Commit create/append/list**

```bash
git add internal/session/jsonl
git commit -m "feat: persist append-only sessions"
```

### Task 5: Add artifacts, replay validation, and crash recovery

**Files:**
- Create: `internal/session/jsonl/artifact.go`
- Create: `internal/session/jsonl/replay.go`
- Modify: `internal/session/jsonl/store.go`
- Test: `internal/session/jsonl/recovery_test.go`

**Interfaces:**
- Consumes: `Store` from Task 4 and the `ports.ArtifactStore`/`ports.SessionStore` contracts.
- Produces: `Store.Load`, `Store.Put`, `Store.Open`, final-line recovery, read-only earlier-corruption behavior.

- [ ] **Step 1: Write exact recovery and unmatched-mutation tests**

```go
package jsonl_test

import (
    "context"
    "os"
    "path/filepath"
    "strings"
    "testing"
    "time"

    "github.com/muratmirgun/yordam/internal/domain"
    "github.com/muratmirgun/yordam/internal/session/jsonl"
)

func TestLoadRecoversOnlyIncompleteFinalLine(t *testing.T) {
    root := t.TempDir()
    store := jsonl.New(root, jsonl.Options{Clock: func() time.Time { return time.Unix(1,0).UTC() }, Entropy: strings.NewReader(strings.Repeat("b", 2048))})
    workspace, _ := jsonl.WorkspaceFromPath(t.TempDir())
    session, err := store.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile:"p", Model:"m"})
    if err != nil { t.Fatal(err) }
    if _, err := store.Append(context.Background(), session.ID, domain.EventToolStarted, map[string]string{"call_id":"c1"}); err != nil { t.Fatal(err) }
    path := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "events.jsonl")
    file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0); if err != nil { t.Fatal(err) }
    if _, err := file.WriteString(`{"schema_version":1`); err != nil { t.Fatal(err) }
    file.Close()

    replay, err := store.Load(context.Background(), session.ID)
    if err != nil { t.Fatal(err) }
    if replay.ReadOnly { t.Fatal("recoverable tail opened read-only") }
    if !strings.Contains(replay.RecoveryNote, "incomplete final line") { t.Fatalf("note=%q", replay.RecoveryNote) }
    if replay.Events[len(replay.Events)-1].Kind != domain.EventTurnInterrupted { t.Fatalf("last=%s", replay.Events[len(replay.Events)-1].Kind) }
}
```

- [ ] **Step 2: Run the recovery test and observe missing `Load`**

Run: `go test ./internal/session/jsonl -run TestLoadRecoversOnlyIncompleteFinalLine -v`

Expected: FAIL because `Store.Load` is undefined.

- [ ] **Step 3: Implement artifact bounds and replay rules**

The implementation must use these exact constants and branches:

```go
// internal/session/jsonl/artifact.go
package jsonl

import (
    "context"
    "fmt"
    "io"
    "os"
    "path/filepath"

    "github.com/muratmirgun/yordam/internal/domain"
)

const MaxArtifactBytes int64 = 10 << 20

func (s *Store) Put(ctx context.Context, sessionID, mediaType string, src io.Reader, limit int64) (domain.Artifact, error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    return s.putLocked(ctx, sessionID, mediaType, src, limit)
}

func (s *Store) putLocked(ctx context.Context, sessionID, mediaType string, src io.Reader, limit int64) (domain.Artifact, error) {
    if err := ctx.Err(); err != nil { return domain.Artifact{}, err }
    if limit <= 0 || limit > MaxArtifactBytes { limit = MaxArtifactBytes }
    session, err := s.findSession(sessionID); if err != nil { return domain.Artifact{}, err }
    id, err := s.nextID(); if err != nil { return domain.Artifact{}, err }
    path := filepath.Join(s.sessionDir(session.Workspace.ID, session.ID), "artifacts", id+".bin")
    file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600); if err != nil { return domain.Artifact{}, err }
    written, copyErr := io.Copy(file, io.LimitReader(src, limit+1))
    truncated := written > limit
    if truncated { if err := file.Truncate(limit); err != nil { copyErr = err }; written = limit }
    if copyErr == nil { copyErr = file.Sync() }
    closeErr := file.Close(); if copyErr == nil { copyErr = closeErr }
    if copyErr != nil { return domain.Artifact{}, copyErr }
    return domain.Artifact{ID:id, SessionID:sessionID, MediaType:mediaType, Path:path, Size:written, Truncated:truncated}, nil
}

func (s *Store) Open(ctx context.Context, artifact domain.Artifact) (io.ReadCloser, error) {
    if err := ctx.Err(); err != nil { return nil, err }
    clean := filepath.Clean(artifact.Path)
    root := filepath.Clean(s.root) + string(os.PathSeparator)
    if len(clean) <= len(root) || clean[:len(root)] != root { return nil, fmt.Errorf("artifact path outside store") }
    return os.Open(clean)
}
```

```go
// internal/session/jsonl/replay.go
package jsonl

import (
    "bufio"
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"

    "github.com/muratmirgun/yordam/internal/domain"
)

func (s *Store) Load(ctx context.Context, sessionID string) (domain.SessionReplay, error) {
    s.mu.Lock(); defer s.mu.Unlock()
    if err := ctx.Err(); err != nil { return domain.SessionReplay{}, err }
    session, err := s.findSession(sessionID); if err != nil { return domain.SessionReplay{}, err }
    path := filepath.Join(s.sessionDir(session.Workspace.ID, session.ID), "events.jsonl")
    raw, err := os.ReadFile(path); if err != nil { return domain.SessionReplay{}, err }
    note := ""
    if len(raw) > 0 && raw[len(raw)-1] != '\n' {
        cut := bytes.LastIndexByte(raw, '\n') + 1
        tail := append([]byte(nil), raw[cut:]...)
        if _, err := s.putLocked(ctx, sessionID, "application/x-yordam-recovery", bytes.NewReader(tail), MaxArtifactBytes); err != nil { return domain.SessionReplay{}, err }
        if err := os.Truncate(path, int64(cut)); err != nil { return domain.SessionReplay{}, err }
        raw = raw[:cut]
        note = "recovered incomplete final line"
    }
    scanner := bufio.NewScanner(bytes.NewReader(raw))
    scanner.Buffer(make([]byte, 64*1024), 2*1024*1024)
    events := make([]domain.DurableEvent, 0)
    expected := uint64(1)
    for scanner.Scan() {
        var event domain.DurableEvent
        if err := json.Unmarshal(scanner.Bytes(), &event); err != nil || event.Validate() != nil || event.Seq != expected {
            return domain.SessionReplay{Session:session, Events:events, RecoveryNote:fmt.Sprintf("corruption at sequence %d", expected), ReadOnly:true}, nil
        }
        events = append(events, event)
        expected++
    }
    if err := scanner.Err(); err != nil { return domain.SessionReplay{}, err }
    validatedSeq := expected - 1
    if session.LastSeq != validatedSeq {
        session.LastSeq = validatedSeq
        if err := writeJSONAtomic(filepath.Join(s.sessionDir(session.Workspace.ID, session.ID), "metadata.json"), session); err != nil { return domain.SessionReplay{}, err }
    }
    openCalls := map[string]bool{}
    for _, event := range events {
        var payload map[string]any
        _ = json.Unmarshal(event.Payload, &payload)
        callID, _ := payload["call_id"].(string)
        if event.Kind == domain.EventToolStarted { openCalls[callID] = true }
        if event.Kind == domain.EventToolResult { delete(openCalls, callID) }
    }
    if len(openCalls) > 0 {
        event, err := s.appendLocked(session, domain.EventTurnInterrupted, map[string]any{"reason":"unmatched tool.started", "call_count":len(openCalls)})
        if err != nil { return domain.SessionReplay{}, err }
        events = append(events, event)
        note = joinNote(note, "marked unmatched tool call interrupted")
    }
    return domain.SessionReplay{Session:session, Events:events, RecoveryNote:note}, nil
}

func joinNote(left, right string) string {
    if left == "" { return right }
    return left + "; " + right
}
```

`Load` already holds `Store.mu`, so it calls `putLocked` directly. Exported `Put` acquires the lock exactly once. `Load` also repairs metadata `LastSeq` to the last validated event before appending recovery events, preventing duplicate sequence numbers. `lastCompleteSequence` scans with the same 2 MiB line limit and validates the final complete event; add a test where metadata lags a durable event, assert direct `Append` rejects it, then `Load` repairs metadata and the following append receives the next unique sequence. Keep the two-second timeout on `TestLoadRecoversOnlyIncompleteFinalLine` as a deadlock regression guard.

- [ ] **Step 4: Run focused recovery/artifact tests and full suite**

Run: `gofmt -w internal/session/jsonl && go test -timeout 2s ./internal/session/jsonl -v && go test ./...`

Expected: PASS; no deadlock; recovered session ends in `turn.interrupted`.

- [ ] **Step 5: Commit recovery and artifacts**

```bash
git add internal/session/jsonl
git commit -m "feat: recover and replay session logs"
```

### Task 6: Prove port conformance and foundation gate

**Files:**
- Create: `internal/session/jsonl/conformance_test.go`
- Modify: `docs/superpowers/plans/2026-07-13-yordam-v0.1-roadmap.md`

**Interfaces:**
- Consumes: all Phase 1 packages.
- Produces: compile-time conformance and recorded Gate 1 completion.

- [ ] **Step 1: Add compile-time conformance and workspace mismatch tests**

```go
package jsonl_test

import (
    "context"
    "testing"

    "github.com/muratmirgun/yordam/internal/domain"
    "github.com/muratmirgun/yordam/internal/ports"
    "github.com/muratmirgun/yordam/internal/session/jsonl"
)

var _ ports.SessionStore = (*jsonl.Store)(nil)
var _ ports.ArtifactStore = (*jsonl.Store)(nil)

func TestListDoesNotCrossWorkspace(t *testing.T) {
    store := jsonl.New(t.TempDir(), jsonl.Options{})
    first, _ := jsonl.WorkspaceFromPath(t.TempDir())
    second, _ := jsonl.WorkspaceFromPath(t.TempDir())
    if _, err := store.Create(context.Background(), first, domain.ModeAsk, domain.ModelSelection{Profile:"p", Model:"m"}); err != nil { t.Fatal(err) }
    got, err := store.List(context.Background(), second)
    if err != nil { t.Fatal(err) }
    if len(got) != 0 { t.Fatalf("cross-workspace sessions: %v", got) }
}
```

- [ ] **Step 2: Run the complete Phase 1 test surface**

Run: `go test ./internal/domain/... ./internal/config/... ./internal/ports/... ./internal/session/...`

Expected: PASS for every package.

- [ ] **Step 3: Run formatting and static checks**

```bash
test -z "$(gofmt -l internal)"
go vet ./internal/domain/... ./internal/config/... ./internal/ports/... ./internal/session/...
git diff --check
```

Expected: all commands exit 0 and print no diagnostics.

- [ ] **Step 4: Mark Gate 1 complete only after evidence exists**

Edit the roadmap checkbox from:

```markdown
- [ ] **Gate 1:**
```

to:

```markdown
- [x] **Gate 1:**
```

Do not change later gate checkboxes.

- [ ] **Step 5: Commit Phase 1 conformance**

```bash
git add internal/session/jsonl/conformance_test.go docs/superpowers/plans/2026-07-13-yordam-v0.1-roadmap.md
git commit -m "test: verify foundation session gate"
```
