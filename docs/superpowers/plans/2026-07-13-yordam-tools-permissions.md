# Yordam Tools and Permissions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement path-safe local tools, the approved three-mode permission system, exact edit previews, bounded/redacted output, and cancellable unsandboxed shell execution.

**Architecture:** Every tool has a non-mutating `Prepare` stage that canonicalizes scope and produces permission UI data before `Execute`. The permission policy evaluates only prepared calls. Prepared edit objects retain verified before/after state in memory and recheck the preimage immediately before atomic replacement.

**Tech Stack:** Go 1.26.4, standard library, `rg` when present with a Go fallback, `github.com/pmezard/go-difflib` v1.0.0.

## Global Constraints

- Complete Phases 1-2 and Gates 1-2 first.
- Built-in tools are exactly `read`, `search`, `edit`, and `shell`.
- File scope uses canonical paths and symlink resolution; lexical prefix checks alone are forbidden.
- `safe`: inside read/search allowed, edit/shell denied, outside file access denied.
- `ask`: inside read/search allowed; outside file access, every edit, and every shell command require approval.
- `auto`: inside file tools allowed; outside file access asks; shell requires the session's trusted-execution acknowledgement.
- Permission options are `allow once`, exact-scope `allow session`, and `deny`.
- Provider key variables are stripped from child process environments.
- Tool output retained limit: 10 MiB; model-facing excerpt: 32 KiB.
- Shell default timeout: 120 seconds; maximum: 30 minutes.
- Mutating tools do not retry automatically.
- Every `Execute` re-resolves the authorized path immediately before opening or replacing it and fails if the canonical scope changed after `Prepare`.

---

### Task 1: Add prepared-tool contracts and canonical scope resolution

**Files:**
- Modify: `internal/domain/tool.go`
- Modify: `internal/ports/tool.go`
- Modify: `internal/agent/runner.go`
- Modify: `internal/session/jsonl/replay.go`
- Create: `internal/scope/path.go`
- Test: `internal/scope/path_test.go`
- Test: `internal/agent/prepare_test.go`
- Test: `internal/session/jsonl/file_recovery_test.go`

**Interfaces:**
- Consumes: Phase 2 `Runner` and tool registry.
- Produces: `domain.PreparedToolRequest`, `ports.PreparedTool`, `Tool.Prepare`, and `scope.Resolve`.

- [ ] **Step 1: Write traversal, symlink, missing-leaf, and prepare-before-policy tests**

```go
package scope_test

import (
    "os"
    "path/filepath"
    "testing"

    "github.com/muratmirgun/yordam/internal/scope"
)

func TestResolveDetectsInsideOutsideAndSymlinkEscape(t *testing.T) {
    root := t.TempDir()
    outside := t.TempDir()
    if err := os.WriteFile(filepath.Join(root, "inside.txt"), []byte("ok"), 0o600); err != nil { t.Fatal(err) }
    if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil { t.Fatal(err) }
    inside, err := scope.Resolve(root, "inside.txt", false)
    if err != nil || !inside.Inside { t.Fatalf("inside=%#v err=%v", inside, err) }
    escaped, err := scope.Resolve(root, filepath.Join("escape", "new.txt"), true)
    if err != nil { t.Fatal(err) }
    if escaped.Inside { t.Fatalf("symlink escape accepted: %#v", escaped) }
    missing, err := scope.Resolve(root, "new.txt", true)
    if err != nil || !missing.Inside { t.Fatalf("missing=%#v err=%v", missing, err) }
}
```

In `internal/agent/prepare_test.go`, use a fake tool whose `Prepare` records a call and a fake policy that fails unless the prepared call contains a canonical scope. Assert call order `prepare, policy, execute`.

- [ ] **Step 2: Run focused tests**

Run: `go test ./internal/scope ./internal/agent -run 'TestResolve|TestRunnerPrepares' -v`

Expected: FAIL because prepared-tool contracts and scope package are missing.

- [ ] **Step 3: Define prepared calls and canonical resolution**

```go
// additions to internal/domain/tool.go
type FileChangePlan struct {
    CallID string `json:"call_id"`
    Path string `json:"path"`
    ExpectedSHA256 string `json:"expected_sha256"`
    PlannedSHA256 string `json:"planned_sha256"`
    Diff string `json:"diff"`
    ArtifactIDs []string `json:"artifact_ids,omitempty"`
}
type FileChange struct {
    CallID string `json:"call_id"`
    Path string `json:"path"`
    BeforeSHA256 string `json:"before_sha256"`
    AfterSHA256 string `json:"after_sha256"`
    Diff string `json:"diff"`
    ArtifactIDs []string `json:"artifact_ids,omitempty"`
}
type PreparedToolRequest struct {
    Request         ToolRequest `json:"request"`
    CanonicalScope  string      `json:"canonical_scope"`
    InsideWorkspace bool        `json:"inside_workspace"`
    ProposedDiff    string      `json:"proposed_diff,omitempty"`
    Summary         string      `json:"summary"`
    FilePlan        *FileChangePlan `json:"file_plan,omitempty"`
}

// add to ToolResult
FileChange *FileChange `json:"file_change,omitempty"`
```

```go
// replace internal/ports/tool.go contracts
package ports

import (
    "context"
    "github.com/muratmirgun/yordam/internal/domain"
)

type PreparedTool interface {
    Preview() domain.PreparedToolRequest
    Execute(context.Context) domain.ToolResult
}

type Tool interface {
    Descriptor() domain.ToolDescriptor
    Prepare(context.Context, domain.ToolRequest) (PreparedTool, error)
}

type ToolRegistry interface {
    Descriptors() []domain.ToolDescriptor
    Lookup(name string) (Tool, bool)
}
```

```go
// internal/scope/path.go
package scope

import (
    "fmt"
    "os"
    "path/filepath"
    "strings"
)

type Resolved struct { Workspace, Path string; Inside bool }

func Resolve(workspace, target string, allowMissingLeaf bool) (Resolved, error) {
    root, err := filepath.Abs(workspace); if err != nil { return Resolved{}, err }
    root, err = filepath.EvalSymlinks(root); if err != nil { return Resolved{}, err }
    candidate := target
    if !filepath.IsAbs(candidate) { candidate = filepath.Join(root, candidate) }
    candidate, err = filepath.Abs(candidate); if err != nil { return Resolved{}, err }
    canonical, err := filepath.EvalSymlinks(candidate)
    if err != nil && allowMissingLeaf && os.IsNotExist(err) {
        parent, parentErr := filepath.EvalSymlinks(filepath.Dir(candidate)); if parentErr != nil { return Resolved{}, parentErr }
        canonical = filepath.Join(parent, filepath.Base(candidate))
    } else if err != nil { return Resolved{}, err }
    rel, err := filepath.Rel(root, canonical); if err != nil { return Resolved{}, err }
    inside := rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) && !filepath.IsAbs(rel)
    if canonical == "" { return Resolved{}, fmt.Errorf("empty canonical path") }
    return Resolved{Workspace:root, Path:canonical, Inside:inside}, nil
}
```

Update `Runner.RunTurn` so it looks up the tool and calls `Prepare` before policy evaluation. Persist the prepared preview in `tool.requested`; pass `prepared.Preview()` to policy/approver; call `prepared.Execute(ctx)` only after allow. Unknown tools produce a failed result without calling policy. When an allowed preview has `FilePlan != nil`, append and flush `file.change_planned` before `tool.started`. When a successful result has `FileChange != nil`, append `file.changed` before `tool.result`. Tests assert the planned payload contains the preimage SHA-256 and replay never calls `Execute` for a planned-but-unfinished edit.

At the same boundary, replace `ports.PermissionPrompt` with `type PermissionPrompt struct { SessionID string; Call domain.PreparedToolRequest }`. The app approval broker publishes this prepared call to the TUI, providing canonical scope, inside/outside marker, summary, and proposed diff without re-parsing raw arguments.

Extend replay bookkeeping to pair `file.change_planned` and `file.changed` by `CallID`. For an interrupted call with only a plan, read the canonical target without writing it: if its SHA-256 equals `PlannedSHA256`, include `planned after-state present` in the recovery notice; otherwise include `planned change not confirmed`. `file_recovery_test.go` covers both branches and asserts that replay performs no rename, truncate, or content write to the target.

- [ ] **Step 4: Run scope and agent tests**

Run: `gofmt -w internal/domain internal/ports internal/scope internal/agent && go test -race ./internal/scope ./internal/agent -v`

Expected: PASS; test recorder shows `prepare, policy, execute`.

- [ ] **Step 5: Commit prepared-tool boundary**

```bash
git add internal/domain/tool.go internal/ports/tool.go internal/scope internal/agent
git commit -m "refactor: authorize prepared tool calls"
```

### Task 2: Implement permission modes, grants, and auto acknowledgement

**Files:**
- Create: `internal/permission/session.go`
- Create: `internal/permission/policy.go`
- Test: `internal/permission/policy_test.go`

**Interfaces:**
- Consumes: `domain.PreparedToolRequest`, `ports.PermissionPolicy`.
- Produces: `permission.NewSession(mode) *SessionPolicy`, exact-scope grants, and auto-shell acknowledgement lifecycle.

- [ ] **Step 1: Write the complete permission matrix test**

```go
package permission_test

import (
    "context"
    "encoding/json"
    "testing"

    "github.com/muratmirgun/yordam/internal/domain"
    "github.com/muratmirgun/yordam/internal/permission"
    "github.com/muratmirgun/yordam/internal/ports"
)

func TestPermissionMatrix(t *testing.T) {
    cases := []struct { name string; mode domain.PermissionMode; tool string; mutation domain.MutationKind; inside bool; want domain.PermissionAction }{
        {"safe read inside", domain.ModeSafe, "read", domain.MutationReadOnly, true, domain.PermissionAllow},
        {"safe read outside", domain.ModeSafe, "read", domain.MutationReadOnly, false, domain.PermissionDeny},
        {"safe edit", domain.ModeSafe, "edit", domain.MutationFile, true, domain.PermissionDeny},
        {"safe shell", domain.ModeSafe, "shell", domain.MutationProcess, true, domain.PermissionDeny},
        {"ask read inside", domain.ModeAsk, "read", domain.MutationReadOnly, true, domain.PermissionAllow},
        {"ask read outside", domain.ModeAsk, "read", domain.MutationReadOnly, false, domain.PermissionAsk},
        {"ask edit", domain.ModeAsk, "edit", domain.MutationFile, true, domain.PermissionAsk},
        {"ask shell", domain.ModeAsk, "shell", domain.MutationProcess, true, domain.PermissionAsk},
        {"auto read inside", domain.ModeAuto, "read", domain.MutationReadOnly, true, domain.PermissionAllow},
        {"auto edit inside", domain.ModeAuto, "edit", domain.MutationFile, true, domain.PermissionAllow},
        {"auto edit outside", domain.ModeAuto, "edit", domain.MutationFile, false, domain.PermissionAsk},
        {"auto shell unacknowledged", domain.ModeAuto, "shell", domain.MutationProcess, true, domain.PermissionAsk},
    }
    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            policy := permission.NewSession(tc.mode)
            decision := policy.Evaluate(context.Background(), ports.PermissionContext{SessionID:"s", Mode:tc.mode, Workspace:"/w"}, domain.PreparedToolRequest{Request:domain.ToolRequest{Name:tc.tool}, CanonicalScope:"/w/a", InsideWorkspace:tc.inside, Summary:tc.name})
            if decision.Action != tc.want { t.Fatalf("got=%s want=%s", decision.Action, tc.want) }
        })
    }
}
```

- [ ] **Step 2: Run matrix test**

Run: `go test ./internal/permission -run TestPermissionMatrix -v`

Expected: FAIL because permission package does not exist and the policy port still accepts raw requests.

- [ ] **Step 3: Change the policy port to prepared calls and implement session state**

```go
// internal/permission/session.go
package permission

import (
    "encoding/json"
    "sync"
    "github.com/muratmirgun/yordam/internal/domain"
)

type SessionPolicy struct { mu sync.RWMutex; mode domain.PermissionMode; grants map[string]struct{}; autoShell bool }
func NewSession(mode domain.PermissionMode) *SessionPolicy { return &SessionPolicy{mode:mode, grants:map[string]struct{}{}} }
func (p *SessionPolicy) SetMode(mode domain.PermissionMode) { p.mu.Lock(); defer p.mu.Unlock(); if p.mode == domain.ModeAuto && mode != domain.ModeAuto { p.autoShell = false }; p.mode = mode }
func (p *SessionPolicy) AcknowledgeAutoShell() { p.mu.Lock(); defer p.mu.Unlock(); if p.mode == domain.ModeAuto { p.autoShell = true } }
func (p *SessionPolicy) GrantSession(tool, scope string) { p.mu.Lock(); defer p.mu.Unlock(); p.grants[tool+"\x00"+scope] = struct{}{} }

func Restore(replay domain.SessionReplay) *SessionPolicy {
    policy := NewSession(replay.Session.Mode)
    for _, event := range replay.Events {
        switch event.Kind {
        case domain.EventModeChanged:
            var payload domain.ModeChangedPayload
            if json.Unmarshal(event.Payload, &payload) == nil { policy.SetMode(payload.Mode) }
        case domain.EventPermissionResolved:
            var payload domain.PermissionPayload
            if json.Unmarshal(event.Payload, &payload) == nil && payload.Decision.Action == domain.PermissionAllow && payload.Decision.Lifetime == domain.PermissionSession { policy.GrantSession(payload.Tool, payload.Decision.Scope) }
        case domain.EventTrustedExecutionAcknowledged:
            var payload domain.TrustedExecutionPayload
            if json.Unmarshal(event.Payload, &payload) == nil && payload.Enabled { policy.AcknowledgeAutoShell() }
        }
    }
    return policy
}
```

`Restore` replays events in order, so a later transition out of auto clears an earlier acknowledgement, while uninterrupted auto sessions regain their acknowledgement and exact-scope grants on resume.

```go
// internal/permission/policy.go
package permission

import (
    "context"
    "github.com/muratmirgun/yordam/internal/domain"
    "github.com/muratmirgun/yordam/internal/ports"
)

func (p *SessionPolicy) Evaluate(_ context.Context, _ ports.PermissionContext, call domain.PreparedToolRequest) domain.PermissionDecision {
    p.mu.RLock(); defer p.mu.RUnlock()
    mode := p.mode
    mutation := mutationFor(call.Request.Name)
    if mode == domain.ModeSafe {
        if mutation == domain.MutationReadOnly && call.InsideWorkspace { return domain.PermissionDecision{Action:domain.PermissionAllow, Lifetime:domain.PermissionOnce, Scope:call.CanonicalScope, Reason:"safe read inside workspace"} }
        return domain.PermissionDecision{Action:domain.PermissionDeny, Lifetime:domain.PermissionOnce, Scope:call.CanonicalScope, Reason:"safe mode"}
    }
    if _, ok := p.grants[call.Request.Name+"\x00"+call.CanonicalScope]; ok {
        if mode == domain.ModeAuto && mutation == domain.MutationProcess && !p.autoShell { return domain.PermissionDecision{Action:domain.PermissionAsk, Lifetime:domain.PermissionOnce, Scope:call.CanonicalScope, Reason:"shell acknowledgement required"} }
        return domain.PermissionDecision{Action:domain.PermissionAllow, Lifetime:domain.PermissionSession, Scope:call.CanonicalScope, Reason:"session grant"}
    }
    if mutation == domain.MutationReadOnly { if call.InsideWorkspace { return domain.PermissionDecision{Action:domain.PermissionAllow, Lifetime:domain.PermissionOnce, Scope:call.CanonicalScope, Reason:"read inside workspace"} }; return domain.PermissionDecision{Action:domain.PermissionAsk, Lifetime:domain.PermissionOnce, Scope:call.CanonicalScope, Reason:"outside workspace"} }
    if mode == domain.ModeAsk { return domain.PermissionDecision{Action:domain.PermissionAsk, Lifetime:domain.PermissionOnce, Scope:call.CanonicalScope, Reason:"ask mode mutation"} }
    if mutation == domain.MutationFile { if call.InsideWorkspace { return domain.PermissionDecision{Action:domain.PermissionAllow, Lifetime:domain.PermissionOnce, Scope:call.CanonicalScope, Reason:"auto file inside workspace"} }; return domain.PermissionDecision{Action:domain.PermissionAsk, Lifetime:domain.PermissionOnce, Scope:call.CanonicalScope, Reason:"outside workspace"} }
    if p.autoShell { return domain.PermissionDecision{Action:domain.PermissionAllow, Lifetime:domain.PermissionOnce, Scope:call.CanonicalScope, Reason:"trusted shell acknowledged"} }
    return domain.PermissionDecision{Action:domain.PermissionAsk, Lifetime:domain.PermissionOnce, Scope:call.CanonicalScope, Reason:"shell acknowledgement required"}
}

func mutationFor(name string) domain.MutationKind { switch name { case "read", "search": return domain.MutationReadOnly; case "edit": return domain.MutationFile; default: return domain.MutationProcess } }
```

Update `ports.PermissionPolicy.Evaluate` and runner calls to accept `domain.PreparedToolRequest`. Add `ports.PermissionGranter` with `GrantSession(tool, scope string)`. After the approver returns `allow + session`, the runner must require that the policy implements `PermissionGranter`, call it with the prepared tool name and canonical scope, and only then append `permission.resolved`. `deny` and `allow once` never create a grant.

The runner validates every approver response before use: action is allow or deny (never ask), lifetime is once or session, and `decision.Scope` exactly equals the prepared canonical scope. Any mismatch becomes a denied `permission.resolved` event with reason `invalid approval response`; execution never starts. Add a hostile-approver test returning a broader scope and assert zero tool executions/grants.

- [ ] **Step 4: Add grant-scope and mode-exit tests, then run package suite**

Add these exact grant and acknowledgement tests to `policy_test.go`:

```go
func prepared(name, scope string, inside bool) domain.PreparedToolRequest {
    return domain.PreparedToolRequest{Request:domain.ToolRequest{Name:name}, CanonicalScope:scope, InsideWorkspace:inside}
}

func TestSessionGrantMatchesToolAndExactScope(t *testing.T) {
    policy := permission.NewSession(domain.ModeAsk)
    policy.GrantSession("edit", "/w/a.go")
    allowed := policy.Evaluate(context.Background(), ports.PermissionContext{}, prepared("edit", "/w/a.go", true))
    otherPath := policy.Evaluate(context.Background(), ports.PermissionContext{}, prepared("edit", "/w/b.go", true))
    otherTool := policy.Evaluate(context.Background(), ports.PermissionContext{}, prepared("shell", "/w/a.go", true))
    if allowed.Action != domain.PermissionAllow || allowed.Lifetime != domain.PermissionSession { t.Fatalf("allowed=%#v", allowed) }
    if otherPath.Action != domain.PermissionAsk || otherTool.Action != domain.PermissionAsk { t.Fatalf("path=%#v tool=%#v", otherPath, otherTool) }
}

func TestSafeOverridesGrantsAndAutoStillRequiresShellAck(t *testing.T) {
    policy:=permission.NewSession(domain.ModeAsk)
    policy.GrantSession("edit","/w/a.go");policy.GrantSession("shell","/w\x00echo ok")
    policy.SetMode(domain.ModeSafe)
    if got:=policy.Evaluate(context.Background(),ports.PermissionContext{},prepared("edit","/w/a.go",true));got.Action!=domain.PermissionDeny{t.Fatalf("safe edit=%#v",got)}
    policy.SetMode(domain.ModeAuto)
    if got:=policy.Evaluate(context.Background(),ports.PermissionContext{},prepared("shell","/w\x00echo ok",true));got.Action!=domain.PermissionAsk{t.Fatalf("auto shell=%#v",got)}
}

func TestLeavingAutoClearsShellAcknowledgement(t *testing.T) {
    policy := permission.NewSession(domain.ModeAuto)
    shell := prepared("shell", "/w\x00echo ok", true)
    policy.AcknowledgeAutoShell()
    if got := policy.Evaluate(context.Background(), ports.PermissionContext{}, shell); got.Action != domain.PermissionAllow { t.Fatalf("acknowledged=%#v", got) }
    policy.SetMode(domain.ModeAsk)
    policy.SetMode(domain.ModeAuto)
    if got := policy.Evaluate(context.Background(), ports.PermissionContext{}, shell); got.Action != domain.PermissionAsk { t.Fatalf("re-entered=%#v", got) }
}

func event(kind domain.EventKind, payload any) domain.DurableEvent { raw,err:=json.Marshal(payload);if err!=nil{panic(err)};return domain.DurableEvent{Kind:kind,Payload:raw} }
func replayWith(mode domain.PermissionMode, events ...domain.DurableEvent) domain.SessionReplay { return domain.SessionReplay{Session:domain.Session{Mode:mode},Events:events} }

func TestRestoreReplaysGrantAndAutoAcknowledgement(t *testing.T) {
    replay := replayWith(domain.ModeAuto,
        event(domain.EventPermissionResolved, domain.PermissionPayload{Tool:"edit", Decision:domain.PermissionDecision{Action:domain.PermissionAllow, Lifetime:domain.PermissionSession, Scope:"/w/a.go"}}),
        event(domain.EventTrustedExecutionAcknowledged, domain.TrustedExecutionPayload{Enabled:true}),
    )
    policy := permission.Restore(replay)
    if got:=policy.Evaluate(context.Background(), ports.PermissionContext{}, prepared("edit","/w/a.go",true)); got.Action!=domain.PermissionAllow || got.Lifetime!=domain.PermissionSession { t.Fatalf("grant=%#v",got) }
    if got:=policy.Evaluate(context.Background(), ports.PermissionContext{}, prepared("shell","/w\x00echo ok",true)); got.Action!=domain.PermissionAllow { t.Fatalf("shell=%#v",got) }
}
```

The normalized shell-scope assertion belongs in Task 6, where `shell.Prepare` exists. Run:

`gofmt -w internal/permission internal/ports internal/agent && go test -race ./internal/permission ./internal/agent -v`

Expected: PASS.

- [ ] **Step 5: Commit permission policy**

```bash
git add internal/permission internal/ports/permission.go internal/agent
git commit -m "feat: enforce session permission modes"
```

### Task 3: Add redaction and bounded output buffering

**Files:**
- Create: `internal/secret/redact.go`
- Create: `internal/logging/logger.go`
- Create: `internal/tools/output/buffer.go`
- Modify: `internal/session/jsonl/store.go`
- Modify: `internal/agent/runner.go`
- Test: `internal/secret/redact_test.go`
- Test: `internal/logging/logger_test.go`
- Test: `internal/tools/output/buffer_test.go`
- Test: `internal/session/jsonl/redaction_test.go`

**Interfaces:**
- Consumes: `ports.ArtifactStore`.
- Produces: `secret.New(values...)`, opt-in redacted JSON logging, and `output.Buffer.Result` with 32 KiB excerpt and 10 MiB retained limit.

- [ ] **Step 1: Write longest-secret-first and output-limit tests**

```go
func TestRedactorRemovesOverlappingSecrets(t *testing.T) {
    redactor := secret.New("abc", "abcdef")
    if got := redactor.String("x abcdef abc y"); got != "x [REDACTED] [REDACTED] y" { t.Fatalf("got=%q", got) }
}

func TestBufferCapsExcerptAndRetention(t *testing.T) {
    store := &fakeArtifactStore{}
    buffer := output.New(output.Options{SessionID:"s", Artifacts:store, Redact:secret.New("secret")})
    input := strings.Repeat("x", output.ModelExcerptBytes) + "secret" + strings.Repeat("y", 100)
    if _, err := buffer.Write([]byte(input)); err != nil { t.Fatal(err) }
    result, err := buffer.Result(context.Background())
    if err != nil { t.Fatal(err) }
    if strings.Contains(result.Content, "secret") || len(result.Content) > output.ModelExcerptBytes { t.Fatalf("result=%#v", result) }
    if len(result.ArtifactIDs) != 1 { t.Fatalf("artifacts=%v", result.ArtifactIDs) }
}

func TestLoggerDropsAuthorizationAndRedactsValues(t *testing.T) {
    var destination bytes.Buffer
    logger:=logging.New(&destination,secret.New("top-secret"))
    if err:=logger.Event("provider_error",map[string]any{"authorization":"Bearer top-secret","message":"failed top-secret"});err!=nil{t.Fatal(err)}
    got:=destination.String()
    if strings.Contains(strings.ToLower(got),"authorization")||strings.Contains(got,"top-secret"){t.Fatalf("log=%s",got)}
}
```

- [ ] **Step 2: Run focused secret/output tests**

Run: `go test ./internal/secret ./internal/tools/output -v`

Expected: FAIL because packages are missing.

- [ ] **Step 3: Implement exact redaction and limits**

```go
// internal/secret/redact.go
package secret

import (
    "encoding/json"
    "slices"
    "strings"
)

type Redactor struct { values []string }
func New(values ...string) Redactor { values = slices.DeleteFunc(values, func(v string) bool { return v == "" }); slices.SortFunc(values, func(a,b string) int { return len(b)-len(a) }); return Redactor{values:values} }
func (r Redactor) String(value string) string { for _, secret := range r.values { value = strings.ReplaceAll(value, secret, "[REDACTED]") }; return value }
func (r Redactor) Bytes(value []byte) []byte { return []byte(r.String(string(value))) }
func (r Redactor) JSON(value any) (json.RawMessage,error) {
    raw,err:=json.Marshal(value);if err!=nil{return nil,err}
    var tree any;if err:=json.Unmarshal(raw,&tree);err!=nil{return nil,err}
    tree=redactTree(tree,r)
    return json.Marshal(tree)
}
func redactTree(value any,r Redactor) any {
    switch typed:=value.(type) {
    case string: return r.String(typed)
    case []any: for index,item:=range typed{typed[index]=redactTree(item,r)};return typed
    case map[string]any: for key,item:=range typed{typed[key]=redactTree(item,r)};return typed
    default: return value
    }
}
```

Map keys remain schema names and are not rewritten.

`logging.New(nil, redactor)` is disabled and writes nothing. A non-nil writer produces one JSON object per line under a mutex. `Event(name, fields)` recursively removes map keys equal to `authorization`, `proxy-authorization`, `api_key`, or `api-key` case-insensitively, marshals the remaining object, applies `Redactor.Bytes`, appends a newline, and writes it once. It never logs HTTP request/response bodies unless the caller explicitly passes an already bounded string.

```go
// internal/tools/output/buffer.go
package output

import (
    "bytes"
    "context"
    "sync"
    "github.com/muratmirgun/yordam/internal/domain"
    "github.com/muratmirgun/yordam/internal/ports"
    "github.com/muratmirgun/yordam/internal/secret"
)

const ModelExcerptBytes = 32 << 10
const RetainedBytes = 10 << 20

type Options struct { SessionID string; Artifacts ports.ArtifactStore; Redact secret.Redactor }
type Buffer struct { mu sync.Mutex; opts Options; data bytes.Buffer; truncated bool }
func New(opts Options) *Buffer { return &Buffer{opts:opts} }
func (b *Buffer) Write(p []byte) (int,error) { b.mu.Lock();defer b.mu.Unlock();original := len(p); remaining := RetainedBytes-b.data.Len(); if remaining <= 0 { b.truncated = true; return original,nil }; if len(p)>remaining { p=p[:remaining];b.truncated=true }; _,err:=b.data.Write(p); return original,err }
func (b *Buffer) Snapshot() (string,bool) { b.mu.Lock();defer b.mu.Unlock();redacted:=b.opts.Redact.Bytes(b.data.Bytes());if len(redacted)>ModelExcerptBytes{redacted=redacted[len(redacted)-ModelExcerptBytes:]};return string(redacted),b.truncated }
func (b *Buffer) Result(ctx context.Context) (domain.ToolResult,error) { b.mu.Lock();defer b.mu.Unlock();redacted:=b.opts.Redact.Bytes(b.data.Bytes()); excerpt:=redacted; if len(excerpt)>ModelExcerptBytes { excerpt=excerpt[len(excerpt)-ModelExcerptBytes:] }; result:=domain.ToolResult{Status:domain.ToolSucceeded, Content:string(excerpt), Truncated:b.truncated}; if len(redacted)>len(excerpt) || b.truncated { artifact,err:=b.opts.Artifacts.Put(ctx,b.opts.SessionID,"text/plain",bytes.NewReader(redacted),RetainedBytes); if err!=nil{return result,err}; result.ArtifactIDs=[]string{artifact.ID} }; return result,nil }
```

Every built-in tool receives an `output.Options` value at construction and routes all model-facing or persisted free-form output through a fresh `Buffer`: read text, search matches, edit diffs/messages, shell stdout/stderr, and Git status/diff. No tool stores an unbounded duplicate outside the buffer. Structured metadata may retain hashes, paths, exit codes, and artifact IDs; any structured `Diff` string is the same redacted 32 KiB excerpt returned by the buffer.

Add `Sanitize func(any) (json.RawMessage,error)` to `jsonl.Options`/`Store`, defaulting to `json.Marshal`; `appendLocked` calls it instead of `json.Marshal(payload)`. Bootstrap passes `redactor.JSON`. Add `Redact func(string) string` to `agent.Runner`, default identity; sanitize the prompt before both durable append and the current model context. `redaction_test.go` appends nested string fields containing a sentinel and recursively scans `events.jsonl` plus artifacts to assert zero sentinel bytes. This storage boundary is defense in depth; provider errors, previews, tool output, and debug logging must still redact before reaching UI/runtime events.

- [ ] **Step 4: Run tests and race detector**

Run: `gofmt -w internal/secret internal/logging internal/tools/output && go test -race ./internal/secret ./internal/logging ./internal/tools/output -v`

Expected: PASS; no result or fake artifact contains the configured secret.

- [ ] **Step 5: Commit output safety**

```bash
git add internal/secret internal/logging internal/tools/output
git commit -m "feat: redact and bound tool output"
```

### Task 4: Implement `read` and `search`

**Files:**
- Create: `internal/tools/read/read.go`
- Create: `internal/tools/search/search.go`
- Create: `internal/tools/search/fallback.go`
- Test: `internal/tools/read/read_test.go`
- Test: `internal/tools/search/search_test.go`

**Interfaces:**
- Consumes: prepared-tool contract and `scope.Resolve`.
- Produces: UTF-8 line-numbered reads and capped text search with `rg`/Go fallback parity.

- [ ] **Step 1: Write read/search behavioral tests**

```go
func TestReadRejectsBinaryAndNumbersLines(t *testing.T) {
    root := t.TempDir()
    os.WriteFile(filepath.Join(root,"a.txt"), []byte("one\ntwo\nthree\n"), 0o600)
    tool := read.New(read.Options{Workspace:root, Output:output.Options{SessionID:"s", Artifacts:&fakeArtifactStore{}}})
    prepared, err := tool.Prepare(context.Background(), request("read", `{"path":"a.txt","offset":2,"limit":1}`, root))
    if err != nil { t.Fatal(err) }
    got := prepared.Execute(context.Background())
    if got.Content != "2: two" { t.Fatalf("content=%q", got.Content) }
    os.WriteFile(filepath.Join(root,"b.bin"), []byte{'a',0,'b'}, 0o600)
    if _, err := tool.Prepare(context.Background(), request("read", `{"path":"b.bin"}`, root)); err == nil { t.Fatal("binary accepted") }
}

func TestSearchFallbackCapsAndOrdersMatches(t *testing.T) {
    root := t.TempDir()
    os.WriteFile(filepath.Join(root,"b.txt"), []byte("needle\n"), 0o600)
    os.WriteFile(filepath.Join(root,"a.txt"), []byte("needle\n"), 0o600)
    tool := search.New(search.Options{Workspace:root, LookPath:func(string)(string,error){return "",exec.ErrNotFound}, MaxMatches:1, Output:output.Options{SessionID:"s", Artifacts:&fakeArtifactStore{}}})
    prepared, err := tool.Prepare(context.Background(), request("search", `{"query":"needle","path":"."}`, root))
    if err != nil { t.Fatal(err) }
    got := prepared.Execute(context.Background())
    if !strings.Contains(got.Content,"a.txt:1:needle") || !got.Truncated { t.Fatalf("result=%#v", got) }
}
```

- [ ] **Step 2: Run tool tests**

Run: `go test ./internal/tools/read ./internal/tools/search -v`

Expected: FAIL because tools are missing.

- [ ] **Step 3: Implement exact schemas and preparation rules**

`read` input is `{path:string, offset:int, limit:int}` with defaults `offset=1`, `limit=200`, maximum `limit=2000`. Reject NUL bytes in the first 8 KiB. Return lines as `<1-based-number>: <text>`.

`search` input is `{query:string, path:string, include:[]string, exclude:[]string}`. Default path is `.` and maximum matches is 500. Sort fallback paths lexicographically and scan lines in ascending order. `rg` invocation is:

```go
args := []string{"--line-number", "--no-heading", "--color", "never", "--fixed-strings"}
```

Append `--glob` arguments for includes and `--glob !pattern` for excludes, then query and canonical root. Treat exit code 1 as zero matches, other non-zero codes as tool failure. Both implementations emit `relative/path:line:text` and the same truncation flag.

The Go fallback skips every `DirEntry` with `Type()&os.ModeSymlink != 0` (using `SkipDir` for symlink directories) and re-resolves each regular file before opening it. A file is searched only when its canonical path remains under the authorized canonical root. Add outside-target symlink file and symlink directory cases and assert their sentinel text is absent in both `rg` and fallback modes.

Each `Prepare` must return a preview containing canonical scope and inside/outside status; `Execute` must verify the same canonical scope immediately before reading.

`read.Options` and `search.Options` include the shared `output.Options`; constructors reject a missing artifact store. Stream formatted lines/matches into a fresh output buffer instead of building an unbounded string.

- [ ] **Step 4: Run parity and escape tests**

Run: `gofmt -w internal/tools/read internal/tools/search && go test -race ./internal/tools/read ./internal/tools/search -v`

Expected: PASS with and without `rg`; symlink escape is reported as outside in preview.

- [ ] **Step 5: Commit read/search tools**

```bash
git add internal/tools/read internal/tools/search
git commit -m "feat: add bounded read and search tools"
```

### Task 5: Implement exact diff preview and atomic `edit`

**Files:**
- Create: `internal/tools/edit/diff.go`
- Create: `internal/tools/edit/edit.go`
- Test: `internal/tools/edit/edit_test.go`

**Interfaces:**
- Consumes: prepared-tool and scope contracts.
- Produces: structured replace/create patch, SHA-256 preimage check, unified preview, atomic same-directory replacement.

- [ ] **Step 1: Write preview, stale-preimage, create, and mode-preservation tests**

```go
func TestEditPreviewsAndRejectsStalePreimage(t *testing.T) {
    root := t.TempDir()
    path := filepath.Join(root,"a.txt")
    os.WriteFile(path, []byte("old\n"), 0o640)
    sum := sha256.Sum256([]byte("old\n"))
    input := fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%x","replacements":[{"old":"old","new":"new","all":false}]}`, sum)
    tool := edit.New(edit.Options{Workspace:root, Output:output.Options{SessionID:"s", Artifacts:&fakeArtifactStore{}}})
    prepared, err := tool.Prepare(context.Background(), request("edit", input, root))
    if err != nil { t.Fatal(err) }
    if !strings.Contains(prepared.Preview().ProposedDiff,"-old") || !strings.Contains(prepared.Preview().ProposedDiff,"+new") { t.Fatalf("diff=%s", prepared.Preview().ProposedDiff) }
    os.WriteFile(path, []byte("changed elsewhere\n"), 0o640)
    result := prepared.Execute(context.Background())
    if result.Status != domain.ToolFailed || !strings.Contains(result.Content,"stale preimage") { t.Fatalf("result=%#v", result) }
}
```

- [ ] **Step 2: Run focused edit test**

Run: `go test ./internal/tools/edit -run TestEditPreviewsAndRejectsStalePreimage -v`

Expected: FAIL because edit package is missing.

- [ ] **Step 3: Implement structured patch and atomic replace**

Run `go get github.com/pmezard/go-difflib@v1.0.0` before adding the diff implementation.

Use these exact input types:

```go
type Replacement struct { Old string `json:"old"`; New string `json:"new"`; All bool `json:"all"` }
type Input struct { Path string `json:"path"`; ExpectedSHA256 string `json:"expected_sha256"`; Create bool `json:"create"`; NewContent string `json:"new_content"`; Replacements []Replacement `json:"replacements"` }
```

Rules:

- `create=true` requires an absent target, empty `expected_sha256`, zero replacements, and uses `new_content`.
- Existing-file edit requires a lowercase 64-character SHA-256 and at least one replacement.
- `all=false` requires exactly one `old` occurrence; `all=true` requires at least one.
- Generate unified diff using `difflib.UnifiedDiff{A:SplitLines(before), B:SplitLines(after), FromFile:"a/"+relative, ToFile:"b/"+relative, Context:3}`.
- Prepared state stores canonical path, before/after bytes, mode, and expected hash; its preview sets `FilePlan{CallID, Path, ExpectedSHA256, PlannedSHA256, Diff}`.
- Execute re-reads the target and compares SHA-256 before writing.
- Write a `0o600` temp file in the same directory, apply original mode, `Sync`, close, rename, then sync the directory.
- Return a `ToolResult` containing the same diff and before/after hashes in bounded content and a structured `FileChange{CallID, Path, BeforeSHA256, AfterSHA256, Diff}`; never call Execute twice in tests.
- `edit.Options` contains workspace and shared `output.Options`. Generate the full diff into the bounded output buffer; use its excerpt/artifact IDs for both `FilePlan` and `FileChange`, so permission/session events never duplicate an unbounded diff.

- [ ] **Step 4: Run edit suite and diff checks**

Run: `go mod tidy && gofmt -w internal/tools/edit && go test -race ./internal/tools/edit -v`

Expected: PASS for replace, replace-all, create, stale hash, symlink escape, mode preservation, and atomic failure cleanup.

- [ ] **Step 5: Commit edit tool**

```bash
git add go.mod go.sum internal/tools/edit
git commit -m "feat: add atomic edit tool with diff preview"
```

### Task 6: Implement cancellable unsandboxed `shell`

**Files:**
- Modify: `internal/domain/tool.go`
- Create: `internal/tools/shell/shell.go`
- Create: `internal/tools/shell/process_unix.go`
- Create: `internal/workspace/changes.go`
- Test: `internal/tools/shell/shell_test.go`
- Test: `internal/workspace/changes_test.go`

**Interfaces:**
- Consumes: prepared-tool, scope, output buffer, artifact store, redactor.
- Produces: shell preview keyed by normalized command plus canonical cwd, provider-key stripping, timeout, process-group cancellation.

- [ ] **Step 1: Write environment stripping and process-group cancellation tests**

```go
func TestShellStripsProviderKeys(t *testing.T) {
    root := t.TempDir()
    t.Setenv("YORDAM_API_KEY","top-secret")
    t.Setenv("PROFILE_KEY","profile-secret")
    tool := shell.New(shell.Options{Workspace:root, ShellPath:"/bin/sh", ProviderKeyEnvs:[]string{"PROFILE_KEY"}, Timeout:2*time.Second, Output:output.Options{SessionID:"s", Artifacts:&fakeArtifactStore{}}})
    prepared, err := tool.Prepare(context.Background(), request("shell", `{"command":"env","cwd":"."}`, root))
    if err != nil { t.Fatal(err) }
    result := prepared.Execute(context.Background())
    if strings.Contains(result.Content,"top-secret") || strings.Contains(result.Content,"profile-secret") { t.Fatalf("secret leaked: %s", result.Content) }
}

func TestShellCancellationKillsProcessGroup(t *testing.T) {
    root := t.TempDir()
    tool := shell.New(shell.Options{Workspace:root, ShellPath:"/bin/sh", Timeout:30*time.Second, Output:output.Options{SessionID:"s", Artifacts:&fakeArtifactStore{}}})
    prepared, err := tool.Prepare(context.Background(), request("shell", `{"command":"sleep 30 & wait","cwd":"."}`, root))
    if err != nil { t.Fatal(err) }
    ctx, cancel := context.WithCancel(context.Background())
    done := make(chan domain.ToolResult,1)
    go func(){ done <- prepared.Execute(ctx) }()
    time.Sleep(100*time.Millisecond); cancel()
    select { case result:=<-done: if result.Status!=domain.ToolCancelled { t.Fatalf("result=%#v",result) }; case <-time.After(3*time.Second): t.Fatal("process group survived cancellation") }
}

func TestShellScopeIncludesCanonicalCWDAndNormalizedCommand(t *testing.T) {
    root := t.TempDir()
    tool := shell.New(shell.Options{Workspace:root, ShellPath:"/bin/sh", Timeout:time.Second, Output:output.Options{SessionID:"s", Artifacts:&fakeArtifactStore{}}})
    prepared, err := tool.Prepare(context.Background(), request("shell", `{"command":"  echo ok  ","cwd":"."}`, root))
    if err != nil { t.Fatal(err) }
    canonical, err := filepath.EvalSymlinks(root)
    if err != nil { t.Fatal(err) }
    if got, want := prepared.Preview().CanonicalScope, canonical+"\x00echo ok"; got != want { t.Fatalf("scope=%q want=%q", got, want) }
}
```

- [ ] **Step 2: Run shell tests**

Run: `go test ./internal/tools/shell -run 'TestShellStrips|TestShellCancellation' -v`

Expected: FAIL because shell package is missing.

- [ ] **Step 3: Implement shell preparation and Unix process groups**

Input is `{command:string, cwd:string}`; reject empty command. Canonicalize cwd with `scope.Resolve`. Preview summary is the full command; canonical scope is `canonical-cwd + "\x00" + strings.TrimSpace(command)`.

Immediately before `cmd.Start`, re-resolve cwd and require the same canonical path and inside/outside classification captured by `Prepare`; a symlink swap fails without launching the shell.

```go
// internal/tools/shell/process_unix.go
//go:build darwin || linux

package shell

import (
    "os/exec"
    "syscall"
    "time"
)

func configureProcess(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid:true} }
func terminateProcessGroup(cmd *exec.Cmd) {
    if cmd.Process == nil { return }
    _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
    timer := time.NewTimer(2*time.Second); defer timer.Stop(); <-timer.C
    _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}
```

In `shell.go`, use `exec.CommandContext(ctx, shellPath, "-lc", command)`, override `Cmd.Cancel` to call `terminateProcessGroup`, set `Cmd.WaitDelay` to three seconds, capture stdout/stderr through one mutex-protected output buffer, and classify context cancellation as `ToolCancelled`, deadline as `ToolFailed` with `ErrorToolTimeout`, non-zero exit as `ToolFailed`, and zero exit as `ToolSucceeded`.

`shell.Options` includes `Progress func(domain.ToolProgress)`. While the child runs, a 50 ms ticker reads `Buffer.Snapshot`; when text/truncation changed, invoke the callback with the call ID and complete current 32 KiB excerpt. Emit one final changed snapshot after `Wait`. Bootstrap's callback performs a non-blocking send of `agent.RuntimeEvent{Kind:RuntimeToolOutput, Progress:&progress}` to the bounded runtime-event channel, so a slow TUI may drop intermediate snapshots but never blocks or changes the final durable result. Add a race test that observes at least one progress update before a two-line slow command completes and asserts no sentinel secret appears in any update.

Construct child environment by removing `YORDAM_API_KEY` and every configured profile key name using exact `NAME=` prefix matching. Preserve all other variables. If `$SHELL` is empty or non-executable, use `/bin/sh`.

Add `WorkspaceChanges *WorkspaceChanges` to `domain.ToolResult`, where `WorkspaceChanges` has `IsGit bool`, `Status string`, `Diff string`, `Notice string`, and `ArtifactIDs []string`. After a shell process terminates, call `workspace.Inspect(ctx, canonicalWorkspace, outputOptions)`. In a Git worktree it runs `git -C <root> status --short` and `git -C <root> diff --no-ext-diff --no-color --`, routes both through fresh bounded/redacted buffers, and attaches the excerpts/artifact IDs to the result. If Git is unavailable or `rev-parse --is-inside-work-tree` is not true, attach the exact notice `Non-Git workspace: shell filesystem changes cannot be exhaustively detected.` without failing the shell result. `changes_test.go` covers a changed tracked file and a plain directory; TUI Task 4 renders status/diff in context and always renders the non-Git notice.

- [ ] **Step 4: Run shell tests under race detector**

Run: `gofmt -w internal/domain internal/tools/shell internal/workspace && go test -race ./internal/tools/shell ./internal/workspace -v`

Expected: PASS on macOS and Linux; cancellation returns within three seconds.

- [ ] **Step 5: Commit shell tool**

```bash
git add internal/domain/tool.go internal/tools/shell internal/workspace
git commit -m "feat: add trusted cancellable shell tool"
```

### Task 7: Register tools and pass the Phase 3 integration gate

**Files:**
- Create: `internal/tools/registry.go`
- Create: `internal/integration/tool_loop_test.go`
- Modify: `docs/superpowers/plans/2026-07-13-yordam-v0.1-roadmap.md`

**Interfaces:**
- Consumes: all tool constructors, session policy, agent runner.
- Produces: deterministic four-tool registry and end-to-end prompt-to-edit/shell evidence.

- [ ] **Step 1: Write registry order and full ask/deny integration tests**

```go
func TestRegistryDescriptorsAreStable(t *testing.T) {
    registry := tools.NewRegistry(readTool, searchTool, editTool, shellTool)
    names := []string{}
    for _, descriptor := range registry.Descriptors() { names = append(names, descriptor.Name) }
    if !slices.Equal(names, []string{"read","search","edit","shell"}) { t.Fatalf("names=%v", names) }
}
```

Use this concrete integration fixture. It drives `read -> search -> edit -> shell -> final` through the real registry, tools, policy, runner, and JSONL store; only the model provider and human approver are scripted.

```go
// internal/integration/tool_loop_test.go
package integration_test

import (
    "context"
    "crypto/sha256"
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "sync"
    "testing"
    "time"

    "github.com/muratmirgun/yordam/internal/agent"
    "github.com/muratmirgun/yordam/internal/domain"
    "github.com/muratmirgun/yordam/internal/permission"
    "github.com/muratmirgun/yordam/internal/ports"
    "github.com/muratmirgun/yordam/internal/session/jsonl"
    "github.com/muratmirgun/yordam/internal/tools"
    edittool "github.com/muratmirgun/yordam/internal/tools/edit"
    readtool "github.com/muratmirgun/yordam/internal/tools/read"
    searchtool "github.com/muratmirgun/yordam/internal/tools/search"
    shelltool "github.com/muratmirgun/yordam/internal/tools/shell"
    "github.com/muratmirgun/yordam/internal/tools/output"
)

type scriptedProvider struct { mu sync.Mutex; streams [][]domain.ModelEvent }
func (p *scriptedProvider) Stream(_ context.Context, _ domain.ModelRequest) (<-chan domain.ModelEvent, error) {
    p.mu.Lock(); defer p.mu.Unlock()
    if len(p.streams) == 0 { return nil, fmt.Errorf("provider script exhausted") }
    events := p.streams[0]; p.streams = p.streams[1:]
    out := make(chan domain.ModelEvent, len(events))
    for _, event := range events { out <- event }
    close(out)
    return out, nil
}

type scriptedApprover struct{}
func (scriptedApprover) Resolve(_ context.Context, prompt ports.PermissionPrompt) (domain.PermissionDecision, error) {
    switch prompt.Call.Request.Name {
    case "edit": return domain.PermissionDecision{Action:domain.PermissionAllow, Lifetime:domain.PermissionOnce, Scope:prompt.Call.CanonicalScope}, nil
    case "shell": return domain.PermissionDecision{Action:domain.PermissionDeny, Lifetime:domain.PermissionOnce, Scope:prompt.Call.CanonicalScope}, nil
    default: return domain.PermissionDecision{}, fmt.Errorf("unexpected approval for %s", prompt.Call.Request.Name)
    }
}

func TestAskModeEditAllowShellDeny(t *testing.T) {
    workspacePath := t.TempDir()
    target := filepath.Join(workspacePath, "a.txt")
    if err := os.WriteFile(target, []byte("old\n"), 0o600); err != nil { t.Fatal(err) }
    before := sha256.Sum256([]byte("old\n"))
    editInput := fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%x","replacements":[{"old":"old","new":"new","all":false}]}`, before)
    provider := &scriptedProvider{streams:[][]domain.ModelEvent{
        {{Kind:domain.ModelToolCall, ToolCall:&domain.ToolCall{ID:"read-1", Name:"read", Arguments:json.RawMessage(`{"path":"a.txt","offset":1,"limit":10}`)}}, {Kind:domain.ModelDone}},
        {{Kind:domain.ModelToolCall, ToolCall:&domain.ToolCall{ID:"search-1", Name:"search", Arguments:json.RawMessage(`{"query":"old","path":"."}`)}}, {Kind:domain.ModelDone}},
        {{Kind:domain.ModelToolCall, ToolCall:&domain.ToolCall{ID:"edit-1", Name:"edit", Arguments:json.RawMessage(editInput)}}, {Kind:domain.ModelDone}},
        {{Kind:domain.ModelToolCall, ToolCall:&domain.ToolCall{ID:"shell-1", Name:"shell", Arguments:json.RawMessage(`{"command":"touch side-effect","cwd":"."}`)}}, {Kind:domain.ModelDone}},
        {{Kind:domain.ModelTextDelta, Text:"done"}, {Kind:domain.ModelDone}},
    }}
    store := jsonl.New(t.TempDir(), jsonl.Options{})
    workspace, err := jsonl.WorkspaceFromPath(workspacePath); if err != nil { t.Fatal(err) }
    session, err := store.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile:"test", Model:"test"}); if err != nil { t.Fatal(err) }
    replay, err := store.Load(context.Background(), session.ID); if err != nil { t.Fatal(err) }
    bounded := output.Options{SessionID:session.ID, Artifacts:store}
    registry := tools.NewRegistry(
        readtool.New(readtool.Options{Workspace:workspacePath, Output:bounded}),
        searchtool.New(searchtool.Options{Workspace:workspacePath, Output:bounded}),
        edittool.New(edittool.Options{Workspace:workspacePath, Output:bounded}),
        shelltool.New(shelltool.Options{Workspace:workspacePath, ShellPath:"/bin/sh", Timeout:2*time.Second, Output:bounded}),
    )
    runner := agent.Runner{Provider:provider, Tools:registry, Policy:permission.NewSession(domain.ModeAsk), Approver:scriptedApprover{}, Sessions:store, MaxToolCalls:32, SystemPrompt:"test"}
    if err := runner.RunTurn(context.Background(), agent.RunInput{Session:session, Replay:replay, Prompt:"update a.txt"}); err != nil { t.Fatal(err) }
    changed, err := os.ReadFile(target); if err != nil { t.Fatal(err) }
    if string(changed) != "new\n" { t.Fatalf("content=%q", changed) }
    if _, err := os.Stat(filepath.Join(workspacePath, "side-effect")); !os.IsNotExist(err) { t.Fatalf("shell side effect exists: %v", err) }
    finalReplay, err := store.Load(context.Background(), session.ID); if err != nil { t.Fatal(err) }
    decisions := map[string]domain.PermissionDecision{}
    for _, event := range finalReplay.Events {
        if event.Kind != domain.EventPermissionResolved { continue }
        var payload domain.PermissionPayload
        if err := json.Unmarshal(event.Payload, &payload); err != nil { t.Fatal(err) }
        decisions[payload.CallID] = payload.Decision
    }
    if decisions["edit-1"].Action != domain.PermissionAllow || decisions["shell-1"].Action != domain.PermissionDeny { t.Fatalf("decisions=%#v", decisions) }
}
```

- [ ] **Step 2: Run integration test and observe missing registry**

Run: `go test ./internal/integration -run TestAskModeEditAllowShellDeny -v`

Expected: FAIL because registry and integration fixture are missing.

- [ ] **Step 3: Implement deterministic registry**

```go
package tools

import (
    "sort"
    "github.com/muratmirgun/yordam/internal/domain"
    "github.com/muratmirgun/yordam/internal/ports"
)

type Registry struct { byName map[string]ports.Tool; ordered []domain.ToolDescriptor }
var builtInRank = map[string]int{"read":0, "search":1, "edit":2, "shell":3}
func NewRegistry(items ...ports.Tool) *Registry {
    r:=&Registry{byName:map[string]ports.Tool{}}
    for _,item:=range items {
        d:=item.Descriptor(); if err:=d.Validate(); err!=nil { panic(err) }
        if _,known:=builtInRank[d.Name]; !known { panic("unknown built-in tool "+d.Name) }
        if _,exists:=r.byName[d.Name]; exists { panic("duplicate tool "+d.Name) }
        r.byName[d.Name]=item; r.ordered=append(r.ordered,d)
    }
    if len(r.byName)!=len(builtInRank){panic("registry requires read, search, edit, and shell")}
    sort.Slice(r.ordered,func(i,j int)bool{return builtInRank[r.ordered[i].Name]<builtInRank[r.ordered[j].Name]})
    return r
}
func (r *Registry) Descriptors() []domain.ToolDescriptor { return append([]domain.ToolDescriptor(nil),r.ordered...) }
func (r *Registry) Lookup(name string)(ports.Tool,bool){ tool,ok:=r.byName[name]; return tool,ok }
```

- [ ] **Step 4: Run Phase 3 gate commands**

```bash
gofmt -w internal/tools internal/integration
go test -race ./internal/scope/... ./internal/permission/... ./internal/tools/... ./internal/integration/...
go test ./...
go vet ./internal/scope/... ./internal/permission/... ./internal/tools/... ./internal/integration/...
git diff --check
```

Expected: PASS, including symlink escape, stale preimage, secret stripping, output limits, and process-group cancellation.

- [ ] **Step 5: Mark Gate 3 and commit**

Change only Gate 3 in the roadmap to `[x]`, then run:

```bash
git add internal/tools/registry.go internal/integration docs/superpowers/plans/2026-07-13-yordam-v0.1-roadmap.md
git commit -m "test: verify tools and permission gate"
```
