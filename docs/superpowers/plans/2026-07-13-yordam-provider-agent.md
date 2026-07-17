# Yordam Provider and Agent Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement an OpenAI-compatible streaming adapter and a deterministic, UI-independent agent loop that persists every meaningful transition.

**Architecture:** The provider translates Chat Completions SSE into normalized model events and owns retry-before-first-delta behavior. The agent runner consumes only ports, executes one tool at a time, emits transient runtime events through a sink, and persists durable events through `SessionStore`.

**Tech Stack:** Go 1.26.4, standard `net/http`/`encoding/json`/`bufio`, Phase 1 domain and storage contracts.

## Global Constraints

- Complete `2026-07-13-yordam-foundation-session.md` and Gate 1 first.
- One OpenAI-compatible Chat Completions adapter; endpoint is normalized base URL plus `/chat/completions`.
- Retry at most three times and only before the first content or tool-call delta.
- One active turn per session and sequential tool execution.
- Default maximum 32 completed tool calls per turn; valid range is 1 through 128.
- Streaming deltas are transient; completed messages, tool calls/results, permission decisions, and terminal turn states are durable.
- A mutating tool call is never retried automatically.
- No Bubble Tea imports in provider, agent, app, domain, or ports packages.

---

### Task 1: Encode Chat Completions requests and parse SSE text

**Files:**
- Create: `internal/provider/openaicompat/client.go`
- Create: `internal/provider/openaicompat/wire.go`
- Create: `internal/provider/openaicompat/stream.go`
- Test: `internal/provider/openaicompat/stream_test.go`

**Interfaces:**
- Consumes: `ports.ModelProvider`, `domain.ModelRequest`, `domain.ModelEvent`.
- Produces: `openaicompat.New(ClientOptions) *Client` and `Client.Stream`.

- [ ] **Step 1: Write a text-stream mapping test**

```go
package openaicompat_test

import (
    "context"
    "errors"
    "fmt"
    "net/http"
    "net/http/httptest"
    "testing"

    "github.com/muratmirgun/yordam/internal/domain"
    "github.com/muratmirgun/yordam/internal/provider/openaicompat"
)

func TestStreamTextAndDone(t *testing.T) {
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if got := r.Header.Get("Authorization"); got != "Bearer key" { t.Errorf("authorization=%q", got) }
        w.Header().Set("Content-Type", "text/event-stream")
        fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"hel"}}]}`)
        fmt.Fprintln(w)
        fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"lo"}}]}`)
        fmt.Fprintln(w)
        fmt.Fprintln(w, `data: [DONE]`)
    }))
    defer server.Close()

    client := openaicompat.New(openaicompat.ClientOptions{HTTPClient:server.Client(), BaseURL:server.URL, APIKey:"key", Model:"model-a"})
    events, err := client.Stream(context.Background(), domain.ModelRequest{Messages:[]domain.Message{{Role:domain.RoleUser, Content:"hi"}}})
    if err != nil { t.Fatal(err) }
    var text string
    var done bool
    for event := range events {
        if event.Err != nil { t.Fatal(event.Err) }
        if event.Kind == domain.ModelTextDelta { text += event.Text }
        if event.Kind == domain.ModelDone { done = true }
    }
    if text != "hello" || !done { t.Fatalf("text=%q done=%v", text, done) }
}
```

- [ ] **Step 2: Run the focused test**

Run: `go test ./internal/provider/openaicompat -run TestStreamTextAndDone -v`

Expected: FAIL because the package is missing.

- [ ] **Step 3: Implement exact wire types and SSE loop**

```go
// internal/provider/openaicompat/wire.go
package openaicompat

import "encoding/json"

type chatRequest struct {
    Model    string       `json:"model"`
    Messages []wireMessage `json:"messages"`
    Tools    []wireTool    `json:"tools,omitempty"`
    Stream   bool          `json:"stream"`
}

type wireMessage struct {
    Role       string         `json:"role"`
    Content    string         `json:"content,omitempty"`
    ToolCalls  []wireToolCall `json:"tool_calls,omitempty"`
    ToolCallID string         `json:"tool_call_id,omitempty"`
}

type wireTool struct {
    Type     string       `json:"type"`
    Function wireFunction `json:"function"`
}

type wireFunction struct { Name string `json:"name"`; Description string `json:"description,omitempty"`; Parameters json.RawMessage `json:"parameters,omitempty"`; Arguments string `json:"arguments,omitempty"` }
type wireToolCall struct { Index int `json:"index,omitempty"`; ID string `json:"id,omitempty"`; Type string `json:"type,omitempty"`; Function wireFunction `json:"function"` }
type streamChunk struct { Choices []struct { Delta struct { Content string `json:"content"`; ToolCalls []wireToolCall `json:"tool_calls"` } `json:"delta"`; FinishReason *string `json:"finish_reason"` } `json:"choices"` }
```

```go
// internal/provider/openaicompat/client.go
package openaicompat

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "net/http"
    "strings"

    "github.com/muratmirgun/yordam/internal/domain"
)

type ClientOptions struct { HTTPClient *http.Client; BaseURL, APIKey, Model string }
type Client struct { http *http.Client; endpoint, apiKey, model string }

func New(opts ClientOptions) *Client {
    if opts.HTTPClient == nil { opts.HTTPClient = http.DefaultClient }
    return &Client{http:opts.HTTPClient, endpoint:strings.TrimRight(opts.BaseURL, "/")+"/chat/completions", apiKey:opts.APIKey, model:opts.Model}
}

func (c *Client) newRequest(ctx context.Context, input domain.ModelRequest) (*http.Request, error) {
    model := c.model
    if input.Selection.Model != "" { model = input.Selection.Model }
    payload := chatRequest{Model:model, Stream:true}
    for _, message := range input.Messages {
        wire := wireMessage{Role:string(message.Role), Content:message.Content, ToolCallID:message.ToolCallID}
        for _, call := range message.ToolCalls { wire.ToolCalls = append(wire.ToolCalls, wireToolCall{ID:call.ID, Type:"function", Function:wireFunction{Name:call.Name, Arguments:string(call.Arguments)}}) }
        payload.Messages = append(payload.Messages, wire)
    }
    for _, tool := range input.Tools { payload.Tools = append(payload.Tools, wireTool{Type:"function", Function:wireFunction{Name:tool.Name, Description:tool.Description, Parameters:tool.InputSchema}}) }
    raw, err := json.Marshal(payload); if err != nil { return nil, err }
    req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(raw)); if err != nil { return nil, err }
    req.Header.Set("Content-Type", "application/json")
    req.Header.Set("Accept", "text/event-stream")
    req.Header.Set("Authorization", "Bearer "+c.apiKey)
    return req, nil
}

type HTTPStatusError struct { Status int }
func (e *HTTPStatusError) Error() string { return fmt.Sprintf("provider returned HTTP %d", e.Status) }
func statusError(resp *http.Response) error { return &HTTPStatusError{Status:resp.StatusCode} }
```

```go
// internal/provider/openaicompat/stream.go
package openaicompat

import (
    "bufio"
    "context"
    "encoding/json"
    "fmt"
    "strings"

    "github.com/muratmirgun/yordam/internal/domain"
)

func (c *Client) Stream(ctx context.Context, input domain.ModelRequest) (<-chan domain.ModelEvent, error) {
    req, err := c.newRequest(ctx, input); if err != nil { return nil, err }
    resp, err := c.http.Do(req); if err != nil { return nil, err }
    if resp.StatusCode < 200 || resp.StatusCode >= 300 { defer resp.Body.Close(); return nil, statusError(resp) }
    out := make(chan domain.ModelEvent)
    go func() {
        defer close(out); defer resp.Body.Close()
        scanner := bufio.NewScanner(resp.Body)
        scanner.Buffer(make([]byte, 64*1024), 2*1024*1024)
        for scanner.Scan() {
            line := scanner.Text()
            if !strings.HasPrefix(line, "data:") { continue }
            data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
            if data == "[DONE]" { out <- domain.ModelEvent{Kind:domain.ModelDone}; return }
            var chunk streamChunk
            if err := json.Unmarshal([]byte(data), &chunk); err != nil { out <- domain.ModelEvent{Kind:domain.ModelStreamError, Err:fmt.Errorf("decode stream: %w", err)}; return }
            for _, choice := range chunk.Choices {
                if choice.Delta.Content != "" { out <- domain.ModelEvent{Kind:domain.ModelTextDelta, Text:choice.Delta.Content} }
            }
        }
        if err := scanner.Err(); err != nil { out <- domain.ModelEvent{Kind:domain.ModelStreamError, Err:err}; return }
        out <- domain.ModelEvent{Kind:domain.ModelStreamError, Err:fmt.Errorf("stream ended without [DONE]")}
    }()
    return out, nil
}
```

- [ ] **Step 4: Run provider text tests**

Run: `gofmt -w internal/provider/openaicompat && go test ./internal/provider/openaicompat -run TestStreamTextAndDone -v`

Expected: PASS.

- [ ] **Step 5: Commit request and text streaming**

```bash
git add internal/provider/openaicompat
git commit -m "feat: stream OpenAI-compatible responses"
```

### Task 2: Assemble fragmented tool calls, classify safe retries, and route profiles

**Files:**
- Modify: `internal/provider/openaicompat/client.go`
- Modify: `internal/provider/openaicompat/stream.go`
- Create: `internal/provider/openaicompat/errors.go`
- Create: `internal/provider/openaicompat/router.go`
- Test: `internal/provider/openaicompat/toolcall_test.go`
- Test: `internal/provider/openaicompat/retry_test.go`
- Test: `internal/provider/openaicompat/router_test.go`

**Interfaces:**
- Consumes: Task 1 `Client.Stream`.
- Produces: one completed `domain.ModelToolCall` event per assembled tool call, `IsRetryable(error)`, and a `ports.ModelProvider` router keyed by profile name.

- [ ] **Step 1: Write fragmented tool-call and pre-delta retry tests**

```go
func TestStreamAssemblesFragmentedToolCall(t *testing.T) {
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Content-Type", "text/event-stream")
        fmt.Fprintln(w, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"re","arguments":"{\"pa"}}]}}]}`)
        fmt.Fprintln(w, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"name":"ad","arguments":"th\":\"a.go\"}"}}]}}]}`)
        fmt.Fprintln(w, `data: [DONE]`)
    }))
    defer server.Close()
    client := openaicompat.New(openaicompat.ClientOptions{HTTPClient:server.Client(), BaseURL:server.URL, APIKey:"k", Model:"m"})
    events, err := client.Stream(context.Background(), domain.ModelRequest{})
    if err != nil { t.Fatal(err) }
    var call *domain.ToolCall
    for event := range events { if event.Kind == domain.ModelToolCall { call = event.ToolCall } }
    if call == nil || call.ID != "call-1" || call.Name != "read" || string(call.Arguments) != `{"path":"a.go"}` { t.Fatalf("call=%#v", call) }
}

func TestRetryStopsAfterFirstDelta(t *testing.T) {
    var requests atomic.Int32
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        n := requests.Add(1)
        w.Header().Set("Content-Type", "text/event-stream")
        if n == 1 { w.WriteHeader(http.StatusServiceUnavailable); return }
        fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"partial"}}]}`)
    }))
    defer server.Close()
    client := openaicompat.New(openaicompat.ClientOptions{HTTPClient:server.Client(), BaseURL:server.URL, APIKey:"k", Model:"m", RetryDelays:[]time.Duration{0,0}})
    events, err := client.Stream(context.Background(), domain.ModelRequest{})
    if err != nil { t.Fatal(err) }
    for range events {}
    if requests.Load() != 2 { t.Fatalf("requests=%d want 2", requests.Load()) }
}
```

- [ ] **Step 2: Run both focused tests**

Run: `go test ./internal/provider/openaicompat -run 'TestStreamAssembles|TestRetryStops' -v`

Expected: FAIL because tool calls and retry delays are not implemented.

- [ ] **Step 3: Add exact accumulator and retry boundary**

```go
// internal/provider/openaicompat/errors.go
package openaicompat

import (
    "errors"
    "net"
)

func IsRetryable(err error) bool {
    var status *HTTPStatusError
    if errors.As(err, &status) { return status.Status == 429 || status.Status >= 500 }
    var network net.Error
    return errors.As(err, &network)
}
```

Extend `ClientOptions` and `Client` with `RetryDelays []time.Duration`, `Jitter func(time.Duration) time.Duration`, and `Redact func(string) string`. Defaults are `250ms`, `1s`, `2s`, random jitter in the inclusive range `[delay/2, delay]`, and identity redaction. Tests inject identity jitter and a sentinel redactor. Move one HTTP/SSE attempt into `consumeAttempt`: it reports whether any content byte or tool-call fragment was observed. Retry retryable open failures and stream failures only when that flag is false; after the first delta, forward one interrupted event and stop. Three delay entries mean at most three retries after the initial attempt. Cancellation during backoff returns `context.Canceled` immediately.

Replace Task 1's status handling with `c.statusError(resp)`. It reads at most 32 KiB through `io.LimitReader`, marks overflow as truncated, applies `Redact` before placing body text in an error, and wraps `HTTPStatusError` in `domain.TypedError`. Classify 429/5xx as `ErrorProviderRetryable`; classify 400/413 responses whose lower-cased body contains `context`, `token`, and either `large`, `long`, or `maximum` as `ErrorContextTooLarge`; otherwise use `ErrorProviderFatal`. The authorization header and unredacted body never enter the returned message.

Use this exact accumulator inside the SSE goroutine:

```go
type callParts struct { id, name, arguments string }
calls := map[int]*callParts{}
for _, delta := range choice.Delta.ToolCalls {
    part := calls[delta.Index]
    if part == nil { part = &callParts{}; calls[delta.Index] = part }
    part.id += delta.ID
    part.name += delta.Function.Name
    part.arguments += delta.Function.Arguments
}
```

When `[DONE]` arrives, sort the integer indexes, validate each accumulated arguments string with `json.Valid`, emit one `ModelToolCall` per index, then emit `ModelDone`. Track whether `[DONE]` was observed: scanner EOF without it emits a typed `ModelStreamError` with `ErrorProviderInterrupted`, even when `scanner.Err()` is nil. Any scanner/decode error after the channel has been returned is emitted once and is never retried. Add tests named `TestHTTPErrorBodyIsBoundedAndRedacted`, `TestContextTooLargeClassification`, `TestRetryJitterIsInjected`, and `TestEOFWithoutDoneIsInterrupted`; the first uses a secret in a body larger than 32 KiB and asserts neither the secret nor bytes beyond the cap appear.

Add the profile router exactly as follows. Bootstrap creates one client per resolved config profile; the request selection chooses the client, while `Client.newRequest` uses the selected model as implemented in Task 1.

```go
// internal/provider/openaicompat/router.go
package openaicompat

import (
    "context"
    "fmt"

    "github.com/muratmirgun/yordam/internal/domain"
    "github.com/muratmirgun/yordam/internal/ports"
)

type Router struct { clients map[string]*Client }

func NewRouter(clients map[string]*Client) (*Router, error) {
    if len(clients) == 0 { return nil, fmt.Errorf("provider router requires at least one profile") }
    copied := make(map[string]*Client, len(clients))
    for name, client := range clients {
        if name == "" || client == nil { return nil, fmt.Errorf("invalid provider profile %q", name) }
        copied[name] = client
    }
    return &Router{clients:copied}, nil
}

func (r *Router) Stream(ctx context.Context, input domain.ModelRequest) (<-chan domain.ModelEvent, error) {
    client, ok := r.clients[input.Selection.Profile]
    if !ok { return nil, fmt.Errorf("unknown provider profile %q", input.Selection.Profile) }
    return client.Stream(ctx, input)
}

var _ ports.ModelProvider = (*Router)(nil)
```

Use this focused router test (with the listed imports) so both profile and model overrides are executable evidence:

```go
// internal/provider/openaicompat/router_test.go
package openaicompat

import (
    "context"
    "encoding/json"
    "fmt"
    "net/http"
    "net/http/httptest"
    "strings"
    "testing"

    "github.com/muratmirgun/yordam/internal/domain"
)

func TestRouterSelectsProfileAndRequestedModel(t *testing.T) {
    cloudCount, localCount, localModel := 0, 0, ""
    cloudServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
        cloudCount++
        fmt.Fprintln(w, "data: [DONE]")
    }))
    defer cloudServer.Close()
    localServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
        localCount++
        var body struct { Model string `json:"model"` }
        if err := json.NewDecoder(request.Body).Decode(&body); err != nil { t.Error(err) }
        localModel = body.Model
        fmt.Fprintln(w, "data: [DONE]")
    }))
    defer localServer.Close()
    router, err := NewRouter(map[string]*Client{
        "cloud":New(ClientOptions{HTTPClient:cloudServer.Client(), BaseURL:cloudServer.URL, Model:"cloud-default"}),
        "local":New(ClientOptions{HTTPClient:localServer.Client(), BaseURL:localServer.URL, Model:"local-default"}),
    })
    if err != nil { t.Fatal(err) }
    events, err := router.Stream(context.Background(), domain.ModelRequest{Selection:domain.ModelSelection{Profile:"local", Model:"code-model"}})
    if err != nil { t.Fatal(err) }
    for event := range events { if event.Err != nil { t.Fatal(event.Err) } }
    if cloudCount != 0 || localCount != 1 || localModel != "code-model" { t.Fatalf("cloud=%d local=%d model=%q", cloudCount, localCount, localModel) }
    if _, err := router.Stream(context.Background(), domain.ModelRequest{Selection:domain.ModelSelection{Profile:"missing"}}); err == nil || !strings.Contains(err.Error(), "unknown provider profile") { t.Fatalf("error=%v", err) }
}
```

- [ ] **Step 4: Run all provider tests and race detector**

Run: `gofmt -w internal/provider/openaicompat && go test -race ./internal/provider/openaicompat -v`

Expected: PASS with exactly two requests in the retry test and profile/model routing covered.

- [ ] **Step 5: Commit tool-call assembly and safe retry**

```bash
git add internal/provider/openaicompat
git commit -m "feat: assemble tool calls and bound retries"
```

### Task 3: Build durable payloads and model context from replay

**Files:**
- Create: `internal/domain/payloads.go`
- Create: `internal/agent/context.go`
- Create: `internal/agent/prompt.go`
- Test: `internal/agent/context_test.go`
- Test: `internal/agent/prompt_test.go`

**Interfaces:**
- Consumes: `domain.SessionReplay` and durable event kinds.
- Produces: typed payloads, `agent.BuildContext(replay, systemPrompt) []domain.Message`, and a permission-mode-aware system prompt.

- [ ] **Step 1: Write replay-to-context ordering test**

```go
package agent_test

import (
    "encoding/json"
    "testing"

    "github.com/muratmirgun/yordam/internal/agent"
    "github.com/muratmirgun/yordam/internal/domain"
)

func TestBuildContextUsesLatestCompaction(t *testing.T) {
    payload := func(v any) json.RawMessage { raw, _ := json.Marshal(v); return raw }
    replay := domain.SessionReplay{Events:[]domain.DurableEvent{
        {Seq:1, Kind:domain.EventUserMessage, Payload:payload(domain.MessagePayload{Content:"old"})},
        {Seq:2, Kind:domain.EventContextCompacted, Payload:payload(domain.CompactionPayload{ThroughSeq:1, Summary:"old work summary"})},
        {Seq:3, Kind:domain.EventUserMessage, Payload:payload(domain.MessagePayload{Content:"new"})},
    }}
    got := agent.BuildContext(replay, "system")
    if len(got) != 3 || got[0].Content != "system" || got[1].Content != "old work summary" || got[2].Content != "new" { t.Fatalf("context=%#v", got) }
}
```

- [ ] **Step 2: Run the focused context test**

Run: `go test ./internal/agent -run TestBuildContextUsesLatestCompaction -v`

Expected: FAIL because payload and context types do not exist.

- [ ] **Step 3: Add exact payloads and replay projection**

```go
// internal/domain/payloads.go
package domain

type MessagePayload struct { Content string `json:"content"`; ToolCalls []ToolCall `json:"tool_calls,omitempty"`; ToolCallID string `json:"tool_call_id,omitempty"` }
type ToolRequestPayload struct { Call ToolCall `json:"call"` }
type ToolResultPayload struct { Result ToolResult `json:"result"` }
type PermissionPayload struct { CallID string `json:"call_id"`; Tool string `json:"tool"`; Decision PermissionDecision `json:"decision"` }
type ModeChangedPayload struct { Mode PermissionMode `json:"mode"` }
type ModelChangedPayload struct { Selection ModelSelection `json:"selection"` }
type TrustedExecutionPayload struct { Enabled bool `json:"enabled"` }
type CompactionPayload struct { FromSeq uint64 `json:"from_seq"`; ThroughSeq uint64 `json:"through_seq"`; Summary string `json:"summary"` }
type TurnTerminalPayload struct { Reason string `json:"reason"`; ErrorKind ErrorKind `json:"error_kind,omitempty"` }
```

```go
// internal/agent/context.go
package agent

import (
    "encoding/json"
    "github.com/muratmirgun/yordam/internal/domain"
)

func BuildContext(replay domain.SessionReplay, systemPrompt string) []domain.Message {
    messages := []domain.Message{{Role:domain.RoleSystem, Content:systemPrompt}}
    startSeq := uint64(0)
    summary := ""
    for _, event := range replay.Events {
        if event.Kind != domain.EventContextCompacted { continue }
        var payload domain.CompactionPayload
        if json.Unmarshal(event.Payload, &payload) == nil && payload.ThroughSeq >= startSeq { startSeq, summary = payload.ThroughSeq, payload.Summary }
    }
    if summary != "" { messages = append(messages, domain.Message{Role:domain.RoleSystem, Content:summary}) }
    for _, event := range replay.Events {
        if event.Seq <= startSeq { continue }
        var payload domain.MessagePayload
        switch event.Kind {
        case domain.EventUserMessage:
            if json.Unmarshal(event.Payload, &payload) == nil { messages = append(messages, domain.Message{Role:domain.RoleUser, Content:payload.Content}) }
        case domain.EventAssistantMessage:
            if json.Unmarshal(event.Payload, &payload) == nil { messages = append(messages, domain.Message{Role:domain.RoleAssistant, Content:payload.Content, ToolCalls:payload.ToolCalls}) }
        case domain.EventToolResult:
            var result domain.ToolResultPayload
            if json.Unmarshal(event.Payload, &result) == nil { messages = append(messages, domain.Message{Role:domain.RoleTool, Content:ToolResultContent(result.Result), ToolCallID:result.Result.CallID}) }
        }
    }
    return messages
}

func ToolResultContent(result domain.ToolResult) string {
    raw,err:=json.Marshal(result)
    if err!=nil{return `{"status":"failed","error_kind":"tool_failed","content":"could not encode tool result"}`}
    return string(raw)
}
```

Add a test that a denied result projects a tool-role JSON message containing `status=denied`, `error_kind=permission_denied`, the call ID, and bounded content.

```go
// internal/agent/prompt.go
package agent

import (
    "fmt"
    "github.com/muratmirgun/yordam/internal/domain"
)

func ComposeSystemPrompt(base string, mode domain.PermissionMode) string {
    rules:=map[domain.PermissionMode]string{
        domain.ModeSafe:"Safe mode: read/search are limited to the workspace; edit and shell are denied.",
        domain.ModeAsk:"Ask mode: outside reads, edits, and shell require visible user approval.",
        domain.ModeAuto:"Auto mode: inside file operations may run automatically; outside file access still asks, and shell is trusted unsandboxed execution requiring session acknowledgement.",
    }
    return fmt.Sprintf("%s\n\n%s Never claim that shell is sandboxed.",base,rules[mode])
}
```

Add a table test for all three modes. In `RunTurn`, call `BuildContext(input.Replay, ComposeSystemPrompt(r.SystemPrompt, input.Session.Mode))`, so a durable mode change affects the next request without rewriting older messages.

- [ ] **Step 4: Run context and domain tests**

Run: `gofmt -w internal/domain internal/agent && go test ./internal/domain ./internal/agent -v`

Expected: PASS.

- [ ] **Step 5: Commit context projection**

```bash
git add internal/domain/payloads.go internal/agent
git commit -m "feat: project session replay into model context"
```

### Task 4: Implement the deterministic agent turn loop

**Files:**
- Create: `internal/ports/approval.go`
- Create: `internal/agent/events.go`
- Create: `internal/agent/runner.go`
- Test: `internal/agent/runner_test.go`

**Interfaces:**
- Consumes: all Phase 1 ports, Task 3 `BuildContext`.
- Produces: `agent.Runner.RunTurn(context.Context, RunInput) error`, `ports.PermissionApprover`, and transient `agent.RuntimeEvent`.

- [ ] **Step 1: Write a scripted prompt-tool-final test**

```go
func TestRunnerPersistsToolLoopInOrder(t *testing.T) {
    provider := &fakeProvider{streams:[][]domain.ModelEvent{
        {{Kind:domain.ModelToolCall, ToolCall:&domain.ToolCall{ID:"c1", Name:"read", Arguments:json.RawMessage(`{"path":"a.go"}`)}}, {Kind:domain.ModelDone}},
        {{Kind:domain.ModelTextDelta, Text:"done"}, {Kind:domain.ModelDone}},
    }}
    sessions := newFakeSessionStore()
    runner := agent.Runner{Provider:provider, Tools:fakeRegistry{tool:fakeTool{result:domain.ToolResult{CallID:"c1", Status:domain.ToolSucceeded, Content:"package a"}}}, Policy:allowPolicy{}, Approver:denyIfCalled{}, Sessions:sessions, MaxToolCalls:32, SystemPrompt:"system"}
    err := runner.RunTurn(context.Background(), agent.RunInput{Session:domain.Session{ID:"s1", Mode:domain.ModeAsk, Workspace:domain.Workspace{CanonicalPath:"/tmp/app"}, Selection:domain.ModelSelection{Profile:"p", Model:"m"}}, Prompt:"inspect"})
    if err != nil { t.Fatal(err) }
    got := sessions.kinds()
    want := []domain.EventKind{domain.EventUserMessage, domain.EventAssistantMessage, domain.EventToolRequested, domain.EventPermissionResolved, domain.EventToolStarted, domain.EventToolResult, domain.EventAssistantMessage, domain.EventTurnCompleted}
    if !slices.Equal(got, want) { t.Fatalf("kinds=%v want=%v", got, want) }
}
```

Extend the test imports with `fmt` and `sync`, then place these fakes in the same test file. Provider and session recordings are mutex-protected; the provider fills and closes a buffered channel before returning it.

```go
type fakeProvider struct {
    mu sync.Mutex
    streams [][]domain.ModelEvent
}

func (f *fakeProvider) Stream(_ context.Context, _ domain.ModelRequest) (<-chan domain.ModelEvent, error) {
    f.mu.Lock()
    defer f.mu.Unlock()
    if len(f.streams) == 0 { return nil, fmt.Errorf("scripted provider exhausted") }
    events := f.streams[0]
    f.streams = f.streams[1:]
    out := make(chan domain.ModelEvent, len(events))
    for _, event := range events { out <- event }
    close(out)
    return out, nil
}

type fakeSessionStore struct {
    mu sync.Mutex
    events []domain.DurableEvent
}

func newFakeSessionStore() *fakeSessionStore { return &fakeSessionStore{} }
func (s *fakeSessionStore) Create(context.Context, domain.Workspace, domain.PermissionMode, domain.ModelSelection) (domain.Session, error) { return domain.Session{}, nil }
func (s *fakeSessionStore) Load(context.Context, string) (domain.SessionReplay, error) { return domain.SessionReplay{}, nil }
func (s *fakeSessionStore) List(context.Context, domain.Workspace) ([]domain.SessionSummary, error) { return nil, nil }
func (s *fakeSessionStore) Append(_ context.Context, sessionID string, kind domain.EventKind, payload any) (domain.DurableEvent, error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    raw, err := json.Marshal(payload)
    if err != nil { return domain.DurableEvent{}, err }
    event := domain.DurableEvent{SchemaVersion:1, SessionID:sessionID, Seq:uint64(len(s.events)+1), Kind:kind, Payload:raw}
    s.events = append(s.events, event)
    return event, nil
}
func (s *fakeSessionStore) kinds() []domain.EventKind {
    s.mu.Lock()
    defer s.mu.Unlock()
    out := make([]domain.EventKind, len(s.events))
    for index, event := range s.events { out[index] = event.Kind }
    return out
}

type fakeTool struct { result domain.ToolResult }
func (f fakeTool) Descriptor() domain.ToolDescriptor { return domain.ToolDescriptor{Name:"read", Description:"read a file", InputSchema:json.RawMessage(`{"type":"object"}`), Mutation:domain.MutationReadOnly} }
func (f fakeTool) Execute(context.Context, domain.ToolRequest) domain.ToolResult { return f.result }

type fakeRegistry struct { tool fakeTool }
func (f fakeRegistry) Descriptors() []domain.ToolDescriptor { return []domain.ToolDescriptor{f.tool.Descriptor()} }
func (f fakeRegistry) Lookup(name string) (ports.Tool, bool) { return f.tool, name == "read" }

type allowPolicy struct{}
func (allowPolicy) Evaluate(context.Context, ports.PermissionContext, domain.ToolRequest) domain.PermissionDecision { return domain.PermissionDecision{Action:domain.PermissionAllow, Lifetime:domain.PermissionOnce, Scope:"read:a.go"} }

type denyIfCalled struct{}
func (denyIfCalled) Resolve(context.Context, ports.PermissionPrompt) (domain.PermissionDecision, error) { panic("approver called for allow decision") }
```

- [ ] **Step 2: Run the runner test and observe missing runner**

Run: `go test ./internal/agent -run TestRunnerPersistsToolLoopInOrder -v`

Expected: FAIL with undefined `agent.Runner`.

- [ ] **Step 3: Implement approval and the exact loop ordering**

```go
// internal/ports/approval.go
package ports

import (
    "context"
    "github.com/muratmirgun/yordam/internal/domain"
)

type PermissionPrompt struct { SessionID string; Request domain.ToolRequest; ProposedDiff string }
type PermissionApprover interface { Resolve(context.Context, PermissionPrompt) (domain.PermissionDecision, error) }
```

```go
// internal/agent/events.go
package agent

import "github.com/muratmirgun/yordam/internal/domain"

type RuntimeEventKind string
const (
    RuntimeStateChanged RuntimeEventKind = "state_changed"
    RuntimeTextDelta RuntimeEventKind = "text_delta"
    RuntimeToolOutput RuntimeEventKind = "tool_output"
    RuntimeToolCompleted RuntimeEventKind = "tool_completed"
)
type RuntimeEvent struct { Kind RuntimeEventKind; State string; Text string; Progress *domain.ToolProgress; Result *domain.ToolResult }
type Sink func(RuntimeEvent)
```

```go
// internal/agent/runner.go
package agent

import (
    "context"
    "fmt"

    "github.com/muratmirgun/yordam/internal/domain"
    "github.com/muratmirgun/yordam/internal/ports"
)

type Runner struct {
    Provider ports.ModelProvider
    Tools ports.ToolRegistry
    Policy ports.PermissionPolicy
    Approver ports.PermissionApprover
    Sessions ports.SessionStore
    MaxToolCalls int
    SystemPrompt string
    Sink Sink
}

type RunInput struct { Session domain.Session; Replay domain.SessionReplay; Prompt string }

func (r Runner) emit(event RuntimeEvent) { if r.Sink != nil { r.Sink(event) } }

func providerErrorKind(err error) domain.ErrorKind {
    var typed *domain.TypedError
    if errors.As(err, &typed) { return typed.Kind }
    return domain.ErrorProviderFatal
}

func (r Runner) RunTurn(ctx context.Context, input RunInput) error {
    if input.Prompt == "" { return fmt.Errorf("prompt is empty") }
    max := r.MaxToolCalls; if max == 0 { max = 32 }
    if max < 1 || max > 128 { return fmt.Errorf("max tool calls must be 1..128") }
    if _, err := r.Sessions.Append(ctx, input.Session.ID, domain.EventUserMessage, domain.MessagePayload{Content:input.Prompt}); err != nil { return err }
    messages := BuildContext(input.Replay, ComposeSystemPrompt(r.SystemPrompt, input.Session.Mode))
    messages = append(messages, domain.Message{Role:domain.RoleUser, Content:input.Prompt})
    completedTools := 0
    for {
        if err := ctx.Err(); err != nil { _, _ = r.Sessions.Append(context.Background(), input.Session.ID, domain.EventTurnInterrupted, domain.TurnTerminalPayload{Reason:err.Error(), ErrorKind:domain.ErrorCancelled}); return err }
        r.emit(RuntimeEvent{Kind:RuntimeStateChanged, State:"streaming_model"})
        stream, err := r.Provider.Stream(ctx, domain.ModelRequest{Selection:input.Session.Selection, Messages:messages, Tools:r.Tools.Descriptors()})
        if err != nil { _, _ = r.Sessions.Append(context.Background(), input.Session.ID, domain.EventTurnFailed, domain.TurnTerminalPayload{Reason:err.Error(), ErrorKind:providerErrorKind(err)}); return err }
        text := ""
        calls := make([]domain.ToolCall, 0)
        for event := range stream {
            if event.Err != nil { _, _ = r.Sessions.Append(context.Background(), input.Session.ID, domain.EventTurnInterrupted, domain.TurnTerminalPayload{Reason:event.Err.Error(), ErrorKind:domain.ErrorProviderInterrupted}); return event.Err }
            if event.Kind == domain.ModelTextDelta { text += event.Text; r.emit(RuntimeEvent{Kind:RuntimeTextDelta, Text:event.Text}) }
            if event.Kind == domain.ModelToolCall && event.ToolCall != nil { calls = append(calls, *event.ToolCall) }
        }
        if text != "" || len(calls) > 0 { if _, err := r.Sessions.Append(ctx, input.Session.ID, domain.EventAssistantMessage, domain.MessagePayload{Content:text, ToolCalls:calls}); err != nil { return err }; messages = append(messages, domain.Message{Role:domain.RoleAssistant, Content:text, ToolCalls:calls}) }
        if len(calls) == 0 { _, err := r.Sessions.Append(ctx, input.Session.ID, domain.EventTurnCompleted, domain.TurnTerminalPayload{Reason:"model completed"}); return err }
        for _, call := range calls {
            if completedTools >= max { err := fmt.Errorf("tool call limit %d reached", max); _, _ = r.Sessions.Append(ctx, input.Session.ID, domain.EventTurnFailed, domain.TurnTerminalPayload{Reason:err.Error(), ErrorKind:domain.ErrorToolFailed}); return err }
            request := domain.ToolRequest{CallID:call.ID, Name:call.Name, Input:call.Arguments, Workspace:input.Session.Workspace.CanonicalPath}
            if _, err := r.Sessions.Append(ctx, input.Session.ID, domain.EventToolRequested, domain.ToolRequestPayload{Call:call}); err != nil { return err }
            decision := r.Policy.Evaluate(ctx, ports.PermissionContext{SessionID:input.Session.ID, Mode:input.Session.Mode, Workspace:input.Session.Workspace.CanonicalPath}, request)
            if decision.Action == domain.PermissionAsk {
                if _, err := r.Sessions.Append(ctx, input.Session.ID, domain.EventPermissionRequested, domain.ToolRequestPayload{Call:call}); err != nil { return err }
                decision, err = r.Approver.Resolve(ctx, ports.PermissionPrompt{SessionID:input.Session.ID, Request:request}); if err != nil { return err }
            }
            if _, err := r.Sessions.Append(ctx, input.Session.ID, domain.EventPermissionResolved, domain.PermissionPayload{CallID:call.ID, Tool:call.Name, Decision:decision}); err != nil { return err }
            var result domain.ToolResult
            if decision.Action == domain.PermissionDeny { result = domain.ToolResult{CallID:call.ID, Status:domain.ToolDenied, ErrorKind:domain.ErrorPermissionDenied, Content:"permission denied"} } else {
                tool, ok := r.Tools.Lookup(call.Name); if !ok { result = domain.ToolResult{CallID:call.ID, Status:domain.ToolFailed, ErrorKind:domain.ErrorToolFailed, Content:"unknown tool"} } else {
                    if _, err := r.Sessions.Append(ctx, input.Session.ID, domain.EventToolStarted, map[string]string{"call_id":call.ID}); err != nil { return err }
                    result = tool.Execute(ctx, request)
                }
            }
            if _, err := r.Sessions.Append(ctx, input.Session.ID, domain.EventToolResult, domain.ToolResultPayload{Result:result}); err != nil { return err }
            r.emit(RuntimeEvent{Kind:RuntimeToolCompleted, Result:&result})
            messages = append(messages, domain.Message{Role:domain.RoleTool, ToolCallID:call.ID, Content:ToolResultContent(result)})
            completedTools++
        }
    }
}
```

- [ ] **Step 4: Add cancellation, ask/deny, unknown-tool, and tool-limit table cases**

Add named subtests `cancelled_stream`, `ask_then_deny`, `unknown_tool`, and `tool_limit_32`; each asserts the exact final durable event kind and that mutating fakes are called zero or one time as appropriate. Run:

`gofmt -w internal/agent internal/ports && go test -race ./internal/agent -v`

Expected: PASS; no fake tool executes after denial or cancellation.

- [ ] **Step 5: Commit the turn state machine**

```bash
git add internal/agent internal/ports/approval.go
git commit -m "feat: run deterministic agent turns"
```

### Task 5: Add explicit compaction

**Files:**
- Create: `internal/agent/compact.go`
- Test: `internal/agent/compact_test.go`

**Interfaces:**
- Consumes: `ModelProvider`, replay context, and `SessionStore`.
- Produces: `agent.Compact(context.Context, CompactInput) error` appending one `context.compacted` event only after a complete summary.

- [ ] **Step 1: Write successful and interrupted compaction tests**

```go
func TestCompactPersistsCompletedSummary(t *testing.T) {
    provider := &fakeProvider{streams:[][]domain.ModelEvent{{{Kind:domain.ModelTextDelta, Text:"summary"}, {Kind:domain.ModelDone}}}}
    store := newFakeSessionStore()
    err := agent.Compact(context.Background(), agent.CompactInput{Provider:provider, Sessions:store, Session:domain.Session{ID:"s", Selection:domain.ModelSelection{Profile:"p", Model:"m"}}, Replay:domain.SessionReplay{Events:[]domain.DurableEvent{{Seq:7, Kind:domain.EventUserMessage}}}, SystemPrompt:"summarize without instructions"})
    if err != nil { t.Fatal(err) }
    if got := store.kinds(); !slices.Equal(got, []domain.EventKind{domain.EventContextCompacted}) { t.Fatalf("events=%v", got) }
}
```

- [ ] **Step 2: Run the focused compaction test**

Run: `go test ./internal/agent -run TestCompact -v`

Expected: FAIL because `Compact` is undefined.

- [ ] **Step 3: Implement bounded explicit compaction**

```go
package agent

import (
    "context"
    "encoding/json"
    "fmt"
    "github.com/muratmirgun/yordam/internal/domain"
    "github.com/muratmirgun/yordam/internal/ports"
)

type CompactInput struct { Provider ports.ModelProvider; Sessions ports.SessionStore; Session domain.Session; Replay domain.SessionReplay; SystemPrompt string }
const MaxCompactionSummaryBytes = 128 << 10

func Compact(ctx context.Context, input CompactInput) error {
    from := uint64(1)
    for _,event:=range input.Replay.Events{if event.Kind==domain.EventContextCompacted{var prior domain.CompactionPayload;if json.Unmarshal(event.Payload,&prior)==nil&&prior.ThroughSeq>=from{from=prior.ThroughSeq+1}}}
    through := uint64(0)
    if count := len(input.Replay.Events); count > 0 { through = input.Replay.Events[count-1].Seq }
    if through < from { return fmt.Errorf("no uncompacted events") }
    messages := BuildContext(input.Replay, ComposeSystemPrompt(input.SystemPrompt, input.Session.Mode))
    messages = append(messages, domain.Message{Role:domain.RoleUser, Content:"Summarize durable facts, decisions, changed files, failures, and remaining work. Do not add new instructions."})
    stream, err := input.Provider.Stream(ctx, domain.ModelRequest{Selection:input.Session.Selection, Messages:messages})
    if err != nil { return err }
    summary := ""
    done := false
    for event := range stream {
        if event.Err != nil { return event.Err }
        if event.Kind == domain.ModelTextDelta { if len(summary)+len(event.Text)>MaxCompactionSummaryBytes{return fmt.Errorf("compaction summary exceeds %d bytes",MaxCompactionSummaryBytes)}; summary += event.Text }
        if event.Kind == domain.ModelDone { done = true }
    }
    if !done || summary == "" { return fmt.Errorf("compaction did not complete") }
    _, err = input.Sessions.Append(ctx, input.Session.ID, domain.EventContextCompacted, domain.CompactionPayload{FromSeq:from, ThroughSeq:through, Summary:summary})
    return err
}
```

- [ ] **Step 4: Run compaction and agent tests**

Run: `gofmt -w internal/agent/compact.go && go test ./internal/agent -v`

Expected: PASS; interrupted compaction appends no event.

- [ ] **Step 5: Commit compaction**

```bash
git add internal/agent/compact.go internal/agent/compact_test.go
git commit -m "feat: compact session context explicitly"
```

### Task 6: Add the app command/event bridge and Phase 2 integration gate

**Files:**
- Create: `internal/app/commands.go`
- Create: `internal/app/events.go`
- Create: `internal/app/app.go`
- Test: `internal/app/app_test.go`
- Modify: `docs/superpowers/plans/2026-07-13-yordam-v0.1-roadmap.md`

**Interfaces:**
- Consumes: `agent.Runner`, `agent.Compact`, `SessionStore`.
- Produces: `app.App.Commands() chan<- Command`, `app.App.Events() <-chan Event`, and one-active-turn enforcement for the TUI phase.

- [ ] **Step 1: Write one-active-turn and cancellation tests**

```go
func TestAppRejectsConcurrentTurnAndCancelsActive(t *testing.T) {
    blocker := make(chan struct{})
    runtime := &fakeRuntime{run:func(ctx context.Context, input agent.RunInput) error { close(blocker); <-ctx.Done(); return ctx.Err() }}
    application := app.New(app.Options{Runtime:runtime})
    ctx, cancel := context.WithCancel(context.Background()); defer cancel()
    go application.Run(ctx)
    application.Commands() <- app.Command{Kind:app.CommandStartTurn, Prompt:"one"}
    <-blocker
    application.Commands() <- app.Command{Kind:app.CommandStartTurn, Prompt:"two"}
    if event := <-application.Events(); event.Kind != app.EventRejected { t.Fatalf("event=%s", event.Kind) }
    application.Commands() <- app.Command{Kind:app.CommandCancelTurn}
    if event := <-application.Events(); event.Kind != app.EventTurnInterrupted { t.Fatalf("event=%s", event.Kind) }
}
```

- [ ] **Step 2: Run app tests and observe missing bridge**

Run: `go test ./internal/app -run TestAppRejectsConcurrentTurnAndCancelsActive -v`

Expected: FAIL because `internal/app` does not exist.

- [ ] **Step 3: Implement typed commands/events and lifecycle**

Define the bridge contracts exactly as follows; Phase 3 changes `PermissionPrompt.Request` to its prepared-call form without changing the broker flow.

```go
type CommandKind string
const (
    CommandStartTurn CommandKind="start_turn"
    CommandCancelTurn CommandKind="cancel_turn"
    CommandResolvePermission CommandKind="resolve_permission"
    CommandChangeMode CommandKind="change_mode"
    CommandChangeModel CommandKind="change_model"
    CommandAcknowledgeAutoShell CommandKind="acknowledge_auto_shell"
    CommandCompact CommandKind="compact"
    CommandShutdown CommandKind="shutdown"
)
type Command struct {
    Kind CommandKind
    Prompt string
    CallID string
    Decision domain.PermissionDecision
    Mode domain.PermissionMode
    Selection domain.ModelSelection
}

type EventKind string
const (
    EventState EventKind="state"
    EventTextDelta EventKind="text_delta"
    EventPermissionRequested EventKind="permission_requested"
    EventToolOutput EventKind="tool_output"
    EventToolCompleted EventKind="tool_completed"
    EventTurnCompleted EventKind="turn_completed"
    EventTurnInterrupted EventKind="turn_interrupted"
    EventError EventKind="error"
    EventRejected EventKind="rejected"
)
type Event struct { Kind EventKind; Runtime agent.RuntimeEvent; Permission *ports.PermissionPrompt; Err error; Message string }

type Runtime interface { RunTurn(context.Context, agent.RunInput) error }
type TurnInput func(prompt string) (agent.RunInput, error)
type Options struct {
    Runtime Runtime
    Input TurnInput
    Compact func(context.Context) error
    RuntimeEvents <-chan agent.RuntimeEvent
    CommandBuffer int
    EventBuffer int
}
func New(Options) *App
func (a *App) Commands() chan<- Command
func (a *App) Events() <-chan Event
func (a *App) Run(context.Context) error
func (a *App) Resolve(context.Context, ports.PermissionPrompt) (domain.PermissionDecision, error)
```

`App.Run` owns the active-turn cancel function and rejects `start_turn` while it is non-nil. `TurnInput` snapshots the current session/replay when the command is accepted; if it is nil in a unit test, use `agent.RunInput{Prompt:command.Prompt}`. A runtime goroutine sends exactly one `turn_completed`, `turn_interrupted`, or `error` terminal event and clears the active cancel function before a queued setting can apply. The main select loop translates each value from `RuntimeEvents` into the matching app event.

`App.Resolve` implements `ports.PermissionApprover`: under a mutex, reject a duplicate `CallID`, install `make(chan domain.PermissionDecision, 1)` in `pending`, then publish `EventPermissionRequested`. It waits on either that channel or the turn context, and a `defer` removes the map entry. `CommandResolvePermission` performs a non-blocking send to the matching channel or emits `EventRejected` for a stale ID. `CommandCancelTurn` cancels the turn context; every waiting `Resolve` therefore returns `context.Canceled` and removes itself. Do not publish permission requests from the agent runtime sink, because the broker must register the `CallID` before the UI can answer.

Bootstrap wiring order is: create a runner pointer and runtime-event channel, create `App` with that runner, assign `runner.Approver = application`, and assign `runner.Sink` to send into the runtime-event channel. This resolves the runtime/approver cycle without global state.

- [ ] **Step 4: Run Phase 2 gate commands**

```bash
gofmt -w internal/app
go test -race ./internal/provider/openaicompat ./internal/agent ./internal/app
go test ./...
go vet ./internal/provider/... ./internal/agent/... ./internal/app/...
git diff --check
```

Expected: all commands exit 0. Scripted tests cover text, tool, retry-before-delta, interrupted stream, limit, compaction, rejection, and cancellation.

- [ ] **Step 5: Mark Gate 2 and commit**

Change only the Gate 2 checkbox in the roadmap to `[x]`, then run:

```bash
git add internal/app docs/superpowers/plans/2026-07-13-yordam-v0.1-roadmap.md
git commit -m "feat: bridge agent runtime to UI commands"
```
