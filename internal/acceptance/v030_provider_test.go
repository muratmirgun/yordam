//go:build acceptance

package acceptance_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
)

type providerStep struct {
	Role          string
	RequiredTools []string
	RequiredText  []string
	SSE           string
}

type capturedProviderRequest struct {
	Model       string
	Roles       []string
	Tools       []string
	Body        string
	Step        int
	Role        string
	ToolCallIDs []string
}

type scriptedProvider struct {
	URL      string
	Requests []capturedProviderRequest

	mu           sync.Mutex
	steps        []providerStep
	server       *httptest.Server
	firstFailure string
}

type providerWireRequest struct {
	Model    string                `json:"model"`
	Messages []providerWireMessage `json:"messages"`
	Tools    []providerWireTool    `json:"tools,omitempty"`
	Stream   bool                  `json:"stream"`
}

type providerWireMessage struct {
	Role       string                 `json:"role"`
	Content    string                 `json:"content,omitempty"`
	ToolCallID string                 `json:"tool_call_id,omitempty"`
	ToolCalls  []providerWireToolCall `json:"tool_calls,omitempty"`
}

type providerWireTool struct {
	Type     string `json:"type"`
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Parameters  json.RawMessage `json:"parameters"`
	} `json:"function"`
}

type providerWireToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

func newScriptedProvider(t *testing.T, steps []providerStep) *scriptedProvider {
	t.Helper()
	copySteps := append([]providerStep(nil), steps...)
	for index := range copySteps {
		copySteps[index].RequiredTools = append([]string(nil), copySteps[index].RequiredTools...)
		copySteps[index].RequiredText = append([]string(nil), copySteps[index].RequiredText...)
		if err := validateProviderStep(copySteps[index]); err != nil {
			t.Fatalf("provider step %d: %v", index, err)
		}
	}
	provider := &scriptedProvider{steps: copySteps}
	provider.server = httptest.NewServer(http.HandlerFunc(provider.serveHTTP))
	provider.URL = provider.server.URL
	return provider
}

func (p *scriptedProvider) Close() {
	if p != nil && p.server != nil {
		p.server.Close()
	}
}

func (p *scriptedProvider) AssertComplete(t *testing.T) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.Requests) != len(p.steps) {
		t.Fatalf("provider script consumed %d of %d steps", len(p.Requests), len(p.steps))
	}
}

func (p *scriptedProvider) serveHTTP(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost || request.URL.Path != "/v1/chat/completions" {
		http.Error(response, "unexpected provider endpoint", http.StatusNotFound)
		return
	}
	if request.Header.Get("Content-Type") != "application/json" || request.Header.Get("Accept") != "text/event-stream" {
		http.Error(response, "provider headers do not match streaming contract", http.StatusBadRequest)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(request.Body, 10<<20+1))
	if err != nil || len(raw) > 10<<20 {
		http.Error(response, "provider request body is unreadable or oversized", http.StatusBadRequest)
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	stepIndex := len(p.Requests)
	if stepIndex >= len(p.steps) {
		http.Error(response, "provider script has no remaining step", http.StatusConflict)
		return
	}
	captured, err := validateProviderRequest(raw, stepIndex, p.steps[stepIndex])
	if err != nil {
		failure := fmt.Sprintf("provider step %d mismatch: %v", stepIndex, err)
		if p.firstFailure == "" {
			p.firstFailure = failure
		}
		http.Error(response, p.firstFailure, http.StatusBadRequest)
		return
	}
	p.firstFailure = ""
	p.Requests = append(p.Requests, captured)
	response.Header().Set("Content-Type", "text/event-stream")
	_, _ = io.WriteString(response, p.steps[stepIndex].SSE)
}

func validateProviderStep(step providerStep) error {
	if step.Role != "parent" && step.Role != "child" && step.Role != "compaction" {
		return fmt.Errorf("invalid role %q", step.Role)
	}
	if step.SSE == "" {
		return fmt.Errorf("empty SSE response")
	}
	seen := make(map[string]bool, len(step.RequiredTools))
	for _, name := range step.RequiredTools {
		if name == "" || seen[name] {
			return fmt.Errorf("invalid or duplicate tool %q", name)
		}
		seen[name] = true
	}
	if step.Role == "parent" && !seen["subagent"] {
		return fmt.Errorf("parent step must expose subagent")
	}
	if (step.Role == "child" || step.Role == "compaction") && seen["subagent"] {
		return fmt.Errorf("%s step must not expose subagent", step.Role)
	}
	if step.Role == "compaction" {
		if len(step.RequiredTools) != 0 {
			return fmt.Errorf("compaction step must expose no tools")
		}
		for _, field := range []string{"goal", "constraints", "decisions", "files", "commands_and_tests", "unresolved", "children", "skills", "unknown_effects"} {
			if !strings.Contains(step.SSE, `\"`+field+`\"`) {
				return fmt.Errorf("compaction response lacks %q", field)
			}
		}
	}
	return nil
}

func validateProviderRequest(raw []byte, stepIndex int, step providerStep) (capturedProviderRequest, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var wire providerWireRequest
	if err := decoder.Decode(&wire); err != nil {
		return capturedProviderRequest{}, fmt.Errorf("decode exact request: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return capturedProviderRequest{}, fmt.Errorf("request has trailing JSON")
	}
	if wire.Model != "self-host-model" {
		return capturedProviderRequest{}, fmt.Errorf("model=%q want self-host-model", wire.Model)
	}
	if !wire.Stream {
		return capturedProviderRequest{}, fmt.Errorf("stream must be true")
	}
	if len(wire.Messages) < 2 || wire.Messages[0].Role != "system" || wire.Messages[1].Role != "user" {
		return capturedProviderRequest{}, fmt.Errorf("message roles must start system,user")
	}
	roles, callIDs, err := validateProviderMessages(wire.Messages)
	if err != nil {
		return capturedProviderRequest{}, err
	}
	tools := make([]string, len(wire.Tools))
	for index, tool := range wire.Tools {
		if tool.Type != "function" || tool.Function.Name == "" || tool.Function.Description == "" || len(tool.Function.Parameters) == 0 {
			return capturedProviderRequest{}, fmt.Errorf("tool %d is incomplete", index)
		}
		tools[index] = tool.Function.Name
	}
	if !slices.Equal(tools, step.RequiredTools) {
		return capturedProviderRequest{}, fmt.Errorf("tools=%v want exact ordered %v", tools, step.RequiredTools)
	}
	if step.Role == "parent" && !slices.Contains(tools, "subagent") {
		return capturedProviderRequest{}, fmt.Errorf("parent exposure lacks subagent")
	}
	if step.Role != "parent" && slices.Contains(tools, "subagent") {
		return capturedProviderRequest{}, fmt.Errorf("%s exposure leaked subagent", step.Role)
	}
	body := string(raw)
	for _, required := range step.RequiredText {
		if required == "" || !strings.Contains(body, required) {
			return capturedProviderRequest{}, fmt.Errorf("required text %q is absent", required)
		}
	}
	if stepIndex == 0 && strings.Contains(body, "Inspect before editing") {
		return capturedProviderRequest{}, fmt.Errorf("skill body loaded before the skill tool result")
	}
	if step.Role == "compaction" {
		if len(tools) != 0 || !strings.Contains(body, "Return only canonical JSON") || !strings.Contains(body, "normalized_sources") {
			return capturedProviderRequest{}, fmt.Errorf("compaction summary contract is incomplete")
		}
	}
	return capturedProviderRequest{Model: wire.Model, Roles: roles, Tools: tools, Body: body, Step: stepIndex, Role: step.Role, ToolCallIDs: callIDs}, nil
}

func validateProviderMessages(messages []providerWireMessage) ([]string, []string, error) {
	roles := make([]string, 0, len(messages))
	pending := make(map[string]bool)
	callIDs := make([]string, 0)
	for index, message := range messages {
		roles = append(roles, message.Role)
		switch message.Role {
		case "system", "user":
			if len(message.ToolCalls) != 0 || message.ToolCallID != "" {
				return nil, nil, fmt.Errorf("message %d role %s carries tool identity", index, message.Role)
			}
		case "assistant":
			if message.ToolCallID != "" {
				return nil, nil, fmt.Errorf("assistant message %d carries tool result ID", index)
			}
			for callIndex, call := range message.ToolCalls {
				argumentsValid := json.Valid([]byte(call.Function.Arguments))
				if call.ID == "" || call.Type != "function" || call.Function.Name == "" || !argumentsValid || pending[call.ID] {
					return nil, nil, fmt.Errorf("tool dup=%t msg=%d item=%d total=%d id=%q", pending[call.ID], index, callIndex, len(message.ToolCalls), call.ID)
				}
				pending[call.ID] = true
				callIDs = append(callIDs, call.ID)
			}
		case "tool":
			if message.ToolCallID == "" || !pending[message.ToolCallID] || len(message.ToolCalls) != 0 {
				return nil, nil, fmt.Errorf("tool message %d does not bind an exact prior call", index)
			}
			delete(pending, message.ToolCallID)
		default:
			return nil, nil, fmt.Errorf("message %d has unsupported role %q", index, message.Role)
		}
	}
	if len(pending) != 0 {
		return nil, nil, fmt.Errorf("assistant tool call lacks a result")
	}
	return roles, callIDs, nil
}

func providerToolCallSSE(callID, name, arguments string) string {
	return fmt.Sprintf("data: {\"id\":\"self-host-script\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":%q,\"type\":\"function\",\"function\":{\"name\":%q,\"arguments\":%q}}]}}]}\n\ndata: {\"id\":\"self-host-script\",\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n", callID, name, arguments)
}

func providerTextSSE(content string) string {
	return fmt.Sprintf("data: {\"id\":\"self-host-script\",\"choices\":[{\"delta\":{\"content\":%q},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", content)
}

func TestScriptedProviderAdvancesOnlyAfterExactSemanticMatch(t *testing.T) {
	steps := []providerStep{
		{Role: "parent", RequiredTools: []string{"read", "search", "skill", "subagent", "edit", "shell"}, RequiredText: []string{"go-development"}, SSE: providerToolCallSSE("load-skill", "skill", `{"name":"go-development"}`)},
		{Role: "parent", RequiredTools: []string{"read", "search", "skill", "subagent", "edit", "shell"}, RequiredText: []string{"load-skill", "Inspect before editing"}, SSE: providerToolCallSSE("delegate-readme", "subagent", `{"task":"change only the README heading","expected_output":"changed file and verification","context":"read, search, exact edit, then focused test"}`)},
		{Role: "child", RequiredTools: []string{"read", "search", "skill", "edit", "shell"}, RequiredText: []string{"change only the README heading"}, SSE: providerToolCallSSE("child-read", "read", `{"path":"README.md"}`)},
		{Role: "child", RequiredTools: []string{"read", "search", "skill", "edit", "shell"}, RequiredText: []string{"child-read", "# Yordam"}, SSE: providerToolCallSSE("child-search", "search", `{"query":"# Yordam","path":"README.md"}`)},
		{Role: "child", RequiredTools: []string{"read", "search", "skill", "edit", "shell"}, RequiredText: []string{"child-search", "sha256-before"}, SSE: providerToolCallSSE("child-edit", "edit", `{"path":"README.md","expected_sha256":"sha256-before","replacements":[{"old":"# Yordam\n","new":"# Yordam self-hosted\n"}]}`)},
		{Role: "child", RequiredTools: []string{"read", "search", "skill", "edit", "shell"}, RequiredText: []string{"child-edit"}, SSE: providerToolCallSSE("child-diff-check", "shell", `{"command":"git diff --check -- README.md","cwd":"."}`)},
		{Role: "child", RequiredTools: []string{"read", "search", "skill", "edit", "shell"}, RequiredText: []string{"child-diff-check"}, SSE: providerTextSSE("child changed README.md and git diff --check passed")},
		{Role: "parent", RequiredTools: []string{"read", "search", "skill", "subagent", "edit", "shell"}, RequiredText: []string{"delegate-readme", "child changed README.md", "terminal_cursor"}, SSE: providerToolCallSSE("parent-test", "shell", `{"command":"go test ./...","cwd":"."}`)},
		{Role: "parent", RequiredTools: []string{"read", "search", "skill", "subagent", "edit", "shell"}, RequiredText: []string{"parent-test"}, SSE: providerTextSSE("self-hosting change and complete verification succeeded")},
		{Role: "compaction", RequiredText: []string{"Return only canonical JSON", "normalized_sources"}, SSE: providerTextSSE(`{"goal":"self-host","constraints":[],"decisions":[],"files":["README.md"],"commands_and_tests":["git diff --check -- README.md","go test ./..."],"unresolved":[],"children":[],"skills":["go-development"],"unknown_effects":[]}`)},
	}
	provider := newScriptedProvider(t, steps)
	defer provider.Close()

	for index, step := range steps {
		response := postProviderFixture(t, provider.URL, scriptedProviderRequest(index, step))
		if response != step.SSE {
			t.Fatalf("step %d response=%q want=%q", index, response, step.SSE)
		}
	}
	if len(provider.Requests) != len(steps) {
		t.Fatalf("captured requests=%d want=%d", len(provider.Requests), len(steps))
	}
	provider.AssertComplete(t)
}

func TestScriptedProviderFailsClosedWithoutAdvancingOrRepeatingEffects(t *testing.T) {
	first := providerStep{Role: "parent", RequiredTools: []string{"read", "subagent"}, RequiredText: []string{"first"}, SSE: providerToolCallSSE("effect-once", "read", `{"path":"README.md"}`)}
	second := providerStep{Role: "parent", RequiredTools: []string{"read", "subagent"}, RequiredText: []string{"second"}, SSE: providerTextSSE("done")}
	provider := newScriptedProvider(t, []providerStep{first, second})
	defer provider.Close()

	bad := scriptedProviderRequest(0, first)
	bad["model"] = "wrong-model"
	if status, body := postProviderFixtureStatus(t, provider.URL, bad); status != http.StatusBadRequest || !strings.Contains(body, "model") {
		t.Fatalf("mismatch status=%d body=%q", status, body)
	}
	if len(provider.Requests) != 0 {
		t.Fatalf("mismatch advanced script: requests=%d", len(provider.Requests))
	}

	if got := postProviderFixture(t, provider.URL, scriptedProviderRequest(0, first)); got != first.SSE {
		t.Fatalf("first valid response=%q", got)
	}
	if status, body := postProviderFixtureStatus(t, provider.URL, scriptedProviderRequest(0, first)); status != http.StatusBadRequest || !strings.Contains(body, "step") {
		t.Fatalf("reordered request status=%d body=%q", status, body)
	}
	if len(provider.Requests) != 1 {
		t.Fatalf("reordered request repeated prior effect: requests=%d", len(provider.Requests))
	}
}

func scriptedProviderRequest(index int, step providerStep) map[string]any {
	messages := []map[string]any{{"role": "system", "content": "fixture system"}, {"role": "user", "content": strings.Join(step.RequiredText, " ")}}
	if index > 0 {
		messages = append(messages,
			map[string]any{
				"role": "assistant", "content": "prior",
				"tool_calls": []map[string]any{{
					"id": stepCallID(index), "type": "function",
					"function": map[string]any{"name": "read", "arguments": `{}`},
				}},
			},
			map[string]any{"role": "tool", "tool_call_id": stepCallID(index), "content": strings.Join(step.RequiredText, " ")},
		)
	}
	tools := make([]map[string]any, 0, len(step.RequiredTools))
	for _, name := range step.RequiredTools {
		tools = append(tools, map[string]any{"type": "function", "function": map[string]any{"name": name, "description": name + " fixture tool", "parameters": map[string]any{"type": "object"}}})
	}
	return map[string]any{"model": "self-host-model", "messages": messages, "tools": tools, "stream": true}
}

func stepCallID(index int) string { return "prior-call-" + string(rune('a'+index)) }

func postProviderFixture(t *testing.T, url string, payload map[string]any) string {
	t.Helper()
	status, body := postProviderFixtureStatus(t, url, payload)
	if status != http.StatusOK {
		t.Fatalf("provider status=%d body=%q", status, body)
	}
	return body
}

func postProviderFixtureStatus(t *testing.T, url string, payload map[string]any) (int, string) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, url+"/v1/chat/completions", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "text/event-stream")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, string(body)
}
