package agent_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
)

func TestBuildContextUsesLatestCompaction(t *testing.T) {
	replay := domain.SessionReplay{Events: []domain.DurableEvent{
		{Seq: 1, Kind: domain.EventUserMessage, Payload: payload(t, domain.MessagePayload{Content: "old"})},
		{Seq: 2, Kind: domain.EventContextCompacted, Payload: payload(t, domain.CompactionPayload{ThroughSeq: 1, Summary: "old work summary"})},
		{Seq: 3, Kind: domain.EventUserMessage, Payload: payload(t, domain.MessagePayload{Content: "new"})},
	}}

	got, err := agent.BuildContext(replay, "system", contextLease(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Content != "system" || got[1].Content != "old work summary" || got[2].Content != "new" {
		t.Fatalf("context=%#v", got)
	}
}

func TestBuildLegacyContextSourcesPreservesCurrentCompactionTranscript(t *testing.T) {
	replay := domain.SessionReplay{Events: []domain.DurableEvent{
		{Seq: 1, Kind: domain.EventUserMessage, Payload: payload(t, domain.MessagePayload{Content: "old"})},
		{Seq: 2, Kind: domain.EventContextCompacted, Payload: payload(t, domain.CompactionPayload{ThroughSeq: 1, Summary: "old work summary"})},
		{Seq: 3, Kind: domain.EventUserMessage, Payload: payload(t, domain.MessagePayload{Content: "new"})},
	}}
	sources, err := agent.BuildLegacyContextSources(replay, "system", contextLease(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 3 || sources[0].Content[0].Text != "system" || sources[1].Content[0].Text != "old work summary" || sources[2].Content[0].Text != "new" {
		t.Fatalf("sources=%#v", sources)
	}
	for _, source := range sources {
		if source.Validate() != nil || source.Digest.Validate() != nil || source.Provenance != "legacy_context_adapter_v1" {
			t.Fatalf("source=%#v validation=%v", source, source.Validate())
		}
	}
}

func TestBuildLegacyContextSourcesPreservesStructuredToolResultAssociation(t *testing.T) {
	call := domain.ToolCall{ID: "call-1", Name: "read", Arguments: json.RawMessage(`{"path":"a.go"}`)}
	result := domain.ToolResult{CallID: call.ID, Status: domain.ToolDenied, ErrorKind: domain.ErrorPermissionDenied, Content: "denied detail"}
	replay := domain.SessionReplay{Events: []domain.DurableEvent{
		{Seq: 1, Kind: domain.EventAssistantMessage, Payload: payload(t, domain.MessagePayload{Content: "checking", ToolCalls: []domain.ToolCall{call}})},
		{Seq: 2, Kind: domain.EventToolResult, Payload: payload(t, domain.ToolResultPayload{Result: result})},
	}}
	sources, err := agent.BuildLegacyContextSources(replay, "system", contextLease(t))
	if err != nil {
		t.Fatal(err)
	}
	toolSource := sources[len(sources)-1]
	if len(toolSource.Content) != 1 || toolSource.Content[0].Kind != protocol.ContentToolResult || toolSource.Content[0].ToolResult == nil {
		t.Fatalf("tool source=%#v", toolSource)
	}
	block := toolSource.Content[0].ToolResult
	if block.CallID != call.ID || block.Status != string(domain.ToolDenied) || !strings.Contains(string(block.JSON), `"content":"denied detail"`) {
		t.Fatalf("tool result=%#v", block)
	}
}

func TestToolResultContentIsStructuredAndBounded(t *testing.T) {
	large := strings.Repeat("nested excerpt ", 32<<10)
	result := domain.ToolResult{
		CallID:      "call-1",
		Status:      domain.ToolSucceeded,
		Content:     large,
		ArtifactIDs: []string{"artifact-1"},
		FileChange: &domain.FileChange{
			CallID: "call-1",
			Path:   "/workspace/a.txt",
			Diff:   large,
		},
		WorkspaceChanges: &domain.WorkspaceChanges{Status: large, Diff: large, Notice: large},
		Truncated:        true,
	}

	encoded := agent.ToolResultContent(result)
	if len(encoded) > 32<<10 {
		t.Fatalf("model tool result bytes=%d want <=%d", len(encoded), 32<<10)
	}
	var projected map[string]any
	if err := json.Unmarshal([]byte(encoded), &projected); err != nil {
		t.Fatal(err)
	}
	if projected["call_id"] != "call-1" || projected["status"] != string(domain.ToolSucceeded) || projected["content"] == "" {
		t.Fatalf("projected result=%#v", projected)
	}
	if _, exists := projected["file_change"]; exists {
		t.Fatalf("model result duplicated file_change excerpt: %#v", projected)
	}
	if _, exists := projected["workspace_changes"]; exists {
		t.Fatalf("model result duplicated workspace_changes excerpt: %#v", projected)
	}
}

func TestToolResultContentUsesGenerationLease(t *testing.T) {
	registry := secret.NewRegistry()
	lease, err := registry.Acquire("generation-model-result", [][]byte{[]byte("model-secret")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Close() }()
	content, err := agent.ToolResultContentLeased(domain.ToolResult{
		CallID: "call", Status: domain.ToolSucceeded, Content: "bW9kZWwtc2VjcmV0",
	}, lease)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(content, "bW9kZWwtc2VjcmV0") || !strings.Contains(content, "[REDACTED]") {
		t.Fatalf("model tool result=%q", content)
	}
}

func TestBuildContextPreservesStructuredToolExchange(t *testing.T) {
	call := domain.ToolCall{
		ID:        "call-1",
		Name:      "read",
		Arguments: json.RawMessage(`{"path":"secret.txt"}`),
	}
	denied := domain.ToolResult{
		CallID:    call.ID,
		Status:    domain.ToolDenied,
		ErrorKind: domain.ErrorPermissionDenied,
		Content:   "bounded denial detail",
		Truncated: true,
	}
	replay := domain.SessionReplay{Events: []domain.DurableEvent{
		{Seq: 1, Kind: domain.EventUserMessage, Payload: payload(t, domain.MessagePayload{Content: "inspect"})},
		{Seq: 2, Kind: domain.EventAssistantMessage, Payload: payload(t, domain.MessagePayload{Content: "checking", ToolCalls: []domain.ToolCall{call}})},
		{Seq: 3, Kind: domain.EventToolResult, Payload: payload(t, domain.ToolResultPayload{Result: denied})},
	}}

	got, err := agent.BuildContext(replay, "system", contextLease(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("context=%#v", got)
	}
	if got[1].Role != domain.RoleUser || got[1].Content != "inspect" {
		t.Fatalf("user message=%#v", got[1])
	}
	if got[2].Role != domain.RoleAssistant || got[2].Content != "checking" || !reflect.DeepEqual(got[2].ToolCalls, []domain.ToolCall{call}) {
		t.Fatalf("assistant message=%#v", got[2])
	}
	if got[3].Role != domain.RoleTool || got[3].ToolCallID != call.ID {
		t.Fatalf("tool message=%#v", got[3])
	}
	var result domain.ToolResult
	if err := json.Unmarshal([]byte(got[3].Content), &result); err != nil {
		t.Fatalf("tool result JSON: %v", err)
	}
	if !reflect.DeepEqual(result, denied) {
		t.Fatalf("tool result=%#v want=%#v", result, denied)
	}
}

func TestBuildContextIgnoresInvalidLaterCompaction(t *testing.T) {
	replay := domain.SessionReplay{Events: []domain.DurableEvent{
		{Seq: 1, Kind: domain.EventUserMessage, Payload: payload(t, domain.MessagePayload{Content: "old"})},
		{Seq: 2, Kind: domain.EventContextCompacted, Payload: payload(t, domain.CompactionPayload{ThroughSeq: 1, Summary: "valid summary"})},
		{Seq: 3, Kind: domain.EventUserMessage, Payload: payload(t, domain.MessagePayload{Content: "new"})},
		{Seq: 4, Kind: domain.EventContextCompacted, Payload: json.RawMessage(`{"through_seq":`)},
		{Seq: 5, Kind: domain.EventContextCompacted, Payload: payload(t, domain.CompactionPayload{FromSeq: 6, ThroughSeq: 4, Summary: "invalid summary"})},
	}}

	got, err := agent.BuildContext(replay, "system", contextLease(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[1].Content != "valid summary" || got[2].Content != "new" {
		t.Fatalf("context=%#v", got)
	}
}

func TestBuildContextAdmitsEveryHistoricalModelField(t *testing.T) {
	registry := secret.NewRegistry()
	lease, err := registry.Acquire("generation-history", [][]byte{[]byte("history-secret")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Close() })
	encoded := "aGlzdG9yeS1zZWNyZXQ="
	replay := domain.SessionReplay{Events: []domain.DurableEvent{
		{Seq: 1, Kind: domain.EventContextCompacted, Payload: payload(t, domain.CompactionPayload{ThroughSeq: 0, Summary: encoded})},
		{Seq: 2, Kind: domain.EventUserMessage, Payload: payload(t, domain.MessagePayload{Content: encoded})},
		{Seq: 3, Kind: domain.EventAssistantMessage, Payload: payload(t, domain.MessagePayload{Content: encoded, ToolCalls: []domain.ToolCall{{ID: encoded, Name: encoded, Arguments: json.RawMessage(`{"value":"aGlzdG9yeS1zZWNyZXQ="}`)}}})},
		{Seq: 4, Kind: domain.EventToolResult, Payload: payload(t, domain.ToolResultPayload{Result: domain.ToolResult{CallID: encoded, Status: domain.ToolSucceeded, Content: encoded, ArtifactIDs: []string{encoded}}})},
	}}
	messages, err := agent.BuildContext(replay, encoded, lease)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(messages)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), encoded) || !strings.Contains(string(raw), "[REDACTED]") {
		t.Fatalf("historical context was not fully admitted: %s", raw)
	}
}

func TestModelContextRejectsMissingOrClosedLease(t *testing.T) {
	if _, err := agent.BuildContext(domain.SessionReplay{}, "system", nil); !errors.Is(err, secret.ErrLeaseClosed) {
		t.Fatalf("nil context lease error=%v", err)
	}
	if _, err := agent.ToolResultContentLeased(domain.ToolResult{}, nil); !errors.Is(err, secret.ErrLeaseClosed) {
		t.Fatalf("nil tool result lease error=%v", err)
	}
	lease := contextLease(t)
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := agent.BuildContext(domain.SessionReplay{}, "system", lease); !errors.Is(err, secret.ErrLeaseClosed) {
		t.Fatalf("closed context lease error=%v", err)
	}
}

func contextLease(t *testing.T) *secret.Lease {
	t.Helper()
	registry := secret.NewRegistry()
	lease, err := registry.Acquire("generation-context", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Close() })
	return lease
}

func payload(t testing.TB, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
