package openaicompat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/provider"
)

func TestAdapterNormalizesSupportedBlocksAndTools(t *testing.T) {
	adapter := NewAdapter(nil)
	request := protocol.ModelRequest{
		RequestID: "request-1", ProviderID: "openai", ModelID: "model-a",
		Messages: []protocol.ModelMessage{
			{Role: "user", Blocks: []protocol.ContentBlock{{Kind: protocol.ContentText, Text: "hello"}}},
			{Role: "assistant", Blocks: []protocol.ContentBlock{{Kind: protocol.ContentText, Text: "calling"}, {Kind: protocol.ContentToolUse, ToolUse: &protocol.ToolUseBlock{CallID: "call-1", Alias: "read", Arguments: json.RawMessage(`{"path":"a.go"}`)}}}},
			{Role: "tool", Blocks: []protocol.ContentBlock{{Kind: protocol.ContentToolResult, ToolResult: &protocol.ToolResultBlock{CallID: "call-1", Status: "succeeded", JSON: json.RawMessage(`{"content":"ok"}`)}}}},
		},
		Tools: protocol.ToolExposure{CatalogRevision: "tools-r1", Tools: []protocol.ExposedTool{{Alias: "read", Identity: protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "read"}, Description: "Read", InputSchema: json.RawMessage(`{"type":"object"}`)}}, Aliases: []protocol.ToolAliasBinding{{Alias: "read", Identity: protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "read"}, SourceRevision: "r1", DescriptorDigest: adapterDigest('a')}}},
	}
	got, err := adapter.normalize(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if got.Model != "model-a" || len(got.Messages) != 3 || got.Messages[1].Content != "calling" || len(got.Messages[1].ToolCalls) != 1 || got.Messages[2].ToolCallID != "call-1" || got.Messages[2].Content != `{"content":"ok"}` || len(got.Tools) != 1 {
		t.Fatalf("normalized=%#v", got)
	}
}

func TestRouterNormalizationIsEffectFree(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("normalization opened a network connection")
	}))
	defer server.Close()
	router, err := NewRouter(map[string]*Client{"openai": New(ClientOptions{HTTPClient: server.Client(), BaseURL: server.URL})})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := router.Normalize(context.Background(), protocol.ModelRequest{RequestID: "request", ProviderID: "openai", ModelID: "model", Messages: []protocol.ModelMessage{{Role: "user", Blocks: []protocol.ContentBlock{{Kind: protocol.ContentText, Text: "hello"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if !prepared.Valid() {
		t.Fatalf("prepared=%#v", prepared)
	}
}

func TestAdapterNormalizesStreamEventsWithOrderingUsageAndTerminalMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("X-Request-ID", "header-request")
		response.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintln(response, `data: {"id":"server-request","choices":[{"delta":{"content":"hello","tool_calls":[{"index":1,"id":"call-b","function":{"name":"write","arguments":"{\"b\":"}},{"index":0,"id":"call-a","function":{"name":"read","arguments":"{\"a\":"}}]}}]}`)
		fmt.Fprintln(response)
		fmt.Fprintln(response, `data: {"id":"server-request","choices":[{"delta":{"tool_calls":[{"index":1,"function":{"arguments":"2}"}},{"index":0,"function":{"arguments":"1}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":11,"completion_tokens":7,"prompt_tokens_details":{"cached_tokens":3},"completion_tokens_details":{"reasoning_tokens":2}}}`)
		fmt.Fprintln(response)
		fmt.Fprintln(response, "data: [DONE]")
	}))
	defer server.Close()
	client := New(ClientOptions{HTTPClient: server.Client(), BaseURL: server.URL, RetryDelays: []time.Duration{}})
	adapter := NewAdapter(client)
	stream, err := adapter.stream(context.Background(), protocol.ModelRequest{RequestID: "client-request", ProviderID: "openai", ModelID: "model-a", Messages: []protocol.ModelMessage{{Role: "user", Blocks: []protocol.ContentBlock{{Kind: protocol.ContentText, Text: "go"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	var events []protocol.ModelEvent
	for event := range stream {
		events = append(events, event)
	}
	if len(events) != 6 {
		t.Fatalf("events=%#v", events)
	}
	if events[0].Kind != protocol.ModelEventContentDelta || events[1].Kind != protocol.ModelEventUsageUpdate || events[2].ToolIntent.CallID != "call-a" || events[3].ToolIntent.CallID != "call-b" || events[4].Kind != protocol.ModelEventContentBlock || events[5].Terminal.Reason != "tool_use" || events[5].Terminal.NativeReason != "tool_calls" || events[5].Terminal.ServerRequestID != "server-request" {
		t.Fatalf("events=%#v", events)
	}
	if events[1].Usage.Input.Value != 11 || events[1].Usage.Cached.Value != 3 || events[1].Usage.Reasoning.Value != 2 {
		t.Fatalf("usage=%#v", events[1].Usage)
	}
	for index, event := range events {
		if event.Sequence != uint64(index+1) || event.Validate() != nil {
			t.Fatalf("event[%d]=%#v validation=%v", index, event, event.Validate())
		}
	}
}

func TestAdapterEmitsRefusalAndTypedErrors(t *testing.T) {
	refusal := normalizeLegacyEvents([]domain.ModelEvent{{Kind: domain.ModelRefusalDelta, Refusal: "cannot comply"}, {Kind: domain.ModelDone, RequestID: "req", FinishReason: "content_filter"}})
	if len(refusal) != 2 || refusal[0].Kind != protocol.ModelEventContentBlock || refusal[0].Block.Kind != protocol.ContentRefusal || refusal[1].Terminal.Reason != "refusal" {
		t.Fatalf("refusal=%#v", refusal)
	}
	typed := &domain.TypedError{Kind: domain.ErrorProviderRetryable, Message: "overloaded"}
	errors := normalizeLegacyEvents([]domain.ModelEvent{{Kind: domain.ModelStreamError, Err: typed}})
	if len(errors) != 1 || errors[0].Kind != protocol.ModelEventError || errors[0].Error.Code != string(domain.ErrorProviderRetryable) || !errors[0].Error.Retryable {
		t.Fatalf("errors=%#v", errors)
	}
}

func TestAdapterDoesNotEmulateUnsupportedPreferredStructuredOutput(t *testing.T) {
	request := protocol.ModelRequest{
		Requirements: []protocol.CapabilityRequirement{{Capability: protocol.CapabilityStructuredOutput, Level: protocol.CapabilityPreferred}},
		Plan:         protocol.NegotiatedProviderPlan{Body: protocol.NegotiatedProviderPlanBody{Descriptor: protocol.ModelDescriptor{Capabilities: []protocol.CapabilityFact{{Capability: protocol.CapabilityStructuredOutput, State: protocol.CapabilityUnsupported}}}}},
	}
	_, err := NewAdapter(nil).Normalize(context.Background(), request)
	if !errors.Is(err, provider.ErrCapabilityUnavailable) {
		t.Fatalf("error=%v", err)
	}
}

func TestMissingUsageCategoriesRemainUnknown(t *testing.T) {
	usage := normalizeUsage(wireUsage{})
	if usage.Input.State != protocol.UsageUnknown || usage.Output.State != protocol.UsageUnknown || usage.Cached.State != protocol.UsageUnknown || usage.Reasoning.State != protocol.UsageUnknown {
		t.Fatalf("usage=%#v", usage)
	}
}

func TestInvalidProviderUsageBecomesTypedModelError(t *testing.T) {
	unknown := protocol.UsageValue{State: protocol.UsageUnknown}
	usage := protocol.ModelUsage{Input: protocol.UsageValue{State: protocol.UsageProviderReported, Value: -1}, Output: unknown, Cached: unknown, CacheWrite: unknown, Reasoning: unknown}
	events := normalizeLegacyEvents([]domain.ModelEvent{{Kind: domain.ModelUsageUpdate, Usage: &usage}})
	if len(events) != 1 || events[0].Kind != protocol.ModelEventError || events[0].Error.Code != "invalid_usage" {
		t.Fatalf("events=%#v", events)
	}
}

func TestAdapterRejectsUnsupportedSemanticBlocks(t *testing.T) {
	adapter := NewAdapter(nil)
	_, err := adapter.normalize(context.Background(), protocol.ModelRequest{ModelID: "model-a", Messages: []protocol.ModelMessage{{Role: "user", Blocks: []protocol.ContentBlock{{Kind: protocol.ContentReferenceKind, Reference: &protocol.ContentReference{URI: "file:///a", MediaType: "text/plain", Digest: adapterDigest('b')}}}}}})
	if !errors.Is(err, provider.ErrCapabilityUnavailable) {
		t.Fatalf("err=%v", err)
	}
}

func TestAdapterRejectsStructuredOutputBeforeTransport(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests++ }))
	defer server.Close()
	adapter := NewAdapter(New(ClientOptions{HTTPClient: server.Client(), BaseURL: server.URL}))
	request := protocol.ModelRequest{
		ProviderID: "openai", ModelID: "model-a",
		Messages:     []protocol.ModelMessage{{Role: "user", Blocks: []protocol.ContentBlock{{Kind: protocol.ContentText, Text: "return JSON"}}}},
		Requirements: []protocol.CapabilityRequirement{{Capability: protocol.CapabilityStructuredOutput, Level: protocol.CapabilityRequired}},
	}
	_, err := adapter.stream(context.Background(), request)
	if !errors.Is(err, provider.ErrCapabilityUnavailable) || requests != 0 {
		t.Fatalf("error=%v requests=%d", err, requests)
	}
}

func adapterDigest(fill byte) protocol.Digest {
	value := make([]byte, 64)
	for i := range value {
		value[i] = fill
	}
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: string(value)}
}
