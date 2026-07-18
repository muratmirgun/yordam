package openaicompat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/provider"
)

const AdapterKind = "openai_compatible"

type Adapter struct {
	client *Client
}

type normalizedRequest struct {
	ProviderID protocol.ProviderID
	Body       chatRequest
}

func NewAdapter(client *Client) *Adapter { return &Adapter{client: client} }

func (*Adapter) Kind() string { return AdapterKind }

func (a *Adapter) Normalize(ctx context.Context, request protocol.ModelRequest) (provider.PreparedRequest, error) {
	normalized, err := a.normalize(ctx, request)
	if err != nil {
		return provider.PreparedRequest{}, err
	}
	return provider.NewPreparedRequest(a.Kind(), normalizedRequest{ProviderID: request.ProviderID, Body: normalized})
}

func (*Adapter) normalize(ctx context.Context, request protocol.ModelRequest) (chatRequest, error) {
	if err := ctx.Err(); err != nil {
		return chatRequest{}, err
	}
	for _, requirement := range request.Requirements {
		if requirement.Capability == protocol.CapabilityStructuredOutput && requirement.Level != protocol.CapabilityUnused {
			return chatRequest{}, capabilityError(protocol.ContentJSON)
		}
	}
	if request.ModelID == "" {
		return chatRequest{}, fmt.Errorf("model ID is required")
	}
	normalized := chatRequest{Model: string(request.ModelID), Stream: true}
	for _, message := range request.Messages {
		wire, err := normalizeMessage(message)
		if err != nil {
			return chatRequest{}, err
		}
		normalized.Messages = append(normalized.Messages, wire)
	}
	if len(request.Tools.Tools) != 0 || len(request.Tools.Aliases) != 0 || request.Tools.CatalogRevision != "" {
		if err := request.Tools.Validate(); err != nil {
			return chatRequest{}, err
		}
		for _, tool := range request.Tools.Tools {
			normalized.Tools = append(normalized.Tools, wireTool{Type: "function", Function: wireFunction{Name: tool.Alias, Description: tool.Description, Parameters: protocol.CloneRawMessage(tool.InputSchema)}})
		}
	}
	return normalized, nil
}

func normalizeMessage(message protocol.ModelMessage) (wireMessage, error) {
	switch message.Role {
	case "system", "user", "assistant", "tool":
	default:
		return wireMessage{}, fmt.Errorf("unsupported model message role %q", message.Role)
	}
	if len(message.Blocks) == 0 {
		return wireMessage{}, fmt.Errorf("model message requires content")
	}
	wire := wireMessage{Role: message.Role}
	toolResults := 0
	for _, block := range message.Blocks {
		if err := block.Validate(); err != nil {
			return wireMessage{}, err
		}
		switch block.Kind {
		case protocol.ContentText:
			if message.Role == "tool" {
				return wireMessage{}, capabilityError(block.Kind)
			}
			wire.Content += block.Text
		case protocol.ContentToolUse:
			if message.Role != "assistant" {
				return wireMessage{}, capabilityError(block.Kind)
			}
			wire.ToolCalls = append(wire.ToolCalls, wireToolCall{ID: block.ToolUse.CallID, Type: "function", Function: wireFunction{Name: block.ToolUse.Alias, Arguments: string(block.ToolUse.Arguments)}})
		case protocol.ContentToolResult:
			if message.Role != "tool" || toolResults != 0 {
				return wireMessage{}, capabilityError(block.Kind)
			}
			toolResults++
			result := block.ToolResult
			if (result.Text == "") == (result.JSON == nil) {
				return wireMessage{}, fmt.Errorf("tool result requires exactly one text or JSON value")
			}
			wire.ToolCallID = result.CallID
			if result.Text != "" {
				wire.Content = result.Text
			} else {
				wire.Content = string(result.JSON)
			}
		default:
			return wireMessage{}, capabilityError(block.Kind)
		}
	}
	if message.Role == "tool" && toolResults != 1 {
		return wireMessage{}, fmt.Errorf("tool message requires one tool result")
	}
	return wire, nil
}

func capabilityError(kind string) error {
	return fmt.Errorf("%w: OpenAI-compatible adapter cannot preserve %s block semantics", provider.ErrCapabilityUnavailable, kind)
}

func (a *Adapter) stream(ctx context.Context, request protocol.ModelRequest) (<-chan protocol.ModelEvent, error) {
	if a == nil || a.client == nil {
		return nil, fmt.Errorf("OpenAI-compatible client is required")
	}
	normalized, err := a.normalize(ctx, request)
	if err != nil {
		return nil, err
	}
	legacy, err := a.client.streamOnce(ctx, legacyRequest(normalized))
	if err != nil {
		return nil, err
	}
	out := make(chan protocol.ModelEvent)
	go func() {
		defer close(out)
		normalizer := eventNormalizer{}
		for event := range legacy {
			for _, normalizedEvent := range normalizer.feed(event) {
				select {
				case <-ctx.Done():
					return
				case out <- normalizedEvent:
				}
			}
		}
	}()
	return out, nil
}

func legacyRequest(request chatRequest) domain.ModelRequest {
	legacy := domain.ModelRequest{Selection: domain.ModelSelection{Model: request.Model}}
	for _, message := range request.Messages {
		converted := domain.Message{Role: domain.Role(message.Role), Content: message.Content, ToolCallID: message.ToolCallID}
		for _, call := range message.ToolCalls {
			converted.ToolCalls = append(converted.ToolCalls, domain.ToolCall{ID: call.ID, Name: call.Function.Name, Arguments: json.RawMessage(call.Function.Arguments)})
		}
		legacy.Messages = append(legacy.Messages, converted)
	}
	for _, tool := range request.Tools {
		legacy.Tools = append(legacy.Tools, domain.ToolDescriptor{Name: tool.Function.Name, Description: tool.Function.Description, InputSchema: protocol.CloneRawMessage(tool.Function.Parameters), Mutation: domain.MutationReadOnly})
	}
	return legacy
}

type eventNormalizer struct {
	sequence uint64
	text     strings.Builder
	refusal  strings.Builder
}

func (n *eventNormalizer) next(event protocol.ModelEvent) protocol.ModelEvent {
	n.sequence++
	event.Sequence = n.sequence
	return event
}

func (n *eventNormalizer) feed(event domain.ModelEvent) []protocol.ModelEvent {
	switch event.Kind {
	case domain.ModelTextDelta:
		if event.Text == "" {
			return []protocol.ModelEvent{n.providerError("invalid_content_delta", "provider returned an empty content delta", false)}
		}
		n.text.WriteString(event.Text)
		delta := protocol.ContentDelta{BlockID: "content-1", Kind: protocol.ContentText, Text: event.Text}
		return []protocol.ModelEvent{n.next(protocol.ModelEvent{Kind: protocol.ModelEventContentDelta, Delta: &delta})}
	case domain.ModelRefusalDelta:
		n.refusal.WriteString(event.Refusal)
		return nil
	case domain.ModelToolCall:
		if event.ToolCall == nil {
			return []protocol.ModelEvent{n.providerError("invalid_tool_intent", "provider returned an empty tool intent", false)}
		}
		intent := protocol.ToolUseBlock{CallID: event.ToolCall.ID, Alias: event.ToolCall.Name, Arguments: protocol.CloneRawMessage(event.ToolCall.Arguments)}
		if err := intent.Validate(); err != nil {
			return []protocol.ModelEvent{n.providerError("invalid_tool_intent", err.Error(), false)}
		}
		return []protocol.ModelEvent{n.next(protocol.ModelEvent{Kind: protocol.ModelEventToolIntent, ToolIntent: &intent})}
	case domain.ModelUsageUpdate:
		if event.Usage == nil {
			return []protocol.ModelEvent{n.providerError("invalid_usage", "provider returned empty usage", false)}
		}
		usage := protocol.DeepCopy(*event.Usage)
		if err := usage.Validate(); err != nil {
			return []protocol.ModelEvent{n.providerError("invalid_usage", err.Error(), false)}
		}
		return []protocol.ModelEvent{n.next(protocol.ModelEvent{Kind: protocol.ModelEventUsageUpdate, Usage: &usage})}
	case domain.ModelStreamError:
		code, retryable := "provider_error", false
		var typed *domain.TypedError
		if errors.As(event.Err, &typed) {
			code = string(typed.Kind)
			retryable = typed.Kind == domain.ErrorProviderRetryable
		}
		message := "provider stream failed"
		if event.Err != nil {
			message = event.Err.Error()
		}
		return []protocol.ModelEvent{n.providerError(code, message, retryable)}
	case domain.ModelDone:
		return n.complete(event)
	default:
		return []protocol.ModelEvent{n.providerError("unsupported_provider_event", fmt.Sprintf("unsupported provider event %q", event.Kind), false)}
	}
}

func (n *eventNormalizer) providerError(code, message string, retryable bool) protocol.ModelEvent {
	return n.next(protocol.ModelEvent{Kind: protocol.ModelEventError, Error: &protocol.ProviderError{Code: code, Message: message, Retryable: retryable}})
}

func (n *eventNormalizer) complete(event domain.ModelEvent) []protocol.ModelEvent {
	result := make([]protocol.ModelEvent, 0, 3)
	if n.text.Len() != 0 {
		block := protocol.ContentBlock{Kind: protocol.ContentText, Text: n.text.String()}
		result = append(result, n.next(protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Block: &block}))
	}
	if n.refusal.Len() != 0 {
		block := protocol.ContentBlock{Kind: protocol.ContentRefusal, Refusal: &protocol.RefusalBlock{Code: "provider_refusal", Message: n.refusal.String()}}
		result = append(result, n.next(protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Block: &block}))
	}
	reason := normalizeFinishReason(event.FinishReason, n.refusal.Len() != 0)
	terminal := protocol.ModelTerminal{Reason: reason, NativeReason: event.FinishReason, ServerRequestID: event.RequestID}
	result = append(result, n.next(protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Terminal: &terminal}))
	return result
}

func normalizeFinishReason(native string, refused bool) string {
	if refused {
		return "refusal"
	}
	switch native {
	case "tool_calls", "function_call":
		return "tool_use"
	case "length":
		return "max_output"
	case "content_filter":
		return "refusal"
	case "stop":
		return "stop"
	case "":
		return "completed"
	default:
		return "provider_terminal"
	}
}

func normalizeLegacyEvents(events []domain.ModelEvent) []protocol.ModelEvent {
	normalizer := eventNormalizer{}
	result := make([]protocol.ModelEvent, 0, len(events)+1)
	for _, event := range events {
		result = append(result, normalizer.feed(event)...)
	}
	return result
}
