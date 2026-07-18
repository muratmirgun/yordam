package agentfixture

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type Provider struct {
	mu       sync.Mutex
	Streams  [][]domain.ModelEvent
	Requests []domain.ModelRequest
	Block    func(context.Context, int) error
}

func (p *Provider) Stream(ctx context.Context, request domain.ModelRequest) (<-chan domain.ModelEvent, error) {
	p.mu.Lock()
	index := len(p.Requests)
	p.Requests = append(p.Requests, request)
	if len(p.Streams) == 0 {
		p.mu.Unlock()
		return nil, fmt.Errorf("provider script exhausted")
	}
	events := append([]domain.ModelEvent(nil), p.Streams[0]...)
	p.Streams = p.Streams[1:]
	block := p.Block
	p.mu.Unlock()
	if block != nil {
		if err := block(ctx, index); err != nil {
			return nil, err
		}
	}
	out := make(chan domain.ModelEvent, len(events))
	for _, event := range events {
		out <- event
	}
	close(out)
	return out, nil
}

func (p *Provider) Snapshot() ([]domain.ModelRequest, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]domain.ModelRequest(nil), p.Requests...), len(p.Streams)
}

type Approver struct {
	mu          sync.Mutex
	ResolveFunc func(ports.PermissionPrompt) domain.PermissionDecision
	Prompts     []ports.PermissionPrompt
}

func (a *Approver) Resolve(_ context.Context, prompt ports.PermissionPrompt) (domain.PermissionDecision, error) {
	a.mu.Lock()
	a.Prompts = append(a.Prompts, prompt)
	resolve := a.ResolveFunc
	a.mu.Unlock()
	if resolve == nil {
		return domain.PermissionDecision{}, fmt.Errorf("unexpected approval for %s", prompt.Call.Request.Name)
	}
	return resolve(prompt), nil
}

func (a *Approver) Snapshot() []ports.PermissionPrompt {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]ports.PermissionPrompt(nil), a.Prompts...)
}

func ToolStream(id, name, input string) []domain.ModelEvent {
	return []domain.ModelEvent{
		{Kind: domain.ModelToolCall, ToolCall: &domain.ToolCall{ID: id, Name: name, Arguments: json.RawMessage(input)}},
		{Kind: domain.ModelDone},
	}
}

func FinalStream(text string) []domain.ModelEvent {
	return []domain.ModelEvent{{Kind: domain.ModelTextDelta, Text: text}, {Kind: domain.ModelDone}}
}

func NeutralToolStream(id, alias, input string) []protocol.ModelEvent {
	return []protocol.ModelEvent{
		{Kind: protocol.ModelEventToolIntent, Sequence: 1, ToolIntent: &protocol.ToolUseBlock{CallID: id, Alias: alias, Arguments: json.RawMessage(input)}},
		{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "tool_use", NativeReason: "tool_calls"}},
	}
}

func NeutralFinalStream(text, requestID string) []protocol.ModelEvent {
	return []protocol.ModelEvent{
		{Kind: protocol.ModelEventContentDelta, Sequence: 1, Delta: &protocol.ContentDelta{BlockID: "content-1", Kind: protocol.ContentText, Text: text}},
		{Kind: protocol.ModelEventContentBlock, Sequence: 2, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: text}},
		{Kind: protocol.ModelEventTerminal, Sequence: 3, Terminal: &protocol.ModelTerminal{Reason: "stop", NativeReason: "stop", ServerRequestID: requestID}},
	}
}
