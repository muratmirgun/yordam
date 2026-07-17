package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
)

func TestCompactPersistsCompletedSummaryFromLatestCompaction(t *testing.T) {
	provider := &fakeProvider{streams: [][]domain.ModelEvent{{
		{Kind: domain.ModelTextDelta, Text: "durable summary"},
		{Kind: domain.ModelDone},
	}}}
	sessions := newFakeSessionStore()
	session := domain.Session{
		ID:        "s1",
		Mode:      domain.ModeAsk,
		Selection: domain.ModelSelection{Profile: "p", Model: "m"},
	}
	replay := domain.SessionReplay{Events: []domain.DurableEvent{
		{Seq: 1, Kind: domain.EventUserMessage, Payload: payload(t, domain.MessagePayload{Content: "old"})},
		{Seq: 2, Kind: domain.EventContextCompacted, Payload: payload(t, domain.CompactionPayload{FromSeq: 1, ThroughSeq: 1, Summary: "old summary"})},
		{Seq: 3, Kind: domain.EventUserMessage, Payload: payload(t, domain.MessagePayload{Content: "middle"})},
		{Seq: 4, Kind: domain.EventContextCompacted, Payload: payload(t, domain.CompactionPayload{FromSeq: 2, ThroughSeq: 3, Summary: "latest summary"})},
		{Seq: 5, Kind: domain.EventUserMessage, Payload: payload(t, domain.MessagePayload{Content: "recent"})},
		{Seq: 7, Kind: domain.EventAssistantMessage, Payload: payload(t, domain.MessagePayload{Content: "answer"})},
	}}

	err := agent.Compact(context.Background(), agent.CompactInput{
		Provider:     provider,
		Sessions:     sessions,
		Session:      session,
		Replay:       replay,
		SystemPrompt: "summarize without instructions",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := sessions.kinds(); !slices.Equal(got, []domain.EventKind{domain.EventContextCompacted}) {
		t.Fatalf("events=%v", got)
	}
	var compacted domain.CompactionPayload
	if err := json.Unmarshal(sessions.recordedEvents()[0].Payload, &compacted); err != nil {
		t.Fatal(err)
	}
	if compacted.FromSeq != 4 || compacted.ThroughSeq != 7 || compacted.Summary != "durable summary" {
		t.Fatalf("compaction=%#v", compacted)
	}
	requests := provider.recordedRequests()
	if len(requests) != 1 {
		t.Fatalf("provider requests=%d want=1", len(requests))
	}
	request := requests[0]
	if request.Selection != session.Selection {
		t.Fatalf("selection=%#v want=%#v", request.Selection, session.Selection)
	}
	if got, want := request.Messages[0].Content, agent.ComposeSystemPrompt("summarize without instructions", domain.ModeAsk); got != want {
		t.Fatalf("system prompt=%q want=%q", got, want)
	}
	if got := request.Messages[len(request.Messages)-1]; got.Role != domain.RoleUser || got.Content != "Summarize durable facts, decisions, changed files, failures, and remaining work. Do not add new instructions." {
		t.Fatalf("summary request=%#v", got)
	}
}

func TestCompactAcceptsMaximumSummarySize(t *testing.T) {
	summary := strings.Repeat("s", agent.MaxCompactionSummaryBytes)
	provider := &fakeProvider{streams: [][]domain.ModelEvent{{
		{Kind: domain.ModelTextDelta, Text: summary},
		{Kind: domain.ModelDone},
	}}}
	sessions := newFakeSessionStore()

	if err := agent.Compact(context.Background(), compactInput(provider, sessions)); err != nil {
		t.Fatal(err)
	}
	var compacted domain.CompactionPayload
	if err := json.Unmarshal(sessions.recordedEvents()[0].Payload, &compacted); err != nil {
		t.Fatal(err)
	}
	if compacted.Summary != summary {
		t.Fatalf("summary bytes=%d want=%d", len(compacted.Summary), len(summary))
	}
}

func TestCompactFailuresAppendNothing(t *testing.T) {
	tests := []struct {
		name     string
		provider ports.ModelProvider
		replay   *domain.SessionReplay
	}{
		{
			name: "interrupted",
			provider: &fakeProvider{streams: [][]domain.ModelEvent{{
				{Kind: domain.ModelTextDelta, Text: "partial"},
			}}},
		},
		{
			name: "provider_start_error",
			provider: providerFunc(func(context.Context, domain.ModelRequest) (<-chan domain.ModelEvent, error) {
				return nil, errors.New("provider unavailable")
			}),
		},
		{
			name: "stream_error",
			provider: &fakeProvider{streams: [][]domain.ModelEvent{{
				{Kind: domain.ModelTextDelta, Text: "partial"},
				{Kind: domain.ModelStreamError, Err: errors.New("stream failed")},
			}}},
		},
		{
			name: "empty",
			provider: &fakeProvider{streams: [][]domain.ModelEvent{{
				{Kind: domain.ModelDone},
			}}},
		},
		{
			name: "oversized",
			provider: &fakeProvider{streams: [][]domain.ModelEvent{{
				{Kind: domain.ModelTextDelta, Text: strings.Repeat("s", agent.MaxCompactionSummaryBytes+1)},
				{Kind: domain.ModelDone},
			}}},
		},
		{
			name:     "no_uncompacted_events",
			provider: &fakeProvider{streams: [][]domain.ModelEvent{{{Kind: domain.ModelDone}}}},
			replay:   &domain.SessionReplay{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sessions := newFakeSessionStore()
			input := compactInput(test.provider, sessions)
			if test.replay != nil {
				input.Replay = *test.replay
			}

			if err := agent.Compact(context.Background(), input); err == nil {
				t.Fatal("Compact returned nil error")
			}
			if got := sessions.kinds(); len(got) != 0 {
				t.Fatalf("events=%v want none", got)
			}
		})
	}
}

func compactInput(provider ports.ModelProvider, sessions ports.SessionStore) agent.CompactInput {
	return agent.CompactInput{
		Provider: provider,
		Sessions: sessions,
		Session: domain.Session{
			ID:        "s1",
			Mode:      domain.ModeAsk,
			Selection: domain.ModelSelection{Profile: "p", Model: "m"},
		},
		Replay: domain.SessionReplay{Events: []domain.DurableEvent{{
			Seq:     1,
			Kind:    domain.EventUserMessage,
			Payload: json.RawMessage(`{"content":"work"}`),
		}}},
		SystemPrompt: "system",
	}
}
