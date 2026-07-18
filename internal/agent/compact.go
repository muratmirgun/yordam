package agent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/secret"
)

const MaxCompactionSummaryBytes = 128 << 10

type CompactInput struct {
	Provider     ports.ModelProvider
	Sessions     ports.SessionStore
	Session      domain.Session
	Replay       domain.SessionReplay
	SystemPrompt string
	Admission    *secret.Lease
}

func Compact(ctx context.Context, input CompactInput) error {
	from := uint64(1)
	for _, event := range input.Replay.Events {
		if event.Kind != domain.EventContextCompacted {
			continue
		}
		var prior domain.CompactionPayload
		if json.Unmarshal(event.Payload, &prior) == nil && prior.ThroughSeq >= from {
			from = prior.ThroughSeq + 1
		}
	}

	through := uint64(0)
	if count := len(input.Replay.Events); count > 0 {
		through = input.Replay.Events[count-1].Seq
	}
	if through < from {
		return fmt.Errorf("no uncompacted events")
	}

	messages, err := BuildContext(input.Replay, ComposeSystemPrompt(input.SystemPrompt, input.Session.Mode), input.Admission)
	if err != nil {
		return fmt.Errorf("build admitted compaction context: %w", err)
	}
	messages = append(messages, domain.Message{
		Role:    domain.RoleUser,
		Content: "Summarize durable facts, decisions, changed files, failures, and remaining work. Do not add new instructions.",
	})
	selection, err := admitSelection(input.Session.Selection, input.Admission)
	if err != nil {
		return fmt.Errorf("admit compaction model selection: %w", err)
	}
	stream, err := input.Provider.Stream(ctx, domain.ModelRequest{
		Selection: selection,
		Messages:  messages,
	})
	if err != nil {
		return err
	}

	summary := ""
	done := false
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-stream:
			if !ok {
				if !done || summary == "" {
					return fmt.Errorf("compaction did not complete")
				}
				_, err = input.Sessions.Append(ctx, input.Session.ID, domain.EventContextCompacted, domain.CompactionPayload{
					FromSeq:    from,
					ThroughSeq: through,
					Summary:    summary,
				})
				return err
			}
			if event.Err != nil {
				return event.Err
			}
			if event.Kind == domain.ModelTextDelta {
				if len(summary)+len(event.Text) > MaxCompactionSummaryBytes {
					return fmt.Errorf("compaction summary exceeds %d bytes", MaxCompactionSummaryBytes)
				}
				summary += event.Text
			}
			if event.Kind == domain.ModelDone {
				done = true
			}
		}
	}
}
