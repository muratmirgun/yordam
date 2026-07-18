package agent

import (
	"context"
	"fmt"
	"strings"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/orchestrator"
	"github.com/muratmirgun/yordam/internal/protocol"
)

// RunInput is the deliberately small compatibility input retained for the
// v0.1 TUI. Durable command identity and the committed session head are
// supplied by the versioned application boundary.
type RunInput struct {
	Session      domain.Session
	Replay       domain.SessionReplay
	Prompt       string
	Command      orchestrator.CommandMetadata
	ExpectedHead protocol.CommittedCursor
}

type TurnOrchestrator interface {
	RunTurn(context.Context, orchestrator.StartTurnRequest) (orchestrator.RunResult, error)
}

// OrchestratedRunner contains no provider, tool, policy, or journal effects.
// It only pins a runtime generation and delegates a complete request.
type OrchestratedRunner struct {
	Orchestrator TurnOrchestrator
	Prepare      func(context.Context, RunInput) (orchestrator.StartTurnRequest, error)
	Acquire      func() (func(), error)
	Sink         Sink
}

func (r OrchestratedRunner) RunTurn(ctx context.Context, input RunInput) error {
	if r.Orchestrator == nil || r.Prepare == nil {
		return fmt.Errorf("orchestrated runner is not configured")
	}
	if input.Session.ID == "" || strings.TrimSpace(input.Prompt) == "" {
		return fmt.Errorf("legacy run input is incomplete")
	}
	if r.Acquire != nil {
		release, err := r.Acquire()
		if err != nil {
			return fmt.Errorf("acquire runtime generation: %w", err)
		}
		defer release()
	}
	request, err := r.Prepare(ctx, input)
	if err != nil {
		return err
	}
	request.SessionID = protocol.SessionID(input.Session.ID)
	request.Prompt = input.Prompt
	result, err := r.Orchestrator.RunTurn(ctx, request)
	if err != nil {
		return err
	}
	if r.Sink != nil {
		for _, block := range result.Assistant.Blocks {
			if block.Kind == protocol.ContentText && block.Text != "" {
				r.Sink(RuntimeEvent{Kind: RuntimeTextDelta, Text: block.Text})
			}
		}
	}
	return nil
}
