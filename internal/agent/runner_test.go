package agent_test

import (
	"context"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/orchestrator"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestOrchestratedRunnerConvertsLegacyRunInputAndDelegates(t *testing.T) {
	service := &capturingTurnOrchestrator{}
	runner := agent.OrchestratedRunner{
		Orchestrator: service,
		Prepare: func(context.Context, agent.RunInput) (orchestrator.StartTurnRequest, error) {
			return orchestrator.StartTurnRequest{
				Command:      orchestrator.CommandMetadata{CommandID: "command-a", IdempotencyKey: "key-a", RequestDigest: digest('a'), Actor: protocol.ActorRef{ID: "user-a", Kind: protocol.ActorUser}},
				ExpectedHead: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "session-a", CommitSeq: 1, TransactionID: "head-a"}, Runtime: protocol.RuntimeGenerationManifest{ID: "generation-a"},
			}, nil
		},
	}
	if err := runner.RunTurn(context.Background(), agent.RunInput{Session: domain.Session{ID: "session-a"}, Prompt: "inspect"}); err != nil {
		t.Fatal(err)
	}
	if service.request.SessionID != "session-a" || service.request.Prompt != "inspect" {
		t.Fatalf("request=%+v", service.request)
	}
}

func TestOrchestratedRunnerPinsRuntimeGenerationUntilTurnReturns(t *testing.T) {
	service := &capturingTurnOrchestrator{}
	acquired, released := 0, 0
	runner := agent.OrchestratedRunner{
		Orchestrator: service,
		Acquire: func() (func(), error) {
			acquired++
			return func() { released++ }, nil
		},
		Prepare: func(context.Context, agent.RunInput) (orchestrator.StartTurnRequest, error) {
			if acquired != 1 || released != 0 {
				t.Fatalf("generation lease acquired=%d released=%d before prepare", acquired, released)
			}
			return orchestrator.StartTurnRequest{}, nil
		},
	}
	if err := runner.RunTurn(context.Background(), agent.RunInput{Session: domain.Session{ID: "session-a"}, Prompt: "inspect"}); err != nil {
		t.Fatal(err)
	}
	if acquired != 1 || released != 1 {
		t.Fatalf("generation lease acquired=%d released=%d", acquired, released)
	}
}

type capturingTurnOrchestrator struct{ request orchestrator.StartTurnRequest }

func (s *capturingTurnOrchestrator) RunTurn(_ context.Context, request orchestrator.StartTurnRequest) (orchestrator.RunResult, error) {
	s.request = request
	return orchestrator.RunResult{Status: "completed"}, nil
}

func digest(value byte) protocol.Digest {
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat(string(value), 64)}
}
