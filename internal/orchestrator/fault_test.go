package orchestrator

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/recovery"
	"github.com/muratmirgun/yordam/internal/verification"
)

func TestNoopBarrierProbeAllowsEveryDurabilityBoundary(t *testing.T) {
	probe := NoopBarrierProbe()
	barriers := []Barrier{
		BarrierCommandAccepted, BarrierGoalDraftCommitted, BarrierContractFrozen,
		BarrierProviderAuthorizationCommitted, BarrierActionPlanCommitted, BarrierCheckpointReady,
		BarrierResourcesRevalidated, BarrierAuthorizationCommitted, BarrierActivityStartedCommitted,
		BarrierEffectDispatch, BarrierActionTerminalCommitted, BarrierProviderContinuation,
		BarrierVerificationCommitted, BarrierTurnTerminalCommitted, BarrierRecoveryTurnTerminalCommitted,
		BarrierCommandCompleted,
	}
	for _, barrier := range barriers {
		if err := probe.Before(context.Background(), barrier, BarrierState{}); err != nil {
			t.Fatalf("before %s: %v", barrier, err)
		}
		if err := probe.After(context.Background(), barrier, BarrierState{}); err != nil {
			t.Fatalf("after %s: %v", barrier, err)
		}
	}
}

func TestRecoveryMaterialFailureCommitsCheckpointFailedAndPreventsDispatch(t *testing.T) {
	log := &recordLog{}
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "mutation")
	service, err := NewService(Dependencies{
		Lane: NewOperationLane(), Repository: &recordingRepository{head: request.ExpectedHead, log: log}, TurnLeases: &recordingTurnLeaseManager{},
		Context: fakeContextPlanner{log: log}, Providers: fakeProviderCatalog{log: log}, Provider: &toolThenFinalProvider{log: log},
		Tools: mutationToolService{log: log}, Authorization: &allowingAuthorization{log: log}, Evidence: recordingEvidence{log: log},
		Recovery: failingRecoveryRecorder{}, Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err == nil {
		t.Fatal("recovery material failure was ignored")
	}
	got := log.snapshot()
	if !slices.Contains(got, "append(checkpoint.failed)") {
		t.Fatalf("checkpoint.failed was not durable: %v", got)
	}
	for _, forbidden := range []string{"tool.revalidate", "tool.execute"} {
		if slices.Contains(got, forbidden) {
			t.Fatalf("%s occurred after recovery failure: %v", forbidden, got)
		}
	}
}

type failingRecoveryRecorder struct{}

func (failingRecoveryRecorder) Put(context.Context, recovery.Candidate) (protocol.RecoveryMaterialRecord, error) {
	return protocol.RecoveryMaterialRecord{}, errors.New("store unavailable")
}

func TestCheckpointReadyFaultPreventsRevalidationAuthorizationAndDispatch(t *testing.T) {
	log := &recordLog{}
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "mutation")
	service, err := NewService(Dependencies{
		Lane: NewOperationLane(), Repository: &recordingRepository{head: request.ExpectedHead, log: log},
		TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: log}, Providers: fakeProviderCatalog{log: log},
		Provider: &toolThenFinalProvider{log: log}, Tools: mutationToolService{log: log}, Authorization: &allowingAuthorization{log: log},
		Evidence: recordingEvidence{log: log}, Recovery: recordingRecovery{log: log}, Verification: verification.NewService(time.Now),
		BarrierProbe: failingBarrierProbe{barrier: BarrierCheckpointReady},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); !errors.Is(err, errInjectedBarrier) {
		t.Fatalf("run error=%v", err)
	}
	got := log.snapshot()
	for _, forbidden := range []string{"tool.revalidate", "tool.execute"} {
		if slices.Contains(got, forbidden) {
			t.Fatalf("%s occurred after failed checkpoint-ready barrier: %v", forbidden, got)
		}
	}
}

func TestStartedCommitFaultPreventsTokenIssuanceAndProviderDispatch(t *testing.T) {
	log := &recordLog{}
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	service, err := NewService(Dependencies{
		Lane: NewOperationLane(), Repository: &recordingRepository{head: request.ExpectedHead, log: log}, TurnLeases: &recordingTurnLeaseManager{},
		Context: fakeContextPlanner{log: log}, Providers: fakeProviderCatalog{log: log}, Provider: fakeProviderService{log: log},
		Tools: noToolService{}, Authorization: &allowingAuthorization{log: log}, Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{},
		Verification: verification.NewService(time.Now), BarrierProbe: failingBarrierProbe{barrier: BarrierActivityStartedCommitted},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); !errors.Is(err, errInjectedBarrier) {
		t.Fatalf("run error=%v", err)
	}
	got := log.snapshot()
	for _, forbidden := range []string{"authorization.issue", "provider.stream"} {
		if slices.Contains(got, forbidden) {
			t.Fatalf("%s occurred after failed started-commit barrier: %v", forbidden, got)
		}
	}
}

var errInjectedBarrier = errors.New("injected barrier failure")

type failingBarrierProbe struct{ barrier Barrier }

func (p failingBarrierProbe) Before(_ context.Context, barrier Barrier, _ BarrierState) error {
	if barrier == p.barrier {
		return errInjectedBarrier
	}
	return nil
}
func (failingBarrierProbe) After(context.Context, Barrier, BarrierState) error { return nil }
