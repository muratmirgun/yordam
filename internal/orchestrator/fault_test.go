package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/authorization"
	contextplanner "github.com/muratmirgun/yordam/internal/context"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/provider"
	"github.com/muratmirgun/yordam/internal/tooling"
	"github.com/muratmirgun/yordam/internal/verification"
)

func TestRunTurnFaultsDurablyTerminalizeAcceptedLifecycle(t *testing.T) {
	for _, test := range []struct {
		name  string
		alter func(*Dependencies)
	}{
		{name: "context planning", alter: func(deps *Dependencies) { deps.Context = failingContextPlanner{} }},
		{name: "provider stream start", alter: func(deps *Dependencies) { deps.Provider = failingProviderService{} }},
		{name: "effect dispatch barrier", alter: func(deps *Dependencies) { deps.BarrierProbe = failingBarrierProbe{barrier: BarrierEffectDispatch} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := validStartTurnRequest()
			request.Runtime = validRuntimeManifest(t, "observation")
			repository := &recordingRepository{head: request.ExpectedHead}
			deps := Dependencies{
				Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repository,
				TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: &recordLog{}}, Providers: fakeProviderCatalog{log: &recordLog{}},
				Provider: fakeProviderService{log: &recordLog{}}, Tools: noToolService{}, Authorization: &allowingAuthorization{log: &recordLog{}},
				Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
			}
			test.alter(&deps)
			service, err := NewService(deps)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.RunTurn(context.Background(), request); err == nil {
				t.Fatal("injected fault was ignored")
			}
			assertAcceptedTurnHasNoOrphans(t, repository.appendRequests())
		})
	}
}

func TestAuthorizationDenialDurablyDeniesActivityAndCommand(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	repository := &recordingRepository{head: request.ExpectedHead}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repository,
		TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: &recordLog{}}, Providers: fakeProviderCatalog{log: &recordLog{}},
		Provider: fakeProviderService{log: &recordLog{}}, Tools: noToolService{}, Authorization: &denyingAuthorization{allowingAuthorization: allowingAuthorization{log: &recordLog{}}},
		Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.RunTurn(context.Background(), request)
	if err == nil {
		t.Fatal("authorization denial was ignored")
	}
	if result.Status != "denied" || result.CommandResult.Status != "denied" {
		t.Fatalf("result=%+v", result)
	}
	kinds := flattenAppendKinds(repository.appendRequests())
	if !slices.Contains(kinds, protocol.EventAuthorizationDecided) || !slices.Contains(kinds, protocol.EventActivityDenied) {
		t.Fatalf("denial lifecycle=%v", kinds)
	}
	for _, forbidden := range []string{protocol.EventActivityAuthorized, protocol.EventActivityStarted, protocol.EventActivityFailed} {
		if slices.Contains(kinds, forbidden) {
			t.Fatalf("denial used forbidden event %s: %v", forbidden, kinds)
		}
	}
	assertAcceptedTurnHasNoOrphans(t, repository.appendRequests())
}

func TestCommittedContractPublishFailureTransitionsRunningTask(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	repository := &recordingRepository{head: request.ExpectedHead}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repository,
		TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: &recordLog{}}, Providers: fakeProviderCatalog{log: &recordLog{}},
		Provider: fakeProviderService{log: &recordLog{}}, Tools: noToolService{}, Authorization: &allowingAuthorization{log: &recordLog{}},
		Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now), Publisher: &nthFailPublisher{failAt: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err == nil {
		t.Fatal("publisher failure was ignored")
	}
	var terminal protocol.TaskStatusChangedV1
	for _, appendRequest := range repository.appendRequests() {
		for _, event := range appendRequest.Events {
			if event.Kind == protocol.EventTaskStatusChanged {
				if err := json.Unmarshal(event.Payload, &terminal); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if terminal.From != string(protocol.TaskRunning) || terminal.To != string(protocol.TaskFailed) {
		t.Fatalf("task terminal=%+v", terminal)
	}
	assertAcceptedTurnHasNoOrphans(t, repository.appendRequests())
}

func TestCommittedTurnTerminalPublishFailureDoesNotAppendSecondTerminal(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	repository := &recordingRepository{head: request.ExpectedHead}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repository,
		TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: &recordLog{}}, Providers: fakeProviderCatalog{log: &recordLog{}},
		Provider: fakeProviderService{log: &recordLog{}}, Tools: noToolService{}, Authorization: &allowingAuthorization{log: &recordLog{}},
		Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now), Publisher: &nthFailPublisher{failAt: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err == nil {
		t.Fatal("publisher failure was ignored")
	}
	kinds := flattenAppendKinds(repository.appendRequests())
	turnTerminals, commandTerminals := 0, 0
	for _, kind := range kinds {
		if kind == protocol.EventTurnCompleted || kind == protocol.EventTurnFailed || kind == protocol.EventTurnInterrupted {
			turnTerminals++
		}
		if kind == protocol.EventCommandCompleted {
			commandTerminals++
		}
	}
	if turnTerminals != 1 || commandTerminals != 1 || !slices.Contains(kinds, protocol.EventTurnCompleted) {
		t.Fatalf("terminal lifecycle=%v", kinds)
	}
	validateAppendRequests(t, repository.appendRequests())
}

func TestEveryTurnBarrierBeforeAndAfterLeavesNoDurableOrphan(t *testing.T) {
	barriers := []struct {
		barrier  Barrier
		mutation bool
	}{
		{BarrierCommandAccepted, false}, {BarrierGoalDraftCommitted, false}, {BarrierContractFrozen, false},
		{BarrierProviderAuthorizationCommitted, false}, {BarrierActionPlanCommitted, true}, {BarrierCheckpointReady, true},
		{BarrierResourcesRevalidated, true}, {BarrierAuthorizationCommitted, true}, {BarrierActivityStartedCommitted, false},
		{BarrierEffectDispatch, false}, {BarrierActionTerminalCommitted, true}, {BarrierProviderContinuation, true},
		{BarrierVerificationCommitted, false}, {BarrierTurnTerminalCommitted, false}, {BarrierCommandCompleted, false},
	}
	for _, test := range barriers {
		for _, phase := range []string{"before", "after"} {
			t.Run(string(test.barrier)+"_"+phase, func(t *testing.T) {
				request := validStartTurnRequest()
				effect := "observation"
				var providerService ProviderService = fakeProviderService{log: &recordLog{}}
				var toolService ToolService = noToolService{}
				var evidenceService EvidenceRecorder = noEvidenceRecorder{}
				var recoveryService RecoveryRecorder = noRecoveryRecorder{}
				if test.mutation {
					effect = "mutation"
					providerService = &toolThenFinalProvider{log: &recordLog{}}
					toolService = mutationToolService{log: &recordLog{}}
					evidenceService = recordingEvidence{log: &recordLog{}}
					recoveryService = recordingRecovery{log: &recordLog{}}
				}
				request.Runtime = validRuntimeManifest(t, effect)
				repository := &recordingRepository{head: request.ExpectedHead}
				service, err := NewService(Dependencies{
					Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repository,
					TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: &recordLog{}}, Providers: fakeProviderCatalog{log: &recordLog{}},
					Provider: providerService, Tools: toolService, Authorization: &allowingAuthorization{log: &recordLog{}}, Evidence: evidenceService,
					Recovery: recoveryService, Verification: verification.NewService(time.Now), BarrierProbe: phaseBarrierProbe{barrier: test.barrier, phase: phase},
				})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := service.RunTurn(context.Background(), request); !errors.Is(err, errInjectedBarrier) {
					t.Fatalf("run error=%v", err)
				}
				assertAcceptedTurnHasNoOrphans(t, repository.appendRequests())
			})
		}
	}
}

func assertAcceptedTurnHasNoOrphans(t *testing.T, requests []journal.AppendRequest) {
	t.Helper()
	planned := make(map[protocol.ActivityID]int)
	terminal := make(map[protocol.ActivityID]int)
	turnTerminals, commandTerminals := 0, 0
	for _, request := range requests {
		for _, event := range request.Events {
			switch event.Kind {
			case protocol.EventActivityPlanned:
				planned[event.ActivityID]++
			case protocol.EventActivitySucceeded, protocol.EventActivityFailed, protocol.EventActivityDenied,
				protocol.EventActivityCancelled, protocol.EventActivityInterruptedNoEffect, protocol.EventActivityUncertain:
				terminal[event.ActivityID]++
			case protocol.EventTurnCompleted, protocol.EventTurnFailed, protocol.EventTurnInterrupted:
				turnTerminals++
			case protocol.EventCommandCompleted:
				commandTerminals++
			}
		}
	}
	for activityID := range planned {
		if terminal[activityID] != 1 {
			t.Fatalf("activity %s terminal count=%d", activityID, terminal[activityID])
		}
	}
	if turnTerminals != 1 || commandTerminals != 1 {
		t.Fatalf("turn terminals=%d command terminals=%d", turnTerminals, commandTerminals)
	}
	validateAppendRequests(t, requests)
}

type failingContextPlanner struct{}

func (failingContextPlanner) Plan(context.Context, contextplanner.Request) (protocol.ContextPlan, error) {
	return protocol.ContextPlan{}, errors.New("context planning fault")
}

type failingProviderService struct{}

func (failingProviderService) Prepare(context.Context, protocol.ActivityID, string, protocol.ModelRequest, protocol.Digest) (provider.ProviderHandle, error) {
	return provider.ProviderHandle{}, nil
}

func (failingProviderService) Stream(context.Context, provider.ProviderHandle, authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	return nil, fmt.Errorf("provider stream fault")
}

type denyingAuthorization struct{ allowingAuthorization }

func (a *denyingAuthorization) Decide(ctx context.Context, request protocol.AuthorizationRequest) (protocol.AuthorizationDecision, error) {
	decision, err := a.allowingAuthorization.Decide(ctx, request)
	decision.Action = "deny"
	decision.Reason = "policy denied"
	return decision, err
}

type nthFailPublisher struct {
	calls  int
	failAt int
}

func (p *nthFailPublisher) PublishCommitted(context.Context, protocol.JournalRef, protocol.CommittedCursor, []protocol.EventEnvelope) error {
	p.calls++
	if p.calls == p.failAt {
		return errors.New("publisher unavailable")
	}
	return nil
}

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
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{},
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

func TestEvidenceMetadataMismatchTerminalizesBeforeContinuation(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "mutation")
	repository := &recordingRepository{head: request.ExpectedHead}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repository,
		TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: &recordLog{}}, Providers: fakeProviderCatalog{log: &recordLog{}},
		Provider: &toolThenFinalProvider{log: &recordLog{}}, Tools: mutationToolService{log: &recordLog{}}, Authorization: &allowingAuthorization{log: &recordLog{}},
		Evidence: corruptEvidenceRecorder{}, Recovery: recordingRecovery{log: &recordLog{}}, Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err == nil {
		t.Fatal("corrupt evidence metadata was accepted")
	}
	assertAcceptedTurnHasNoOrphans(t, repository.appendRequests())
}

type corruptEvidenceRecorder struct{}

func (corruptEvidenceRecorder) Put(_ context.Context, candidate protocol.EvidenceCandidate) (protocol.EvidenceRecord, error) {
	body := protocol.EvidenceRecordBody{
		ID: candidate.ID, Kind: candidate.Kind, WorkspaceID: candidate.WorkspaceID, SessionID: candidate.SessionID,
		Availability: protocol.ContentWithheldSecret, MediaType: candidate.MediaType, Size: int64(len(candidate.Content)),
		ProducingActivityID: candidate.ProducingActivityID, Actor: candidate.Actor, Subject: candidate.Subject,
		CreatedAt: time.Now().UTC(), Redacted: true,
	}
	return protocol.EvidenceRecord{Body: body, Digest: repeatedDigest("f")}, nil
}

func TestRecoveryMetadataMismatchFailsCheckpointAndTerminalizes(t *testing.T) {
	log := &recordLog{}
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "mutation")
	repository := &recordingRepository{head: request.ExpectedHead, log: log}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repository,
		TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: log}, Providers: fakeProviderCatalog{log: log},
		Provider: &toolThenFinalProvider{log: log}, Tools: mutationToolService{log: log}, Authorization: &allowingAuthorization{log: log},
		Evidence: recordingEvidence{log: log}, Recovery: corruptRecoveryRecorder{delegate: recordingRecovery{log: log}}, Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err == nil {
		t.Fatal("corrupt recovery metadata was accepted")
	}
	if !slices.Contains(flattenAppendKinds(repository.appendRequests()), protocol.EventCheckpointFailed) {
		t.Fatal("checkpoint.failed was not committed")
	}
	assertAcceptedTurnHasNoOrphans(t, repository.appendRequests())
}

type corruptRecoveryRecorder struct{ delegate recordingRecovery }

func (r corruptRecoveryRecorder) PrepareAndPut(ctx context.Context, preview tooling.PreviewResult, activityID protocol.ActivityID, checkpoint protocol.CheckpointBody, plan protocol.ActionPlan) (protocol.RecoveryMaterialRecord, error) {
	record, err := r.delegate.PrepareAndPut(ctx, preview, activityID, checkpoint, plan)
	record.WorkspaceID = "wrong-workspace"
	return record, err
}

type failingRecoveryRecorder struct{}

func (failingRecoveryRecorder) PrepareAndPut(context.Context, tooling.PreviewResult, protocol.ActivityID, protocol.CheckpointBody, protocol.ActionPlan) (protocol.RecoveryMaterialRecord, error) {
	return protocol.RecoveryMaterialRecord{}, errors.New("store unavailable")
}

func TestCheckpointReadyFaultPreventsRevalidationAuthorizationAndDispatch(t *testing.T) {
	log := &recordLog{}
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "mutation")
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{},
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
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{},
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

type phaseBarrierProbe struct {
	barrier Barrier
	phase   string
}

func (p phaseBarrierProbe) Before(_ context.Context, barrier Barrier, _ BarrierState) error {
	if p.phase == "before" && barrier == p.barrier {
		return errInjectedBarrier
	}
	return nil
}

func (p phaseBarrierProbe) After(_ context.Context, barrier Barrier, _ BarrierState) error {
	if p.phase == "after" && barrier == p.barrier {
		return errInjectedBarrier
	}
	return nil
}
