package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	contextplanner "github.com/muratmirgun/yordam/internal/context"
	"github.com/muratmirgun/yordam/internal/eventcodec"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/provider"
	"github.com/muratmirgun/yordam/internal/tooling"
	"github.com/muratmirgun/yordam/internal/verification"
)

func TestRunTurnProviderLifecycleUsesDurableAuthorizationAndTerminalBarriers(t *testing.T) {
	log := &recordLog{}
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	repository := &recordingRepository{head: request.ExpectedHead, log: log}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{},
		Lane: &loggingLane{delegate: NewOperationLane(), log: log}, Repository: repository,
		TurnLeases: &recordingTurnLeaseManager{log: log}, Context: fakeContextPlanner{log: log},
		Providers: fakeProviderCatalog{log: log}, Provider: fakeProviderService{log: log},
		Tools: noToolService{}, Authorization: &allowingAuthorization{log: log},
		Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{},
		Verification: loggingVerification{log: log, delegate: verification.NewService(func() time.Time {
			return time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
		})},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := service.RunTurn(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.CommandResult.Status != "completed" {
		t.Fatalf("result=%+v", result)
	}
	got := log.snapshot()
	wantPrefix := []string{
		"lane.acquire(turn)", "turn_lease.acquire",
		"append(command.accepted,task.created,user.message,outcome.contract_declared,turn.accepted)",
		"append(outcome.contract_amended,task.status_changed)",
		"context.plan", "provider.negotiate", "provider.prepare",
		"append(context.plan_recorded,provider.capability_decided,activity.planned,authorization.requested)",
		"authorization.decide", "append(authorization.decided,activity.authorized)",
		"append(authorization.decision_consumed,activity.started)", "authorization.issue", "provider.stream",
		"append(assistant.message,provider.attempt_terminal,activity.succeeded)",
		"verification.assess",
		"append(verification.receipt_recorded,outcome.criterion_assessed,outcome.final_assessed)",
		"append(turn.completed,task.status_changed,command.completed)",
		"turn_lease.release", "lane.release",
	}
	if !slices.Equal(got, wantPrefix) {
		t.Fatalf("order:\n got=%v\nwant=%v", got, wantPrefix)
	}
}

func TestDurableOrderingMutationPreviewCheckpointRevalidationExecutionAndContinuation(t *testing.T) {
	log := &recordLog{}
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "mutation")
	repository := &recordingRepository{head: request.ExpectedHead, log: log}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{},
		Lane: &loggingLane{delegate: NewOperationLane(), log: log}, Repository: repository,
		TurnLeases: &recordingTurnLeaseManager{log: log}, Context: fakeContextPlanner{log: log},
		Providers: fakeProviderCatalog{log: log}, Provider: &toolThenFinalProvider{log: log},
		Tools: mutationToolService{log: log}, Authorization: &allowingAuthorization{log: log},
		Evidence: recordingEvidence{log: log}, Recovery: recordingRecovery{log: log},
		Verification: loggingVerification{log: log, delegate: verification.NewService(time.Now)},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := service.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	got := log.snapshot()
	wantSequence := []string{
		"tool.plan_preview",
		"append(activity.planned,execution.plan_declared,authorization.requested)",
		"authorization.decide", "append(authorization.decided,activity.authorized)",
		"append(authorization.decision_consumed,activity.started)", "authorization.issue",
		"tool.prepare_preview", "evidence.put",
		"append(activity.succeeded,evidence.recorded,evidence.linked)",
		"tool.plan_mutation", "append(activity.planned,execution.plan_declared)",
		"append(checkpoint.planned)", "tool.recovery_candidate", "recovery.put", "append(checkpoint.ready)",
		"tool.revalidate", "append(authorization.requested)", "authorization.decide",
		"append(authorization.decided,activity.authorized)", "append(authorization.decision_consumed,activity.started)",
		"authorization.issue", "tool.execute", "evidence.put",
		"append(activity.succeeded,evidence.recorded,evidence.linked)",
		"context.plan", "provider.negotiate", "provider.prepare",
	}
	if !containsContiguous(got, wantSequence) {
		t.Fatalf("mutation order missing:\n got=%v\nwant contiguous=%v", got, wantSequence)
	}
}

func TestSequentialToolIntentsExecuteFIFOInProviderOrder(t *testing.T) {
	log := &recordLog{}
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{},
		Lane: NewOperationLane(), Repository: &recordingRepository{head: request.ExpectedHead}, TurnLeases: &recordingTurnLeaseManager{},
		Context: fakeContextPlanner{log: log}, Providers: fakeProviderCatalog{log: log}, Provider: &twoToolThenFinalProvider{log: log},
		Tools: observationToolService{log: log}, Authorization: &allowingAuthorization{log: log}, Evidence: recordingEvidence{log: log},
		Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	executions := make([]string, 0, 2)
	for _, value := range log.snapshot() {
		if strings.HasPrefix(value, "tool.execute(") {
			executions = append(executions, value)
		}
	}
	if !slices.Equal(executions, []string{"tool.execute(call-b)", "tool.execute(call-a)"}) {
		t.Fatalf("execution order=%v", executions)
	}
}

func TestRunTurnEventsValidateFoundationRegistry(t *testing.T) {
	log := &recordLog{}
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "mutation")
	repository := &recordingRepository{head: request.ExpectedHead}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{},
		Lane: NewOperationLane(), Repository: repository, TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: log},
		Providers: fakeProviderCatalog{log: log}, Provider: &toolThenFinalProvider{log: log}, Tools: mutationToolService{log: log},
		Authorization: &allowingAuthorization{log: log}, Evidence: recordingEvidence{log: log}, Recovery: recordingRecovery{log: log},
		Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	validateAppendRequests(t, repository.appendRequests())
}

func TestRunTurnConcurrentDuplicateExecutesProviderExactlyOnce(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	repository := &recordingRepository{head: request.ExpectedHead}
	providerService := &countingProviderService{delegate: fakeProviderService{log: &recordLog{}}}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{},
		Lane: NewOperationLane(), Repository: repository, TurnLeases: &recordingTurnLeaseManager{},
		Context: fakeContextPlanner{log: &recordLog{}}, Providers: fakeProviderCatalog{log: &recordLog{}}, Provider: providerService,
		Tools: noToolService{}, Authorization: &allowingAuthorization{log: &recordLog{}}, Evidence: noEvidenceRecorder{},
		Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan RunResult, 2)
	errCh := make(chan error, 2)
	var callers sync.WaitGroup
	callers.Add(2)
	for range 2 {
		go func() {
			defer callers.Done()
			result, runErr := service.RunTurn(context.Background(), request)
			results <- result
			errCh <- runErr
		}()
	}
	callers.Wait()
	close(results)
	close(errCh)
	for runErr := range errCh {
		if runErr != nil {
			t.Fatal(runErr)
		}
	}
	for result := range results {
		if result.Status != "completed" || result.CommandResult.Status != "completed" {
			t.Fatalf("result=%+v", result)
		}
	}
	if got := providerService.streams.Load(); got != 1 {
		t.Fatalf("provider streams=%d want=1", got)
	}

	changed := request
	changed.Command.RequestDigest = repeatedDigest("9")
	if _, err := service.RunTurn(context.Background(), changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed digest error=%v", err)
	}
	if got := providerService.streams.Load(); got != 1 {
		t.Fatalf("provider streams after conflict=%d want=1", got)
	}
}

func validateAppendRequests(t *testing.T, requests []journal.AppendRequest) {
	t.Helper()
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	for _, appendRequest := range requests {
		for index, proposed := range appendRequest.Events {
			envelope := protocol.EventEnvelope{
				SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: proposed.PayloadVersion,
				JournalKind: appendRequest.Journal.Kind, JournalID: appendRequest.Journal.ID, EventID: proposed.EventID,
				SessionID: proposed.SessionID, Seq: appendRequest.ExpectedHead.CommitSeq + uint64(index), Time: proposed.Time, Kind: proposed.Kind,
				TaskID: proposed.TaskID, TurnID: proposed.TurnID, ActivityID: proposed.ActivityID, ParentActivityID: proposed.ParentActivityID,
				CausationEventID: proposed.CausationEventID, Actor: proposed.Actor, RuntimeGenerationID: proposed.RuntimeGenerationID,
				TransactionID: appendRequest.TransactionID, Payload: proposed.Payload,
			}
			raw, marshalErr := canonicaljson.Marshal(envelope)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			record, decodeErr := registry.Decode(raw)
			if decodeErr != nil {
				t.Fatalf("decode %s: %v", proposed.Kind, decodeErr)
			}
			if validateErr := registry.Validate(record); validateErr != nil {
				t.Fatalf("validate %s: %v", proposed.Kind, validateErr)
			}
		}
	}
}

func TestCommitPureCommandReturnsDurableDuplicateAndRejectsChangedDigest(t *testing.T) {
	request := validStartTurnRequest()
	repository := &recordingRepository{head: request.ExpectedHead}
	service, err := NewService(Dependencies{Lane: NewOperationLane(), Repository: repository})
	if err != nil {
		t.Fatal(err)
	}
	completion := PureCommandCompletion{
		Journal:      protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)},
		ExpectedHead: request.ExpectedHead, PayloadVersion: 1, Payload: json.RawMessage(`{"ok":true}`),
	}
	first, err := service.CommitPureCommand(context.Background(), request.Command, completion)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := service.CommitPureCommand(context.Background(), request.Command, completion)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(first.Payload, duplicate.Payload) || first.Cursor.SelectedSession == nil || duplicate.Cursor.SelectedSession == nil || *first.Cursor.SelectedSession != *duplicate.Cursor.SelectedSession {
		t.Fatalf("first=%+v duplicate=%+v", first, duplicate)
	}
	changed := request.Command
	changed.RequestDigest = repeatedDigest("9")
	if _, err := service.CommitPureCommand(context.Background(), changed, completion); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed digest error=%v", err)
	}
	if got := len(repository.batchKinds()); got != 1 {
		t.Fatalf("committed batches=%d want=1", got)
	}
}

func TestConcurrentSameCommandPureAndSessionChangesCommitOneLifecycle(t *testing.T) {
	t.Run("pure", func(t *testing.T) {
		request := validStartTurnRequest()
		repository := &recordingRepository{head: request.ExpectedHead}
		service, err := NewService(Dependencies{Lane: NewOperationLane(), Repository: repository, Admission: passthroughAdmission{}})
		if err != nil {
			t.Fatal(err)
		}
		completion := PureCommandCompletion{
			Journal:      protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)},
			ExpectedHead: request.ExpectedHead, PayloadVersion: 1, Payload: json.RawMessage(`{"ok":true}`),
		}
		runConcurrent(t, func() error {
			_, commitErr := service.CommitPureCommand(context.Background(), request.Command, completion)
			return commitErr
		})
		if got := len(repository.batchKinds()); got != 1 {
			t.Fatalf("committed batches=%d want=1", got)
		}
	})

	t.Run("session_change", func(t *testing.T) {
		request := validStartTurnRequest()
		repository := &recordingRepository{head: request.ExpectedHead}
		service, err := NewService(Dependencies{Lane: NewOperationLane(), Repository: repository, Admission: passthroughAdmission{}})
		if err != nil {
			t.Fatal(err)
		}
		change := SessionChangeRequest{
			Command: request.Command, OperationID: "operation-a",
			Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)}, SessionID: request.SessionID,
			ExpectedHead: request.ExpectedHead, TransactionID: "transaction-session-change", RuntimeGenerationID: "generation-a", Consequential: true,
			Event: proposedSessionEvent(protocol.EventModeChanged, request.SessionID, protocol.ModeChangedV1{Mode: "safe"}),
		}
		runConcurrent(t, func() error {
			_, commitErr := service.CommitSessionChange(context.Background(), change)
			return commitErr
		})
		if got := len(repository.batchKinds()); got != 1 {
			t.Fatalf("committed batches=%d want=1", got)
		}
		changed := change
		changed.Command.RequestDigest = repeatedDigest("9")
		if _, err := service.CommitSessionChange(context.Background(), changed); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("changed digest error=%v", err)
		}
		if got := len(repository.batchKinds()); got != 1 {
			t.Fatalf("committed batches after conflict=%d want=1", got)
		}
	})
}

func runConcurrent(t *testing.T, call func() error) {
	t.Helper()
	errCh := make(chan error, 2)
	var callers sync.WaitGroup
	callers.Add(2)
	for range 2 {
		go func() {
			defer callers.Done()
			errCh <- call()
		}()
	}
	callers.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestCommitSessionChangeRoutesModeThroughApplicationControlLaneAndIsIdempotent(t *testing.T) {
	log := &recordLog{}
	request := validStartTurnRequest()
	repository := &recordingRepository{head: request.ExpectedHead, log: log}
	service, err := NewService(Dependencies{Lane: &loggingLane{delegate: NewOperationLane(), log: log}, Repository: repository, Admission: passthroughAdmission{}})
	if err != nil {
		t.Fatal(err)
	}
	change := SessionChangeRequest{
		Command: request.Command, OperationID: "operation-a",
		Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)}, SessionID: request.SessionID,
		ExpectedHead: request.ExpectedHead, TransactionID: "transaction-session-change", RuntimeGenerationID: "generation-a", Consequential: true,
		Event: proposedSessionEvent(protocol.EventModeChanged, request.SessionID, protocol.ModeChangedV1{Mode: "safe"}),
	}
	first, err := service.CommitSessionChange(context.Background(), change)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := service.CommitSessionChange(context.Background(), change)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != "completed" || duplicate.Status != first.Status || len(repository.batchKinds()) != 1 {
		t.Fatalf("first=%+v duplicate=%+v batches=%v", first, duplicate, repository.batchKinds())
	}
	if got := log.snapshot(); !containsContiguous(got, []string{"lane.acquire(control)", "append(command.accepted,mode.changed,command.completed)", "lane.release"}) {
		t.Fatalf("control lane ordering=%v", got)
	}

	bypass := change
	bypass.Command.CommandID = "command-b"
	bypass.Command.RequestDigest = repeatedDigest("8")
	bypass.Consequential = false
	if _, err := service.CommitSessionChange(context.Background(), bypass); err == nil {
		t.Fatal("mode change bypassed the application control lane")
	}
}

func TestRunControlCommitsAuthorizedLifecycleAndTerminalCommand(t *testing.T) {
	log := &recordLog{}
	runtime := validRuntimeManifest(t, "observation")
	ref := protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace-control-a"}
	head := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 1, TransactionID: "head-a"}
	repository := &recordingRepository{head: head, log: log}
	authorization := &allowingAuthorization{log: log}
	service, err := NewService(Dependencies{
		Lane: &loggingLane{delegate: NewOperationLane(), log: log}, Repository: repository, Authorization: authorization,
		Admission: passthroughAdmission{},
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := testActionPlan(tooling.PlanRequest{CallID: "reload-a", Alias: "reload", RuntimeGenerationID: runtime.ID}, "mutation", "not_reversible")
	actor := protocol.ActorRef{ID: "user-a", Kind: protocol.ActorUser}
	event := protocol.ProposedEvent{
		EventID: "event-runtime", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventRuntimeGenerationActivated,
		Actor: &actor, RuntimeGenerationID: runtime.ID, Payload: mustCanonical(protocol.RuntimeGenerationActivatedV1{Manifest: runtime}),
	}
	request := ControlRequest{
		Command:     CommandMetadata{CommandID: "control-command-a", IdempotencyKey: "control-key-a", RequestDigest: repeatedDigest("7"), Actor: actor},
		OperationID: "reload-operation-a", Kind: OperationReloadActivation, Journal: ref, ExpectedHead: head,
		TransactionID: "control-terminal-a", Runtime: runtime, Plan: plan, Event: event,
	}
	result, err := service.RunControl(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.CommandResult.Status != "completed" {
		t.Fatalf("result=%+v", result)
	}
	want := []string{
		"lane.acquire(reload_activation)", "append(command.accepted,control_operation.planned,authorization.requested)",
		"authorization.decide", "append(authorization.decided,control_operation.authorized)",
		"append(authorization.decision_consumed,control_operation.started)", "authorization.issue",
		"append(runtime_generation.activated,control_operation.completed,command.completed)", "lane.release",
	}
	if got := log.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("control order got=%v want=%v", got, want)
	}
	validateAppendRequests(t, repository.appendRequests())
}

func TestRunControlConcurrentDuplicateDispatchesExactlyOnce(t *testing.T) {
	runtime := validRuntimeManifest(t, "observation")
	ref := protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace-control-a"}
	head := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 1, TransactionID: "head-a"}
	repository := &recordingRepository{head: head}
	authorizationService := &allowingAuthorization{log: &recordLog{}}
	service, err := NewService(Dependencies{Lane: NewOperationLane(), Repository: repository, Authorization: authorizationService, Admission: passthroughAdmission{}})
	if err != nil {
		t.Fatal(err)
	}
	plan := testActionPlan(tooling.PlanRequest{CallID: "reload-a", Alias: "reload", RuntimeGenerationID: runtime.ID}, "mutation", "not_reversible")
	actor := protocol.ActorRef{ID: "user-a", Kind: protocol.ActorUser}
	request := ControlRequest{
		Command:     CommandMetadata{CommandID: "control-command-a", IdempotencyKey: "control-key-a", RequestDigest: repeatedDigest("7"), Actor: actor},
		OperationID: "reload-operation-a", Kind: OperationReloadActivation, Journal: ref, ExpectedHead: head,
		TransactionID: "control-terminal-a", Runtime: runtime, Plan: plan,
		Event: protocol.ProposedEvent{EventID: "event-runtime", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventRuntimeGenerationActivated, Actor: &actor, RuntimeGenerationID: runtime.ID, Payload: mustCanonical(protocol.RuntimeGenerationActivatedV1{Manifest: runtime})},
	}
	runConcurrent(t, func() error {
		_, runErr := service.RunControl(context.Background(), request)
		return runErr
	})
	if got := authorizationService.dispatches.Load(); got != 1 {
		t.Fatalf("control dispatches=%d want=1", got)
	}

	changed := request
	changed.Command.RequestDigest = repeatedDigest("8")
	if _, err := service.RunControl(context.Background(), changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed digest error=%v", err)
	}
	if got := authorizationService.dispatches.Load(); got != 1 {
		t.Fatalf("control dispatches after conflict=%d want=1", got)
	}
}

func proposedSessionEvent(kind string, sessionID protocol.SessionID, payload any) protocol.ProposedEvent {
	raw, _ := canonicaljson.Marshal(payload)
	actor := protocol.ActorRef{ID: "user-a", Kind: protocol.ActorUser}
	return protocol.ProposedEvent{
		EventID: "event-session-change", Time: time.Now().UTC(), PayloadVersion: 1, Kind: kind, SessionID: sessionID,
		Actor: &actor, RuntimeGenerationID: "generation-a", Payload: raw,
	}
}

func containsContiguous(values, target []string) bool {
	for start := 0; start+len(target) <= len(values); start++ {
		if slices.Equal(values[start:start+len(target)], target) {
			return true
		}
	}
	return false
}

func TestRunTurnCommitsCommandTaskContractAndTurnAcceptanceAtomically(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	repository := &recordingRepository{head: request.ExpectedHead}
	turnLeases := &recordingTurnLeaseManager{}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{},
		Lane: NewOperationLane(), Repository: repository, TurnLeases: turnLeases,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, _ = service.RunTurn(context.Background(), request)
	batches := repository.batchKinds()
	if len(batches) == 0 {
		t.Fatal("no journal batch was committed")
	}
	want := []string{
		protocol.EventCommandAccepted,
		protocol.EventTaskCreated,
		protocol.EventUserMessage,
		protocol.EventOutcomeContractDeclared,
		protocol.EventTurnAccepted,
	}
	if !slices.Equal(batches[0], want) {
		t.Fatalf("first batch=%v want=%v", batches[0], want)
	}
	if got := turnLeases.acquisitions.Load(); got != 1 {
		t.Fatalf("turn lease acquisitions=%d want=1", got)
	}
}

func TestRunTurnValidatesEveryIdentityBeforeApplicationLane(t *testing.T) {
	lane := &countingLane{}
	service, err := NewService(Dependencies{Lane: lane, Repository: inertRepository{}})
	if err != nil {
		t.Fatal(err)
	}

	request := validStartTurnRequest()
	request.Command.CommandID = ""
	if _, err := service.RunTurn(context.Background(), request); err == nil || !strings.Contains(err.Error(), "command") {
		t.Fatalf("invalid command error=%v", err)
	}
	if got := lane.acquisitions.Load(); got != 0 {
		t.Fatalf("lane acquisitions=%d, want 0", got)
	}

	request = validStartTurnRequest()
	request.ExpectedHead.JournalID = "another-session"
	if _, err := service.RunTurn(context.Background(), request); err == nil || !strings.Contains(err.Error(), "cursor") {
		t.Fatalf("invalid cursor error=%v", err)
	}
	if got := lane.acquisitions.Load(); got != 0 {
		t.Fatalf("lane acquisitions=%d, want 0", got)
	}
}

type countingLane struct{ acquisitions atomic.Int64 }

func (l *countingLane) Acquire(context.Context, OperationClaim) (OperationLease, error) {
	l.acquisitions.Add(1)
	return inertOperationLease{}, nil
}

type inertOperationLease struct{}

func (inertOperationLease) Claim() OperationClaim {
	return OperationClaim{Kind: OperationTurn, SessionID: "session-a"}
}
func (inertOperationLease) Release() {}

type inertRepository struct{}

func (inertRepository) Inspect(context.Context, protocol.JournalRef) (journal.Inspection, error) {
	return journal.Inspection{}, nil
}
func (inertRepository) Head(context.Context, protocol.JournalRef) (protocol.CommittedCursor, error) {
	return protocol.CommittedCursor{}, nil
}
func (inertRepository) ReadRange(context.Context, journal.ReadRangeRequest) (journal.EventPage, error) {
	return journal.EventPage{}, nil
}
func (inertRepository) AppendBatch(context.Context, journal.AppendRequest) (journal.AppendResult, error) {
	return journal.AppendResult{}, nil
}
func (inertRepository) LookupTransaction(context.Context, protocol.JournalRef, protocol.TransactionID) (journal.TransactionLookup, error) {
	return journal.TransactionLookup{}, nil
}
func (inertRepository) ReadCommittedTransaction(context.Context, protocol.JournalRef, protocol.TransactionID) (journal.CommittedTransaction, error) {
	return journal.CommittedTransaction{}, nil
}
func (inertRepository) Recover(context.Context, journal.RecoveryRequest) (journal.RecoveryResult, error) {
	return journal.RecoveryResult{}, nil
}

func validStartTurnRequest() StartTurnRequest {
	sessionID := protocol.SessionID("session-a")
	return StartTurnRequest{
		Command: CommandMetadata{
			CommandID:      "command-a",
			IdempotencyKey: "key-a",
			RequestDigest:  repeatedDigest("a"),
			Actor:          protocol.ActorRef{ID: "user-a", Kind: protocol.ActorUser},
		},
		SessionID: sessionID,
		ExpectedHead: protocol.CommittedCursor{
			JournalKind:   protocol.JournalSession,
			JournalID:     protocol.JournalID(sessionID),
			CommitSeq:     1,
			TransactionID: "transaction-head",
		},
		Prompt: "inspect",
		Runtime: protocol.RuntimeGenerationManifest{
			ID:     "generation-a",
			Digest: repeatedDigest("b"),
		},
	}
}

func repeatedDigest(fill string) protocol.Digest {
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat(fill, 64)}
}

type recordingRepository struct {
	mu       sync.Mutex
	head     protocol.CommittedCursor
	batches  [][]string
	log      *recordLog
	events   []protocol.ProposedEvent
	requests []journal.AppendRequest
}

func (r *recordingRepository) Inspect(context.Context, protocol.JournalRef) (journal.Inspection, error) {
	return journal.Inspection{Head: r.head, Writable: true}, nil
}
func (r *recordingRepository) Head(context.Context, protocol.JournalRef) (protocol.CommittedCursor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.head, nil
}
func (r *recordingRepository) ReadRange(context.Context, journal.ReadRangeRequest) (journal.EventPage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	records := make([]protocol.EventRecord, len(r.events))
	for index, event := range r.events {
		records[index] = protocol.EventRecord{Envelope: protocol.EventEnvelope{
			JournalKind: r.head.JournalKind, JournalID: r.head.JournalID, SessionID: event.SessionID,
			EventID: event.EventID, Time: event.Time, Kind: event.Kind, PayloadVersion: event.PayloadVersion, Payload: protocol.DeepCopy(event.Payload),
		}}
	}
	return journal.EventPage{Events: records, Head: r.head, Cursor: r.head}, nil
}
func (r *recordingRepository) AppendBatch(_ context.Context, request journal.AppendRequest) (journal.AppendResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if request.ExpectedHead != r.head {
		return journal.AppendResult{Status: journal.AppendConflict, CurrentHead: r.head}, nil
	}
	kinds := make([]string, len(request.Events))
	for index, event := range request.Events {
		kinds[index] = event.Kind
	}
	r.batches = append(r.batches, kinds)
	r.events = append(r.events, protocol.DeepCopy(request.Events)...)
	r.requests = append(r.requests, protocol.DeepCopy(request))
	if r.log != nil {
		r.log.add("append(" + strings.Join(kinds, ",") + ")")
	}
	r.head = protocol.CommittedCursor{
		JournalKind: request.Journal.Kind, JournalID: request.Journal.ID,
		CommitSeq: r.head.CommitSeq + uint64(len(request.Events)) + 1, TransactionID: request.TransactionID,
	}
	return journal.AppendResult{Status: journal.AppendCommitted, Cursor: r.head}, nil
}
func (r *recordingRepository) LookupTransaction(context.Context, protocol.JournalRef, protocol.TransactionID) (journal.TransactionLookup, error) {
	return journal.TransactionLookup{State: journal.TransactionNotCommitted}, nil
}
func (r *recordingRepository) ReadCommittedTransaction(context.Context, protocol.JournalRef, protocol.TransactionID) (journal.CommittedTransaction, error) {
	return journal.CommittedTransaction{}, nil
}
func (r *recordingRepository) Recover(context.Context, journal.RecoveryRequest) (journal.RecoveryResult, error) {
	return journal.RecoveryResult{}, nil
}
func (r *recordingRepository) batchKinds() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([][]string, len(r.batches))
	for index := range r.batches {
		result[index] = append([]string(nil), r.batches[index]...)
	}
	return result
}
func (r *recordingRepository) appendRequests() []journal.AppendRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return protocol.DeepCopy(r.requests)
}

type recordingTurnLeaseManager struct {
	acquisitions atomic.Int64
	log          *recordLog
}

func (m *recordingTurnLeaseManager) AcquireTurnLease(_ context.Context, sessionID protocol.SessionID, turnID protocol.TurnID, _ protocol.CommittedCursor) (journal.TurnLease, error) {
	m.acquisitions.Add(1)
	if m.log != nil {
		m.log.add("turn_lease.acquire")
	}
	return recordingTurnLease{sessionID: sessionID, turnID: turnID, log: m.log}, nil
}
func (m *recordingTurnLeaseManager) AcquireTurnRecoveryLease(_ context.Context, sessionID protocol.SessionID, turnID protocol.TurnID, _ protocol.CommittedCursor) (journal.TurnLease, error) {
	m.acquisitions.Add(1)
	if m.log != nil {
		m.log.add("turn_recovery_lease.acquire")
	}
	return recordingTurnLease{sessionID: sessionID, turnID: turnID, log: m.log}, nil
}

type recordingTurnLease struct {
	sessionID protocol.SessionID
	turnID    protocol.TurnID
	log       *recordLog
}

func (l recordingTurnLease) SessionID() protocol.SessionID { return l.sessionID }
func (l recordingTurnLease) TurnID() protocol.TurnID       { return l.turnID }

func (l recordingTurnLease) Release(context.Context, protocol.CommittedCursor) error {
	if l.log != nil {
		l.log.add("turn_lease.release")
	}
	return nil
}

type recordLog struct {
	mu     sync.Mutex
	values []string
}

func (l *recordLog) add(value string) {
	l.mu.Lock()
	l.values = append(l.values, value)
	l.mu.Unlock()
}
func (l *recordLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.values...)
}

type loggingLane struct {
	delegate OperationLane
	log      *recordLog
}

func (l *loggingLane) Acquire(ctx context.Context, claim OperationClaim) (OperationLease, error) {
	l.log.add("lane.acquire(" + string(claim.Kind) + ")")
	lease, err := l.delegate.Acquire(ctx, claim)
	if err != nil {
		return nil, err
	}
	return loggingOperationLease{OperationLease: lease, log: l.log}, nil
}

type loggingOperationLease struct {
	OperationLease
	log *recordLog
}

func (l loggingOperationLease) Release() {
	l.OperationLease.Release()
	l.log.add("lane.release")
}

type fakeContextPlanner struct{ log *recordLog }

func (p fakeContextPlanner) Plan(_ context.Context, request contextplanner.Request) (protocol.ContextPlan, error) {
	p.log.add("context.plan")
	content := []protocol.ContentBlock{{Kind: protocol.ContentText, Text: "inspect"}}
	sourceDigest, _ := canonicaljson.Digest(content)
	body := protocol.ContextPlanBody{
		Sources:              []protocol.ContentSource{{ID: "prompt", Kind: "user_message", Scope: "turn", Provenance: "test", Digest: sourceDigest, Content: content}},
		Excluded:             []protocol.ExcludedContentSource{},
		EstimatedInputTokens: protocol.ValueInt64{State: protocol.ValueUnknown}, OutputReserve: 0,
		ContextWindow: protocol.ValueInt64{State: protocol.ValueUnknown}, CompactionRevision: "none", ToolExposureRevision: "tools-a",
	}
	digest, _ := canonicaljson.Digest(body)
	return protocol.ContextPlan{Body: body, Digest: digest}, nil
}

type fakeProviderCatalog struct{ log *recordLog }

func (c fakeProviderCatalog) Resolve(protocol.ProviderID, protocol.ModelID) (protocol.ModelDescriptor, bool) {
	return protocol.ModelDescriptor{}, false
}
func (c fakeProviderCatalog) Negotiate(_ protocol.ProviderID, _ protocol.ModelID, _ []protocol.CapabilityRequirement, _ string) (protocol.NegotiatedProviderPlan, error) {
	c.log.add("provider.negotiate")
	manifest := validModelDescriptor("generation-a")
	body := protocol.NegotiatedProviderPlanBody{Descriptor: manifest, Requirements: []protocol.CapabilityRequirement{}, ToolExposureRevision: "tools-a", Warnings: []string{}}
	digest, _ := canonicaljson.Digest(body)
	return protocol.NegotiatedProviderPlan{Body: body, Digest: digest}, nil
}

type fakeProviderService struct{ log *recordLog }

type countingProviderService struct {
	delegate fakeProviderService
	streams  atomic.Int64
}

func (s *countingProviderService) Prepare(ctx context.Context, activityID protocol.ActivityID, callID string, request protocol.ModelRequest, digest protocol.Digest) (provider.ProviderHandle, error) {
	return s.delegate.Prepare(ctx, activityID, callID, request, digest)
}

func (s *countingProviderService) Stream(ctx context.Context, handle provider.ProviderHandle, token authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	s.streams.Add(1)
	return s.delegate.Stream(ctx, handle, token)
}

func (s fakeProviderService) Prepare(context.Context, protocol.ActivityID, string, protocol.ModelRequest, protocol.Digest) (provider.ProviderHandle, error) {
	s.log.add("provider.prepare")
	return provider.ProviderHandle{}, nil
}

type toolThenFinalProvider struct {
	log     *recordLog
	streams atomic.Int64
}

type twoToolThenFinalProvider struct {
	log     *recordLog
	streams atomic.Int64
}

func (s *twoToolThenFinalProvider) Prepare(context.Context, protocol.ActivityID, string, protocol.ModelRequest, protocol.Digest) (provider.ProviderHandle, error) {
	s.log.add("provider.prepare")
	return provider.ProviderHandle{}, nil
}
func (s *twoToolThenFinalProvider) Stream(context.Context, provider.ProviderHandle, authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	s.log.add("provider.stream")
	stream := make(chan protocol.ModelEvent, 3)
	if s.streams.Add(1) == 1 {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventToolIntent, Sequence: 1, ToolIntent: &protocol.ToolUseBlock{CallID: "call-b", Alias: "read", Arguments: json.RawMessage(`{"path":"b.go"}`)}}
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventToolIntent, Sequence: 2, ToolIntent: &protocol.ToolUseBlock{CallID: "call-a", Alias: "read", Arguments: json.RawMessage(`{"path":"a.go"}`)}}
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 3, Terminal: &protocol.ModelTerminal{Reason: "tool_use"}}
	} else {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: "done"}}
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	}
	close(stream)
	return stream, nil
}

func (s *toolThenFinalProvider) Prepare(context.Context, protocol.ActivityID, string, protocol.ModelRequest, protocol.Digest) (provider.ProviderHandle, error) {
	s.log.add("provider.prepare")
	return provider.ProviderHandle{}, nil
}
func (s *toolThenFinalProvider) Stream(context.Context, provider.ProviderHandle, authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	s.log.add("provider.stream")
	stream := make(chan protocol.ModelEvent, 2)
	if s.streams.Add(1) == 1 {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventToolIntent, Sequence: 1, ToolIntent: &protocol.ToolUseBlock{CallID: "call-a", Alias: "read", Arguments: json.RawMessage(`{"path":"a.go"}`)}}
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "tool_use"}}
	} else {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: "done"}}
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	}
	close(stream)
	return stream, nil
}
func (s fakeProviderService) Stream(context.Context, provider.ProviderHandle, authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	s.log.add("provider.stream")
	stream := make(chan protocol.ModelEvent, 2)
	stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: "done"}}
	stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	close(stream)
	return stream, nil
}

type allowingAuthorization struct {
	log        *recordLog
	nonce      atomic.Int64
	dispatches atomic.Int64
}

func (a *allowingAuthorization) Decide(_ context.Context, request protocol.AuthorizationRequest) (protocol.AuthorizationDecision, error) {
	a.log.add("authorization.decide")
	return protocol.AuthorizationDecision{
		Request: request, Action: "allow", Scope: protocol.CanonicalAuthorizationScope{Capability: request.Action, Source: request.Source, Resources: request.Resources, Constraints: []protocol.AuthorizationConstraint{}},
		Constraints: []protocol.AuthorizationConstraint{},
		Lifetime:    protocol.AuthorizationLifetimeOnce, PolicySource: "test", PolicyGeneration: request.PolicyGeneration,
		Reason: "allowed", DecidedAt: time.Now().UTC(), PlanDigest: request.PlanDigest,
		DecisionNonce: protocol.DecisionNonce(fmt.Sprintf("nonce-%d", a.nonce.Add(1))),
	}, nil
}
func (a *allowingAuthorization) ResolveInteractive(context.Context, protocol.AuthorizationRequest, protocol.AuthorizationDecision, protocol.ApprovalResponse) (protocol.AuthorizationDecision, error) {
	return protocol.AuthorizationDecision{}, fmt.Errorf("unexpected interactive decision")
}
func (a *allowingAuthorization) Issue(context.Context, authorization.CommitReference) (authorization.CommittedToken, error) {
	a.log.add("authorization.issue")
	return authorization.CommittedToken{}, nil
}
func (a *allowingAuthorization) Dispatch(_ context.Context, _ authorization.CommittedToken, _ authorization.DispatchBinding, callback func(context.Context) error) error {
	a.dispatches.Add(1)
	return callback(context.Background())
}

type noToolService struct{}

func (noToolService) Plan(context.Context, tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error) {
	return tooling.ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("unexpected tool plan")
}

type mutationToolService struct{ log *recordLog }

func (s mutationToolService) Plan(context.Context, tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error) {
	return tooling.ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("unexpected direct plan")
}

type observationToolService struct{ log *recordLog }

func (s observationToolService) Plan(_ context.Context, request tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error) {
	s.log.add("tool.plan(" + request.CallID + ")")
	return tooling.ActionHandle{}, testActionPlan(request, "observation", "not_applicable"), nil
}
func (s observationToolService) PlanPreviewInspection(context.Context, tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error) {
	return tooling.ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("unexpected preview")
}
func (s observationToolService) PreparePreview(context.Context, tooling.ActionHandle, authorization.CommittedToken) (tooling.PreviewResult, protocol.ActionPlan, []protocol.EvidenceCandidate, error) {
	return tooling.PreviewResult{}, protocol.ActionPlan{}, nil, fmt.Errorf("unexpected preview")
}
func (s observationToolService) PlanMutation(context.Context, tooling.PreviewResult, tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error) {
	return tooling.ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("unexpected mutation")
}
func (s observationToolService) Revalidate(context.Context, tooling.ActionHandle) (protocol.ActionPlan, bool, error) {
	return protocol.ActionPlan{}, false, nil
}
func (s observationToolService) Execute(_ context.Context, _ tooling.ActionHandle, _ authorization.CommittedToken) (protocol.ExecutionResult, error) {
	// The plan/execute calls are strictly synchronous, so the most recent plan
	// entry identifies this opaque fake handle's call for the FIFO assertion.
	values := s.log.snapshot()
	callID := ""
	for index := len(values) - 1; index >= 0; index-- {
		if strings.HasPrefix(values[index], "tool.plan(") {
			callID = strings.TrimSuffix(strings.TrimPrefix(values[index], "tool.plan("), ")")
			break
		}
	}
	s.log.add("tool.execute(" + callID + ")")
	return protocol.ExecutionResult{Outcome: protocol.ActivityOutcomeV1{Status: "succeeded"}, ToolResult: protocol.ToolResultBlock{CallID: callID, Status: "succeeded", Text: "ok"}}, nil
}
func (s mutationToolService) PlanPreviewInspection(_ context.Context, request tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error) {
	s.log.add("tool.plan_preview")
	return tooling.ActionHandle{}, testActionPlan(request, "observation", "not_applicable"), nil
}
func (s mutationToolService) PreparePreview(_ context.Context, _ tooling.ActionHandle, _ authorization.CommittedToken) (tooling.PreviewResult, protocol.ActionPlan, []protocol.EvidenceCandidate, error) {
	s.log.add("tool.prepare_preview")
	return tooling.PreviewResult{}, protocol.ActionPlan{}, []protocol.EvidenceCandidate{{
		ID: "evidence-preview", Kind: "tool_preview", MediaType: "text/plain", ProducingActivityID: "placeholder",
		Actor: protocol.ActorRef{ID: "read", Kind: protocol.ActorTool}, Subject: protocol.SubjectRef{Kind: "file", ID: "a.go"}, Content: []byte("preview"), Limit: 1024,
	}}, nil
}
func (s mutationToolService) PlanMutation(_ context.Context, _ tooling.PreviewResult, request tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error) {
	s.log.add("tool.plan_mutation")
	return tooling.ActionHandle{}, testActionPlan(request, "mutation", "exact"), nil
}
func (s mutationToolService) Revalidate(_ context.Context, _ tooling.ActionHandle) (protocol.ActionPlan, bool, error) {
	s.log.add("tool.revalidate")
	return protocol.ActionPlan{}, false, nil
}
func (s mutationToolService) Execute(_ context.Context, _ tooling.ActionHandle, _ authorization.CommittedToken) (protocol.ExecutionResult, error) {
	s.log.add("tool.execute")
	return protocol.ExecutionResult{
		Outcome:    protocol.ActivityOutcomeV1{Status: "succeeded"},
		ToolResult: protocol.ToolResultBlock{CallID: "call-a", Status: "succeeded", Text: "edited"},
		Evidence:   []protocol.EvidenceCandidate{{ID: "evidence-result", Kind: "tool_output", MediaType: "text/plain", ProducingActivityID: "placeholder", Actor: protocol.ActorRef{ID: "read", Kind: protocol.ActorTool}, Subject: protocol.SubjectRef{Kind: "file", ID: "a.go"}, Content: []byte("edited"), Limit: 1024}},
	}, nil
}

func testActionPlan(request tooling.PlanRequest, effect, reversibility string) protocol.ActionPlan {
	body := protocol.ActionPlanBody{
		CallID: request.CallID, Tool: protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: request.Alias},
		SourceRevision: "tools-a", DescriptorDigest: repeatedDigest("c"), Action: request.Alias, Purpose: "inspect",
		Resources: []protocol.ResourceTarget{{Kind: "file", CanonicalID: "a.go"}}, ExecutionLocus: "local", Effect: effect,
		Boundary: "workspace", Reversibility: reversibility, VerificationCoverage: "exact", RequestedProfile: "restricted",
		EffectiveProfile: "restricted", RuntimeGenerationID: request.RuntimeGenerationID,
	}
	if effect != "observation" {
		body.Purpose = "mutate"
	}
	digest, _ := canonicaljson.Digest(body)
	return protocol.ActionPlan{Body: body, Digest: digest}
}
func (noToolService) PlanPreviewInspection(context.Context, tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error) {
	return tooling.ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("unexpected preview plan")
}
func (noToolService) PreparePreview(context.Context, tooling.ActionHandle, authorization.CommittedToken) (tooling.PreviewResult, protocol.ActionPlan, []protocol.EvidenceCandidate, error) {
	return tooling.PreviewResult{}, protocol.ActionPlan{}, nil, fmt.Errorf("unexpected preview")
}
func (noToolService) PlanMutation(context.Context, tooling.PreviewResult, tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error) {
	return tooling.ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("unexpected mutation plan")
}
func (noToolService) Revalidate(context.Context, tooling.ActionHandle) (protocol.ActionPlan, bool, error) {
	return protocol.ActionPlan{}, false, fmt.Errorf("unexpected revalidation")
}
func (noToolService) Execute(context.Context, tooling.ActionHandle, authorization.CommittedToken) (protocol.ExecutionResult, error) {
	return protocol.ExecutionResult{}, fmt.Errorf("unexpected execution")
}

type noEvidenceRecorder struct{}

func (noEvidenceRecorder) Put(context.Context, protocol.EvidenceCandidate) (protocol.EvidenceRecord, error) {
	return protocol.EvidenceRecord{}, fmt.Errorf("unexpected evidence")
}

type recordingEvidence struct{ log *recordLog }

func (r recordingEvidence) Put(_ context.Context, candidate protocol.EvidenceCandidate) (protocol.EvidenceRecord, error) {
	r.log.add("evidence.put")
	body := protocol.EvidenceRecordBody{
		ID: candidate.ID, Kind: candidate.Kind, WorkspaceID: candidate.WorkspaceID, SessionID: candidate.SessionID,
		Availability: protocol.ContentWithheldSecret, MediaType: candidate.MediaType, Size: int64(len(candidate.Content)),
		ProducingActivityID: candidate.ProducingActivityID, Actor: candidate.Actor, Subject: candidate.Subject,
		CreatedAt: time.Now().UTC(), Redacted: true,
	}
	digest, _ := canonicaljson.Digest(body)
	return protocol.EvidenceRecord{Body: body, Digest: digest}, nil
}

type noRecoveryRecorder struct{}

func (noRecoveryRecorder) PrepareAndPut(context.Context, tooling.PreviewResult, protocol.ActivityID, protocol.CheckpointBody, protocol.ActionPlan) (protocol.RecoveryMaterialRecord, error) {
	return protocol.RecoveryMaterialRecord{}, fmt.Errorf("unexpected recovery material")
}

type recordingRecovery struct{ log *recordLog }

func (r recordingRecovery) PrepareAndPut(_ context.Context, _ tooling.PreviewResult, activityID protocol.ActivityID, checkpoint protocol.CheckpointBody, plan protocol.ActionPlan) (protocol.RecoveryMaterialRecord, error) {
	r.log.add("tool.recovery_candidate")
	r.log.add("recovery.put")
	return protocol.RecoveryMaterialRecord{
		ID: "recovery-a", WorkspaceID: protocol.WorkspaceID(checkpoint.SessionID), ActivityID: activityID,
		CheckpointID: checkpoint.ID, Subject: checkpoint.Coverage[0].Subject,
		Body:           protocol.RecoveryMaterialBody{PlanDigest: plan.Digest, PreimageDigest: repeatedDigest("d"), ExpectedPostimageDigest: repeatedDigest("e"), Mode: 0o644, CreatedAt: time.Now().UTC()},
		MaterialDigest: repeatedDigest("f"),
	}, nil
}

type loggingVerification struct {
	log      *recordLog
	delegate *verification.Service
}

func (v loggingVerification) Assess(ctx context.Context, request verification.Request) (verification.Result, error) {
	v.log.add("verification.assess")
	return v.delegate.Assess(ctx, request)
}

func validRuntimeManifest(t *testing.T, effect string) protocol.RuntimeGenerationManifest {
	t.Helper()
	descriptor := protocol.ToolDescriptor{Body: protocol.ToolDescriptorBody{
		Identity: protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "read"}, SourceRevision: "tools-a",
		DisplayName: "Read", Description: "read", InputSchema: json.RawMessage(`{"type":"object"}`),
		Effect: effect, Mutation: "none", ExecutionLoci: []string{"local"}, ClassificationSource: "builtin", Idempotency: "safe", Retry: "safe",
	}}
	descriptor.DescriptorDigest, _ = canonicaljson.Digest(descriptor.Body)
	body := protocol.RuntimeGenerationBody{
		ProviderCatalogRevision: "providers-a", Models: []protocol.ModelDescriptor{validModelDescriptor("generation-a")},
		ToolCatalogRevision: "tools-a", Tools: []protocol.ToolDescriptor{descriptor}, InstructionRevision: "instructions-a",
		PolicyGeneration: "policy-a", ExecutionProfiles: []string{"restricted"},
		Limits: protocol.RuntimeLimits{MaxToolCalls: 4, ShellTimeoutNanos: 1, ApplicationQueueCapacity: 8},
	}
	digest, err := canonicaljson.Digest(body)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.RuntimeGenerationManifest{ID: "generation-a", Body: body, Digest: digest}
}

func validModelDescriptor(generation protocol.RuntimeGenerationID) protocol.ModelDescriptor {
	return protocol.ModelDescriptor{
		ProviderID: "provider-a", ModelID: "model-a", AdapterKind: "test", DisplayName: "Test",
		ContextWindow: protocol.ValueInt64{State: protocol.ValueUnknown}, MaximumOutput: protocol.ValueInt64{State: protocol.ValueUnknown},
		Capabilities: []protocol.CapabilityFact{}, UsageCategories: []string{}, Pricing: []protocol.PricingFact{},
		CredentialBindingRef: "credential-a", SourceRevision: "providers-a", RuntimeGenerationID: generation,
	}
}
