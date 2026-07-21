package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	contextplanner "github.com/muratmirgun/yordam/internal/context"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/provider"
	"github.com/muratmirgun/yordam/internal/recovery"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/tooling"
	"github.com/muratmirgun/yordam/internal/verification"
)

func TestRecoveryBoundaryNeverExposesRawCandidateOrPreimage(t *testing.T) {
	bannedCandidate := reflect.TypeOf(recovery.Candidate{})
	bannedBytes := reflect.TypeOf([]byte(nil))
	for _, boundary := range []reflect.Type{
		reflect.TypeOf((*ToolService)(nil)).Elem(),
		reflect.TypeOf((*RecoveryRecorder)(nil)).Elem(),
	} {
		for index := 0; index < boundary.NumMethod(); index++ {
			method := boundary.Method(index)
			for argument := 0; argument < method.Type.NumIn(); argument++ {
				got := method.Type.In(argument)
				if got == bannedCandidate || got == bannedBytes {
					t.Fatalf("%s.%s exposes banned recovery type %s", boundary.Name(), method.Name, got)
				}
			}
		}
	}
}

func TestSessionChangeUsesFoundationSemanticValidationBeforeLane(t *testing.T) {
	request := validStartTurnRequest()
	lane := &countingLane{}
	service, err := NewService(Dependencies{Lane: lane, Repository: inertRepository{}})
	if err != nil {
		t.Fatal(err)
	}
	actor := request.Command.Actor
	change := SessionChangeRequest{
		Command: request.Command, OperationID: "operation-a",
		Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)}, SessionID: request.SessionID,
		ExpectedHead: request.ExpectedHead, TransactionID: "transaction-a", RuntimeGenerationID: "generation-a", Consequential: true,
		Event: protocol.ProposedEvent{
			EventID: "event-mode", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventModeChanged,
			SessionID: request.SessionID, Actor: &actor, RuntimeGenerationID: "generation-a", Payload: json.RawMessage(`{"mode":"not-a-mode"}`),
		},
	}
	if _, err := service.CommitSessionChange(context.Background(), change); err == nil {
		t.Fatal("semantically invalid mode event was accepted")
	}
	if got := lane.acquisitions.Load(); got != 0 {
		t.Fatalf("lane acquisitions=%d want=0", got)
	}
}

func TestRunTurnUsesFoundationRuntimeValidationBeforeLane(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	request.Runtime.Body.Models = append(request.Runtime.Body.Models, request.Runtime.Body.Models[0])
	request.Runtime.Digest, _ = canonicaljson.Digest(request.Runtime.Body)
	lane := &countingLane{}
	service, err := NewService(Dependencies{Lane: lane, Repository: inertRepository{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err == nil {
		t.Fatal("duplicate runtime model was accepted")
	}
	if got := lane.acquisitions.Load(); got != 0 {
		t.Fatalf("lane acquisitions=%d want=0", got)
	}
}

func TestRunTurnRequiresGenerationAdmissionAndInstructionsBeforeLane(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	for _, test := range []struct {
		name         string
		admission    AdmissionService
		instructions InstructionService
	}{
		{name: "admission", instructions: emptyInstructionService{}},
		{name: "instructions", admission: passthroughAdmission{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			lane := &countingLane{}
			service, err := NewService(Dependencies{
				Lane: lane, Repository: inertRepository{}, Admission: test.admission, Instructions: test.instructions,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.RunTurn(context.Background(), request); err == nil {
				t.Fatal("missing safety dependency was accepted")
			}
			if got := lane.acquisitions.Load(); got != 0 {
				t.Fatalf("lane acquisitions=%d want=0", got)
			}
		})
	}
}

type passthroughAdmission struct{}

func (passthroughAdmission) SanitizeText(_ context.Context, _ protocol.RuntimeGenerationID, value string) (string, error) {
	return value, nil
}

func (passthroughAdmission) SanitizeJSON(_ context.Context, _ protocol.RuntimeGenerationID, value json.RawMessage) (json.RawMessage, error) {
	return protocol.DeepCopy(value), nil
}

func (passthroughAdmission) OpenTextStream(context.Context, protocol.RuntimeGenerationID) (StreamingSanitizer, error) {
	return passthroughStreamingSanitizer{}, nil
}

type passthroughStreamingSanitizer struct{}

func (passthroughStreamingSanitizer) Write(value string) (string, error) { return value, nil }
func (passthroughStreamingSanitizer) Close() (string, error)             { return "", nil }

type emptyInstructionService struct{}

func (emptyInstructionService) SystemInstructions(context.Context, protocol.RuntimeGenerationID, string, protocol.SessionID) ([]protocol.ContentSource, error) {
	return []protocol.ContentSource{}, nil
}

func TestLookupCommandPaginatesThroughProjectionHead(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session-a"}
	digest := repeatedDigest("a")
	commandID := protocol.CommandID("command-a")
	firstCursor := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 3, TransactionID: "first"}
	head := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 6, TransactionID: "second"}
	result := protocol.CommandResult{
		ProtocolVersion: protocol.ApplicationProtocolVersion, CommandID: commandID, Status: "completed", RequestDigest: digest,
		Cursor: protocol.ApplicationCursor{SelectedSession: &head}, PayloadVersion: 1, Payload: json.RawMessage(`{"status":"completed"}`),
	}
	rawResult, err := canonicaljson.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	repository := &pagedCommandRepository{pages: []journal.EventPage{
		{
			Events: []protocol.EventRecord{{Envelope: protocol.EventEnvelope{Kind: protocol.EventCommandAccepted, Payload: mustCanonical(protocol.CommandAcceptedV1{CommandID: commandID, RequestDigest: digest, IdempotencyKey: "key-a"})}}},
			Cursor: firstCursor, Head: head, More: true,
		},
		{
			Events: []protocol.EventRecord{{Envelope: protocol.EventEnvelope{Kind: protocol.EventCommandCompleted, Payload: mustCanonical(protocol.CommandCompletedV1{CommandID: commandID, RequestDigest: digest, Status: "completed", Result: rawResult})}}},
			Cursor: head, Head: head,
		},
	}}
	service, err := NewService(Dependencies{Lane: NewOperationLane(), Repository: repository})
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := service.LookupCommand(context.Background(), ref, commandID, digest)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || got.Status != "completed" || repository.calls.Load() != 2 {
		t.Fatalf("result=%+v ok=%v reads=%d", got, ok, repository.calls.Load())
	}
	if repository.after[1] != firstCursor {
		t.Fatalf("second page cursor=%+v want=%+v", repository.after[1], firstCursor)
	}
}

func TestReadFullHistoryPaginatesFromOriginToCurrentPrefix(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session-a"}
	firstCursor := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 2, TransactionID: "first"}
	head := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 4, TransactionID: "second"}
	repository := &pagedCommandRepository{pages: []journal.EventPage{
		{Events: []protocol.EventRecord{{Envelope: protocol.EventEnvelope{EventID: "event-a"}}}, Cursor: firstCursor, Head: head, More: true},
		{Events: []protocol.EventRecord{{Envelope: protocol.EventEnvelope{EventID: "event-b"}}}, Cursor: head, Head: head},
	}}
	service, err := NewService(Dependencies{Lane: NewOperationLane(), Repository: repository})
	if err != nil {
		t.Fatal(err)
	}
	events, err := service.readFullHistory(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if got := []protocol.EventID{events[0].Envelope.EventID, events[1].Envelope.EventID}; !slices.Equal(got, []protocol.EventID{"event-a", "event-b"}) {
		t.Fatalf("history=%v", got)
	}
	if repository.after[0] != (protocol.CommittedCursor{}) || repository.after[1] != firstCursor {
		t.Fatalf("page cursors=%+v", repository.after)
	}
}

func TestRunTurnSuppliesRuntimeInstructionsToContextPlanner(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	content := []protocol.ContentBlock{{Kind: protocol.ContentText, Text: "system policy"}}
	digest, _ := canonicaljson.Digest(content)
	instructions := []protocol.ContentSource{{ID: "system-a", Kind: "system_instruction", Scope: "generation", Provenance: "test", Digest: digest, Content: content}}
	planner := &capturingContextPlanner{delegate: fakeContextPlanner{log: &recordLog{}}}
	service, err := NewService(Dependencies{
		Lane: NewOperationLane(), Repository: &recordingRepository{head: request.ExpectedHead}, TurnLeases: &recordingTurnLeaseManager{},
		Context: planner, Providers: fakeProviderCatalog{log: &recordLog{}}, Provider: fakeProviderService{log: &recordLog{}},
		Tools: noToolService{}, Authorization: &allowingAuthorization{log: &recordLog{}}, Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{},
		Verification: verificationServiceForReview{}, Admission: passthroughAdmission{}, Instructions: staticInstructionService{sources: instructions},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	captured := planner.last()
	if !reflect.DeepEqual(captured.SystemInstructions, instructions) {
		t.Fatalf("system instructions=%+v want=%+v", captured.SystemInstructions, instructions)
	}
}

func TestProviderSplitSecretNeverAppearsInDurableEvents(t *testing.T) {
	const sentinel = "split-secret"
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	repository := &recordingRepository{head: request.ExpectedHead}
	service, err := NewService(Dependencies{
		Lane: NewOperationLane(), Repository: repository, TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: &recordLog{}},
		Providers: fakeProviderCatalog{log: &recordLog{}}, Provider: splitSecretProvider{}, Tools: noToolService{},
		Authorization: &allowingAuthorization{log: &recordLog{}}, Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{},
		Verification: verificationServiceForReview{}, Admission: newRedactingAdmission(sentinel), Instructions: emptyInstructionService{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	for _, appendRequest := range repository.appendRequests() {
		for _, event := range appendRequest.Events {
			if strings.Contains(string(event.Payload), sentinel) {
				t.Fatalf("secret leaked in %s: %s", event.Kind, event.Payload)
			}
		}
	}
}

func TestProviderPreservesInterleavedDeltaAndBlockOrder(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	repository := &recordingRepository{head: request.ExpectedHead}
	service, err := NewService(Dependencies{
		Lane: NewOperationLane(), Repository: repository, TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: &recordLog{}},
		Providers: fakeProviderCatalog{log: &recordLog{}}, Provider: interleavedProvider{}, Tools: noToolService{},
		Authorization: &allowingAuthorization{log: &recordLog{}}, Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{},
		Verification: verificationServiceForReview{}, Admission: passthroughAdmission{}, Instructions: emptyInstructionService{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	var message protocol.AssistantMessageV1
	for _, appendRequest := range repository.appendRequests() {
		for _, event := range appendRequest.Events {
			if event.Kind == protocol.EventAssistantMessage {
				if err := json.Unmarshal(event.Payload, &message); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if len(message.Blocks) != 3 || message.Blocks[0].Text != "first" || message.Blocks[1].JSON == nil || message.Blocks[2].Text != "last" {
		t.Fatalf("assistant blocks=%+v", message.Blocks)
	}
}

func TestRunControlConsequentialEventCommitsInsideDispatchCallback(t *testing.T) {
	request := validControlRequestForReview(t)
	repository := &recordingRepository{head: request.ExpectedHead}
	authorizationService := &inspectingDispatchAuthorization{
		allowingAuthorization: allowingAuthorization{log: &recordLog{}}, repository: repository, eventKind: request.Event.Kind,
	}
	service, err := NewService(Dependencies{Lane: NewOperationLane(), Repository: repository, Authorization: authorizationService, Admission: passthroughAdmission{}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.RunControl(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || !authorizationService.observed.Load() {
		t.Fatalf("result=%+v mutation observed in callback=%v", result, authorizationService.observed.Load())
	}
}

func TestRunControlCancelledDispatchTerminalizesWithoutConsequentialEvent(t *testing.T) {
	request := validControlRequestForReview(t)
	repository := &recordingRepository{head: request.ExpectedHead}
	service, err := NewService(Dependencies{
		Lane: NewOperationLane(), Repository: repository,
		Authorization: &cancelDispatchAuthorization{allowingAuthorization: allowingAuthorization{log: &recordLog{}}},
		Admission:     passthroughAdmission{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunControl(context.Background(), request); !errors.Is(err, context.Canceled) {
		t.Fatalf("run error=%v", err)
	}
	kinds := flattenAppendKinds(repository.appendRequests())
	if slices.Contains(kinds, request.Event.Kind) {
		t.Fatalf("consequential event committed after cancelled dispatch: %v", kinds)
	}
	if !slices.Contains(kinds, protocol.EventControlOperationInterrupted) || !slices.Contains(kinds, protocol.EventCommandCompleted) {
		t.Fatalf("cancelled control was not durably terminalized: %v", kinds)
	}
	validateAppendRequests(t, repository.appendRequests())
}

func TestControlKindRequiresItsExactConsequentialEvent(t *testing.T) {
	base := validControlRequestForReview(t)
	diagnostic := protocol.ProposedEvent{
		EventID: "event-diagnostic", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventMigrationDiagnostic,
		Actor: base.Event.Actor, RuntimeGenerationID: base.Runtime.ID,
		Payload: mustCanonical(protocol.DiagnosticV1{Diagnostic: protocol.Diagnostic{Code: "control.requested", Message: "requested", Journal: base.Journal}}),
	}
	for _, test := range []struct {
		name     string
		kind     OperationKind
		recovery bool
		event    protocol.ProposedEvent
	}{
		{name: "generic control", kind: OperationControl, event: base.Event},
		{name: "compaction", kind: OperationCompaction, event: base.Event},
		{name: "recovery", kind: OperationRecovery, recovery: true, event: diagnostic},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := base
			request.Kind = test.kind
			request.Event = test.event
			if err := validateControlRequest(request, test.recovery); err == nil {
				t.Fatalf("kind %s accepted event %s", test.kind, test.event.Kind)
			}
		})
	}
}

func TestControlDiagnosticAdmissionRedactsBeforeDurableAppend(t *testing.T) {
	const sentinel = "diagnostic-secret"
	request := validControlRequestForReview(t)
	request.Kind = OperationControl
	request.Event.Kind = protocol.EventMigrationDiagnostic
	request.Event.Payload = mustCanonical(protocol.DiagnosticV1{Diagnostic: protocol.Diagnostic{
		Code: "migration.requested", Message: "contains " + sentinel, Journal: request.Journal,
		Details: json.RawMessage(`{"token":"diagnostic-secret"}`),
	}})
	repository := &recordingRepository{head: request.ExpectedHead}
	service, err := NewService(Dependencies{
		Lane: NewOperationLane(), Repository: repository, Admission: newRedactingAdmission(sentinel),
		Authorization: &allowingAuthorization{log: &recordLog{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunControl(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	for _, appendRequest := range repository.appendRequests() {
		for _, event := range appendRequest.Events {
			if strings.Contains(string(event.Payload), sentinel) {
				t.Fatalf("secret persisted in %s: %s", event.Kind, event.Payload)
			}
		}
	}
}

func TestEveryControlAuthorizationAndDispatchBarrierLeavesOneDurableTerminal(t *testing.T) {
	for _, barrier := range []Barrier{BarrierAuthorizationCommitted, BarrierEffectDispatch} {
		for _, phase := range []string{"before", "after"} {
			t.Run(string(barrier)+"_"+phase, func(t *testing.T) {
				request := validControlRequestForReview(t)
				repository := &recordingRepository{head: request.ExpectedHead}
				service, err := NewService(Dependencies{
					Lane: NewOperationLane(), Repository: repository, Admission: passthroughAdmission{},
					Authorization: &allowingAuthorization{log: &recordLog{}}, BarrierProbe: phaseBarrierProbe{barrier: barrier, phase: phase},
				})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := service.RunControl(context.Background(), request); !errors.Is(err, errInjectedBarrier) {
					t.Fatalf("control error=%v", err)
				}
				kinds := flattenAppendKinds(repository.appendRequests())
				terminals, commandTerminals := 0, 0
				for _, kind := range kinds {
					if kind == protocol.EventControlOperationCompleted || kind == protocol.EventControlOperationFailed || kind == protocol.EventControlOperationInterrupted {
						terminals++
					}
					if kind == protocol.EventCommandCompleted {
						commandTerminals++
					}
				}
				if terminals != 1 || commandTerminals != 1 {
					t.Fatalf("terminal lifecycle=%v", kinds)
				}
				if slices.Contains(kinds, request.Event.Kind) != (barrier == BarrierEffectDispatch && phase == "after") {
					t.Fatalf("consequential event placement=%v", kinds)
				}
				validateAppendRequests(t, repository.appendRequests())
			})
		}
	}
}

func TestControlAuthorizationDenialCommitsDecisionAndDeniedCommand(t *testing.T) {
	request := validControlRequestForReview(t)
	repository := &recordingRepository{head: request.ExpectedHead}
	service, err := NewService(Dependencies{
		Lane: NewOperationLane(), Repository: repository, Admission: passthroughAdmission{},
		Authorization: &denyingAuthorization{allowingAuthorization: allowingAuthorization{log: &recordLog{}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.RunControl(context.Background(), request)
	if err == nil {
		t.Fatal("control denial was ignored")
	}
	if result.Status != "denied" || result.CommandResult.Status != "denied" || result.Error == nil || result.Error.Code != "authorization_denied" {
		t.Fatalf("result=%+v", result)
	}
	kinds := flattenAppendKinds(repository.appendRequests())
	for _, required := range []string{protocol.EventAuthorizationDecided, protocol.EventControlOperationFailed, protocol.EventCommandCompleted} {
		if !slices.Contains(kinds, required) {
			t.Fatalf("missing %s in %v", required, kinds)
		}
	}
	for _, forbidden := range []string{protocol.EventControlOperationAuthorized, protocol.EventControlOperationStarted, request.Event.Kind} {
		if slices.Contains(kinds, forbidden) {
			t.Fatalf("denied control committed %s: %v", forbidden, kinds)
		}
	}
	validateAppendRequests(t, repository.appendRequests())
}

func TestUnknownToolSyntheticResultHasDurableLifecycle(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	repository := &recordingRepository{head: request.ExpectedHead}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repository,
		TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: &recordLog{}}, Providers: fakeProviderCatalog{log: &recordLog{}},
		Provider: &unknownToolThenFinalProvider{}, Tools: noToolService{}, Authorization: &allowingAuthorization{log: &recordLog{}},
		Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	assertAcceptedTurnHasNoOrphans(t, repository.appendRequests())
	kinds := flattenAppendKinds(repository.appendRequests())
	if !slices.Contains(kinds, protocol.EventActivityPlanned) || !slices.Contains(kinds, protocol.EventActivityFailed) {
		t.Fatalf("unknown tool lifecycle=%v", kinds)
	}
}

func TestObservationDriftSyntheticResultHasDurableLifecycle(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	repository := &recordingRepository{head: request.ExpectedHead}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repository,
		TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: &recordLog{}}, Providers: fakeProviderCatalog{log: &recordLog{}},
		Provider: &toolThenFinalProvider{log: &recordLog{}}, Tools: driftingObservationTool{}, Authorization: &allowingAuthorization{log: &recordLog{}},
		Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	assertAcceptedTurnHasNoOrphans(t, repository.appendRequests())
	kinds := flattenAppendKinds(repository.appendRequests())
	if !slices.Contains(kinds, protocol.EventActivityFailed) || countKind(kinds, protocol.EventActivityPlanned) < 3 {
		t.Fatalf("observation drift lifecycle=%v", kinds)
	}
}

func TestProviderProvedZeroByteRetryUsesFreshAuthorizedActivity(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	repository := &recordingRepository{head: request.ExpectedHead}
	providerService := &zeroByteThenFinalProvider{}
	authorizationService := &allowingAuthorization{log: &recordLog{}}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repository,
		TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: &recordLog{}}, Providers: fakeProviderCatalog{log: &recordLog{}},
		Provider: providerService, Tools: noToolService{}, Authorization: authorizationService,
		Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	kinds := flattenAppendKinds(repository.appendRequests())
	if providerService.streams.Load() != 2 || countKind(kinds, protocol.EventActivityPlanned) != 2 ||
		countKind(kinds, protocol.EventActivityInterruptedNoEffect) != 1 || authorizationService.nonce.Load() != 2 {
		t.Fatalf("streams=%d decisions=%d lifecycle=%v", providerService.streams.Load(), authorizationService.nonce.Load(), kinds)
	}
	assertAcceptedTurnHasNoOrphans(t, repository.appendRequests())
}

type unknownToolThenFinalProvider struct{ streams atomic.Int64 }

func (p *unknownToolThenFinalProvider) Prepare(context.Context, protocol.ActivityID, string, protocol.ModelRequest, protocol.Digest) (provider.ProviderHandle, error) {
	return provider.ProviderHandle{}, nil
}

func (p *unknownToolThenFinalProvider) Stream(context.Context, provider.ProviderHandle, authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	stream := make(chan protocol.ModelEvent, 2)
	if p.streams.Add(1) == 1 {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventToolIntent, Sequence: 1, ToolIntent: &protocol.ToolUseBlock{CallID: "call-unknown", Alias: "missing", Arguments: json.RawMessage(`{}`)}}
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "tool_use"}}
	} else {
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: "done"}}
		stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	}
	close(stream)
	return stream, nil
}

type driftingObservationTool struct{ noToolService }

func (driftingObservationTool) Plan(_ context.Context, request tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error) {
	return tooling.ActionHandle{}, testActionPlan(request, "observation", "not_applicable"), nil
}

func (driftingObservationTool) Revalidate(context.Context, tooling.ActionHandle) (protocol.ActionPlan, bool, error) {
	return protocol.ActionPlan{}, true, nil
}

type zeroByteThenFinalProvider struct{ streams atomic.Int64 }

func (p *zeroByteThenFinalProvider) Prepare(context.Context, protocol.ActivityID, string, protocol.ModelRequest, protocol.Digest) (provider.ProviderHandle, error) {
	return provider.ProviderHandle{}, nil
}

func (p *zeroByteThenFinalProvider) Stream(context.Context, provider.ProviderHandle, authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	if p.streams.Add(1) == 1 {
		return nil, provedZeroByteError{}
	}
	stream := make(chan protocol.ModelEvent, 2)
	stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: "done"}}
	stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	close(stream)
	return stream, nil
}

type provedZeroByteError struct{}

func (provedZeroByteError) Error() string       { return "proved zero-byte provider start" }
func (provedZeroByteError) Retryable() bool     { return true }
func (provedZeroByteError) ZeroBytesSent() bool { return true }

func validControlRequestForReview(t *testing.T) ControlRequest {
	t.Helper()
	runtime := validRuntimeManifest(t, "observation")
	ref := protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace-control-a"}
	head := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 1, TransactionID: "head-a"}
	plan := testActionPlan(tooling.PlanRequest{CallID: "reload-a", Alias: "reload", RuntimeGenerationID: runtime.ID}, "mutation", "not_reversible")
	actor := protocol.ActorRef{ID: "user-a", Kind: protocol.ActorUser}
	return ControlRequest{
		Command:     CommandMetadata{CommandID: "control-command-a", IdempotencyKey: "control-key-a", RequestDigest: repeatedDigest("7"), Actor: actor},
		OperationID: "reload-operation-a", Kind: OperationReloadActivation, Journal: ref, ExpectedHead: head,
		TransactionID: "control-terminal-a", Runtime: runtime, Plan: plan,
		Event: protocol.ProposedEvent{EventID: "event-runtime", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventRuntimeGenerationActivated, Actor: &actor, RuntimeGenerationID: runtime.ID, Payload: mustCanonical(protocol.RuntimeGenerationActivatedV1{Manifest: runtime})},
	}
}

type inspectingDispatchAuthorization struct {
	allowingAuthorization
	repository *recordingRepository
	eventKind  string
	observed   atomic.Bool
}

func (a *inspectingDispatchAuthorization) Dispatch(ctx context.Context, _ authorization.CommittedToken, _ authorization.DispatchBinding, callback func(context.Context) error) error {
	if err := callback(ctx); err != nil {
		return err
	}
	if slices.Contains(flattenAppendKinds(a.repository.appendRequests()), a.eventKind) {
		a.observed.Store(true)
		return nil
	}
	return errors.New("consequential mutation was not committed inside dispatch callback")
}

type cancelDispatchAuthorization struct{ allowingAuthorization }

func (*cancelDispatchAuthorization) Dispatch(context.Context, authorization.CommittedToken, authorization.DispatchBinding, func(context.Context) error) error {
	return context.Canceled
}

func flattenAppendKinds(requests []journal.AppendRequest) []string {
	var kinds []string
	for _, request := range requests {
		for _, event := range request.Events {
			kinds = append(kinds, event.Kind)
		}
	}
	return kinds
}

func countKind(kinds []string, target string) int {
	count := 0
	for _, kind := range kinds {
		if kind == target {
			count++
		}
	}
	return count
}

type staticInstructionService struct{ sources []protocol.ContentSource }

func (s staticInstructionService) SystemInstructions(context.Context, protocol.RuntimeGenerationID, string, protocol.SessionID) ([]protocol.ContentSource, error) {
	return protocol.DeepCopy(s.sources), nil
}

type capturingContextPlanner struct {
	mu       sync.Mutex
	delegate fakeContextPlanner
	request  contextplanner.Request
}

func (p *capturingContextPlanner) Plan(ctx context.Context, request contextplanner.Request) (protocol.ContextPlan, error) {
	p.mu.Lock()
	p.request = protocol.DeepCopy(request)
	p.mu.Unlock()
	return p.delegate.Plan(ctx, request)
}

func (p *capturingContextPlanner) last() contextplanner.Request {
	p.mu.Lock()
	defer p.mu.Unlock()
	return protocol.DeepCopy(p.request)
}

type verificationServiceForReview struct{}

func (verificationServiceForReview) Assess(ctx context.Context, request verification.Request) (verification.Result, error) {
	return verification.NewService(time.Now).Assess(ctx, request)
}

type redactingAdmission struct{ redactor secret.Redactor }

func newRedactingAdmission(values ...string) redactingAdmission {
	return redactingAdmission{redactor: secret.New(values...)}
}

func (a redactingAdmission) SanitizeText(_ context.Context, _ protocol.RuntimeGenerationID, value string) (string, error) {
	return a.redactor.String(value), nil
}

func (a redactingAdmission) SanitizeJSON(_ context.Context, _ protocol.RuntimeGenerationID, value json.RawMessage) (json.RawMessage, error) {
	var decoded any
	if err := json.Unmarshal(value, &decoded); err != nil {
		return nil, err
	}
	return a.redactor.JSON(decoded)
}

func (a redactingAdmission) OpenTextStream(context.Context, protocol.RuntimeGenerationID) (StreamingSanitizer, error) {
	return redactingStream{stream: a.redactor.Stream()}, nil
}

type redactingStream struct{ stream *secret.Stream }

func (s redactingStream) Write(value string) (string, error) { return s.stream.Write(value), nil }
func (s redactingStream) Close() (string, error)             { return s.stream.Close(), nil }

func TestSkillResultPresentationIsSanitizedAndTransientOnly(t *testing.T) {
	publisher := &capturingTransientPublisher{}
	service := &Service{
		deps:      Dependencies{Admission: newRedactingAdmission("presentation-secret")},
		publisher: publisher,
	}
	request := StartTurnRequest{SessionID: "session-1", Runtime: validRuntimeManifest(t, "observation")}
	state := &turnState{turnID: "turn-1"}
	plan := protocol.ActionPlan{Body: protocol.ActionPlanBody{Tool: protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "skill"}}}
	execution := protocol.ExecutionResult{
		ToolResult:   protocol.ToolResultBlock{CallID: "skill-call", Status: "succeeded", Text: "durable-secret"},
		Presentation: protocol.ToolResultPresentation{Content: "presentation-secret", DurationNanos: int64(time.Second), Truncated: true},
	}
	if err := service.publishToolPresentation(context.Background(), request, state, "activity-1", plan, execution); err != nil {
		t.Fatal(err)
	}
	if publisher.committed != 0 || len(publisher.transient) != 1 {
		t.Fatalf("publishes=%+v", publisher)
	}
	event := publisher.transient[0]
	if event.Kind != protocol.EventToolResultAvailable || event.Correlation.ActivityID != "activity-1" {
		t.Fatalf("event=%+v", event)
	}
	var available protocol.ToolResultAvailableV1
	if err := json.Unmarshal(event.Payload, &available); err != nil {
		t.Fatal(err)
	}
	if available.Content == "presentation-secret" || strings.Contains(available.Content, "presentation-secret") || available.Content == "" || available.DurationNanos != int64(time.Second) || !available.Truncated {
		t.Fatalf("unsanitized or incomplete presentation: %+v", available)
	}
}

type capturingTransientPublisher struct {
	committed int
	transient []protocol.ApplicationEvent
}

func (p *capturingTransientPublisher) PublishCommitted(context.Context, protocol.JournalRef, protocol.CommittedCursor, []protocol.EventEnvelope) error {
	p.committed++
	return nil
}

func (p *capturingTransientPublisher) PublishTransient(event protocol.ApplicationEvent) error {
	p.transient = append(p.transient, protocol.DeepCopy(event))
	return nil
}

type splitSecretProvider struct{}

func (splitSecretProvider) Prepare(context.Context, protocol.ActivityID, string, protocol.ModelRequest, protocol.Digest) (provider.ProviderHandle, error) {
	return provider.ProviderHandle{}, nil
}

func (splitSecretProvider) Stream(context.Context, provider.ProviderHandle, authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	stream := make(chan protocol.ModelEvent, 3)
	stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentDelta, Sequence: 1, Delta: &protocol.ContentDelta{BlockID: "text-a", Kind: protocol.ContentText, Text: "prefix split-"}}
	stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentDelta, Sequence: 2, Delta: &protocol.ContentDelta{BlockID: "text-a", Kind: protocol.ContentText, Text: "secret suffix"}}
	stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 3, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	close(stream)
	return stream, nil
}

type interleavedProvider struct{}

func (interleavedProvider) Prepare(context.Context, protocol.ActivityID, string, protocol.ModelRequest, protocol.Digest) (provider.ProviderHandle, error) {
	return provider.ProviderHandle{}, nil
}

func (interleavedProvider) Stream(context.Context, provider.ProviderHandle, authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	stream := make(chan protocol.ModelEvent, 4)
	stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentDelta, Sequence: 1, Delta: &protocol.ContentDelta{BlockID: "text-a", Kind: protocol.ContentText, Text: "first"}}
	stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 2, Block: &protocol.ContentBlock{Kind: protocol.ContentJSON, JSON: json.RawMessage(`{"middle":true}`)}}
	stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentDelta, Sequence: 3, Delta: &protocol.ContentDelta{BlockID: "text-b", Kind: protocol.ContentText, Text: "last"}}
	stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 4, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	close(stream)
	return stream, nil
}

type pagedCommandRepository struct {
	inertRepository
	mu    sync.Mutex
	pages []journal.EventPage
	after []protocol.CommittedCursor
	calls atomic.Int64
}

func (r *pagedCommandRepository) ReadRange(_ context.Context, request journal.ReadRangeRequest) (journal.EventPage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.after = append(r.after, request.After)
	index := int(r.calls.Add(1) - 1)
	return protocol.DeepCopy(r.pages[index]), nil
}
