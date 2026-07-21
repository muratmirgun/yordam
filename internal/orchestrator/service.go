package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	contextplanner "github.com/muratmirgun/yordam/internal/context"
	"github.com/muratmirgun/yordam/internal/eventcodec"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/recovery"
	"github.com/muratmirgun/yordam/internal/tooling"
	toolset "github.com/muratmirgun/yordam/internal/tools"
	edittool "github.com/muratmirgun/yordam/internal/tools/edit"
	"github.com/muratmirgun/yordam/internal/verification"
)

var ErrIdempotencyConflict = errors.New("idempotency_conflict")

var ErrCommitUncertain = errors.New("journal commit outcome is uncertain")

type CommitUncertainError struct {
	Journal       protocol.JournalRef
	TransactionID protocol.TransactionID
	Cause         error
}

func (e *CommitUncertainError) Error() string {
	return fmt.Sprintf("%s for transaction %q in %s/%s", ErrCommitUncertain, e.TransactionID, e.Journal.Kind, e.Journal.ID)
}

func (e *CommitUncertainError) Unwrap() []error {
	if e.Cause == nil {
		return []error{ErrCommitUncertain}
	}
	return []error{ErrCommitUncertain, e.Cause}
}
func (e *CommitUncertainError) UncertainCommit() {}

type Service struct {
	lane       OperationLane
	repository journal.Repository
	turnLeases journal.TurnLeaseManager
	deps       Dependencies
	publisher  ApplicationEventPublisher
	probe      BarrierProbe
}

// SetChildCoordinator is used only while wiring an immutable runtime before it
// is exposed to callers. Keeping the coordinator injected preserves small test
// seams while production uses SequentialChildCoordinator.
func (s *Service) SetChildCoordinator(coordinator ChildCoordinator) {
	s.deps.Children = coordinator
}

func NewService(dependencies Dependencies) (*Service, error) {
	if dependencies.Repository == nil {
		return nil, fmt.Errorf("journal repository is required")
	}
	if dependencies.Lane == nil {
		return nil, fmt.Errorf("application operation lane is required")
	}
	probe := dependencies.BarrierProbe
	if probe == nil {
		probe = NoopBarrierProbe()
	}
	return &Service{
		lane: dependencies.Lane, repository: dependencies.Repository, turnLeases: dependencies.TurnLeases,
		deps: dependencies, publisher: dependencies.Publisher, probe: probe,
	}, nil
}

func (s *Service) RunTurn(ctx context.Context, request StartTurnRequest) (result RunResult, runErr error) {
	if err := validateStartTurnRequest(request); err != nil {
		return RunResult{}, err
	}
	if s.deps.Admission == nil || s.deps.Instructions == nil {
		return RunResult{}, fmt.Errorf("generation admission and instruction services are required")
	}
	admittedPrompt, err := s.deps.Admission.SanitizeText(ctx, request.Runtime.ID, request.Prompt)
	if err != nil {
		return RunResult{}, err
	}
	request.Prompt = admittedPrompt
	lease, err := acquireManagedOperationLease(ctx, s.lane, OperationClaim{Kind: OperationTurn, SessionID: request.SessionID})
	if err != nil {
		return RunResult{}, err
	}
	defer lease.Release()
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)}
	if durable, ok, lookupErr := s.LookupCommand(ctx, ref, request.Command.CommandID, request.Command.RequestDigest); lookupErr != nil {
		return RunResult{}, lookupErr
	} else if ok {
		return RunResult{
			TaskID: protocol.TaskID(stableID("task", string(request.Command.CommandID))), TurnID: protocol.TurnID(stableID("turn", string(request.Command.CommandID))),
			Cursor: commandResultCursor(durable), Status: durable.Status, CommandResult: durable,
		}, nil
	}
	if s.turnLeases == nil {
		return RunResult{}, fmt.Errorf("turn lease manager is required")
	}

	state := newTurnState(request)
	turnLease, err := s.turnLeases.AcquireTurnLease(ctx, request.SessionID, state.turnID, request.ExpectedHead)
	if err != nil {
		return RunResult{}, err
	}
	defer func() { _ = turnLease.Release(context.WithoutCancel(ctx), state.head) }()
	defer func() {
		if runErr == nil || !state.accepted || state.terminal {
			return
		}
		terminalResult, terminalErr := s.terminalizeTurnFailure(context.WithoutCancel(ctx), request, &state, runErr)
		if terminalErr != nil {
			runErr = errors.Join(runErr, terminalErr)
			return
		}
		result = terminalResult
	}()

	if request.child != nil {
		manifestEvents, manifestErr := s.turnEvents(state, request.Runtime.ID, "subagent-manifest", []struct {
			kind    string
			payload any
		}{{protocol.EventSubagentManifest, request.child.manifest}})
		if manifestErr != nil {
			return RunResult{}, manifestErr
		}
		if manifestErr = s.append(ctx, &state, "subagent-manifest", manifestEvents); manifestErr != nil {
			return RunResult{}, manifestErr
		}
	}
	initial, err := s.initialTurnEvents(request, state)
	if err != nil {
		return RunResult{}, err
	}
	acceptedHead := state.head
	if err := s.append(ctx, &state, "turn-accepted", initial); err != nil {
		state.accepted = state.head != acceptedHead
		return RunResult{}, err
	}
	state.accepted = true
	if err := s.cross(ctx, BarrierCommandAccepted, state.barrierState()); err != nil {
		return RunResult{}, err
	}
	if err := s.cross(ctx, BarrierGoalDraftCommitted, state.barrierState()); err != nil {
		return RunResult{}, err
	}

	freeze, err := s.contractFreezeEvents(request, state)
	if err != nil {
		return RunResult{}, err
	}
	freezeHead := state.head
	if err := s.append(ctx, &state, "contract-frozen", freeze); err != nil {
		state.taskRunning = state.head != freezeHead
		return RunResult{}, err
	}
	state.taskRunning = true
	if err := s.cross(ctx, BarrierContractFrozen, state.barrierState()); err != nil {
		return RunResult{}, err
	}
	if s.deps.Context == nil || s.deps.Providers == nil || s.deps.Provider == nil || s.deps.Authorization == nil || s.deps.Verification == nil {
		return RunResult{TaskID: state.taskID, TurnID: state.turnID, Cursor: state.head, Status: "accepted"}, fmt.Errorf("turn orchestration dependencies are incomplete")
	}

	completedTools := 0
	zeroByteRetries := 0
	for attempt := 0; ; attempt++ {
		assistant, terminal, err := s.runProviderActivity(ctx, lease, request, &state, attempt)
		if err != nil {
			var proved ProvenZeroByteProviderError
			if errors.As(err, &proved) && proved.Retryable() && proved.ZeroBytesSent() && zeroByteRetries == 0 && state.activeActivityID != "" {
				activityID := state.activeActivityID
				if terminalErr := s.appendActivityEvidence(context.WithoutCancel(ctx), request, &state, activityID, fmt.Sprintf("provider-%d-zero-byte-terminal", attempt), "interrupted_no_effect", nil, nil); terminalErr != nil {
					return RunResult{}, errors.Join(err, terminalErr)
				}
				zeroByteRetries++
				continue
			}
			return RunResult{}, err
		}
		if len(assistant.ToolIntents) == 0 {
			return s.completeTurn(ctx, request, &state, assistant, terminal)
		}
		maxTools := request.Runtime.Body.Limits.MaxToolCalls
		if request.child != nil {
			maxTools = request.child.manifest.MaxToolCalls
		}
		if completedTools+len(assistant.ToolIntents) > maxTools {
			return RunResult{}, fmt.Errorf("tool call limit %d reached", maxTools)
		}
		if s.deps.Tools == nil || s.deps.Evidence == nil || s.deps.Recovery == nil {
			return RunResult{}, fmt.Errorf("tool orchestration dependencies are incomplete")
		}
		for _, intent := range assistant.ToolIntents {
			_, runErr := s.runToolIntent(ctx, lease, request, &state, intent)
			if runErr != nil {
				return RunResult{}, runErr
			}
			// A cancelled tool may still return a durable uncertain/no-effect
			// result after its detached terminal write. Do not mistake that receipt
			// for authority to start another provider activity.
			if err := ctx.Err(); err != nil {
				return RunResult{}, err
			}
			completedTools++
		}
	}
}

func commandResultCursor(result protocol.CommandResult) protocol.CommittedCursor {
	if result.Cursor.SelectedSession != nil {
		return *result.Cursor.SelectedSession
	}
	return result.Cursor.WorkspaceControl
}

func (s *Service) runProviderActivity(ctx context.Context, lease managedOperationLease, request StartTurnRequest, state *turnState, attempt int) (protocol.AssistantMessageV1, protocol.ProviderAttemptTerminalV1, error) {
	model, ok := selectedRuntimeModel(request)
	if !ok {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, fmt.Errorf("selected model %q/%q is not in runtime generation %q", request.ProviderID, request.ModelID, request.Runtime.ID)
	}
	requirements := []protocol.CapabilityRequirement{}
	instructions, err := s.deps.Instructions.SystemInstructions(ctx, request.Runtime.ID, request.Runtime.Body.InstructionRevision, request.SessionID)
	if err != nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	for index, source := range instructions {
		for blockIndex, block := range source.Content {
			admitted, admitErr := s.sanitizeContentBlock(ctx, request.Runtime.ID, block)
			if admitErr != nil {
				return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, admitErr
			}
			source.Content[blockIndex] = admitted
		}
		source.Digest, err = canonicaljson.Digest(source.Content)
		if err != nil {
			return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
		}
		instructions[index] = source
		if err := source.Validate(); err != nil {
			return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, fmt.Errorf("system instruction source: %w", err)
		}
	}
	var contextPlan protocol.ContextPlan
	for rebuild := 0; ; rebuild++ {
		history, historyErr := s.readFullHistory(ctx, state.ref)
		if historyErr != nil {
			return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, historyErr
		}
		contextPlan, err = s.deps.Context.Plan(ctx, contextplanner.Request{
			Session: request.SessionID, TaskID: state.taskID, OutcomeContractID: state.contractID, OutcomeContractVersion: 2,
			Events: history, SystemInstructions: instructions, Model: model,
		})
		if err != nil {
			return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
		}
		if rebuild != 0 {
			break
		}
		decision, policyErr := automaticCompactionDecision(contextPlan, request.Runtime.Body.Limits)
		if policyErr != nil {
			return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, policyErr
		}
		if !decision.ShouldCompact {
			break
		}
		compacted, compactErr := s.compactWithinTurn(ctx, lease, request, state, state.head, history)
		if compactErr != nil {
			return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, compactErr
		}
		if !compacted {
			break
		}
	}
	state.contextPlanDigest = contextPlan.Digest
	exposure := effectiveToolExposure(request)
	plan, err := s.deps.Providers.Negotiate(model.ProviderID, model.ModelID, requirements, exposure.CatalogRevision)
	if err != nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	messages := contextMessages(contextPlan)
	modelRequest := protocol.ModelRequest{
		RequestID:  stableID("provider-request", string(request.Command.CommandID), fmt.Sprint(attempt)),
		ProviderID: model.ProviderID, ModelID: model.ModelID, Messages: messages,
		Tools: exposure, Requirements: requirements, Plan: plan,
	}
	activityID := protocol.ActivityID(stableID("activity", string(request.Command.CommandID), "provider", fmt.Sprint(attempt)))
	callID := stableID("provider-call", string(request.Command.CommandID), fmt.Sprint(attempt))
	handle, err := s.deps.Provider.Prepare(ctx, activityID, callID, modelRequest, contextPlan.Digest)
	if err != nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	authorizationRequest, err := providerAuthorizationRequest(request, state, activityID, callID, modelRequest, contextPlan.Digest)
	if err != nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	label := fmt.Sprintf("provider-%d", attempt)
	planned := []struct {
		kind    string
		payload any
	}{
		{protocol.EventContextPlanRecorded, protocol.ContextPlanRecordedV1{Plan: contextPlan}},
		{protocol.EventProviderCapabilityDecided, protocol.ProviderCapabilityDecidedV1{Plan: plan, Status: "accepted"}},
		{protocol.EventActivityPlanned, protocol.ActivityPlannedV1{Kind: "provider", Purpose: "continue task", PurposeActor: protocol.ActorRef{ID: "orchestrator", Kind: protocol.ActorAgent}, Source: "provider", RequestedProfile: "network", EffectiveProfile: "network"}},
		{protocol.EventAuthorizationRequested, protocol.AuthorizationRequestedV1{Request: authorizationRequest}},
	}
	events, err := s.activityEvents(*state, request.Runtime.ID, activityID, label+"-planned", planned)
	if err != nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	plannedHead := state.head
	if err := s.append(ctx, state, label+"-planned", events); err != nil {
		if state.head != plannedHead {
			state.activeActivityID, state.activeStarted, state.activeDispatched = activityID, false, false
			state.activeRequiresToolResult = false
		}
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	state.activeActivityID, state.activeStarted, state.activeDispatched = activityID, false, false
	state.activeRequiresToolResult = false
	token, err := s.authorizeActivity(ctx, lease, request, state, activityID, callID, label, authorizationRequest)
	if err != nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	barrierState := state.barrierState()
	barrierState.ActivityID = activityID
	if err := s.cross(ctx, BarrierProviderAuthorizationCommitted, barrierState); err != nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	if attempt > 0 {
		if err := s.cross(ctx, BarrierProviderContinuation, barrierState); err != nil {
			return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
		}
	}
	if err := s.probe.Before(ctx, BarrierEffectDispatch, barrierState); err != nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	state.activeDispatched = true
	stream, err := s.deps.Provider.Stream(ctx, handle, token)
	if err != nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	if err := s.probe.After(ctx, BarrierEffectDispatch, barrierState); err != nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	message, terminal, err := s.collectProviderStream(ctx, request.Runtime.ID, stream)
	if err != nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	terminalEvents := []struct {
		kind    string
		payload any
	}{
		{protocol.EventAssistantMessage, message},
		{protocol.EventProviderAttemptTerminal, terminal},
		{protocol.EventActivitySucceeded, protocol.ActivityOutcomeV1{Status: "succeeded"}},
	}
	events, err = s.activityEvents(*state, request.Runtime.ID, activityID, label+"-terminal", terminalEvents)
	if err != nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	terminalHead := state.head
	if err := s.append(ctx, state, label+"-terminal", events); err != nil {
		if state.head != terminalHead {
			state.activeActivityID, state.activeStarted, state.activeDispatched = "", false, false
			state.activeRequiresToolResult = false
		}
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	state.activeActivityID, state.activeStarted, state.activeDispatched = "", false, false
	state.activeRequiresToolResult = false
	return message, terminal, nil
}

func (s *Service) readFullHistory(ctx context.Context, ref protocol.JournalRef) ([]protocol.EventRecord, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	after := protocol.CommittedCursor{}
	var projectionHead protocol.CommittedCursor
	events := make([]protocol.EventRecord, 0)
	for {
		page, err := s.repository.ReadRange(ctx, journal.ReadRangeRequest{Journal: ref, After: after, Limit: 1000})
		if err != nil {
			return nil, err
		}
		if projectionHead == (protocol.CommittedCursor{}) {
			projectionHead = page.Head
		} else if page.Head != projectionHead {
			return nil, fmt.Errorf("context projection head changed during read")
		}
		events = append(events, protocol.DeepCopy(page.Events)...)
		if !page.More {
			return events, nil
		}
		if page.Cursor == after || page.Cursor == (protocol.CommittedCursor{}) {
			return nil, fmt.Errorf("context history pagination did not advance")
		}
		after = page.Cursor
	}
}

func (s *Service) runToolIntent(ctx context.Context, lease managedOperationLease, request StartTurnRequest, state *turnState, intent protocol.ToolUseBlock) (protocol.ToolResultBlock, error) {
	descriptor, ok := toolDescriptor(request.Runtime, intent.Alias)
	if !ok {
		if err := s.appendSyntheticToolFailure(ctx, request, state, intent, nil, "unknown tool"); err != nil {
			return protocol.ToolResultBlock{}, err
		}
		return protocol.ToolResultBlock{CallID: intent.CallID, Status: "failed", Text: "unknown tool"}, nil
	}
	if canonicalSubagentDescriptor(descriptor) {
		return s.runSubagentIntent(ctx, lease, request, state, intent)
	}
	mutating := descriptor.Body.Effect != "observation"
	if !mutating {
		return s.runObservationIntent(ctx, lease, request, state, intent)
	}
	for round := 0; round < 3; round++ {
		previewActivityID := protocol.ActivityID(stableID("activity", string(request.Command.CommandID), "preview", intent.CallID, fmt.Sprint(round)))
		previewRequest := tooling.PlanRequest{
			TurnID: state.turnID, ActivityID: previewActivityID, CallID: intent.CallID, Alias: intent.Alias,
			Arguments: protocol.DeepCopy(intent.Arguments), RuntimeGenerationID: request.Runtime.ID,
		}
		previewHandle, previewPlan, err := s.deps.Tools.PlanPreviewInspection(ctx, previewRequest)
		if err != nil {
			return protocol.ToolResultBlock{}, err
		}
		previewAuthorization, err := toolAuthorizationRequest(request, state, previewRequest, previewPlan, nil)
		if err != nil {
			return protocol.ToolResultBlock{}, err
		}
		previewLabel := fmt.Sprintf("tool-%s-preview-%d", intent.CallID, round)
		planned := []struct {
			kind    string
			payload any
		}{
			{protocol.EventActivityPlanned, activityPlan(previewPlan, "tool preview", nil)},
			{protocol.EventExecutionPlanDeclared, protocol.ExecutionPlanDeclaredV1{Plan: previewPlan}},
			{protocol.EventAuthorizationRequested, protocol.AuthorizationRequestedV1{Request: previewAuthorization}},
		}
		events, err := s.activityEvents(*state, request.Runtime.ID, previewActivityID, previewLabel+"-planned", planned)
		if err != nil {
			return protocol.ToolResultBlock{}, err
		}
		plannedHead := state.head
		if err := s.append(ctx, state, previewLabel+"-planned", events); err != nil {
			if state.head != plannedHead {
				state.activeActivityID, state.activeStarted, state.activeDispatched = previewActivityID, false, false
				state.activeRequiresToolResult = false
			}
			return protocol.ToolResultBlock{}, err
		}
		state.activeActivityID, state.activeStarted, state.activeDispatched = previewActivityID, false, false
		state.activeRequiresToolResult = false
		barrierState := state.barrierState()
		barrierState.ActivityID, barrierState.PlanDigest = previewActivityID, previewPlan.Digest
		if err := s.cross(ctx, BarrierActionPlanCommitted, barrierState); err != nil {
			return protocol.ToolResultBlock{}, err
		}
		previewToken, err := s.authorizeActivity(ctx, lease, request, state, previewActivityID, intent.CallID, previewLabel, previewAuthorization)
		if err != nil {
			return protocol.ToolResultBlock{}, err
		}
		if err := s.cross(ctx, BarrierAuthorizationCommitted, barrierState); err != nil {
			return protocol.ToolResultBlock{}, err
		}
		if err := s.probe.Before(ctx, BarrierEffectDispatch, barrierState); err != nil {
			return protocol.ToolResultBlock{}, err
		}
		state.activeDispatched = true
		preview, _, candidates, err := s.deps.Tools.PreparePreview(ctx, previewHandle, previewToken)
		if err != nil {
			return protocol.ToolResultBlock{}, err
		}
		if err := s.probe.After(ctx, BarrierEffectDispatch, barrierState); err != nil {
			return protocol.ToolResultBlock{}, err
		}
		previewRecords, err := s.recordEvidence(ctx, request, previewActivityID, previewPlan, candidates)
		if err != nil {
			return protocol.ToolResultBlock{}, err
		}
		if err := s.appendActivityEvidence(ctx, request, state, previewActivityID, previewLabel+"-terminal", "succeeded", nil, previewRecords); err != nil {
			return protocol.ToolResultBlock{}, err
		}

		mutationActivityID := protocol.ActivityID(stableID("activity", string(request.Command.CommandID), "mutation", intent.CallID, fmt.Sprint(round)))
		mutationRequest := previewRequest
		mutationRequest.ActivityID = mutationActivityID
		mutationHandle, mutationPlan, err := s.deps.Tools.PlanMutation(ctx, preview, mutationRequest)
		if err != nil {
			return protocol.ToolResultBlock{}, err
		}
		mutationLabel := fmt.Sprintf("tool-%s-mutation-%d", intent.CallID, round)
		mutationPlanned := []struct {
			kind    string
			payload any
		}{
			{protocol.EventActivityPlanned, activityPlan(mutationPlan, "tool mutation", evidenceIDs(previewRecords))},
			{protocol.EventExecutionPlanDeclared, protocol.ExecutionPlanDeclaredV1{Plan: mutationPlan}},
		}
		events, err = s.activityEvents(*state, request.Runtime.ID, mutationActivityID, mutationLabel+"-planned", mutationPlanned)
		if err != nil {
			return protocol.ToolResultBlock{}, err
		}
		plannedHead = state.head
		if err := s.append(ctx, state, mutationLabel+"-planned", events); err != nil {
			if state.head != plannedHead {
				state.activeActivityID, state.activeStarted, state.activeDispatched = mutationActivityID, false, false
				state.activeRequiresToolResult = false
			}
			return protocol.ToolResultBlock{}, err
		}
		state.activeActivityID, state.activeStarted, state.activeDispatched = mutationActivityID, false, false
		state.activeRequiresToolResult = false
		checkpoint, err := s.prepareCheckpoint(ctx, request, state, mutationActivityID, mutationLabel, preview, mutationPlan, previewRecords)
		if err != nil {
			return protocol.ToolResultBlock{}, err
		}
		_ = checkpoint
		revalidated, changed, err := s.deps.Tools.Revalidate(ctx, mutationHandle)
		if err != nil {
			return protocol.ToolResultBlock{}, err
		}
		if !revalidated.Digest.IsZero() {
			mutationPlan = revalidated
		}
		barrierState = state.barrierState()
		barrierState.ActivityID, barrierState.PlanDigest = mutationActivityID, mutationPlan.Digest
		if err := s.cross(ctx, BarrierResourcesRevalidated, barrierState); err != nil {
			return protocol.ToolResultBlock{}, err
		}
		if changed {
			status := "cancelled"
			var toolResult *protocol.ToolResultBlock
			if round == 2 {
				status = "failed"
				state.activeRequiresToolResult = true
				result, sanitizeErr := s.sanitizeToolResult(ctx, request.Runtime.ID, protocol.ToolResultBlock{CallID: intent.CallID, Status: "failed", Text: "resource drift did not stabilize"})
				if sanitizeErr != nil {
					return protocol.ToolResultBlock{}, sanitizeErr
				}
				toolResult = &result
			}
			if err := s.abandonDriftRound(ctx, request, state, mutationActivityID, mutationLabel, mutationPlan.Digest, status, toolResult); err != nil {
				return protocol.ToolResultBlock{}, err
			}
			if round == 2 {
				return protocol.DeepCopy(*toolResult), nil
			}
			continue
		}
		mutationAuthorization, err := toolAuthorizationRequest(request, state, mutationRequest, mutationPlan, evidenceDigests(previewRecords))
		if err != nil {
			return protocol.ToolResultBlock{}, err
		}
		authEvent := []struct {
			kind    string
			payload any
		}{{protocol.EventAuthorizationRequested, protocol.AuthorizationRequestedV1{Request: mutationAuthorization}}}
		events, err = s.activityEvents(*state, request.Runtime.ID, mutationActivityID, mutationLabel+"-authorization", authEvent)
		if err != nil {
			return protocol.ToolResultBlock{}, err
		}
		if err := s.append(ctx, state, mutationLabel+"-authorization", events); err != nil {
			return protocol.ToolResultBlock{}, err
		}
		token, err := s.authorizeActivity(ctx, lease, request, state, mutationActivityID, intent.CallID, mutationLabel, mutationAuthorization)
		if err != nil {
			return protocol.ToolResultBlock{}, err
		}
		if err := s.cross(ctx, BarrierAuthorizationCommitted, barrierState); err != nil {
			return protocol.ToolResultBlock{}, err
		}
		if err := s.probe.Before(ctx, BarrierEffectDispatch, barrierState); err != nil {
			return protocol.ToolResultBlock{}, err
		}
		state.activeDispatched = true
		execution, executeErr := s.deps.Tools.Execute(ctx, mutationHandle, token)
		if executeErr != nil {
			state.activeRequiresToolResult = true
			status := "uncertain"
			if s.deps.EffectProbe != nil {
				if noEffect, probeErr := s.deps.EffectProbe.ProvesNoEffect(ctx, mutationActivityID); probeErr == nil && noEffect {
					status = "interrupted_no_effect"
				}
			}
			result, sanitizeErr := s.sanitizeToolResult(context.WithoutCancel(ctx), request.Runtime.ID, protocol.ToolResultBlock{CallID: intent.CallID, Status: status, Text: executeErr.Error()})
			if sanitizeErr != nil {
				return protocol.ToolResultBlock{}, errors.Join(executeErr, sanitizeErr)
			}
			if appendErr := s.appendActivityEvidence(context.WithoutCancel(ctx), request, state, mutationActivityID, mutationLabel+"-terminal", status, &result, nil); appendErr != nil {
				return protocol.ToolResultBlock{}, errors.Join(executeErr, appendErr)
			}
			return result, nil
		}
		if err := s.probe.After(ctx, BarrierEffectDispatch, barrierState); err != nil {
			return protocol.ToolResultBlock{}, err
		}
		state.activeRequiresToolResult = true
		status := normalizeActivityStatus(execution.Outcome.Status)
		execution.ToolResult.CallID = intent.CallID
		execution.ToolResult.Status = status
		records, err := s.recordEvidence(ctx, request, mutationActivityID, mutationPlan, execution.Evidence)
		if err != nil {
			return protocol.ToolResultBlock{}, err
		}
		fileChange, err := structuredFileChangedEffect(mutationPlan, execution, records)
		if err != nil {
			return protocol.ToolResultBlock{}, err
		}
		execution.ToolResult.EvidenceIDs = evidenceIDs(records)
		result, err := s.sanitizeToolResult(ctx, request.Runtime.ID, execution.ToolResult)
		if err != nil {
			return protocol.ToolResultBlock{}, err
		}
		if err := s.appendActivityEvidence(ctx, request, state, mutationActivityID, mutationLabel+"-terminal", status, &result, records, fileChange); err != nil {
			return protocol.ToolResultBlock{}, err
		}
		return result, nil
	}
	return protocol.ToolResultBlock{CallID: intent.CallID, Status: "failed", Text: "resource drift did not stabilize"}, nil
}

func (s *Service) runObservationIntent(ctx context.Context, lease managedOperationLease, request StartTurnRequest, state *turnState, intent protocol.ToolUseBlock) (protocol.ToolResultBlock, error) {
	activityID := protocol.ActivityID(stableID("activity", string(request.Command.CommandID), "tool", intent.CallID))
	planRequest := tooling.PlanRequest{TurnID: state.turnID, ActivityID: activityID, CallID: intent.CallID, Alias: intent.Alias, Arguments: protocol.DeepCopy(intent.Arguments), RuntimeGenerationID: request.Runtime.ID}
	handle, plan, err := s.deps.Tools.Plan(ctx, planRequest)
	if err != nil {
		return protocol.ToolResultBlock{}, err
	}
	revalidated, changed, err := s.deps.Tools.Revalidate(ctx, handle)
	if err != nil {
		return protocol.ToolResultBlock{}, err
	}
	if changed {
		if err := s.appendSyntheticToolFailure(ctx, request, state, intent, &plan, "resource drift"); err != nil {
			return protocol.ToolResultBlock{}, err
		}
		return protocol.ToolResultBlock{CallID: intent.CallID, Status: "failed", Text: "resource drift"}, nil
	}
	if !revalidated.Digest.IsZero() {
		plan = revalidated
	}
	authRequest, err := toolAuthorizationRequest(request, state, planRequest, plan, nil)
	if err != nil {
		return protocol.ToolResultBlock{}, err
	}
	label := "tool-" + intent.CallID
	planned := []struct {
		kind    string
		payload any
	}{{protocol.EventActivityPlanned, activityPlan(plan, "tool observation", nil)}, {protocol.EventExecutionPlanDeclared, protocol.ExecutionPlanDeclaredV1{Plan: plan}}, {protocol.EventAuthorizationRequested, protocol.AuthorizationRequestedV1{Request: authRequest}}}
	events, err := s.activityEvents(*state, request.Runtime.ID, activityID, label+"-planned", planned)
	if err != nil {
		return protocol.ToolResultBlock{}, err
	}
	plannedHead := state.head
	if err := s.append(ctx, state, label+"-planned", events); err != nil {
		if state.head != plannedHead {
			state.activeActivityID, state.activeStarted, state.activeDispatched = activityID, false, false
			state.activeRequiresToolResult = false
		}
		return protocol.ToolResultBlock{}, err
	}
	state.activeActivityID, state.activeStarted, state.activeDispatched = activityID, false, false
	state.activeRequiresToolResult = false
	barrierState := state.barrierState()
	barrierState.ActivityID, barrierState.PlanDigest = activityID, plan.Digest
	if err := s.cross(ctx, BarrierActionPlanCommitted, barrierState); err != nil {
		return protocol.ToolResultBlock{}, err
	}
	token, err := s.authorizeActivity(ctx, lease, request, state, activityID, intent.CallID, label, authRequest)
	if err != nil {
		return protocol.ToolResultBlock{}, err
	}
	if err := s.cross(ctx, BarrierAuthorizationCommitted, barrierState); err != nil {
		return protocol.ToolResultBlock{}, err
	}
	if err := s.probe.Before(ctx, BarrierEffectDispatch, barrierState); err != nil {
		return protocol.ToolResultBlock{}, err
	}
	state.activeDispatched = true
	execution, err := s.deps.Tools.Execute(ctx, handle, token)
	if err != nil {
		state.activeRequiresToolResult = true
		result, sanitizeErr := s.sanitizeToolResult(context.WithoutCancel(ctx), request.Runtime.ID, protocol.ToolResultBlock{CallID: intent.CallID, Status: "uncertain", Text: err.Error()})
		if sanitizeErr != nil {
			return protocol.ToolResultBlock{}, errors.Join(err, sanitizeErr)
		}
		if appendErr := s.appendActivityEvidence(context.WithoutCancel(ctx), request, state, activityID, label+"-terminal", "uncertain", &result, nil); appendErr != nil {
			return protocol.ToolResultBlock{}, errors.Join(err, appendErr)
		}
		return result, nil
	}
	if err := s.probe.After(ctx, BarrierEffectDispatch, barrierState); err != nil {
		return protocol.ToolResultBlock{}, err
	}
	state.activeRequiresToolResult = true
	status := normalizeActivityStatus(execution.Outcome.Status)
	execution.ToolResult.CallID = intent.CallID
	execution.ToolResult.Status = status
	if err := s.publishToolPresentation(ctx, request, state, activityID, plan, execution); err != nil {
		return protocol.ToolResultBlock{}, err
	}
	records, err := s.recordEvidence(ctx, request, activityID, plan, execution.Evidence)
	if err != nil {
		return protocol.ToolResultBlock{}, err
	}
	execution.ToolResult.EvidenceIDs = evidenceIDs(records)
	result, err := s.sanitizeToolResult(ctx, request.Runtime.ID, execution.ToolResult)
	if err != nil {
		return protocol.ToolResultBlock{}, err
	}
	if err := s.appendActivityEvidence(ctx, request, state, activityID, label+"-terminal", status, &result, records); err != nil {
		return protocol.ToolResultBlock{}, err
	}
	return result, nil
}

func (s *Service) publishToolPresentation(ctx context.Context, request StartTurnRequest, state *turnState, activityID protocol.ActivityID, plan protocol.ActionPlan, execution protocol.ExecutionResult) error {
	if plan.Body.Tool != (protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "skill"}) {
		return nil
	}
	publisher, ok := s.publisher.(TransientApplicationEventPublisher)
	if !ok {
		return nil
	}
	content, err := s.deps.Admission.SanitizeText(ctx, request.Runtime.ID, execution.Presentation.Content)
	if err != nil {
		return err
	}
	available := protocol.ToolResultAvailableV1{ActivityID: activityID, CallID: execution.ToolResult.CallID, Status: execution.ToolResult.Status, Content: content, DurationNanos: execution.Presentation.DurationNanos, Truncated: execution.Presentation.Truncated}
	if err := available.Validate(); err != nil {
		return err
	}
	payload, err := canonicaljson.Marshal(available)
	if err != nil {
		return err
	}
	return publisher.PublishTransient(protocol.ApplicationEvent{Correlation: protocol.EventCorrelation{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(request.SessionID), SessionID: request.SessionID, TurnID: state.turnID, ActivityID: activityID}, Time: time.Now().UTC(), Kind: protocol.EventToolResultAvailable, PayloadVersion: 1, Payload: payload})
}

func (s *Service) sanitizeToolResult(ctx context.Context, generation protocol.RuntimeGenerationID, result protocol.ToolResultBlock) (protocol.ToolResultBlock, error) {
	block, err := s.sanitizeContentBlock(ctx, generation, protocol.ContentBlock{Kind: protocol.ContentToolResult, ToolResult: &result})
	if err != nil {
		return protocol.ToolResultBlock{}, err
	}
	return protocol.DeepCopy(*block.ToolResult), nil
}

func (s *Service) appendSyntheticToolFailure(ctx context.Context, request StartTurnRequest, state *turnState, intent protocol.ToolUseBlock, plan *protocol.ActionPlan, reason string) error {
	activityID := protocol.ActivityID(stableID("activity", string(request.Command.CommandID), "synthetic-tool", intent.CallID, reason))
	planned := protocol.ActivityPlannedV1{
		Kind: "tool", Purpose: reason, PurposeActor: protocol.ActorRef{ID: "orchestrator", Kind: protocol.ActorAgent},
		Source: "model_tool_intent", InputEvidenceIDs: []protocol.EvidenceID{}, RequestedProfile: "none", EffectiveProfile: "none",
	}
	if plan != nil {
		copyPlan := protocol.DeepCopy(*plan)
		planned.Plan = &copyPlan
		planned.Source, planned.RequestedProfile, planned.EffectiveProfile = copyPlan.Body.Tool.Source, copyPlan.Body.RequestedProfile, copyPlan.Body.EffectiveProfile
	}
	result, err := s.sanitizeToolResult(ctx, request.Runtime.ID, protocol.ToolResultBlock{CallID: intent.CallID, Status: "failed", Text: reason})
	if err != nil {
		return err
	}
	values := []struct {
		kind    string
		payload any
	}{
		{protocol.EventActivityPlanned, planned},
		{protocol.EventToolMessage, protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{protocol.DeepCopy(result)}}},
		{protocol.EventActivityFailed, protocol.ActivityOutcomeV1{Status: "failed"}},
	}
	events, err := s.activityEvents(*state, request.Runtime.ID, activityID, "synthetic-tool-"+intent.CallID, values)
	if err != nil {
		return err
	}
	return s.append(ctx, state, "synthetic-tool-"+intent.CallID, events)
}

func (s *Service) recordEvidence(ctx context.Context, request StartTurnRequest, activityID protocol.ActivityID, plan protocol.ActionPlan, candidates []protocol.EvidenceCandidate) ([]protocol.EvidenceRecord, error) {
	records := make([]protocol.EvidenceRecord, 0, len(candidates))
	for index, candidate := range candidates {
		candidate = protocol.DeepCopy(candidate)
		if candidate.ID == "" {
			candidate.ID = protocol.EvidenceID(stableID("evidence", string(request.Command.CommandID), string(activityID), fmt.Sprint(index)))
		}
		candidate.WorkspaceID = effectiveWorkspaceID(request.WorkspaceID, request.SessionID)
		candidate.SessionID = request.SessionID
		candidate.ProducingActivityID = activityID
		if candidate.Actor.Validate() != nil {
			candidate.Actor = protocol.ActorRef{ID: protocol.ActorID(plan.Body.Tool.Name), Kind: protocol.ActorTool}
		}
		if candidate.Subject.Validate() != nil {
			candidate.Subject = protocol.SubjectRef{Kind: "action", ID: plan.Body.CallID}
		}
		content, err := s.deps.Admission.SanitizeText(ctx, request.Runtime.ID, string(candidate.Content))
		if err != nil {
			return nil, err
		}
		candidate.Content = []byte(content)
		candidateID, err := s.deps.Admission.SanitizeText(ctx, request.Runtime.ID, string(candidate.ID))
		if err != nil {
			return nil, err
		}
		kind, err := s.deps.Admission.SanitizeText(ctx, request.Runtime.ID, candidate.Kind)
		if err != nil {
			return nil, err
		}
		mediaType, err := s.deps.Admission.SanitizeText(ctx, request.Runtime.ID, candidate.MediaType)
		if err != nil {
			return nil, err
		}
		candidate.ID, candidate.Kind, candidate.MediaType = protocol.EvidenceID(candidateID), kind, mediaType
		actorID, err := s.deps.Admission.SanitizeText(ctx, request.Runtime.ID, string(candidate.Actor.ID))
		if err != nil {
			return nil, err
		}
		subjectKind, err := s.deps.Admission.SanitizeText(ctx, request.Runtime.ID, candidate.Subject.Kind)
		if err != nil {
			return nil, err
		}
		subjectID, err := s.deps.Admission.SanitizeText(ctx, request.Runtime.ID, candidate.Subject.ID)
		if err != nil {
			return nil, err
		}
		candidate.Actor.ID, candidate.Subject.Kind, candidate.Subject.ID = protocol.ActorID(actorID), subjectKind, subjectID
		record, err := s.deps.Evidence.Put(ctx, candidate)
		if err != nil {
			return nil, err
		}
		if err := record.Body.Validate(); err != nil {
			return nil, fmt.Errorf("evidence recorder returned invalid body: %w", err)
		}
		if err := canonicaljson.ValidateDigest(record.Body, record.Digest); err != nil {
			return nil, fmt.Errorf("evidence recorder returned invalid digest: %w", err)
		}
		if record.Body.ID != candidate.ID || record.Body.Kind != candidate.Kind || record.Body.WorkspaceID != candidate.WorkspaceID ||
			record.Body.SessionID != candidate.SessionID || record.Body.ProducingActivityID != activityID || record.Body.Actor != candidate.Actor ||
			record.Body.Subject != candidate.Subject || record.Body.MediaType != candidate.MediaType {
			return nil, fmt.Errorf("evidence recorder returned mismatched identity")
		}
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Body.ID < records[j].Body.ID })
	return records, nil
}

func (s *Service) appendActivityEvidence(ctx context.Context, request StartTurnRequest, state *turnState, activityID protocol.ActivityID, label, status string, toolResult *protocol.ToolResultBlock, records []protocol.EvidenceRecord, fileChanges ...*protocol.FileChangedV1) error {
	status = normalizeActivityStatus(status)
	kind := map[string]string{
		"succeeded": protocol.EventActivitySucceeded, "failed": protocol.EventActivityFailed,
		"denied": protocol.EventActivityDenied, "cancelled": protocol.EventActivityCancelled,
		"interrupted_no_effect": protocol.EventActivityInterruptedNoEffect, "uncertain": protocol.EventActivityUncertain,
	}[status]
	values := make([]struct {
		kind    string
		payload any
	}, 0, 2+len(records)*2+len(fileChanges))
	for _, fileChange := range fileChanges {
		if fileChange != nil {
			values = append(values, struct {
				kind    string
				payload any
			}{protocol.EventFileChanged, *fileChange})
		}
	}
	if toolResult != nil {
		result := protocol.DeepCopy(*toolResult)
		values = append(values, struct {
			kind    string
			payload any
		}{protocol.EventToolMessage, protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{result}}})
	}
	values = append(values, struct {
		kind    string
		payload any
	}{kind, protocol.ActivityOutcomeV1{Status: status, OutputEvidenceIDs: evidenceIDs(records)}})
	for _, record := range records {
		values = append(values,
			struct {
				kind    string
				payload any
			}{protocol.EventEvidenceRecorded, protocol.EvidenceRecordedV1{Record: record}},
			struct {
				kind    string
				payload any
			}{protocol.EventEvidenceLinked, protocol.EvidenceLinkedV1{EvidenceID: record.Body.ID, Subject: record.Body.Subject, Relation: "output"}},
		)
	}
	events, err := s.activityEvents(*state, request.Runtime.ID, activityID, label, values)
	if err != nil {
		return err
	}
	terminalHead := state.head
	if err := s.append(ctx, state, label, events); err != nil {
		if state.head != terminalHead && state.activeActivityID == activityID {
			state.activeActivityID, state.activeStarted, state.activeDispatched = "", false, false
			state.activeRequiresToolResult = false
		}
		return err
	}
	if state.activeActivityID == activityID {
		state.activeActivityID, state.activeStarted, state.activeDispatched = "", false, false
		state.activeRequiresToolResult = false
	}
	barrierState := state.barrierState()
	barrierState.ActivityID = activityID
	return s.cross(ctx, BarrierActionTerminalCommitted, barrierState)
}

func normalizeActivityStatus(status string) string {
	switch status {
	case "succeeded", "failed", "denied", "cancelled", "interrupted_no_effect", "uncertain":
		return status
	default:
		return "failed"
	}
}

func structuredFileChangedEffect(plan protocol.ActionPlan, execution protocol.ExecutionResult, records []protocol.EvidenceRecord) (*protocol.FileChangedV1, error) {
	if execution.FileChange == nil {
		return nil, nil
	}
	expectedDescriptor := edittool.BuiltinDescriptor()
	expectedClassification := toolset.EditClassification()
	if plan.Body.Tool != expectedDescriptor.Body.Identity || plan.Body.SourceRevision != expectedDescriptor.Body.SourceRevision || plan.Body.DescriptorDigest != expectedDescriptor.DescriptorDigest ||
		plan.Body.Action != "edit" || plan.Body.Purpose != "mutate" || plan.Body.Effect != expectedClassification.Effect || plan.Body.ExecutionLocus != expectedClassification.ExecutionLoci[0] ||
		plan.Body.Reversibility != expectedClassification.Reversibility || plan.Body.VerificationCoverage != expectedClassification.VerificationCoverage || plan.Body.RequestedProfile != expectedClassification.RequestedProfile || plan.Body.EffectiveProfile != expectedClassification.EffectiveProfile ||
		plan.Body.Boundary != expectedClassification.Boundary {
		return nil, fmt.Errorf("structured file change requires the canonical edit tool")
	}
	if err := canonicaljson.ValidateDigest(plan.Body, plan.Digest); err != nil {
		return nil, fmt.Errorf("structured file change plan digest: %w", err)
	}
	change := protocol.DeepCopy(*execution.FileChange)
	if change.CallID == "" || change.CallID != plan.Body.CallID || change.Subject.Kind != "file" || change.Subject.ID == "" || change.Subject.Validate() != nil || change.Before.Validate() != nil || change.After.Validate() != nil || len(plan.Body.Resources) != 1 {
		return nil, fmt.Errorf("structured file change is incomplete or unbound")
	}
	resource := plan.Body.Resources[0]
	create := len(resource.Attributes) == 1 && resource.Attributes[0] == (protocol.ResourceAttribute{Name: "create", Value: "true"})
	const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if resource.Kind != "file" || resource.CanonicalID != change.Subject.ID ||
		create && (resource.Digest != "" || resource.ParentID == "" || change.Before.Value != emptySHA256) ||
		!create && (len(resource.Attributes) != 0 || resource.ParentID != "" || resource.Digest != change.Before.Value) {
		return nil, fmt.Errorf("structured file change does not match the authorized resource")
	}
	change.EvidenceIDs = evidenceIDs(records)
	return &change, nil
}

func (s *Service) prepareCheckpoint(ctx context.Context, request StartTurnRequest, state *turnState, activityID protocol.ActivityID, label string, preview tooling.PreviewResult, plan protocol.ActionPlan, evidence []protocol.EvidenceRecord) (protocol.CheckpointBody, error) {
	subject := protocol.SubjectRef{Kind: "action", ID: plan.Body.CallID}
	if len(plan.Body.Resources) != 0 {
		subject = protocol.SubjectRef{Kind: plan.Body.Resources[0].Kind, ID: plan.Body.Resources[0].CanonicalID}
	}
	coverageClass := "not_reversible"
	if plan.Body.Reversibility == "exact" {
		coverageClass = "exact"
	} else if plan.Body.ExecutionLocus == "remote" || plan.Body.Effect == "external" {
		coverageClass = "external_unknown"
	}
	body := protocol.CheckpointBody{
		ID:        protocol.CheckpointID(stableID("checkpoint", string(request.Command.CommandID), plan.Body.CallID, string(activityID))),
		SessionID: request.SessionID, TaskID: state.taskID, TurnID: state.turnID, EventHead: state.head,
		ContextPlanDigest: state.contextPlanDigest, OutcomeContractID: state.contractID, ContractVersion: 2,
		RuntimeGenerationID: request.Runtime.ID, PlanDigest: plan.Digest,
		Coverage:  []protocol.CheckpointCoverage{{Subject: subject, Class: coverageClass, EvidenceIDs: evidenceIDs(evidence)}},
		CreatedAt: time.Now().UTC(),
	}
	planned := []struct {
		kind    string
		payload any
	}{{protocol.EventCheckpointPlanned, protocol.CheckpointPlannedV1{Body: body, PlanDigest: plan.Digest}}}
	events, err := s.activityEvents(*state, request.Runtime.ID, activityID, label+"-checkpoint-planned", planned)
	if err != nil {
		return protocol.CheckpointBody{}, err
	}
	if err := s.append(ctx, state, label+"-checkpoint-planned", events); err != nil {
		return protocol.CheckpointBody{}, err
	}

	materialIDs := make([]protocol.RecoveryMaterialID, 0, 1)
	if coverageClass == "exact" {
		material, putErr := s.deps.Recovery.PrepareAndPut(ctx, preview, activityID, body, plan)
		if errors.Is(putErr, recovery.ErrSecretDetected) {
			body.Coverage[0].Class = "drift_detectable"
		} else if putErr != nil {
			return protocol.CheckpointBody{}, s.failCheckpoint(ctx, request, state, activityID, label, plan.Digest, putErr)
		} else {
			if err := validateRecoveryMaterialMetadata(material, effectiveWorkspaceID(request.WorkspaceID, request.SessionID), activityID, body, plan); err != nil {
				return protocol.CheckpointBody{}, s.failCheckpoint(ctx, request, state, activityID, label, plan.Digest, fmt.Errorf("recovery material binding mismatch"))
			}
			body.Coverage[0].RecoveryMaterialID = material.ID
			materialIDs = append(materialIDs, material.ID)
		}
	}
	checkpointDigest, err := canonicaljson.Digest(body)
	if err != nil {
		return protocol.CheckpointBody{}, err
	}
	ready := []struct {
		kind    string
		payload any
	}{{protocol.EventCheckpointReady, protocol.CheckpointReadyV1{Body: body, Digest: checkpointDigest, RecoveryMaterialIDs: materialIDs}}}
	events, err = s.activityEvents(*state, request.Runtime.ID, activityID, label+"-checkpoint-ready", ready)
	if err != nil {
		return protocol.CheckpointBody{}, err
	}
	if err := s.append(ctx, state, label+"-checkpoint-ready", events); err != nil {
		return protocol.CheckpointBody{}, err
	}
	barrierState := state.barrierState()
	barrierState.ActivityID, barrierState.PlanDigest = activityID, plan.Digest
	if err := s.cross(ctx, BarrierCheckpointReady, barrierState); err != nil {
		return protocol.CheckpointBody{}, err
	}
	return body, nil
}

func validateRecoveryMaterialMetadata(record protocol.RecoveryMaterialRecord, workspaceID protocol.WorkspaceID, activityID protocol.ActivityID, checkpoint protocol.CheckpointBody, plan protocol.ActionPlan) error {
	if record.ID == "" || record.WorkspaceID != workspaceID || record.ActivityID != activityID || record.CheckpointID != checkpoint.ID ||
		record.Subject != checkpoint.Coverage[0].Subject || record.Body.PlanDigest != plan.Digest || record.Body.CreatedAt.IsZero() {
		return fmt.Errorf("recovery material identity mismatch")
	}
	for _, digest := range []protocol.Digest{record.Body.PlanDigest, record.Body.PreimageDigest, record.Body.ExpectedPostimageDigest, record.MaterialDigest} {
		if err := digest.Validate(); err != nil {
			return err
		}
	}
	return protocol.ValidateBounds(record)
}

func (s *Service) failCheckpoint(ctx context.Context, request StartTurnRequest, state *turnState, activityID protocol.ActivityID, label string, planDigest protocol.Digest, cause error) error {
	reason, sanitizeErr := s.deps.Admission.SanitizeText(context.WithoutCancel(ctx), request.Runtime.ID, cause.Error())
	if sanitizeErr != nil {
		reason = "checkpoint preparation failed"
	}
	failed := []struct {
		kind    string
		payload any
	}{{protocol.EventCheckpointFailed, protocol.CheckpointFailedV1{PlanDigest: planDigest, ErrorCode: "checkpoint_failed", Reason: reason}}}
	events, err := s.activityEvents(*state, request.Runtime.ID, activityID, label+"-checkpoint-failed", failed)
	if err != nil {
		return errors.Join(cause, sanitizeErr, err)
	}
	if err := s.append(context.WithoutCancel(ctx), state, label+"-checkpoint-failed", events); err != nil {
		return errors.Join(cause, sanitizeErr, err)
	}
	return errors.Join(cause, sanitizeErr)
}

func (s *Service) abandonDriftRound(ctx context.Context, request StartTurnRequest, state *turnState, activityID protocol.ActivityID, label string, planDigest protocol.Digest, status string, toolResult *protocol.ToolResultBlock) error {
	activityKind := protocol.EventActivityCancelled
	if status == "failed" {
		activityKind = protocol.EventActivityFailed
	}
	values := []struct {
		kind    string
		payload any
	}{
		{protocol.EventCheckpointFailed, protocol.CheckpointFailedV1{PlanDigest: planDigest, ErrorCode: "resources_changed", Reason: "resources changed after checkpoint"}},
	}
	if toolResult != nil {
		values = append(values, struct {
			kind    string
			payload any
		}{protocol.EventToolMessage, protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{protocol.DeepCopy(*toolResult)}}})
	}
	values = append(values, struct {
		kind    string
		payload any
	}{activityKind, protocol.ActivityOutcomeV1{Status: status}})
	events, err := s.activityEvents(*state, request.Runtime.ID, activityID, label+"-drift-terminal", values)
	if err != nil {
		return err
	}
	terminalHead := state.head
	if err := s.append(ctx, state, label+"-drift-terminal", events); err != nil {
		if state.head != terminalHead && state.activeActivityID == activityID {
			state.activeActivityID, state.activeStarted, state.activeDispatched = "", false, false
			state.activeRequiresToolResult = false
		}
		return err
	}
	if state.activeActivityID == activityID {
		state.activeActivityID, state.activeStarted, state.activeDispatched = "", false, false
		state.activeRequiresToolResult = false
	}
	return nil
}

func (s *Service) authorizeActivity(ctx context.Context, lease managedOperationLease, request StartTurnRequest, state *turnState, activityID protocol.ActivityID, callID, label string, authorizationRequest protocol.AuthorizationRequest) (authorization.CommittedToken, error) {
	decision, err := s.deps.Authorization.Decide(ctx, authorizationRequest)
	if err != nil {
		return authorization.CommittedToken{}, err
	}
	if decision.Action == "ask" {
		if s.deps.Approver == nil {
			return authorization.CommittedToken{}, fmt.Errorf("interactive approver is required")
		}
		response, approveErr := s.awaitInteractiveApproval(ctx, lease, decision)
		if approveErr != nil {
			return authorization.CommittedToken{}, approveErr
		}
		if err := s.reconcileInteractiveApprovalJournal(ctx, request, state); err != nil {
			return authorization.CommittedToken{}, err
		}
		decision, err = s.deps.Authorization.ResolveInteractive(ctx, authorizationRequest, decision, response)
		if err != nil {
			return authorization.CommittedToken{}, err
		}
	}
	if err := authorization.ValidateBinding(authorizationRequest, decision); err != nil {
		return authorization.CommittedToken{}, err
	}
	if decision.Action != "allow" {
		denied := &authorizationDeniedError{reason: decision.Reason}
		decisionEvents := []struct {
			kind    string
			payload any
		}{
			{protocol.EventAuthorizationDecided, protocol.AuthorizationDecidedV1{Decision: decision}},
		}
		if authorizationRequest.Source.Source != "provider" {
			if state.activeActivityID == activityID {
				state.activeRequiresToolResult = true
			}
			result, sanitizeErr := s.sanitizeToolResult(ctx, request.Runtime.ID, protocol.ToolResultBlock{CallID: callID, Status: "denied", Text: decision.Reason})
			if sanitizeErr != nil {
				return authorization.CommittedToken{}, sanitizeErr
			}
			decisionEvents = append(decisionEvents, struct {
				kind    string
				payload any
			}{protocol.EventToolMessage, protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{result}}})
		}
		decisionEvents = append(decisionEvents, struct {
			kind    string
			payload any
		}{protocol.EventActivityDenied, protocol.ActivityOutcomeV1{Status: "denied"}})
		events, eventErr := s.activityEvents(*state, request.Runtime.ID, activityID, label+"-decision", decisionEvents)
		if eventErr != nil {
			return authorization.CommittedToken{}, eventErr
		}
		decisionHead := state.head
		if appendErr := s.append(ctx, state, label+"-decision", events); appendErr != nil {
			if state.head != decisionHead {
				state.activeActivityID, state.activeStarted, state.activeDispatched = "", false, false
				state.activeRequiresToolResult = false
				return authorization.CommittedToken{}, errors.Join(denied, appendErr)
			}
			return authorization.CommittedToken{}, appendErr
		}
		state.activeActivityID, state.activeStarted, state.activeDispatched = "", false, false
		state.activeRequiresToolResult = false
		return authorization.CommittedToken{}, denied
	}
	decisionEventID := eventID(state.command.CommandID, label+"-decision", 0, protocol.EventAuthorizationDecided)
	decisionEvents := []struct {
		kind    string
		payload any
	}{
		{protocol.EventAuthorizationDecided, protocol.AuthorizationDecidedV1{Decision: decision}},
		{protocol.EventActivityAuthorized, protocol.ActivityAuthorizedV1{
			DecisionNonce: decision.DecisionNonce, DecisionEventID: decisionEventID, PlanDigest: authorizationRequest.PlanDigest,
			RequestDigest: authorizationRequest.RequestDigest, DispatchDigest: authorizationRequest.DispatchDigest,
		}},
	}
	events, err := s.activityEvents(*state, request.Runtime.ID, activityID, label+"-decision", decisionEvents)
	if err != nil {
		return authorization.CommittedToken{}, err
	}
	if err := s.append(ctx, state, label+"-decision", events); err != nil {
		return authorization.CommittedToken{}, err
	}
	decisionDigest, err := canonicaljson.Digest(decision)
	if err != nil {
		return authorization.CommittedToken{}, err
	}
	consumedEventID := eventID(state.command.CommandID, label+"-start", 0, protocol.EventAuthorizationDecisionConsumed)
	startedEventID := eventID(state.command.CommandID, label+"-start", 1, protocol.EventActivityStarted)
	startEvents := []struct {
		kind    string
		payload any
	}{
		{protocol.EventAuthorizationDecisionConsumed, protocol.AuthorizationDecisionConsumedV1{
			DecisionNonce: decision.DecisionNonce, DecisionEventID: decisionEventID, DecisionDigest: decisionDigest,
			RequestID: authorizationRequest.RequestID, ActivityID: activityID, CallID: callID, PlanDigest: authorizationRequest.PlanDigest,
			RequestDigest: authorizationRequest.RequestDigest, DispatchDigest: authorizationRequest.DispatchDigest,
			RuntimeGenerationID: request.Runtime.ID,
		}},
		{protocol.EventActivityStarted, protocol.ActivityStartedV1{
			DecisionNonce: decision.DecisionNonce, DecisionEventID: decisionEventID, ActivityID: activityID, CallID: callID,
			PlanDigest: authorizationRequest.PlanDigest, RequestDigest: authorizationRequest.RequestDigest,
			DispatchDigest: authorizationRequest.DispatchDigest, RuntimeGenerationID: request.Runtime.ID, DispatchState: "registered",
		}},
	}
	events, err = s.activityEvents(*state, request.Runtime.ID, activityID, label+"-start", startEvents)
	if err != nil {
		return authorization.CommittedToken{}, err
	}
	startHead := state.head
	if err := s.append(ctx, state, label+"-start", events); err != nil {
		state.activeStarted = state.head != startHead
		return authorization.CommittedToken{}, err
	}
	state.activeStarted = true
	barrierState := state.barrierState()
	barrierState.ActivityID, barrierState.PlanDigest = activityID, authorizationRequest.PlanDigest
	if err := s.cross(ctx, BarrierActivityStartedCommitted, barrierState); err != nil {
		return authorization.CommittedToken{}, err
	}
	return s.deps.Authorization.Issue(ctx, authorization.CommitReference{
		Journal: state.ref, DecisionTransactionID: protocol.TransactionID(stableID("transaction", string(state.command.CommandID), label+"-decision")),
		DecisionEventID: decisionEventID, StartTransactionID: protocol.TransactionID(stableID("transaction", string(state.command.CommandID), label+"-start")),
		ConsumedEventID: consumedEventID, StartedEventID: startedEventID,
	})
}

func (s *Service) awaitInteractiveApproval(ctx context.Context, lease managedOperationLease, decision protocol.AuthorizationDecision) (protocol.ApprovalResponse, error) {
	if lease == nil {
		return protocol.ApprovalResponse{}, fmt.Errorf("interactive approval requires an operation lease")
	}
	var response protocol.ApprovalResponse
	err := lease.Yield(ctx, func(yielded context.Context) error {
		var approveErr error
		response, approveErr = s.deps.Approver.Approve(yielded, decision)
		return approveErr
	})
	return response, err
}

func (s *Service) reconcileInteractiveApprovalJournal(ctx context.Context, request StartTurnRequest, state *turnState) error {
	page, err := s.repository.ReadRange(ctx, journal.ReadRangeRequest{Journal: state.ref, After: state.head, Limit: 3})
	if err != nil {
		return fmt.Errorf("read interactive approval journal suffix: %w", err)
	}
	if page.Head == state.head {
		return nil
	}
	if page.More || page.Cursor != page.Head || len(page.Events) != 1 || page.Head.CommitSeq != state.head.CommitSeq+2 {
		return fmt.Errorf("interactive approval journal drift b=%d c=%d h=%d n=%d more=%t", state.head.CommitSeq, page.Cursor.CommitSeq, page.Head.CommitSeq, len(page.Events), page.More)
	}
	event := page.Events[0].Envelope
	if event.Kind != protocol.EventTrustedExecutionAcknowledged || event.SessionID != request.SessionID || event.RuntimeGenerationID != request.Runtime.ID || event.Actor == nil || *event.Actor != request.Command.Actor || event.Seq != state.head.CommitSeq+1 || event.TransactionID != page.Head.TransactionID {
		return fmt.Errorf("interactive approval journal suffix has invalid trusted-shell provenance: kind=%q session=%q generation=%q actor=%+v seq=%d transaction=%q", event.Kind, event.SessionID, event.RuntimeGenerationID, event.Actor, event.Seq, event.TransactionID)
	}
	var acknowledged protocol.TrustedExecutionAcknowledgedV1
	if err := json.Unmarshal(event.Payload, &acknowledged); err != nil || !acknowledged.Enabled || acknowledged.Profile != "unsandboxed" {
		return fmt.Errorf("interactive approval journal suffix has invalid trusted-shell payload")
	}
	state.head = page.Head
	return nil
}

func (s *Service) completeTurn(ctx context.Context, request StartTurnRequest, state *turnState, assistant protocol.AssistantMessageV1, terminal protocol.ProviderAttemptTerminalV1) (RunResult, error) {
	verificationActivityID := protocol.ActivityID(stableID("activity", string(request.Command.CommandID), "verification"))
	assessment, err := s.deps.Verification.Assess(ctx, verification.Request{
		TaskID: state.taskID, OutcomeContractID: state.contractID, ContractVersion: 2,
		CriterionID: "legacy_turn_terminal", ActivityID: verificationActivityID, TurnID: state.turnID, TerminalStatus: "completed",
	})
	if err != nil {
		return RunResult{}, err
	}
	verificationEvents := []struct {
		kind    string
		payload any
	}{
		{protocol.EventVerificationReceiptRecorded, protocol.VerificationReceiptRecordedV1{Receipt: assessment.Receipt}},
		{protocol.EventOutcomeCriterionAssessed, protocol.CriterionAssessedV1{
			OutcomeContractID: state.contractID, ContractVersion: 2, CriterionID: "legacy_turn_terminal", Status: assessment.CriterionStatus,
			EvidenceIDs: []protocol.EvidenceID{}, ReceiptIDs: []protocol.ReceiptID{assessment.Receipt.Body.ID}, Reason: assessment.Reason,
		}},
		{protocol.EventOutcomeFinalAssessed, protocol.OutcomeFinalAssessedV1{
			OutcomeContractID: state.contractID, ContractVersion: 2, Status: assessment.FinalStatus,
			CriterionIDs: []string{"legacy_turn_terminal"}, ReceiptIDs: []protocol.ReceiptID{assessment.Receipt.Body.ID}, UnknownEffects: []protocol.SubjectRef{},
		}},
	}
	events, err := s.activityEvents(*state, request.Runtime.ID, verificationActivityID, "verification", verificationEvents)
	if err != nil {
		return RunResult{}, err
	}
	if err := s.append(ctx, state, "verification", events); err != nil {
		return RunResult{}, err
	}
	if err := s.cross(ctx, BarrierVerificationCommitted, state.barrierState()); err != nil {
		return RunResult{}, err
	}

	finalTransactionID := protocol.TransactionID(stableID("transaction", string(state.command.CommandID), "turn-terminal"))
	terminalEventCount := uint64(3)
	if request.child != nil {
		terminalEventCount++
	}
	finalCursor := protocol.CommittedCursor{JournalKind: state.ref.Kind, JournalID: state.ref.ID, CommitSeq: state.head.CommitSeq + terminalEventCount + 1, TransactionID: finalTransactionID}
	commandPayload, err := canonicaljson.Marshal(struct {
		TaskID protocol.TaskID `json:"task_id"`
		TurnID protocol.TurnID `json:"turn_id"`
		Status string          `json:"status"`
	}{state.taskID, state.turnID, "completed"})
	if err != nil {
		return RunResult{}, err
	}
	commandResult := protocol.CommandResult{
		ProtocolVersion: protocol.ApplicationProtocolVersion, CommandID: request.Command.CommandID, Status: "completed", RequestDigest: request.Command.RequestDigest,
		Cursor: protocol.ApplicationCursor{SelectedSession: &finalCursor}, PayloadVersion: 1, Payload: commandPayload,
	}
	rawResult, err := canonicaljson.Marshal(commandResult)
	if err != nil {
		return RunResult{}, err
	}
	terminalEvents := []struct {
		kind    string
		payload any
	}{
		{protocol.EventTurnCompleted, protocol.TurnTerminalV1{Status: "completed", Reason: "provider completed"}},
		{protocol.EventTaskStatusChanged, protocol.TaskStatusChangedV1{From: string(protocol.TaskRunning), To: string(protocol.TaskCompleted), Reason: "legacy turn terminal"}},
		{protocol.EventCommandCompleted, protocol.CommandCompletedV1{CommandID: request.Command.CommandID, RequestDigest: request.Command.RequestDigest, Status: "completed", Result: rawResult}},
	}
	if request.child != nil {
		receipt, receiptErr := s.projectChildReceipt(ctx, state, request.child.manifest, "succeeded", assistantSummary(assistant), protocol.CommittedCursor{JournalKind: state.ref.Kind, JournalID: state.ref.ID, CommitSeq: state.head.CommitSeq + terminalEventCount, TransactionID: finalTransactionID}, terminal.Usage, nil)
		if receiptErr != nil {
			return RunResult{}, receiptErr
		}
		terminalEvents = append(terminalEvents, struct {
			kind    string
			payload any
		}{protocol.EventSubagentReceipt, receipt})
	}
	events, err = s.turnEvents(*state, request.Runtime.ID, "turn-terminal", terminalEvents)
	if err != nil {
		return RunResult{}, err
	}
	terminalHead := state.head
	if err := s.append(ctx, state, "turn-terminal", events); err != nil {
		state.terminal = state.head != terminalHead
		return RunResult{}, err
	}
	state.terminal = true
	if state.head != finalCursor {
		return RunResult{}, fmt.Errorf("terminal command cursor prediction mismatch")
	}
	if err := s.cross(ctx, BarrierTurnTerminalCommitted, state.barrierState()); err != nil {
		return RunResult{}, err
	}
	if err := s.cross(ctx, BarrierCommandCompleted, state.barrierState()); err != nil {
		return RunResult{}, err
	}
	return RunResult{TaskID: state.taskID, TurnID: state.turnID, Cursor: state.head, Status: "completed", CommandResult: commandResult, Assistant: protocol.DeepCopy(assistant)}, nil
}

func (s *Service) terminalizeTurnFailure(ctx context.Context, request StartTurnRequest, state *turnState, cause error) (RunResult, error) {
	turnKind, turnStatus := protocol.EventTurnFailed, "failed"
	commandStatus := "failed"
	taskTo := string(protocol.TaskFailed)
	reason, code := "turn orchestration failed", "turn_failed"
	var denied *authorizationDeniedError
	if errors.As(cause, &denied) {
		commandStatus = "denied"
		reason, code = "turn authorization denied", "authorization_denied"
	} else if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		turnKind, turnStatus = protocol.EventTurnInterrupted, "interrupted"
		commandStatus, taskTo = "interrupted", string(protocol.TaskCancelled)
		reason, code = "turn orchestration interrupted", "turn_interrupted"
	} else {
		var providerFailure *providerFailureError
		if errors.As(cause, &providerFailure) && providerFailure.code == "context_too_large" {
			reason, code = "provider context is too large; compact or reduce the request", "context_too_large"
		}
		if !state.taskRunning {
			taskTo = string(protocol.TaskCancelled)
		}
	}
	activityStatus := "failed"
	if denied != nil {
		activityStatus = "denied"
	} else if state.activeDispatched {
		activityStatus = "uncertain"
	} else if turnStatus == "interrupted" {
		activityStatus = "cancelled"
	} else if !state.activeStarted {
		activityStatus = "cancelled"
	}
	eventCount := 3
	terminalizeActiveActivity := state.activeActivityID != "" && !state.activeRequiresToolResult
	if terminalizeActiveActivity {
		eventCount++
	}
	if request.child != nil {
		eventCount++
	}
	transactionID := protocol.TransactionID(stableID("transaction", string(request.Command.CommandID), "turn-failure-terminal"))
	finalCursor := protocol.CommittedCursor{
		JournalKind: state.ref.Kind, JournalID: state.ref.ID,
		CommitSeq: state.head.CommitSeq + uint64(eventCount) + 1, TransactionID: transactionID,
	}
	publicError := &protocol.PublicError{Code: code, Message: reason, Retryable: false}
	payload, err := canonicaljson.Marshal(struct {
		TaskID protocol.TaskID `json:"task_id"`
		TurnID protocol.TurnID `json:"turn_id"`
		Status string          `json:"status"`
	}{state.taskID, state.turnID, commandStatus})
	if err != nil {
		return RunResult{}, err
	}
	commandResult := protocol.CommandResult{
		ProtocolVersion: protocol.ApplicationProtocolVersion, CommandID: request.Command.CommandID, Status: commandStatus,
		RequestDigest: request.Command.RequestDigest, Cursor: protocol.ApplicationCursor{SelectedSession: &finalCursor},
		PayloadVersion: 1, Payload: payload, Error: publicError,
	}
	rawResult, err := canonicaljson.Marshal(commandResult)
	if err != nil {
		return RunResult{}, err
	}
	events := make([]protocol.ProposedEvent, 0, eventCount)
	if terminalizeActiveActivity {
		activityKind := map[string]string{
			"failed": protocol.EventActivityFailed, "denied": protocol.EventActivityDenied,
			"cancelled": protocol.EventActivityCancelled, "uncertain": protocol.EventActivityUncertain,
		}[activityStatus]
		activityEvents, eventErr := s.activityEvents(*state, request.Runtime.ID, state.activeActivityID, "turn-failure-active", []struct {
			kind    string
			payload any
		}{{activityKind, protocol.ActivityOutcomeV1{Status: activityStatus}}})
		if eventErr != nil {
			return RunResult{}, eventErr
		}
		events = append(events, activityEvents...)
	}
	from := string(protocol.TaskPending)
	if state.taskRunning {
		from = string(protocol.TaskRunning)
	}
	terminalValues := []struct {
		kind    string
		payload any
	}{
		{turnKind, protocol.TurnTerminalV1{Status: turnStatus, Reason: reason, ErrorCode: code}},
		{protocol.EventTaskStatusChanged, protocol.TaskStatusChangedV1{From: from, To: taskTo, Reason: reason}},
		{protocol.EventCommandCompleted, protocol.CommandCompletedV1{
			CommandID: request.Command.CommandID, RequestDigest: request.Command.RequestDigest,
			Status: commandStatus, Result: rawResult, Error: publicError,
		}},
	}
	if request.child != nil {
		receiptStatus := "failed"
		unknownEffects := []protocol.ActivityID{}
		if activityStatus == "uncertain" {
			receiptStatus = "uncertain"
			if state.activeActivityID != "" {
				unknownEffects = append(unknownEffects, state.activeActivityID)
			}
		} else if turnStatus == "interrupted" {
			receiptStatus = "cancelled"
		}
		receipt, receiptErr := s.projectChildReceipt(ctx, state, request.child.manifest, receiptStatus, reason, protocol.CommittedCursor{JournalKind: state.ref.Kind, JournalID: state.ref.ID, CommitSeq: state.head.CommitSeq + uint64(eventCount), TransactionID: transactionID}, unknownUsage(), publicError)
		if receiptErr != nil {
			return RunResult{}, receiptErr
		}
		if len(unknownEffects) != 0 {
			receipt.Status = "uncertain"
			seen := make(map[protocol.ActivityID]struct{}, len(receipt.UnknownEffects)+len(unknownEffects))
			for _, activityID := range receipt.UnknownEffects {
				seen[activityID] = struct{}{}
			}
			for _, activityID := range unknownEffects {
				seen[activityID] = struct{}{}
			}
			receipt.UnknownEffects = receipt.UnknownEffects[:0]
			for activityID := range seen {
				receipt.UnknownEffects = append(receipt.UnknownEffects, activityID)
			}
			sort.Slice(receipt.UnknownEffects, func(i, j int) bool { return receipt.UnknownEffects[i] < receipt.UnknownEffects[j] })
		}
		terminalValues = append(terminalValues, struct {
			kind    string
			payload any
		}{protocol.EventSubagentReceipt, receipt})
	}
	turnEvents, err := s.turnEvents(*state, request.Runtime.ID, "turn-failure-terminal", terminalValues)
	if err != nil {
		return RunResult{}, err
	}
	events = append(events, turnEvents...)
	if err := s.append(ctx, state, "turn-failure-terminal", events); err != nil {
		return RunResult{}, err
	}
	if state.head != finalCursor {
		return RunResult{}, fmt.Errorf("failure terminal cursor prediction mismatch")
	}
	state.activeActivityID, state.activeStarted, state.activeDispatched, state.activeRequiresToolResult, state.terminal = "", false, false, false, true
	return RunResult{TaskID: state.taskID, TurnID: state.turnID, Cursor: state.head, Status: commandStatus, CommandResult: commandResult}, nil
}

func contextMessages(plan protocol.ContextPlan) []protocol.ModelMessage {
	messages := make([]protocol.ModelMessage, 0, len(plan.Body.Sources))
	for _, source := range plan.Body.Sources {
		role := "system"
		switch source.Kind {
		case "user_message":
			role = "user"
		case "assistant_message":
			role = "assistant"
		case "tool_message":
			role = "tool"
		}
		messages = append(messages, protocol.ModelMessage{Role: role, Blocks: protocol.DeepCopy(source.Content)})
	}
	return messages
}

func toolExposure(manifest protocol.RuntimeGenerationManifest) protocol.ToolExposure {
	exposure := protocol.ToolExposure{CatalogRevision: manifest.Body.ToolCatalogRevision}
	for _, descriptor := range manifest.Body.Tools {
		alias := descriptor.Body.Identity.Name
		exposure.Tools = append(exposure.Tools, protocol.ExposedTool{Alias: alias, Identity: descriptor.Body.Identity, Description: descriptor.Body.Description, InputSchema: protocol.DeepCopy(descriptor.Body.InputSchema)})
		exposure.Aliases = append(exposure.Aliases, protocol.ToolAliasBinding{Alias: alias, Identity: descriptor.Body.Identity, SourceRevision: descriptor.Body.SourceRevision, DescriptorDigest: descriptor.DescriptorDigest})
	}
	return exposure
}

func effectiveToolExposure(request StartTurnRequest) protocol.ToolExposure {
	if request.child != nil {
		return protocol.DeepCopy(request.child.exposure)
	}
	return toolExposure(request.Runtime)
}

func providerAuthorizationRequest(request StartTurnRequest, state *turnState, activityID protocol.ActivityID, callID string, modelRequest protocol.ModelRequest, contextPlanDigest protocol.Digest) (protocol.AuthorizationRequest, error) {
	requestDigest, err := canonicaljson.Digest(modelRequest)
	if err != nil {
		return protocol.AuthorizationRequest{}, err
	}
	dispatchDigest, err := canonicaljson.Digest(struct {
		RequestDigest       protocol.Digest              `json:"request_digest"`
		ContextPlanDigest   protocol.Digest              `json:"context_plan_digest"`
		ProviderPlanDigest  protocol.Digest              `json:"provider_plan_digest"`
		RuntimeGenerationID protocol.RuntimeGenerationID `json:"runtime_generation_id"`
	}{requestDigest, contextPlanDigest, modelRequest.Plan.Digest, request.Runtime.ID})
	if err != nil {
		return protocol.AuthorizationRequest{}, err
	}
	descriptorDigest, err := canonicaljson.Digest(modelRequest.Plan.Body.Descriptor)
	if err != nil {
		return protocol.AuthorizationRequest{}, err
	}
	resource := protocol.ResourceTarget{Kind: "model", CanonicalID: string(modelRequest.ProviderID) + "/" + string(modelRequest.ModelID)}
	return protocol.AuthorizationRequest{
		RequestID: stableID("authorization-request", string(request.Command.CommandID), string(activityID)), Principal: request.Command.Actor,
		Actor: protocol.ActorRef{ID: "orchestrator", Kind: protocol.ActorAgent}, SessionID: request.SessionID, TaskID: state.taskID, TurnID: state.turnID,
		ActivityID: activityID, CallID: callID, QueueID: string(state.turnID),
		Source:         protocol.ToolIdentity{Source: "provider", Authority: string(modelRequest.ProviderID), Name: string(modelRequest.ModelID)},
		SourceRevision: modelRequest.Plan.Body.Descriptor.SourceRevision, DescriptorDigest: descriptorDigest, Action: "provider.stream",
		Resources: []protocol.ResourceTarget{resource}, ExecutionLocus: "remote", RequestedProfile: "network", EffectiveProfile: "network",
		Effect: "egress", Boundary: "network", Reversibility: "not_reversible", VerificationCoverage: "provider_terminal",
		RuntimeGenerationID: request.Runtime.ID, PolicyGeneration: request.Runtime.Body.PolicyGeneration,
		PolicyProvenance: []protocol.PolicyProvenance{{Source: "runtime", Revision: request.Runtime.Body.ProviderCatalogRevision, Generation: request.Runtime.Body.PolicyGeneration}},
		PlanDigest:       modelRequest.Plan.Digest, RequestDigest: requestDigest, DispatchDigest: dispatchDigest,
	}, nil
}

func toolAuthorizationRequest(request StartTurnRequest, state *turnState, planRequest tooling.PlanRequest, plan protocol.ActionPlan, evidence []protocol.Digest) (protocol.AuthorizationRequest, error) {
	requestDigest, err := canonicaljson.Digest(planRequest)
	if err != nil {
		return protocol.AuthorizationRequest{}, err
	}
	dispatchDigest, err := canonicaljson.Digest(struct {
		PlanDigest          protocol.Digest
		RequestDigest       protocol.Digest
		Resources           []protocol.ResourceTarget
		ExecutionLocus      string
		Effect              string
		Boundary            string
		RequestedProfile    string
		EffectiveProfile    string
		RuntimeGenerationID protocol.RuntimeGenerationID
		EvidenceDigests     []protocol.Digest
	}{plan.Digest, requestDigest, plan.Body.Resources, plan.Body.ExecutionLocus, plan.Body.Effect, plan.Body.Boundary, plan.Body.RequestedProfile, plan.Body.EffectiveProfile, plan.Body.RuntimeGenerationID, evidence})
	if err != nil {
		return protocol.AuthorizationRequest{}, err
	}
	return protocol.AuthorizationRequest{
		RequestID: stableID("authorization-request", string(request.Command.CommandID), string(planRequest.ActivityID)), Principal: request.Command.Actor,
		Actor: protocol.ActorRef{ID: "orchestrator", Kind: protocol.ActorAgent}, SessionID: request.SessionID, TaskID: state.taskID,
		TurnID: state.turnID, ActivityID: planRequest.ActivityID, CallID: planRequest.CallID, QueueID: string(state.turnID),
		Source: plan.Body.Tool, SourceRevision: plan.Body.SourceRevision, DescriptorDigest: plan.Body.DescriptorDigest,
		Action: plan.Body.Action, Resources: protocol.DeepCopy(plan.Body.Resources), ExecutionLocus: plan.Body.ExecutionLocus,
		RequestedProfile: plan.Body.RequestedProfile, EffectiveProfile: plan.Body.EffectiveProfile, Effect: plan.Body.Effect,
		Boundary: plan.Body.Boundary, Reversibility: plan.Body.Reversibility, VerificationCoverage: plan.Body.VerificationCoverage,
		RuntimeGenerationID: request.Runtime.ID, PolicyGeneration: request.Runtime.Body.PolicyGeneration,
		PolicyProvenance: []protocol.PolicyProvenance{{Source: "runtime", Revision: request.Runtime.Body.ToolCatalogRevision, Generation: request.Runtime.Body.PolicyGeneration}},
		PlanDigest:       plan.Digest, RequestDigest: requestDigest, DispatchDigest: dispatchDigest,
	}, nil
}

func activityPlan(plan protocol.ActionPlan, purpose string, inputs []protocol.EvidenceID) protocol.ActivityPlannedV1 {
	inputs = append([]protocol.EvidenceID(nil), inputs...)
	sort.Slice(inputs, func(i, j int) bool { return inputs[i] < inputs[j] })
	return protocol.ActivityPlannedV1{
		Kind: "tool", Purpose: purpose, PurposeActor: protocol.ActorRef{ID: "orchestrator", Kind: protocol.ActorAgent},
		Source: plan.Body.Tool.Source, Plan: &plan, InputEvidenceIDs: inputs,
		RequestedProfile: plan.Body.RequestedProfile, EffectiveProfile: plan.Body.EffectiveProfile,
	}
}

func toolDescriptor(manifest protocol.RuntimeGenerationManifest, alias string) (protocol.ToolDescriptor, bool) {
	for _, descriptor := range manifest.Body.Tools {
		if descriptor.Body.Identity.Name == alias {
			return protocol.DeepCopy(descriptor), true
		}
	}
	return protocol.ToolDescriptor{}, false
}

func evidenceIDs(records []protocol.EvidenceRecord) []protocol.EvidenceID {
	ids := make([]protocol.EvidenceID, len(records))
	for index, record := range records {
		ids[index] = record.Body.ID
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func evidenceDigests(records []protocol.EvidenceRecord) []protocol.Digest {
	digests := make([]protocol.Digest, 0, len(records))
	for _, record := range records {
		candidate := protocol.Digest{Algorithm: protocol.DigestSHA256, Value: string(record.Body.ID)}
		if candidate.Validate() != nil {
			candidate = record.Digest
		}
		digests = append(digests, candidate)
	}
	return digests
}

func (s *Service) collectProviderStream(ctx context.Context, generation protocol.RuntimeGenerationID, stream <-chan protocol.ModelEvent) (protocol.AssistantMessageV1, protocol.ProviderAttemptTerminalV1, error) {
	if stream == nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, fmt.Errorf("provider returned nil stream")
	}
	message := protocol.AssistantMessageV1{}
	terminal := protocol.ProviderAttemptTerminalV1{Status: "succeeded", Usage: unknownUsage()}
	var err error
	type pendingDelta struct {
		blockID string
		kind    string
		text    strings.Builder
		json    strings.Builder
		stream  StreamingSanitizer
	}
	var pending pendingDelta
	flushDelta := func() error {
		if pending.blockID == "" {
			return nil
		}
		switch pending.kind {
		case protocol.ContentText:
			trailing, err := pending.stream.Close()
			if err != nil {
				return err
			}
			pending.text.WriteString(trailing)
			if pending.text.Len() != 0 {
				message.Blocks = append(message.Blocks, protocol.ContentBlock{Kind: protocol.ContentText, Text: pending.text.String()})
			}
		case protocol.ContentJSON:
			raw := json.RawMessage(pending.json.String())
			redacted, err := s.deps.Admission.SanitizeJSON(ctx, generation, raw)
			if err != nil {
				return err
			}
			message.Blocks = append(message.Blocks, protocol.ContentBlock{Kind: protocol.ContentJSON, JSON: redacted})
		default:
			return fmt.Errorf("unsupported provider delta kind %q", pending.kind)
		}
		pending = pendingDelta{}
		return nil
	}
	var sequence uint64
	for {
		select {
		case <-ctx.Done():
			return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, ctx.Err()
		case event, ok := <-stream:
			if !ok {
				if terminal.TerminalReason == "" {
					if err := ctx.Err(); err != nil {
						return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
					}
					return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, fmt.Errorf("provider stream closed without terminal")
				}
				if err := flushDelta(); err != nil {
					return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
				}
				if len(message.Blocks) == 0 {
					return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, fmt.Errorf("provider produced no durable assistant content")
				}
				return message, terminal, nil
			}
			if event.Sequence != sequence+1 {
				return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, fmt.Errorf("provider event sequence is not contiguous")
			}
			sequence = event.Sequence
			if err := event.Validate(); err != nil {
				return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
			}
			if event.Kind != protocol.ModelEventContentDelta {
				if err := flushDelta(); err != nil {
					return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
				}
			}
			switch event.Kind {
			case protocol.ModelEventContentDelta:
				if pending.blockID != "" && (pending.blockID != event.Delta.BlockID || pending.kind != event.Delta.Kind) {
					if err := flushDelta(); err != nil {
						return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
					}
				}
				if pending.blockID == "" {
					pending.blockID, pending.kind = event.Delta.BlockID, event.Delta.Kind
					if pending.kind == protocol.ContentText {
						pending.stream, err = s.deps.Admission.OpenTextStream(ctx, generation)
						if err != nil {
							return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
						}
					}
				}
				if event.Delta.Kind == protocol.ContentText {
					admitted, err := pending.stream.Write(event.Delta.Text)
					if err != nil {
						return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
					}
					pending.text.WriteString(admitted)
				} else {
					pending.json.WriteString(event.Delta.JSONFragment)
				}
			case protocol.ModelEventContentBlock:
				block, err := s.sanitizeContentBlock(ctx, generation, *event.Block)
				if err != nil {
					return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
				}
				if len(message.Blocks) == 0 || !reflect.DeepEqual(message.Blocks[len(message.Blocks)-1], block) {
					message.Blocks = append(message.Blocks, block)
				}
			case protocol.ModelEventToolIntent:
				intent := protocol.DeepCopy(*event.ToolIntent)
				intent.CallID, err = s.deps.Admission.SanitizeText(ctx, generation, intent.CallID)
				if err == nil {
					intent.Alias, err = s.deps.Admission.SanitizeText(ctx, generation, intent.Alias)
				}
				if err == nil {
					intent.Arguments, err = s.deps.Admission.SanitizeJSON(ctx, generation, intent.Arguments)
				}
				if err != nil {
					return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
				}
				message.ToolIntents = append(message.ToolIntents, intent)
				message.Blocks = append(message.Blocks, protocol.ContentBlock{Kind: protocol.ContentToolUse, ToolUse: &intent})
			case protocol.ModelEventUsageUpdate:
				terminal.Usage, err = s.sanitizeModelUsage(ctx, generation, *event.Usage)
				if err != nil {
					return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
				}
			case protocol.ModelEventTerminal:
				terminal.TerminalReason, err = s.deps.Admission.SanitizeText(ctx, generation, event.Terminal.Reason)
				if err != nil {
					return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
				}
				terminal.NativeReason, err = s.deps.Admission.SanitizeText(ctx, generation, event.Terminal.NativeReason)
				if err != nil {
					return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
				}
				terminal.ServerRequestID, err = s.deps.Admission.SanitizeText(ctx, generation, event.Terminal.ServerRequestID)
				if err != nil {
					return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
				}
			case protocol.ModelEventError:
				code, sanitizeErr := s.deps.Admission.SanitizeText(ctx, generation, event.Error.Code)
				if sanitizeErr != nil {
					return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, sanitizeErr
				}
				messageText, sanitizeErr := s.deps.Admission.SanitizeText(ctx, generation, event.Error.Message)
				if sanitizeErr != nil {
					return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, sanitizeErr
				}
				return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, &providerFailureError{code: code, message: messageText}
			}
		}
	}
}

func (s *Service) sanitizeContentBlock(ctx context.Context, generation protocol.RuntimeGenerationID, block protocol.ContentBlock) (protocol.ContentBlock, error) {
	block = protocol.DeepCopy(block)
	var err error
	switch block.Kind {
	case protocol.ContentText:
		block.Text, err = s.deps.Admission.SanitizeText(ctx, generation, block.Text)
	case protocol.ContentJSON:
		block.JSON, err = s.deps.Admission.SanitizeJSON(ctx, generation, block.JSON)
	case protocol.ContentReasoningSummary:
		block.ReasoningSummary, err = s.deps.Admission.SanitizeText(ctx, generation, block.ReasoningSummary)
	case protocol.ContentRefusal:
		block.Refusal.Code, err = s.deps.Admission.SanitizeText(ctx, generation, block.Refusal.Code)
		if err == nil {
			block.Refusal.Message, err = s.deps.Admission.SanitizeText(ctx, generation, block.Refusal.Message)
		}
	case protocol.ContentToolUse:
		block.ToolUse.CallID, err = s.deps.Admission.SanitizeText(ctx, generation, block.ToolUse.CallID)
		if err == nil {
			block.ToolUse.Alias, err = s.deps.Admission.SanitizeText(ctx, generation, block.ToolUse.Alias)
		}
		if err == nil {
			block.ToolUse.Arguments, err = s.deps.Admission.SanitizeJSON(ctx, generation, block.ToolUse.Arguments)
		}
	case protocol.ContentToolResult:
		block.ToolResult.CallID, err = s.deps.Admission.SanitizeText(ctx, generation, block.ToolResult.CallID)
		if err == nil {
			block.ToolResult.Status, err = s.deps.Admission.SanitizeText(ctx, generation, block.ToolResult.Status)
		}
		if err == nil {
			block.ToolResult.Text, err = s.deps.Admission.SanitizeText(ctx, generation, block.ToolResult.Text)
		}
		if err == nil && block.ToolResult.JSON != nil {
			block.ToolResult.JSON, err = s.deps.Admission.SanitizeJSON(ctx, generation, block.ToolResult.JSON)
		}
		for index := 0; err == nil && index < len(block.ToolResult.EvidenceIDs); index++ {
			var admitted string
			admitted, err = s.deps.Admission.SanitizeText(ctx, generation, string(block.ToolResult.EvidenceIDs[index]))
			block.ToolResult.EvidenceIDs[index] = protocol.EvidenceID(admitted)
		}
	case protocol.ContentReferenceKind:
		block.Reference.URI, err = s.deps.Admission.SanitizeText(ctx, generation, block.Reference.URI)
		if err == nil {
			block.Reference.Name, err = s.deps.Admission.SanitizeText(ctx, generation, block.Reference.Name)
		}
		if err == nil {
			block.Reference.MediaType, err = s.deps.Admission.SanitizeText(ctx, generation, block.Reference.MediaType)
		}
	}
	if err != nil {
		return protocol.ContentBlock{}, err
	}
	if err := block.Validate(); err != nil {
		return protocol.ContentBlock{}, fmt.Errorf("sanitized provider block: %w", err)
	}
	return block, nil
}

func (s *Service) sanitizeModelUsage(ctx context.Context, generation protocol.RuntimeGenerationID, usage protocol.ModelUsage) (protocol.ModelUsage, error) {
	usage = protocol.DeepCopy(usage)
	values := []*protocol.UsageValue{&usage.Input, &usage.Output, &usage.Cached, &usage.CacheWrite, &usage.Reasoning}
	for _, value := range values {
		admitted, err := s.deps.Admission.SanitizeText(ctx, generation, value.Provenance)
		if err != nil {
			return protocol.ModelUsage{}, err
		}
		value.Provenance = admitted
	}
	if err := usage.Validate(); err != nil {
		return protocol.ModelUsage{}, fmt.Errorf("sanitized provider usage: %w", err)
	}
	return usage, nil
}

func unknownUsage() protocol.ModelUsage {
	unknown := protocol.UsageValue{State: protocol.UsageUnknown}
	return protocol.ModelUsage{Input: unknown, Output: unknown, Cached: unknown, CacheWrite: unknown, Reasoning: unknown}
}

type turnState struct {
	ref               protocol.JournalRef
	head              protocol.CommittedCursor
	command           CommandMetadata
	taskID            protocol.TaskID
	turnID            protocol.TurnID
	contractID        protocol.OutcomeContractID
	contextPlanDigest protocol.Digest
	accepted          bool
	taskRunning       bool
	terminal          bool
	activeActivityID  protocol.ActivityID
	activeStarted     bool
	activeDispatched  bool
	// Once terminal result assembly begins, generic turn-failure cleanup must
	// leave this activity unresolved rather than commit a resultless terminal.
	activeRequiresToolResult bool
	compactedSources         map[protocol.Digest]struct{}
	subagentAttempts         int
	subagentDepth            int
	activeSubagent           bool
}

type authorizationDeniedError struct{ reason string }

func (e *authorizationDeniedError) Error() string {
	if e.reason == "" {
		return "authorization denied"
	}
	return "authorization denied: " + e.reason
}

type providerFailureError struct {
	code    string
	message string
}

func (e *providerFailureError) Error() string {
	return fmt.Sprintf("provider error %s: %s", e.code, e.message)
}

func newTurnState(request StartTurnRequest) turnState {
	state := turnState{
		ref:  protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)},
		head: request.ExpectedHead, command: request.Command,
		taskID:           protocol.TaskID(stableID("task", string(request.Command.CommandID))),
		turnID:           protocol.TurnID(stableID("turn", string(request.Command.CommandID))),
		contractID:       protocol.OutcomeContractID(stableID("contract", string(request.Command.CommandID))),
		compactedSources: make(map[protocol.Digest]struct{}),
	}
	if request.child != nil {
		state.taskID, state.turnID = request.child.manifest.ChildTaskID, request.child.manifest.ChildTurnID
		state.subagentDepth = 1
	}
	return state
}

func (s *Service) initialTurnEvents(request StartTurnRequest, state turnState) ([]protocol.ProposedEvent, error) {
	criterion := protocol.CriterionV1{
		ID: "legacy_turn_terminal", Required: true, Description: "legacy turn reaches a durable terminal state",
		VerificationMethod: "v1_compatibility", ExpectedEvidenceKind: "legacy_turn_terminal",
	}
	values := []struct {
		kind    string
		payload any
	}{
		{protocol.EventCommandAccepted, protocol.CommandAcceptedV1{CommandID: request.Command.CommandID, RequestDigest: request.Command.RequestDigest, IdempotencyKey: request.Command.IdempotencyKey}},
		{protocol.EventTaskCreated, protocol.TaskCreatedV1{Goal: request.Prompt, OutcomeContractID: state.contractID, ContractVersion: 1}},
		{protocol.EventUserMessage, protocol.UserMessageV1{Content: request.Prompt}},
		{protocol.EventOutcomeContractDeclared, protocol.OutcomeContractDeclaredV1{OutcomeContractID: state.contractID, Version: 1, Goal: request.Prompt, Source: "v1_compatibility", Frozen: false, Criteria: []protocol.CriterionV1{criterion}}},
		{protocol.EventTurnAccepted, protocol.TurnAcceptedV1{CommandID: request.Command.CommandID, Goal: request.Prompt, OutcomeContractID: state.contractID, ContractVersion: 1}},
	}
	events, err := s.turnEvents(state, request.Runtime.ID, "turn-accepted", values)
	if err != nil {
		return nil, err
	}
	commandActor := protocol.DeepCopy(request.Command.Actor)
	events[0].Actor, events[2].Actor = &commandActor, &commandActor
	return events, nil
}

func (s *Service) contractFreezeEvents(request StartTurnRequest, state turnState) ([]protocol.ProposedEvent, error) {
	criterion := protocol.CriterionV1{
		ID: "legacy_turn_terminal", Required: true, Description: "legacy turn reaches a durable terminal state",
		VerificationMethod: "v1_compatibility", ExpectedEvidenceKind: "legacy_turn_terminal",
	}
	values := []struct {
		kind    string
		payload any
	}{
		{protocol.EventOutcomeContractAmended, protocol.OutcomeContractAmendedV1{
			OutcomeContractID: state.contractID, FromVersion: 1, ToVersion: 2, Reason: "freeze compatibility contract",
			Actor: protocol.ActorRef{ID: "orchestrator", Kind: protocol.ActorSystem}, Criteria: []protocol.CriterionV1{criterion}, Frozen: true,
		}},
		{protocol.EventTaskStatusChanged, protocol.TaskStatusChangedV1{From: string(protocol.TaskPending), To: string(protocol.TaskRunning), Reason: "compatibility contract frozen"}},
		{protocol.EventTurnStateChanged, protocol.TurnStateChangedV1{From: string(protocol.TurnAccepted), To: string(protocol.TurnRunning), Reason: "compatibility contract frozen"}},
	}
	return s.turnEvents(state, request.Runtime.ID, "contract-frozen", values)
}

func (s *Service) turnEvents(state turnState, generation protocol.RuntimeGenerationID, label string, values []struct {
	kind    string
	payload any
}) ([]protocol.ProposedEvent, error) {
	events := make([]protocol.ProposedEvent, 0, len(values))
	actor := protocol.ActorRef{ID: "orchestrator", Kind: protocol.ActorSystem}
	for index, value := range values {
		payload, err := canonicaljson.Marshal(value.payload)
		if err != nil {
			return nil, err
		}
		event := protocol.ProposedEvent{
			EventID: protocol.EventID(stableID("event", string(state.command.CommandID), label, fmt.Sprint(index), value.kind)),
			Time:    time.Now().UTC(), PayloadVersion: 1, Kind: value.kind, SessionID: protocol.SessionID(state.ref.ID),
			TaskID: state.taskID, TurnID: state.turnID, Actor: &actor, RuntimeGenerationID: generation, Payload: payload,
		}
		events = append(events, event)
	}
	return events, nil
}

func (s *Service) activityEvents(state turnState, generation protocol.RuntimeGenerationID, activityID protocol.ActivityID, label string, values []struct {
	kind    string
	payload any
}) ([]protocol.ProposedEvent, error) {
	events, err := s.turnEvents(state, generation, label, values)
	if err != nil {
		return nil, err
	}
	for index := range events {
		events[index].ActivityID = activityID
	}
	return events, nil
}

func eventID(commandID protocol.CommandID, label string, index int, kind string) protocol.EventID {
	return protocol.EventID(stableID("event", string(commandID), label, fmt.Sprint(index), kind))
}

func (s *Service) append(ctx context.Context, state *turnState, label string, events []protocol.ProposedEvent) error {
	transactionID := protocol.TransactionID(stableID("transaction", string(state.command.CommandID), label))
	validate := validateProposedEvents
	for _, event := range events {
		if event.Kind == protocol.EventContextCompacted || event.Kind == protocol.EventSubagentRequested || event.Kind == protocol.EventSubagentReceipt {
			validate = func(events []protocol.ProposedEvent, ref protocol.JournalRef) error {
				return validateProposedEventsAt(events, ref, state.head.CommitSeq+1, transactionID)
			}
			break
		}
	}
	if err := validate(events, state.ref); err != nil {
		return err
	}
	result, err := s.appendBatch(ctx, journal.AppendRequest{
		Journal: state.ref, ExpectedHead: state.head, TransactionID: transactionID, Events: events,
	})
	if err != nil {
		return err
	}
	switch result.Status {
	case journal.AppendCommitted:
		state.head = result.Cursor
		if s.publisher != nil {
			if err := s.publisher.PublishCommitted(ctx, state.ref, result.Cursor, result.Events); err != nil {
				return err
			}
		}
		return nil
	case journal.AppendConflict:
		return fmt.Errorf("journal expected-head conflict")
	case journal.AppendRecoveryRequired:
		return journal.ErrTurnRecoveryRequired
	case journal.AppendCommitUnknown:
		return fmt.Errorf("journal commit outcome is unknown")
	default:
		return fmt.Errorf("invalid journal append status %q", result.Status)
	}
}

func (s *Service) cross(ctx context.Context, barrier Barrier, state BarrierState) error {
	if err := s.probe.Before(ctx, barrier, state); err != nil {
		return err
	}
	return s.probe.After(ctx, barrier, state)
}

func (s turnState) barrierState() BarrierState {
	return BarrierState{Journal: s.ref, Cursor: s.head, CommandID: s.command.CommandID, TaskID: s.taskID, TurnID: s.turnID}
}

func stableID(prefix string, parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}
	return prefix + "-" + hex.EncodeToString(hash.Sum(nil)[:16])
}

func (s *Service) LookupCommand(ctx context.Context, ref protocol.JournalRef, commandID protocol.CommandID, digest protocol.Digest) (protocol.CommandResult, bool, error) {
	if err := ref.Validate(); err != nil || commandID == "" || digest.Validate() != nil {
		return protocol.CommandResult{}, false, fmt.Errorf("invalid command lookup")
	}
	accepted := false
	after := protocol.CommittedCursor{}
	var projectionHead protocol.CommittedCursor
	for {
		page, err := s.repository.ReadRange(ctx, journal.ReadRangeRequest{Journal: ref, After: after, Limit: 1000})
		if err != nil {
			return protocol.CommandResult{}, false, err
		}
		if projectionHead == (protocol.CommittedCursor{}) {
			projectionHead = page.Head
		} else if page.Head != projectionHead {
			return protocol.CommandResult{}, false, fmt.Errorf("command projection head changed during lookup")
		}
		for _, record := range page.Events {
			switch record.Envelope.Kind {
			case protocol.EventCommandAccepted:
				var payload protocol.CommandAcceptedV1
				if err := json.Unmarshal(record.Envelope.Payload, &payload); err != nil || payload.CommandID != commandID {
					continue
				}
				if payload.RequestDigest != digest {
					return protocol.CommandResult{}, false, ErrIdempotencyConflict
				}
				accepted = true
			case protocol.EventCommandCompleted:
				var payload protocol.CommandCompletedV1
				if err := json.Unmarshal(record.Envelope.Payload, &payload); err != nil || payload.CommandID != commandID {
					continue
				}
				if payload.RequestDigest != digest {
					return protocol.CommandResult{}, false, ErrIdempotencyConflict
				}
				var result protocol.CommandResult
				if err := json.Unmarshal(payload.Result, &result); err != nil {
					return protocol.CommandResult{}, false, err
				}
				return protocol.DeepCopy(result), true, nil
			}
		}
		if !page.More {
			break
		}
		if page.Cursor == after || page.Cursor == (protocol.CommittedCursor{}) {
			return protocol.CommandResult{}, false, fmt.Errorf("command projection pagination did not advance")
		}
		after = page.Cursor
	}
	if !accepted {
		return protocol.CommandResult{}, false, nil
	}
	payload, _ := canonicaljson.Marshal(struct {
		Status string `json:"status"`
	}{"accepted"})
	return protocol.CommandResult{
		ProtocolVersion: protocol.ApplicationProtocolVersion, CommandID: commandID, Status: "accepted", RequestDigest: digest,
		PayloadVersion: 1, Payload: payload,
	}, true, nil
}

func (s *Service) CommitPureCommand(ctx context.Context, command CommandMetadata, completion PureCommandCompletion) (protocol.CommandResult, error) {
	if err := validateCommandMetadata(command); err != nil {
		return protocol.CommandResult{}, fmt.Errorf("command: %w", err)
	}
	if err := completion.Journal.Validate(); err != nil {
		return protocol.CommandResult{}, err
	}
	if err := validateExpectedHead(completion.ExpectedHead, completion.Journal); err != nil {
		return protocol.CommandResult{}, err
	}
	if completion.PayloadVersion == 0 || protocol.ValidateRawJSON(completion.Payload) != nil {
		return protocol.CommandResult{}, fmt.Errorf("pure command completion payload is invalid")
	}
	if completion.Error != nil && (completion.Error.Code == "" || completion.Error.Message == "") {
		return protocol.CommandResult{}, fmt.Errorf("pure command public error is invalid")
	}
	transactionID := protocol.TransactionID(stableID("transaction", string(command.CommandID), "pure-command"))
	finalCursor := protocol.CommittedCursor{
		JournalKind: completion.Journal.Kind, JournalID: completion.Journal.ID,
		CommitSeq: completion.ExpectedHead.CommitSeq + 3, TransactionID: transactionID,
	}
	status := "completed"
	if completion.Error != nil {
		status = "failed"
	}
	result := protocol.CommandResult{
		ProtocolVersion: protocol.ApplicationProtocolVersion, CommandID: command.CommandID, Status: status, RequestDigest: command.RequestDigest,
		PayloadVersion: completion.PayloadVersion, Payload: protocol.DeepCopy(completion.Payload), Error: protocol.DeepCopy(completion.Error),
	}
	if completion.Journal.Kind == protocol.JournalWorkspaceControl {
		result.Cursor.WorkspaceControl = finalCursor
	} else {
		result.Cursor.SelectedSession = &finalCursor
	}
	rawResult, err := canonicaljson.Marshal(result)
	if err != nil {
		return protocol.CommandResult{}, err
	}
	actor := protocol.DeepCopy(command.Actor)
	sessionID := protocol.SessionID("")
	if completion.Journal.Kind == protocol.JournalSession {
		sessionID = protocol.SessionID(completion.Journal.ID)
	}
	now := time.Now().UTC()
	events := []protocol.ProposedEvent{
		{
			EventID: eventID(command.CommandID, "pure-command", 0, protocol.EventCommandAccepted), Time: now, PayloadVersion: 1,
			Kind: protocol.EventCommandAccepted, SessionID: sessionID, Actor: &actor,
			Payload: mustCanonical(protocol.CommandAcceptedV1{CommandID: command.CommandID, RequestDigest: command.RequestDigest, IdempotencyKey: command.IdempotencyKey}),
		},
		{
			EventID: eventID(command.CommandID, "pure-command", 1, protocol.EventCommandCompleted), Time: now, PayloadVersion: 1,
			Kind: protocol.EventCommandCompleted, SessionID: sessionID, Actor: &actor,
			Payload: mustCanonical(protocol.CommandCompletedV1{CommandID: command.CommandID, RequestDigest: command.RequestDigest, Status: status, Result: rawResult, Error: protocol.DeepCopy(completion.Error)}),
		},
	}
	appendResult, err := s.appendBatch(ctx, journal.AppendRequest{
		Journal: completion.Journal, ExpectedHead: completion.ExpectedHead, TransactionID: transactionID, Events: events,
	})
	if err != nil {
		return protocol.CommandResult{}, err
	}
	if appendResult.Status == journal.AppendCommitted {
		if appendResult.Cursor != finalCursor {
			return protocol.CommandResult{}, fmt.Errorf("pure command cursor prediction mismatch")
		}
		if s.publisher != nil {
			if err := s.publisher.PublishCommitted(ctx, completion.Journal, appendResult.Cursor, appendResult.Events); err != nil {
				return protocol.CommandResult{}, err
			}
		}
		return result, nil
	}
	if appendResult.Status == journal.AppendConflict {
		winning, ok, lookupErr := s.LookupCommand(ctx, completion.Journal, command.CommandID, command.RequestDigest)
		if lookupErr != nil {
			return protocol.CommandResult{}, lookupErr
		}
		if ok {
			return winning, nil
		}
		return protocol.CommandResult{}, ErrIdempotencyConflict
	}
	return protocol.CommandResult{}, fmt.Errorf("pure command append status %q", appendResult.Status)
}

func (s *Service) CommitSessionChange(ctx context.Context, request SessionChangeRequest) (protocol.CommandResult, error) {
	if err := validateSessionChangeRequest(request); err != nil {
		return protocol.CommandResult{}, err
	}
	if s.deps.Admission == nil {
		return protocol.CommandResult{}, fmt.Errorf("generation admission service is required")
	}
	admittedPayload, err := s.deps.Admission.SanitizeJSON(ctx, request.RuntimeGenerationID, request.Event.Payload)
	if err != nil {
		return protocol.CommandResult{}, err
	}
	request.Event.Payload = admittedPayload
	if err := validateSessionChangeRequest(request); err != nil {
		return protocol.CommandResult{}, fmt.Errorf("admitted session change: %w", err)
	}
	var lease OperationLease
	err = nil
	if request.Consequential {
		lease, err = s.lane.Acquire(ctx, OperationClaim{Kind: OperationControl, SessionID: request.SessionID, ControlOperationID: request.OperationID})
		if err != nil {
			return protocol.CommandResult{}, err
		}
		defer lease.Release()
	}
	if durable, ok, lookupErr := s.LookupCommand(ctx, request.Journal, request.Command.CommandID, request.Command.RequestDigest); lookupErr != nil {
		return protocol.CommandResult{}, lookupErr
	} else if ok {
		return durable, nil
	}
	commitDelta := uint64(4)
	if strings.HasPrefix(string(request.ExpectedHead.TransactionID), "legacy:") {
		commitDelta++
	}
	finalCursor := protocol.CommittedCursor{
		JournalKind: request.Journal.Kind, JournalID: request.Journal.ID,
		CommitSeq: request.ExpectedHead.CommitSeq + commitDelta, TransactionID: request.TransactionID,
	}
	payload, err := canonicaljson.Marshal(struct {
		OperationID protocol.ControlOperationID `json:"operation_id"`
		EventKind   string                      `json:"event_kind"`
	}{request.OperationID, request.Event.Kind})
	if err != nil {
		return protocol.CommandResult{}, err
	}
	result := protocol.CommandResult{
		ProtocolVersion: protocol.ApplicationProtocolVersion, CommandID: request.Command.CommandID, Status: "completed",
		RequestDigest: request.Command.RequestDigest, Cursor: applicationCursor(request.Journal, finalCursor), PayloadVersion: 1, Payload: payload,
	}
	rawResult, err := canonicaljson.Marshal(result)
	if err != nil {
		return protocol.CommandResult{}, err
	}
	actor := protocol.DeepCopy(request.Command.Actor)
	events := []protocol.ProposedEvent{
		{
			EventID: eventID(request.Command.CommandID, "session-change", 0, protocol.EventCommandAccepted), Time: request.Event.Time,
			PayloadVersion: 1, Kind: protocol.EventCommandAccepted, SessionID: request.SessionID, Actor: &actor, RuntimeGenerationID: request.RuntimeGenerationID,
			Payload: mustCanonical(protocol.CommandAcceptedV1{CommandID: request.Command.CommandID, RequestDigest: request.Command.RequestDigest, IdempotencyKey: request.Command.IdempotencyKey}),
		},
		protocol.CloneProposedEvent(request.Event),
		{
			EventID: eventID(request.Command.CommandID, "session-change", 2, protocol.EventCommandCompleted), Time: request.Event.Time,
			PayloadVersion: 1, Kind: protocol.EventCommandCompleted, SessionID: request.SessionID, Actor: &actor, RuntimeGenerationID: request.RuntimeGenerationID,
			Payload: mustCanonical(protocol.CommandCompletedV1{CommandID: request.Command.CommandID, RequestDigest: request.Command.RequestDigest, Status: "completed", Result: rawResult}),
		},
	}
	if err := validateProposedEvents(events, request.Journal); err != nil {
		return protocol.CommandResult{}, err
	}
	appendResult, err := s.appendBatch(ctx, journal.AppendRequest{Journal: request.Journal, ExpectedHead: request.ExpectedHead, TransactionID: request.TransactionID, Events: events})
	if err != nil {
		return protocol.CommandResult{}, err
	}
	if appendResult.Status == journal.AppendCommitted {
		if appendResult.Cursor != finalCursor {
			return protocol.CommandResult{}, fmt.Errorf("session change cursor prediction mismatch")
		}
		if s.publisher != nil {
			if err := s.publisher.PublishCommitted(ctx, request.Journal, appendResult.Cursor, appendResult.Events); err != nil {
				return protocol.CommandResult{}, err
			}
		}
		return result, nil
	}
	if appendResult.Status == journal.AppendConflict {
		if durable, ok, lookupErr := s.LookupCommand(ctx, request.Journal, request.Command.CommandID, request.Command.RequestDigest); lookupErr != nil {
			return protocol.CommandResult{}, lookupErr
		} else if ok {
			return durable, nil
		}
		return protocol.CommandResult{}, ErrIdempotencyConflict
	}
	return protocol.CommandResult{}, fmt.Errorf("session change append status %q", appendResult.Status)
}

func (s *Service) RunControl(ctx context.Context, request ControlRequest) (result ControlResult, runErr error) {
	if err := validateControlRequest(request, false); err != nil {
		return ControlResult{}, err
	}
	if s.deps.Admission == nil {
		return ControlResult{}, fmt.Errorf("generation admission service is required")
	}
	if request.Event.Kind == protocol.EventMigrationDiagnostic || request.Event.Kind == protocol.EventRecoveryDiagnostic {
		admitted, err := s.deps.Admission.SanitizeJSON(ctx, request.Runtime.ID, request.Event.Payload)
		if err != nil {
			return ControlResult{}, err
		}
		request.Event.Payload = admitted
		if err := validateControlRequest(request, false); err != nil {
			return ControlResult{}, fmt.Errorf("admitted control event: %w", err)
		}
	}
	lease, err := s.lane.Acquire(ctx, OperationClaim{Kind: request.Kind, ControlOperationID: request.OperationID})
	if err != nil {
		return ControlResult{}, err
	}
	defer lease.Release()
	if durable, ok, lookupErr := s.LookupCommand(ctx, request.Journal, request.Command.CommandID, request.Command.RequestDigest); lookupErr != nil {
		return ControlResult{}, lookupErr
	} else if ok {
		return ControlResult{OperationID: request.OperationID, Cursor: commandResultCursor(durable), Status: durable.Status, Error: durable.Error, CommandResult: durable}, nil
	}
	if s.deps.Authorization == nil {
		return ControlResult{}, fmt.Errorf("authorization service is required")
	}
	state := controlState{ref: request.Journal, head: request.ExpectedHead, command: request.Command, operationID: request.OperationID}
	defer func() {
		if runErr == nil || !state.accepted || state.terminal {
			return
		}
		terminalResult, terminalErr := s.terminalizeControlFailure(context.WithoutCancel(ctx), request, &state, runErr)
		if terminalErr != nil {
			runErr = errors.Join(runErr, terminalErr)
			return
		}
		result = terminalResult
	}()
	authorizationRequest, err := controlAuthorizationRequest(request)
	if err != nil {
		return ControlResult{}, err
	}
	planned := []struct {
		kind    string
		payload any
	}{
		{protocol.EventCommandAccepted, protocol.CommandAcceptedV1{CommandID: request.Command.CommandID, RequestDigest: request.Command.RequestDigest, IdempotencyKey: request.Command.IdempotencyKey}},
		{protocol.EventControlOperationPlanned, protocol.ControlOperationPlannedV1{ControlOperationID: request.OperationID, Kind: string(request.Kind), Purpose: request.Plan.Body.Purpose, Plan: request.Plan}},
		{protocol.EventAuthorizationRequested, protocol.AuthorizationRequestedV1{Request: authorizationRequest}},
	}
	events, err := s.controlEvents(state, request.Runtime.ID, "control-planned", planned)
	if err != nil {
		return ControlResult{}, err
	}
	plannedHead := state.head
	if err := s.appendControl(ctx, &state, "control-planned", events, ""); err != nil {
		state.accepted = state.head != plannedHead
		return ControlResult{}, err
	}
	state.accepted = true
	token, err := s.authorizeControl(ctx, request, &state, authorizationRequest)
	if err != nil {
		return ControlResult{}, err
	}
	binding := authorization.DispatchBinding{
		Kind: "control", HandleID: stableID("control-handle", string(request.OperationID)), ControlOperationID: request.OperationID,
		CallID: authorizationRequest.CallID, PlanDigest: authorizationRequest.PlanDigest, RequestDigest: authorizationRequest.RequestDigest,
		DispatchDigest: authorizationRequest.DispatchDigest, RuntimeGenerationID: request.Runtime.ID,
	}
	settingControl := isSessionSettingEvent(request.Event.Kind)
	terminalEventCount := uint64(3)
	if settingControl {
		terminalEventCount = 2
	}
	finalCursor := protocol.CommittedCursor{
		JournalKind: request.Journal.Kind, JournalID: request.Journal.ID,
		CommitSeq: state.head.CommitSeq + terminalEventCount + 1, TransactionID: request.TransactionID,
	}
	var consequentialCursor *protocol.CommittedCursor
	if settingControl {
		commitAdvance := uint64(2)
		if strings.HasPrefix(string(request.ConsequentialExpectedHead.TransactionID), "legacy:") {
			commitAdvance++
		}
		cursor := protocol.CommittedCursor{
			JournalKind: request.ConsequentialJournal.Kind, JournalID: request.ConsequentialJournal.ID,
			CommitSeq:     request.ConsequentialExpectedHead.CommitSeq + commitAdvance,
			TransactionID: protocol.TransactionID(stableID("transaction", string(request.Command.CommandID), "control-consequential")),
		}
		consequentialCursor = &cursor
	}
	payload := mustCanonical(struct {
		OperationID protocol.ControlOperationID `json:"operation_id"`
		Status      string                      `json:"status"`
	}{request.OperationID, "completed"})
	commandResult := protocol.CommandResult{
		ProtocolVersion: protocol.ApplicationProtocolVersion, CommandID: request.Command.CommandID, Status: "completed",
		RequestDigest: request.Command.RequestDigest, Cursor: controlApplicationCursor(finalCursor, consequentialCursor), PayloadVersion: 1, Payload: payload,
	}
	rawResult, err := canonicaljson.Marshal(commandResult)
	if err != nil {
		return ControlResult{}, err
	}
	terminal := []struct {
		kind    string
		payload any
	}{}
	if !settingControl {
		terminal = append(terminal, struct {
			kind    string
			payload any
		}{request.Event.Kind, json.RawMessage(request.Event.Payload)})
	}
	terminal = append(terminal,
		struct {
			kind    string
			payload any
		}{protocol.EventControlOperationCompleted, protocol.ControlOperationTerminalV1{ControlOperationID: request.OperationID, Status: "completed"}},
		struct {
			kind    string
			payload any
		}{protocol.EventCommandCompleted, protocol.CommandCompletedV1{CommandID: request.Command.CommandID, RequestDigest: request.Command.RequestDigest, Status: "completed", Result: rawResult}},
	)
	events, err = s.controlEvents(state, request.Runtime.ID, "control-terminal", terminal)
	if err != nil {
		return ControlResult{}, err
	}
	if !settingControl {
		// Retain the caller's admitted event identity and actor while keeping the
		// lifecycle event ordering authored by the orchestrator.
		events[0] = protocol.CloneProposedEvent(request.Event)
	}
	barrierState := BarrierState{Journal: state.ref, Cursor: state.head, CommandID: request.Command.CommandID, ControlOperationID: request.OperationID, PlanDigest: request.Plan.Digest}
	if err := s.cross(ctx, BarrierAuthorizationCommitted, barrierState); err != nil {
		return ControlResult{}, err
	}
	if err := s.probe.Before(ctx, BarrierEffectDispatch, barrierState); err != nil {
		return ControlResult{}, err
	}
	dispatchHead := state.head
	if err := s.deps.Authorization.Dispatch(ctx, token, binding, func(runContext context.Context) error {
		if settingControl {
			if err := s.appendControlConsequence(runContext, request, *consequentialCursor); err != nil {
				return err
			}
		}
		appendErr := s.appendControl(runContext, &state, "control-terminal", events, request.TransactionID)
		if state.head != dispatchHead {
			state.terminal = true
		}
		return appendErr
	}); err != nil {
		return ControlResult{}, err
	}
	if err := s.probe.After(ctx, BarrierEffectDispatch, barrierState); err != nil {
		return ControlResult{}, err
	}
	if state.head != finalCursor {
		return ControlResult{}, fmt.Errorf("control terminal cursor prediction mismatch")
	}
	return ControlResult{OperationID: request.OperationID, Cursor: state.head, Status: "completed", CommandResult: commandResult}, nil
}

func (s *Service) terminalizeControlFailure(ctx context.Context, request ControlRequest, state *controlState, cause error) (ControlResult, error) {
	operationStatus, commandStatus := "failed", "failed"
	eventKind, code, message := protocol.EventControlOperationFailed, "control_failed", "control operation failed"
	var denied *authorizationDeniedError
	if errors.As(cause, &denied) {
		commandStatus, code, message = "denied", "authorization_denied", "control authorization denied"
	} else if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		operationStatus, commandStatus = "interrupted", "interrupted"
		eventKind, code, message = protocol.EventControlOperationInterrupted, "control_interrupted", "control operation interrupted"
	}
	transactionID := protocol.TransactionID(stableID("transaction", string(request.Command.CommandID), "control-failure-terminal"))
	finalCursor := protocol.CommittedCursor{
		JournalKind: state.ref.Kind, JournalID: state.ref.ID,
		CommitSeq: state.head.CommitSeq + 3, TransactionID: transactionID,
	}
	publicError := &protocol.PublicError{Code: code, Message: message, Retryable: false}
	payload := mustCanonical(struct {
		OperationID protocol.ControlOperationID `json:"operation_id"`
		Status      string                      `json:"status"`
	}{request.OperationID, commandStatus})
	commandResult := protocol.CommandResult{
		ProtocolVersion: protocol.ApplicationProtocolVersion, CommandID: request.Command.CommandID, Status: commandStatus,
		RequestDigest: request.Command.RequestDigest, Cursor: applicationCursor(state.ref, finalCursor), PayloadVersion: 1,
		Payload: payload, Error: publicError,
	}
	rawResult, err := canonicaljson.Marshal(commandResult)
	if err != nil {
		return ControlResult{}, err
	}
	events, err := s.controlEvents(*state, request.Runtime.ID, "control-failure-terminal", []struct {
		kind    string
		payload any
	}{
		{eventKind, protocol.ControlOperationTerminalV1{ControlOperationID: request.OperationID, Status: operationStatus, ErrorCode: code}},
		{protocol.EventCommandCompleted, protocol.CommandCompletedV1{
			CommandID: request.Command.CommandID, RequestDigest: request.Command.RequestDigest,
			Status: commandStatus, Result: rawResult, Error: publicError,
		}},
	})
	if err != nil {
		return ControlResult{}, err
	}
	if err := s.appendControl(ctx, state, "control-failure-terminal", events, transactionID); err != nil {
		return ControlResult{}, err
	}
	if state.head != finalCursor {
		return ControlResult{}, fmt.Errorf("control failure cursor prediction mismatch")
	}
	state.terminal = true
	return ControlResult{OperationID: request.OperationID, Cursor: state.head, Status: commandStatus, Error: publicError, CommandResult: commandResult}, nil
}

type controlState struct {
	ref         protocol.JournalRef
	head        protocol.CommittedCursor
	command     CommandMetadata
	operationID protocol.ControlOperationID
	accepted    bool
	terminal    bool
}

func (s *Service) controlEvents(state controlState, generation protocol.RuntimeGenerationID, label string, values []struct {
	kind    string
	payload any
}) ([]protocol.ProposedEvent, error) {
	events := make([]protocol.ProposedEvent, 0, len(values))
	actor := protocol.ActorRef{ID: "orchestrator", Kind: protocol.ActorControl}
	for index, value := range values {
		var payload json.RawMessage
		if raw, ok := value.payload.(json.RawMessage); ok {
			payload = protocol.DeepCopy(raw)
		} else {
			encoded, err := canonicaljson.Marshal(value.payload)
			if err != nil {
				return nil, err
			}
			payload = encoded
		}
		eventActor := actor
		if value.kind == protocol.EventCommandAccepted {
			eventActor = protocol.DeepCopy(state.command.Actor)
		}
		events = append(events, protocol.ProposedEvent{
			EventID: eventID(state.command.CommandID, label, index, value.kind), Time: time.Now().UTC(), PayloadVersion: 1,
			Kind: value.kind, Actor: &eventActor, RuntimeGenerationID: generation, Payload: payload,
		})
	}
	return events, nil
}

func (s *Service) appendBatch(ctx context.Context, request journal.AppendRequest) (journal.AppendResult, error) {
	if request.Compatibility == nil && request.Journal.Kind == protocol.JournalSession && strings.HasPrefix(string(request.ExpectedHead.TransactionID), "legacy:") {
		request.Compatibility = &journal.CompatibilityDeclaration{
			ReaderVersion: protocol.EnvelopeVersion, WriterVersion: protocol.EnvelopeVersion, LegacyHead: request.ExpectedHead,
		}
	}
	result, appendErr := s.repository.AppendBatch(ctx, request)
	if result.Status == journal.AppendCommitUnknown {
		resolved, resolveErr := s.resolveUnknownCommit(context.WithoutCancel(ctx), request, appendErr)
		if resolveErr != nil {
			return result, resolveErr
		}
		result, appendErr = resolved, nil
	}
	if appendErr != nil {
		return result, appendErr
	}
	return result, nil
}

func (s *Service) resolveUnknownCommit(ctx context.Context, request journal.AppendRequest, appendErr error) (journal.AppendResult, error) {
	lookup, lookupErr := s.repository.LookupTransaction(ctx, request.Journal, request.TransactionID)
	if lookupErr != nil || lookup.State == journal.TransactionUnknown {
		return journal.AppendResult{Status: journal.AppendCommitUnknown}, &CommitUncertainError{
			Journal: request.Journal, TransactionID: request.TransactionID, Cause: errors.Join(appendErr, lookupErr),
		}
	}
	if lookup.State == journal.TransactionNotCommitted {
		if appendErr == nil {
			appendErr = fmt.Errorf("journal rejected transaction before commit")
		}
		return journal.AppendResult{Status: journal.AppendCommitUnknown}, appendErr
	}
	if lookup.State != journal.TransactionCommitted || lookup.Cursor.Validate() != nil ||
		lookup.Cursor.JournalKind != request.Journal.Kind || lookup.Cursor.JournalID != request.Journal.ID || lookup.Cursor.TransactionID != request.TransactionID {
		return journal.AppendResult{Status: journal.AppendCommitUnknown}, fmt.Errorf("invalid transaction lookup result for %q", request.TransactionID)
	}
	committed, err := s.repository.ReadCommittedTransaction(ctx, request.Journal, request.TransactionID)
	if err != nil {
		return journal.AppendResult{Status: journal.AppendCommitUnknown}, fmt.Errorf("read proven committed transaction %q: %w", request.TransactionID, err)
	}
	if committed.Journal != request.Journal || committed.TransactionID != request.TransactionID || committed.Cursor != lookup.Cursor {
		return journal.AppendResult{Status: journal.AppendCommitUnknown}, fmt.Errorf("committed transaction proof mismatch for %q", request.TransactionID)
	}
	for _, event := range committed.Events {
		if event.JournalKind != request.Journal.Kind || event.JournalID != request.Journal.ID || event.TransactionID != request.TransactionID {
			return journal.AppendResult{Status: journal.AppendCommitUnknown}, fmt.Errorf("committed transaction event proof mismatch for %q", request.TransactionID)
		}
	}
	return journal.AppendResult{Status: journal.AppendCommitted, Cursor: committed.Cursor, Events: protocol.DeepCopy(committed.Events)}, nil
}

func (s *Service) appendControl(ctx context.Context, state *controlState, label string, events []protocol.ProposedEvent, explicit protocol.TransactionID) error {
	if err := validateProposedEvents(events, state.ref); err != nil {
		return err
	}
	transactionID := explicit
	if transactionID == "" {
		transactionID = protocol.TransactionID(stableID("transaction", string(state.command.CommandID), label))
	}
	result, err := s.appendBatch(ctx, journal.AppendRequest{Journal: state.ref, ExpectedHead: state.head, TransactionID: transactionID, Events: events})
	if err != nil {
		return err
	}
	if result.Status == journal.AppendCommitted {
		state.head = result.Cursor
		if s.publisher != nil {
			return s.publisher.PublishCommitted(ctx, state.ref, result.Cursor, result.Events)
		}
		return nil
	}
	if result.Status == journal.AppendConflict {
		return ErrIdempotencyConflict
	}
	return fmt.Errorf("control append status %q", result.Status)
}

func (s *Service) appendControlConsequence(ctx context.Context, request ControlRequest, expected protocol.CommittedCursor) error {
	if err := validateProposedEvent(request.Event, request.ConsequentialJournal); err != nil {
		return err
	}
	transactionID := protocol.TransactionID(stableID("transaction", string(request.Command.CommandID), "control-consequential"))
	result, err := s.appendBatch(ctx, journal.AppendRequest{
		Journal: request.ConsequentialJournal, ExpectedHead: request.ConsequentialExpectedHead,
		TransactionID: transactionID, Events: []protocol.ProposedEvent{protocol.CloneProposedEvent(request.Event)},
	})
	if err != nil {
		return err
	}
	if result.Status == journal.AppendCommitted {
		if result.Cursor != expected {
			return fmt.Errorf("control consequential cursor prediction mismatch")
		}
		if s.publisher != nil {
			return s.publisher.PublishCommitted(ctx, request.ConsequentialJournal, result.Cursor, result.Events)
		}
		return nil
	}
	if result.Status == journal.AppendConflict {
		return ErrIdempotencyConflict
	}
	return fmt.Errorf("control consequential append status %q", result.Status)
}

func (s *Service) authorizeControl(ctx context.Context, request ControlRequest, state *controlState, authorizationRequest protocol.AuthorizationRequest) (authorization.CommittedToken, error) {
	decision, err := s.deps.Authorization.Decide(ctx, authorizationRequest)
	if err != nil {
		return authorization.CommittedToken{}, err
	}
	if decision.Action == "ask" {
		if s.deps.Approver == nil {
			return authorization.CommittedToken{}, fmt.Errorf("interactive approver is required")
		}
		response, approveErr := s.deps.Approver.Approve(ctx, decision)
		if approveErr != nil {
			return authorization.CommittedToken{}, approveErr
		}
		decision, err = s.deps.Authorization.ResolveInteractive(ctx, authorizationRequest, decision, response)
		if err != nil {
			return authorization.CommittedToken{}, err
		}
	}
	if err := authorization.ValidateBinding(authorizationRequest, decision); err != nil {
		return authorization.CommittedToken{}, err
	}
	decisionEventID := eventID(request.Command.CommandID, "control-decision", 0, protocol.EventAuthorizationDecided)
	if decision.Action != "allow" {
		denied := &authorizationDeniedError{reason: decision.Reason}
		events, eventErr := s.controlEvents(*state, request.Runtime.ID, "control-decision", []struct {
			kind    string
			payload any
		}{{protocol.EventAuthorizationDecided, protocol.AuthorizationDecidedV1{Decision: decision}}})
		if eventErr != nil {
			return authorization.CommittedToken{}, eventErr
		}
		decisionHead := state.head
		if appendErr := s.appendControl(ctx, state, "control-decision", events, ""); appendErr != nil {
			if state.head != decisionHead {
				return authorization.CommittedToken{}, errors.Join(denied, appendErr)
			}
			return authorization.CommittedToken{}, appendErr
		}
		return authorization.CommittedToken{}, denied
	}
	decisionEvents := []struct {
		kind    string
		payload any
	}{
		{protocol.EventAuthorizationDecided, protocol.AuthorizationDecidedV1{Decision: decision}},
		{protocol.EventControlOperationAuthorized, protocol.ControlOperationAuthorizedV1{
			ControlOperationID: request.OperationID, DecisionEventID: decisionEventID, DecisionNonce: decision.DecisionNonce,
			PlanDigest: authorizationRequest.PlanDigest, RequestDigest: authorizationRequest.RequestDigest, DispatchDigest: authorizationRequest.DispatchDigest,
		}},
	}
	events, err := s.controlEvents(*state, request.Runtime.ID, "control-decision", decisionEvents)
	if err != nil {
		return authorization.CommittedToken{}, err
	}
	if err := s.appendControl(ctx, state, "control-decision", events, ""); err != nil {
		return authorization.CommittedToken{}, err
	}
	decisionDigest, err := canonicaljson.Digest(decision)
	if err != nil {
		return authorization.CommittedToken{}, err
	}
	consumedEventID := eventID(request.Command.CommandID, "control-start", 0, protocol.EventAuthorizationDecisionConsumed)
	startedEventID := eventID(request.Command.CommandID, "control-start", 1, protocol.EventControlOperationStarted)
	startEvents := []struct {
		kind    string
		payload any
	}{
		{protocol.EventAuthorizationDecisionConsumed, protocol.AuthorizationDecisionConsumedV1{
			DecisionNonce: decision.DecisionNonce, DecisionEventID: decisionEventID, DecisionDigest: decisionDigest,
			RequestID: authorizationRequest.RequestID, ControlOperationID: request.OperationID, CallID: authorizationRequest.CallID,
			PlanDigest: authorizationRequest.PlanDigest, RequestDigest: authorizationRequest.RequestDigest,
			DispatchDigest: authorizationRequest.DispatchDigest, RuntimeGenerationID: request.Runtime.ID,
		}},
		{protocol.EventControlOperationStarted, protocol.ControlOperationStartedV1{
			ControlOperationID: request.OperationID, DecisionEventID: decisionEventID, DecisionNonce: decision.DecisionNonce,
			PlanDigest: authorizationRequest.PlanDigest, RequestDigest: authorizationRequest.RequestDigest,
			DispatchDigest: authorizationRequest.DispatchDigest, RuntimeGenerationID: request.Runtime.ID,
		}},
	}
	events, err = s.controlEvents(*state, request.Runtime.ID, "control-start", startEvents)
	if err != nil {
		return authorization.CommittedToken{}, err
	}
	if err := s.appendControl(ctx, state, "control-start", events, ""); err != nil {
		return authorization.CommittedToken{}, err
	}
	return s.deps.Authorization.Issue(ctx, authorization.CommitReference{
		Journal: request.Journal, DecisionTransactionID: protocol.TransactionID(stableID("transaction", string(request.Command.CommandID), "control-decision")),
		DecisionEventID: decisionEventID, StartTransactionID: protocol.TransactionID(stableID("transaction", string(request.Command.CommandID), "control-start")),
		ConsumedEventID: consumedEventID, StartedEventID: startedEventID,
	})
}

func controlAuthorizationRequest(request ControlRequest) (protocol.AuthorizationRequest, error) {
	requestDigest := request.Command.RequestDigest
	dispatchDigest, err := canonicaljson.Digest(struct {
		OperationID protocol.ControlOperationID
		PlanDigest  protocol.Digest
		Request     protocol.Digest
		Generation  protocol.RuntimeGenerationID
	}{request.OperationID, request.Plan.Digest, requestDigest, request.Runtime.ID})
	if err != nil {
		return protocol.AuthorizationRequest{}, err
	}
	return protocol.AuthorizationRequest{
		RequestID: stableID("authorization-request", string(request.Command.CommandID), string(request.OperationID)), Principal: request.Command.Actor,
		Actor: protocol.ActorRef{ID: "orchestrator", Kind: protocol.ActorControl}, ControlOperationID: request.OperationID,
		CallID: request.Plan.Body.CallID, QueueID: string(request.OperationID), Source: request.Plan.Body.Tool,
		SourceRevision: request.Plan.Body.SourceRevision, DescriptorDigest: request.Plan.Body.DescriptorDigest,
		Action: request.Plan.Body.Action, Resources: protocol.DeepCopy(request.Plan.Body.Resources), ExecutionLocus: request.Plan.Body.ExecutionLocus,
		RequestedProfile: request.Plan.Body.RequestedProfile, EffectiveProfile: request.Plan.Body.EffectiveProfile,
		Effect: request.Plan.Body.Effect, Boundary: request.Plan.Body.Boundary, Reversibility: request.Plan.Body.Reversibility,
		VerificationCoverage: request.Plan.Body.VerificationCoverage, RuntimeGenerationID: request.Runtime.ID,
		PolicyGeneration: request.Runtime.Body.PolicyGeneration,
		PolicyProvenance: []protocol.PolicyProvenance{{Source: "runtime", Revision: request.Runtime.Body.ToolCatalogRevision, Generation: request.Runtime.Body.PolicyGeneration}},
		PlanDigest:       request.Plan.Digest, RequestDigest: requestDigest, DispatchDigest: dispatchDigest,
	}, nil
}

func validateControlRequest(request ControlRequest, recovery bool) error {
	if err := validateCommandMetadata(request.Command); err != nil {
		return fmt.Errorf("command: %w", err)
	}
	if request.OperationID == "" || request.TransactionID == "" {
		return fmt.Errorf("control operation identity is incomplete")
	}
	if request.Journal.Kind != protocol.JournalWorkspaceControl || request.Journal.Validate() != nil {
		return fmt.Errorf("control operation requires workspace-control journal")
	}
	if err := validateExpectedHead(request.ExpectedHead, request.Journal); err != nil {
		return err
	}
	wantRecovery := request.Kind == OperationRecovery
	if recovery != wantRecovery || (!recovery && request.Kind != OperationControl && request.Kind != OperationCompaction && request.Kind != OperationReloadActivation) {
		return fmt.Errorf("control request kind is inconsistent")
	}
	if err := validateRuntimeManifest(request.Runtime); err != nil {
		return err
	}
	if err := request.Plan.Body.Validate(); err != nil || canonicaljson.ValidateDigest(request.Plan.Body, request.Plan.Digest) != nil || request.Plan.Body.RuntimeGenerationID != request.Runtime.ID {
		return fmt.Errorf("control action plan is invalid")
	}
	settingControl := isSessionSettingEvent(request.Event.Kind)
	if settingControl {
		if request.Kind != OperationControl || request.ConsequentialJournal.Kind != protocol.JournalSession || request.ConsequentialJournal.Validate() != nil ||
			request.Event.SessionID == "" || request.ConsequentialJournal.ID != protocol.JournalID(request.Event.SessionID) {
			return fmt.Errorf("session setting control journal identity mismatch")
		}
		if err := validateExpectedHead(request.ConsequentialExpectedHead, request.ConsequentialJournal); err != nil {
			return fmt.Errorf("consequential expected cursor: %w", err)
		}
		if err := validateProposedEvent(request.Event, request.ConsequentialJournal); err != nil {
			return err
		}
	} else {
		if request.ConsequentialJournal != (protocol.JournalRef{}) || request.ConsequentialExpectedHead != (protocol.CommittedCursor{}) {
			return fmt.Errorf("non-setting control carries a consequential session journal")
		}
		if err := validateProposedEvent(request.Event, request.Journal); err != nil {
			return err
		}
		if request.Event.Kind == protocol.EventProjectSkillTrustChanged {
			if err := validateProjectSkillTrustControl(request); err != nil {
				return err
			}
		}
	}
	wantEvent := map[OperationKind]string{
		OperationControl:          protocol.EventMigrationDiagnostic,
		OperationCompaction:       protocol.EventMigrationDiagnostic,
		OperationRecovery:         protocol.EventRecoveryDiagnostic,
		OperationReloadActivation: protocol.EventRuntimeGenerationActivated,
	}[request.Kind]
	if !settingControl && request.Event.Kind != wantEvent && !(request.Kind == OperationControl && request.Event.Kind == protocol.EventProjectSkillTrustChanged) {
		return fmt.Errorf("control kind %q requires event %q", request.Kind, wantEvent)
	}
	return nil
}

func validateProjectSkillTrustControl(request ControlRequest) error {
	var trust protocol.ProjectSkillTrustChangedV1
	if err := json.Unmarshal(request.Event.Payload, &trust); err != nil || trust.Validate() != nil || protocol.JournalID(trust.WorkspaceID) != request.Journal.ID {
		return fmt.Errorf("project skill trust event binding is invalid")
	}
	body := request.Plan.Body
	if body.Tool != (protocol.ToolIdentity{Source: "runtime", Authority: "yordam", Name: "skill-trust"}) || body.Action != "runtime.skill.trust" || body.Boundary != "workspace_control" || len(body.Resources) != 1 {
		return fmt.Errorf("project skill trust action plan is invalid")
	}
	resource := body.Resources[0]
	if resource.Kind != "skill_catalog" || resource.CanonicalID != string(trust.WorkspaceID) || resource.Digest != trust.CatalogDigest.Algorithm+":"+trust.CatalogDigest.Value || len(resource.Attributes) != 1 || resource.Attributes[0] != (protocol.ResourceAttribute{Name: "decision", Value: trust.Decision}) {
		return fmt.Errorf("project skill trust target binding is invalid")
	}
	return nil
}

func isSessionSettingEvent(kind string) bool {
	switch kind {
	case protocol.EventModeChanged, protocol.EventModelChanged, protocol.EventTrustedExecutionAcknowledged:
		return true
	default:
		return false
	}
}

func validateSessionChangeRequest(request SessionChangeRequest) error {
	if err := validateCommandMetadata(request.Command); err != nil {
		return fmt.Errorf("command: %w", err)
	}
	if request.OperationID == "" || request.TransactionID == "" || request.RuntimeGenerationID == "" || request.SessionID == "" {
		return fmt.Errorf("session change identity is incomplete")
	}
	if request.Journal != (protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)}) {
		return fmt.Errorf("session change journal identity mismatch")
	}
	if err := validateExpectedHead(request.ExpectedHead, request.Journal); err != nil {
		return err
	}
	if err := validateProposedEvent(request.Event, request.Journal); err != nil {
		return err
	}
	if request.Event.Kind != protocol.EventSessionTitleChanged {
		return fmt.Errorf("unsupported session change event %q", request.Event.Kind)
	}
	if request.Consequential {
		return fmt.Errorf("session title change is pure metadata")
	}
	return nil
}

func validateProposedEvent(event protocol.ProposedEvent, ref protocol.JournalRef) error {
	if event.EventID == "" || event.Time.IsZero() || event.PayloadVersion == 0 || event.Kind == "" || event.RuntimeGenerationID == "" {
		return fmt.Errorf("proposed event identity is incomplete")
	}
	if err := protocol.ValidateRawJSON(event.Payload); err != nil {
		return err
	}
	if event.Actor == nil || event.Actor.Validate() != nil {
		return fmt.Errorf("proposed event actor is invalid")
	}
	if ref.Kind == protocol.JournalSession {
		if event.SessionID == "" || protocol.JournalID(event.SessionID) != ref.ID {
			return fmt.Errorf("proposed session event journal mismatch")
		}
	} else if event.SessionID != "" {
		return fmt.Errorf("workspace-control event carries a session ID")
	}
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		return err
	}
	envelope := protocol.EventEnvelope{
		SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: event.PayloadVersion,
		JournalKind: ref.Kind, JournalID: ref.ID, EventID: event.EventID, Seq: 1, Time: event.Time, Kind: event.Kind,
		SessionID: event.SessionID, TaskID: event.TaskID, TurnID: event.TurnID, ActivityID: event.ActivityID,
		ParentActivityID: event.ParentActivityID, CausationEventID: event.CausationEventID, Actor: protocol.DeepCopy(event.Actor),
		RuntimeGenerationID: event.RuntimeGenerationID, TransactionID: "validation", Payload: protocol.DeepCopy(event.Payload),
	}
	raw, err := canonicaljson.Marshal(envelope)
	if err != nil {
		return err
	}
	record, err := registry.Decode(raw)
	if err != nil {
		return err
	}
	return registry.Validate(record)
}

func validateProposedEvents(events []protocol.ProposedEvent, ref protocol.JournalRef) error {
	for index, event := range events {
		if err := validateProposedEvent(event, ref); err != nil {
			return fmt.Errorf("proposed event %d (%s): %w", index, event.Kind, err)
		}
	}
	return nil
}

func applicationCursor(ref protocol.JournalRef, cursor protocol.CommittedCursor) protocol.ApplicationCursor {
	if ref.Kind == protocol.JournalWorkspaceControl {
		return protocol.ApplicationCursor{WorkspaceControl: cursor}
	}
	return protocol.ApplicationCursor{SelectedSession: &cursor}
}

func controlApplicationCursor(workspace protocol.CommittedCursor, selected *protocol.CommittedCursor) protocol.ApplicationCursor {
	return protocol.ApplicationCursor{WorkspaceControl: workspace, SelectedSession: protocol.DeepCopy(selected)}
}

func mustCanonical(value any) json.RawMessage {
	raw, err := canonicaljson.Marshal(value)
	if err != nil {
		panic(err)
	}
	return raw
}

func validateStartTurnRequest(request StartTurnRequest) error {
	if err := validateCommandMetadata(request.Command); err != nil {
		return fmt.Errorf("command: %w", err)
	}
	if request.SessionID == "" {
		return fmt.Errorf("session ID is required")
	}
	if err := validateExpectedHead(request.ExpectedHead, protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)}); err != nil {
		return fmt.Errorf("expected cursor: %w", err)
	}
	if strings.TrimSpace(request.Prompt) == "" {
		return fmt.Errorf("prompt is required")
	}
	if request.Runtime.ID == "" {
		return fmt.Errorf("runtime generation is required")
	}
	if err := request.Runtime.Digest.Validate(); err != nil {
		return fmt.Errorf("runtime generation digest: %w", err)
	}
	if err := validateRuntimeManifest(request.Runtime); err != nil {
		return fmt.Errorf("runtime generation: %w", err)
	}
	if _, ok := selectedRuntimeModel(request); !ok {
		return fmt.Errorf("selected model %q/%q is not in runtime generation", request.ProviderID, request.ModelID)
	}
	return nil
}

func effectiveWorkspaceID(workspaceID protocol.WorkspaceID, sessionID protocol.SessionID) protocol.WorkspaceID {
	if workspaceID != "" {
		return workspaceID
	}
	// Legacy/internal callers created before workspace provenance was carried on
	// orchestration requests remain replayable; production builders always bind it.
	return protocol.WorkspaceID(sessionID)
}

func selectedRuntimeModel(request StartTurnRequest) (protocol.ModelDescriptor, bool) {
	for _, model := range request.Runtime.Body.Models {
		if model.ProviderID == request.ProviderID && model.ModelID == request.ModelID {
			return protocol.DeepCopy(model), true
		}
	}
	return protocol.ModelDescriptor{}, false
}

func validateRuntimeManifest(manifest protocol.RuntimeGenerationManifest) error {
	if len(manifest.Body.Models) == 0 || manifest.Body.Limits.MaxToolCalls > 128 {
		return fmt.Errorf("runtime generation manifest has invalid execution limits or models")
	}
	actor := protocol.ActorRef{ID: "runtime-validator", Kind: protocol.ActorSystem}
	return validateProposedEvent(protocol.ProposedEvent{
		EventID: "runtime-validation", Time: time.Unix(1, 0).UTC(), PayloadVersion: 1,
		Kind: protocol.EventRuntimeGenerationActivated, Actor: &actor, RuntimeGenerationID: manifest.ID,
		Payload: mustCanonical(protocol.RuntimeGenerationActivatedV1{Manifest: manifest}),
	}, protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "runtime-validation"})
}

func validateCommandMetadata(command CommandMetadata) error {
	if command.CommandID == "" || strings.TrimSpace(command.IdempotencyKey) == "" {
		return fmt.Errorf("command identity and idempotency key are required")
	}
	if err := command.RequestDigest.Validate(); err != nil {
		return fmt.Errorf("request digest: %w", err)
	}
	if err := command.Actor.Validate(); err != nil {
		return fmt.Errorf("actor: %w", err)
	}
	return nil
}

func validateExpectedHead(cursor protocol.CommittedCursor, journalRef protocol.JournalRef) error {
	if cursor == (protocol.CommittedCursor{}) && journalRef.Kind == protocol.JournalWorkspaceControl {
		return nil
	}
	if err := cursor.Validate(); err != nil {
		return err
	}
	if cursor.JournalKind != journalRef.Kind || cursor.JournalID != journalRef.ID {
		return fmt.Errorf("cursor journal does not match request journal")
	}
	return nil
}
