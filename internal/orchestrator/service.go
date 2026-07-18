package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	contextplanner "github.com/muratmirgun/yordam/internal/context"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/recovery"
	"github.com/muratmirgun/yordam/internal/tooling"
	"github.com/muratmirgun/yordam/internal/verification"
)

var ErrIdempotencyConflict = errors.New("idempotency_conflict")

type Service struct {
	lane       OperationLane
	repository journal.Repository
	turnLeases journal.TurnLeaseManager
	deps       Dependencies
	publisher  ApplicationEventPublisher
	probe      BarrierProbe
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

func (s *Service) RunTurn(ctx context.Context, request StartTurnRequest) (RunResult, error) {
	if err := validateStartTurnRequest(request); err != nil {
		return RunResult{}, err
	}
	lease, err := s.lane.Acquire(ctx, OperationClaim{Kind: OperationTurn, SessionID: request.SessionID})
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

	initial, err := s.initialTurnEvents(request, state)
	if err != nil {
		return RunResult{}, err
	}
	if err := s.append(ctx, &state, "turn-accepted", initial); err != nil {
		return RunResult{}, err
	}
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
	if err := s.append(ctx, &state, "contract-frozen", freeze); err != nil {
		return RunResult{}, err
	}
	if err := s.cross(ctx, BarrierContractFrozen, state.barrierState()); err != nil {
		return RunResult{}, err
	}
	if s.deps.Context == nil || s.deps.Providers == nil || s.deps.Provider == nil || s.deps.Authorization == nil || s.deps.Verification == nil {
		return RunResult{TaskID: state.taskID, TurnID: state.turnID, Cursor: state.head, Status: "accepted"}, fmt.Errorf("turn orchestration dependencies are incomplete")
	}

	extraMessages := make([]protocol.ModelMessage, 0)
	completedTools := 0
	for attempt := 0; ; attempt++ {
		assistant, terminal, err := s.runProviderActivity(ctx, request, &state, attempt, extraMessages)
		if err != nil {
			return RunResult{}, err
		}
		if len(assistant.ToolIntents) == 0 {
			return s.completeTurn(ctx, request, &state, terminal)
		}
		if completedTools+len(assistant.ToolIntents) > request.Runtime.Body.Limits.MaxToolCalls {
			return RunResult{}, fmt.Errorf("tool call limit %d reached", request.Runtime.Body.Limits.MaxToolCalls)
		}
		if s.deps.Tools == nil || s.deps.Evidence == nil || s.deps.Recovery == nil {
			return RunResult{}, fmt.Errorf("tool orchestration dependencies are incomplete")
		}
		results := make([]protocol.ContentBlock, 0, len(assistant.ToolIntents))
		for _, intent := range assistant.ToolIntents {
			result, runErr := s.runToolIntent(ctx, request, &state, intent)
			if runErr != nil {
				return RunResult{}, runErr
			}
			resultCopy := result
			results = append(results, protocol.ContentBlock{Kind: protocol.ContentToolResult, ToolResult: &resultCopy})
			completedTools++
		}
		extraMessages = []protocol.ModelMessage{{Role: "user", Blocks: results}}
	}
}

func commandResultCursor(result protocol.CommandResult) protocol.CommittedCursor {
	if result.Cursor.SelectedSession != nil {
		return *result.Cursor.SelectedSession
	}
	return result.Cursor.WorkspaceControl
}

func (s *Service) runProviderActivity(ctx context.Context, request StartTurnRequest, state *turnState, attempt int, extra []protocol.ModelMessage) (protocol.AssistantMessageV1, protocol.ProviderAttemptTerminalV1, error) {
	model := request.Runtime.Body.Models[0]
	requirements := []protocol.CapabilityRequirement{}
	page, err := s.repository.ReadRange(ctx, journal.ReadRangeRequest{Journal: state.ref, After: request.ExpectedHead, Limit: protocol.MaxCollectionMembers})
	if err != nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	contextPlan, err := s.deps.Context.Plan(ctx, contextplanner.Request{
		Session: request.SessionID, TaskID: state.taskID, OutcomeContractID: state.contractID, OutcomeContractVersion: 2,
		Events: page.Events, Model: model,
	})
	if err != nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	state.contextPlanDigest = contextPlan.Digest
	plan, err := s.deps.Providers.Negotiate(model.ProviderID, model.ModelID, requirements, request.Runtime.Body.ToolCatalogRevision)
	if err != nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	messages := contextMessages(contextPlan)
	messages = append(messages, protocol.DeepCopy(extra)...)
	modelRequest := protocol.ModelRequest{
		RequestID:  stableID("provider-request", string(request.Command.CommandID), fmt.Sprint(attempt)),
		ProviderID: model.ProviderID, ModelID: model.ModelID, Messages: messages,
		Tools: toolExposure(request.Runtime), Requirements: requirements, Plan: plan,
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
	if err := s.append(ctx, state, label+"-planned", events); err != nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	token, err := s.authorizeActivity(ctx, request, state, activityID, callID, label, authorizationRequest)
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
	stream, err := s.deps.Provider.Stream(ctx, handle, token)
	if err != nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	if err := s.probe.After(ctx, BarrierEffectDispatch, barrierState); err != nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	message, terminal, err := collectProviderStream(ctx, stream)
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
	if err := s.append(ctx, state, label+"-terminal", events); err != nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, err
	}
	return message, terminal, nil
}

func (s *Service) runToolIntent(ctx context.Context, request StartTurnRequest, state *turnState, intent protocol.ToolUseBlock) (protocol.ToolResultBlock, error) {
	descriptor, ok := toolDescriptor(request.Runtime, intent.Alias)
	if !ok {
		return protocol.ToolResultBlock{CallID: intent.CallID, Status: "failed", Text: "unknown tool"}, nil
	}
	mutating := descriptor.Body.Effect != "observation"
	if !mutating {
		return s.runObservationIntent(ctx, request, state, intent)
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
		if err := s.append(ctx, state, previewLabel+"-planned", events); err != nil {
			return protocol.ToolResultBlock{}, err
		}
		barrierState := state.barrierState()
		barrierState.ActivityID, barrierState.PlanDigest = previewActivityID, previewPlan.Digest
		if err := s.cross(ctx, BarrierActionPlanCommitted, barrierState); err != nil {
			return protocol.ToolResultBlock{}, err
		}
		previewToken, err := s.authorizeActivity(ctx, request, state, previewActivityID, intent.CallID, previewLabel, previewAuthorization)
		if err != nil {
			return protocol.ToolResultBlock{}, err
		}
		if err := s.probe.Before(ctx, BarrierEffectDispatch, barrierState); err != nil {
			return protocol.ToolResultBlock{}, err
		}
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
		if err := s.appendActivityEvidence(ctx, request, state, previewActivityID, previewLabel+"-terminal", "succeeded", previewRecords); err != nil {
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
		if err := s.append(ctx, state, mutationLabel+"-planned", events); err != nil {
			return protocol.ToolResultBlock{}, err
		}
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
		if err := s.cross(ctx, BarrierAuthorizationCommitted, barrierState); err != nil {
			return protocol.ToolResultBlock{}, err
		}
		token, err := s.authorizeActivity(ctx, request, state, mutationActivityID, intent.CallID, mutationLabel, mutationAuthorization)
		if err != nil {
			return protocol.ToolResultBlock{}, err
		}
		if err := s.probe.Before(ctx, BarrierEffectDispatch, barrierState); err != nil {
			return protocol.ToolResultBlock{}, err
		}
		execution, executeErr := s.deps.Tools.Execute(ctx, mutationHandle, token)
		if executeErr != nil {
			status := "uncertain"
			if s.deps.EffectProbe != nil {
				if noEffect, probeErr := s.deps.EffectProbe.ProvesNoEffect(ctx, mutationActivityID); probeErr == nil && noEffect {
					status = "interrupted_no_effect"
				}
			}
			if appendErr := s.appendActivityEvidence(context.WithoutCancel(ctx), request, state, mutationActivityID, mutationLabel+"-terminal", status, nil); appendErr != nil {
				return protocol.ToolResultBlock{}, errors.Join(executeErr, appendErr)
			}
			return protocol.ToolResultBlock{CallID: intent.CallID, Status: status, Text: executeErr.Error()}, nil
		}
		if err := s.probe.After(ctx, BarrierEffectDispatch, barrierState); err != nil {
			return protocol.ToolResultBlock{}, err
		}
		records, err := s.recordEvidence(ctx, request, mutationActivityID, mutationPlan, execution.Evidence)
		if err != nil {
			return protocol.ToolResultBlock{}, err
		}
		status := execution.Outcome.Status
		if status == "" {
			status = "failed"
		}
		if err := s.appendActivityEvidence(ctx, request, state, mutationActivityID, mutationLabel+"-terminal", status, records); err != nil {
			return protocol.ToolResultBlock{}, err
		}
		execution.ToolResult.EvidenceIDs = evidenceIDs(records)
		return execution.ToolResult, nil
	}
	return protocol.ToolResultBlock{CallID: intent.CallID, Status: "failed", Text: "resource drift did not stabilize"}, nil
}

func (s *Service) runObservationIntent(ctx context.Context, request StartTurnRequest, state *turnState, intent protocol.ToolUseBlock) (protocol.ToolResultBlock, error) {
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
	if err := s.append(ctx, state, label+"-planned", events); err != nil {
		return protocol.ToolResultBlock{}, err
	}
	token, err := s.authorizeActivity(ctx, request, state, activityID, intent.CallID, label, authRequest)
	if err != nil {
		return protocol.ToolResultBlock{}, err
	}
	execution, err := s.deps.Tools.Execute(ctx, handle, token)
	if err != nil {
		if appendErr := s.appendActivityEvidence(context.WithoutCancel(ctx), request, state, activityID, label+"-terminal", "uncertain", nil); appendErr != nil {
			return protocol.ToolResultBlock{}, errors.Join(err, appendErr)
		}
		return protocol.ToolResultBlock{CallID: intent.CallID, Status: "uncertain", Text: err.Error()}, nil
	}
	records, err := s.recordEvidence(ctx, request, activityID, plan, execution.Evidence)
	if err != nil {
		return protocol.ToolResultBlock{}, err
	}
	if err := s.appendActivityEvidence(ctx, request, state, activityID, label+"-terminal", execution.Outcome.Status, records); err != nil {
		return protocol.ToolResultBlock{}, err
	}
	execution.ToolResult.EvidenceIDs = evidenceIDs(records)
	return execution.ToolResult, nil
}

func (s *Service) recordEvidence(ctx context.Context, request StartTurnRequest, activityID protocol.ActivityID, plan protocol.ActionPlan, candidates []protocol.EvidenceCandidate) ([]protocol.EvidenceRecord, error) {
	records := make([]protocol.EvidenceRecord, 0, len(candidates))
	for index, candidate := range candidates {
		candidate = protocol.DeepCopy(candidate)
		if candidate.ID == "" {
			candidate.ID = protocol.EvidenceID(stableID("evidence", string(request.Command.CommandID), string(activityID), fmt.Sprint(index)))
		}
		candidate.WorkspaceID = protocol.WorkspaceID(request.SessionID)
		candidate.SessionID = request.SessionID
		candidate.ProducingActivityID = activityID
		if candidate.Actor.Validate() != nil {
			candidate.Actor = protocol.ActorRef{ID: protocol.ActorID(plan.Body.Tool.Name), Kind: protocol.ActorTool}
		}
		if candidate.Subject.Validate() != nil {
			candidate.Subject = protocol.SubjectRef{Kind: "action", ID: plan.Body.CallID}
		}
		record, err := s.deps.Evidence.Put(ctx, candidate)
		if err != nil {
			return nil, err
		}
		if record.Body.ID != candidate.ID || record.Body.ProducingActivityID != activityID {
			return nil, fmt.Errorf("evidence recorder returned mismatched identity")
		}
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Body.ID < records[j].Body.ID })
	return records, nil
}

func (s *Service) appendActivityEvidence(ctx context.Context, request StartTurnRequest, state *turnState, activityID protocol.ActivityID, label, status string, records []protocol.EvidenceRecord) error {
	kind := map[string]string{
		"succeeded": protocol.EventActivitySucceeded, "failed": protocol.EventActivityFailed,
		"denied": protocol.EventActivityDenied, "cancelled": protocol.EventActivityCancelled,
		"interrupted_no_effect": protocol.EventActivityInterruptedNoEffect, "uncertain": protocol.EventActivityUncertain,
	}[status]
	if kind == "" {
		kind, status = protocol.EventActivityFailed, "failed"
	}
	values := make([]struct {
		kind    string
		payload any
	}, 0, 1+len(records)*2)
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
	if err := s.append(ctx, state, label, events); err != nil {
		return err
	}
	barrierState := state.barrierState()
	barrierState.ActivityID = activityID
	return s.cross(ctx, BarrierActionTerminalCommitted, barrierState)
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
		candidate, candidateErr := s.deps.Tools.RecoveryCandidate(ctx, preview, body, plan)
		if candidateErr != nil {
			return protocol.CheckpointBody{}, s.failCheckpoint(ctx, request, state, activityID, label, plan.Digest, candidateErr)
		}
		candidate.ActivityID, candidate.CheckpointID, candidate.PlanDigest = activityID, body.ID, plan.Digest
		material, putErr := s.deps.Recovery.Put(ctx, candidate)
		if errors.Is(putErr, recovery.ErrSecretDetected) {
			body.Coverage[0].Class = "drift_detectable"
		} else if putErr != nil {
			return protocol.CheckpointBody{}, s.failCheckpoint(ctx, request, state, activityID, label, plan.Digest, putErr)
		} else {
			if material.ID == "" || material.ActivityID != activityID || material.CheckpointID != body.ID || material.Body.PlanDigest != plan.Digest {
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

func (s *Service) failCheckpoint(ctx context.Context, request StartTurnRequest, state *turnState, activityID protocol.ActivityID, label string, planDigest protocol.Digest, cause error) error {
	failed := []struct {
		kind    string
		payload any
	}{{protocol.EventCheckpointFailed, protocol.CheckpointFailedV1{PlanDigest: planDigest, ErrorCode: "checkpoint_failed", Reason: cause.Error()}}}
	events, err := s.activityEvents(*state, request.Runtime.ID, activityID, label+"-checkpoint-failed", failed)
	if err != nil {
		return errors.Join(cause, err)
	}
	if err := s.append(context.WithoutCancel(ctx), state, label+"-checkpoint-failed", events); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (s *Service) authorizeActivity(ctx context.Context, request StartTurnRequest, state *turnState, activityID protocol.ActivityID, callID, label string, authorizationRequest protocol.AuthorizationRequest) (authorization.CommittedToken, error) {
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
	if decision.Action != "allow" {
		return authorization.CommittedToken{}, fmt.Errorf("authorization denied: %s", decision.Reason)
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
	if err := s.append(ctx, state, label+"-start", events); err != nil {
		return authorization.CommittedToken{}, err
	}
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

func (s *Service) completeTurn(ctx context.Context, request StartTurnRequest, state *turnState, _ protocol.ProviderAttemptTerminalV1) (RunResult, error) {
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
	finalCursor := protocol.CommittedCursor{JournalKind: state.ref.Kind, JournalID: state.ref.ID, CommitSeq: state.head.CommitSeq + 4, TransactionID: finalTransactionID}
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
	events, err = s.turnEvents(*state, request.Runtime.ID, "turn-terminal", terminalEvents)
	if err != nil {
		return RunResult{}, err
	}
	if err := s.append(ctx, state, "turn-terminal", events); err != nil {
		return RunResult{}, err
	}
	if state.head != finalCursor {
		return RunResult{}, fmt.Errorf("terminal command cursor prediction mismatch")
	}
	if err := s.cross(ctx, BarrierTurnTerminalCommitted, state.barrierState()); err != nil {
		return RunResult{}, err
	}
	if err := s.cross(ctx, BarrierCommandCompleted, state.barrierState()); err != nil {
		return RunResult{}, err
	}
	return RunResult{TaskID: state.taskID, TurnID: state.turnID, Cursor: state.head, Status: "completed", CommandResult: commandResult}, nil
}

func contextMessages(plan protocol.ContextPlan) []protocol.ModelMessage {
	messages := make([]protocol.ModelMessage, 0, len(plan.Body.Sources))
	for _, source := range plan.Body.Sources {
		role := "system"
		if source.Kind == "user_message" {
			role = "user"
		} else if source.Kind == "assistant_message" {
			role = "assistant"
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
		Effect: "external", Boundary: "network", Reversibility: "not_reversible", VerificationCoverage: "provider_terminal",
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

func collectProviderStream(ctx context.Context, stream <-chan protocol.ModelEvent) (protocol.AssistantMessageV1, protocol.ProviderAttemptTerminalV1, error) {
	if stream == nil {
		return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, fmt.Errorf("provider returned nil stream")
	}
	message := protocol.AssistantMessageV1{}
	terminal := protocol.ProviderAttemptTerminalV1{Status: "succeeded", Usage: unknownUsage()}
	var text strings.Builder
	var sequence uint64
	for {
		select {
		case <-ctx.Done():
			return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, ctx.Err()
		case event, ok := <-stream:
			if !ok {
				if terminal.TerminalReason == "" {
					return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, fmt.Errorf("provider stream closed without terminal")
				}
				if text.Len() != 0 {
					message.Blocks = append(message.Blocks, protocol.ContentBlock{Kind: protocol.ContentText, Text: text.String()})
				}
				if len(message.Blocks) == 0 && len(message.ToolIntents) != 0 {
					for _, intent := range message.ToolIntents {
						copyIntent := protocol.DeepCopy(intent)
						message.Blocks = append(message.Blocks, protocol.ContentBlock{Kind: protocol.ContentToolUse, ToolUse: &copyIntent})
					}
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
			switch event.Kind {
			case protocol.ModelEventContentDelta:
				text.WriteString(event.Delta.Text)
			case protocol.ModelEventContentBlock:
				message.Blocks = append(message.Blocks, protocol.DeepCopy(*event.Block))
			case protocol.ModelEventToolIntent:
				message.ToolIntents = append(message.ToolIntents, protocol.DeepCopy(*event.ToolIntent))
			case protocol.ModelEventUsageUpdate:
				terminal.Usage = protocol.DeepCopy(*event.Usage)
			case protocol.ModelEventTerminal:
				terminal.TerminalReason, terminal.NativeReason, terminal.ServerRequestID = event.Terminal.Reason, event.Terminal.NativeReason, event.Terminal.ServerRequestID
			case protocol.ModelEventError:
				return protocol.AssistantMessageV1{}, protocol.ProviderAttemptTerminalV1{}, fmt.Errorf("provider error %s: %s", event.Error.Code, event.Error.Message)
			}
		}
	}
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
}

func newTurnState(request StartTurnRequest) turnState {
	return turnState{
		ref:  protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)},
		head: request.ExpectedHead, command: request.Command,
		taskID:     protocol.TaskID(stableID("task", string(request.Command.CommandID))),
		turnID:     protocol.TurnID(stableID("turn", string(request.Command.CommandID))),
		contractID: protocol.OutcomeContractID(stableID("contract", string(request.Command.CommandID))),
	}
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
	return s.turnEvents(state, request.Runtime.ID, "turn-accepted", values)
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
	result, err := s.repository.AppendBatch(ctx, journal.AppendRequest{
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
	page, err := s.repository.ReadRange(ctx, journal.ReadRangeRequest{Journal: ref, Limit: protocol.MaxCollectionMembers})
	if err != nil {
		return protocol.CommandResult{}, false, err
	}
	accepted := false
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
	appendResult, err := s.repository.AppendBatch(ctx, journal.AppendRequest{
		Journal: completion.Journal, ExpectedHead: completion.ExpectedHead, TransactionID: transactionID, Events: events,
	})
	if err != nil {
		return protocol.CommandResult{}, err
	}
	if appendResult.Status == journal.AppendCommitted {
		if appendResult.Cursor != finalCursor {
			return protocol.CommandResult{}, fmt.Errorf("pure command cursor prediction mismatch")
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
	var lease OperationLease
	var err error
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
	finalCursor := protocol.CommittedCursor{
		JournalKind: request.Journal.Kind, JournalID: request.Journal.ID,
		CommitSeq: request.ExpectedHead.CommitSeq + 4, TransactionID: request.TransactionID,
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
	appendResult, err := s.repository.AppendBatch(ctx, journal.AppendRequest{Journal: request.Journal, ExpectedHead: request.ExpectedHead, TransactionID: request.TransactionID, Events: events})
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

func (s *Service) RunControl(ctx context.Context, request ControlRequest) (ControlResult, error) {
	if err := validateControlRequest(request, false); err != nil {
		return ControlResult{}, err
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
	if err := s.appendControl(ctx, &state, "control-planned", events, ""); err != nil {
		return ControlResult{}, err
	}
	token, err := s.authorizeControl(ctx, request, &state, authorizationRequest)
	if err != nil {
		return ControlResult{}, err
	}
	binding := authorization.DispatchBinding{
		Kind: "control", HandleID: stableID("control-handle", string(request.OperationID)), ControlOperationID: request.OperationID,
		CallID: authorizationRequest.CallID, PlanDigest: authorizationRequest.PlanDigest, RequestDigest: authorizationRequest.RequestDigest,
		DispatchDigest: authorizationRequest.DispatchDigest, RuntimeGenerationID: request.Runtime.ID,
	}
	barrierState := BarrierState{Journal: state.ref, Cursor: state.head, CommandID: request.Command.CommandID, ControlOperationID: request.OperationID, PlanDigest: request.Plan.Digest}
	if err := s.probe.Before(ctx, BarrierEffectDispatch, barrierState); err != nil {
		return ControlResult{}, err
	}
	if err := s.deps.Authorization.Dispatch(ctx, token, binding, func(context.Context) error { return nil }); err != nil {
		return ControlResult{}, err
	}
	if err := s.probe.After(ctx, BarrierEffectDispatch, barrierState); err != nil {
		return ControlResult{}, err
	}
	finalCursor := protocol.CommittedCursor{
		JournalKind: request.Journal.Kind, JournalID: request.Journal.ID,
		CommitSeq: state.head.CommitSeq + 4, TransactionID: request.TransactionID,
	}
	payload := mustCanonical(struct {
		OperationID protocol.ControlOperationID `json:"operation_id"`
		Status      string                      `json:"status"`
	}{request.OperationID, "completed"})
	commandResult := protocol.CommandResult{
		ProtocolVersion: protocol.ApplicationProtocolVersion, CommandID: request.Command.CommandID, Status: "completed",
		RequestDigest: request.Command.RequestDigest, Cursor: applicationCursor(request.Journal, finalCursor), PayloadVersion: 1, Payload: payload,
	}
	rawResult, err := canonicaljson.Marshal(commandResult)
	if err != nil {
		return ControlResult{}, err
	}
	terminal := []struct {
		kind    string
		payload any
	}{
		{request.Event.Kind, json.RawMessage(request.Event.Payload)},
		{protocol.EventControlOperationCompleted, protocol.ControlOperationTerminalV1{ControlOperationID: request.OperationID, Status: "completed"}},
		{protocol.EventCommandCompleted, protocol.CommandCompletedV1{CommandID: request.Command.CommandID, RequestDigest: request.Command.RequestDigest, Status: "completed", Result: rawResult}},
	}
	events, err = s.controlEvents(state, request.Runtime.ID, "control-terminal", terminal)
	if err != nil {
		return ControlResult{}, err
	}
	// Retain the caller's admitted event identity and actor while keeping the
	// lifecycle event ordering authored by the orchestrator.
	events[0] = protocol.CloneProposedEvent(request.Event)
	if err := s.appendControl(ctx, &state, "control-terminal", events, request.TransactionID); err != nil {
		return ControlResult{}, err
	}
	if state.head != finalCursor {
		return ControlResult{}, fmt.Errorf("control terminal cursor prediction mismatch")
	}
	return ControlResult{OperationID: request.OperationID, Cursor: state.head, Status: "completed", CommandResult: commandResult}, nil
}

type controlState struct {
	ref         protocol.JournalRef
	head        protocol.CommittedCursor
	command     CommandMetadata
	operationID protocol.ControlOperationID
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
		events = append(events, protocol.ProposedEvent{
			EventID: eventID(state.command.CommandID, label, index, value.kind), Time: time.Now().UTC(), PayloadVersion: 1,
			Kind: value.kind, Actor: &actor, RuntimeGenerationID: generation, Payload: payload,
		})
	}
	return events, nil
}

func (s *Service) appendControl(ctx context.Context, state *controlState, label string, events []protocol.ProposedEvent, explicit protocol.TransactionID) error {
	transactionID := explicit
	if transactionID == "" {
		transactionID = protocol.TransactionID(stableID("transaction", string(state.command.CommandID), label))
	}
	result, err := s.repository.AppendBatch(ctx, journal.AppendRequest{Journal: state.ref, ExpectedHead: state.head, TransactionID: transactionID, Events: events})
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
	if err := authorization.ValidateBinding(authorizationRequest, decision); err != nil || decision.Action != "allow" {
		return authorization.CommittedToken{}, errors.Join(err, fmt.Errorf("control authorization denied"))
	}
	decisionEventID := eventID(request.Command.CommandID, "control-decision", 0, protocol.EventAuthorizationDecided)
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
	if err := validateProposedEvent(request.Event, request.Journal); err != nil {
		return err
	}
	if request.Kind == OperationReloadActivation && request.Event.Kind != protocol.EventRuntimeGenerationActivated {
		return fmt.Errorf("reload activation event kind mismatch")
	}
	return nil
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
	consequential := map[string]bool{
		protocol.EventModeChanged: true, protocol.EventModelChanged: true, protocol.EventTrustedExecutionAcknowledged: true,
		protocol.EventContextCompacted: true,
	}[request.Event.Kind]
	if request.Event.Kind != protocol.EventSessionTitleChanged && !consequential {
		return fmt.Errorf("unsupported session change event %q", request.Event.Kind)
	}
	if consequential && !request.Consequential {
		return fmt.Errorf("session change %q is consequential", request.Event.Kind)
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
	return nil
}

func applicationCursor(ref protocol.JournalRef, cursor protocol.CommittedCursor) protocol.ApplicationCursor {
	if ref.Kind == protocol.JournalWorkspaceControl {
		return protocol.ApplicationCursor{WorkspaceControl: cursor}
	}
	return protocol.ApplicationCursor{SelectedSession: &cursor}
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
	return nil
}

func validateRuntimeManifest(manifest protocol.RuntimeGenerationManifest) error {
	body := manifest.Body
	if body.ProviderCatalogRevision == "" || body.ToolCatalogRevision == "" || body.InstructionRevision == "" || body.PolicyGeneration == "" || len(body.Models) == 0 || body.Limits.MaxToolCalls <= 0 || body.Limits.MaxToolCalls > 128 || body.Limits.ShellTimeoutNanos <= 0 || body.Limits.ApplicationQueueCapacity <= 0 {
		return fmt.Errorf("manifest body is incomplete")
	}
	for _, descriptor := range body.Models {
		if err := descriptor.Validate(); err != nil || descriptor.RuntimeGenerationID != manifest.ID {
			return fmt.Errorf("invalid model descriptor")
		}
	}
	for _, descriptor := range body.Tools {
		if err := descriptor.Body.Validate(); err != nil {
			return err
		}
		if err := canonicaljson.ValidateDigest(descriptor.Body, descriptor.DescriptorDigest); err != nil {
			return err
		}
	}
	return canonicaljson.ValidateDigest(body, manifest.Digest)
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
	if err := cursor.Validate(); err != nil {
		return err
	}
	if cursor.JournalKind != journalRef.Kind || cursor.JournalID != journalRef.ID {
		return fmt.Errorf("cursor journal does not match request journal")
	}
	return nil
}
