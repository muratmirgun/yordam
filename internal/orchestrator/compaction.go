package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/compaction"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/provider"
)

// RunCompaction summarizes a stable, older range of a session. The provider is
// reached only after the authorization decision and activity start are durable.
func (s *Service) RunCompaction(ctx context.Context, request CompactRequest) (result CompactResult, runErr error) {
	if err := validateCompactRequest(request); err != nil {
		return CompactResult{}, err
	}
	if s.deps.Admission == nil || s.deps.Providers == nil || s.deps.Provider == nil || s.deps.Authorization == nil || s.deps.Evidence == nil {
		return CompactResult{}, fmt.Errorf("compaction orchestration dependencies are incomplete")
	}
	operationID := protocol.ControlOperationID(stableID("compaction", string(request.Command.CommandID)))
	lease, err := s.lane.Acquire(ctx, OperationClaim{Kind: OperationCompaction, ControlOperationID: operationID})
	if err != nil {
		return CompactResult{}, err
	}
	defer lease.Release()

	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)}
	if durable, ok, lookupErr := s.LookupCommand(ctx, ref, request.Command.CommandID, request.Command.RequestDigest); lookupErr != nil {
		return CompactResult{}, lookupErr
	} else if ok {
		return compactResultFromCommand(durable)
	}
	inspector, ok := s.repository.(journal.ActiveTurnInspector)
	if !ok {
		return CompactResult{}, fmt.Errorf("compaction requires a durable idle inspector")
	}
	turnID, terminal, inspectErr := inspector.ActiveTurn(ctx, request.SessionID, request.ExpectedHead)
	if inspectErr != nil {
		return CompactResult{}, inspectErr
	}
	if turnID != "" && !terminal {
		return CompactResult{}, fmt.Errorf("compaction requires an idle session")
	}

	history, err := s.readFullHistory(ctx, ref)
	if err != nil {
		return CompactResult{}, err
	}
	selection, err := compaction.Select(history, request.ExpectedHead, request.Trigger, 0)
	if err != nil {
		return CompactResult{}, err
	}
	state := compactionState{ref: ref, head: request.ExpectedHead, command: request.Command}
	defer func() {
		if !state.accepted || state.terminal || runErr == nil || errors.Is(runErr, ErrCommitUncertain) {
			return
		}
		if terminalErr := s.terminalizeCompactionFailure(context.WithoutCancel(ctx), request, &state, runErr); terminalErr != nil {
			runErr = errors.Join(runErr, terminalErr)
		}
	}()
	if err := s.appendCompaction(ctx, &state, "accepted", []protocol.ProposedEvent{compactionCommandAccepted(request)}); err != nil {
		return CompactResult{}, err
	}
	state.accepted = true

	model, _ := selectedCompactRuntimeModel(request)
	plan, err := s.deps.Providers.Negotiate(model.ProviderID, model.ModelID, nil, request.Runtime.Body.ToolCatalogRevision)
	if err != nil {
		return CompactResult{}, err
	}
	modelRequest, err := compaction.BuildSummaryRequest(selection, nil)
	if err != nil {
		return CompactResult{}, err
	}
	modelRequest.RequestID = stableID("compaction-request", string(request.Command.CommandID), selection.SourceDigest.Value, string(request.Runtime.ID))
	modelRequest.ProviderID, modelRequest.ModelID, modelRequest.Plan = model.ProviderID, model.ModelID, plan
	activityID := protocol.ActivityID(stableID("activity", string(request.Command.CommandID), "compaction", selection.SourceDigest.Value, string(request.Runtime.ID)))
	state.activityID = activityID
	callID := stableID("compaction-call", string(request.Command.CommandID), selection.SourceDigest.Value, string(request.Runtime.ID))
	handle, err := s.deps.Provider.Prepare(ctx, activityID, callID, modelRequest, selection.SourceDigest)
	if err != nil {
		return CompactResult{}, err
	}
	authorizationRequest, err := compactionAuthorizationRequest(request, activityID, callID, modelRequest, selection)
	if err != nil {
		return CompactResult{}, err
	}
	planned, err := s.compactionEvents(request, activityID, "planned", []compactionEventValue{
		{protocol.EventActivityPlanned, protocol.ActivityPlannedV1{Kind: "provider", Purpose: "summarize stable context", PurposeActor: protocol.ActorRef{ID: "orchestrator", Kind: protocol.ActorSystem}, Source: "provider", RequestedProfile: "network", EffectiveProfile: "network", CompactionTrigger: string(request.Trigger)}},
		{protocol.EventAuthorizationRequested, protocol.AuthorizationRequestedV1{Request: authorizationRequest}},
	})
	if err != nil {
		return CompactResult{}, err
	}
	if err := s.appendCompaction(ctx, &state, "planned", planned); err != nil {
		return CompactResult{}, err
	}
	state.activityPlanned = true
	token, err := s.authorizeCompaction(ctx, request, &state, activityID, callID, authorizationRequest)
	if err != nil {
		return CompactResult{}, err
	}
	state.activityStarted = true

	stream, err := s.deps.Provider.Stream(ctx, handle, token)
	if err != nil {
		return CompactResult{}, err
	}
	summary, usage, err := s.collectCompactionSummary(ctx, request.Runtime.ID, stream)
	if err != nil {
		return CompactResult{}, err
	}
	revision, err := compaction.Revision(selection, summary)
	if err != nil {
		return CompactResult{}, err
	}
	evidence, err := s.recordCompactionEvidence(ctx, request, activityID, selection, summary)
	if err != nil {
		return CompactResult{}, err
	}
	state.evidence = evidence

	transactionID := protocol.TransactionID(stableID("transaction", string(request.Command.CommandID), selection.SourceDigest.Value, string(request.Runtime.ID)))
	finalCursor := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: state.head.CommitSeq + 4, TransactionID: transactionID}
	result = CompactResult{Cursor: finalCursor, SummaryEvidence: evidence, From: selection.From, Through: selection.Through, Revision: revision, Usage: usage}
	completed, err := compactCommandCompleted(request, result)
	if err != nil {
		return CompactResult{}, err
	}
	final, err := s.compactionEvents(request, activityID, "completed", []compactionEventValue{
		{protocol.EventActivitySucceeded, protocol.ActivityOutcomeV1{Status: "succeeded", OutputEvidenceIDs: []protocol.EvidenceID{evidence.Body.ID}, Usage: &usage, OutputBytes: evidence.Body.Size}},
		{protocol.EventContextCompacted, protocol.ContextCompactedV1{From: selection.From, Through: selection.Through, SummaryEvidenceID: evidence.Body.ID, Revision: revision}},
		{protocol.EventCommandCompleted, completed},
	})
	if err != nil {
		return CompactResult{}, err
	}
	if err := s.appendCompactionWithTransaction(ctx, &state, transactionID, final); err != nil {
		state.terminal = state.head == finalCursor
		return CompactResult{}, err
	}
	if state.head != finalCursor {
		return CompactResult{}, fmt.Errorf("compaction cursor prediction mismatch")
	}
	state.terminal = true
	return result, nil
}

type compactionState struct {
	ref              protocol.JournalRef
	head             protocol.CommittedCursor
	command          CommandMetadata
	accepted         bool
	terminal         bool
	activityID       protocol.ActivityID
	activityPlanned  bool
	activityStarted  bool
	activityTerminal bool
	evidence         protocol.EvidenceRecord
}

// compactWithinTurn compacts one safe source range while the caller retains
// the turn's lane and lease. It deliberately uses the turn state and append
// path directly: calling RunCompaction here would try to acquire both again.
func (s *Service) compactWithinTurn(ctx context.Context, lease managedOperationLease, request StartTurnRequest, state *turnState, head protocol.CommittedCursor, history []protocol.EventRecord) (bool, error) {
	if state == nil || state.head != head {
		return false, fmt.Errorf("automatic compaction turn head changed")
	}
	selection, err := compaction.Select(history, head, compaction.TriggerAutomatic, 0)
	if errors.Is(err, compaction.ErrNothingToCompact) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if _, alreadyCompacted := state.compactedSources[selection.SourceDigest]; alreadyCompacted {
		return false, nil
	}
	model, ok := selectedRuntimeModel(request)
	if !ok {
		return false, fmt.Errorf("selected model %q/%q is not in runtime generation %q", request.ProviderID, request.ModelID, request.Runtime.ID)
	}
	plan, err := s.deps.Providers.Negotiate(model.ProviderID, model.ModelID, nil, request.Runtime.Body.ToolCatalogRevision)
	if err != nil {
		return false, err
	}
	modelRequest, err := compaction.BuildSummaryRequest(selection, nil)
	if err != nil {
		return false, err
	}
	modelRequest.RequestID = stableID("compaction-request", string(request.Command.CommandID), selection.SourceDigest.Value, string(request.Runtime.ID))
	modelRequest.ProviderID, modelRequest.ModelID, modelRequest.Plan = model.ProviderID, model.ModelID, plan
	activityID := protocol.ActivityID(stableID("activity", string(request.Command.CommandID), "automatic-compaction", selection.SourceDigest.Value, string(request.Runtime.ID)))
	callID := stableID("compaction-call", string(request.Command.CommandID), "automatic", selection.SourceDigest.Value, string(request.Runtime.ID))
	label := "automatic-compaction-" + selection.SourceDigest.Value
	compactRequest := CompactRequest{Command: request.Command, WorkspaceID: request.WorkspaceID, SessionID: request.SessionID, ExpectedHead: head, ProviderID: request.ProviderID, ModelID: request.ModelID, Runtime: request.Runtime, Trigger: compaction.TriggerAutomatic}
	handle, err := s.deps.Provider.Prepare(ctx, activityID, callID, modelRequest, selection.SourceDigest)
	if err != nil {
		return false, err
	}
	authorizationRequest, err := providerAuthorizationRequest(request, state, activityID, callID, modelRequest, selection.SourceDigest)
	if err != nil {
		return false, err
	}
	planned, err := s.activityEvents(*state, request.Runtime.ID, activityID, label+"-planned", []struct {
		kind    string
		payload any
	}{
		{protocol.EventActivityPlanned, protocol.ActivityPlannedV1{Kind: "provider", Purpose: "summarize stable context", PurposeActor: protocol.ActorRef{ID: "orchestrator", Kind: protocol.ActorSystem}, Source: "provider", RequestedProfile: "network", EffectiveProfile: "network", CompactionTrigger: string(compaction.TriggerAutomatic)}},
		{protocol.EventAuthorizationRequested, protocol.AuthorizationRequestedV1{Request: authorizationRequest}},
	})
	if err != nil {
		return false, err
	}
	if err := s.append(ctx, state, label+"-planned", planned); err != nil {
		return false, err
	}
	state.activeActivityID, state.activeStarted, state.activeDispatched = activityID, false, false
	token, err := s.authorizeActivity(ctx, lease, request, state, activityID, callID, label, authorizationRequest)
	if err != nil {
		return false, err
	}
	state.activeDispatched = true
	stream, err := s.deps.Provider.Stream(ctx, handle, token)
	if err != nil {
		return false, err
	}
	summary, usage, err := s.collectCompactionSummary(ctx, request.Runtime.ID, stream)
	if err != nil {
		return false, err
	}
	revision, err := compaction.Revision(selection, summary)
	if err != nil {
		return false, err
	}
	evidence, err := s.recordCompactionEvidence(ctx, compactRequest, activityID, selection, summary)
	if err != nil {
		return false, err
	}
	completed, err := s.activityEvents(*state, request.Runtime.ID, activityID, label+"-completed", []struct {
		kind    string
		payload any
	}{
		{protocol.EventActivitySucceeded, protocol.ActivityOutcomeV1{Status: "succeeded", OutputEvidenceIDs: []protocol.EvidenceID{evidence.Body.ID}, Usage: &usage, OutputBytes: evidence.Body.Size}},
		{protocol.EventContextCompacted, protocol.ContextCompactedV1{From: selection.From, Through: selection.Through, SummaryEvidenceID: evidence.Body.ID, Revision: revision}},
	})
	if err != nil {
		return false, err
	}
	if err := s.append(ctx, state, label+"-completed", completed); err != nil {
		return false, err
	}
	state.activeActivityID, state.activeStarted, state.activeDispatched = "", false, false
	state.compactedSources[selection.SourceDigest] = struct{}{}
	return true, nil
}

func automaticCompactionDecision(plan protocol.ContextPlan, limits protocol.RuntimeLimits) (compaction.Decision, error) {
	policy := compaction.Policy{AutoCompact: limits.AutoCompact}
	if limits.CompactReserveTokens != (protocol.ValueInt64{}) {
		if err := limits.CompactReserveTokens.Validate(); err != nil {
			return compaction.Decision{Reason: "invalid_budget"}, fmt.Errorf("invalid automatic compaction reserve: %w", err)
		}
		if limits.CompactReserveTokens.State == protocol.ValueKnown {
			reserve := limits.CompactReserveTokens.Value
			policy.CompactReserveTokens = &reserve
		}
	}
	if err := plan.Body.EstimatedInputTokens.Validate(); err != nil {
		return compaction.Decision{Reason: "invalid_budget"}, fmt.Errorf("invalid automatic compaction input estimate: %w", err)
	}
	return compaction.Evaluate(plan.Body.EstimatedInputTokens.Value, plan.Body.OutputReserve, plan.Body.ContextWindow, policy)
}

type compactionEventValue struct {
	kind    string
	payload any
}

func (s *Service) compactionEvents(request CompactRequest, activityID protocol.ActivityID, label string, values []compactionEventValue) ([]protocol.ProposedEvent, error) {
	actor := protocol.ActorRef{ID: "orchestrator", Kind: protocol.ActorSystem}
	events := make([]protocol.ProposedEvent, 0, len(values))
	for index, value := range values {
		payload, err := canonicaljson.Marshal(value.payload)
		if err != nil {
			return nil, err
		}
		events = append(events, protocol.ProposedEvent{EventID: eventID(request.Command.CommandID, "compaction-"+label, index, value.kind), Time: time.Now().UTC(), PayloadVersion: 1, Kind: value.kind, SessionID: request.SessionID, ActivityID: activityID, Actor: &actor, RuntimeGenerationID: request.Runtime.ID, Payload: payload})
	}
	return events, nil
}

func compactionCommandAccepted(request CompactRequest) protocol.ProposedEvent {
	payload := mustCanonical(protocol.CommandAcceptedV1{CommandID: request.Command.CommandID, RequestDigest: request.Command.RequestDigest, IdempotencyKey: request.Command.IdempotencyKey})
	actor := protocol.DeepCopy(request.Command.Actor)
	return protocol.ProposedEvent{EventID: eventID(request.Command.CommandID, "compaction-accepted", 0, protocol.EventCommandAccepted), Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventCommandAccepted, SessionID: request.SessionID, Actor: &actor, RuntimeGenerationID: request.Runtime.ID, Payload: payload}
}

func (s *Service) appendCompaction(ctx context.Context, state *compactionState, label string, events []protocol.ProposedEvent) error {
	return s.appendCompactionWithTransaction(ctx, state, protocol.TransactionID(stableID("transaction", string(state.command.CommandID), "compaction", label)), events)
}

func (s *Service) appendCompactionWithTransaction(ctx context.Context, state *compactionState, transactionID protocol.TransactionID, events []protocol.ProposedEvent) error {
	if err := validateCompactionProposedEvents(events, state.ref, state.head.CommitSeq+1); err != nil {
		return err
	}
	result, err := s.appendBatch(ctx, journal.AppendRequest{Journal: state.ref, ExpectedHead: state.head, TransactionID: transactionID, Events: events})
	if err != nil {
		return err
	}
	if result.Status != journal.AppendCommitted {
		return fmt.Errorf("journal expected-head conflict")
	}
	state.head = result.Cursor
	if s.publisher != nil {
		return s.publisher.PublishCommitted(ctx, state.ref, result.Cursor, result.Events)
	}
	return nil
}

func (s *Service) terminalizeCompactionFailure(ctx context.Context, request CompactRequest, state *compactionState, cause error) error {
	status, code, message := "failed", "compaction_failed", "context compaction failed"
	activityKind, activityStatus := protocol.EventActivityFailed, "failed"
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		status, code, message = "interrupted", "compaction_interrupted", "context compaction interrupted"
		activityKind, activityStatus = protocol.EventActivityCancelled, "cancelled"
	}
	var denied *authorizationDeniedError
	if errors.As(cause, &denied) {
		status, code, message = "denied", "authorization_denied", "compaction authorization denied"
		activityKind, activityStatus = protocol.EventActivityDenied, "denied"
	}
	values := make([]compactionEventValue, 0, 2)
	if state.activityPlanned && !state.activityTerminal && state.activityID != "" {
		outcome := protocol.ActivityOutcomeV1{Status: activityStatus}
		if state.evidence.Body.ID != "" {
			activityKind, outcome.Status, outcome.OutputEvidenceIDs = protocol.EventActivitySucceeded, "succeeded", []protocol.EvidenceID{state.evidence.Body.ID}
		}
		values = append(values, compactionEventValue{activityKind, outcome})
	}
	transactionID := protocol.TransactionID(stableID("transaction", string(state.command.CommandID), "compaction", "failure-terminal"))
	finalCursor := protocol.CommittedCursor{JournalKind: state.ref.Kind, JournalID: state.ref.ID, CommitSeq: state.head.CommitSeq + uint64(len(values)) + 2, TransactionID: transactionID}
	payload, err := canonicaljson.Marshal(struct {
		Status string `json:"status"`
	}{status})
	if err != nil {
		return err
	}
	commandResult := protocol.CommandResult{ProtocolVersion: protocol.ApplicationProtocolVersion, CommandID: state.command.CommandID, Status: status, RequestDigest: state.command.RequestDigest, Cursor: protocol.ApplicationCursor{SelectedSession: &finalCursor}, PayloadVersion: 1, Payload: payload, Error: &protocol.PublicError{Code: code, Message: message, Retryable: false}}
	raw, err := canonicaljson.Marshal(commandResult)
	if err != nil {
		return err
	}
	values = append(values, compactionEventValue{protocol.EventCommandCompleted, protocol.CommandCompletedV1{CommandID: state.command.CommandID, RequestDigest: state.command.RequestDigest, Status: status, Result: raw, Error: commandResult.Error}})
	events, err := s.compactionEvents(request, state.activityID, "failure-terminal", values)
	if err != nil {
		return err
	}
	if err := s.appendCompactionWithTransaction(ctx, state, transactionID, events); err != nil {
		return err
	}
	if state.head != finalCursor {
		return fmt.Errorf("compaction failure cursor prediction mismatch")
	}
	state.terminal = true
	return nil
}

func (s *Service) authorizeCompaction(ctx context.Context, request CompactRequest, state *compactionState, activityID protocol.ActivityID, callID string, authRequest protocol.AuthorizationRequest) (authorization.CommittedToken, error) {
	decision, err := s.deps.Authorization.Decide(ctx, authRequest)
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
		decision, err = s.deps.Authorization.ResolveInteractive(ctx, authRequest, decision, response)
		if err != nil {
			return authorization.CommittedToken{}, err
		}
	}
	if err := authorization.ValidateBinding(authRequest, decision); err != nil {
		return authorization.CommittedToken{}, err
	}
	if decision.Action != "allow" {
		deniedEvents, eventErr := s.compactionEvents(request, activityID, "decision", []compactionEventValue{{protocol.EventAuthorizationDecided, protocol.AuthorizationDecidedV1{Decision: decision}}, {protocol.EventActivityDenied, protocol.ActivityOutcomeV1{Status: "denied"}}})
		if eventErr != nil {
			return authorization.CommittedToken{}, eventErr
		}
		if appendErr := s.appendCompaction(ctx, state, "decision", deniedEvents); appendErr != nil {
			return authorization.CommittedToken{}, appendErr
		}
		state.activityTerminal = true
		return authorization.CommittedToken{}, &authorizationDeniedError{reason: decision.Reason}
	}
	decisionEventID := eventID(request.Command.CommandID, "compaction-decision", 0, protocol.EventAuthorizationDecided)
	decisionEvents, err := s.compactionEvents(request, activityID, "decision", []compactionEventValue{{protocol.EventAuthorizationDecided, protocol.AuthorizationDecidedV1{Decision: decision}}, {protocol.EventActivityAuthorized, protocol.ActivityAuthorizedV1{DecisionNonce: decision.DecisionNonce, DecisionEventID: decisionEventID, PlanDigest: authRequest.PlanDigest, RequestDigest: authRequest.RequestDigest, DispatchDigest: authRequest.DispatchDigest}}})
	if err != nil {
		return authorization.CommittedToken{}, err
	}
	if err := s.appendCompaction(ctx, state, "decision", decisionEvents); err != nil {
		return authorization.CommittedToken{}, err
	}
	decisionDigest, err := canonicaljson.Digest(decision)
	if err != nil {
		return authorization.CommittedToken{}, err
	}
	consumedID := eventID(request.Command.CommandID, "compaction-start", 0, protocol.EventAuthorizationDecisionConsumed)
	startedID := eventID(request.Command.CommandID, "compaction-start", 1, protocol.EventActivityStarted)
	startEvents, err := s.compactionEvents(request, activityID, "start", []compactionEventValue{{protocol.EventAuthorizationDecisionConsumed, protocol.AuthorizationDecisionConsumedV1{DecisionNonce: decision.DecisionNonce, DecisionEventID: decisionEventID, DecisionDigest: decisionDigest, RequestID: authRequest.RequestID, ActivityID: activityID, CallID: callID, PlanDigest: authRequest.PlanDigest, RequestDigest: authRequest.RequestDigest, DispatchDigest: authRequest.DispatchDigest, RuntimeGenerationID: request.Runtime.ID}}, {protocol.EventActivityStarted, protocol.ActivityStartedV1{DecisionNonce: decision.DecisionNonce, DecisionEventID: decisionEventID, ActivityID: activityID, CallID: callID, PlanDigest: authRequest.PlanDigest, RequestDigest: authRequest.RequestDigest, DispatchDigest: authRequest.DispatchDigest, RuntimeGenerationID: request.Runtime.ID, DispatchState: "registered"}}})
	if err != nil {
		return authorization.CommittedToken{}, err
	}
	if err := s.appendCompaction(ctx, state, "start", startEvents); err != nil {
		return authorization.CommittedToken{}, err
	}
	return s.deps.Authorization.Issue(ctx, authorization.CommitReference{Journal: state.ref, DecisionTransactionID: protocol.TransactionID(stableID("transaction", string(state.command.CommandID), "compaction", "decision")), DecisionEventID: decisionEventID, StartTransactionID: protocol.TransactionID(stableID("transaction", string(state.command.CommandID), "compaction", "start")), ConsumedEventID: consumedID, StartedEventID: startedID})
}

func compactionAuthorizationRequest(request CompactRequest, activityID protocol.ActivityID, callID string, modelRequest protocol.ModelRequest, selection compaction.Selection) (protocol.AuthorizationRequest, error) {
	requestDigest, err := canonicaljson.Digest(modelRequest)
	if err != nil {
		return protocol.AuthorizationRequest{}, err
	}
	dispatchDigest, err := provider.DispatchDigest(requestDigest, selection.SourceDigest, modelRequest.Plan.Digest, request.Runtime.ID)
	if err != nil {
		return protocol.AuthorizationRequest{}, err
	}
	descriptorDigest, err := canonicaljson.Digest(modelRequest.Plan.Body.Descriptor)
	if err != nil {
		return protocol.AuthorizationRequest{}, err
	}
	return protocol.AuthorizationRequest{RequestID: stableID("authorization-request", string(request.Command.CommandID), string(activityID)), Principal: request.Command.Actor, Actor: protocol.ActorRef{ID: "orchestrator", Kind: protocol.ActorSystem}, SessionID: request.SessionID, ActivityID: activityID, CallID: callID, QueueID: string(activityID), Source: protocol.ToolIdentity{Source: "provider", Authority: string(modelRequest.ProviderID), Name: string(modelRequest.ModelID)}, SourceRevision: modelRequest.Plan.Body.Descriptor.SourceRevision, DescriptorDigest: descriptorDigest, Action: "provider.stream", Resources: []protocol.ResourceTarget{{Kind: "model", CanonicalID: string(modelRequest.ProviderID) + "/" + string(modelRequest.ModelID)}}, ExecutionLocus: "remote", RequestedProfile: "network", EffectiveProfile: "network", Effect: "egress", Boundary: "network", Reversibility: "not_reversible", VerificationCoverage: "provider_terminal", RuntimeGenerationID: request.Runtime.ID, PolicyGeneration: request.Runtime.Body.PolicyGeneration, PolicyProvenance: []protocol.PolicyProvenance{{Source: "runtime", Revision: request.Runtime.Body.ProviderCatalogRevision, Generation: request.Runtime.Body.PolicyGeneration}}, PlanDigest: modelRequest.Plan.Digest, RequestDigest: requestDigest, DispatchDigest: dispatchDigest}, nil
}

func (s *Service) recordCompactionEvidence(ctx context.Context, request CompactRequest, activityID protocol.ActivityID, selection compaction.Selection, summary []byte) (protocol.EvidenceRecord, error) {
	binding, err := compaction.NewEvidenceBinding(selection)
	if err != nil {
		return protocol.EvidenceRecord{}, err
	}
	encodedBinding, err := compaction.EncodeEvidenceBinding(binding)
	if err != nil {
		return protocol.EvidenceRecord{}, err
	}
	candidate := protocol.EvidenceCandidate{ID: protocol.EvidenceID(stableID("evidence", string(request.Command.CommandID), selection.SourceDigest.Value, string(request.Runtime.ID))), Kind: "context_summary", WorkspaceID: effectiveWorkspaceID(request.WorkspaceID, request.SessionID), SessionID: request.SessionID, MediaType: "application/json", ProducingActivityID: activityID, Actor: protocol.ActorRef{ID: "orchestrator", Kind: protocol.ActorSystem}, Subject: protocol.SubjectRef{Kind: "context_compaction_summary", ID: encodedBinding}, Content: summary, Limit: compaction.MaxSummaryBytes}
	record, err := s.deps.Evidence.Put(ctx, candidate)
	if err != nil {
		return protocol.EvidenceRecord{}, err
	}
	if err := record.Validate(); err != nil {
		return protocol.EvidenceRecord{}, fmt.Errorf("evidence recorder returned invalid record: %w", err)
	}
	if err := canonicaljson.ValidateDigest(record.Body, record.Digest); err != nil {
		return protocol.EvidenceRecord{}, fmt.Errorf("evidence recorder returned invalid digest: %w", err)
	}
	if record.Body.ID != candidate.ID || record.Body.Kind != candidate.Kind || record.Body.WorkspaceID != candidate.WorkspaceID || record.Body.SessionID != candidate.SessionID || record.Body.ProducingActivityID != candidate.ProducingActivityID || record.Body.Actor != candidate.Actor || record.Body.Subject != candidate.Subject || record.Body.MediaType != candidate.MediaType {
		return protocol.EvidenceRecord{}, fmt.Errorf("evidence recorder returned mismatched identity")
	}
	return record, nil
}

// collectCompactionSummary admits exactly one bounded summary response. Unlike
// a turn transcript it has no reason to retain arbitrarily long provider data.
func (s *Service) collectCompactionSummary(ctx context.Context, generation protocol.RuntimeGenerationID, stream <-chan protocol.ModelEvent) ([]byte, protocol.ModelUsage, error) {
	if stream == nil {
		return nil, protocol.ModelUsage{}, fmt.Errorf("provider returned nil stream")
	}
	usage := unknownUsage()
	var summary []byte
	var previous uint64
	terminal := false
	for !terminal {
		select {
		case <-ctx.Done():
			return nil, protocol.ModelUsage{}, ctx.Err()
		case event, ok := <-stream:
			if !ok {
				return nil, protocol.ModelUsage{}, fmt.Errorf("compaction provider stream ended without terminal")
			}
			if event.Validate() != nil || event.Sequence != previous+1 {
				return nil, protocol.ModelUsage{}, fmt.Errorf("invalid compaction provider stream")
			}
			previous = event.Sequence
			switch event.Kind {
			case protocol.ModelEventContentDelta:
				// Streaming adapters may emit provisional deltas before their
				// terminal immutable content block. The block below remains the
				// sole accepted compaction summary.
				if summary != nil || event.Delta.Kind != protocol.ContentText {
					return nil, protocol.ModelUsage{}, fmt.Errorf("compaction provider output is not text")
				}
			case protocol.ModelEventUsageUpdate:
				var err error
				usage, err = s.sanitizeModelUsage(ctx, generation, *event.Usage)
				if err != nil {
					return nil, protocol.ModelUsage{}, err
				}
			case protocol.ModelEventContentBlock:
				if event.Block.Kind != protocol.ContentText || summary != nil {
					return nil, protocol.ModelUsage{}, fmt.Errorf("compaction provider output is not one summary text block")
				}
				admitted, err := s.deps.Admission.SanitizeText(ctx, generation, event.Block.Text)
				if err != nil {
					return nil, protocol.ModelUsage{}, err
				}
				if len(admitted) > compaction.MaxSummaryBytes {
					return nil, protocol.ModelUsage{}, fmt.Errorf("compaction summary exceeds %d bytes", compaction.MaxSummaryBytes)
				}
				summary = []byte(admitted)
			case protocol.ModelEventError:
				return nil, protocol.ModelUsage{}, fmt.Errorf("provider %s: %s", event.Error.Code, event.Error.Message)
			case protocol.ModelEventTerminal:
				terminal = true
			default:
				return nil, protocol.ModelUsage{}, fmt.Errorf("compaction provider emitted unsupported %s", event.Kind)
			}
		}
	}
	admitted, err := compaction.ParseSummary(summary)
	if err != nil {
		return nil, protocol.ModelUsage{}, err
	}
	return admitted, usage, nil
}

type compactCommandPayload struct {
	Evidence protocol.EvidenceRecord  `json:"summary_evidence"`
	From     protocol.CommittedCursor `json:"from"`
	Through  protocol.CommittedCursor `json:"through"`
	Revision string                   `json:"revision"`
	Usage    protocol.ModelUsage      `json:"usage"`
}

func compactCommandCompleted(request CompactRequest, result CompactResult) (protocol.CommandCompletedV1, error) {
	payload, err := canonicaljson.Marshal(compactCommandPayload{result.SummaryEvidence, result.From, result.Through, result.Revision, result.Usage})
	if err != nil {
		return protocol.CommandCompletedV1{}, err
	}
	command := protocol.CommandResult{ProtocolVersion: protocol.ApplicationProtocolVersion, CommandID: request.Command.CommandID, Status: "completed", RequestDigest: request.Command.RequestDigest, Cursor: protocol.ApplicationCursor{SelectedSession: &result.Cursor}, PayloadVersion: 1, Payload: payload}
	raw, err := canonicaljson.Marshal(command)
	if err != nil {
		return protocol.CommandCompletedV1{}, err
	}
	return protocol.CommandCompletedV1{CommandID: request.Command.CommandID, RequestDigest: request.Command.RequestDigest, Status: "completed", Result: raw}, nil
}
func compactResultFromCommand(command protocol.CommandResult) (CompactResult, error) {
	if command.Status == "accepted" {
		return CompactResult{}, fmt.Errorf("compaction command is accepted but not terminal: %w", ErrCommitUncertain)
	}
	if command.Status != "completed" {
		if command.Error != nil {
			return CompactResult{}, fmt.Errorf("compaction command %s: %s", command.Status, command.Error.Message)
		}
		return CompactResult{}, fmt.Errorf("compaction command ended with %s", command.Status)
	}
	var payload compactCommandPayload
	if err := jsonUnmarshal(command.Payload, &payload); err != nil {
		return CompactResult{}, err
	}
	cursor := command.Cursor.WorkspaceControl
	if command.Cursor.SelectedSession != nil {
		cursor = *command.Cursor.SelectedSession
	}
	return CompactResult{Cursor: cursor, SummaryEvidence: payload.Evidence, From: payload.From, Through: payload.Through, Revision: payload.Revision, Usage: payload.Usage}, nil
}

// Kept as a variable-free helper to make the replay boundary explicit.
func jsonUnmarshal(raw []byte, target any) error { return json.Unmarshal(raw, target) }

func validateCompactRequest(request CompactRequest) error {
	if err := validateCommandMetadata(request.Command); err != nil {
		return fmt.Errorf("command: %w", err)
	}
	if request.SessionID == "" {
		return fmt.Errorf("session ID is required")
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)}
	if err := validateExpectedHead(request.ExpectedHead, ref); err != nil {
		return fmt.Errorf("expected cursor: %w", err)
	}
	if request.Trigger != compaction.TriggerManual && request.Trigger != compaction.TriggerAutomatic {
		return fmt.Errorf("invalid compaction trigger %q", request.Trigger)
	}
	if request.Runtime.ID == "" || request.Runtime.Digest.Validate() != nil || validateRuntimeManifest(request.Runtime) != nil {
		return fmt.Errorf("runtime generation is invalid")
	}
	if _, ok := selectedCompactRuntimeModel(request); !ok {
		return fmt.Errorf("selected model %q/%q is not in runtime generation", request.ProviderID, request.ModelID)
	}
	return nil
}
func selectedCompactRuntimeModel(request CompactRequest) (protocol.ModelDescriptor, bool) {
	return selectedRuntimeModel(StartTurnRequest{ProviderID: request.ProviderID, ModelID: request.ModelID, Runtime: request.Runtime})
}
