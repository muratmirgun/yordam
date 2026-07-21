package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	receiptprojector "github.com/muratmirgun/yordam/internal/subagent"
)

func (s *Service) RecoverTurn(ctx context.Context, request RecoveryControlRequest) (result RecoveryControlResult, runErr error) {
	if err := validateRecoveryControlRequest(request); err != nil {
		return RecoveryControlResult{}, err
	}
	if s.deps.Admission == nil {
		return RecoveryControlResult{}, fmt.Errorf("generation admission service is required")
	}
	admitted, err := s.deps.Admission.SanitizeJSON(ctx, request.Control.Runtime.ID, request.Control.Event.Payload)
	if err != nil {
		return RecoveryControlResult{}, err
	}
	request.Control.Event.Payload = admitted
	if err := validateRecoveryControlRequest(request); err != nil {
		return RecoveryControlResult{}, fmt.Errorf("admitted recovery control event: %w", err)
	}
	sessionID := protocol.SessionID(request.Storage.Journal.ID)
	laneLease, err := acquireManagedOperationLease(ctx, s.lane, OperationClaim{Kind: OperationRecovery, SessionID: sessionID, ControlOperationID: request.Control.OperationID})
	if err != nil {
		return RecoveryControlResult{}, err
	}
	defer laneLease.Release()
	if durable, ok, lookupErr := s.LookupCommand(ctx, request.Control.Journal, request.Control.Command.CommandID, request.Control.Command.RequestDigest); lookupErr != nil {
		return RecoveryControlResult{}, lookupErr
	} else if ok && durable.Status != "accepted" {
		var payload struct {
			Recovery journal.RecoveryResult `json:"recovery"`
		}
		if err := json.Unmarshal(durable.Payload, &payload); err != nil {
			return RecoveryControlResult{}, err
		}
		return RecoveryControlResult{Recovery: payload.Recovery, CommandResult: durable}, nil
	}
	if s.deps.Authorization == nil || s.deps.Projection == nil || s.turnLeases == nil {
		return RecoveryControlResult{}, fmt.Errorf("recovery dependencies are incomplete")
	}
	projection, err := s.deps.Projection.InspectRecovery(ctx, request.Storage.Journal, request.Storage.ExpectedHead)
	if err != nil {
		return RecoveryControlResult{}, err
	}
	var turnLease journal.TurnLease
	if projection.ActiveTurnID != "" {
		turnLease, err = s.turnLeases.AcquireTurnRecoveryLease(ctx, sessionID, projection.ActiveTurnID, request.Storage.ExpectedHead)
		if err != nil {
			return RecoveryControlResult{}, err
		}
	}
	releaseHead := request.Storage.ExpectedHead
	defer func() {
		if turnLease != nil {
			if releaseErr := turnLease.Release(context.WithoutCancel(ctx), releaseHead); releaseErr != nil {
				runErr = errors.Join(runErr, releaseErr)
			}
		}
	}()

	controlState := controlState{ref: request.Control.Journal, head: request.Control.ExpectedHead, command: request.Control.Command, operationID: request.Control.OperationID}
	defer func() {
		if runErr == nil || !controlState.accepted || controlState.terminal {
			return
		}
		terminalResult, terminalErr := s.terminalizeControlFailure(context.WithoutCancel(ctx), request.Control, &controlState, runErr)
		if terminalErr != nil {
			runErr = errors.Join(runErr, terminalErr)
			return
		}
		result = RecoveryControlResult{CommandResult: terminalResult.CommandResult}
	}()
	authorizationRequest, err := controlAuthorizationRequest(request.Control)
	if err != nil {
		return RecoveryControlResult{}, err
	}
	planned := []struct {
		kind    string
		payload any
	}{
		{protocol.EventCommandAccepted, protocol.CommandAcceptedV1{CommandID: request.Control.Command.CommandID, RequestDigest: request.Control.Command.RequestDigest, IdempotencyKey: request.Control.Command.IdempotencyKey}},
		{protocol.EventControlOperationPlanned, protocol.ControlOperationPlannedV1{ControlOperationID: request.Control.OperationID, Kind: string(OperationRecovery), Purpose: request.Control.Plan.Body.Purpose, Plan: request.Control.Plan}},
		{protocol.EventAuthorizationRequested, protocol.AuthorizationRequestedV1{Request: authorizationRequest}},
	}
	events, err := s.controlEvents(controlState, request.Control.Runtime.ID, "recovery-planned", planned)
	if err != nil {
		return RecoveryControlResult{}, err
	}
	plannedHead := controlState.head
	if err := s.appendControl(ctx, &controlState, "recovery-planned", events, ""); err != nil {
		controlState.accepted = controlState.head != plannedHead
		return RecoveryControlResult{}, err
	}
	controlState.accepted = true
	token, err := s.authorizeControl(ctx, request.Control, &controlState, authorizationRequest)
	if err != nil {
		return RecoveryControlResult{}, err
	}
	binding := authorization.DispatchBinding{
		Kind: "control", HandleID: stableID("recovery-handle", string(request.Control.OperationID)), ControlOperationID: request.Control.OperationID,
		CallID: authorizationRequest.CallID, PlanDigest: authorizationRequest.PlanDigest, RequestDigest: authorizationRequest.RequestDigest,
		DispatchDigest: authorizationRequest.DispatchDigest, RuntimeGenerationID: request.Control.Runtime.ID,
	}
	var recoveryResult journal.RecoveryResult
	barrierState := BarrierState{Journal: controlState.ref, Cursor: controlState.head, CommandID: request.Control.Command.CommandID, ControlOperationID: request.Control.OperationID, PlanDigest: request.Control.Plan.Digest}
	if err := s.cross(ctx, BarrierAuthorizationCommitted, barrierState); err != nil {
		return RecoveryControlResult{}, err
	}
	if err := s.probe.Before(ctx, BarrierEffectDispatch, barrierState); err != nil {
		return RecoveryControlResult{}, err
	}
	dispatchErr := s.deps.Authorization.Dispatch(ctx, token, binding, func(runContext context.Context) error {
		var recoverErr error
		recoveryResult, recoverErr = s.repository.Recover(runContext, request.Storage)
		return recoverErr
	})
	if dispatchErr != nil {
		return RecoveryControlResult{}, dispatchErr
	}
	postDispatchBarrierErr := s.probe.After(ctx, BarrierEffectDispatch, barrierState)
	if recoveryResult.Status != "recovered" {
		return RecoveryControlResult{}, fmt.Errorf("repository recovery status %q", recoveryResult.Status)
	}
	if recoveryResult.Cursor.Validate() != nil || recoveryResult.Cursor.JournalKind != request.Storage.Journal.Kind || recoveryResult.Cursor.JournalID != request.Storage.Journal.ID {
		return RecoveryControlResult{}, fmt.Errorf("repository returned invalid recovery cursor")
	}

	projected, err := s.deps.Projection.InspectRecovery(ctx, request.Storage.Journal, recoveryResult.Cursor)
	if err != nil {
		return RecoveryControlResult{}, err
	}
	sessionHead := recoveryResult.Cursor
	releaseHead = sessionHead
	// A parent waiting on a sequential child has a durable cross-session
	// protocol, not an ordinary in-process activity. Reconcile that protocol
	// before the generic active-turn terminalization below can erase its exact
	// child receipt/attachment boundary.
	if reconciledHead, reconciled, resumed, reconcileErr := s.reconcileWaitingSubagents(ctx, laneLease, request, &projected, sessionHead); reconcileErr != nil {
		if projected.SubagentRecoveryDiagnostic == nil {
			return RecoveryControlResult{}, reconcileErr
		}
		if reconciledHead.Validate() == nil && reconciledHead.JournalKind == request.Storage.Journal.Kind && reconciledHead.JournalID == request.Storage.Journal.ID && reconciledHead.CommitSeq >= sessionHead.CommitSeq {
			sessionHead, releaseHead = reconciledHead, reconciledHead
		}
		failureDiagnostic := protocol.DeepCopy(projected.SubagentRecoveryDiagnostic)
		failedProjection, inspectErr := s.deps.Projection.InspectRecovery(ctx, request.Storage.Journal, sessionHead)
		if inspectErr != nil {
			return RecoveryControlResult{}, errors.Join(reconcileErr, inspectErr)
		}
		failedProjection.SubagentRecoveryDiagnostic = failureDiagnostic
		if failedProjection.ActiveTurnID == "" {
			return RecoveryControlResult{}, errors.Join(reconcileErr, fmt.Errorf("recovered parent continuation failure is not active at terminalization"))
		}
		sessionHead, err = s.appendRecoverySessionTerminal(ctx, request, failedProjection, sessionHead)
		if err != nil {
			return RecoveryControlResult{}, errors.Join(reconcileErr, err)
		}
		barrierState := BarrierState{Journal: request.Storage.Journal, Cursor: sessionHead, CommandID: failedProjection.OriginalCommandID, ControlOperationID: request.Control.OperationID, TaskID: failedProjection.TaskID, TurnID: failedProjection.ActiveTurnID}
		if err := s.cross(ctx, BarrierRecoveryTurnTerminalCommitted, barrierState); err != nil {
			return RecoveryControlResult{}, errors.Join(reconcileErr, err)
		}
		releaseHead = sessionHead
		projected = RecoveryProjection{}
	} else if reconciled {
		sessionHead, releaseHead = reconciledHead, reconciledHead
		projected, err = s.deps.Projection.InspectRecovery(ctx, request.Storage.Journal, sessionHead)
		if err != nil {
			return RecoveryControlResult{}, err
		}
		if resumed && projected.ActiveTurnID != "" {
			return RecoveryControlResult{}, fmt.Errorf("resumed parent remains active")
		}
	}
	if turnLease == nil && projected.ActiveTurnID != "" {
		turnLease, err = s.turnLeases.AcquireTurnRecoveryLease(ctx, sessionID, projected.ActiveTurnID, recoveryResult.Cursor)
		if err != nil {
			return RecoveryControlResult{}, err
		}
	}
	if projected.ActiveTurnID != "" {
		if projection.ActiveTurnID != "" && projected.ActiveTurnID != projection.ActiveTurnID {
			return RecoveryControlResult{}, fmt.Errorf("active turn changed during recovery")
		}
		sessionHead, err = s.appendRecoverySessionTerminal(ctx, request, projected, recoveryResult.Cursor)
		if err != nil {
			return RecoveryControlResult{}, err
		}
		barrierState := BarrierState{Journal: request.Storage.Journal, Cursor: sessionHead, CommandID: projected.OriginalCommandID, ControlOperationID: request.Control.OperationID, TaskID: projected.TaskID, TurnID: projected.ActiveTurnID}
		if err := s.cross(ctx, BarrierRecoveryTurnTerminalCommitted, barrierState); err != nil {
			return RecoveryControlResult{}, err
		}
		releaseHead = sessionHead
	}
	if postDispatchBarrierErr != nil {
		return RecoveryControlResult{}, postDispatchBarrierErr
	}

	finalCursor := protocol.CommittedCursor{
		JournalKind: request.Control.Journal.Kind, JournalID: request.Control.Journal.ID,
		CommitSeq: controlState.head.CommitSeq + 3, TransactionID: request.Control.TransactionID,
	}
	payload := mustCanonical(struct {
		Recovery journal.RecoveryResult `json:"recovery"`
	}{recoveryResult})
	commandResult := protocol.CommandResult{
		ProtocolVersion: protocol.ApplicationProtocolVersion, CommandID: request.Control.Command.CommandID, Status: "completed",
		RequestDigest: request.Control.Command.RequestDigest, Cursor: applicationCursor(request.Control.Journal, finalCursor), PayloadVersion: 1, Payload: payload,
	}
	rawResult, err := canonicaljson.Marshal(commandResult)
	if err != nil {
		return RecoveryControlResult{}, err
	}
	terminal := []struct {
		kind    string
		payload any
	}{
		{protocol.EventControlOperationCompleted, protocol.ControlOperationTerminalV1{ControlOperationID: request.Control.OperationID, Status: "completed"}},
		{protocol.EventCommandCompleted, protocol.CommandCompletedV1{CommandID: request.Control.Command.CommandID, RequestDigest: request.Control.Command.RequestDigest, Status: "completed", Result: rawResult}},
	}
	events, err = s.controlEvents(controlState, request.Control.Runtime.ID, "recovery-terminal", terminal)
	if err != nil {
		return RecoveryControlResult{}, err
	}
	terminalHead := controlState.head
	if err := s.appendControl(ctx, &controlState, "recovery-terminal", events, request.Control.TransactionID); err != nil {
		controlState.terminal = controlState.head != terminalHead
		return RecoveryControlResult{}, err
	}
	if controlState.head != finalCursor {
		return RecoveryControlResult{}, fmt.Errorf("recovery control cursor prediction mismatch")
	}
	controlState.terminal = true
	return RecoveryControlResult{Recovery: recoveryResult, CommandResult: commandResult}, nil
}

func (s *Service) appendRecoverySessionTerminal(ctx context.Context, request RecoveryControlRequest, projection RecoveryProjection, expected protocol.CommittedCursor) (protocol.CommittedCursor, error) {
	eventCount := len(projection.StartedActivities) + 2
	if projection.OriginalCommandID != "" {
		eventCount++
	}
	if projection.ChildManifest != nil {
		eventCount++
	}
	if projection.SubagentRecoveryDiagnostic != nil {
		eventCount++
	}
	transactionID := protocol.TransactionID(stableID("transaction", string(request.Control.Command.CommandID), "recovery-session-terminal"))
	finalCursor := protocol.CommittedCursor{
		JournalKind: request.Storage.Journal.Kind, JournalID: request.Storage.Journal.ID,
		CommitSeq: expected.CommitSeq + uint64(eventCount) + 1, TransactionID: transactionID,
	}
	actor := protocol.ActorRef{ID: "recovery-orchestrator", Kind: protocol.ActorSystem}
	now := time.Now().UTC()
	events := make([]protocol.ProposedEvent, 0, eventCount)
	for index, activityID := range projection.StartedActivities {
		status, kind := "uncertain", protocol.EventActivityUncertain
		if s.deps.EffectProbe != nil {
			if noEffect, err := s.deps.EffectProbe.ProvesNoEffect(ctx, activityID); err == nil && noEffect {
				status, kind = "interrupted_no_effect", protocol.EventActivityInterruptedNoEffect
			}
		}
		events = append(events, protocol.ProposedEvent{
			EventID: eventID(request.Control.Command.CommandID, "recovery-session-terminal", index, kind), Time: now, PayloadVersion: 1,
			Kind: kind, SessionID: protocol.SessionID(request.Storage.Journal.ID), TaskID: projection.TaskID, TurnID: projection.ActiveTurnID,
			ActivityID: activityID, Actor: &actor, RuntimeGenerationID: request.Control.Runtime.ID,
			Payload: mustCanonical(protocol.ActivityOutcomeV1{Status: status}),
		})
	}
	events = append(events,
		protocol.ProposedEvent{
			EventID: eventID(request.Control.Command.CommandID, "recovery-session-terminal", len(events), protocol.EventTurnInterrupted), Time: now, PayloadVersion: 1,
			Kind: protocol.EventTurnInterrupted, SessionID: protocol.SessionID(request.Storage.Journal.ID), TaskID: projection.TaskID, TurnID: projection.ActiveTurnID,
			Actor: &actor, RuntimeGenerationID: request.Control.Runtime.ID,
			Payload: mustCanonical(protocol.TurnTerminalV1{Status: "interrupted", Reason: "session journal recovery"}),
		},
		protocol.ProposedEvent{
			EventID: eventID(request.Control.Command.CommandID, "recovery-session-terminal", len(events)+1, protocol.EventTaskStatusChanged), Time: now, PayloadVersion: 1,
			Kind: protocol.EventTaskStatusChanged, SessionID: protocol.SessionID(request.Storage.Journal.ID), TaskID: projection.TaskID, TurnID: projection.ActiveTurnID,
			Actor: &actor, RuntimeGenerationID: request.Control.Runtime.ID,
			Payload: mustCanonical(protocol.TaskStatusChangedV1{From: string(protocol.TaskRunning), To: string(protocol.TaskFailed), Reason: "turn interrupted by recovery"}),
		},
	)
	if projection.OriginalCommandID != "" {
		publicError := &protocol.PublicError{Code: "recovery_interrupted", Message: "turn interrupted during journal recovery", Retryable: false}
		if diagnostic := projection.SubagentRecoveryDiagnostic; diagnostic != nil {
			publicError = &protocol.PublicError{Code: "subagent_recovery_uncertain", Message: "sequential child recovery state is uncertain: " + diagnostic.Reason, Retryable: false}
		}
		payload := mustCanonical(struct {
			TurnID protocol.TurnID `json:"turn_id"`
			Status string          `json:"status"`
		}{projection.ActiveTurnID, "interrupted"})
		result := protocol.CommandResult{
			ProtocolVersion: protocol.ApplicationProtocolVersion, CommandID: projection.OriginalCommandID, Status: "interrupted",
			RequestDigest: projection.OriginalRequestDigest, Cursor: applicationCursor(request.Storage.Journal, finalCursor),
			PayloadVersion: 1, Payload: payload, Error: publicError,
		}
		rawResult, err := canonicaljson.Marshal(result)
		if err != nil {
			return protocol.CommittedCursor{}, err
		}
		events = append(events, protocol.ProposedEvent{
			EventID: eventID(request.Control.Command.CommandID, "recovery-session-terminal", len(events), protocol.EventCommandCompleted), Time: now, PayloadVersion: 1,
			Kind: protocol.EventCommandCompleted, SessionID: protocol.SessionID(request.Storage.Journal.ID), TaskID: projection.TaskID, TurnID: projection.ActiveTurnID,
			Actor: &actor, RuntimeGenerationID: request.Control.Runtime.ID,
			Payload: mustCanonical(protocol.CommandCompletedV1{CommandID: projection.OriginalCommandID, RequestDigest: projection.OriginalRequestDigest, Status: "interrupted", Result: rawResult, Error: publicError}),
		})
	}
	if projection.ChildManifest != nil {
		prefix, err := s.durablePrefix(ctx, request.Storage.Journal, expected)
		if err != nil {
			return protocol.CommittedCursor{}, err
		}
		status := "cancelled"
		for _, activityID := range projection.StartedActivities {
			if !projection.UnmatchedNoEffect[activityID] {
				status = "uncertain"
				break
			}
		}
		receiptCursor := protocol.CommittedCursor{JournalKind: request.Storage.Journal.Kind, JournalID: request.Storage.Journal.ID, CommitSeq: expected.CommitSeq + uint64(eventCount), TransactionID: transactionID}
		receipt := receiptprojector.ProjectReceipt(*projection.ChildManifest, receiptCursor, status, "turn interrupted during journal recovery", protocol.ModelUsage{}, &protocol.PublicError{Code: "recovery_interrupted", Message: "turn interrupted during journal recovery"}, prefix)
		events = append(events, protocol.ProposedEvent{EventID: eventID(request.Control.Command.CommandID, "recovery-session-terminal", len(events), protocol.EventSubagentReceipt), Time: now, PayloadVersion: 1, Kind: protocol.EventSubagentReceipt, SessionID: protocol.SessionID(request.Storage.Journal.ID), TaskID: projection.ChildManifest.ChildTaskID, TurnID: projection.ChildManifest.ChildTurnID, Actor: &actor, RuntimeGenerationID: projection.ChildManifest.RuntimeGenerationID, Payload: mustCanonical(receipt)})
	}
	if diagnostic := projection.SubagentRecoveryDiagnostic; diagnostic != nil {
		details, err := canonicaljson.Marshal(diagnostic)
		if err != nil {
			return protocol.CommittedCursor{}, err
		}
		events = append(events, protocol.ProposedEvent{
			EventID: eventID(request.Control.Command.CommandID, "recovery-session-terminal", len(events), protocol.EventRecoveryDiagnostic), Time: now, PayloadVersion: 1,
			Kind: protocol.EventRecoveryDiagnostic, SessionID: protocol.SessionID(request.Storage.Journal.ID), TaskID: projection.TaskID, TurnID: projection.ActiveTurnID,
			Actor: &actor, RuntimeGenerationID: request.Control.Runtime.ID,
			Payload: mustCanonical(protocol.DiagnosticV1{Diagnostic: protocol.Diagnostic{Code: "subagent.recovery_uncertain", Message: "sequential child recovery state is uncertain", Journal: request.Storage.Journal, AtSeq: expected.CommitSeq, Details: details}}),
		})
	}
	if err := validateProposedEventsAt(events, request.Storage.Journal, expected.CommitSeq+1, transactionID); err != nil {
		return protocol.CommittedCursor{}, err
	}
	appendResult, err := s.appendBatch(ctx, journal.AppendRequest{Journal: request.Storage.Journal, ExpectedHead: expected, TransactionID: transactionID, Events: events})
	if err != nil {
		return protocol.CommittedCursor{}, err
	}
	if appendResult.Status != journal.AppendCommitted || appendResult.Cursor != finalCursor {
		return protocol.CommittedCursor{}, fmt.Errorf("recovery session terminal append status %q", appendResult.Status)
	}
	if s.publisher != nil {
		if err := s.publisher.PublishCommitted(ctx, request.Storage.Journal, appendResult.Cursor, appendResult.Events); err != nil {
			return protocol.CommittedCursor{}, err
		}
	}
	return finalCursor, nil
}

// reconcileWaitingSubagents repairs only proven sequential handoff edges. A
// typed, proven-absent reserved child may be created once; ambiguous child
// activity is never replayed. The child receipt is always committed before the
// parent attachment and provider continuation.
func (s *Service) reconcileWaitingSubagents(ctx context.Context, lease managedOperationLease, request RecoveryControlRequest, projection *RecoveryProjection, parentHead protocol.CommittedCursor) (protocol.CommittedCursor, bool, bool, error) {
	// An attachment followed by a terminal continuation is already complete.
	// Replaying it would make a restart dispatch a second provider activity.
	if projection.ActiveTurnID == "" {
		return parentHead, false, false, nil
	}
	if s.deps.ChildSessions == nil || s.deps.ParentSessions == nil || s.deps.Evidence == nil || s.deps.Children == nil {
		return parentHead, false, false, nil
	}
	parentEvents, err := s.durablePrefix(ctx, request.Storage.Journal, parentHead)
	if err != nil {
		return parentHead, false, false, err
	}
	projector := receiptprojector.Projector{}
	state := projector.Zero(request.Storage.Journal)
	type inspectedChild struct {
		inspection journal.Inspection
		absent     bool
	}
	inspectedChildren := make(map[protocol.DelegationAttemptID]inspectedChild)
	attemptOrder := make([]protocol.DelegationAttemptID, 0)
	manifests := make(map[protocol.DelegationAttemptID]protocol.SubagentManifestV1)

	// Decode and inspect every reserved child before replay can perform any
	// durable repair. An infrastructure failure on a later attempt therefore
	// cannot leave an earlier attempt partially repaired.
	for _, event := range parentEvents {
		if event.Envelope.Kind != protocol.EventSubagentRequested && event.Envelope.Kind != protocol.EventSubagentWaiting && event.Envelope.Kind != protocol.EventSubagentResultAttached {
			continue
		}
		if err := decodeSubagentRecoveryEvent(&event); err != nil {
			return parentHead, false, false, err
		}
		if event.Envelope.Kind != protocol.EventSubagentRequested {
			continue
		}
		payload, ok := event.Decoded.(*protocol.SubagentRequestedV1)
		if !ok || payload.Validate() != nil || payload.Manifest.ParentSessionID != protocol.SessionID(request.Storage.Journal.ID) || payload.Manifest.ParentCursor.CommitSeq >= event.Envelope.Seq || event.Envelope.JournalKind != request.Storage.Journal.Kind || event.Envelope.JournalID != request.Storage.Journal.ID || event.Envelope.SessionID != payload.Manifest.ParentSessionID || event.Envelope.RuntimeGenerationID != payload.Manifest.RuntimeGenerationID || event.Envelope.TaskID == "" || event.Envelope.TurnID == "" {
			return parentHead, false, false, fmt.Errorf("invalid subagent request during recovery preflight")
		}
		if _, exists := manifests[payload.Manifest.AttemptID]; !exists {
			attemptOrder = append(attemptOrder, payload.Manifest.AttemptID)
			manifests[payload.Manifest.AttemptID] = protocol.DeepCopy(payload.Manifest)
		}
	}
	for _, attemptID := range attemptOrder {
		manifest := manifests[attemptID]
		child, inspectErr := s.deps.ChildSessions.InspectSession(ctx, manifest.ChildSessionID)
		switch {
		case inspectErr == nil:
			inspectedChildren[attemptID] = inspectedChild{inspection: child}
		case errors.Is(inspectErr, journal.ErrSessionNotFound):
			inspectedChildren[attemptID] = inspectedChild{absent: true}
		default:
			return parentHead, false, false, fmt.Errorf("inspect sequential child session %q: %w", manifest.ChildSessionID, inspectErr)
		}
	}

	// Replay the parent history in commit order. A durable parent attachment is
	// accepted only after the exact receipt has been hydrated from its child
	// journal at that historical boundary. This closes the previous attempt
	// before the next request is projected without trusting parent data alone.
	hydrated := make(map[protocol.DelegationAttemptID]bool)
	hydrateReceipt := func(attemptID protocol.DelegationAttemptID) error {
		if hydrated[attemptID] {
			return nil
		}
		inspected, ok := inspectedChildren[attemptID]
		if !ok || inspected.absent {
			return fmt.Errorf("attached sequential child %q has no inspectable receipt", manifests[attemptID].ChildSessionID)
		}
		for _, childEvent := range inspected.inspection.Events {
			if childEvent.Envelope.Kind != protocol.EventSubagentManifest && childEvent.Envelope.Kind != protocol.EventSubagentReceipt {
				continue
			}
			if err := decodeSubagentRecoveryEvent(&childEvent); err != nil {
				return err
			}
			var applyErr error
			state, applyErr = projector.Apply(state, childEvent)
			if applyErr != nil {
				return fmt.Errorf("project child subagent recovery: %w", applyErr)
			}
		}
		hydrated[attemptID] = true
		return nil
	}
	for _, event := range parentEvents {
		if event.Envelope.Kind != protocol.EventSubagentRequested && event.Envelope.Kind != protocol.EventSubagentWaiting && event.Envelope.Kind != protocol.EventSubagentResultAttached {
			continue
		}
		if err := decodeSubagentRecoveryEvent(&event); err != nil {
			return parentHead, false, false, err
		}
		if event.Envelope.Kind == protocol.EventSubagentResultAttached {
			payload, ok := event.Decoded.(*protocol.SubagentResultAttachedV1)
			if !ok {
				return parentHead, false, false, fmt.Errorf("decoded subagent attachment has unexpected type")
			}
			if err := hydrateReceipt(payload.AttemptID); err != nil {
				return parentHead, false, false, err
			}
		}
		state, err = projector.Apply(state, event)
		if err != nil {
			return parentHead, false, false, fmt.Errorf("project parent subagent recovery: %w", err)
		}
	}
	// Hydrate only the currently unresolved attempt after historical parent
	// replay. A terminal receipt must not close an attempt before its parent
	// attachment appears in the committed order.
	for _, attemptID := range attemptOrder {
		attempt := state.Attempts[attemptID]
		if attempt.State != receiptprojector.StateWaiting && attempt.State != receiptprojector.StateTerminal {
			continue
		}
		if inspected := inspectedChildren[attemptID]; !inspected.absent {
			if err := hydrateReceipt(attemptID); err != nil {
				return parentHead, false, false, err
			}
		}
	}
	changed := false
	for _, attemptID := range attemptOrder {
		initial := state.Attempts[attemptID]
		if initial.State != receiptprojector.StateWaiting && initial.State != receiptprojector.StateTerminal {
			continue
		}
		inspected := inspectedChildren[initial.AttemptID]
		child := inspected.inspection
		childState := receiptprojector.ChildRecoveryState{Exists: !inspected.absent, CommitKnown: inspected.absent || childInspectionCommitKnown(child)}
		attempt := initial
		if !inspected.absent {
			attempt = state.Attempts[initial.AttemptID]
			if attempt.Receipt == nil {
				childProjection, projectionErr := s.deps.Projection.InspectRecovery(ctx, child.Journal, child.Head)
				if projectionErr != nil {
					childState.CommitKnown = false
				} else {
					for _, activityID := range childProjection.StartedActivities {
						if s.deps.EffectProbe == nil {
							continue
						}
						noEffect, probeErr := s.deps.EffectProbe.ProvesNoEffect(ctx, activityID)
						if probeErr == nil && noEffect {
							childProjection.UnmatchedNoEffect[activityID] = true
						}
					}
					childState.NoUnmatchedEffect = len(childProjection.StartedActivities) == 0
					if len(childProjection.StartedActivities) != 0 {
						childState.NoUnmatchedEffect = true
						for _, activityID := range childProjection.StartedActivities {
							if !childProjection.UnmatchedNoEffect[activityID] {
								childState.NoUnmatchedEffect = false
								break
							}
						}
					}
					if childState.CommitKnown {
						// Persist the bounded receipt first. Any non-proven activity is
						// represented as uncertain by appendRecoverySessionTerminal.
						childRequest := request
						childRequest.Storage.Journal, childRequest.Storage.ExpectedHead = child.Journal, child.Head
						childRequest.Control.Runtime = protocol.DeepCopy(request.Control.Runtime)
						if childRequest.Control.Runtime.ID != initial.Manifest.RuntimeGenerationID {
							childState.CommitKnown = false
						} else if _, appendErr := s.appendRecoverySessionTerminal(ctx, childRequest, childProjection, child.Head); appendErr != nil {
							return parentHead, changed, false, appendErr
						} else {
							changed = true
							child, inspectErr := s.deps.ChildSessions.InspectSession(ctx, initial.Manifest.ChildSessionID)
							if inspectErr != nil {
								return parentHead, changed, false, inspectErr
							}
							for _, event := range child.Events {
								if event.Envelope.Kind != protocol.EventSubagentReceipt {
									continue
								}
								if err := decodeSubagentRecoveryEvent(&event); err != nil {
									return parentHead, changed, false, err
								}
								state, err = projector.Apply(state, event)
								if err != nil {
									return parentHead, changed, false, err
								}
							}
							attempt = state.Attempts[initial.AttemptID]
						}
					}
				}
			}
		}
		result, reconcileErr := receiptprojector.Reconcile(receiptprojector.ReconcileRequest{ParentSessionID: protocol.SessionID(request.Storage.Journal.ID), ParentCursor: parentHead, Runtime: request.Control.Runtime}, attempt, childState)
		if reconcileErr != nil || result.ChildStatus == "uncertain" {
			// The generic recovery terminal below makes this non-retryable and
			// visible; do not create or replay a child from ambiguous state.
			if projection.SubagentRecoveryDiagnostic == nil {
				reason := result.Diagnostic
				if reconcileErr != nil {
					reason = reconcileErr.Error()
				}
				projection.SubagentRecoveryDiagnostic = &SubagentRecoveryDiagnostic{AttemptID: initial.AttemptID, ChildSessionID: initial.Manifest.ChildSessionID, ChildStatus: result.ChildStatus, TerminalCursor: attempt.TerminalCursor, ReceiptDigest: attempt.ReceiptDigest, Reason: reason}
			}
			continue
		}
		if result.CreateOnce {
			parentRequest, rebuildErr := s.recoverParentStartRequest(ctx, request, *projection, initial, parentHead, parentEvents)
			if rebuildErr != nil {
				if !isRecoveredParentTerminalFailure(rebuildErr) {
					return parentHead, changed, false, rebuildErr
				}
				return parentHead, changed, false, recoveredParentContinuationError(projection, initial, attempt, "reconstruct recovered parent continuation", rebuildErr)
			}
			var receipt protocol.SubagentReceiptV1
			if yieldErr := lease.Yield(ctx, func(childContext context.Context) error {
				var runErr error
				receipt, runErr = s.deps.Children.RunChild(childContext, ChildRunRequest{Manifest: initial.Manifest, Call: initial.Call, Parent: parentRequest})
				return runErr
			}); yieldErr != nil {
				return parentHead, changed, false, yieldErr
			}
			if !sameSubagentManifest(receipt.Manifest, initial.Manifest) || receipt.Validate() != nil || s.verifyChildReceipt(ctx, initial.Manifest, receipt) != nil {
				return parentHead, changed, false, fmt.Errorf("recovery-created child receipt is not durably bound")
			}
			attempt.Receipt = protocol.DeepCopy(&receipt)
			attempt.TerminalCursor = receipt.TerminalCursor
			attempt.ReceiptDigest, _ = canonicaljson.Digest(receipt)
			result, reconcileErr = receiptprojector.Reconcile(receiptprojector.ReconcileRequest{ParentSessionID: protocol.SessionID(request.Storage.Journal.ID), ParentCursor: parentHead, Runtime: request.Control.Runtime}, attempt, receiptprojector.ChildRecoveryState{Exists: true, CommitKnown: true})
			if reconcileErr != nil || result.ChildStatus == "uncertain" || attempt.Receipt == nil {
				return parentHead, changed, false, fmt.Errorf("recovery-created child receipt cannot be reconciled")
			}
		}
		if result.Attached {
			parentRequest, rebuildErr := s.recoverParentStartRequest(ctx, request, *projection, initial, parentHead, parentEvents)
			if rebuildErr != nil {
				if !isRecoveredParentTerminalFailure(rebuildErr) {
					return parentHead, changed, false, rebuildErr
				}
				return parentHead, changed, false, recoveredParentContinuationError(projection, initial, attempt, "reconstruct recovered parent continuation", rebuildErr)
			}
			resumed, resumeErr := s.resumeRecoveredParent(ctx, lease, parentRequest, initial, *attempt.Receipt)
			if resumeErr != nil {
				if !isRecoveredParentTerminalFailure(resumeErr) {
					return parentHead, changed, false, resumeErr
				}
				if resumed.Cursor.Validate() == nil && resumed.Cursor.JournalKind == parentHead.JournalKind && resumed.Cursor.JournalID == parentHead.JournalID && resumed.Cursor.CommitSeq >= parentHead.CommitSeq {
					parentHead = resumed.Cursor
				}
				return parentHead, true, false, recoveredParentContinuationError(projection, initial, attempt, "resume recovered parent continuation", resumeErr)
			}
			return resumed.Cursor, true, true, nil
		}
		if attempt.Receipt == nil {
			continue
		}
		if err := s.verifyParentHead(ctx, &turnState{ref: request.Storage.Journal, head: parentHead}); err != nil {
			return parentHead, changed, false, err
		}
		recoveryTurn := turnState{ref: request.Storage.Journal, head: parentHead, command: CommandMetadata{CommandID: projection.OriginalCommandID, RequestDigest: projection.OriginalRequestDigest}, taskID: attempt.ParentTaskID, turnID: attempt.ParentTurnID, activeActivityID: attempt.ActivityID}
		if recoveryTurn.command.CommandID == "" || recoveryTurn.activeActivityID == "" {
			continue
		}
		_, attachErr := s.attachSubagentReceipt(ctx, StartTurnRequest{Command: recoveryTurn.command, SessionID: protocol.SessionID(request.Storage.Journal.ID), Runtime: protocol.DeepCopy(request.Control.Runtime)}, &recoveryTurn, protocol.ToolUseBlock{CallID: string(attempt.AttemptID)}, attempt.ActivityID, *attempt.Receipt)
		if attachErr != nil {
			return parentHead, changed, false, attachErr
		}
		parentHead, changed = recoveryTurn.head, true
		if result.ParentResumable || attempt.Receipt != nil {
			parentRequest, rebuildErr := s.recoverParentStartRequest(ctx, request, *projection, initial, parentHead, parentEvents)
			if rebuildErr != nil {
				if !isRecoveredParentTerminalFailure(rebuildErr) {
					return parentHead, changed, false, rebuildErr
				}
				return parentHead, changed, false, recoveredParentContinuationError(projection, initial, attempt, "reconstruct recovered parent continuation", rebuildErr)
			}
			resumed, resumeErr := s.resumeRecoveredParent(ctx, lease, parentRequest, initial, *attempt.Receipt)
			if resumeErr != nil {
				if !isRecoveredParentTerminalFailure(resumeErr) {
					return parentHead, changed, false, resumeErr
				}
				if resumed.Cursor.Validate() == nil && resumed.Cursor.JournalKind == parentHead.JournalKind && resumed.Cursor.JournalID == parentHead.JournalID && resumed.Cursor.CommitSeq >= parentHead.CommitSeq {
					parentHead = resumed.Cursor
				}
				return parentHead, true, false, recoveredParentContinuationError(projection, initial, attempt, "resume recovered parent continuation", resumeErr)
			}
			return resumed.Cursor, true, true, nil
		}
	}
	return parentHead, changed, false, nil
}

func recoveredParentContinuationError(projection *RecoveryProjection, initial, attempt receiptprojector.Attempt, phase string, cause error) error {
	reason := phase + ": " + cause.Error()
	projection.SubagentRecoveryDiagnostic = &SubagentRecoveryDiagnostic{
		AttemptID: initial.AttemptID, ChildSessionID: initial.Manifest.ChildSessionID, ChildStatus: "uncertain",
		TerminalCursor: attempt.TerminalCursor, ReceiptDigest: attempt.ReceiptDigest, Reason: reason,
	}
	return fmt.Errorf("%s: %w", phase, cause)
}

type recoveredParentTerminalFailure struct{ cause error }

func (e *recoveredParentTerminalFailure) Error() string { return e.cause.Error() }
func (e *recoveredParentTerminalFailure) Unwrap() error { return e.cause }

func terminalRecoveredParentFailure(cause error) error {
	if cause == nil || isRecoveredParentTerminalFailure(cause) {
		return cause
	}
	return &recoveredParentTerminalFailure{cause: cause}
}

func terminalRecoveredParentFailuref(format string, values ...any) error {
	return terminalRecoveredParentFailure(fmt.Errorf(format, values...))
}

func isRecoveredParentTerminalFailure(err error) bool {
	var failure *recoveredParentTerminalFailure
	return errors.As(err, &failure)
}

func decodeSubagentRecoveryEvent(event *protocol.EventRecord) error {
	if event.Decoded != nil {
		return nil
	}
	switch event.Envelope.Kind {
	case protocol.EventSubagentRequested:
		var payload protocol.SubagentRequestedV1
		if err := json.Unmarshal(event.Envelope.Payload, &payload); err != nil {
			return err
		}
		event.Decoded = &payload
	case protocol.EventSubagentWaiting:
		var payload protocol.SubagentWaitingV1
		if err := json.Unmarshal(event.Envelope.Payload, &payload); err != nil {
			return err
		}
		event.Decoded = &payload
	case protocol.EventSubagentManifest:
		var payload protocol.SubagentManifestV1
		if err := json.Unmarshal(event.Envelope.Payload, &payload); err != nil {
			return err
		}
		event.Decoded = &payload
	case protocol.EventSubagentReceipt:
		var payload protocol.SubagentReceiptV1
		if err := json.Unmarshal(event.Envelope.Payload, &payload); err != nil {
			return err
		}
		event.Decoded = &payload
	case protocol.EventSubagentResultAttached:
		var payload protocol.SubagentResultAttachedV1
		if err := json.Unmarshal(event.Envelope.Payload, &payload); err != nil {
			return err
		}
		event.Decoded = &payload
	}
	return nil
}

// recoverParentStartRequest rebuilds only immutable request facts from the
// committed parent journal.  Recovery never borrows mutable current-session
// configuration: a missing or mismatched frozen generation fails closed.
func (s *Service) recoverParentStartRequest(ctx context.Context, control RecoveryControlRequest, projection RecoveryProjection, attempt receiptprojector.Attempt, head protocol.CommittedCursor, events []protocol.EventRecord) (StartTurnRequest, error) {
	if attempt.Manifest.RuntimeGenerationID != control.Control.Runtime.ID {
		return StartTurnRequest{}, terminalRecoveredParentFailuref("subagent recovery runtime mismatch for child %q", attempt.Manifest.ChildSessionID)
	}
	var accepted protocol.TurnAcceptedV1
	var command protocol.CommandAcceptedV1
	var actor protocol.ActorRef
	foundTurn, foundCommand := false, false
	for _, event := range events {
		if event.Envelope.TurnID != attempt.ParentTurnID || event.Envelope.RuntimeGenerationID != control.Control.Runtime.ID {
			continue
		}
		switch event.Envelope.Kind {
		case protocol.EventTurnAccepted:
			if err := json.Unmarshal(event.Envelope.Payload, &accepted); err != nil {
				return StartTurnRequest{}, err
			}
			foundTurn = true
		case protocol.EventCommandAccepted:
			if err := json.Unmarshal(event.Envelope.Payload, &command); err != nil {
				return StartTurnRequest{}, err
			}
			if event.Envelope.Actor == nil {
				return StartTurnRequest{}, terminalRecoveredParentFailuref("subagent recovery parent command actor is absent")
			}
			actor = protocol.DeepCopy(*event.Envelope.Actor)
			foundCommand = true
		}
	}
	if !foundTurn || !foundCommand || accepted.CommandID != command.CommandID || accepted.Goal == "" || command.CommandID != projection.OriginalCommandID || command.RequestDigest != projection.OriginalRequestDigest {
		return StartTurnRequest{}, terminalRecoveredParentFailuref("subagent recovery parent identity is not reconstructible")
	}
	parent, err := s.deps.ParentSessions.InspectSession(ctx, protocol.SessionID(control.Storage.Journal.ID))
	if err != nil {
		return StartTurnRequest{}, fmt.Errorf("inspect recovery parent session: %w", err)
	}
	var model protocol.ModelDescriptor
	foundModel := false
	for _, candidate := range control.Control.Runtime.Body.Models {
		if candidate.ProviderID == protocol.ProviderID(parent.Session.Selection.Profile) && candidate.ModelID == protocol.ModelID(parent.Session.Selection.Model) {
			if foundModel {
				return StartTurnRequest{}, terminalRecoveredParentFailuref("subagent recovery parent model is ambiguous")
			}
			model, foundModel = candidate, true
		}
	}
	if !foundModel {
		return StartTurnRequest{}, terminalRecoveredParentFailuref("subagent recovery frozen model is unavailable")
	}
	return StartTurnRequest{Command: CommandMetadata{CommandID: command.CommandID, IdempotencyKey: command.IdempotencyKey, RequestDigest: command.RequestDigest, Actor: actor}, SessionID: protocol.SessionID(control.Storage.Journal.ID), ExpectedHead: head, Prompt: accepted.Goal, ProviderID: model.ProviderID, ModelID: model.ModelID, Runtime: protocol.DeepCopy(control.Control.Runtime)}, nil
}

func childInspectionCommitKnown(child journal.Inspection) bool {
	if !child.Writable || child.IncompleteTransaction != "" {
		return false
	}
	for _, diagnostic := range child.Diagnostics {
		if diagnostic.Code == "recovery.available" {
			return false
		}
	}
	return true
}

// resumeRecoveredParent continues the ordinary provider/tool loop after the
// exact canonical child receipt has been attached. The recovery lease remains
// owned throughout; nested children use its Yield path and all new provider or
// tool dispatches therefore receive fresh authorization.
func (s *Service) resumeRecoveredParent(ctx context.Context, lease managedOperationLease, request StartTurnRequest, attempt receiptprojector.Attempt, receipt protocol.SubagentReceiptV1) (RunResult, error) {
	if err := validateStartTurnRequest(request); err != nil {
		return RunResult{}, terminalRecoveredParentFailure(err)
	}
	intent, providerAttempts, err := s.recoverSubagentIntent(ctx, request, attempt)
	if err != nil {
		return RunResult{}, err
	}
	state := newTurnState(request)
	state.taskID, state.turnID, state.contractID = attempt.ParentTaskID, attempt.ParentTurnID, protocol.OutcomeContractID(stableID("contract", string(request.Command.CommandID)))
	state.accepted, state.taskRunning, state.subagentAttempts = true, true, 1
	encoded, err := canonicaljson.Marshal(receipt)
	if err != nil {
		return RunResult{}, terminalRecoveredParentFailure(err)
	}
	resultBlock := protocol.ToolResultBlock{CallID: intent.CallID, Status: receipt.Status, JSON: encoded}
	extra := []protocol.ModelMessage{{Role: "tool", Blocks: []protocol.ContentBlock{{Kind: protocol.ContentToolResult, ToolResult: &resultBlock}}}}
	completedTools := 1
	for providerAttempt := providerAttempts; ; providerAttempt++ {
		assistant, terminal, runErr := s.runProviderActivity(ctx, lease, request, &state, providerAttempt, extra)
		if runErr != nil {
			if errors.Is(runErr, ErrCommitUncertain) {
				return RunResult{TaskID: state.taskID, TurnID: state.turnID, Cursor: state.head, Status: "uncertain"}, runErr
			}
			return RunResult{TaskID: state.taskID, TurnID: state.turnID, Cursor: state.head, Status: "uncertain"}, terminalRecoveredParentFailure(runErr)
		}
		if len(assistant.ToolIntents) == 0 {
			completed, completeErr := s.completeTurn(ctx, request, &state, assistant, terminal)
			if completeErr != nil && completed.Cursor == (protocol.CommittedCursor{}) {
				completed = RunResult{TaskID: state.taskID, TurnID: state.turnID, Cursor: state.head, Status: "uncertain"}
			}
			if completeErr != nil && !errors.Is(completeErr, ErrCommitUncertain) {
				completeErr = terminalRecoveredParentFailure(completeErr)
			}
			return completed, completeErr
		}
		if completedTools+len(assistant.ToolIntents) > request.Runtime.Body.Limits.MaxToolCalls {
			return RunResult{TaskID: state.taskID, TurnID: state.turnID, Cursor: state.head, Status: "uncertain"}, terminalRecoveredParentFailuref("tool call limit %d reached", request.Runtime.Body.Limits.MaxToolCalls)
		}
		results := make([]protocol.ContentBlock, 0, len(assistant.ToolIntents))
		for _, next := range assistant.ToolIntents {
			toolResult, toolErr := s.runToolIntent(ctx, lease, request, &state, next)
			if toolErr != nil {
				if !errors.Is(toolErr, ErrCommitUncertain) {
					toolErr = terminalRecoveredParentFailure(toolErr)
				}
				return RunResult{TaskID: state.taskID, TurnID: state.turnID, Cursor: state.head, Status: "uncertain"}, toolErr
			}
			copyResult := toolResult
			results = append(results, protocol.ContentBlock{Kind: protocol.ContentToolResult, ToolResult: &copyResult})
			completedTools++
		}
		extra = make([]protocol.ModelMessage, 0, len(results))
		for _, result := range results {
			extra = append(extra, protocol.ModelMessage{Role: "tool", Blocks: []protocol.ContentBlock{result}})
		}
	}
}

func (s *Service) recoverSubagentIntent(ctx context.Context, request StartTurnRequest, attempt receiptprojector.Attempt) (protocol.ToolUseBlock, int, error) {
	events, err := s.durablePrefix(ctx, protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)}, request.ExpectedHead)
	if err != nil {
		return protocol.ToolUseBlock{}, 0, err
	}
	providerAttempts := 0
	var recovered protocol.ToolUseBlock
	found := false
	for _, event := range events {
		if event.Envelope.TurnID != attempt.ParentTurnID {
			continue
		}
		if event.Envelope.Kind == protocol.EventProviderAttemptTerminal {
			providerAttempts++
		}
		if event.Envelope.Kind != protocol.EventAssistantMessage {
			continue
		}
		var assistant protocol.AssistantMessageV1
		if err := json.Unmarshal(event.Envelope.Payload, &assistant); err != nil {
			return protocol.ToolUseBlock{}, 0, err
		}
		for _, intent := range assistant.ToolIntents {
			if protocol.ActivityID(stableID("activity", string(request.Command.CommandID), "subagent", intent.CallID)) == attempt.ActivityID {
				if found && !reflect.DeepEqual(recovered, intent) {
					return protocol.ToolUseBlock{}, 0, terminalRecoveredParentFailuref("subagent recovery tool use is conflicting")
				}
				recovered, found = protocol.DeepCopy(intent), true
			}
		}
	}
	if found {
		return recovered, providerAttempts, nil
	}
	return protocol.ToolUseBlock{}, 0, terminalRecoveredParentFailuref("subagent recovery assistant tool use is not reconstructible")
}

func validateRecoveryControlRequest(request RecoveryControlRequest) error {
	if err := validateControlRequest(request.Control, true); err != nil {
		return err
	}
	storage := request.Storage
	if storage.OperationID == "" || storage.OperationID != request.Control.OperationID || storage.Journal.Kind != protocol.JournalSession || storage.Journal.Validate() != nil || storage.TransactionID == "" || storage.RuntimeGenerationID != request.Control.Runtime.ID {
		return fmt.Errorf("recovery storage request identity mismatch")
	}
	if err := validateExpectedHead(storage.ExpectedHead, storage.Journal); err != nil {
		return err
	}
	if err := storage.ObservedTailDigest.Validate(); err != nil {
		return err
	}
	return nil
}

func joinRecoveryErrors(primary, terminal error) error { return errors.Join(primary, terminal) }
