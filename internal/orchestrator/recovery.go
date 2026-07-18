package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
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
	laneLease, err := s.lane.Acquire(ctx, OperationClaim{Kind: OperationRecovery, SessionID: sessionID, ControlOperationID: request.Control.OperationID})
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
	if err := validateProposedEvents(events, request.Storage.Journal); err != nil {
		return protocol.CommittedCursor{}, err
	}
	appendResult, err := s.repository.AppendBatch(ctx, journal.AppendRequest{Journal: request.Storage.Journal, ExpectedHead: expected, TransactionID: transactionID, Events: events})
	if err != nil {
		return protocol.CommittedCursor{}, err
	}
	if appendResult.Status != journal.AppendCommitted || appendResult.Cursor != finalCursor {
		return protocol.CommittedCursor{}, fmt.Errorf("recovery session terminal append status %q", appendResult.Status)
	}
	return finalCursor, nil
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
