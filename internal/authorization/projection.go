package authorization

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/projection"
	"github.com/muratmirgun/yordam/internal/protocol"
)

const projectionVersion uint32 = 1

type DecisionRecord struct {
	Decision protocol.AuthorizationDecision `json:"decision"`
	EventID  protocol.EventID               `json:"event_id"`
}

type Grant struct {
	GrantID          string                         `json:"grant_id"`
	Decision         protocol.AuthorizationDecision `json:"decision"`
	Revoked          bool                           `json:"revoked"`
	RevocationEpoch  uint64                         `json:"revocation_epoch,omitempty"`
	RevocationReason string                         `json:"revocation_reason,omitempty"`
}

type Consumption struct {
	DecisionNonce protocol.DecisionNonce                   `json:"decision_nonce"`
	EventID       protocol.EventID                         `json:"event_id"`
	Bindings      protocol.AuthorizationDecisionConsumedV1 `json:"bindings"`
}

type CommandRecord struct {
	Journal          protocol.JournalRef     `json:"journal"`
	CommandID        protocol.CommandID      `json:"command_id"`
	RequestDigest    protocol.Digest         `json:"request_digest"`
	IdempotencyKey   string                  `json:"idempotency_key"`
	AcceptedEventID  protocol.EventID        `json:"accepted_event_id"`
	Result           *protocol.CommandResult `json:"result,omitempty"`
	CompletedEventID protocol.EventID        `json:"completed_event_id,omitempty"`
}

type Projection struct {
	Journal         protocol.JournalRef                       `json:"journal"`
	Requests        map[string]protocol.AuthorizationRequest  `json:"requests"`
	Decisions       map[protocol.DecisionNonce]DecisionRecord `json:"decisions"`
	Grants          map[string]Grant                          `json:"grants"`
	Consumed        map[protocol.DecisionNonce]Consumption    `json:"consumed"`
	Commands        map[string]CommandRecord                  `json:"commands"`
	RevocationEpoch uint64                                    `json:"revocation_epoch"`
}

type Projector struct{}

func (Projector) Version() uint32 { return projectionVersion }

func (Projector) Zero(ref protocol.JournalRef) Projection {
	return Projection{
		Journal: ref, Requests: make(map[string]protocol.AuthorizationRequest), Decisions: make(map[protocol.DecisionNonce]DecisionRecord),
		Grants: make(map[string]Grant), Consumed: make(map[protocol.DecisionNonce]Consumption), Commands: make(map[string]CommandRecord),
	}
}

func (Projector) Apply(current Projection, event protocol.EventRecord) (Projection, error) {
	if err := projection.ValidateFoundationEvent(event); err != nil {
		return current, err
	}
	next := protocol.DeepCopy(current)
	ensureMaps(&next)
	switch event.Envelope.Kind {
	case protocol.EventAuthorizationRequested:
		payload, ok := event.Decoded.(*protocol.AuthorizationRequestedV1)
		if !ok {
			return current, fmt.Errorf("invalid authorization request")
		}
		if existing, exists := next.Requests[payload.Request.RequestID]; exists {
			if !reflect.DeepEqual(existing, payload.Request) {
				return current, fmt.Errorf("authorization request %q changed binding", payload.Request.RequestID)
			}
			return next, nil
		}
		next.Requests[payload.Request.RequestID] = protocol.DeepCopy(payload.Request)
	case protocol.EventAuthorizationDecided:
		payload, ok := event.Decoded.(*protocol.AuthorizationDecidedV1)
		if !ok {
			return current, fmt.Errorf("invalid authorization decision")
		}
		request, exists := next.Requests[payload.Decision.Request.RequestID]
		if !exists || !reflect.DeepEqual(request, payload.Decision.Request) {
			return current, fmt.Errorf("authorization decision request binding changed")
		}
		if existing, exists := next.Decisions[payload.Decision.DecisionNonce]; exists {
			if !reflect.DeepEqual(existing.Decision, payload.Decision) || existing.EventID != event.Envelope.EventID {
				return current, fmt.Errorf("authorization decision nonce %q changed binding", payload.Decision.DecisionNonce)
			}
			return next, nil
		}
		next.Decisions[payload.Decision.DecisionNonce] = DecisionRecord{Decision: protocol.DeepCopy(payload.Decision), EventID: event.Envelope.EventID}
		if payload.Decision.Action == "allow" && payload.Decision.Lifetime == protocol.AuthorizationLifetimeSession {
			grantID := string(payload.Decision.DecisionNonce)
			next.Grants[grantID] = Grant{GrantID: grantID, Decision: protocol.DeepCopy(payload.Decision)}
		}
	case protocol.EventAuthorizationDecisionConsumed:
		payload, ok := event.Decoded.(*protocol.AuthorizationDecisionConsumedV1)
		if !ok {
			return current, fmt.Errorf("invalid authorization consumption")
		}
		if _, consumed := next.Consumed[payload.DecisionNonce]; consumed {
			return current, fmt.Errorf("authorization nonce %q already consumed", payload.DecisionNonce)
		}
		decisionRecord, exists := next.Decisions[payload.DecisionNonce]
		if !exists || decisionRecord.Decision.Action != "allow" {
			return current, fmt.Errorf("authorization decision %q is not an allow", payload.DecisionNonce)
		}
		if err := validateConsumption(event, *payload, decisionRecord); err != nil {
			return current, err
		}
		next.Consumed[payload.DecisionNonce] = Consumption{DecisionNonce: payload.DecisionNonce, EventID: event.Envelope.EventID, Bindings: protocol.DeepCopy(*payload)}
	case protocol.EventAuthorizationGrantRevoked:
		payload, ok := event.Decoded.(*protocol.AuthorizationGrantRevokedV1)
		if !ok {
			return current, fmt.Errorf("invalid grant revocation")
		}
		grant, exists := next.Grants[payload.GrantID]
		if !exists || payload.RevocationEpoch <= next.RevocationEpoch {
			return current, fmt.Errorf("invalid grant revocation for %q", payload.GrantID)
		}
		grant.Revoked, grant.RevocationEpoch, grant.RevocationReason = true, payload.RevocationEpoch, payload.Reason
		next.Grants[payload.GrantID], next.RevocationEpoch = grant, payload.RevocationEpoch
	case protocol.EventCommandAccepted:
		payload, ok := event.Decoded.(*protocol.CommandAcceptedV1)
		if !ok {
			return current, fmt.Errorf("invalid accepted command")
		}
		key := commandKey(next.Journal, payload.CommandID)
		if existing, exists := next.Commands[key]; exists {
			if existing.RequestDigest != payload.RequestDigest {
				return current, fmt.Errorf("command %q request digest changed", payload.CommandID)
			}
			if existing.IdempotencyKey != payload.IdempotencyKey {
				return current, fmt.Errorf("command %q idempotency key changed", payload.CommandID)
			}
			return next, nil
		}
		next.Commands[key] = CommandRecord{
			Journal: next.Journal, CommandID: payload.CommandID, RequestDigest: payload.RequestDigest,
			IdempotencyKey: payload.IdempotencyKey, AcceptedEventID: event.Envelope.EventID,
		}
	case protocol.EventCommandCompleted:
		payload, ok := event.Decoded.(*protocol.CommandCompletedV1)
		if !ok {
			return current, fmt.Errorf("invalid completed command")
		}
		key := commandKey(next.Journal, payload.CommandID)
		record, exists := next.Commands[key]
		if !exists || record.RequestDigest != payload.RequestDigest {
			return current, fmt.Errorf("command completion request digest binding mismatch")
		}
		var result protocol.CommandResult
		if err := json.Unmarshal(payload.Result, &result); err != nil {
			return current, fmt.Errorf("decode command result: %w", err)
		}
		if result.ProtocolVersion != protocol.ApplicationProtocolVersion || result.CommandID != payload.CommandID || result.RequestDigest != payload.RequestDigest || result.Status != payload.Status {
			return current, fmt.Errorf("command result binding mismatch")
		}
		if record.Result != nil {
			if !reflect.DeepEqual(*record.Result, result) {
				return current, fmt.Errorf("command %q terminal result changed", payload.CommandID)
			}
			return next, nil
		}
		record.Result, record.CompletedEventID = protocol.DeepCopy(&result), event.Envelope.EventID
		next.Commands[key] = record
	default:
		return next, nil
	}
	return next, nil
}

func (Projector) NonceConsumed(state Projection, nonce protocol.DecisionNonce) bool {
	_, ok := state.Consumed[nonce]
	return ok
}

func (Projector) LookupGrant(state Projection, grantID string) (Grant, bool) {
	grant, ok := state.Grants[grantID]
	return protocol.DeepCopy(grant), ok
}

func (Projector) LookupCommand(state Projection, ref protocol.JournalRef, commandID protocol.CommandID) (CommandRecord, bool) {
	record, ok := state.Commands[commandKey(ref, commandID)]
	return protocol.DeepCopy(record), ok
}

func validateConsumption(event protocol.EventRecord, payload protocol.AuthorizationDecisionConsumedV1, record DecisionRecord) error {
	decision, request := record.Decision, record.Decision.Request
	digest, err := canonicaljson.Digest(decision)
	if err != nil {
		return err
	}
	if payload.DecisionEventID != record.EventID || payload.DecisionDigest != digest || payload.RequestID != request.RequestID ||
		payload.CallID != request.CallID || payload.PlanDigest != request.PlanDigest || payload.RequestDigest != request.RequestDigest ||
		payload.DispatchDigest != request.DispatchDigest || payload.RuntimeGenerationID != request.RuntimeGenerationID {
		return fmt.Errorf("authorization consumption binding mismatch")
	}
	if request.SessionID != "" {
		if payload.ActivityID != request.ActivityID || payload.ControlOperationID != "" || event.Envelope.ActivityID != request.ActivityID {
			return fmt.Errorf("authorization consumption activity binding mismatch")
		}
	} else if payload.ControlOperationID != request.ControlOperationID || payload.ActivityID != "" {
		return fmt.Errorf("authorization consumption control binding mismatch")
	}
	return nil
}

func ensureMaps(state *Projection) {
	if state.Requests == nil {
		state.Requests = make(map[string]protocol.AuthorizationRequest)
	}
	if state.Decisions == nil {
		state.Decisions = make(map[protocol.DecisionNonce]DecisionRecord)
	}
	if state.Grants == nil {
		state.Grants = make(map[string]Grant)
	}
	if state.Consumed == nil {
		state.Consumed = make(map[protocol.DecisionNonce]Consumption)
	}
	if state.Commands == nil {
		state.Commands = make(map[string]CommandRecord)
	}
}

func commandKey(ref protocol.JournalRef, commandID protocol.CommandID) string {
	return string(ref.Kind) + "\x00" + string(ref.ID) + "\x00" + string(commandID)
}
