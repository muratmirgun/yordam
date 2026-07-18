package authorization

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
)

var (
	ErrStaleDecision         = errors.New("authorization decision is stale")
	ErrExpiredDecision       = errors.New("authorization decision is expired")
	ErrRevokedDecision       = errors.New("authorization decision is revoked")
	ErrAlreadyUsedDecision   = errors.New("authorization decision was already used")
	ErrTerminalDecision      = errors.New("authorization decision is terminal")
	ErrInvalidCommittedToken = errors.New("invalid committed authorization token")
	ErrAlreadyDispatched     = errors.New("authorization was already dispatched")
)

type CommitReference struct {
	Journal               protocol.JournalRef
	DecisionTransactionID protocol.TransactionID
	DecisionEventID       protocol.EventID
	StartTransactionID    protocol.TransactionID
	ConsumedEventID       protocol.EventID
	StartedEventID        protocol.EventID
}

type DispatchBinding struct {
	Kind                string
	HandleID            string
	ActivityID          protocol.ActivityID
	ControlOperationID  protocol.ControlOperationID
	CallID              string
	PlanDigest          protocol.Digest
	RequestDigest       protocol.Digest
	DispatchDigest      protocol.Digest
	RuntimeGenerationID protocol.RuntimeGenerationID
}

type TokenIssuer interface {
	Issue(context.Context, CommitReference) (CommittedToken, error)
}

type DispatchGate interface {
	Dispatch(context.Context, CommittedToken, DispatchBinding, func(context.Context) error) error
}

type Service struct {
	reader        journal.CommittedTransactionReader
	mu            sync.Mutex
	issued        map[protocol.DecisionNonce]struct{}
	dispatched    map[protocol.DecisionNonce]struct{}
	handles       map[string]struct{}
	revocations   map[protocol.DecisionNonce]uint64
	cancellations map[protocol.DecisionNonce]context.CancelFunc
}

func NewService(reader journal.CommittedTransactionReader) *Service {
	return &Service{
		reader:        reader,
		issued:        make(map[protocol.DecisionNonce]struct{}),
		dispatched:    make(map[protocol.DecisionNonce]struct{}),
		handles:       make(map[string]struct{}),
		revocations:   make(map[protocol.DecisionNonce]uint64),
		cancellations: make(map[protocol.DecisionNonce]context.CancelFunc),
	}
}

func ValidateBinding(request protocol.AuthorizationRequest, decision protocol.AuthorizationDecision) error {
	if err := request.Validate(); err != nil {
		return fmt.Errorf("%w: request: %v", ErrStaleDecision, err)
	}
	if !reflect.DeepEqual(request, decision.Request) {
		return ErrStaleDecision
	}
	if err := decision.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrStaleDecision, err)
	}
	if decision.ExpiresAt != nil && !time.Now().Before(*decision.ExpiresAt) {
		return ErrExpiredDecision
	}
	return nil
}

func (s *Service) ResolveInteractive(request protocol.AuthorizationRequest, pending protocol.AuthorizationDecision, response protocol.ApprovalResponse) (protocol.AuthorizationDecision, error) {
	if err := ValidateBinding(request, pending); err != nil {
		return protocol.AuthorizationDecision{}, err
	}
	if pending.Action != "ask" {
		return protocol.AuthorizationDecision{}, ErrTerminalDecision
	}
	if response.RequestID != request.RequestID || (response.Action != "allow" && response.Action != "deny") || (response.Lifetime != protocol.AuthorizationLifetimeOnce && response.Lifetime != protocol.AuthorizationLifetimeSession) || response.Reason == "" {
		return protocol.AuthorizationDecision{}, ErrStaleDecision
	}
	if err := response.Actor.Validate(); err != nil || response.Actor.Kind != protocol.ActorUser {
		return protocol.AuthorizationDecision{}, ErrStaleDecision
	}
	scopeDigest, err := canonicaljson.Digest(pending.Scope)
	if err != nil || scopeDigest != response.ScopeDigest {
		return protocol.AuthorizationDecision{}, ErrStaleDecision
	}
	nonce, err := newDecisionNonce()
	if err != nil {
		return protocol.AuthorizationDecision{}, err
	}
	resolved := protocol.DeepCopy(pending)
	resolved.Action = response.Action
	resolved.Lifetime = response.Lifetime
	resolved.PolicySource = "interactive"
	resolved.Reason = response.Reason
	actor := response.Actor
	resolved.ResolvedActor = &actor
	resolved.DecidedAt = time.Now().UTC()
	resolved.ExpiresAt = nil
	resolved.DecisionNonce = nonce
	if err := ValidateBinding(request, resolved); err != nil {
		return protocol.AuthorizationDecision{}, err
	}
	return resolved, nil
}

func (s *Service) Issue(ctx context.Context, reference CommitReference) (CommittedToken, error) {
	if s == nil || s.reader == nil {
		return CommittedToken{}, fmt.Errorf("committed transaction reader is required")
	}
	if err := reference.validate(); err != nil {
		return CommittedToken{}, err
	}
	decisionTransaction, err := s.reader.ReadCommittedTransaction(ctx, reference.Journal, reference.DecisionTransactionID)
	if err != nil {
		return CommittedToken{}, fmt.Errorf("read committed decision transaction: %w", err)
	}
	if decisionTransaction.Journal != reference.Journal {
		return CommittedToken{}, fmt.Errorf("committed decision journal mismatch")
	}
	decisionEnvelope, err := namedEvent(decisionTransaction, reference.DecisionTransactionID, reference.DecisionEventID, protocol.EventAuthorizationDecided)
	if err != nil {
		return CommittedToken{}, err
	}
	var decided protocol.AuthorizationDecidedV1
	if err := decodePayload(decisionEnvelope.Payload, &decided); err != nil {
		return CommittedToken{}, fmt.Errorf("decode committed decision: %w", err)
	}
	decision := decided.Decision
	if decision.Action != "allow" {
		return CommittedToken{}, ErrTerminalDecision
	}
	if err := ValidateBinding(decision.Request, decision); err != nil {
		return CommittedToken{}, err
	}
	decisionDigest, err := canonicaljson.Digest(decision)
	if err != nil {
		return CommittedToken{}, err
	}

	startTransaction, err := s.reader.ReadCommittedTransaction(ctx, reference.Journal, reference.StartTransactionID)
	if err != nil {
		return CommittedToken{}, fmt.Errorf("read committed start transaction: %w", err)
	}
	if startTransaction.Journal != reference.Journal {
		return CommittedToken{}, fmt.Errorf("committed start journal mismatch")
	}
	consumedEnvelope, err := namedEvent(startTransaction, reference.StartTransactionID, reference.ConsumedEventID, protocol.EventAuthorizationDecisionConsumed)
	if err != nil {
		return CommittedToken{}, err
	}
	var consumed protocol.AuthorizationDecisionConsumedV1
	if err := decodePayload(consumedEnvelope.Payload, &consumed); err != nil {
		return CommittedToken{}, fmt.Errorf("decode committed consumption: %w", err)
	}
	if err := consumed.Validate(); err != nil {
		return CommittedToken{}, fmt.Errorf("invalid committed consumption: %w", err)
	}
	if err := matchConsumption(decision, decisionDigest, reference.DecisionEventID, consumed); err != nil {
		return CommittedToken{}, err
	}

	token := CommittedToken{
		valid: true, activityID: consumed.ActivityID, controlOperationID: consumed.ControlOperationID, callID: consumed.CallID,
		planDigest: consumed.PlanDigest, requestDigest: consumed.RequestDigest, dispatchDigest: consumed.DispatchDigest,
		runtimeGenerationID: consumed.RuntimeGenerationID, nonce: consumed.DecisionNonce,
	}
	if consumed.ControlOperationID != "" {
		token.kind = "control"
	} else if decision.Request.Source.Source == "provider" {
		token.kind = "provider"
	} else {
		token.kind = "tool"
	}
	if consumed.ActivityID != "" {
		startedEnvelope, eventErr := namedEvent(startTransaction, reference.StartTransactionID, reference.StartedEventID, protocol.EventActivityStarted)
		if eventErr != nil {
			return CommittedToken{}, eventErr
		}
		var started protocol.ActivityStartedV1
		if err := decodePayload(startedEnvelope.Payload, &started); err != nil {
			return CommittedToken{}, err
		}
		if started.DecisionNonce != consumed.DecisionNonce || started.DecisionEventID != reference.DecisionEventID || started.ActivityID != consumed.ActivityID || started.CallID != consumed.CallID || started.PlanDigest != consumed.PlanDigest || started.RequestDigest != consumed.RequestDigest || started.DispatchDigest != consumed.DispatchDigest || started.RuntimeGenerationID != consumed.RuntimeGenerationID {
			return CommittedToken{}, ErrStaleDecision
		}
	} else {
		startedEnvelope, eventErr := namedEvent(startTransaction, reference.StartTransactionID, reference.StartedEventID, protocol.EventControlOperationStarted)
		if eventErr != nil {
			return CommittedToken{}, eventErr
		}
		var started protocol.ControlOperationStartedV1
		if err := decodePayload(startedEnvelope.Payload, &started); err != nil {
			return CommittedToken{}, err
		}
		if started.DecisionNonce != consumed.DecisionNonce || started.DecisionEventID != reference.DecisionEventID || started.ControlOperationID != consumed.ControlOperationID || started.PlanDigest != consumed.PlanDigest || started.RequestDigest != consumed.RequestDigest || started.DispatchDigest != consumed.DispatchDigest || started.RuntimeGenerationID != consumed.RuntimeGenerationID {
			return CommittedToken{}, ErrStaleDecision
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.revocations[token.nonce] != 0 {
		return CommittedToken{}, ErrRevokedDecision
	}
	if _, exists := s.issued[token.nonce]; exists {
		return CommittedToken{}, ErrAlreadyUsedDecision
	}
	token.revocationEpoch = s.revocations[token.nonce]
	s.issued[token.nonce] = struct{}{}
	return token, nil
}

func (s *Service) Dispatch(ctx context.Context, token CommittedToken, binding DispatchBinding, register func(context.Context) error) error {
	if s == nil || !token.valid || binding.Kind == "" || binding.HandleID == "" || register == nil {
		return ErrInvalidCommittedToken
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if token.kind != binding.Kind || token.activityID != binding.ActivityID || token.controlOperationID != binding.ControlOperationID || token.callID != binding.CallID || token.planDigest != binding.PlanDigest || token.requestDigest != binding.RequestDigest || token.dispatchDigest != binding.DispatchDigest || token.runtimeGenerationID != binding.RuntimeGenerationID {
		return ErrInvalidCommittedToken
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.revocations[token.nonce] != token.revocationEpoch {
		return ErrRevokedDecision
	}
	if _, consumed := s.dispatched[token.nonce]; consumed {
		return ErrAlreadyDispatched
	}
	handleKey := binding.Kind + "\x00" + binding.HandleID
	if _, consumed := s.handles[handleKey]; consumed {
		return ErrAlreadyDispatched
	}
	s.dispatched[token.nonce] = struct{}{}
	s.handles[handleKey] = struct{}{}
	registrationContext := context.WithValue(ctx, cancellationRegistrationKey{}, func(cancel context.CancelFunc) error {
		if cancel == nil {
			return fmt.Errorf("cancellation hook is nil")
		}
		s.cancellations[token.nonce] = cancel
		return nil
	})
	return register(registrationContext)
}

func (s *Service) Revoke(nonce protocol.DecisionNonce) {
	if s == nil || nonce == "" {
		return
	}
	s.mu.Lock()
	s.revocations[nonce]++
	if cancel := s.cancellations[nonce]; cancel != nil {
		cancel()
		delete(s.cancellations, nonce)
	}
	s.mu.Unlock()
}

type cancellationRegistrationKey struct{}

func RegisterCancellation(ctx context.Context, cancel context.CancelFunc) error {
	register, ok := ctx.Value(cancellationRegistrationKey{}).(func(context.CancelFunc) error)
	if !ok {
		return nil
	}
	return register(cancel)
}

func (r CommitReference) validate() error {
	if err := r.Journal.Validate(); err != nil {
		return err
	}
	if r.DecisionTransactionID == "" || r.DecisionEventID == "" || r.StartTransactionID == "" || r.ConsumedEventID == "" || r.StartedEventID == "" {
		return fmt.Errorf("commit reference is incomplete")
	}
	return nil
}

func namedEvent(transaction journal.CommittedTransaction, transactionID protocol.TransactionID, eventID protocol.EventID, kind string) (protocol.EventEnvelope, error) {
	if transaction.TransactionID != transactionID || transaction.Journal.Validate() != nil {
		return protocol.EventEnvelope{}, fmt.Errorf("committed transaction identity mismatch")
	}
	var found protocol.EventEnvelope
	kindCount := 0
	for _, event := range transaction.Events {
		if event.TransactionID != transactionID {
			return protocol.EventEnvelope{}, fmt.Errorf("committed event transaction identity mismatch")
		}
		if event.Kind == kind {
			kindCount++
		}
		if event.EventID == eventID {
			if event.Kind != kind {
				return protocol.EventEnvelope{}, fmt.Errorf("named committed event kind mismatch")
			}
			found = event
		}
	}
	if found.EventID == "" || kindCount != 1 {
		return protocol.EventEnvelope{}, fmt.Errorf("committed transaction has incomplete or ambiguous %s event", kind)
	}
	return found, nil
}

func decodePayload(raw json.RawMessage, target any) error {
	if _, err := canonicaljson.Marshal(raw); err != nil {
		return err
	}
	return json.Unmarshal(raw, target)
}

func matchConsumption(decision protocol.AuthorizationDecision, digest protocol.Digest, decisionEventID protocol.EventID, consumed protocol.AuthorizationDecisionConsumedV1) error {
	r := decision.Request
	if consumed.DecisionNonce != decision.DecisionNonce || consumed.DecisionEventID != decisionEventID || consumed.DecisionDigest != digest || consumed.RequestID != r.RequestID || consumed.ActivityID != r.ActivityID || consumed.ControlOperationID != r.ControlOperationID || consumed.CallID != r.CallID || consumed.PlanDigest != r.PlanDigest || consumed.RequestDigest != r.RequestDigest || consumed.DispatchDigest != r.DispatchDigest || consumed.RuntimeGenerationID != r.RuntimeGenerationID {
		return ErrStaleDecision
	}
	return nil
}

func newDecisionNonce() (protocol.DecisionNonce, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate decision nonce: %w", err)
	}
	return protocol.DecisionNonce(hex.EncodeToString(raw)), nil
}

var _ TokenIssuer = (*Service)(nil)
var _ DispatchGate = (*Service)(nil)
