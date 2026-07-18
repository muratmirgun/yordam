package provider

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type recordingAdapter struct {
	calls  int
	starts atomic.Int64
}

func (a *recordingAdapter) Kind() string { return "openai_compatible" }

func (a *recordingAdapter) Normalize(_ context.Context, request protocol.ModelRequest) (PreparedRequest, error) {
	a.calls++
	return NewPreparedRequest(a.Kind(), protocol.DeepCopy(request))
}

func (a *recordingAdapter) StartPrepared(_ context.Context, prepared PreparedRequest) (<-chan protocol.ModelEvent, error) {
	var request protocol.ModelRequest
	if err := prepared.Decode(a.Kind(), &request); err != nil {
		return nil, err
	}
	a.starts.Add(1)
	events := make(chan protocol.ModelEvent)
	close(events)
	return events, nil
}

func TestDispatchProviderBindsCommittedTokenAndStartsExactlyOneAttempt(t *testing.T) {
	service, adapter, handle := preparedProvider(t, nil)
	gate, token := providerToken(t, handle)
	service.gate = gate
	events, err := service.Stream(context.Background(), handle, token)
	if err != nil {
		t.Fatal(err)
	}
	for range events {
	}
	if adapter.starts.Load() != 1 {
		t.Fatalf("provider starts=%d want 1", adapter.starts.Load())
	}
	if _, err := service.Stream(context.Background(), handle, token); !errors.Is(err, authorization.ErrAlreadyDispatched) {
		t.Fatalf("repeat err=%v", err)
	}
	if adapter.starts.Load() != 1 {
		t.Fatalf("repeat sent provider request: starts=%d", adapter.starts.Load())
	}
}

func TestDispatchProviderRejectsZeroStaleRevokedAndMutatedBindingsBeforeAttempt(t *testing.T) {
	mutations := map[string]func(*ProviderHandle){
		"activity":   func(h *ProviderHandle) { h.activityID = "other" },
		"call":       func(h *ProviderHandle) { h.callID = "other" },
		"plan":       func(h *ProviderHandle) { h.planDigest = testDigest('a') },
		"request":    func(h *ProviderHandle) { h.requestDigest = testDigest('a') },
		"dispatch":   func(h *ProviderHandle) { h.dispatchDigest = testDigest('a') },
		"generation": func(h *ProviderHandle) { h.runtimeGenerationID = "other" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			service, adapter, original := preparedProvider(t, nil)
			gate, token := providerToken(t, original)
			service.gate = gate
			changed := original
			mutate(&changed)
			if _, err := service.Stream(context.Background(), changed, token); !errors.Is(err, authorization.ErrInvalidCommittedToken) {
				t.Fatalf("err=%v", err)
			}
			if adapter.starts.Load() != 0 {
				t.Fatalf("provider starts=%d", adapter.starts.Load())
			}
		})
	}
	service, adapter, handle := preparedProvider(t, authorization.NewService(nil))
	if _, err := service.Stream(context.Background(), handle, authorization.CommittedToken{}); !errors.Is(err, authorization.ErrInvalidCommittedToken) {
		t.Fatalf("zero token err=%v", err)
	}
	if adapter.starts.Load() != 0 {
		t.Fatalf("zero token starts=%d", adapter.starts.Load())
	}

	revokedService, revokedAdapter, revokedHandle := preparedProvider(t, nil)
	gate, token := providerToken(t, revokedHandle)
	revokedService.gate = gate
	gate.Revoke("provider-nonce")
	if _, err := revokedService.Stream(context.Background(), revokedHandle, token); !errors.Is(err, authorization.ErrRevokedDecision) {
		t.Fatalf("revoked err=%v", err)
	}
	if revokedAdapter.starts.Load() != 0 {
		t.Fatalf("revoked starts=%d", revokedAdapter.starts.Load())
	}
}

func TestDispatchProviderConcurrentDoubleDispatchSendsOneAttempt(t *testing.T) {
	service, adapter, handle := preparedProvider(t, nil)
	gate, token := providerToken(t, handle)
	service.gate = gate
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wait sync.WaitGroup
	wait.Add(2)
	for range 2 {
		go func() {
			defer wait.Done()
			<-start
			_, err := service.Stream(context.Background(), handle, token)
			errs <- err
		}()
	}
	close(start)
	wait.Wait()
	close(errs)
	var success, repeated int
	for err := range errs {
		if err == nil {
			success++
		} else if errors.Is(err, authorization.ErrAlreadyDispatched) {
			repeated++
		} else {
			t.Fatalf("err=%v", err)
		}
	}
	if success != 1 || repeated != 1 || adapter.starts.Load() != 1 {
		t.Fatalf("success=%d repeated=%d starts=%d", success, repeated, adapter.starts.Load())
	}
}

func preparedProvider(t *testing.T, gate authorization.DispatchGate) (*Service, *recordingAdapter, ProviderHandle) {
	t.Helper()
	descriptor := descriptorWith(protocol.CapabilityTextInput, protocol.CapabilitySupported)
	catalog := NewCatalog("rev-1", []protocol.ModelDescriptor{descriptor})
	plan, err := catalog.Negotiate("openai", "model-a", []protocol.CapabilityRequirement{{Capability: protocol.CapabilityTextInput, Level: protocol.CapabilityRequired}}, "tools-rev")
	if err != nil {
		t.Fatal(err)
	}
	adapter := &recordingAdapter{}
	service, err := NewService(catalog, []Adapter{adapter}, gate)
	if err != nil {
		t.Fatal(err)
	}
	request := protocol.ModelRequest{RequestID: "request-1", ProviderID: "openai", ModelID: "model-a", Messages: []protocol.ModelMessage{{Role: "user", Blocks: []protocol.ContentBlock{{Kind: protocol.ContentText, Text: "hello"}}}}, Requirements: plan.Body.Requirements, Plan: plan}
	handle, err := service.Prepare(context.Background(), "activity-1", "call-1", request, testDigest('c'))
	if err != nil {
		t.Fatal(err)
	}
	return service, adapter, handle
}

type providerTokenReader struct {
	transactions map[protocol.TransactionID]journal.CommittedTransaction
}

func (r providerTokenReader) ReadCommittedTransaction(_ context.Context, _ protocol.JournalRef, id protocol.TransactionID) (journal.CommittedTransaction, error) {
	transaction, ok := r.transactions[id]
	if !ok {
		return journal.CommittedTransaction{}, errors.New("not committed")
	}
	return transaction, nil
}

func providerToken(t *testing.T, handle ProviderHandle) (*authorization.Service, authorization.CommittedToken) {
	t.Helper()
	request := protocol.AuthorizationRequest{
		RequestID: "provider-authorization", Principal: protocol.ActorRef{ID: "user", Kind: protocol.ActorUser}, Actor: protocol.ActorRef{ID: "agent", Kind: protocol.ActorAgent},
		SessionID: "session", ActivityID: handle.activityID, CallID: handle.callID, QueueID: "queue", Source: protocol.ToolIdentity{Source: "provider", Authority: "openai", Name: "model-a"},
		SourceRevision: "provider-r1", DescriptorDigest: testDigest('d'), Action: "model_egress", Resources: []protocol.ResourceTarget{{Kind: "provider", CanonicalID: "openai/model-a"}},
		ExecutionLocus: "remote", RequestedProfile: "networked", EffectiveProfile: "networked", Effect: "egress", Boundary: "network", Reversibility: "not_applicable", VerificationCoverage: "provider_reported",
		RuntimeGenerationID: handle.runtimeGenerationID, PolicyGeneration: "policy-1", PolicyProvenance: []protocol.PolicyProvenance{{Source: "compatibility", Revision: "1", Generation: "policy-1"}},
		PlanDigest: handle.planDigest, RequestDigest: handle.requestDigest, DispatchDigest: handle.dispatchDigest,
	}
	decision := protocol.AuthorizationDecision{Request: request, Action: "allow", Scope: protocol.CanonicalAuthorizationScope{Capability: request.Action, Source: request.Source, Resources: request.Resources, Constraints: []protocol.AuthorizationConstraint{}}, Constraints: []protocol.AuthorizationConstraint{}, Lifetime: protocol.AuthorizationLifetimeOnce, PolicySource: "compatibility", PolicyGeneration: request.PolicyGeneration, Reason: "configured provider", DecidedAt: time.Now().UTC(), PlanDigest: request.PlanDigest, DecisionNonce: "provider-nonce"}
	decisionDigest, err := canonicaljson.Digest(decision)
	if err != nil {
		t.Fatal(err)
	}
	consumed := protocol.AuthorizationDecisionConsumedV1{DecisionNonce: decision.DecisionNonce, DecisionEventID: "decision-event", DecisionDigest: decisionDigest, RequestID: request.RequestID, ActivityID: request.ActivityID, CallID: request.CallID, PlanDigest: request.PlanDigest, RequestDigest: request.RequestDigest, DispatchDigest: request.DispatchDigest, RuntimeGenerationID: request.RuntimeGenerationID}
	started := protocol.ActivityStartedV1{DecisionNonce: decision.DecisionNonce, DecisionEventID: "decision-event", ActivityID: request.ActivityID, CallID: request.CallID, PlanDigest: request.PlanDigest, RequestDigest: request.RequestDigest, DispatchDigest: request.DispatchDigest, RuntimeGenerationID: request.RuntimeGenerationID, DispatchState: "registered"}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session"}
	reader := providerTokenReader{transactions: map[protocol.TransactionID]journal.CommittedTransaction{
		"decision-tx": {Journal: ref, TransactionID: "decision-tx", Cursor: protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 10, TransactionID: "decision-tx"}, Events: []protocol.EventEnvelope{{EventID: "decision-event", TransactionID: "decision-tx", Kind: protocol.EventAuthorizationDecided, Payload: providerJSON(t, protocol.AuthorizationDecidedV1{Decision: decision})}}},
		"start-tx":    {Journal: ref, TransactionID: "start-tx", Cursor: protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 11, TransactionID: "start-tx"}, Events: []protocol.EventEnvelope{{EventID: "consumed-event", TransactionID: "start-tx", Kind: protocol.EventAuthorizationDecisionConsumed, Payload: providerJSON(t, consumed)}, {EventID: "started-event", TransactionID: "start-tx", Kind: protocol.EventActivityStarted, Payload: providerJSON(t, started)}}},
	}}
	gate := authorization.NewService(reader)
	token, err := gate.Issue(context.Background(), authorization.CommitReference{Journal: ref, DecisionTransactionID: "decision-tx", DecisionEventID: "decision-event", StartTransactionID: "start-tx", ConsumedEventID: "consumed-event", StartedEventID: "started-event"})
	if err != nil {
		t.Fatal(err)
	}
	return gate, token
}

func providerJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestServicePrepareIsEffectFreeAndBindsOpaqueHandle(t *testing.T) {
	descriptor := descriptorWith(protocol.CapabilityTextInput, protocol.CapabilitySupported)
	catalog := NewCatalog("rev-1", []protocol.ModelDescriptor{descriptor})
	plan, err := catalog.Negotiate("openai", "model-a", []protocol.CapabilityRequirement{{Capability: protocol.CapabilityTextInput, Level: protocol.CapabilityRequired}}, "tools-rev")
	if err != nil {
		t.Fatal(err)
	}
	adapter := &recordingAdapter{}
	service, err := NewService(catalog, []Adapter{adapter})
	if err != nil {
		t.Fatal(err)
	}
	request := protocol.ModelRequest{RequestID: "request-1", ProviderID: "openai", ModelID: "model-a", Messages: []protocol.ModelMessage{{Role: "user", Blocks: []protocol.ContentBlock{{Kind: protocol.ContentText, Text: "hello"}}}}, Requirements: plan.Body.Requirements, Plan: plan}
	contextDigest := testDigest('c')
	handle, err := service.Prepare(context.Background(), "activity-1", "call-1", request, contextDigest)
	if err != nil {
		t.Fatal(err)
	}
	if !handle.Valid() || adapter.calls != 1 || handle.activityID != "activity-1" || handle.callID != "call-1" || handle.contextPlanDigest != contextDigest || handle.runtimeGenerationID != "generation-1" {
		t.Fatalf("handle=%#v calls=%d", handle, adapter.calls)
	}
	wantRequest, _ := canonicaljson.Digest(request)
	if handle.requestDigest != wantRequest {
		t.Fatalf("request digest=%v want=%v", handle.requestDigest, wantRequest)
	}
	wantDispatch, _ := canonicaljson.Digest(dispatchBinding{RequestDigest: wantRequest, ContextPlanDigest: contextDigest, ProviderPlanDigest: plan.Digest, RuntimeGenerationID: "generation-1"})
	if handle.dispatchDigest != wantDispatch {
		t.Fatalf("dispatch digest=%v want=%v", handle.dispatchDigest, wantDispatch)
	}
	if (ProviderHandle{}).Valid() {
		t.Fatal("zero handle is valid")
	}
}

func TestServiceRejectsUnnegotiatedOrMismatchedRequestsBeforeAdapter(t *testing.T) {
	descriptor := descriptorWith(protocol.CapabilityStructuredOutput, protocol.CapabilityUnknown)
	adapter := &recordingAdapter{}
	service, err := NewService(NewCatalog("rev-1", []protocol.ModelDescriptor{descriptor}), []Adapter{adapter})
	if err != nil {
		t.Fatal(err)
	}
	requirements := []protocol.CapabilityRequirement{{Capability: protocol.CapabilityStructuredOutput, Level: protocol.CapabilityRequired}}
	body := protocol.NegotiatedProviderPlanBody{Descriptor: descriptor, Requirements: requirements, ToolExposureRevision: "tools-r1", Warnings: []string{}}
	digest, err := canonicaljson.Digest(body)
	if err != nil {
		t.Fatal(err)
	}
	request := protocol.ModelRequest{RequestID: "request-1", ProviderID: "openai", ModelID: "model-a", Requirements: requirements, Plan: protocol.NegotiatedProviderPlan{Body: body, Digest: digest}}
	if _, err := service.Prepare(context.Background(), "activity", "call", request, testDigest('d')); !errors.Is(err, ErrCapabilityUnavailable) {
		t.Fatalf("unnegotiated request error=%v", err)
	}
	if adapter.calls != 0 {
		t.Fatalf("adapter calls=%d", adapter.calls)
	}
}
