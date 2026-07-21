package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type PreparedRequest struct {
	adapterKind string
	canonical   json.RawMessage
}

func (r PreparedRequest) Valid() bool { return r.adapterKind != "" && len(r.canonical) != 0 }

// Decode is available only to adapter implementations receiving an opaque
// PreparedRequest from Service. It never exposes the canonical bytes or a
// transport capability to callers holding ProviderHandle.
func (r PreparedRequest) Decode(adapterKind string, target any) error {
	if !r.Valid() || adapterKind == "" || r.adapterKind != adapterKind || target == nil {
		return fmt.Errorf("prepared request adapter binding mismatch")
	}
	return json.Unmarshal(r.canonical, target)
}

func NewPreparedRequest(adapterKind string, value any) (PreparedRequest, error) {
	if adapterKind == "" || value == nil {
		return PreparedRequest{}, fmt.Errorf("prepared request is incomplete")
	}
	canonical, err := canonicaljson.Marshal(value)
	if err != nil {
		return PreparedRequest{}, fmt.Errorf("canonicalize prepared request: %w", err)
	}
	return PreparedRequest{adapterKind: adapterKind, canonical: canonical}, nil
}

type Adapter interface {
	Kind() string
	Normalize(context.Context, protocol.ModelRequest) (PreparedRequest, error)
	StartPrepared(context.Context, PreparedRequest) (<-chan protocol.ModelEvent, error)
}

type preparedRequest struct {
	adapterKind string
	normalized  PreparedRequest
	adapter     Adapter
}

type ProviderHandle struct {
	id                  string
	activityID          protocol.ActivityID
	callID              string
	planDigest          protocol.Digest
	runtimeGenerationID protocol.RuntimeGenerationID
	requestDigest       protocol.Digest
	contextPlanDigest   protocol.Digest
	dispatchDigest      protocol.Digest
	prepared            *preparedRequest
}

func (h ProviderHandle) Valid() bool {
	return h.id != "" && h.activityID != "" && h.callID != "" && h.runtimeGenerationID != "" && h.prepared != nil && h.planDigest.Validate() == nil && h.requestDigest.Validate() == nil && h.contextPlanDigest.Validate() == nil && h.dispatchDigest.Validate() == nil
}

type Service struct {
	catalog  Catalog
	adapters map[string]Adapter
	nextID   atomic.Uint64
	gate     authorization.DispatchGate
	mu       sync.RWMutex
	handles  map[string]ProviderHandle
}

func NewService(catalog Catalog, adapters []Adapter, gates ...authorization.DispatchGate) (*Service, error) {
	if catalog == nil {
		return nil, fmt.Errorf("provider catalog is required")
	}
	if len(gates) > 1 {
		return nil, fmt.Errorf("provider service accepts at most one dispatch gate")
	}
	service := &Service{catalog: catalog, adapters: make(map[string]Adapter, len(adapters)), handles: make(map[string]ProviderHandle)}
	if len(gates) == 1 {
		service.gate = gates[0]
	}
	for _, adapter := range adapters {
		if adapter == nil || adapter.Kind() == "" {
			return nil, fmt.Errorf("provider adapter is invalid")
		}
		if _, duplicate := service.adapters[adapter.Kind()]; duplicate {
			return nil, fmt.Errorf("duplicate provider adapter %q", adapter.Kind())
		}
		service.adapters[adapter.Kind()] = adapter
	}
	return service, nil
}

type dispatchBinding struct {
	RequestDigest       protocol.Digest              `json:"request_digest"`
	ContextPlanDigest   protocol.Digest              `json:"context_plan_digest"`
	ProviderPlanDigest  protocol.Digest              `json:"provider_plan_digest"`
	RuntimeGenerationID protocol.RuntimeGenerationID `json:"runtime_generation_id"`
}

func (s *Service) Prepare(ctx context.Context, activityID protocol.ActivityID, callID string, request protocol.ModelRequest, contextPlanDigest protocol.Digest) (ProviderHandle, error) {
	if err := ctx.Err(); err != nil {
		return ProviderHandle{}, err
	}
	if activityID == "" || callID == "" || request.RequestID == "" {
		return ProviderHandle{}, fmt.Errorf("provider preparation identity is incomplete")
	}
	if err := contextPlanDigest.Validate(); err != nil {
		return ProviderHandle{}, fmt.Errorf("context plan digest: %w", err)
	}
	if err := validateNegotiatedRequest(s.catalog, request); err != nil {
		return ProviderHandle{}, err
	}
	adapter := s.adapters[request.Plan.Body.Descriptor.AdapterKind]
	if adapter == nil {
		return ProviderHandle{}, fmt.Errorf("provider adapter %q is unavailable", request.Plan.Body.Descriptor.AdapterKind)
	}
	normalized, err := adapter.Normalize(ctx, protocol.DeepCopy(request))
	if err != nil {
		return ProviderHandle{}, err
	}
	if normalized.adapterKind != adapter.Kind() || len(normalized.canonical) == 0 {
		return ProviderHandle{}, fmt.Errorf("adapter returned invalid prepared request")
	}
	requestDigest, err := canonicaljson.Digest(request)
	if err != nil {
		return ProviderHandle{}, err
	}
	boundDispatchDigest, err := DispatchDigest(requestDigest, contextPlanDigest, request.Plan.Digest, request.Plan.Body.Descriptor.RuntimeGenerationID)
	if err != nil {
		return ProviderHandle{}, err
	}
	id := fmt.Sprintf("provider-handle-%d", s.nextID.Add(1))
	handle := ProviderHandle{id: id, activityID: activityID, callID: callID, planDigest: request.Plan.Digest, runtimeGenerationID: request.Plan.Body.Descriptor.RuntimeGenerationID, requestDigest: requestDigest, contextPlanDigest: contextPlanDigest, dispatchDigest: boundDispatchDigest, prepared: &preparedRequest{adapterKind: adapter.Kind(), normalized: normalized, adapter: adapter}}
	s.mu.Lock()
	s.handles[id] = handle
	s.mu.Unlock()
	return handle, nil
}

func (s *Service) Stream(ctx context.Context, handle ProviderHandle, token authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	if s == nil || s.gate == nil || !handle.Valid() {
		return nil, authorization.ErrInvalidCommittedToken
	}
	s.mu.RLock()
	stored, ok := s.handles[handle.id]
	s.mu.RUnlock()
	if !ok || !sameProviderHandle(stored, handle) {
		return nil, authorization.ErrInvalidCommittedToken
	}
	binding := authorization.DispatchBinding{
		Kind: "provider", HandleID: stored.id, ActivityID: stored.activityID, CallID: stored.callID,
		PlanDigest: stored.planDigest, RequestDigest: stored.requestDigest, DispatchDigest: stored.dispatchDigest,
		RuntimeGenerationID: stored.runtimeGenerationID,
	}
	var stream <-chan protocol.ModelEvent
	runContext, cancel := context.WithCancel(ctx)
	err := s.gate.Dispatch(ctx, token, binding, func(startContext context.Context) error {
		if registerErr := authorization.RegisterCancellation(startContext, cancel); registerErr != nil {
			return registerErr
		}
		var startErr error
		stream, startErr = stored.prepared.adapter.StartPrepared(runContext, stored.prepared.normalized)
		return startErr
	})
	if err != nil {
		cancel()
		return nil, err
	}
	if stream == nil {
		return nil, fmt.Errorf("provider adapter returned a nil stream")
	}
	return stream, nil
}

func sameProviderHandle(left, right ProviderHandle) bool {
	return left.id == right.id && left.activityID == right.activityID && left.callID == right.callID && left.planDigest == right.planDigest && left.runtimeGenerationID == right.runtimeGenerationID && left.requestDigest == right.requestDigest && left.contextPlanDigest == right.contextPlanDigest && left.dispatchDigest == right.dispatchDigest && left.prepared == right.prepared
}

// DispatchDigest is the canonical authorization binding shared by provider
// preparation and orchestration. Any egress authorization must use this exact
// shape or ProviderHandle.Stream will reject its committed token.
func DispatchDigest(requestDigest, contextPlanDigest, providerPlanDigest protocol.Digest, runtimeGenerationID protocol.RuntimeGenerationID) (protocol.Digest, error) {
	return canonicaljson.Digest(dispatchBinding{
		RequestDigest: requestDigest, ContextPlanDigest: contextPlanDigest,
		ProviderPlanDigest: providerPlanDigest, RuntimeGenerationID: runtimeGenerationID,
	})
}

func validateNegotiatedRequest(catalog Catalog, request protocol.ModelRequest) error {
	if request.ProviderID == "" || request.ModelID == "" || request.ProviderID != request.Plan.Body.Descriptor.ProviderID || request.ModelID != request.Plan.Body.Descriptor.ModelID {
		return fmt.Errorf("provider request does not match negotiated descriptor")
	}
	if err := request.Plan.Body.Descriptor.Validate(); err != nil {
		return err
	}
	if err := canonicaljson.ValidateDigest(request.Plan.Body, request.Plan.Digest); err != nil {
		return fmt.Errorf("provider plan: %w", err)
	}
	if len(request.Requirements) != len(request.Plan.Body.Requirements) {
		return fmt.Errorf("provider requirements do not match negotiated plan")
	}
	for index := range request.Requirements {
		if request.Requirements[index] != request.Plan.Body.Requirements[index] {
			return fmt.Errorf("provider requirements do not match negotiated plan")
		}
	}
	negotiated, err := catalog.Negotiate(request.ProviderID, request.ModelID, request.Requirements, request.Plan.Body.ToolExposureRevision)
	if err != nil {
		return err
	}
	if negotiated.Digest != request.Plan.Digest {
		return fmt.Errorf("provider plan is stale or does not match the catalog")
	}
	for _, message := range request.Messages {
		if message.Role == "" || len(message.Blocks) == 0 {
			return fmt.Errorf("provider message is incomplete")
		}
		for _, block := range message.Blocks {
			if err := block.Validate(); err != nil {
				return err
			}
		}
	}
	return nil
}
