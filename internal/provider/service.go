package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type PreparedRequest struct {
	adapterKind string
	canonical   json.RawMessage
}

func (r PreparedRequest) Valid() bool { return r.adapterKind != "" && len(r.canonical) != 0 }

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
}

type preparedRequest struct {
	adapterKind string
	normalized  PreparedRequest
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
	return h.id != "" && h.activityID != "" && h.callID != "" && h.prepared != nil && h.planDigest.Validate() == nil && h.requestDigest.Validate() == nil && h.contextPlanDigest.Validate() == nil && h.dispatchDigest.Validate() == nil
}

type Service struct {
	catalog  Catalog
	adapters map[string]Adapter
	nextID   atomic.Uint64
}

func NewService(catalog Catalog, adapters []Adapter) (*Service, error) {
	if catalog == nil {
		return nil, fmt.Errorf("provider catalog is required")
	}
	service := &Service{catalog: catalog, adapters: make(map[string]Adapter, len(adapters))}
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
	boundDispatchDigest, err := dispatchDigest(requestDigest, contextPlanDigest, request.Plan.Digest, request.Plan.Body.Descriptor.RuntimeGenerationID)
	if err != nil {
		return ProviderHandle{}, err
	}
	id := fmt.Sprintf("provider-handle-%d", s.nextID.Add(1))
	return ProviderHandle{id: id, activityID: activityID, callID: callID, planDigest: request.Plan.Digest, runtimeGenerationID: request.Plan.Body.Descriptor.RuntimeGenerationID, requestDigest: requestDigest, contextPlanDigest: contextPlanDigest, dispatchDigest: boundDispatchDigest, prepared: &preparedRequest{adapterKind: adapter.Kind(), normalized: normalized}}, nil
}

func dispatchDigest(requestDigest, contextPlanDigest, providerPlanDigest protocol.Digest, runtimeGenerationID protocol.RuntimeGenerationID) (protocol.Digest, error) {
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
