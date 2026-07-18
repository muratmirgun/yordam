package tooling

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type ActionHandle struct{ id string }

type PreviewResult struct {
	handleID          string
	observationDigest protocol.Digest
	evidenceDigests   []protocol.Digest
}

type PlanRequest struct {
	TurnID              protocol.TurnID
	ActivityID          protocol.ActivityID
	CallID              string
	Alias               string
	Arguments           json.RawMessage
	RuntimeGenerationID protocol.RuntimeGenerationID
}

type plannedAction struct {
	turnID      protocol.TurnID
	activityID  protocol.ActivityID
	request     PlanRequest
	entry       catalogEntry
	prepared    ports.PreparedTool
	plan        protocol.ActionPlan
	revalidated bool
}

type Service struct {
	catalog *Catalog
	mu      sync.Mutex
	actions map[string]*plannedAction
}

func NewService(catalog *Catalog) *Service {
	if catalog == nil {
		panic("tooling: catalog is nil")
	}
	return &Service{catalog: catalog, actions: make(map[string]*plannedAction)}
}

func (s *Service) Plan(ctx context.Context, request PlanRequest) (ActionHandle, protocol.ActionPlan, error) {
	return s.plan(ctx, request, "")
}

func (s *Service) PlanPreviewInspection(ctx context.Context, request PlanRequest) (ActionHandle, protocol.ActionPlan, error) {
	return s.plan(ctx, request, "observation")
}

func (s *Service) PlanMutation(ctx context.Context, preview PreviewResult, request PlanRequest) (ActionHandle, protocol.ActionPlan, error) {
	if preview.handleID == "" || preview.observationDigest.IsZero() {
		return ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("preview result is invalid")
	}
	s.mu.Lock()
	observed, ok := s.actions[preview.handleID]
	s.mu.Unlock()
	if !ok || observed.turnID != request.TurnID || observed.activityID != request.ActivityID || observed.plan.Digest != preview.observationDigest {
		return ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("preview result does not match turn and activity")
	}
	for _, digest := range preview.evidenceDigests {
		if err := digest.Validate(); err != nil {
			return ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("preview evidence digest: %w", err)
		}
	}
	return s.plan(ctx, request, "mutation")
}

func (s *Service) plan(ctx context.Context, request PlanRequest, requiredEffect string) (ActionHandle, protocol.ActionPlan, error) {
	if err := validatePlanRequest(request); err != nil {
		return ActionHandle{}, protocol.ActionPlan{}, err
	}
	entry, ok := s.catalog.byAlias[request.Alias]
	if !ok {
		return ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("unknown tool alias %q", request.Alias)
	}
	if requiredEffect != "" && entry.classification.Effect != requiredEffect {
		return ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("tool alias %q has effect %q, want %q", request.Alias, entry.classification.Effect, requiredEffect)
	}
	planner, ok := entry.tool.(ports.ToolPlanner)
	if !ok {
		return ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("tool alias %q does not support pure planning", request.Alias)
	}
	domainRequest := domain.ToolRequest{CallID: request.CallID, Name: request.Alias, Input: cloneRaw(request.Arguments)}
	prepared, err := planner.Plan(ctx, domainRequest)
	if err != nil {
		return ActionHandle{}, protocol.ActionPlan{}, err
	}
	if prepared == nil {
		return ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("tool alias %q returned nil plan", request.Alias)
	}
	plan, err := buildActionPlan(request, entry, prepared.Preview())
	if err != nil {
		return ActionHandle{}, protocol.ActionPlan{}, err
	}
	id, err := newHandleID()
	if err != nil {
		return ActionHandle{}, protocol.ActionPlan{}, err
	}
	s.mu.Lock()
	s.actions[id] = &plannedAction{turnID: request.TurnID, activityID: request.ActivityID, request: clonePlanRequest(request), entry: entry, prepared: prepared, plan: plan}
	s.mu.Unlock()
	return ActionHandle{id: id}, plan, nil
}

func (s *Service) Revalidate(ctx context.Context, handle ActionHandle) (protocol.ActionPlan, bool, error) {
	if handle.id == "" {
		return protocol.ActionPlan{}, false, fmt.Errorf("action handle is invalid")
	}
	s.mu.Lock()
	action, ok := s.actions[handle.id]
	if !ok {
		s.mu.Unlock()
		return protocol.ActionPlan{}, false, fmt.Errorf("action handle is unknown")
	}
	if action.revalidated {
		s.mu.Unlock()
		return protocol.ActionPlan{}, false, fmt.Errorf("action handle was already revalidated")
	}
	action.revalidated = true
	s.mu.Unlock()

	revalidator, ok := action.prepared.(ports.ResourceRevalidator)
	if !ok {
		return protocol.ActionPlan{}, false, fmt.Errorf("tool alias %q does not support resource revalidation", action.request.Alias)
	}
	preview, err := revalidator.Revalidate(ctx)
	if err != nil {
		return protocol.ActionPlan{}, false, err
	}
	current, err := buildActionPlan(action.request, action.entry, preview)
	if err != nil {
		return protocol.ActionPlan{}, false, err
	}
	changed := current.Digest != action.plan.Digest
	s.mu.Lock()
	action.plan = current
	s.mu.Unlock()
	return current, changed, nil
}

func validatePlanRequest(request PlanRequest) error {
	if request.TurnID == "" || request.ActivityID == "" || request.CallID == "" || request.Alias == "" || request.RuntimeGenerationID == "" {
		return fmt.Errorf("plan request is incomplete")
	}
	if err := protocol.ValidateBounds(request); err != nil {
		return fmt.Errorf("validate plan request bounds: %w", err)
	}
	return nil
}

func buildActionPlan(request PlanRequest, entry catalogEntry, preview domain.PreparedToolRequest) (protocol.ActionPlan, error) {
	resources, err := canonicalResources(preview.Resources)
	if err != nil {
		return protocol.ActionPlan{}, err
	}
	boundary := entry.classification.Boundary
	if boundary == "workspace" && !preview.InsideWorkspace {
		boundary = "filesystem_external"
	}
	body := protocol.ActionPlanBody{
		CallID: request.CallID, Tool: entry.descriptor.Body.Identity, SourceRevision: entry.descriptor.Body.SourceRevision,
		DescriptorDigest: entry.descriptor.DescriptorDigest, Action: entry.descriptor.Body.Identity.Name, Purpose: "mutate", Resources: resources,
		ExecutionLocus: entry.classification.ExecutionLoci[0], Effect: entry.classification.Effect, Boundary: boundary,
		Reversibility: entry.classification.Reversibility, VerificationCoverage: entry.classification.VerificationCoverage,
		RequestedProfile: entry.classification.RequestedProfile, EffectiveProfile: entry.classification.EffectiveProfile,
		RuntimeGenerationID: request.RuntimeGenerationID,
	}
	if entry.classification.Effect == "observation" {
		body.Purpose = "inspect"
	}
	if err := body.Validate(); err != nil {
		return protocol.ActionPlan{}, fmt.Errorf("validate action plan: %w", err)
	}
	digest, err := canonicalDigest(body)
	if err != nil {
		return protocol.ActionPlan{}, err
	}
	return protocol.ActionPlan{Body: body, Digest: digest}, nil
}

func canonicalResources(resources []protocol.ResourceTarget) ([]protocol.ResourceTarget, error) {
	result := make([]protocol.ResourceTarget, len(resources))
	for index, resource := range resources {
		resource.Attributes = append([]protocol.ResourceAttribute(nil), resource.Attributes...)
		sort.Slice(resource.Attributes, func(i, j int) bool {
			if resource.Attributes[i].Name != resource.Attributes[j].Name {
				return resource.Attributes[i].Name < resource.Attributes[j].Name
			}
			return resource.Attributes[i].Value < resource.Attributes[j].Value
		})
		result[index] = resource
	}
	sort.Slice(result, func(i, j int) bool { return resourceKey(result[i]) < resourceKey(result[j]) })
	seen := make(map[string]struct{}, len(result))
	for _, resource := range result {
		key := resourceKey(resource)
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate canonical resource %q", resource.CanonicalID)
		}
		seen[key] = struct{}{}
	}
	return result, nil
}

func resourceKey(resource protocol.ResourceTarget) string {
	return resource.Kind + "\x00" + resource.CanonicalID + "\x00" + resource.ParentID + "\x00" + resource.Digest
}

func canonicalDigest(value any) (protocol.Digest, error) { return canonicaljson.Digest(value) }

func newHandleID() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate action handle: %w", err)
	}
	return hex.EncodeToString(raw), nil
}

func clonePlanRequest(request PlanRequest) PlanRequest {
	request.Arguments = cloneRaw(request.Arguments)
	return request
}
