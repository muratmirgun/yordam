package tooling

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/muratmirgun/yordam/internal/authorization"
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
	turnID          protocol.TurnID
	activityID      protocol.ActivityID
	request         PlanRequest
	entry           catalogEntry
	prepared        ports.PreparedTool
	plan            protocol.ActionPlan
	requestDigest   protocol.Digest
	dispatchDigest  protocol.Digest
	evidenceDigests []protocol.Digest
	revalidated     bool
	operationMu     *sync.Mutex
}

type Service struct {
	catalog *Catalog
	mu      sync.Mutex
	actions map[string]*plannedAction
	gate    authorization.DispatchGate
}

func NewService(catalog *Catalog, gates ...authorization.DispatchGate) *Service {
	if catalog == nil {
		panic("tooling: catalog is nil")
	}
	if len(gates) > 1 {
		panic("tooling: at most one dispatch gate is allowed")
	}
	service := &Service{catalog: catalog, actions: make(map[string]*plannedAction)}
	if len(gates) == 1 {
		service.gate = gates[0]
	}
	return service
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
	var observedTurnID protocol.TurnID
	var observedActivityID protocol.ActivityID
	var observedDigest protocol.Digest
	var observedRequest PlanRequest
	var observedEntry catalogEntry
	var observedPrepared ports.PreparedTool
	var observedRequestDigest protocol.Digest
	var observedOperationMu *sync.Mutex
	if ok {
		observedTurnID = observed.turnID
		observedActivityID = observed.activityID
		observedDigest = observed.plan.Digest
		observedRequest = clonePlanRequest(observed.request)
		observedEntry = observed.entry
		observedPrepared = observed.prepared
		observedRequestDigest = observed.requestDigest
		observedOperationMu = observed.operationMu
	}
	s.mu.Unlock()
	if !ok || observedTurnID != request.TurnID || observedActivityID != request.ActivityID || observedDigest != preview.observationDigest {
		return ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("preview result does not match turn and activity")
	}
	for _, digest := range preview.evidenceDigests {
		if err := digest.Validate(); err != nil {
			return ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("preview evidence digest: %w", err)
		}
	}
	if observedRequest.Alias == request.Alias && observedEntry.classification.Effect != "observation" {
		if err := validatePlanRequest(request); err != nil {
			return ActionHandle{}, protocol.ActionPlan{}, err
		}
		requestDigest, err := canonicalDigest(request)
		if err != nil {
			return ActionHandle{}, protocol.ActionPlan{}, err
		}
		if requestDigest != observedRequestDigest {
			return ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("mutation request does not match the observed preview input")
		}
		plan, err := buildActionPlan(request, observedEntry, observedPrepared.Preview(), "")
		if err != nil {
			return ActionHandle{}, protocol.ActionPlan{}, err
		}
		dispatchDigest, err := toolDispatchDigest(plan, requestDigest, preview.evidenceDigests)
		if err != nil {
			return ActionHandle{}, protocol.ActionPlan{}, err
		}
		id, err := newHandleID()
		if err != nil {
			return ActionHandle{}, protocol.ActionPlan{}, err
		}
		s.mu.Lock()
		s.actions[id] = &plannedAction{turnID: request.TurnID, activityID: request.ActivityID, request: clonePlanRequest(request), entry: observedEntry, prepared: observedPrepared, plan: plan, requestDigest: requestDigest, dispatchDigest: dispatchDigest, evidenceDigests: append([]protocol.Digest(nil), preview.evidenceDigests...), operationMu: observedOperationMu}
		s.mu.Unlock()
		return ActionHandle{id: id}, plan, nil
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
	previewOnly := requiredEffect == "observation" && entry.classification.Effect != "observation"
	if requiredEffect != "" && entry.classification.Effect != requiredEffect && !previewOnly {
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
	if previewOnly {
		if _, ok := prepared.(ports.PreviewPreparer); !ok {
			return ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("tool alias %q has effect %q, want %q (no observation preview seam)", request.Alias, entry.classification.Effect, requiredEffect)
		}
	}
	effectOverride := ""
	if previewOnly {
		effectOverride = "observation"
	}
	plan, err := buildActionPlan(request, entry, prepared.Preview(), effectOverride)
	if err != nil {
		return ActionHandle{}, protocol.ActionPlan{}, err
	}
	id, err := newHandleID()
	if err != nil {
		return ActionHandle{}, protocol.ActionPlan{}, err
	}
	requestDigest, err := canonicalDigest(request)
	if err != nil {
		return ActionHandle{}, protocol.ActionPlan{}, err
	}
	boundDispatchDigest, err := toolDispatchDigest(plan, requestDigest, nil)
	if err != nil {
		return ActionHandle{}, protocol.ActionPlan{}, err
	}
	s.mu.Lock()
	s.actions[id] = &plannedAction{turnID: request.TurnID, activityID: request.ActivityID, request: clonePlanRequest(request), entry: entry, prepared: prepared, plan: plan, requestDigest: requestDigest, dispatchDigest: boundDispatchDigest, operationMu: &sync.Mutex{}}
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
	originalPlan := action.plan
	s.mu.Unlock()
	action.operationMu.Lock()
	defer action.operationMu.Unlock()

	revalidator, ok := action.prepared.(ports.ResourceRevalidator)
	if !ok {
		return protocol.ActionPlan{}, false, fmt.Errorf("tool alias %q does not support resource revalidation", action.request.Alias)
	}
	preview, err := revalidator.Revalidate(ctx)
	if err != nil {
		return protocol.ActionPlan{}, false, err
	}
	override := ""
	if originalPlan.Body.Effect == "observation" && action.entry.classification.Effect != "observation" {
		override = "observation"
	}
	current, err := buildActionPlan(action.request, action.entry, preview, override)
	if err != nil {
		return protocol.ActionPlan{}, false, err
	}
	changed := current.Digest != originalPlan.Digest
	s.mu.Lock()
	action.plan = current
	action.dispatchDigest, err = toolDispatchDigest(current, action.requestDigest, action.evidenceDigests)
	s.mu.Unlock()
	if err != nil {
		return protocol.ActionPlan{}, false, err
	}
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

func buildActionPlan(request PlanRequest, entry catalogEntry, preview domain.PreparedToolRequest, effectOverride string) (protocol.ActionPlan, error) {
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
	if effectOverride == "observation" {
		body.Action += ".preview"
		body.Purpose = "inspect"
		body.Effect = "observation"
		body.Reversibility = "not_applicable"
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

type toolDispatchBinding struct {
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
}

func toolDispatchDigest(plan protocol.ActionPlan, requestDigest protocol.Digest, evidenceDigests []protocol.Digest) (protocol.Digest, error) {
	return canonicalDigest(toolDispatchBinding{
		PlanDigest: plan.Digest, RequestDigest: requestDigest, Resources: plan.Body.Resources,
		ExecutionLocus: plan.Body.ExecutionLocus, Effect: plan.Body.Effect, Boundary: plan.Body.Boundary,
		RequestedProfile: plan.Body.RequestedProfile, EffectiveProfile: plan.Body.EffectiveProfile,
		RuntimeGenerationID: plan.Body.RuntimeGenerationID, EvidenceDigests: evidenceDigests,
	})
}

func (s *Service) Execute(ctx context.Context, handle ActionHandle, token authorization.CommittedToken) (protocol.ExecutionResult, error) {
	action, err := s.actionForDispatch(ctx, handle)
	if err != nil {
		return protocol.ExecutionResult{}, err
	}
	type executionOutput struct {
		result domain.ToolResult
		err    error
	}
	resultChannel := make(chan executionOutput, 1)
	runContext, cancel := context.WithCancel(ctx)
	binding := actionBinding(handle.id, action)
	err = s.gate.Dispatch(ctx, token, binding, func(registrationContext context.Context) error {
		if err := authorization.RegisterCancellation(registrationContext, cancel); err != nil {
			return err
		}
		go func() {
			action.operationMu.Lock()
			defer action.operationMu.Unlock()
			if revalidateErr := revalidateDispatch(runContext, action); revalidateErr != nil {
				resultChannel <- executionOutput{err: revalidateErr}
				return
			}
			resultChannel <- executionOutput{result: action.prepared.Execute(runContext)}
		}()
		return nil
	})
	if err != nil {
		cancel()
		return protocol.ExecutionResult{}, err
	}
	defer cancel()
	select {
	case <-ctx.Done():
		return protocol.ExecutionResult{}, ctx.Err()
	case completed := <-resultChannel:
		if completed.err != nil {
			return protocol.ExecutionResult{}, completed.err
		}
		return executionResult(completed.result), nil
	}
}

func (s *Service) PreparePreview(ctx context.Context, handle ActionHandle, token authorization.CommittedToken) (PreviewResult, protocol.ActionPlan, []protocol.EvidenceCandidate, error) {
	action, err := s.actionForDispatch(ctx, handle)
	if err != nil {
		return PreviewResult{}, protocol.ActionPlan{}, nil, err
	}
	if action.plan.Body.Effect != "observation" {
		return PreviewResult{}, protocol.ActionPlan{}, nil, fmt.Errorf("action handle is not an observation preview")
	}
	type previewOutput struct {
		result domain.ToolResult
		err    error
	}
	output := make(chan previewOutput, 1)
	runContext, cancel := context.WithCancel(ctx)
	err = s.gate.Dispatch(ctx, token, actionBinding(handle.id, action), func(registrationContext context.Context) error {
		if err := authorization.RegisterCancellation(registrationContext, cancel); err != nil {
			return err
		}
		go func() {
			action.operationMu.Lock()
			defer action.operationMu.Unlock()
			if revalidateErr := revalidateDispatch(runContext, action); revalidateErr != nil {
				output <- previewOutput{err: revalidateErr}
				return
			}
			if preparer, ok := action.prepared.(ports.PreviewPreparer); ok {
				if prepareErr := preparer.PreparePreview(runContext); prepareErr != nil {
					output <- previewOutput{err: prepareErr}
					return
				}
				preview := action.prepared.Preview()
				output <- previewOutput{result: domain.ToolResult{CallID: action.request.CallID, Status: domain.ToolSucceeded, Content: preview.ProposedDiff}}
				return
			}
			output <- previewOutput{result: action.prepared.Execute(runContext)}
		}()
		return nil
	})
	if err != nil {
		cancel()
		return PreviewResult{}, protocol.ActionPlan{}, nil, err
	}
	defer cancel()
	var completed previewOutput
	select {
	case <-ctx.Done():
		return PreviewResult{}, protocol.ActionPlan{}, nil, ctx.Err()
	case completed = <-output:
	}
	if completed.err != nil {
		return PreviewResult{}, protocol.ActionPlan{}, nil, completed.err
	}
	if completed.result.Status != domain.ToolSucceeded {
		return PreviewResult{}, protocol.ActionPlan{}, nil, fmt.Errorf("preview failed: %s", completed.result.Content)
	}
	evidence, evidenceDigest, err := previewEvidence(action, completed.result.Content)
	if err != nil {
		return PreviewResult{}, protocol.ActionPlan{}, nil, err
	}
	return PreviewResult{handleID: handle.id, observationDigest: action.plan.Digest, evidenceDigests: []protocol.Digest{evidenceDigest}}, action.plan, []protocol.EvidenceCandidate{evidence}, nil
}

func (s *Service) actionForDispatch(_ context.Context, handle ActionHandle) (*plannedAction, error) {
	if s == nil || s.gate == nil || handle.id == "" {
		return nil, authorization.ErrInvalidCommittedToken
	}
	s.mu.Lock()
	action, ok := s.actions[handle.id]
	if !ok {
		s.mu.Unlock()
		return nil, authorization.ErrInvalidCommittedToken
	}
	snapshot := *action
	s.mu.Unlock()
	return &snapshot, nil
}

func revalidateDispatch(ctx context.Context, action *plannedAction) error {
	revalidator, ok := action.prepared.(ports.ResourceRevalidator)
	if !ok {
		return fmt.Errorf("tool alias %q does not support resource revalidation", action.request.Alias)
	}
	preview, err := revalidator.Revalidate(ctx)
	if err != nil {
		return err
	}
	override := ""
	if action.plan.Body.Effect == "observation" && action.entry.classification.Effect != "observation" {
		override = "observation"
	}
	current, err := buildActionPlan(action.request, action.entry, preview, override)
	if err != nil {
		return err
	}
	dispatchDigest, err := toolDispatchDigest(current, action.requestDigest, action.evidenceDigests)
	if err != nil {
		return err
	}
	if current.Digest != action.plan.Digest || dispatchDigest != action.dispatchDigest {
		return authorization.ErrStaleDecision
	}
	return nil
}

func actionBinding(handleID string, action *plannedAction) authorization.DispatchBinding {
	return authorization.DispatchBinding{Kind: "tool", HandleID: handleID, ActivityID: action.activityID, CallID: action.request.CallID, PlanDigest: action.plan.Digest, RequestDigest: action.requestDigest, DispatchDigest: action.dispatchDigest, RuntimeGenerationID: action.request.RuntimeGenerationID}
}

func executionResult(result domain.ToolResult) protocol.ExecutionResult {
	return protocol.ExecutionResult{Outcome: protocol.ActivityOutcomeV1{Status: string(result.Status), Reason: string(result.ErrorKind)}, ToolResult: protocol.ToolResultBlock{CallID: result.CallID, Status: string(result.Status), Text: result.Content}}
}

func previewEvidence(action *plannedAction, content string) (protocol.EvidenceCandidate, protocol.Digest, error) {
	digest, err := canonicalDigest(struct {
		ActivityID protocol.ActivityID
		PlanDigest protocol.Digest
		Content    string
	}{action.activityID, action.plan.Digest, content})
	if err != nil {
		return protocol.EvidenceCandidate{}, protocol.Digest{}, err
	}
	subject := protocol.SubjectRef{Kind: "action", ID: action.request.CallID}
	if len(action.plan.Body.Resources) != 0 {
		subject = protocol.SubjectRef{Kind: action.plan.Body.Resources[0].Kind, ID: action.plan.Body.Resources[0].CanonicalID}
	}
	redacted := []byte("[redacted preview; observation digest sha256:" + digest.Value + "]")
	candidate := protocol.EvidenceCandidate{ID: protocol.EvidenceID(digest.Value), Kind: "tool_preview", MediaType: "text/plain", ProducingActivityID: action.activityID, Actor: protocol.ActorRef{ID: protocol.ActorID(action.plan.Body.Tool.Name), Kind: protocol.ActorTool}, Subject: subject, Content: redacted, Limit: protocol.MaxByteFieldBytes}
	return candidate, digest, nil
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
