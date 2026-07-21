package tooling

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/recovery"
)

type ActionHandle struct{ id string }

type PreviewResult struct {
	handleID          string
	observationDigest protocol.Digest
	evidenceDigests   []protocol.Digest
	requestDigest     protocol.Digest
	preparedDigest    protocol.Digest
	effect            string
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
	previewReady    bool
	previewConsumed bool
	previewPrepared protocol.Digest
	previewEvidence []protocol.Digest
	revalidated     bool
	operationMu     *sync.Mutex
}

type Service struct {
	catalog *Catalog
	mu      sync.Mutex
	actions map[string]*plannedAction
	gate    authorization.DispatchGate
}

type RecoverySink interface {
	Put(context.Context, recovery.Candidate) (protocol.RecoveryMaterialRecord, error)
}

// PrepareAndPutRecovery deliberately keeps raw preimage bytes behind the
// tooling boundary. Callers receive only the durable material metadata.
func (s *Service) PrepareAndPutRecovery(ctx context.Context, preview PreviewResult, activityID protocol.ActivityID, checkpoint protocol.CheckpointBody, plan protocol.ActionPlan, sink RecoverySink) (protocol.RecoveryMaterialRecord, error) {
	if s == nil || preview.handleID == "" || activityID == "" || checkpoint.ID == "" || checkpoint.SessionID == "" || len(checkpoint.Coverage) == 0 || sink == nil {
		return protocol.RecoveryMaterialRecord{}, fmt.Errorf("recovery material binding is incomplete")
	}
	if err := plan.Digest.Validate(); err != nil {
		return protocol.RecoveryMaterialRecord{}, fmt.Errorf("recovery plan digest: %w", err)
	}
	s.mu.Lock()
	action, ok := s.actions[preview.handleID]
	s.mu.Unlock()
	if !ok || !action.previewReady {
		return protocol.RecoveryMaterialRecord{}, fmt.Errorf("recovery preview is stale")
	}
	provider, ok := action.prepared.(ports.RecoveryMaterialProvider)
	if !ok {
		return protocol.RecoveryMaterialRecord{}, fmt.Errorf("tool does not provide recovery material")
	}
	action.operationMu.Lock()
	candidate, available, err := provider.RecoveryMaterial(ctx)
	action.operationMu.Unlock()
	if err != nil {
		return protocol.RecoveryMaterialRecord{}, err
	}
	if !available {
		return protocol.RecoveryMaterialRecord{}, fmt.Errorf("recovery material is unavailable")
	}
	candidate.WorkspaceID = string(checkpoint.SessionID)
	candidate.ActivityID = activityID
	candidate.CheckpointID = checkpoint.ID
	candidate.Subject = checkpoint.Coverage[0].Subject
	candidate.PlanDigest = plan.Digest
	return sink.Put(ctx, candidate)
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

// PlanOrchestratedAuthorization builds a dispatch-free orchestration plan for a
// catalog-bound OrchestratedTool. It is intentionally separate from the
// ordinary preview seam: no resource is inspected and the returned handle is
// never executable by the tool dispatcher. The exact canonical descriptor and
// marker are checked before the authorization plan can be created.
func (s *Service) PlanOrchestratedAuthorization(ctx context.Context, request PlanRequest, expectedKind string) (ActionHandle, protocol.ActionPlan, error) {
	if expectedKind == "" {
		return ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("orchestrated kind is required")
	}
	entry, ok := s.catalog.byAlias[request.Alias]
	if !ok {
		return ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("unknown tool alias %q", request.Alias)
	}
	kind, ok := s.catalog.OrchestratedKind(request.Alias, entry.descriptor)
	if !ok || kind != expectedKind || entry.classification.Effect != "orchestration" {
		return ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("tool alias %q is not exact orchestrated kind %q", request.Alias, expectedKind)
	}
	handle, plan, err := s.plan(ctx, request, "orchestration")
	if err != nil {
		return ActionHandle{}, protocol.ActionPlan{}, err
	}
	// The orchestrator owns execution. Remove the temporary planned action so
	// no token can ever dispatch the marker through ordinary tool execution.
	s.mu.Lock()
	delete(s.actions, handle.id)
	s.mu.Unlock()
	return ActionHandle{}, plan, nil
}

func (s *Service) PlanMutation(_ context.Context, preview PreviewResult, request PlanRequest) (ActionHandle, protocol.ActionPlan, error) {
	if preview.handleID == "" || preview.observationDigest.IsZero() || preview.requestDigest.IsZero() || preview.preparedDigest.IsZero() || preview.effect == "" {
		return ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("preview result is invalid")
	}
	s.mu.Lock()
	observed, ok := s.actions[preview.handleID]
	var snapshot plannedAction
	if ok {
		snapshot = *observed
		snapshot.request = clonePlanRequest(observed.request)
		snapshot.previewEvidence = append([]protocol.Digest(nil), observed.previewEvidence...)
	}
	s.mu.Unlock()
	if !ok || !snapshot.previewReady || snapshot.turnID != request.TurnID || snapshot.plan.Digest != preview.observationDigest || snapshot.plan.Body.Effect != "observation" {
		return ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("preview result does not match the prepared observation")
	}
	if snapshot.entry.classification.Effect == "observation" || preview.effect != snapshot.entry.classification.Effect {
		return ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("preview is not bound to a mutation-capable effect")
	}
	if err := validatePlanRequest(request); err != nil {
		return ActionHandle{}, protocol.ActionPlan{}, err
	}
	requestDigest, err := canonicalDigest(request)
	if err != nil {
		return ActionHandle{}, protocol.ActionPlan{}, err
	}
	observedRequest := clonePlanRequest(request)
	observedRequest.ActivityID = snapshot.request.ActivityID
	observedRequestDigest, err := canonicalDigest(observedRequest)
	if err != nil {
		return ActionHandle{}, protocol.ActionPlan{}, err
	}
	if observedRequestDigest != snapshot.requestDigest || preview.requestDigest != snapshot.requestDigest || snapshot.request.Alias != request.Alias {
		return ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("mutation request does not match the observed preview input")
	}
	if preview.preparedDigest != snapshot.previewPrepared || !reflect.DeepEqual(preview.evidenceDigests, snapshot.previewEvidence) {
		return ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("preview state or evidence binding changed")
	}
	for _, digest := range preview.evidenceDigests {
		if err := digest.Validate(); err != nil {
			return ActionHandle{}, protocol.ActionPlan{}, fmt.Errorf("preview evidence digest: %w", err)
		}
	}
	snapshot.operationMu.Lock()
	defer snapshot.operationMu.Unlock()
	prepared := snapshot.prepared.Preview()
	preparedDigest, err := canonicalDigest(prepared)
	if err != nil {
		return ActionHandle{}, protocol.ActionPlan{}, err
	}
	if preparedDigest != snapshot.previewPrepared {
		return ActionHandle{}, protocol.ActionPlan{}, authorization.ErrStaleDecision
	}
	s.mu.Lock()
	stored, exists := s.actions[preview.handleID]
	if !exists || stored.previewConsumed || !stored.previewReady || stored.previewPrepared != snapshot.previewPrepared || !reflect.DeepEqual(stored.previewEvidence, snapshot.previewEvidence) {
		s.mu.Unlock()
		return ActionHandle{}, protocol.ActionPlan{}, authorization.ErrStaleDecision
	}
	stored.previewConsumed = true
	s.mu.Unlock()
	plan, err := buildActionPlan(request, snapshot.entry, prepared, "")
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
	s.actions[id] = &plannedAction{turnID: request.TurnID, activityID: request.ActivityID, request: clonePlanRequest(request), entry: snapshot.entry, prepared: snapshot.prepared, plan: plan, requestDigest: requestDigest, dispatchDigest: dispatchDigest, evidenceDigests: append([]protocol.Digest(nil), preview.evidenceDigests...), operationMu: snapshot.operationMu}
	s.mu.Unlock()
	return ActionHandle{id: id}, plan, nil
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
		if _, previewCapable := prepared.(ports.PreviewPreparer); !previewCapable {
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
		cancel()
		<-resultChannel
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
		result   domain.ToolResult
		prepared domain.PreparedToolRequest
		err      error
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
				output <- previewOutput{result: domain.ToolResult{CallID: action.request.CallID, Status: domain.ToolSucceeded, Content: preview.ProposedDiff}, prepared: preview}
				return
			}
			output <- previewOutput{result: action.prepared.Execute(runContext), prepared: action.prepared.Preview()}
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
	preparedDigest, err := canonicalDigest(completed.prepared)
	if err != nil {
		return PreviewResult{}, protocol.ActionPlan{}, nil, err
	}
	evidenceDigests := []protocol.Digest{evidenceDigest}
	s.mu.Lock()
	stored, exists := s.actions[handle.id]
	if !exists || stored.previewReady {
		s.mu.Unlock()
		return PreviewResult{}, protocol.ActionPlan{}, nil, authorization.ErrStaleDecision
	}
	stored.previewReady = true
	stored.previewPrepared = preparedDigest
	stored.previewEvidence = append([]protocol.Digest(nil), evidenceDigests...)
	s.mu.Unlock()
	return PreviewResult{
		handleID: handle.id, observationDigest: action.plan.Digest, evidenceDigests: evidenceDigests,
		requestDigest: action.requestDigest, preparedDigest: preparedDigest, effect: action.entry.classification.Effect,
	}, action.plan, []protocol.EvidenceCandidate{evidence}, nil
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
	status := string(result.Status)
	if result.FileChange != nil && result.Status != domain.ToolSucceeded {
		status = "uncertain"
	}
	execution := protocol.ExecutionResult{Outcome: protocol.ActivityOutcomeV1{Status: status, Reason: string(result.ErrorKind)}, ToolResult: protocol.ToolResultBlock{CallID: result.CallID, Status: status, Text: result.Content}, Presentation: protocol.ToolResultPresentation{Content: result.Content, DurationNanos: int64(result.Duration), Truncated: result.Truncated}}
	if result.FileChange != nil {
		before := result.FileChange.BeforeSHA256
		if before == "" {
			before = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
		}
		execution.FileChange = &protocol.FileChangedV1{
			CallID: result.FileChange.CallID, Subject: protocol.SubjectRef{Kind: "file", ID: result.FileChange.Path},
			Before: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: before}, After: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: result.FileChange.AfterSHA256}, EvidenceIDs: []protocol.EvidenceID{},
		}
	}
	return execution
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
