package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/muratmirgun/yordam/internal/activity"
	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/compaction"
	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/orchestrator"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/projection"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/provider"
	"github.com/muratmirgun/yordam/internal/provider/openaicompat"
	"github.com/muratmirgun/yordam/internal/recovery"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
	taskprojection "github.com/muratmirgun/yordam/internal/task"
	"github.com/muratmirgun/yordam/internal/tooling"
)

func runtimeModelDescriptors(cfg config.Config, generation protocol.RuntimeGenerationID) []protocol.ModelDescriptor {
	selections := cfg.Models()
	slices.SortFunc(selections, func(left, right domain.ModelSelection) int {
		if left.Profile != right.Profile {
			if left.Profile < right.Profile {
				return -1
			}
			return 1
		}
		if left.Model < right.Model {
			return -1
		}
		if left.Model > right.Model {
			return 1
		}
		return 0
	})
	result := make([]protocol.ModelDescriptor, 0, len(selections))
	for _, selection := range selections {
		profile := cfg.Profiles[selection.Profile]
		window := protocol.ValueInt64{State: protocol.ValueUnknown}
		if value := profile.ModelContextWindows[selection.Model]; value > 0 {
			window = protocol.ValueInt64{State: protocol.ValueKnown, Value: value, Provenance: "configured_claim"}
		}
		result = append(result, protocol.ModelDescriptor{
			ProviderID: protocol.ProviderID(selection.Profile), ModelID: protocol.ModelID(selection.Model),
			AdapterKind: openaicompat.AdapterKind, DisplayName: selection.Model,
			ContextWindow: window, MaximumOutput: protocol.ValueInt64{State: protocol.ValueUnknown},
			Capabilities: []protocol.CapabilityFact{
				{Capability: protocol.CapabilityStreaming, State: protocol.CapabilitySupported, Provenance: "openai-compatible-adapter", RuntimeGenerationID: generation},
				{Capability: protocol.CapabilityTextInput, State: protocol.CapabilitySupported, Provenance: "openai-compatible-adapter", RuntimeGenerationID: generation},
				{Capability: protocol.CapabilityTextOutput, State: protocol.CapabilitySupported, Provenance: "openai-compatible-adapter", RuntimeGenerationID: generation},
				{Capability: protocol.CapabilityToolUse, State: protocol.CapabilitySupported, Provenance: "openai-compatible-adapter", RuntimeGenerationID: generation},
			},
			UsageCategories: []string{}, Pricing: []protocol.PricingFact{},
			CredentialBindingRef: profile.APIKeyEnv,
			SourceRevision:       "configured-v1",
			RuntimeGenerationID:  generation,
		})
	}
	return result
}

func runtimeCompactReserve(cfg config.Config) protocol.ValueInt64 {
	if cfg.Context.CompactReserveTokens == nil {
		return protocol.ValueInt64{State: protocol.ValueUnknown}
	}
	return protocol.ValueInt64{State: protocol.ValueKnown, Value: *cfg.Context.CompactReserveTokens, Provenance: "configured"}
}

func runtimeCompactionMetadata(sessionID protocol.SessionID, head protocol.CommittedCursor, generation protocol.RuntimeGenerationID) (orchestrator.CommandMetadata, error) {
	identity, err := canonicaljson.Digest(struct {
		SessionID  protocol.SessionID           `json:"session_id"`
		Head       protocol.CommittedCursor     `json:"head"`
		Trigger    compaction.Trigger           `json:"trigger"`
		Generation protocol.RuntimeGenerationID `json:"generation"`
	}{sessionID, head, compaction.TriggerManual, generation})
	if err != nil {
		return orchestrator.CommandMetadata{}, err
	}
	commandID := protocol.CommandID("compact-" + identity.Value)
	return orchestrator.CommandMetadata{CommandID: commandID, IdempotencyKey: string(commandID), RequestDigest: identity, Actor: protocol.ActorRef{ID: "legacy-user", Kind: protocol.ActorUser}}, nil
}

type routedProviderRequest struct {
	ProviderID protocol.ProviderID   `json:"provider_id"`
	Request    protocol.ModelRequest `json:"request"`
}

type openAIAdapterRouter struct {
	byProvider map[protocol.ProviderID]*openaicompat.Adapter
}

func (r *openAIAdapterRouter) Kind() string { return openaicompat.AdapterKind }

func (r *openAIAdapterRouter) Normalize(ctx context.Context, request protocol.ModelRequest) (provider.PreparedRequest, error) {
	if err := ctx.Err(); err != nil {
		return provider.PreparedRequest{}, err
	}
	if r == nil || r.byProvider[request.ProviderID] == nil {
		return provider.PreparedRequest{}, fmt.Errorf("provider %q has no OpenAI-compatible adapter", request.ProviderID)
	}
	return provider.NewPreparedRequest(r.Kind(), routedProviderRequest{ProviderID: request.ProviderID, Request: protocol.DeepCopy(request)})
}

func (r *openAIAdapterRouter) StartPrepared(ctx context.Context, prepared provider.PreparedRequest) (<-chan protocol.ModelEvent, error) {
	var routed routedProviderRequest
	if err := prepared.Decode(r.Kind(), &routed); err != nil {
		return nil, err
	}
	adapter := r.byProvider[routed.ProviderID]
	if adapter == nil {
		return nil, fmt.Errorf("provider %q has no OpenAI-compatible adapter", routed.ProviderID)
	}
	normalized, err := adapter.Normalize(ctx, routed.Request)
	if err != nil {
		return nil, err
	}
	return adapter.StartPrepared(ctx, normalized)
}

type runtimeAuthorization struct {
	Service       *authorization.Service
	Policy        *policyBinding
	Tools         *tooling.Catalog
	Workspace     domain.Workspace
	ActiveSession *sessionBinding
}

func (a *runtimeAuthorization) Decide(ctx context.Context, request protocol.AuthorizationRequest) (protocol.AuthorizationDecision, error) {
	if a == nil || a.Policy == nil || a.Tools == nil {
		return protocol.AuthorizationDecision{}, fmt.Errorf("runtime authorization is not configured")
	}
	if request.ControlOperationID != "" && request.Source.Source == "runtime" {
		nonceBytes := make([]byte, 32)
		if _, err := rand.Read(nonceBytes); err != nil {
			return protocol.AuthorizationDecision{}, err
		}
		constraints := []protocol.AuthorizationConstraint{}
		return protocol.AuthorizationDecision{
			Request: protocol.DeepCopy(request), Action: "allow",
			Scope:       protocol.CanonicalAuthorizationScope{Capability: request.Action, Source: request.Source, Resources: protocol.DeepCopy(request.Resources), Constraints: constraints},
			Constraints: constraints, Lifetime: protocol.AuthorizationLifetimeOnce, PolicySource: "runtime_control",
			PolicyGeneration: request.PolicyGeneration, Reason: "serialized runtime control operation", DecidedAt: time.Now().UTC(),
			PlanDigest: request.PlanDigest, DecisionNonce: protocol.DecisionNonce(hex.EncodeToString(nonceBytes)),
		}, nil
	}
	var descriptor protocol.ToolDescriptor
	permissionContext := ports.PermissionContext{Workspace: a.Workspace.CanonicalPath}
	if request.SessionID != "" {
		permissionContext.SessionID = string(request.SessionID)
	}
	if request.Source.Source == "provider" {
		permissionContext.ConfiguredProvider = &ports.ConfiguredProviderBinding{
			Identity: request.Source, SourceRevision: request.SourceRevision, DescriptorDigest: request.DescriptorDigest,
		}
	} else {
		for _, alias := range a.Tools.Expose().Aliases {
			candidate, ok := a.Tools.Descriptor(alias.Alias)
			if ok && candidate.Body.Identity == request.Source {
				descriptor = candidate
				break
			}
		}
	}
	return a.Policy.EvaluateAuthorization(ctx, ports.EvaluationInput{Permission: permissionContext, Request: request, Descriptor: descriptor})
}

func (a *runtimeAuthorization) ResolveInteractive(_ context.Context, request protocol.AuthorizationRequest, pending protocol.AuthorizationDecision, response protocol.ApprovalResponse) (protocol.AuthorizationDecision, error) {
	if a == nil || a.Service == nil {
		return protocol.AuthorizationDecision{}, fmt.Errorf("durable authorization service is required")
	}
	resolved, err := a.Service.ResolveInteractive(request, pending, response)
	if err != nil {
		return protocol.AuthorizationDecision{}, err
	}
	if resolved.Action == "allow" && resolved.Lifetime == protocol.AuthorizationLifetimeSession && a.Policy != nil {
		if err := a.Policy.GrantAuthorizationSession(request, resolved.Constraints); err != nil {
			return protocol.AuthorizationDecision{}, err
		}
	}
	return resolved, nil
}

func (a *runtimeAuthorization) Issue(ctx context.Context, reference authorization.CommitReference) (authorization.CommittedToken, error) {
	return a.Service.Issue(ctx, reference)
}

func (a *runtimeAuthorization) Dispatch(ctx context.Context, token authorization.CommittedToken, binding authorization.DispatchBinding, run func(context.Context) error) error {
	return a.Service.Dispatch(ctx, token, binding, run)
}

type interactiveApproverBinding struct {
	mu       sync.RWMutex
	approver ports.PermissionApprover
}

func (b *interactiveApproverBinding) set(approver ports.PermissionApprover) {
	b.mu.Lock()
	b.approver = approver
	b.mu.Unlock()
}

func (b *interactiveApproverBinding) Approve(ctx context.Context, pending protocol.AuthorizationDecision) (protocol.ApprovalResponse, error) {
	b.mu.RLock()
	approver := b.approver
	b.mu.RUnlock()
	if approver == nil {
		return protocol.ApprovalResponse{}, fmt.Errorf("interactive approver is not bound")
	}
	scope := "runtime"
	if len(pending.Scope.Resources) != 0 {
		scope = pending.Scope.Resources[0].CanonicalID
	}
	mutation := domain.MutationFile
	if pending.Request.Effect == "observation" {
		mutation = domain.MutationReadOnly
	} else if pending.Request.ExecutionLocus == "process" {
		mutation = domain.MutationProcess
	}
	decision, err := approver.Resolve(ctx, ports.PermissionPrompt{
		SessionID: string(pending.Request.SessionID),
		Call: domain.PreparedToolRequest{
			Request:  domain.ToolRequest{CallID: pending.Request.CallID, Name: pending.Request.Source.Name, Input: json.RawMessage(`{}`)},
			Mutation: mutation, CanonicalScope: scope, InsideWorkspace: pending.Request.Boundary == "workspace", Summary: pending.Reason,
		},
	})
	if err != nil {
		return protocol.ApprovalResponse{}, err
	}
	scopeDigest, err := canonicaljson.Digest(pending.Scope)
	if err != nil {
		return protocol.ApprovalResponse{}, err
	}
	lifetime := protocol.AuthorizationLifetimeOnce
	if decision.Lifetime == domain.PermissionSession {
		lifetime = protocol.AuthorizationLifetimeSession
	}
	return protocol.ApprovalResponse{
		RequestID: pending.Request.RequestID, Action: string(decision.Action), Lifetime: lifetime, ScopeDigest: scopeDigest,
		Actor: protocol.ActorRef{ID: "interactive-user", Kind: protocol.ActorUser}, Reason: decision.Reason,
	}, nil
}

type generationAdmission struct{ Registry *secret.Registry }

func (a generationAdmission) SanitizeText(_ context.Context, generation protocol.RuntimeGenerationID, value string) (string, error) {
	lease, err := a.Registry.AcquireExisting(generation)
	if err != nil {
		return "", err
	}
	defer lease.Close()
	return lease.String(value), nil
}

func (a generationAdmission) SanitizeJSON(_ context.Context, generation protocol.RuntimeGenerationID, value json.RawMessage) (json.RawMessage, error) {
	if !json.Valid(value) {
		return nil, fmt.Errorf("sanitize invalid JSON")
	}
	lease, err := a.Registry.AcquireExisting(generation)
	if err != nil {
		return nil, err
	}
	defer lease.Close()
	redacted := json.RawMessage(lease.Bytes(value))
	if !json.Valid(redacted) {
		return nil, fmt.Errorf("sanitized JSON is invalid")
	}
	if err := lease.Admit(redacted); err != nil {
		return nil, err
	}
	return protocol.CloneRawMessage(redacted), nil
}

func (a generationAdmission) OpenTextStream(_ context.Context, generation protocol.RuntimeGenerationID) (orchestrator.StreamingSanitizer, error) {
	lease, err := a.Registry.AcquireExisting(generation)
	if err != nil {
		return nil, err
	}
	stream, err := lease.RedactionStream()
	if err != nil {
		_ = lease.Close()
		return nil, err
	}
	return &generationStream{lease: lease, stream: stream}, nil
}

type generationStream struct {
	mu     sync.Mutex
	lease  *secret.Lease
	stream *secret.LeasedRedactionStream
	closed bool
}

func (s *generationStream) Write(value string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", fmt.Errorf("generation stream is closed")
	}
	return s.stream.Write(value), nil
}

func (s *generationStream) Close() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", nil
	}
	s.closed = true
	value := s.stream.Close()
	return value, s.lease.Close()
}

type staticInstructions struct{}

func (staticInstructions) SystemInstructions(_ context.Context, generation protocol.RuntimeGenerationID, revision string, sessionID protocol.SessionID) ([]protocol.ContentSource, error) {
	if generation == "" || revision == "" || sessionID == "" {
		return nil, fmt.Errorf("instruction binding is incomplete")
	}
	blocks := []protocol.ContentBlock{{Kind: protocol.ContentText, Text: systemPrompt}}
	digest, err := canonicaljson.Digest(blocks)
	if err != nil {
		return nil, err
	}
	return []protocol.ContentSource{{ID: revision, Kind: "system_instruction", Scope: "generation", Provenance: revision, Digest: digest, Content: blocks}}, nil
}

type recoveryRecorder struct {
	Tools *tooling.Service
	Store recovery.RecoveryStore
}

func (r recoveryRecorder) PrepareAndPut(ctx context.Context, preview tooling.PreviewResult, activityID protocol.ActivityID, checkpoint protocol.CheckpointBody, plan protocol.ActionPlan) (protocol.RecoveryMaterialRecord, error) {
	return r.Tools.PrepareAndPutRecovery(ctx, preview, activityID, checkpoint, plan, r.Store)
}

type recoveryProjection struct{ Repository journal.Repository }

func (p recoveryProjection) InspectRecovery(ctx context.Context, ref protocol.JournalRef, expected protocol.CommittedCursor) (orchestrator.RecoveryProjection, error) {
	inspection, err := p.Repository.Inspect(ctx, ref)
	if err != nil {
		return orchestrator.RecoveryProjection{}, err
	}
	if inspection.Head != expected {
		return orchestrator.RecoveryProjection{}, fmt.Errorf("recovery inspection head changed")
	}
	result := orchestrator.RecoveryProjection{UnmatchedNoEffect: make(map[protocol.ActivityID]bool)}
	started := make(map[protocol.ActivityID]bool)
	for _, record := range inspection.Events {
		event := record.Envelope
		switch event.Kind {
		case protocol.EventCommandAccepted:
			var payload protocol.CommandAcceptedV1
			if json.Unmarshal(event.Payload, &payload) == nil {
				result.OriginalCommandID, result.OriginalRequestDigest = payload.CommandID, payload.RequestDigest
			}
		case protocol.EventTaskCreated:
			result.TaskID = event.TaskID
		case protocol.EventTurnAccepted:
			result.ActiveTurnID, result.TaskID = event.TurnID, event.TaskID
		case protocol.EventTurnCompleted, protocol.EventTurnFailed, protocol.EventTurnInterrupted:
			if event.TurnID == result.ActiveTurnID {
				result.ActiveTurnID = ""
			}
		case protocol.EventActivityStarted:
			started[event.ActivityID] = true
		case protocol.EventActivitySucceeded, protocol.EventActivityFailed, protocol.EventActivityDenied, protocol.EventActivityCancelled, protocol.EventActivityUncertain:
			delete(started, event.ActivityID)
		case protocol.EventActivityInterruptedNoEffect:
			delete(started, event.ActivityID)
			result.UnmatchedNoEffect[event.ActivityID] = true
		}
	}
	for activityID := range started {
		result.StartedActivities = append(result.StartedActivities, activityID)
	}
	slices.Sort(result.StartedActivities)
	return result, nil
}

type runtimeBrokerSource struct {
	Repository journal.Repository
	Workspace  protocol.JournalRef
	Generation protocol.RuntimeGenerationID
	Manifest   protocol.RuntimeGenerationManifest
}

func (s runtimeBrokerSource) WorkspaceControl() protocol.JournalRef { return s.Workspace }

func (s runtimeBrokerSource) Session(id protocol.SessionID) protocol.JournalRef {
	return protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(id)}
}

func (s runtimeBrokerSource) Head(ctx context.Context, ref protocol.JournalRef) (protocol.CommittedCursor, error) {
	return s.Repository.Head(ctx, ref)
}

func (s runtimeBrokerSource) ReadRange(ctx context.Context, request journal.ReadRangeRequest) (journal.EventPage, error) {
	return s.Repository.ReadRange(ctx, request)
}

func (s runtimeBrokerSource) Project(ctx context.Context, vector SnapshotVector) (protocol.DurableProjection, protocol.RuntimeProjection, error) {
	workspaceRef := s.Workspace
	workspaceAuthorization, err := projection.New[authorization.Projection](s.Repository, authorization.Projector{}, nil).At(ctx, workspaceRef, vector.WorkspaceControl)
	if err != nil {
		return protocol.DurableProjection{}, protocol.RuntimeProjection{}, fmt.Errorf("project workspace control at cursor: %w", err)
	}
	workspaceData, err := canonicaljson.Marshal(workspaceAuthorization.State)
	if err != nil {
		return protocol.DurableProjection{}, protocol.RuntimeProjection{}, err
	}
	unknown := func(id, kind string) protocol.ProjectionView {
		return protocol.ProjectionView{ID: id, Kind: kind, Status: "unknown", State: protocol.ValueUnknown, Data: json.RawMessage(`{}`)}
	}
	known := func(id, kind, status string, data json.RawMessage) protocol.ProjectionView {
		return protocol.ProjectionView{ID: id, Kind: kind, Status: status, State: protocol.ValueKnown, Data: protocol.CloneRawMessage(data)}
	}
	generations, generationErr := projection.New[map[protocol.RuntimeGenerationID]protocol.RuntimeGenerationManifest](s.Repository, generationCatalogProjector{}, nil).At(ctx, workspaceRef, vector.WorkspaceControl)
	if generationErr != nil {
		return protocol.DurableProjection{}, protocol.RuntimeProjection{}, fmt.Errorf("project runtime generations at cursor: %w", generationErr)
	}
	durable := protocol.DurableProjection{
		Workspace:  known(string(workspaceRef.ID), "workspace", "ready", workspaceData),
		Activities: []protocol.ProjectionView{}, Provider: unknown("provider", "provider"), MCP: []protocol.ProjectionView{},
		Instructions: []protocol.ProjectionView{}, Permissions: known("workspace-permissions", "permissions", "ready", workspaceData), Context: unknown("context", "context"),
		Usage: protocol.ModelUsage{
			Input: protocol.UsageValue{State: protocol.UsageUnknown}, Output: protocol.UsageValue{State: protocol.UsageUnknown},
			Cached: protocol.UsageValue{State: protocol.UsageUnknown}, CacheWrite: protocol.UsageValue{State: protocol.UsageUnknown}, Reasoning: protocol.UsageValue{State: protocol.UsageUnknown},
		},
		Cost: protocol.CostValue{State: protocol.ValueUnknown}, Checkpoints: []protocol.ProjectionView{}, Evidence: []protocol.ProjectionView{}, Receipts: []protocol.ProjectionView{}, RecoveryDiagnostics: []protocol.Diagnostic{},
	}
	if vector.SelectedSession != nil {
		ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: vector.SelectedSession.JournalID}
		tasks, projectErr := projection.New[taskprojection.Projection](s.Repository, taskprojection.Projector{}, nil).At(ctx, ref, *vector.SelectedSession)
		if projectErr != nil {
			return protocol.DurableProjection{}, protocol.RuntimeProjection{}, fmt.Errorf("project tasks at cursor: %w", projectErr)
		}
		activities, projectErr := projection.New[activity.Projection](s.Repository, activity.Projector{}, nil).At(ctx, ref, *vector.SelectedSession)
		if projectErr != nil {
			return protocol.DurableProjection{}, protocol.RuntimeProjection{}, fmt.Errorf("project activities at cursor: %w", projectErr)
		}
		permissions, projectErr := projection.New[authorization.Projection](s.Repository, authorization.Projector{}, nil).At(ctx, ref, *vector.SelectedSession)
		if projectErr != nil {
			return protocol.DurableProjection{}, protocol.RuntimeProjection{}, fmt.Errorf("project permissions at cursor: %w", projectErr)
		}
		taskData, marshalErr := canonicaljson.Marshal(tasks.State)
		if marshalErr != nil {
			return protocol.DurableProjection{}, protocol.RuntimeProjection{}, marshalErr
		}
		selected := known(string(ref.ID), "session", "ready", taskData)
		durable.SelectedSession = &selected
		permissionData, marshalErr := canonicaljson.Marshal(permissions.State)
		if marshalErr != nil {
			return protocol.DurableProjection{}, protocol.RuntimeProjection{}, marshalErr
		}
		durable.Permissions = known(string(ref.ID), "permissions", "ready", permissionData)
		taskIDs := make([]protocol.TaskID, 0, len(tasks.State.Tasks))
		for taskID := range tasks.State.Tasks {
			taskIDs = append(taskIDs, taskID)
		}
		slices.Sort(taskIDs)
		if len(taskIDs) != 0 {
			record := tasks.State.Tasks[taskIDs[len(taskIDs)-1]]
			recordData, marshalErr := canonicaljson.Marshal(record)
			if marshalErr != nil {
				return protocol.DurableProjection{}, protocol.RuntimeProjection{}, marshalErr
			}
			taskView := known(string(record.TaskID), "task", string(record.State), recordData)
			durable.Task = &taskView
			outcomeData, marshalErr := canonicaljson.Marshal(record.Outcome)
			if marshalErr != nil {
				return protocol.DurableProjection{}, protocol.RuntimeProjection{}, marshalErr
			}
			outcomeView := known(string(record.OutcomeContractID), "outcome", record.Outcome.Status, outcomeData)
			durable.Outcome = &outcomeView
		}
		for _, activityID := range activities.State.Order {
			record := activities.State.Activities[activityID]
			recordData, marshalErr := canonicaljson.Marshal(record)
			if marshalErr != nil {
				return protocol.DurableProjection{}, protocol.RuntimeProjection{}, marshalErr
			}
			durable.Activities = append(durable.Activities, known(string(activityID), record.Kind, string(record.State), recordData))
		}
		evidenceIDs := make([]protocol.EvidenceID, 0, len(tasks.State.EvidenceAvailability))
		for evidenceID := range tasks.State.EvidenceAvailability {
			evidenceIDs = append(evidenceIDs, evidenceID)
		}
		slices.Sort(evidenceIDs)
		for _, evidenceID := range evidenceIDs {
			availability := tasks.State.EvidenceAvailability[evidenceID]
			data, marshalErr := canonicaljson.Marshal(availability)
			if marshalErr != nil {
				return protocol.DurableProjection{}, protocol.RuntimeProjection{}, marshalErr
			}
			durable.Evidence = append(durable.Evidence, known(string(evidenceID), "evidence", string(availability), data))
		}
		durable.RecoveryDiagnostics = protocol.DeepCopy(tasks.State.Diagnostics)
		contextState, contextErr := projection.New[protocol.ContextProjectionV1](s.Repository, contextProjectionProjector{generations: generations.State, bootstrap: s.Manifest}, nil).At(ctx, ref, *vector.SelectedSession)
		if contextErr != nil {
			return protocol.DurableProjection{}, protocol.RuntimeProjection{}, fmt.Errorf("project context at cursor: %w", contextErr)
		}
		contextData, marshalErr := canonicaljson.Marshal(contextState.State)
		if marshalErr != nil {
			return protocol.DurableProjection{}, protocol.RuntimeProjection{}, marshalErr
		}
		durable.Context = known(string(ref.ID), "context", "ready", contextData)
	}
	runtime := protocol.RuntimeProjection{RuntimeGenerationID: s.Generation, ActiveStreams: []protocol.ProjectionView{}, Connections: []protocol.ProjectionView{}, RevocationEpoch: workspaceAuthorization.State.RevocationEpoch}
	return durable, runtime, nil
}

type generationCatalogProjector struct{}

func (generationCatalogProjector) Version() uint32 { return 1 }
func (generationCatalogProjector) Zero(protocol.JournalRef) map[protocol.RuntimeGenerationID]protocol.RuntimeGenerationManifest {
	return map[protocol.RuntimeGenerationID]protocol.RuntimeGenerationManifest{}
}
func (generationCatalogProjector) Apply(state map[protocol.RuntimeGenerationID]protocol.RuntimeGenerationManifest, record protocol.EventRecord) (map[protocol.RuntimeGenerationID]protocol.RuntimeGenerationManifest, error) {
	if value, ok := record.Decoded.(*protocol.RuntimeGenerationActivatedV1); ok {
		state[value.Manifest.ID] = protocol.DeepCopy(value.Manifest)
	}
	return state, nil
}

type contextProjectionProjector struct {
	generations map[protocol.RuntimeGenerationID]protocol.RuntimeGenerationManifest
	bootstrap   protocol.RuntimeGenerationManifest
}

func (contextProjectionProjector) Version() uint32 { return 1 }
func (contextProjectionProjector) Zero(protocol.JournalRef) protocol.ContextProjectionV1 {
	return protocol.ContextProjectionV1{AutoReason: "unknown_context_window", EstimatedInputTokens: protocol.ValueInt64{State: protocol.ValueUnknown}, ContextWindow: protocol.ValueInt64{State: protocol.ValueUnknown}, ReserveTokens: protocol.ValueInt64{State: protocol.ValueUnknown}}
}
func (p contextProjectionProjector) Apply(state protocol.ContextProjectionV1, record protocol.EventRecord) (protocol.ContextProjectionV1, error) {
	switch value := record.Decoded.(type) {
	case *protocol.ContextPlanRecordedV1:
		state.EstimatedInputTokens, state.ContextWindow, state.OutputReserve, state.Revision = value.Plan.Body.EstimatedInputTokens, value.Plan.Body.ContextWindow, value.Plan.Body.OutputReserve, value.Plan.Body.CompactionRevision
		manifest, ok := p.generations[record.Envelope.RuntimeGenerationID]
		if !ok && p.bootstrap.ID != "" && p.bootstrap.ID == record.Envelope.RuntimeGenerationID {
			manifest, ok = p.bootstrap, true
		}
		if !ok {
			state.AutoAvailable, state.AutoReason, state.ReserveTokens = false, "unknown_generation", protocol.ValueInt64{State: protocol.ValueUnknown}
			return state, nil
		}
		policy := compaction.Policy{AutoCompact: manifest.Body.Limits.AutoCompact}
		if manifest.Body.Limits.CompactReserveTokens.State == protocol.ValueKnown {
			reserve := manifest.Body.Limits.CompactReserveTokens.Value
			policy.CompactReserveTokens = &reserve
		}
		decision, err := compaction.Evaluate(value.Plan.Body.EstimatedInputTokens.Value, value.Plan.Body.OutputReserve, value.Plan.Body.ContextWindow, policy)
		state.AutoAvailable, state.AutoReason = decision.Available, decision.Reason
		if err == nil && decision.Available {
			state.ReserveTokens = protocol.ValueInt64{State: protocol.ValueKnown, Value: decision.ReserveTokens, Provenance: "compaction_policy"}
		} else {
			state.ReserveTokens = protocol.ValueInt64{State: protocol.ValueUnknown}
		}
	case *protocol.ContextCompactedV1:
		r := protocol.CompactionRange{From: value.From, Through: value.Through}
		state.LatestRange, state.Revision, state.SummaryEvidenceID = &r, value.Revision, value.SummaryEvidenceID
	}
	return state, nil
}

type runtimeCommandDispatcher struct {
	Orchestrator *orchestrator.Service
	Store        *jsonl.Store
	Manifest     protocol.RuntimeGenerationManifest
	Workspace    protocol.JournalRef
}

func (d runtimeCommandDispatcher) DispatchCommand(ctx context.Context, metadata orchestrator.CommandMetadata, command protocol.Command, decoded any) (protocol.CommandResult, error) {
	if d.Orchestrator == nil || d.Store == nil {
		return failedCommand(command, "service_unavailable", "runtime dispatcher is unavailable", true), nil
	}
	switch payload := decoded.(type) {
	case *protocol.StartTurnCommandV1:
		if command.Expected == nil || command.Expected.Session == nil || command.Expected.SelectedSessionID == "" {
			return failedCommand(command, codeInvalidCommand, "turn command requires a selected session cursor", false), nil
		}
		inspection, err := d.Store.InspectSession(ctx, command.Expected.SelectedSessionID)
		if err != nil {
			return protocol.CommandResult{}, err
		}
		state := ProjectSessionState(LegacyReplayFromInspection(inspection))
		result, err := d.Orchestrator.RunTurn(ctx, orchestrator.StartTurnRequest{
			Command: metadata, SessionID: command.Expected.SelectedSessionID, ExpectedHead: *command.Expected.Session,
			Prompt: payload.Prompt, ProviderID: protocol.ProviderID(state.Selection.Profile), ModelID: protocol.ModelID(state.Selection.Model), Runtime: protocol.DeepCopy(d.Manifest),
		})
		return result.CommandResult, err
	case *protocol.ChangeModeCommandV1:
		mode := domain.PermissionMode(payload.Mode)
		if err := mode.Validate(); err != nil {
			return failedCommand(command, codeInvalidPayload, err.Error(), false), nil
		}
		return d.runSettingControl(ctx, metadata, command, protocol.EventModeChanged, protocol.ModeChangedV1{Mode: payload.Mode}, "mode", "change permission mode")
	case *protocol.ChangeModelCommandV1:
		configured := false
		for _, model := range d.Manifest.Body.Models {
			if model.ProviderID == payload.ProviderID && model.ModelID == payload.ModelID {
				configured = true
				break
			}
		}
		if !configured {
			return failedCommand(command, codeInvalidPayload, "model selection is not part of this runtime generation", false), nil
		}
		return d.runSettingControl(ctx, metadata, command, protocol.EventModelChanged, protocol.ModelChangedV1{ProviderID: payload.ProviderID, ModelID: payload.ModelID}, "model", "change model selection")
	case *protocol.TrustedShellCommandV1:
		return d.runSettingControl(ctx, metadata, command, protocol.EventTrustedExecutionAcknowledged, protocol.TrustedExecutionAcknowledgedV1{Enabled: payload.Enabled, Profile: "unsandboxed"}, "trusted-shell", "acknowledge trusted shell execution")
	case *protocol.EmptyCommandV1:
		if command.Kind != string(CommandCompact) {
			return failedCommand(command, codeUnsupportedCommand, "command is not available through this runtime generation", false), nil
		}
		if command.Expected == nil || command.Expected.SelectedSessionID == "" || command.Expected.Session == nil {
			return failedCommand(command, codeInvalidCommand, "compact command requires a selected session cursor", false), nil
		}
		ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(command.Expected.SelectedSessionID)}
		head, err := d.Store.Head(ctx, ref)
		if err != nil {
			return protocol.CommandResult{}, err
		}
		if head != *command.Expected.Session {
			return failedCommand(command, "stale_cursor", "selected session cursor is stale", false), nil
		}
		inspection, err := d.Store.InspectSession(ctx, command.Expected.SelectedSessionID)
		if err != nil {
			return protocol.CommandResult{}, err
		}
		state := ProjectSessionState(LegacyReplayFromInspection(inspection))
		providerID, modelID := protocol.ProviderID(state.Selection.Profile), protocol.ModelID(state.Selection.Model)
		if providerID == "" || modelID == "" {
			if len(d.Manifest.Body.Models) == 0 {
				return failedCommand(command, codeInvalidCommand, "compact command has no configured model", false), nil
			}
			providerID, modelID = d.Manifest.Body.Models[0].ProviderID, d.Manifest.Body.Models[0].ModelID
		}
		_, runErr := d.Orchestrator.RunCompaction(ctx, orchestrator.CompactRequest{
			Command: metadata, SessionID: command.Expected.SelectedSessionID, ExpectedHead: *command.Expected.Session,
			ProviderID: providerID, ModelID: modelID,
			Runtime: protocol.DeepCopy(d.Manifest), Trigger: compaction.TriggerManual,
		})
		result, ok, err := d.Orchestrator.LookupCommand(ctx, ref, metadata.CommandID, metadata.RequestDigest)
		if err != nil {
			return protocol.CommandResult{}, err
		}
		if ok {
			return result, nil
		}
		if runErr != nil {
			return compactFailedCommand(command, runErr), nil
		}
		return protocol.CommandResult{}, fmt.Errorf("compaction completed without a durable command result")
	default:
		return failedCommand(command, codeUnsupportedCommand, "command is not available through this runtime generation", false), nil
	}
}

func compactFailedCommand(command protocol.Command, err error) protocol.CommandResult {
	code, message := "compaction_failed", "compaction failed"
	switch {
	case errors.Is(err, compaction.ErrNothingToCompact):
		code, message = "nothing_to_compact", "no safe context range is available to compact"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		code, message = "cancelled", "compaction was cancelled"
	case errors.Is(err, orchestrator.ErrCommitUncertain):
		code, message = "commit_uncertain", "compaction commit outcome is uncertain"
	case strings.Contains(err.Error(), "idle session"):
		code, message = "session_not_idle", "compaction requires an idle session"
	case strings.Contains(err.Error(), "selected model"):
		code, message = "invalid_model", "selected model is unavailable for compaction"
	}
	return failedCommand(command, code, message, false)
}

func (d runtimeCommandDispatcher) runSettingControl(ctx context.Context, metadata orchestrator.CommandMetadata, command protocol.Command, eventKind string, eventPayload any, action, purpose string) (protocol.CommandResult, error) {
	if command.Expected == nil || command.Expected.WorkspaceControl == nil || command.Expected.Session == nil || command.Expected.SelectedSessionID == "" {
		return failedCommand(command, codeInvalidCommand, "setting command requires workspace and selected-session cursors", false), nil
	}
	workspace := d.Workspace
	if workspace == (protocol.JournalRef{}) {
		workspace = protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: command.Expected.WorkspaceControl.JournalID}
	}
	session := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(command.Expected.SelectedSessionID)}
	identity := "setting-" + string(command.CommandID)
	descriptorDigest, err := canonicaljson.Digest(struct {
		Name string `json:"name"`
	}{"runtime.setting." + action})
	if err != nil {
		return protocol.CommandResult{}, err
	}
	body := protocol.ActionPlanBody{
		CallID: identity, Tool: protocol.ToolIdentity{Source: "runtime", Authority: "yordam", Name: "setting"}, SourceRevision: "runtime-v1",
		DescriptorDigest: descriptorDigest, Action: "runtime.setting." + action, Purpose: purpose,
		Resources:            []protocol.ResourceTarget{{Kind: "session_journal", CanonicalID: string(command.Expected.SelectedSessionID)}},
		ExecutionLocus:       "runtime",
		Effect:               "mutation",
		Boundary:             "session",
		Reversibility:        "exact",
		VerificationCoverage: "exact",
		RequestedProfile:     "restricted",
		EffectiveProfile:     "restricted",
		RuntimeGenerationID:  d.Manifest.ID,
	}
	planDigest, err := canonicaljson.Digest(body)
	if err != nil {
		return protocol.CommandResult{}, err
	}
	payload, err := canonicaljson.Marshal(eventPayload)
	if err != nil {
		return protocol.CommandResult{}, err
	}
	actor := protocol.DeepCopy(metadata.Actor)
	result, err := d.Orchestrator.RunControl(ctx, orchestrator.ControlRequest{
		Command: metadata, OperationID: protocol.ControlOperationID(identity), Kind: orchestrator.OperationControl,
		Journal: workspace, ExpectedHead: *command.Expected.WorkspaceControl, TransactionID: protocol.TransactionID(identity + "-terminal"),
		ConsequentialJournal: session, ConsequentialExpectedHead: *command.Expected.Session,
		Runtime: protocol.DeepCopy(d.Manifest), Plan: protocol.ActionPlan{Body: body, Digest: planDigest},
		Event: protocol.ProposedEvent{
			EventID: protocol.EventID(identity + "-consequence"), Time: time.Now().UTC(), PayloadVersion: 1, Kind: eventKind,
			SessionID: command.Expected.SelectedSessionID, Actor: &actor, RuntimeGenerationID: d.Manifest.ID, Payload: payload,
		},
	})
	return result.CommandResult, err
}

func runtimeCommandExpectation(store *jsonl.Store, workspace protocol.JournalRef, active *sessionBinding) func() *protocol.CommandExpectation {
	return func() *protocol.CommandExpectation {
		workspaceHead, err := store.Head(context.Background(), workspace)
		if err != nil {
			return nil
		}
		expectation := &protocol.CommandExpectation{WorkspaceControl: &workspaceHead}
		if active == nil || active.get() == "" {
			return expectation
		}
		sessionID := protocol.SessionID(active.get())
		sessionHead, err := store.Head(context.Background(), protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(sessionID)})
		if err != nil {
			return nil
		}
		expectation.SelectedSessionID = sessionID
		expectation.Session = &sessionHead
		return expectation
	}
}

type runtimeLifecycle struct {
	mu      sync.Mutex
	active  int
	retired bool
	close   func()
	once    sync.Once
}

func newRuntimeLifecycle(close func()) *runtimeLifecycle { return &runtimeLifecycle{close: close} }

func (l *runtimeLifecycle) acquire() (func(), error) {
	l.mu.Lock()
	if l.retired {
		l.mu.Unlock()
		return nil, fmt.Errorf("runtime generation is retired")
	}
	l.active++
	l.mu.Unlock()
	var once sync.Once
	return func() { once.Do(l.release) }, nil
}

func (l *runtimeLifecycle) release() {
	l.mu.Lock()
	l.active--
	closeNow := l.retired && l.active == 0
	l.mu.Unlock()
	if closeNow {
		l.once.Do(l.close)
	}
}

func (l *runtimeLifecycle) retire() {
	l.mu.Lock()
	l.retired = true
	closeNow := l.active == 0
	l.mu.Unlock()
	if closeNow {
		l.once.Do(l.close)
	}
}
