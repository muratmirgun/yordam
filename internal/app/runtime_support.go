package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/orchestrator"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/provider"
	"github.com/muratmirgun/yordam/internal/provider/openaicompat"
	"github.com/muratmirgun/yordam/internal/recovery"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
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
		result = append(result, protocol.ModelDescriptor{
			ProviderID: protocol.ProviderID(selection.Profile), ModelID: protocol.ModelID(selection.Model),
			AdapterKind: openaicompat.AdapterKind, DisplayName: selection.Model,
			ContextWindow: protocol.ValueInt64{State: protocol.ValueUnknown}, MaximumOutput: protocol.ValueInt64{State: protocol.ValueUnknown},
			Capabilities: []protocol.CapabilityFact{
				{Capability: protocol.CapabilityStreaming, State: protocol.CapabilitySupported, Provenance: "openai-compatible-adapter", RuntimeGenerationID: generation},
				{Capability: protocol.CapabilityTextInput, State: protocol.CapabilitySupported, Provenance: "openai-compatible-adapter", RuntimeGenerationID: generation},
				{Capability: protocol.CapabilityTextOutput, State: protocol.CapabilitySupported, Provenance: "openai-compatible-adapter", RuntimeGenerationID: generation},
				{Capability: protocol.CapabilityToolUse, State: protocol.CapabilitySupported, Provenance: "openai-compatible-adapter", RuntimeGenerationID: generation},
			},
			UsageCategories: []string{}, Pricing: []protocol.PricingFact{},
			CredentialBindingRef: cfg.Profiles[selection.Profile].APIKeyEnv,
			SourceRevision:       "configured-v1",
			RuntimeGenerationID:  generation,
		})
	}
	return result
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

func (s runtimeBrokerSource) Project(_ context.Context, vector SnapshotVector) (protocol.DurableProjection, protocol.RuntimeProjection, error) {
	data := json.RawMessage(`{}`)
	view := func(id, kind string) protocol.ProjectionView {
		return protocol.ProjectionView{ID: id, Kind: kind, Status: "ready", State: protocol.ValueKnown, Data: protocol.CloneRawMessage(data)}
	}
	durable := protocol.DurableProjection{
		Workspace:  view(string(vector.WorkspaceControl.JournalID), "workspace"),
		Activities: []protocol.ProjectionView{}, Provider: view("provider", "provider"), MCP: []protocol.ProjectionView{},
		Instructions: []protocol.ProjectionView{}, Permissions: view("permissions", "permissions"), Context: view("context", "context"),
		Usage: protocol.ModelUsage{
			Input: protocol.UsageValue{State: protocol.UsageUnknown}, Output: protocol.UsageValue{State: protocol.UsageUnknown},
			Cached: protocol.UsageValue{State: protocol.UsageUnknown}, CacheWrite: protocol.UsageValue{State: protocol.UsageUnknown}, Reasoning: protocol.UsageValue{State: protocol.UsageUnknown},
		},
		Cost: protocol.CostValue{State: protocol.ValueUnknown}, Checkpoints: []protocol.ProjectionView{}, Evidence: []protocol.ProjectionView{}, Receipts: []protocol.ProjectionView{}, RecoveryDiagnostics: []protocol.Diagnostic{},
	}
	if vector.SelectedSession != nil {
		selected := view(string(vector.SelectedSession.JournalID), "session")
		durable.SelectedSession = &selected
	}
	runtime := protocol.RuntimeProjection{RuntimeGenerationID: s.Generation, ActiveStreams: []protocol.ProjectionView{}, Connections: []protocol.ProjectionView{}}
	return durable, runtime, nil
}

type runtimeCommandDispatcher struct {
	Orchestrator *orchestrator.Service
	Store        *jsonl.Store
	Manifest     protocol.RuntimeGenerationManifest
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
	default:
		return failedCommand(command, codeUnsupportedCommand, "command is not available through this runtime generation", false), nil
	}
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
