package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/cli"
	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/logging"
	"github.com/muratmirgun/yordam/internal/orchestrator"
	"github.com/muratmirgun/yordam/internal/permission"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
)

const systemPrompt = "You are Yordam, a terminal agent working in the user's current workspace. Be accurate, concise, and explicit about tool effects."

type BootstrapOptions struct {
	ConfigPath string
	CLI        cli.Options
	CWD        string
	HTTPClient *http.Client
}

type Snapshot struct {
	Workspace          domain.Workspace
	Session            domain.Session
	Replay             domain.SessionReplay
	Sessions           []domain.SessionSummary
	Models             []domain.ModelSelection
	ConfigurationError error
	Context            *protocol.ContextProjectionV1
	Skills             SkillSnapshot
}

func Bootstrap(ctx context.Context, options BootstrapOptions) (_ *App, _ Snapshot, err error) {
	workspace, err := resolveWorkspace(options.CWD)
	if err != nil {
		return nil, Snapshot{}, err
	}
	dataDir, err := resolveDataDir(options.CLI.DataDir)
	if err != nil {
		return nil, Snapshot{}, err
	}

	secretRegistry := secret.NewRegistry()
	bootstrapGeneration := protocol.RuntimeGenerationID(fmt.Sprintf("bootstrap-%d", runtimeSecretGeneration.Add(1)))
	bootstrapAdmission, err := secretRegistry.Acquire(bootstrapGeneration, nil)
	if err != nil {
		return nil, Snapshot{}, fmt.Errorf("bind bootstrap admission: %w", err)
	}
	redactors := secret.NewBinding(bootstrapAdmission)
	bootstrapRetireOnce := &sync.Once{}
	bootstrapRetire := func() {
		bootstrapRetireOnce.Do(func() {
			_ = secretRegistry.Retire(bootstrapGeneration)
			_ = bootstrapAdmission.Close()
		})
	}
	bootstrapHandedOff := false
	defer func() {
		if err != nil && !bootstrapHandedOff {
			bootstrapRetire()
		}
	}()
	logger, closeDebugLog, err := openDebugLogger(options.CLI.DebugLog, redactors)
	if err != nil {
		return nil, Snapshot{}, err
	}
	debugLogOwned := closeDebugLog != nil
	defer func() {
		if err != nil && debugLogOwned {
			_ = closeDebugLog()
		}
	}()
	store, err := jsonl.NewAdmitted(dataDir, jsonl.Options{Sanitize: redactors.JSON, Secrets: secretRegistry, Admission: redactors})
	if err != nil {
		return nil, Snapshot{}, err
	}
	workspaceControl, err := store.EnsureWorkspaceControl(ctx, workspace)
	if err != nil {
		return nil, Snapshot{}, fmt.Errorf("initialize workspace-control journal: %w", err)
	}
	cfg, configPath, configErr := loadBootstrapConfig(options)
	initialSelection := domain.ModelSelection{}
	if configErr == nil && !options.CLI.Continue && options.CLI.Session == "" {
		resolved, resolveErr := cfg.Resolve(config.ResolveOptions{
			Profile: options.CLI.Profile,
			Model:   options.CLI.Model,
			BaseURL: options.CLI.BaseURL,
		})
		if resolveErr != nil {
			configErr = configurationError(configPath, fmt.Sprintf("configuration is invalid (%v); edit the file and run /reload", resolveErr), resolveErr)
		} else {
			initialSelection = domain.ModelSelection{Profile: resolved.Name, Model: resolved.Model}
		}
	}
	session, replay, err := selectSession(ctx, store, workspace, options.CLI, initialSelection)
	if err != nil {
		return nil, Snapshot{}, err
	}
	state := ProjectSessionState(replay)
	session.Mode = state.Mode
	session.Selection = state.Selection
	runtimeEvents := make(chan agent.RuntimeEvent, 64)
	activeSession := &sessionBinding{id: session.ID}
	policy := newPolicyBinding(permission.Restore(replay))
	policies := permission.NewSessionPolicyRegistry()
	if err := policies.RegisterParent(protocol.SessionID(session.ID), policy.current); err != nil {
		return nil, Snapshot{}, err
	}
	policy.registry = policies
	builder := runtimeBuilder{
		configPath:       configPath,
		cli:              options.CLI,
		workspace:        workspace,
		store:            store,
		policy:           policy,
		policies:         policies,
		activeSession:    activeSession,
		runtimeEvents:    runtimeEvents,
		httpClient:       options.HTTPClient,
		secrets:          secretRegistry,
		lane:             orchestrator.NewOperationLane(),
		dataDir:          dataDir,
		workspaceControl: workspaceControl,
	}
	runtimeSet := RuntimeSet{ConfigurationError: configErr, configPath: configPath, Admission: bootstrapAdmission, Redactor: bootstrapAdmission, RuntimeGenerationID: bootstrapGeneration, retire: bootstrapRetire}
	if configErr == nil {
		candidate, buildErr := builder.build(cfg, session.Selection)
		if buildErr != nil {
			configErr = buildErr
			runtimeSet = RuntimeSet{Models: cfg.Models(), DefaultSelection: cfg.DefaultSelection(), ConfigurationError: buildErr, configPath: configPath, Admission: bootstrapAdmission, Redactor: bootstrapAdmission, RuntimeGenerationID: bootstrapGeneration, retire: bootstrapRetire}
		} else {
			runtimeSet = candidate
			bootstrapApplication := &App{runtimeSet: RuntimeSet{}, sessions: store, workspaceControl: workspaceControl}
			if err := bootstrapApplication.activateRuntimeGeneration(ctx, candidate); err != nil {
				candidate.retireSecrets()
				return nil, Snapshot{}, fmt.Errorf("activate initial runtime generation: %w", err)
			}
			if err := recoverBootstrapSession(ctx, store, candidate, workspaceControl, protocol.SessionID(session.ID)); err != nil {
				candidate.retireSecrets()
				return nil, Snapshot{}, fmt.Errorf("recover selected session: %w", err)
			}
			if options.CLI.ModeSet && session.Mode != options.CLI.Mode {
				if err := appendBootstrapMode(ctx, candidate, store, &session, &replay, options.CLI.Mode); err != nil {
					return nil, Snapshot{}, fmt.Errorf("persist CLI mode override: %w", err)
				}
				policy.SetMode(options.CLI.Mode)
			}
			if options.CLI.ProfileSet || options.CLI.ModelSet {
				resolved, resolveErr := cfg.Resolve(config.ResolveOptions{
					Profile:        options.CLI.Profile,
					Model:          options.CLI.Model,
					BaseURL:        options.CLI.BaseURL,
					DefaultProfile: session.Selection.Profile,
					DefaultModel:   session.Selection.Model,
				})
				if resolveErr != nil {
					return nil, Snapshot{}, fmt.Errorf("resolve CLI model override: %w", resolveErr)
				}
				override := domain.ModelSelection{Profile: resolved.Name, Model: resolved.Model}
				if session.Selection != override {
					if err := appendBootstrapSelection(ctx, candidate, store, &session, &replay, override); err != nil {
						return nil, Snapshot{}, fmt.Errorf("persist CLI model override: %w", err)
					}
				}
			}
			if !slices.Contains(candidate.Models, session.Selection) {
				if err := appendBootstrapSelection(ctx, candidate, store, &session, &replay, candidate.DefaultSelection); err != nil {
					return nil, Snapshot{}, fmt.Errorf("persist default model: %w", err)
				}
			}
			redactors.Replace(candidate.Redactor)
			bootstrapRetire()
			configErr = candidate.Ready(session.Selection)
		}
	}
	replay, err = inspectBootstrapSession(ctx, store, session.ID)
	if err != nil {
		return nil, Snapshot{}, fmt.Errorf("inspect selected session after bootstrap controls: %w", err)
	}
	state = ProjectSessionState(replay)
	replay.Session.Mode = state.Mode
	replay.Session.Selection = state.Selection
	session = replay.Session
	sessions, err := store.List(ctx, workspace)
	if err != nil {
		return nil, Snapshot{}, fmt.Errorf("list sessions: %w", err)
	}
	var reload ReloadRuntime
	if configPath != "" {
		reload = func(_ context.Context, current domain.ModelSelection) (RuntimeSet, error) {
			reloaded, loadErr := config.Load(config.LoadOptions{ConfigPath: configPath})
			if loadErr != nil {
				return RuntimeSet{}, configurationError(configPath, fmt.Sprintf("configuration is invalid (%v); edit the file and run /reload", loadErr), loadErr)
			}
			return builder.build(reloaded, current)
		}
	}
	application := New(Options{
		RuntimeSet:       runtimeSet,
		ReloadRuntime:    reload,
		Redactors:        redactors,
		RuntimeEvents:    runtimeEvents,
		Sessions:         store,
		Session:          session,
		Replay:           replay,
		Workspace:        workspace,
		Policy:           policy,
		RestorePolicy:    policy.restore,
		CommandBuffer:    16,
		EventBuffer:      64,
		Logger:           logger,
		Close:            closeDebugLog,
		SessionChanged:   activeSession.set,
		WorkspaceControl: workspaceControl,
	})
	debugLogOwned = false
	bootstrapHandedOff = true
	if logger != nil {
		if err := logger.Event("bootstrap", map[string]any{
			"workspace": workspace.CanonicalPath,
			"profile":   session.Selection.Profile,
			"model":     session.Selection.Model,
		}); err != nil {
			return nil, Snapshot{}, fmt.Errorf("write debug log: %w", err)
		}
	}

	var contextState *protocol.ContextProjectionV1
	if runtimeSet.ApplicationService != nil {
		durable, snapErr := runtimeSet.ApplicationService.Snapshot(ctx, protocol.SnapshotRequest{ProtocolVersion: protocol.ApplicationProtocolVersion, SelectedSessionID: protocol.SessionID(session.ID), Consumer: "bootstrap_context", QueueCapacity: 1})
		if snapErr != nil {
			return nil, Snapshot{}, fmt.Errorf("snapshot durable context: %w", snapErr)
		}
		contextState, snapErr = DurableContext(durable)
		if snapErr != nil {
			return nil, Snapshot{}, fmt.Errorf("decode durable context: %w", snapErr)
		}
	}
	return application, Snapshot{
		Workspace:          workspace,
		Session:            session,
		Replay:             replay,
		Sessions:           sessions,
		Models:             append([]domain.ModelSelection(nil), runtimeSet.Models...),
		ConfigurationError: configErr,
		Context:            contextState,
		Skills:             runtimeSet.SkillSnapshot(),
	}, nil
}

func recoverBootstrapSession(ctx context.Context, store *jsonl.Store, runtime RuntimeSet, workspaceControl protocol.JournalRef, sessionID protocol.SessionID) error {
	if store == nil || runtime.Orchestrator == nil || sessionID == "" {
		return nil
	}
	inspection, err := store.InspectSession(ctx, sessionID)
	if err != nil {
		return err
	}
	var observedTail protocol.Digest
	for _, diagnostic := range inspection.Journal.Diagnostics {
		if diagnostic.Code != "recovery.available" {
			continue
		}
		var details struct {
			ObservedTailDigest protocol.Digest `json:"observed_tail_digest"`
		}
		if err := json.Unmarshal(diagnostic.Details, &details); err != nil {
			return fmt.Errorf("decode recovery observation: %w", err)
		}
		observedTail = details.ObservedTailDigest
		break
	}
	if observedTail.IsZero() {
		return nil
	}
	controlHead, err := store.Head(ctx, workspaceControl)
	if err != nil {
		return err
	}
	identityDigest, err := canonicaljson.Digest(struct {
		SessionID    protocol.SessionID       `json:"session_id"`
		ExpectedHead protocol.CommittedCursor `json:"expected_head"`
		ObservedTail protocol.Digest          `json:"observed_tail"`
	}{sessionID, inspection.Journal.Head, observedTail})
	if err != nil {
		return err
	}
	identity := "recovery-" + identityDigest.Value[:32]
	operationID := protocol.ControlOperationID(identity)
	storage := journal.RecoveryRequest{
		OperationID: operationID, Journal: inspection.Journal.Journal, ExpectedHead: inspection.Journal.Head,
		ObservedTailDigest: observedTail, TransactionID: protocol.TransactionID(identity + "-storage"), RuntimeGenerationID: runtime.RuntimeGenerationID,
	}
	descriptorDigest, err := canonicaljson.Digest(struct {
		Name string `json:"name"`
	}{"runtime.recovery"})
	if err != nil {
		return err
	}
	body := protocol.ActionPlanBody{
		CallID: identity, Tool: protocol.ToolIdentity{Source: "runtime", Authority: "yordam", Name: "recovery"}, SourceRevision: "runtime-v1",
		DescriptorDigest: descriptorDigest, Action: "runtime.recovery", Purpose: "recover eligible session journal tail",
		Resources:      []protocol.ResourceTarget{{Kind: "session_journal", CanonicalID: string(sessionID)}},
		ExecutionLocus: "runtime", Effect: "mutation", Boundary: "process", Reversibility: "exact", VerificationCoverage: "exact",
		RequestedProfile: "restricted", EffectiveProfile: "restricted", RuntimeGenerationID: runtime.RuntimeGenerationID,
	}
	planDigest, err := canonicaljson.Digest(body)
	if err != nil {
		return err
	}
	diagnostic := protocol.Diagnostic{Code: "recovery.requested", Message: "recover eligible session journal tail", Journal: workspaceControl}
	payload, err := canonicaljson.Marshal(protocol.DiagnosticV1{Diagnostic: diagnostic})
	if err != nil {
		return err
	}
	requestDigest, err := canonicaljson.Digest(struct {
		OperationID protocol.ControlOperationID `json:"operation_id"`
		Storage     journal.RecoveryRequest     `json:"storage"`
		Plan        protocol.Digest             `json:"plan"`
	}{operationID, storage, planDigest})
	if err != nil {
		return err
	}
	actor := protocol.ActorRef{ID: "bootstrap-user", Kind: protocol.ActorUser}
	result, err := runtime.Orchestrator.RecoverTurn(ctx, orchestrator.RecoveryControlRequest{
		Control: orchestrator.ControlRequest{
			Command:     orchestrator.CommandMetadata{CommandID: protocol.CommandID(identity), IdempotencyKey: identity, RequestDigest: requestDigest, Actor: actor},
			OperationID: operationID, Kind: orchestrator.OperationRecovery, Journal: workspaceControl, ExpectedHead: controlHead,
			TransactionID: protocol.TransactionID(identity + "-control"), Runtime: protocol.DeepCopy(runtime.Manifest), Plan: protocol.ActionPlan{Body: body, Digest: planDigest},
			Event: protocol.ProposedEvent{EventID: protocol.EventID(identity), Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventRecoveryDiagnostic, Actor: &actor, RuntimeGenerationID: runtime.RuntimeGenerationID, Payload: payload},
		},
		Storage: storage,
	})
	if err != nil {
		return err
	}
	if result.Recovery.Status != "recovered" {
		return fmt.Errorf("recovery status %q", result.Recovery.Status)
	}
	return nil
}

func loadBootstrapConfig(options BootstrapOptions) (config.Config, string, error) {
	configPath, err := canonicalConfigPath(options.ConfigPath)
	if err != nil {
		return config.Config{}, options.ConfigPath, configurationError(options.ConfigPath, fmt.Sprintf("configuration path is invalid (%v)", err), err)
	}
	cfg, err := config.Load(config.LoadOptions{ConfigPath: configPath})
	if err != nil {
		return config.Config{}, configPath, configurationError(configPath, fmt.Sprintf("configuration is invalid (%v); edit the file and run /reload", err), err)
	}
	return cfg, configPath, nil
}

// canonicalConfigPath freezes a stable on-disk configuration identity before
// the runtime derives the adjacent global skills directory. Resolving this
// once prevents a relative path or symlink from changing the skill root on a
// later reload.
func canonicalConfigPath(path string) (string, error) {
	if path == "" {
		var err error
		path, err = config.DefaultConfigPath()
		if err != nil {
			return "", err
		}
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(filepath.Clean(abs))
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(resolved) || filepath.Clean(resolved) != resolved {
		return "", fmt.Errorf("non-canonical config path")
	}
	return resolved, nil
}

func appendBootstrapSelection(ctx context.Context, runtime RuntimeSet, store sessionStore, session *domain.Session, replay *domain.SessionReplay, selection domain.ModelSelection) error {
	application := &App{runtimeSet: runtime, sessions: store, session: *session, sessionChanges: runtime.SessionChanges}
	if err := application.commitSessionChange(ctx, protocol.EventModelChanged, protocol.ModelChangedV1{ProviderID: protocol.ProviderID(selection.Profile), ModelID: protocol.ModelID(selection.Model)}); err != nil {
		return err
	}
	session.Selection = selection
	session.LastSeq = application.session.LastSeq
	session.UpdatedAt = application.session.UpdatedAt
	replay.Session = *session
	return nil
}

func appendBootstrapMode(ctx context.Context, runtime RuntimeSet, store sessionStore, session *domain.Session, replay *domain.SessionReplay, mode domain.PermissionMode) error {
	application := &App{runtimeSet: runtime, sessions: store, session: *session, sessionChanges: runtime.SessionChanges}
	if err := application.commitSessionChange(ctx, protocol.EventModeChanged, protocol.ModeChangedV1{Mode: string(mode)}); err != nil {
		return err
	}
	session.Mode = mode
	session.LastSeq = application.session.LastSeq
	session.UpdatedAt = application.session.UpdatedAt
	replay.Session = *session
	return nil
}

func resolveWorkspace(cwd string) (domain.Workspace, error) {
	if cwd == "" {
		var err error
		cwd, err = os.Getwd()
		if err != nil {
			return domain.Workspace{}, fmt.Errorf("get current directory: %w", err)
		}
	}
	workspace, err := jsonl.WorkspaceFromPath(cwd)
	if err != nil {
		return domain.Workspace{}, fmt.Errorf("resolve workspace: %w", err)
	}
	return workspace, nil
}

func resolveDataDir(configured string) (string, error) {
	if configured != "" {
		return configured, nil
	}
	path, err := config.DefaultDataDir()
	if err != nil {
		return "", fmt.Errorf("resolve data directory: %w", err)
	}
	return path, nil
}

func selectSession(
	ctx context.Context,
	store *jsonl.Store,
	workspace domain.Workspace,
	options cli.Options,
	selection domain.ModelSelection,
) (domain.Session, domain.SessionReplay, error) {
	var session domain.Session
	var replay domain.SessionReplay
	var err error
	switch {
	case options.Session != "":
		replay, err = inspectBootstrapSession(ctx, store, options.Session)
		if err == nil && replay.Session.Workspace != workspace {
			return domain.Session{}, domain.SessionReplay{}, fmt.Errorf("session belongs to a different workspace")
		}
		session = replay.Session
	case options.Continue:
		var sessions []domain.SessionSummary
		sessions, err = store.List(ctx, workspace)
		if err == nil && len(sessions) == 0 {
			err = fmt.Errorf("no session exists for the current workspace")
		}
		if err == nil {
			replay, err = inspectBootstrapSession(ctx, store, sessions[0].ID)
			session = replay.Session
		}
	default:
		session, err = store.Create(ctx, workspace, options.Mode, selection)
		if err == nil {
			replay, err = inspectBootstrapSession(ctx, store, session.ID)
			session = replay.Session
		}
	}
	if err != nil {
		return domain.Session{}, domain.SessionReplay{}, fmt.Errorf("select session: %w", err)
	}
	return session, replay, nil
}

func inspectBootstrapSession(ctx context.Context, store *jsonl.Store, sessionID string) (domain.SessionReplay, error) {
	inspection, err := store.InspectSession(ctx, protocol.SessionID(sessionID))
	if err != nil {
		return domain.SessionReplay{}, err
	}
	return LegacyReplayFromInspection(inspection), nil
}

func openDebugLogger(path string, redactor *secret.Binding) (*logging.Logger, func() error, error) {
	if path == "" {
		return nil, nil, nil
	}
	lease, err := redactor.AcquireLease()
	if err != nil {
		return nil, nil, fmt.Errorf("validate debug log admission: %w", err)
	}
	_ = lease.Close()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, nil, fmt.Errorf("create debug log directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("open debug log: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, nil, fmt.Errorf("set debug log permissions: %w", err)
	}
	logger, err := logging.NewGenerationBound(file, redactor)
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	return logger, file.Close, nil
}

func zeroBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

type policyBinding struct {
	mu       sync.RWMutex
	current  *permission.SessionPolicy
	registry *permission.SessionPolicyRegistry
}

type sessionBinding struct {
	mu sync.RWMutex
	id string
}

func (s *sessionBinding) get() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.id
}

func (s *sessionBinding) set(sessionID string) {
	s.mu.Lock()
	s.id = sessionID
	s.mu.Unlock()
}

func newPolicyBinding(current *permission.SessionPolicy) *policyBinding {
	return &policyBinding{current: current}
}

func (p *policyBinding) Evaluate(ctx context.Context, permissionContext ports.PermissionContext, request domain.PreparedToolRequest) domain.PermissionDecision {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.current.Evaluate(ctx, permissionContext, request)
}

func (p *policyBinding) EvaluateAuthorization(ctx context.Context, input ports.EvaluationInput) (protocol.AuthorizationDecision, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.current.EvaluateAuthorization(ctx, input)
}

func (p *policyBinding) GrantAuthorizationSession(request protocol.AuthorizationRequest, constraints []protocol.AuthorizationConstraint) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current.GrantAuthorizationSession(request, constraints)
}

func (p *policyBinding) SetMode(mode domain.PermissionMode) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current.SetMode(mode)
}

func (p *policyBinding) AcknowledgeAutoShell() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current.AcknowledgeAutoShell()
}

func (p *policyBinding) GrantSession(tool, scope string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current.GrantSession(tool, scope)
}

func (p *policyBinding) restore(replay domain.SessionReplay) MutablePolicy {
	p.mu.Lock()
	p.current = permission.Restore(replay)
	if p.registry != nil {
		_ = p.registry.RegisterParent(protocol.SessionID(replay.Session.ID), p.current)
	}
	p.mu.Unlock()
	return p
}
