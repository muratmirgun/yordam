package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/cli"
	"github.com/muratmirgun/yordam/internal/compaction"
	"github.com/muratmirgun/yordam/internal/config"
	contextplanner "github.com/muratmirgun/yordam/internal/context"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/evidence"
	"github.com/muratmirgun/yordam/internal/orchestrator"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/provider"
	"github.com/muratmirgun/yordam/internal/provider/openaicompat"
	"github.com/muratmirgun/yordam/internal/recovery"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
	"github.com/muratmirgun/yordam/internal/tooling"
	edittool "github.com/muratmirgun/yordam/internal/tools/edit"
	"github.com/muratmirgun/yordam/internal/tools/output"
	readtool "github.com/muratmirgun/yordam/internal/tools/read"
	searchtool "github.com/muratmirgun/yordam/internal/tools/search"
	shelltool "github.com/muratmirgun/yordam/internal/tools/shell"
	"github.com/muratmirgun/yordam/internal/verification"
)

type RuntimeSet struct {
	Runtime              Runtime
	CompactSession       CompactSession
	Models               []domain.ModelSelection
	DefaultSelection     domain.ModelSelection
	CredentialEnvs       map[string]string
	Credentials          map[string]string
	Redactor             secret.Redacting
	Admission            *secret.Lease
	RuntimeGenerationID  protocol.RuntimeGenerationID
	Manifest             protocol.RuntimeGenerationManifest
	Orchestrator         *orchestrator.Service
	SessionChanges       sessionChangeService
	ProviderCatalog      provider.Catalog
	ProviderService      *provider.Service
	ToolService          *tooling.Service
	AuthorizationService orchestrator.AuthorizationService
	Broker               *Broker
	ApplicationService   *ProtocolService
	LegacyAdapter        *LegacyAdapter
	ConfigurationError   error
	configPath           string
	bindApprover         func(ports.PermissionApprover)
	unchecked            bool
	retire               func()
}

var runtimeSecretGeneration atomic.Uint64

func (s RuntimeSet) retireSecrets() {
	if s.retire != nil {
		s.retire()
	}
}

func (s RuntimeSet) Validate() error {
	if s.ConfigurationError != nil {
		return s.ConfigurationError
	}
	if s.Manifest.ID == "" || s.Manifest.ID != s.RuntimeGenerationID {
		return fmt.Errorf("runtime generation manifest identity mismatch")
	}
	if err := canonicaljson.ValidateDigest(s.Manifest.Body, s.Manifest.Digest); err != nil {
		return fmt.Errorf("runtime generation manifest: %w", err)
	}
	if len(s.Manifest.Body.Models) == 0 || s.Manifest.Body.Limits.MaxToolCalls < 1 || s.Manifest.Body.Limits.MaxToolCalls > 128 || s.Manifest.Body.Limits.ShellTimeoutNanos <= 0 || s.Manifest.Body.Limits.ApplicationQueueCapacity <= 0 {
		return fmt.Errorf("runtime generation manifest limits or models are invalid")
	}
	reserve := s.Manifest.Body.Limits.CompactReserveTokens
	if err := reserve.Validate(); err != nil || (reserve.State != protocol.ValueKnown && reserve.State != protocol.ValueUnknown) || (reserve.State == protocol.ValueKnown && reserve.Value <= 0) {
		return fmt.Errorf("runtime generation compact reserve is invalid")
	}
	if s.CompactSession == nil {
		return fmt.Errorf("runtime compaction is not configured")
	}
	for _, model := range s.Manifest.Body.Models {
		if err := model.Validate(); err != nil || model.RuntimeGenerationID != s.Manifest.ID {
			return fmt.Errorf("runtime generation model is invalid")
		}
	}
	for _, descriptor := range s.Manifest.Body.Tools {
		if err := descriptor.Body.Validate(); err != nil || canonicaljson.ValidateDigest(descriptor.Body, descriptor.DescriptorDigest) != nil {
			return fmt.Errorf("runtime generation tool is invalid")
		}
	}
	return nil
}

type ReloadRuntime func(context.Context, domain.ModelSelection) (RuntimeSet, error)

// String returns a diagnostic summary without exposing credential values.
func (s RuntimeSet) String() string {
	return fmt.Sprintf("RuntimeSet{Models:%v DefaultSelection:%+v CredentialEnvs:%v ConfigurationError:%t}", s.Models, s.DefaultSelection, s.CredentialEnvs, s.ConfigurationError != nil)
}

// MarshalJSON serializes only the non-sensitive runtime metadata.
func (s RuntimeSet) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Models             []domain.ModelSelection `json:"models"`
		DefaultSelection   domain.ModelSelection   `json:"defaultSelection"`
		CredentialEnvs     map[string]string       `json:"credentialEnvs"`
		ConfigurationError bool                    `json:"configurationError"`
	}{
		Models:             s.Models,
		DefaultSelection:   s.DefaultSelection,
		CredentialEnvs:     s.CredentialEnvs,
		ConfigurationError: s.ConfigurationError != nil,
	})
}

func (s RuntimeSet) Ready(selection domain.ModelSelection) error {
	if s.ConfigurationError != nil {
		return s.ConfigurationError
	}
	if s.unchecked {
		return nil
	}
	if !slices.Contains(s.Models, selection) {
		return configurationError(s.configPath, fmt.Sprintf("model %q is not configured for provider %q; edit the file and run /reload", selection.Model, selection.Profile), nil)
	}
	if s.Credentials[selection.Profile] == "" {
		environment := s.CredentialEnvs[selection.Profile]
		return configurationError(s.configPath, fmt.Sprintf("API key environment variable %q is empty; export it and restart Yordam", environment), nil)
	}
	return nil
}

func (s RuntimeSet) BindApprover(approver ports.PermissionApprover) {
	if s.bindApprover != nil {
		s.bindApprover(approver)
	}
}

type runtimeBuilder struct {
	configPath       string
	cli              cli.Options
	workspace        domain.Workspace
	store            *jsonl.Store
	policy           *policyBinding
	activeSession    *sessionBinding
	runtimeEvents    chan<- agent.RuntimeEvent
	httpClient       *http.Client
	secrets          *secret.Registry
	lane             orchestrator.OperationLane
	publisher        orchestrator.ApplicationEventPublisher
	dataDir          string
	workspaceControl protocol.JournalRef
}

func (b *runtimeBuilder) build(cfg config.Config, current domain.ModelSelection) (RuntimeSet, error) {
	defaultProfile, defaultModel := "", ""
	if slices.Contains(cfg.Models(), current) {
		defaultProfile, defaultModel = current.Profile, current.Model
	}
	selected, err := cfg.Resolve(config.ResolveOptions{
		Profile:        b.cli.Profile,
		Model:          b.cli.Model,
		BaseURL:        b.cli.BaseURL,
		DefaultProfile: defaultProfile,
		DefaultModel:   defaultModel,
	})
	if err != nil {
		return RuntimeSet{}, configurationError(b.configPath, "configuration is invalid; edit the file and run /reload", err)
	}
	credentials := cfg.APIKeys()
	redactionValues := profileKeyValues(credentials)
	credentials[selected.Name] = selected.APIKey
	redactionValues = append(redactionValues, selected.APIKey)
	values := make([][]byte, 0, len(redactionValues))
	for _, value := range redactionValues {
		if value != "" {
			values = append(values, []byte(value))
		}
	}
	secretRegistry := b.secrets
	if secretRegistry == nil {
		secretRegistry = secret.NewRegistry()
	}
	generationID := protocol.RuntimeGenerationID(fmt.Sprintf("runtime-%d", runtimeSecretGeneration.Add(1)))
	admission, err := secretRegistry.Acquire(generationID, values)
	if err != nil {
		return RuntimeSet{}, configurationError(b.configPath, "bind runtime secret admission", err)
	}
	retireOnce := &sync.Once{}
	producerLeases := make([]*secret.Lease, 0, len(cfg.Profiles)+2)
	retire := func() {
		retireOnce.Do(func() {
			_ = secretRegistry.Retire(generationID)
			_ = admission.Close()
			for _, producer := range producerLeases {
				_ = producer.Close()
			}
		})
	}
	succeeded := false
	defer func() {
		if !succeeded {
			retire()
		}
	}()
	clients := make(map[string]*openaicompat.Client, len(cfg.Profiles))
	routedAdapters := make(map[protocol.ProviderID]*openaicompat.Adapter, len(cfg.Profiles))
	credentialEnvs := make(map[string]string, len(cfg.Profiles))
	profileNames := make([]string, 0, len(cfg.Profiles))
	for name := range cfg.Profiles {
		profileNames = append(profileNames, name)
	}
	slices.Sort(profileNames)
	for _, name := range profileNames {
		profile := cfg.Profiles[name]
		clientAdmission, leaseErr := admission.Derive()
		if leaseErr != nil {
			return RuntimeSet{}, configurationError(b.configPath, "bind provider secret admission", leaseErr)
		}
		producerLeases = append(producerLeases, clientAdmission)
		baseURL := profile.BaseURL
		model := profile.DefaultModel
		if name == selected.Name {
			baseURL = selected.BaseURL
			model = selected.Model
		}
		credentialEnvs[name] = profile.APIKeyEnv
		clients[name] = openaicompat.New(openaicompat.ClientOptions{
			HTTPClient: b.httpClient,
			BaseURL:    baseURL,
			APIKey:     credentials[name],
			Model:      model,
			Redact:     clientAdmission.String,
			Admission:  clientAdmission,
		})
		routedAdapters[protocol.ProviderID(name)] = openaicompat.NewAdapter(clients[name])
	}
	outputOptions := output.Options{
		SessionID:        b.activeSession.get(),
		CurrentSessionID: b.activeSession.get,
		Artifacts:        b.store,
		Redact:           admission,
		Admission:        admission,
	}
	runnerAdmission, err := admission.Derive()
	if err != nil {
		return RuntimeSet{}, configurationError(b.configPath, "bind runner secret admission", err)
	}
	producerLeases = append(producerLeases, runnerAdmission)
	progress := func(progress domain.ToolProgress) {
		if b.runtimeEvents == nil {
			return
		}
		event := agent.RuntimeEvent{Kind: agent.RuntimeToolOutput, Progress: &progress}
		select {
		case b.runtimeEvents <- event:
		default:
		}
	}
	toolItems := []ports.Tool{
		readtool.New(readtool.Options{Workspace: b.workspace.CanonicalPath, Output: outputOptions}),
		searchtool.New(searchtool.Options{Workspace: b.workspace.CanonicalPath, Output: outputOptions}),
		edittool.New(edittool.Options{Workspace: b.workspace.CanonicalPath, Output: outputOptions}),
		shelltool.New(shelltool.Options{
			Workspace:       b.workspace.CanonicalPath,
			ProviderKeyEnvs: cfg.ProviderKeyEnvironmentNames(),
			Timeout:         effectiveShellTimeout(cfg, b.cli),
			Output:          outputOptions,
			Progress:        progress,
		}),
	}
	toolCatalogRevision := "builtin-v1"
	toolCatalog, err := tooling.NewCatalog(toolCatalogRevision, toolItems...)
	if err != nil {
		return RuntimeSet{}, configurationError(b.configPath, "build tool catalog", err)
	}
	models := runtimeModelDescriptors(cfg, generationID)
	providerCatalogRevision := "configured-v1"
	providerCatalog := provider.NewCatalog(providerCatalogRevision, models)
	durableAuthorization := authorization.NewService(b.store)
	authorizer := &runtimeAuthorization{
		Service: durableAuthorization, Policy: b.policy, Tools: toolCatalog,
		Workspace: b.workspace, ActiveSession: b.activeSession,
	}
	providerService, err := provider.NewService(providerCatalog, []provider.Adapter{&openAIAdapterRouter{byProvider: routedAdapters}}, authorizer)
	if err != nil {
		return RuntimeSet{}, configurationError(b.configPath, "build provider service", err)
	}
	toolService := tooling.NewService(toolCatalog, authorizer)
	toolDescriptors := make([]protocol.ToolDescriptor, 0, len(toolItems))
	for _, exposed := range toolCatalog.Expose().Aliases {
		descriptor, ok := toolCatalog.Descriptor(exposed.Alias)
		if !ok {
			return RuntimeSet{}, configurationError(b.configPath, "build tool manifest", fmt.Errorf("tool alias %q disappeared", exposed.Alias))
		}
		toolDescriptors = append(toolDescriptors, descriptor)
	}
	body := protocol.RuntimeGenerationBody{
		ProviderCatalogRevision: providerCatalogRevision,
		Models:                  protocol.DeepCopy(models),
		ToolCatalogRevision:     toolCatalogRevision,
		Tools:                   protocol.DeepCopy(toolDescriptors),
		InstructionRevision:     "system-v1",
		PolicyGeneration:        "compatibility-v1",
		ExecutionProfiles:       []string{"network", "restricted", "unsandboxed"},
		Limits: protocol.RuntimeLimits{
			MaxToolCalls: effectiveMaxToolCalls(cfg, b.cli), ShellTimeoutNanos: int64(effectiveShellTimeout(cfg, b.cli)), ApplicationQueueCapacity: 64,
			AutoCompact: cfg.Context.AutoCompact, CompactReserveTokens: runtimeCompactReserve(cfg),
		},
	}
	manifestDigest, err := canonicaljson.Digest(body)
	if err != nil {
		return RuntimeSet{}, configurationError(b.configPath, "digest runtime manifest", err)
	}
	manifest := protocol.RuntimeGenerationManifest{ID: generationID, Body: protocol.DeepCopy(body), Digest: manifestDigest}
	if b.lane == nil {
		b.lane = orchestrator.NewOperationLane()
	}
	workspaceControl := b.workspaceControl
	if workspaceControl == (protocol.JournalRef{}) {
		workspaceControl, err = b.store.EnsureWorkspaceControl(context.Background(), b.workspace)
		if err != nil {
			return RuntimeSet{}, configurationError(b.configPath, "initialize application broker", err)
		}
	}
	broker, err := NewBroker(BrokerOptions{
		Source: runtimeBrokerSource{Repository: b.store, Workspace: workspaceControl, Generation: generationID, Manifest: manifest},
		Epoch:  string(generationID), DefaultQueueCapacity: 64, MaxQueueCapacity: 1024,
	})
	if err != nil {
		return RuntimeSet{}, configurationError(b.configPath, "build application broker", err)
	}
	dataDir := b.dataDir
	if dataDir == "" {
		dataDir = filepath.Join(b.workspace.CanonicalPath, ".yordam-runtime")
	}
	evidenceStore, err := evidence.New(filepath.Join(dataDir, "evidence"), runnerAdmission, evidence.WithLegacyResolver(b.store))
	if err != nil {
		return RuntimeSet{}, configurationError(b.configPath, "build evidence store", err)
	}
	recoveryStore, err := recovery.New(filepath.Join(dataDir, "recovery"), runnerAdmission)
	if err != nil {
		_ = evidenceStore.Close()
		return RuntimeSet{}, configurationError(b.configPath, "build recovery store", err)
	}
	approverBridge := &interactiveApproverBinding{}
	publisher := b.publisher
	if publisher == nil {
		publisher = broker
	}
	service, err := orchestrator.NewService(orchestrator.Dependencies{
		Lane: b.lane, Repository: b.store, TurnLeases: b.store,
		Context: contextplanner.NewPlanner(toolCatalogRevision, contextplanner.NewEvidenceSummaryResolver(evidenceStore)), Providers: providerCatalog, Provider: providerService,
		Tools: toolService, Authorization: authorizer, Approver: approverBridge,
		Evidence: evidenceStore, Recovery: recoveryRecorder{Tools: toolService, Store: recoveryStore},
		Verification: verification.NewService(time.Now), Projection: recoveryProjection{Repository: b.store},
		Publisher: publisher, Admission: generationAdmission{Registry: secretRegistry}, Instructions: staticInstructions{},
	})
	if err != nil {
		_ = evidenceStore.Close()
		_ = recoveryStore.Close()
		return RuntimeSet{}, configurationError(b.configPath, "build turn orchestrator", err)
	}
	compactSession := func(ctx context.Context, session domain.Session, _ domain.SessionReplay) error {
		if session.ID == "" {
			return fmt.Errorf("compaction requires a selected session")
		}
		ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(session.ID)}
		head, err := b.store.Head(ctx, ref)
		if err != nil {
			return err
		}
		metadata, err := runtimeCompactionMetadata(protocol.SessionID(session.ID), head, manifest.ID)
		if err != nil {
			return err
		}
		_, err = service.RunCompaction(ctx, orchestrator.CompactRequest{
			Command: metadata, SessionID: protocol.SessionID(session.ID), ExpectedHead: head,
			ProviderID: protocol.ProviderID(session.Selection.Profile), ModelID: protocol.ModelID(session.Selection.Model),
			Runtime: protocol.DeepCopy(manifest), Trigger: compaction.TriggerManual,
		})
		return err
	}
	lifecycle := newRuntimeLifecycle(func() {
		_ = evidenceStore.Close()
		_ = recoveryStore.Close()
		_ = secretRegistry.Retire(generationID)
		_ = admission.Close()
		for _, producer := range producerLeases {
			_ = producer.Close()
		}
	})
	retire = lifecycle.retire
	runner := &agent.OrchestratedRunner{
		Orchestrator: service,
		Prepare: func(_ context.Context, input agent.RunInput) (orchestrator.StartTurnRequest, error) {
			if input.ExpectedHead.Validate() != nil || input.Command.CommandID == "" {
				return orchestrator.StartTurnRequest{}, fmt.Errorf("turn input has no committed command or session head")
			}
			return orchestrator.StartTurnRequest{
				Command: input.Command, ExpectedHead: input.ExpectedHead,
				ProviderID: protocol.ProviderID(input.Session.Selection.Profile), ModelID: protocol.ModelID(input.Session.Selection.Model),
				Runtime: protocol.DeepCopy(manifest),
			}, nil
		},
		Acquire: lifecycle.acquire,
		Sink: func(event agent.RuntimeEvent) {
			if b.runtimeEvents == nil {
				return
			}
			select {
			case b.runtimeEvents <- event:
			default:
			}
		},
	}
	dispatcher := runtimeCommandDispatcher{Orchestrator: service, Store: b.store, Manifest: protocol.DeepCopy(manifest), Workspace: workspaceControl}
	applicationService, err := NewProtocolService(ProtocolServiceOptions{
		Orchestrator: service, Dispatcher: dispatcher, Broker: broker, WorkspaceControl: workspaceControl,
	})
	if err != nil {
		lifecycle.retire()
		return RuntimeSet{}, configurationError(b.configPath, "build application service", err)
	}
	legacyAdapter := NewLegacyAdapter(LegacyAdapterOptions{
		Actor: protocol.ActorRef{ID: "legacy-user", Kind: protocol.ActorUser}, SelectedSessionID: protocol.SessionID(b.activeSession.get()),
		Cursor: runtimeCommandExpectation(b.store, workspaceControl, b.activeSession), RuntimeGenerationID: generationID,
	})
	succeeded = true
	result := RuntimeSet{
		Runtime:              runner,
		CompactSession:       compactSession,
		Models:               cfg.Models(),
		DefaultSelection:     cfg.DefaultSelection(),
		CredentialEnvs:       credentialEnvs,
		Credentials:          credentials,
		Redactor:             admission,
		Admission:            admission,
		RuntimeGenerationID:  generationID,
		Manifest:             protocol.DeepCopy(manifest),
		Orchestrator:         service,
		SessionChanges:       service,
		ProviderCatalog:      providerCatalog,
		ProviderService:      providerService,
		ToolService:          toolService,
		AuthorizationService: authorizer,
		Broker:               broker,
		ApplicationService:   applicationService,
		LegacyAdapter:        legacyAdapter,
		configPath:           b.configPath,
		bindApprover: func(approver ports.PermissionApprover) {
			approverBridge.set(approver)
		},
		retire: retire,
	}
	if err := result.Validate(); err != nil {
		result.retireSecrets()
		return RuntimeSet{}, configurationError(b.configPath, "validate runtime generation", err)
	}
	return result, nil
}

type failClosedDeltaRedactor struct{}

func (failClosedDeltaRedactor) Write(string) string { return "[REDACTED]" }
func (failClosedDeltaRedactor) Close() string       { return "" }

func profileKeyValues(keys map[string]string) []string {
	values := make([]string, 0, len(keys))
	for _, key := range keys {
		if key != "" {
			values = append(values, key)
		}
	}
	return values
}

func effectiveMaxToolCalls(cfg config.Config, options cli.Options) int {
	if options.MaxToolsSet || (options.MaxToolCalls != 0 && options.MaxToolCalls != 32) {
		return options.MaxToolCalls
	}
	return cfg.MaxToolCalls
}

func effectiveShellTimeout(cfg config.Config, options cli.Options) time.Duration {
	if options.TimeoutSet || (options.ShellTimeout != 0 && options.ShellTimeout != 120*time.Second) {
		return options.ShellTimeout
	}
	return time.Duration(cfg.ShellTimeoutSeconds) * time.Second
}

func configurationError(path, message string, cause error) error {
	if path != "" {
		message = path + ": " + message
	}
	return &domain.TypedError{Kind: domain.ErrorConfigurationInvalid, Message: message, Cause: cause}
}
