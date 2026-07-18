package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/cli"
	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/provider/openaicompat"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
	"github.com/muratmirgun/yordam/internal/tools"
	edittool "github.com/muratmirgun/yordam/internal/tools/edit"
	"github.com/muratmirgun/yordam/internal/tools/output"
	readtool "github.com/muratmirgun/yordam/internal/tools/read"
	searchtool "github.com/muratmirgun/yordam/internal/tools/search"
	shelltool "github.com/muratmirgun/yordam/internal/tools/shell"
)

type RuntimeSet struct {
	Runtime             Runtime
	CompactSession      CompactSession
	Models              []domain.ModelSelection
	DefaultSelection    domain.ModelSelection
	CredentialEnvs      map[string]string
	Credentials         map[string]string
	Redactor            secret.Redacting
	Admission           *secret.Lease
	RuntimeGenerationID protocol.RuntimeGenerationID
	ConfigurationError  error
	configPath          string
	bindApprover        func(ports.PermissionApprover)
	unchecked           bool
	retire              func()
}

var runtimeSecretGeneration atomic.Uint64

func (s RuntimeSet) retireSecrets() {
	if s.retire != nil {
		s.retire()
	}
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
	configPath    string
	cli           cli.Options
	workspace     domain.Workspace
	store         *jsonl.Store
	policy        *policyBinding
	activeSession *sessionBinding
	runtimeEvents chan<- agent.RuntimeEvent
	httpClient    *http.Client
}

func (b runtimeBuilder) build(cfg config.Config, current domain.ModelSelection) (RuntimeSet, error) {
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
	secretRegistry := secret.NewRegistry()
	generationID := protocol.RuntimeGenerationID(fmt.Sprintf("runtime-%d", runtimeSecretGeneration.Add(1)))
	admission, err := secretRegistry.Acquire(generationID, values)
	if err != nil {
		return RuntimeSet{}, configurationError(b.configPath, "bind runtime secret admission", err)
	}
	retireOnce := &sync.Once{}
	retire := func() {
		retireOnce.Do(func() {
			_ = secretRegistry.Retire(generationID)
		})
	}
	succeeded := false
	defer func() {
		if !succeeded {
			retire()
			_ = admission.Close()
		}
	}()
	clients := make(map[string]*openaicompat.Client, len(cfg.Profiles))
	credentialEnvs := make(map[string]string, len(cfg.Profiles))
	for name, profile := range cfg.Profiles {
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
			Redact:     admission.String,
			Admission:  admission,
		})
	}
	provider, err := openaicompat.NewRouter(clients)
	if err != nil {
		return RuntimeSet{}, configurationError(b.configPath, "build provider runtime", err)
	}
	outputOptions := output.Options{
		SessionID:        b.activeSession.get(),
		CurrentSessionID: b.activeSession.get,
		Artifacts:        b.store,
		Redact:           admission,
		Admission:        admission,
	}
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
	registry := tools.NewRegistry(
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
	)
	runner := &agent.Runner{
		Provider:     provider,
		Tools:        registry,
		Policy:       b.policy,
		Sessions:     b.store,
		MaxToolCalls: effectiveMaxToolCalls(cfg, b.cli),
		SystemPrompt: systemPrompt,
		Redact:       admission.String,
		Admission:    admission,
		NewDeltaRedactor: func() agent.DeltaRedactor {
			stream, streamErr := admission.RedactionStream()
			if streamErr != nil {
				return secret.New().Stream()
			}
			return stream
		},
	}
	if b.runtimeEvents != nil {
		runner.Sink = func(event agent.RuntimeEvent) { b.runtimeEvents <- event }
	}
	succeeded = true
	return RuntimeSet{
		Runtime:             runner,
		CompactSession:      compactSession(provider, b.store),
		Models:              cfg.Models(),
		DefaultSelection:    cfg.DefaultSelection(),
		CredentialEnvs:      credentialEnvs,
		Credentials:         credentials,
		Redactor:            admission,
		Admission:           admission,
		RuntimeGenerationID: generationID,
		configPath:          b.configPath,
		bindApprover: func(approver ports.PermissionApprover) {
			runner.Approver = approver
		},
		retire: retire,
	}, nil
}

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

func compactSession(provider ports.ModelProvider, store ports.SessionStore) CompactSession {
	return func(ctx context.Context, session domain.Session, replay domain.SessionReplay) error {
		return agent.Compact(ctx, agent.CompactInput{
			Provider:     provider,
			Sessions:     store,
			Session:      session,
			Replay:       replay,
			SystemPrompt: systemPrompt,
		})
	}
}

func configurationError(path, message string, cause error) error {
	if path != "" {
		message = path + ": " + message
	}
	return &domain.TypedError{Kind: domain.ErrorConfigurationInvalid, Message: message, Cause: cause}
}
