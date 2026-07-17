package app

import (
	"context"
	"fmt"
	"net/http"
	"slices"

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/cli"
	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
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
	Runtime            Runtime
	CompactSession     CompactSession
	Models             []domain.ModelSelection
	DefaultSelection   domain.ModelSelection
	CredentialEnvs     map[string]string
	Credentials        map[string]string
	Redactor           secret.Redactor
	ConfigurationError error
	configPath         string
	bindApprover       func(ports.PermissionApprover)
	unchecked          bool
}

type ReloadRuntime func(context.Context, domain.ModelSelection) (RuntimeSet, error)

func (s RuntimeSet) Ready(selection domain.ModelSelection) error {
	if s.unchecked {
		return nil
	}
	if s.ConfigurationError != nil {
		return s.ConfigurationError
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
	redactor := secret.New(append(redactionValues, selected.APIKey)...)
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
			Redact:     redactor.String,
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
		Redact:           redactor,
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
		Provider:         provider,
		Tools:            registry,
		Policy:           b.policy,
		Sessions:         b.store,
		MaxToolCalls:     effectiveMaxToolCalls(cfg, b.cli),
		SystemPrompt:     systemPrompt,
		Redact:           redactor.String,
		NewDeltaRedactor: func() agent.DeltaRedactor { return redactor.Stream() },
	}
	if b.runtimeEvents != nil {
		runner.Sink = func(event agent.RuntimeEvent) { b.runtimeEvents <- event }
	}
	return RuntimeSet{
		Runtime:          runner,
		CompactSession:   compactSession(provider, b.store),
		Models:           cfg.Models(),
		DefaultSelection: cfg.DefaultSelection(),
		CredentialEnvs:   credentialEnvs,
		Credentials:      credentials,
		Redactor:         redactor,
		configPath:       b.configPath,
		bindApprover: func(approver ports.PermissionApprover) {
			runner.Approver = approver
		},
	}, nil
}

func configurationError(path, message string, cause error) error {
	if path != "" {
		message = path + ": " + message
	}
	return &domain.TypedError{Kind: domain.ErrorConfigurationInvalid, Message: message, Cause: cause}
}
