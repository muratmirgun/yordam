package app

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/cli"
	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/logging"
	"github.com/muratmirgun/yordam/internal/permission"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
)

const systemPrompt = "You are Yordam, a terminal agent working in the user's current workspace. Be accurate, concise, and explicit about tool effects."

type BootstrapOptions struct {
	ConfigPath string
	Config     config.Config
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

	redactors := secret.NewBinding(secret.New())
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
	store := jsonl.New(dataDir, jsonl.Options{Sanitize: redactors.JSON})
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
	if options.CLI.ModeSet {
		event, appendErr := store.Append(ctx, session.ID, domain.EventModeChanged, domain.ModeChangedPayload{Mode: options.CLI.Mode})
		if appendErr != nil {
			return nil, Snapshot{}, fmt.Errorf("persist CLI mode override: %w", appendErr)
		}
		replay.Events = append(replay.Events, event)
		session.Mode = options.CLI.Mode
	}
	runtimeEvents := make(chan agent.RuntimeEvent, 64)
	activeSession := &sessionBinding{id: session.ID}
	policy := newPolicyBinding(permission.Restore(replay))
	builder := runtimeBuilder{
		configPath:    configPath,
		cli:           options.CLI,
		workspace:     workspace,
		store:         store,
		policy:        policy,
		activeSession: activeSession,
		runtimeEvents: runtimeEvents,
		httpClient:    options.HTTPClient,
	}
	runtimeSet := RuntimeSet{ConfigurationError: configErr, configPath: configPath}
	if configErr == nil {
		candidate, buildErr := builder.build(cfg, session.Selection)
		if buildErr != nil {
			configErr = buildErr
			runtimeSet = RuntimeSet{Models: cfg.Models(), DefaultSelection: cfg.DefaultSelection(), ConfigurationError: buildErr, configPath: configPath}
		} else {
			runtimeSet = candidate
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
					if err := appendBootstrapSelection(ctx, store, &session, &replay, override); err != nil {
						return nil, Snapshot{}, fmt.Errorf("persist CLI model override: %w", err)
					}
				}
			}
			if !slices.Contains(candidate.Models, session.Selection) {
				if err := appendBootstrapSelection(ctx, store, &session, &replay, candidate.DefaultSelection); err != nil {
					return nil, Snapshot{}, fmt.Errorf("persist default model: %w", err)
				}
			}
			redactors.Replace(candidate.Redactor)
			configErr = candidate.Ready(session.Selection)
		}
	}
	replay.Session = session
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
		RuntimeSet:     runtimeSet,
		ReloadRuntime:  reload,
		Redactors:      redactors,
		RuntimeEvents:  runtimeEvents,
		Sessions:       store,
		Session:        session,
		Replay:         replay,
		Workspace:      workspace,
		Policy:         policy,
		RestorePolicy:  policy.restore,
		CommandBuffer:  16,
		EventBuffer:    64,
		Logger:         logger,
		Close:          closeDebugLog,
		SessionChanged: activeSession.set,
	})
	debugLogOwned = false
	if logger != nil {
		if err := logger.Event("bootstrap", map[string]any{
			"workspace": workspace.CanonicalPath,
			"profile":   session.Selection.Profile,
			"model":     session.Selection.Model,
		}); err != nil {
			return nil, Snapshot{}, fmt.Errorf("write debug log: %w", err)
		}
	}

	return application, Snapshot{
		Workspace:          workspace,
		Session:            session,
		Replay:             replay,
		Sessions:           sessions,
		Models:             append([]domain.ModelSelection(nil), runtimeSet.Models...),
		ConfigurationError: configErr,
	}, nil
}

func loadBootstrapConfig(options BootstrapOptions) (config.Config, string, error) {
	if options.ConfigPath != "" {
		cfg, err := config.Load(config.LoadOptions{ConfigPath: options.ConfigPath})
		if err != nil {
			return config.Config{}, options.ConfigPath, configurationError(options.ConfigPath, fmt.Sprintf("configuration is invalid (%v); edit the file and run /reload", err), err)
		}
		return cfg, options.ConfigPath, nil
	}
	if err := options.Config.Validate(); err != nil {
		return config.Config{}, "", configurationError("", fmt.Sprintf("configuration is invalid (%v)", err), err)
	}
	return options.Config, "", nil
}

func appendBootstrapSelection(ctx context.Context, store ports.SessionStore, session *domain.Session, replay *domain.SessionReplay, selection domain.ModelSelection) error {
	event, err := store.Append(ctx, session.ID, domain.EventModelChanged, domain.ModelChangedPayload{Selection: selection})
	if err != nil {
		return err
	}
	replay.Events = append(replay.Events, event)
	session.Selection = selection
	session.LastSeq = event.Seq
	session.UpdatedAt = event.Time
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
		replay, err = store.Load(ctx, options.Session)
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
			replay, err = store.Load(ctx, sessions[0].ID)
			session = replay.Session
		}
	default:
		session, err = store.Create(ctx, workspace, options.Mode, selection)
		if err == nil {
			replay, err = store.Load(ctx, session.ID)
			session = replay.Session
		}
	}
	if err != nil {
		return domain.Session{}, domain.SessionReplay{}, fmt.Errorf("select session: %w", err)
	}
	return session, replay, nil
}

func openDebugLogger(path string, redactor secret.Redacting) (*logging.Logger, func() error, error) {
	if path == "" {
		return nil, nil, nil
	}
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
	return logging.New(file, redactor), file.Close, nil
}

func zeroBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

type policyBinding struct {
	mu      sync.RWMutex
	current *permission.SessionPolicy
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
	p.mu.Unlock()
	return p
}
