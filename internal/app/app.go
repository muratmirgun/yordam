package app

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/secret"
)

type Runtime interface {
	RunTurn(context.Context, agent.RunInput) error
}

type TurnInput func(prompt string) (agent.RunInput, error)
type CompactSession func(context.Context, domain.Session, domain.SessionReplay) error

type MutablePolicy interface {
	SetMode(domain.PermissionMode)
	AcknowledgeAutoShell()
}

type RestorePolicy func(domain.SessionReplay) MutablePolicy

type EventLogger interface {
	Event(string, map[string]any) error
}

type Options struct {
	RuntimeSet       RuntimeSet
	ReloadRuntime    ReloadRuntime
	Redactors        *secret.Binding
	Runtime          Runtime
	Input            TurnInput
	Compact          func(context.Context) error
	CompactSession   CompactSession
	RuntimeEvents    <-chan agent.RuntimeEvent
	Sessions         ports.SessionStore
	Session          domain.Session
	Replay           domain.SessionReplay
	Workspace        domain.Workspace
	Policy           MutablePolicy
	RestorePolicy    RestorePolicy
	ConfiguredModels []domain.ModelSelection
	CommandBuffer    int
	EventBuffer      int
	Logger           EventLogger
	Close            func() error
	SessionChanged   func(string)
}

type App struct {
	runtimeSet       RuntimeSet
	reloadRuntime    ReloadRuntime
	redactors        *secret.Binding
	runtime          Runtime
	input            TurnInput
	compact          func(context.Context) error
	compactSession   CompactSession
	runtimeEvents    <-chan agent.RuntimeEvent
	sessions         ports.SessionStore
	session          domain.Session
	replay           domain.SessionReplay
	replayValid      bool
	workspace        domain.Workspace
	policy           MutablePolicy
	restorePolicy    RestorePolicy
	configuredModels []domain.ModelSelection
	commands         chan Command
	events           chan Event
	logger           EventLogger
	close            func() error
	sessionChanged   func(string)
	emitTurnAccepted bool

	pendingMu  sync.Mutex
	pending    map[string]chan domain.PermissionDecision
	eventMu    sync.Mutex
	eventQueue []Event
	eventWake  chan struct{}
}

type operationResult struct {
	kind       operationKind
	err        error
	runtimeSet RuntimeSet
}

type operationKind string

const (
	operationTurn    operationKind = "turn"
	operationCompact operationKind = "compact"
	operationReload  operationKind = "reload"
)

func New(options Options) *App {
	workspace := options.Workspace
	if workspace == (domain.Workspace{}) {
		workspace = options.Session.Workspace
	}
	runtimeSet := options.RuntimeSet
	emitTurnAccepted := runtimeSet.Runtime != nil || runtimeSet.ConfigurationError != nil
	if runtimeSet.Runtime == nil && (options.Runtime != nil || options.Compact != nil || options.CompactSession != nil || len(options.ConfiguredModels) > 0) {
		runtimeSet = RuntimeSet{
			Runtime:          options.Runtime,
			CompactSession:   options.CompactSession,
			Models:           append([]domain.ModelSelection(nil), options.ConfiguredModels...),
			DefaultSelection: options.Session.Selection,
			unchecked:        true,
		}
	}
	if runtimeSet.CompactSession == nil && options.CompactSession != nil {
		runtimeSet.CompactSession = options.CompactSession
	}
	redactors := options.Redactors
	if redactors == nil {
		redactors = secret.NewBinding(runtimeSet.Redactor)
	}
	application := &App{
		runtimeSet:       runtimeSet,
		reloadRuntime:    options.ReloadRuntime,
		redactors:        redactors,
		runtime:          options.Runtime,
		input:            options.Input,
		compact:          options.Compact,
		compactSession:   options.CompactSession,
		runtimeEvents:    options.RuntimeEvents,
		sessions:         options.Sessions,
		session:          options.Session,
		replay:           options.Replay,
		replayValid:      true,
		workspace:        workspace,
		policy:           options.Policy,
		restorePolicy:    options.RestorePolicy,
		configuredModels: append([]domain.ModelSelection(nil), options.ConfiguredModels...),
		commands:         make(chan Command, options.CommandBuffer),
		events:           make(chan Event, options.EventBuffer),
		logger:           options.Logger,
		close:            options.Close,
		sessionChanged:   options.SessionChanged,
		emitTurnAccepted: emitTurnAccepted,
		pending:          make(map[string]chan domain.PermissionDecision),
		eventWake:        make(chan struct{}, 1),
	}
	runtimeSet.BindApprover(application)
	return application
}

func (a *App) Commands() chan<- Command {
	return a.commands
}

func (a *App) Events() <-chan Event {
	return a.events
}

func (a *App) Run(ctx context.Context) error {
	publisherCtx, stopPublisher := context.WithCancel(context.Background())
	publisherDone := make(chan struct{})
	go func() {
		defer close(publisherDone)
		a.publishEvents(publisherCtx)
	}()
	defer func() {
		stopPublisher()
		<-publisherDone
	}()
	if a.close != nil {
		defer func() { _ = a.close() }()
	}
	done := make(chan operationResult, 1)
	runtimeEvents := a.runtimeEvents
	var activeCancel context.CancelFunc
	var activeOperation operationKind
	var queuedMode domain.PermissionMode
	var queuedSelection domain.ModelSelection
	ctxDone := ctx.Done()
	shutdownRequested := false
	var shutdownErr error

	for {
		select {
		case <-ctxDone:
			if activeCancel != nil {
				activeCancel()
			}
			a.clearPending()
			shutdownRequested = true
			shutdownErr = ctx.Err()
			ctxDone = nil
			if activeOperation == "" {
				return shutdownErr
			}
		case runtimeEvent, ok := <-runtimeEvents:
			if !ok {
				runtimeEvents = nil
				continue
			}
			if shutdownRequested {
				continue
			}
			if event, ok := translateRuntimeEvent(runtimeEvent); ok {
				if !a.publish(ctx, event) {
					if activeCancel != nil {
						activeCancel()
					}
					a.clearPending()
					shutdownRequested = true
					shutdownErr = ctx.Err()
					ctxDone = nil
				}
			}
		case command := <-a.commands:
			switch command.Kind {
			case CommandStartTurn:
				if !a.replayValid {
					a.publish(ctx, Event{Kind: EventRejected, Message: "session context is unavailable until replay reload succeeds", Draft: command.Prompt})
					continue
				}
				if activeOperation != "" {
					a.publish(ctx, Event{Kind: EventRejected, Message: "a turn is already active", Draft: command.Prompt})
					continue
				}
				activeSet := a.runtimeSet
				if err := activeSet.Ready(a.session.Selection); err != nil {
					a.publish(ctx, Event{Kind: EventError, Err: err, Message: err.Error(), Draft: command.Prompt})
					continue
				}
				input := agent.RunInput{Session: a.session, Replay: a.replay, Prompt: command.Prompt}
				if a.input != nil {
					var err error
					input, err = a.input(command.Prompt)
					if err != nil {
						a.publish(ctx, Event{Kind: EventError, Err: err, Message: err.Error()})
						continue
					}
				}
				if activeSet.Runtime == nil {
					err := errors.New("runtime is not configured")
					a.publish(ctx, Event{Kind: EventError, Err: err, Message: err.Error(), Draft: command.Prompt})
					continue
				}
				if a.emitTurnAccepted {
					a.publish(ctx, Event{Kind: EventTurnAccepted, Draft: command.Prompt})
				}
				turnCtx, cancel := context.WithCancel(ctx)
				activeCancel = cancel
				activeOperation = operationTurn
				go func() {
					done <- operationResult{kind: operationTurn, err: activeSet.Runtime.RunTurn(turnCtx, input)}
				}()
			case CommandCancelTurn:
				if activeCancel != nil {
					activeCancel()
					a.clearPending()
				}
			case CommandResolvePermission:
				if !a.resolvePermission(command.CallID, command.Decision) {
					a.publish(ctx, Event{Kind: EventRejected, Message: fmt.Sprintf("permission call %q is stale", command.CallID)})
				}
			case CommandCompact:
				if !a.replayValid {
					a.publish(ctx, Event{Kind: EventRejected, Message: "session context is unavailable until replay reload succeeds"})
					continue
				}
				if activeOperation != "" {
					a.publish(ctx, Event{Kind: EventRejected, Message: "an operation is already active"})
					continue
				}
				activeSet := a.runtimeSet
				if err := activeSet.Ready(a.session.Selection); err != nil {
					a.publish(ctx, Event{Kind: EventError, Err: err, Message: err.Error()})
					continue
				}
				if a.compact == nil && activeSet.CompactSession == nil {
					err := errors.New("compaction is not configured")
					a.publish(ctx, Event{Kind: EventError, Err: err, Message: err.Error()})
					continue
				}
				compactCtx, cancel := context.WithCancel(ctx)
				activeCancel = cancel
				activeOperation = operationCompact
				session, replay := a.session, a.replay
				go func() {
					var err error
					if activeSet.CompactSession != nil {
						err = activeSet.CompactSession(compactCtx, session, replay)
					} else {
						err = a.compact(compactCtx)
					}
					done <- operationResult{kind: operationCompact, err: err}
				}()
			case CommandReloadConfig:
				if activeOperation != "" {
					a.publish(ctx, Event{Kind: EventRejected, Message: "an operation is already active"})
					continue
				}
				if a.reloadRuntime == nil {
					err := errors.New("configuration reload is not available")
					a.publish(ctx, Event{Kind: EventReloadCompleted, Err: err, Message: err.Error()})
					continue
				}
				reloadCtx, cancel := context.WithCancel(ctx)
				activeCancel = cancel
				activeOperation = operationReload
				selection := a.session.Selection
				go func() {
					candidate, err := a.reloadRuntime(reloadCtx, selection)
					done <- operationResult{kind: operationReload, err: err, runtimeSet: candidate}
				}()
			case CommandChangeMode:
				if err := command.Mode.Validate(); err != nil {
					a.publish(ctx, Event{Kind: EventRejected, Message: err.Error()})
					continue
				}
				if activeOperation == operationTurn {
					queuedMode = command.Mode
					continue
				}
				a.applyMode(ctx, command.Mode)
			case CommandChangeModel:
				if !a.modelConfigured(command.Selection) {
					a.publish(ctx, Event{Kind: EventRejected, Message: fmt.Sprintf("model %q is not configured for profile %q", command.Selection.Model, command.Selection.Profile)})
					continue
				}
				if activeOperation == operationReload {
					a.publish(ctx, Event{Kind: EventRejected, Message: "an operation is already active"})
					continue
				}
				if activeOperation == operationTurn {
					queuedSelection = command.Selection
					continue
				}
				a.applyModel(ctx, command.Selection)
			case CommandAcknowledgeAutoShell:
				a.acknowledgeAutoShell(ctx, command.CallID, command.Decision)
			case CommandNewSession:
				if activeOperation != "" {
					a.publish(ctx, Event{Kind: EventRejected, Message: "cannot change sessions while an operation is active"})
					continue
				}
				a.newSession(ctx)
			case CommandOpenSession:
				if activeOperation != "" {
					a.publish(ctx, Event{Kind: EventRejected, Message: "cannot change sessions while an operation is active"})
					continue
				}
				a.openSession(ctx, command.SessionID)
			case CommandShutdown:
				shutdownRequested = true
				if activeCancel != nil {
					activeCancel()
				}
				a.clearPending()
				if activeOperation == "" {
					return nil
				}
			}
		case result := <-done:
			if activeCancel != nil {
				activeCancel()
			}
			activeCancel = nil
			activeOperation = ""
			if shutdownRequested {
				return shutdownErr
			}
			if result.kind == operationReload {
				a.completeReload(ctx, result)
				continue
			}
			a.drainRuntimeEvents(ctx, runtimeEvents)
			event := Event{Kind: EventTurnCompleted}
			if result.err != nil {
				event.Err = result.err
				event.Message = result.err.Error()
				if errors.Is(result.err, context.Canceled) || errors.Is(result.err, context.DeadlineExceeded) {
					event.Kind = EventTurnInterrupted
				} else {
					event.Kind = EventError
				}
			}
			if a.sessions != nil && a.session.ID != "" && (result.kind == operationTurn || result.err == nil) {
				refreshed, err := a.sessions.Load(ctx, a.session.ID)
				if err != nil {
					a.replayValid = false
					event = Event{Kind: EventError, Err: err, Message: err.Error()}
				} else {
					a.replayValid = true
					a.replay = refreshed
					a.session = refreshed.Session
					state := ProjectSessionState(refreshed)
					a.session.Mode = state.Mode
					a.session.Selection = state.Selection
					event.Replay = refreshed
					event.Session = a.session
				}
			}
			if !a.publish(ctx, event) {
				if !shutdownRequested {
					return ctx.Err()
				}
			}
			if result.kind == operationTurn {
				if queuedMode != "" {
					a.applyMode(ctx, queuedMode)
					queuedMode = ""
				}
				if queuedSelection != (domain.ModelSelection{}) {
					a.applyModel(ctx, queuedSelection)
					queuedSelection = domain.ModelSelection{}
				}
			}
		}
	}
}

func (a *App) drainRuntimeEvents(ctx context.Context, runtimeEvents <-chan agent.RuntimeEvent) {
	for runtimeEvents != nil {
		select {
		case runtimeEvent, ok := <-runtimeEvents:
			if !ok {
				return
			}
			if event, ok := translateRuntimeEvent(runtimeEvent); ok {
				a.publish(ctx, event)
			}
		default:
			return
		}
	}
}

func (a *App) completeReload(ctx context.Context, result operationResult) {
	if result.err != nil {
		a.publish(ctx, Event{Kind: EventReloadCompleted, Err: result.err, Message: result.err.Error()})
		return
	}
	candidate := result.runtimeSet
	selection := a.session.Selection
	if !slices.Contains(candidate.Models, selection) {
		selection = candidate.DefaultSelection
		if selection == (domain.ModelSelection{}) {
			err := configurationError(candidate.configPath, "configuration has no default model", nil)
			a.publish(ctx, Event{Kind: EventReloadCompleted, Err: err, Message: err.Error()})
			return
		}
		if a.sessions != nil && a.session.ID != "" {
			event, err := a.sessions.Append(ctx, a.session.ID, domain.EventModelChanged, domain.ModelChangedPayload{Selection: selection})
			if err != nil {
				a.publish(ctx, Event{Kind: EventReloadCompleted, Err: err, Message: err.Error()})
				return
			}
			a.replay.Events = append(a.replay.Events, event)
			a.session.LastSeq = event.Seq
			a.session.UpdatedAt = event.Time
		}
		a.session.Selection = selection
		a.replay.Session = a.session
	}
	candidate.BindApprover(a)
	a.runtimeSet = candidate
	if a.redactors != nil {
		a.redactors.Replace(candidate.Redactor)
	}
	event := Event{
		Kind:      EventReloadCompleted,
		Applied:   true,
		Message:   "configuration reloaded",
		Models:    append([]domain.ModelSelection(nil), candidate.Models...),
		Selection: selection,
	}
	if readyErr := candidate.Ready(selection); readyErr != nil {
		event.Err = readyErr
	}
	a.publish(ctx, event)
}

func (a *App) applyMode(ctx context.Context, mode domain.PermissionMode) bool {
	if a.sessions == nil || a.session.ID == "" {
		a.publish(ctx, Event{Kind: EventError, Message: "session store is not configured", NonTerminal: true})
		return false
	}
	event, err := a.sessions.Append(ctx, a.session.ID, domain.EventModeChanged, domain.ModeChangedPayload{Mode: mode})
	if err != nil {
		a.publish(ctx, Event{Kind: EventError, Err: err, Message: err.Error(), NonTerminal: true})
		return false
	}
	a.replay.Events = append(a.replay.Events, event)
	a.session.Mode = mode
	a.session.LastSeq = event.Seq
	a.session.UpdatedAt = event.Time
	if a.policy != nil {
		a.policy.SetMode(mode)
	}
	return a.publish(ctx, a.settingEvent())
}

func (a *App) applyModel(ctx context.Context, selection domain.ModelSelection) bool {
	if a.sessions == nil || a.session.ID == "" {
		a.publish(ctx, Event{Kind: EventError, Message: "session store is not configured", NonTerminal: true})
		return false
	}
	event, err := a.sessions.Append(ctx, a.session.ID, domain.EventModelChanged, domain.ModelChangedPayload{Selection: selection})
	if err != nil {
		a.publish(ctx, Event{Kind: EventError, Err: err, Message: err.Error(), NonTerminal: true})
		return false
	}
	a.replay.Events = append(a.replay.Events, event)
	a.session.Selection = selection
	a.session.LastSeq = event.Seq
	a.session.UpdatedAt = event.Time
	return a.publish(ctx, a.settingEvent())
}

func (a *App) acknowledgeAutoShell(ctx context.Context, callID string, decision domain.PermissionDecision) bool {
	if a.session.Mode != domain.ModeAuto {
		return a.publish(ctx, Event{Kind: EventRejected, Message: "trusted shell acknowledgement requires auto mode"})
	}
	if a.sessions == nil || a.session.ID == "" {
		return a.publish(ctx, Event{Kind: EventError, Message: "session store is not configured", NonTerminal: true})
	}
	event, err := a.sessions.Append(ctx, a.session.ID, domain.EventTrustedExecutionAcknowledged, domain.TrustedExecutionPayload{Enabled: true})
	if err != nil {
		return a.publish(ctx, Event{Kind: EventError, Err: err, Message: err.Error(), NonTerminal: true})
	}
	a.replay.Events = append(a.replay.Events, event)
	a.session.LastSeq = event.Seq
	a.session.UpdatedAt = event.Time
	if a.policy != nil {
		a.policy.AcknowledgeAutoShell()
	}
	if callID != "" && !a.resolvePermission(callID, decision) {
		return a.publish(ctx, Event{Kind: EventRejected, Message: fmt.Sprintf("permission call %q is stale", callID)})
	}
	return a.publish(ctx, a.settingEvent())
}

func (a *App) newSession(ctx context.Context) bool {
	if a.sessions == nil {
		return a.publish(ctx, Event{Kind: EventError, Message: "session store is not configured", NonTerminal: true})
	}
	selection := a.session.Selection
	if !a.selectionConfigured(selection) && a.selectionConfigured(a.runtimeSet.DefaultSelection) {
		selection = a.runtimeSet.DefaultSelection
	}
	session, err := a.sessions.Create(ctx, a.workspace, a.session.Mode, selection)
	if err != nil {
		return a.publish(ctx, Event{Kind: EventError, Err: err, Message: err.Error(), NonTerminal: true})
	}
	return a.openSession(ctx, session.ID)
}

func (a *App) openSession(ctx context.Context, sessionID string) bool {
	if a.sessions == nil {
		return a.publish(ctx, Event{Kind: EventError, Message: "session store is not configured", NonTerminal: true})
	}
	if sessionID == "" {
		return a.publish(ctx, Event{Kind: EventRejected, Message: "session ID is empty"})
	}
	replay, err := a.sessions.Load(ctx, sessionID)
	if err != nil {
		return a.publish(ctx, Event{Kind: EventError, Err: err, Message: err.Error(), NonTerminal: true})
	}
	if replay.Session.Workspace.ID != a.workspace.ID || replay.Session.Workspace.CanonicalPath != a.workspace.CanonicalPath {
		return a.publish(ctx, Event{Kind: EventRejected, Message: "session belongs to a different workspace"})
	}
	state := ProjectSessionState(replay)
	replay.Session.Mode = state.Mode
	replay.Session.Selection = state.Selection
	if !a.selectionConfigured(replay.Session.Selection) && a.selectionConfigured(a.runtimeSet.DefaultSelection) {
		selection := a.runtimeSet.DefaultSelection
		event, err := a.sessions.Append(ctx, replay.Session.ID, domain.EventModelChanged, domain.ModelChangedPayload{Selection: selection})
		if err != nil {
			return a.publish(ctx, Event{Kind: EventError, Err: err, Message: err.Error(), NonTerminal: true})
		}
		replay.Events = append(replay.Events, event)
		replay.Session.Selection = selection
		replay.Session.LastSeq = event.Seq
		replay.Session.UpdatedAt = event.Time
	}
	var policy MutablePolicy
	if a.restorePolicy != nil {
		policy = a.restorePolicy(replay)
	} else if a.policy != nil {
		return a.publish(ctx, Event{Kind: EventError, Message: "session policy restoration is not configured", NonTerminal: true})
	}
	a.session = replay.Session
	a.replay = replay
	a.replayValid = true
	if a.sessionChanged != nil {
		a.sessionChanged(a.session.ID)
	}
	if policy != nil {
		a.policy = policy
	}
	return a.publish(ctx, a.stateEvent())
}

func (a *App) modelConfigured(selection domain.ModelSelection) bool {
	return slices.Contains(a.runtimeSet.Models, selection) || slices.Contains(a.configuredModels, selection)
}

func (a *App) selectionConfigured(selection domain.ModelSelection) bool {
	if a.runtimeSet.unchecked && len(a.runtimeSet.Models) == 0 && len(a.configuredModels) == 0 {
		return true
	}
	return a.modelConfigured(selection)
}

func (a *App) stateEvent() Event {
	return Event{
		Kind:      EventState,
		Mode:      a.session.Mode,
		Selection: a.session.Selection,
		Session:   a.session,
		Replay:    a.replay,
	}
}

func (a *App) settingEvent() Event {
	return Event{
		Kind:      EventState,
		Mode:      a.session.Mode,
		Selection: a.session.Selection,
	}
}

func (a *App) Resolve(ctx context.Context, prompt ports.PermissionPrompt) (domain.PermissionDecision, error) {
	callID := prompt.Call.Request.CallID
	decision := make(chan domain.PermissionDecision, 1)

	a.pendingMu.Lock()
	if _, exists := a.pending[callID]; exists {
		a.pendingMu.Unlock()
		return domain.PermissionDecision{}, fmt.Errorf("permission call %q is already pending", callID)
	}
	a.pending[callID] = decision
	a.pendingMu.Unlock()
	defer a.removePending(callID, decision)

	if !a.publish(ctx, Event{Kind: EventPermissionRequested, Permission: &prompt}) {
		return domain.PermissionDecision{}, ctx.Err()
	}
	select {
	case resolved := <-decision:
		return resolved, nil
	case <-ctx.Done():
		return domain.PermissionDecision{}, ctx.Err()
	}
}

func (a *App) resolvePermission(callID string, decision domain.PermissionDecision) bool {
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()

	pending, exists := a.pending[callID]
	if !exists {
		return false
	}
	select {
	case pending <- decision:
		delete(a.pending, callID)
		return true
	default:
		return false
	}
}

func (a *App) removePending(callID string, decision chan domain.PermissionDecision) {
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	if a.pending[callID] == decision {
		delete(a.pending, callID)
	}
}

func (a *App) clearPending() {
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	clear(a.pending)
}

func (a *App) publish(ctx context.Context, event Event) bool {
	if a.logger != nil {
		_ = a.logger.Event("app_event", map[string]any{
			"kind":    event.Kind,
			"message": event.Message,
		})
	}
	if ctx.Err() != nil {
		return false
	}
	a.eventMu.Lock()
	a.eventQueue = append(a.eventQueue, event)
	a.eventMu.Unlock()
	select {
	case a.eventWake <- struct{}{}:
	default:
	}
	return true
}

func (a *App) publishEvents(ctx context.Context) {
	for {
		a.eventMu.Lock()
		if len(a.eventQueue) == 0 {
			a.eventMu.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-a.eventWake:
			}
			continue
		}
		event := a.eventQueue[0]
		a.eventMu.Unlock()

		select {
		case <-ctx.Done():
			return
		case a.events <- event:
			a.eventMu.Lock()
			a.eventQueue[0] = Event{}
			a.eventQueue = a.eventQueue[1:]
			a.eventMu.Unlock()
		}
	}
}

func translateRuntimeEvent(runtimeEvent agent.RuntimeEvent) (Event, bool) {
	event := Event{Runtime: runtimeEvent}
	switch runtimeEvent.Kind {
	case agent.RuntimeStateChanged:
		event.Kind = EventState
	case agent.RuntimeTextDelta:
		event.Kind = EventTextDelta
	case agent.RuntimeToolStarted:
		event.Kind = EventToolStarted
	case agent.RuntimeToolOutput:
		event.Kind = EventToolOutput
	case agent.RuntimeToolCompleted:
		event.Kind = EventToolCompleted
	default:
		return Event{}, false
	}
	return event, true
}
