package app

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/orchestrator"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
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

type sessionChangeService interface {
	CommitSessionChange(context.Context, orchestrator.SessionChangeRequest) (protocol.CommandResult, error)
}

type sessionStore interface {
	Create(context.Context, domain.Workspace, domain.PermissionMode, domain.ModelSelection) (domain.Session, error)
	List(context.Context, domain.Workspace) ([]domain.SessionSummary, error)
}

type EventLogger interface {
	Event(string, map[string]any) error
}

type Options struct {
	RuntimeSet    RuntimeSet
	ReloadRuntime ReloadRuntime
	Redactors     *secret.Binding
	Input         TurnInput
	Compact       func(context.Context) error
	RuntimeEvents <-chan agent.RuntimeEvent
	Sessions      sessionStore
	Session       domain.Session
	Replay        domain.SessionReplay
	Workspace     domain.Workspace
	Policy        MutablePolicy
	RestorePolicy RestorePolicy
	CommandBuffer int
	EventBuffer   int
	// LegacyQueueCapacity bounds the compatibility TUI consumer. The versioned
	// broker owns all reconnectable production subscriptions.
	LegacyQueueCapacity      int
	NonReconnectableOverflow func()
	Logger                   EventLogger
	Close                    func() error
	SessionChanged           func(string)
	WorkspaceControl         protocol.JournalRef
}

type App struct {
	runtimeSet       RuntimeSet
	reloadRuntime    ReloadRuntime
	redactors        *secret.Binding
	input            TurnInput
	compact          func(context.Context) error
	runtimeEvents    <-chan agent.RuntimeEvent
	sessions         sessionStore
	session          domain.Session
	replay           domain.SessionReplay
	replayValid      bool
	workspace        domain.Workspace
	policy           MutablePolicy
	restorePolicy    RestorePolicy
	commands         chan Command
	events           chan Event
	logger           EventLogger
	close            func() error
	sessionChanged   func(string)
	workspaceControl protocol.JournalRef
	sessionChanges   sessionChangeService

	pendingMu              sync.Mutex
	pending                map[string]*pendingPermission
	pendingInternal        map[string]*pendingPermission
	permissionCallSequence uint64
	eventMu                sync.Mutex
	legacyQueue            []Event
	legacyQueueCapacity    int
	overflowOnce           sync.Once
	nonReconnectableCancel func()
	eventWake              chan struct{}
}

type pendingPermission struct {
	internalKey     string
	displayedCallID string
	decision        chan domain.PermissionDecision
	internalScope   string
	displayedScope  string
}

type permissionResolution uint8

const (
	permissionResolved permissionResolution = iota
	permissionStale
	permissionInvalidScope
)

var errPermissionCorrelationUnavailable = errors.New("permission correlation unavailable")

type operationResult struct {
	kind            operationKind
	err             error
	runtimeSet      RuntimeSet
	throughProtocol bool
	compactTerminal *Event
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
	redactors := options.Redactors
	if redactors == nil {
		redactors = secret.NewBinding(secret.New())
	}
	legacyQueueCapacity := options.LegacyQueueCapacity
	if legacyQueueCapacity <= 0 {
		legacyQueueCapacity = 256
	}
	application := &App{
		runtimeSet:             runtimeSet,
		reloadRuntime:          options.ReloadRuntime,
		redactors:              redactors,
		input:                  options.Input,
		compact:                options.Compact,
		runtimeEvents:          options.RuntimeEvents,
		sessions:               options.Sessions,
		session:                options.Session,
		replay:                 options.Replay,
		replayValid:            true,
		workspace:              workspace,
		policy:                 options.Policy,
		restorePolicy:          options.RestorePolicy,
		commands:               make(chan Command, options.CommandBuffer),
		events:                 make(chan Event, options.EventBuffer),
		logger:                 options.Logger,
		close:                  options.Close,
		sessionChanged:         options.SessionChanged,
		workspaceControl:       options.WorkspaceControl,
		sessionChanges:         runtimeSet.SessionChanges,
		legacyQueueCapacity:    legacyQueueCapacity,
		nonReconnectableCancel: options.NonReconnectableOverflow,
		pending:                make(map[string]*pendingPermission),
		pendingInternal:        make(map[string]*pendingPermission),
		eventWake:              make(chan struct{}, 1),
	}
	if application.sessionChanges == nil {
		application.sessionChanges, _ = options.Sessions.(sessionChangeService)
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
	defer func() { a.runtimeSet.retireSecrets() }()
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
	protocolEvents := make(chan Event, 64)
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
		case event := <-protocolEvents:
			if event.Kind != "" && !a.publish(ctx, event) {
				if activeCancel != nil {
					activeCancel()
				}
				a.clearPending()
				shutdownRequested = true
				shutdownErr = ctx.Err()
				ctxDone = nil
			}
		case command := <-a.commands:
			switch command.Kind {
			case CommandStartTurn:
				if !a.replayValid {
					a.publish(ctx, Event{Kind: EventRejected, DraftID: command.DraftID, Message: "session context is unavailable until replay reload succeeds", Draft: command.Prompt})
					continue
				}
				if activeOperation != "" {
					a.publish(ctx, Event{Kind: EventRejected, DraftID: command.DraftID, Message: "a turn is already active", Draft: command.Prompt})
					continue
				}
				activeSet := a.runtimeSet
				if err := activeSet.Ready(a.session.Selection); err != nil {
					a.publish(ctx, Event{Kind: EventError, DraftID: command.DraftID, Err: err, Message: err.Error(), Draft: command.Prompt})
					continue
				}
				turnCtx, cancel := context.WithCancel(ctx)
				activeCancel = cancel
				activeOperation = operationTurn
				if activeSet.ApplicationService != nil && activeSet.LegacyAdapter != nil {
					applicationCommand, err := activeSet.LegacyAdapter.Command(command)
					if err != nil {
						cancel()
						activeCancel, activeOperation = nil, ""
						a.publish(ctx, Event{Kind: EventError, DraftID: command.DraftID, Err: err, Message: err.Error(), Draft: command.Prompt})
						continue
					}
					snapshot, subscription, err := activeSet.ApplicationService.SnapshotAndSubscribe(ctx, protocol.SnapshotRequest{
						ProtocolVersion: protocol.ApplicationProtocolVersion, SelectedSessionID: protocol.SessionID(a.session.ID), Consumer: "legacy_tui", QueueCapacity: 256,
					})
					if err != nil {
						cancel()
						activeCancel, activeOperation = nil, ""
						a.publish(ctx, Event{Kind: EventError, DraftID: command.DraftID, Err: err, Message: err.Error(), Draft: command.Prompt})
						continue
					}
					contextState, contextErr := DurableContext(snapshot)
					if contextErr != nil {
						_ = subscription.Close()
						cancel()
						activeCancel, activeOperation = nil, ""
						a.publish(ctx, Event{Kind: EventError, Err: contextErr, Message: "invalid durable context", NonTerminal: true})
						continue
					}
					if contextState != nil {
						a.publish(ctx, Event{Kind: EventState, Context: contextState})
					}
					// The operation result owns successful turn completion so it can
					// clear activeOperation before publishing the sole terminal event.
					go consumeLegacyProtocolEvents(ctx, subscription, activeSet.LegacyAdapter, protocolEvents, true, nil)
					go func() {
						result, executeErr := activeSet.ApplicationService.Execute(turnCtx, applicationCommand)
						if executeErr == nil && result.Error != nil {
							executeErr = errors.New(result.Error.Message)
						}
						done <- operationResult{kind: operationTurn, err: executeErr, throughProtocol: true}
					}()
					continue
				}
				input := agent.RunInput{Session: a.session, Replay: a.replay, Prompt: command.Prompt}
				if a.input != nil {
					var err error
					input, err = a.input(command.Prompt)
					if err != nil {
						cancel()
						activeCancel, activeOperation = nil, ""
						a.publish(ctx, Event{Kind: EventError, DraftID: command.DraftID, Err: err, Message: err.Error(), Draft: command.Prompt})
						continue
					}
				}
				if activeSet.Orchestrator != nil {
					metadata, head, err := a.prepareTurnCommand(ctx, command.Prompt)
					if err != nil {
						cancel()
						activeCancel, activeOperation = nil, ""
						a.publish(ctx, Event{Kind: EventError, DraftID: command.DraftID, Err: err, Message: err.Error(), Draft: command.Prompt})
						continue
					}
					input.Command, input.ExpectedHead = metadata, head
				}
				if activeSet.Runtime == nil {
					cancel()
					activeCancel, activeOperation = nil, ""
					err := errors.New("runtime is not configured")
					a.publish(ctx, Event{Kind: EventError, DraftID: command.DraftID, Err: err, Message: err.Error(), Draft: command.Prompt})
					continue
				}
				a.publish(ctx, Event{Kind: EventTurnAccepted, DraftID: command.DraftID, Draft: command.Prompt})
				go func() {
					done <- operationResult{kind: operationTurn, err: activeSet.Runtime.RunTurn(turnCtx, input)}
				}()
			case CommandCancelTurn:
				if activeCancel != nil {
					activeCancel()
					a.clearPending()
				}
			case CommandResolvePermission:
				switch a.resolvePermission(command.CallID, command.Decision) {
				case permissionStale:
					a.publish(ctx, Event{Kind: EventRejected, Message: fmt.Sprintf("permission call %q is stale", command.CallID)})
				case permissionInvalidScope:
					a.publish(ctx, Event{Kind: EventRejected, Message: fmt.Sprintf("permission call %q response is invalid", command.CallID)})
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
				if activeSet.ApplicationService != nil && activeSet.LegacyAdapter != nil {
					applicationCommand, err := activeSet.LegacyAdapter.Command(command)
					if err != nil {
						cancel()
						activeCancel, activeOperation = nil, ""
						a.publish(ctx, Event{Kind: EventError, Err: err, Message: err.Error()})
						continue
					}
					snapshot, subscription, err := activeSet.ApplicationService.SnapshotAndSubscribe(compactCtx, protocol.SnapshotRequest{
						ProtocolVersion: protocol.ApplicationProtocolVersion, SelectedSessionID: protocol.SessionID(a.session.ID), Consumer: "legacy_tui", QueueCapacity: 256,
					})
					if err != nil {
						cancel()
						activeCancel, activeOperation = nil, ""
						a.publish(ctx, Event{Kind: EventError, Err: err, Message: err.Error()})
						continue
					}
					contextState, contextErr := DurableContext(snapshot)
					if contextErr != nil {
						_ = subscription.Close()
						cancel()
						activeCancel, activeOperation = nil, ""
						a.publish(ctx, Event{Kind: EventError, Err: contextErr, Message: "invalid durable context", NonTerminal: true})
						continue
					}
					if contextState != nil {
						a.publish(ctx, Event{Kind: EventState, Context: contextState})
					}
					compactTerminals := make(chan Event, 1)
					go consumeLegacyProtocolEvents(compactCtx, subscription, activeSet.LegacyAdapter, protocolEvents, true, compactTerminals)
					go func() {
						result, executeErr := activeSet.ApplicationService.Execute(compactCtx, applicationCommand)
						awaitTerminal := executeErr == nil && result.Error == nil
						var terminal *Event
						if executeErr == nil && result.Error != nil {
							terminal = serializableCompactionFailure(*result.Error)
							executeErr = errors.New(result.Error.Message)
						}
						if awaitTerminal {
							select {
							case event, ok := <-compactTerminals:
								if ok {
									terminal = &event
								} else {
									terminal = &Event{Kind: EventCompactionFailed, Message: "context compaction lifecycle ended unexpectedly", Compaction: &protocol.CompactionEventV1{Trigger: "manual", Stage: protocol.CompactionFailed, Usage: unknownCompactionUsage(), Error: &protocol.PublicError{Code: "compaction_failed", Message: "context compaction lifecycle ended unexpectedly"}}}
									executeErr = errors.New(terminal.Message)
								}
							case <-compactCtx.Done():
							}
						}
						cancel()
						_ = subscription.Close()
						done <- operationResult{kind: operationCompact, err: executeErr, compactTerminal: terminal}
					}()
					continue
				}
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
			case CommandTrustSkillCatalog:
				if activeOperation != "" {
					a.publish(ctx, Event{Kind: EventRejected, Message: "an operation is already active"})
					continue
				}
				if a.runtimeSet.LegacyAdapter == nil || a.runtimeSet.ApplicationService == nil || a.reloadRuntime == nil {
					a.publish(ctx, Event{Kind: EventRejected, Message: "skill trust reload is not available"})
					continue
				}
				reloadCtx, cancel := context.WithCancel(ctx)
				activeCancel = cancel
				activeOperation = operationReload
				selection := a.session.Selection
				activeSet := a.runtimeSet
				go func() {
					protocolCommand, err := activeSet.LegacyAdapter.Command(command)
					if err == nil {
						result, executeErr := activeSet.ApplicationService.Execute(reloadCtx, protocolCommand)
						if executeErr != nil {
							err = executeErr
						} else if result.Status != "completed" {
							if result.Error != nil {
								err = fmt.Errorf("skill trust command failed: %s", result.Error.Message)
							} else {
								err = fmt.Errorf("skill trust command status %q", result.Status)
							}
						}
					}
					if err != nil {
						done <- operationResult{kind: operationReload, err: err}
						return
					}
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
			if result.kind == operationCompact && result.compactTerminal != nil {
				event = *result.compactTerminal
			}
			if result.err != nil && result.compactTerminal == nil {
				event.Err = result.err
				event.Message = result.err.Error()
				if errors.Is(result.err, context.Canceled) || errors.Is(result.err, context.DeadlineExceeded) {
					event.Kind = EventTurnInterrupted
				} else {
					event.Kind = EventError
				}
			}
			if a.sessions != nil && a.session.ID != "" && (result.kind == operationTurn || result.err == nil || result.compactTerminal != nil) {
				refreshed, err := a.inspectSession(ctx, a.session.ID)
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
			if event.Kind != "" && !a.publish(ctx, event) {
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

func serializableCompactionFailure(public protocol.PublicError) *Event {
	stage := protocol.CompactionFailed
	if public.Code == "cancelled" || public.Code == "compaction_interrupted" {
		stage = protocol.CompactionCancelled
	} else if public.Code == "commit_uncertain" {
		stage = protocol.CompactionUncertain
	}
	return &Event{Kind: EventCompactionFailed, Message: public.Message, Code: public.Code, Compaction: &protocol.CompactionEventV1{Trigger: "manual", Stage: stage, Usage: unknownCompactionUsage(), Error: protocol.DeepCopy(&public)}}
}

type sessionHeadReader interface {
	Head(context.Context, protocol.JournalRef) (protocol.CommittedCursor, error)
}

type sessionInspector interface {
	InspectSession(context.Context, protocol.SessionID) (journal.SessionInspection, error)
}

type legacyReplayReader interface {
	Replay(context.Context, string) (domain.SessionReplay, error)
}

func (a *App) inspectSession(ctx context.Context, sessionID string) (domain.SessionReplay, error) {
	inspector, ok := a.sessions.(sessionInspector)
	if ok {
		inspection, err := inspector.InspectSession(ctx, protocol.SessionID(sessionID))
		if err != nil {
			return domain.SessionReplay{}, err
		}
		return LegacyReplayFromInspection(inspection), nil
	}
	if reader, legacy := a.sessions.(legacyReplayReader); legacy && a.runtimeSet.Orchestrator == nil {
		return reader.Replay(ctx, sessionID)
	}
	return domain.SessionReplay{}, fmt.Errorf("pure session inspector is not configured")
}

func (a *App) prepareTurnCommand(ctx context.Context, prompt string) (orchestrator.CommandMetadata, protocol.CommittedCursor, error) {
	reader, ok := a.sessions.(sessionHeadReader)
	if !ok {
		return orchestrator.CommandMetadata{}, protocol.CommittedCursor{}, fmt.Errorf("session journal head reader is not configured")
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(a.session.ID)}
	head, err := reader.Head(ctx, ref)
	if err != nil {
		return orchestrator.CommandMetadata{}, protocol.CommittedCursor{}, err
	}
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return orchestrator.CommandMetadata{}, protocol.CommittedCursor{}, err
	}
	id := fmt.Sprintf("legacy-turn-%x", raw)
	digest, err := canonicaljson.Digest(struct {
		SessionID string `json:"session_id"`
		Prompt    string `json:"prompt"`
	}{a.session.ID, prompt})
	if err != nil {
		return orchestrator.CommandMetadata{}, protocol.CommittedCursor{}, err
	}
	return orchestrator.CommandMetadata{
		CommandID: protocol.CommandID(id), IdempotencyKey: id, RequestDigest: digest,
		Actor: protocol.ActorRef{ID: "legacy-user", Kind: protocol.ActorUser},
	}, head, nil
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
			candidate.retireSecrets()
			return
		}
		if a.sessions != nil && a.session.ID != "" {
			changeService := candidate.SessionChanges
			if changeService == nil {
				changeService = a.sessionChanges
			}
			if err := a.commitSettingUsing(ctx, candidate, changeService, protocol.EventModelChanged, protocol.ModelChangedV1{ProviderID: protocol.ProviderID(selection.Profile), ModelID: protocol.ModelID(selection.Model)}); err != nil {
				a.publish(ctx, Event{Kind: EventReloadCompleted, Err: err, Message: err.Error()})
				candidate.retireSecrets()
				return
			}
		}
		a.session.Selection = selection
		a.replay.Session = a.session
	}
	if candidate.Orchestrator != nil && a.workspaceControl != (protocol.JournalRef{}) {
		if err := a.activateRuntimeGeneration(ctx, candidate); err != nil {
			a.publish(ctx, Event{Kind: EventReloadCompleted, Err: err, Message: err.Error()})
			candidate.retireSecrets()
			return
		}
	}
	candidate.BindApprover(a)
	previous := a.runtimeSet
	a.runtimeSet = candidate
	if candidate.SessionChanges != nil {
		a.sessionChanges = candidate.SessionChanges
	}
	if a.redactors != nil {
		a.redactors.Replace(candidate.Redactor)
	}
	previous.retireSecrets()
	skills := candidate.SkillSnapshot()
	event := Event{
		Kind:      EventReloadCompleted,
		Applied:   true,
		Message:   "configuration reloaded",
		Models:    append([]domain.ModelSelection(nil), candidate.Models...),
		Selection: selection,
		Skills:    &skills,
	}
	if readyErr := candidate.Ready(selection); readyErr != nil {
		event.Err = readyErr
		event.NonTerminal = true
	}
	a.publish(ctx, event)
}

func (a *App) activateRuntimeGeneration(ctx context.Context, candidate RuntimeSet) error {
	reader, ok := a.sessions.(sessionHeadReader)
	if !ok {
		return fmt.Errorf("workspace-control head reader is not configured")
	}
	head, err := reader.Head(ctx, a.workspaceControl)
	if err != nil {
		return err
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return err
	}
	identity := fmt.Sprintf("reload-%x", random)
	actor := protocol.ActorRef{ID: "legacy-user", Kind: protocol.ActorUser}
	requestDigest, err := canonicaljson.Digest(struct {
		Manifest protocol.RuntimeGenerationManifest `json:"manifest"`
		Previous protocol.RuntimeGenerationID       `json:"previous"`
	}{candidate.Manifest, a.runtimeSet.RuntimeGenerationID})
	if err != nil {
		return err
	}
	descriptorDigest, err := canonicaljson.Digest(struct {
		Name string `json:"name"`
	}{"runtime.reload"})
	if err != nil {
		return err
	}
	body := protocol.ActionPlanBody{
		CallID: identity, Tool: protocol.ToolIdentity{Source: "runtime", Authority: "yordam", Name: "reload"}, SourceRevision: "runtime-v1",
		DescriptorDigest: descriptorDigest, Action: "runtime.reload", Purpose: "activate runtime generation",
		Resources:      []protocol.ResourceTarget{{Kind: "runtime_generation", CanonicalID: string(candidate.RuntimeGenerationID)}},
		ExecutionLocus: "runtime", Effect: "mutation", Boundary: "process", Reversibility: "exact", VerificationCoverage: "exact",
		RequestedProfile: "restricted", EffectiveProfile: "restricted", RuntimeGenerationID: candidate.RuntimeGenerationID,
	}
	planDigest, err := canonicaljson.Digest(body)
	if err != nil {
		return err
	}
	payload, err := canonicaljson.Marshal(protocol.RuntimeGenerationActivatedV1{Manifest: protocol.DeepCopy(candidate.Manifest), Previous: a.runtimeSet.RuntimeGenerationID})
	if err != nil {
		return err
	}
	eventActor := actor
	result, err := candidate.Orchestrator.RunControl(ctx, orchestrator.ControlRequest{
		Command:     orchestrator.CommandMetadata{CommandID: protocol.CommandID(identity), IdempotencyKey: identity, RequestDigest: requestDigest, Actor: actor},
		OperationID: protocol.ControlOperationID(identity), Kind: orchestrator.OperationReloadActivation, Journal: a.workspaceControl, ExpectedHead: head,
		TransactionID: protocol.TransactionID(identity), Runtime: protocol.DeepCopy(candidate.Manifest), Plan: protocol.ActionPlan{Body: body, Digest: planDigest},
		Event: protocol.ProposedEvent{EventID: protocol.EventID(identity), Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventRuntimeGenerationActivated, Actor: &eventActor, RuntimeGenerationID: candidate.RuntimeGenerationID, Payload: payload},
	})
	if err != nil {
		return err
	}
	if result.Status != "completed" {
		return fmt.Errorf("runtime activation status %q", result.Status)
	}
	return nil
}

func (a *App) applyMode(ctx context.Context, mode domain.PermissionMode) bool {
	if a.sessions == nil || a.session.ID == "" {
		a.publish(ctx, Event{Kind: EventError, Message: "session store is not configured", NonTerminal: true})
		return false
	}
	if err := a.commitSessionChange(ctx, protocol.EventModeChanged, protocol.ModeChangedV1{Mode: string(mode)}); err != nil {
		a.publish(ctx, Event{Kind: EventError, Err: err, Message: err.Error(), NonTerminal: true})
		return false
	}
	a.session.Mode = mode
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
	if err := a.commitSessionChange(ctx, protocol.EventModelChanged, protocol.ModelChangedV1{ProviderID: protocol.ProviderID(selection.Profile), ModelID: protocol.ModelID(selection.Model)}); err != nil {
		a.publish(ctx, Event{Kind: EventError, Err: err, Message: err.Error(), NonTerminal: true})
		return false
	}
	a.session.Selection = selection
	return a.publish(ctx, a.settingEvent())
}

func (a *App) acknowledgeAutoShell(ctx context.Context, callID string, decision domain.PermissionDecision) bool {
	if a.session.Mode != domain.ModeAuto {
		return a.publish(ctx, Event{Kind: EventRejected, Message: "trusted shell acknowledgement requires auto mode"})
	}
	if a.sessions == nil || a.session.ID == "" {
		return a.publish(ctx, Event{Kind: EventError, Message: "session store is not configured", NonTerminal: true})
	}
	if callID != "" {
		switch a.validatePermissionResponse(callID, decision) {
		case permissionStale:
			return a.publish(ctx, Event{Kind: EventRejected, Message: fmt.Sprintf("permission call %q is stale", callID)})
		case permissionInvalidScope:
			return a.publish(ctx, Event{Kind: EventRejected, Message: fmt.Sprintf("permission call %q response is invalid", callID)})
		}
	}
	if err := a.commitSessionChange(ctx, protocol.EventTrustedExecutionAcknowledged, protocol.TrustedExecutionAcknowledgedV1{Enabled: true, Profile: "unsandboxed"}); err != nil {
		return a.publish(ctx, Event{Kind: EventError, Err: err, Message: err.Error(), NonTerminal: true})
	}
	if a.policy != nil {
		a.policy.AcknowledgeAutoShell()
	}
	if callID != "" {
		switch a.resolvePermission(callID, decision) {
		case permissionStale:
			return a.publish(ctx, Event{Kind: EventRejected, Message: fmt.Sprintf("permission call %q is stale", callID)})
		case permissionInvalidScope:
			return a.publish(ctx, Event{Kind: EventRejected, Message: fmt.Sprintf("permission call %q response is invalid", callID)})
		}
	}
	return a.publish(ctx, a.settingEvent())
}

func (a *App) commitSessionChange(ctx context.Context, kind string, payload any) error {
	return a.commitSettingUsing(ctx, a.runtimeSet, a.sessionChanges, kind, payload)
}

func (a *App) commitSettingUsing(ctx context.Context, runtime RuntimeSet, fallback sessionChangeService, kind string, payload any) error {
	if runtime.ApplicationService != nil && runtime.LegacyAdapter != nil {
		var command Command
		switch value := payload.(type) {
		case protocol.ModeChangedV1:
			command = Command{Kind: CommandChangeMode, Mode: domain.PermissionMode(value.Mode)}
		case protocol.ModelChangedV1:
			command = Command{Kind: CommandChangeModel, Selection: domain.ModelSelection{Profile: string(value.ProviderID), Model: string(value.ModelID)}}
		case protocol.TrustedExecutionAcknowledgedV1:
			command = Command{Kind: CommandAcknowledgeAutoShell}
		default:
			return fmt.Errorf("unsupported application setting payload %T", payload)
		}
		applicationCommand, err := runtime.LegacyAdapter.Command(command)
		if err != nil {
			return err
		}
		result, err := runtime.ApplicationService.Execute(ctx, applicationCommand)
		if err != nil {
			return err
		}
		if result.Error != nil {
			return fmt.Errorf("%s: %s", result.Error.Code, result.Error.Message)
		}
		if result.Status != "completed" || result.Cursor.SelectedSession == nil {
			return fmt.Errorf("setting command returned status %q without a selected-session cursor", result.Status)
		}
		a.session.LastSeq = result.Cursor.SelectedSession.CommitSeq
		a.session.UpdatedAt = time.Now().UTC()
		updated, err := a.inspectSession(ctx, a.session.ID)
		if err != nil {
			return err
		}
		projected := ProjectSessionState(updated)
		updated.Session.Mode = projected.Mode
		updated.Session.Selection = projected.Selection
		a.replay = updated
		a.replayValid = true
		return nil
	}
	return a.commitSessionChangeCompatibility(ctx, fallback, runtime.RuntimeGenerationID, kind, payload)
}

func (a *App) commitSessionChangeCompatibility(ctx context.Context, service sessionChangeService, generation protocol.RuntimeGenerationID, kind string, payload any) error {
	if service == nil {
		return fmt.Errorf("session change orchestrator is not configured")
	}
	metadata, head, err := a.prepareTurnCommand(ctx, kind)
	if err != nil {
		return err
	}
	raw, err := canonicaljson.Marshal(payload)
	if err != nil {
		return err
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return err
	}
	identity := fmt.Sprintf("session-change-%x", random)
	actor := protocol.DeepCopy(metadata.Actor)
	result, err := service.CommitSessionChange(ctx, orchestrator.SessionChangeRequest{
		Command: metadata, OperationID: protocol.ControlOperationID(identity),
		Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(a.session.ID)}, SessionID: protocol.SessionID(a.session.ID),
		ExpectedHead: head, TransactionID: protocol.TransactionID(identity), RuntimeGenerationID: generation,
		Consequential: true,
		Event: protocol.ProposedEvent{
			EventID: protocol.EventID(identity), Time: time.Now().UTC(), PayloadVersion: 1, Kind: kind, SessionID: protocol.SessionID(a.session.ID),
			Actor: &actor, RuntimeGenerationID: generation, Payload: raw,
		},
	})
	if err != nil {
		return err
	}
	if result.Cursor.SelectedSession == nil {
		return fmt.Errorf("session change returned no session cursor")
	}
	a.session.LastSeq = result.Cursor.SelectedSession.CommitSeq
	a.session.UpdatedAt = time.Now().UTC()
	updated, err := a.inspectSession(ctx, a.session.ID)
	if err != nil {
		return err
	}
	projected := ProjectSessionState(updated)
	updated.Session.Mode = projected.Mode
	updated.Session.Selection = projected.Selection
	a.replay = updated
	a.replayValid = true
	return nil
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
	replay, err := a.inspectSession(ctx, sessionID)
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
		previousSession := a.session
		a.session = replay.Session
		if err := a.commitSessionChange(ctx, protocol.EventModelChanged, protocol.ModelChangedV1{ProviderID: protocol.ProviderID(selection.Profile), ModelID: protocol.ModelID(selection.Model)}); err != nil {
			a.session = previousSession
			return a.publish(ctx, Event{Kind: EventError, Err: err, Message: err.Error(), NonTerminal: true})
		}
		replay.Session.Selection = selection
		replay.Session.LastSeq = a.session.LastSeq
		replay.Session.UpdatedAt = a.session.UpdatedAt
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
	event := a.stateEvent()
	if a.runtimeSet.ApplicationService != nil {
		snapshot, snapshotErr := a.runtimeSet.ApplicationService.Snapshot(ctx, protocol.SnapshotRequest{ProtocolVersion: protocol.ApplicationProtocolVersion, SelectedSessionID: protocol.SessionID(a.session.ID), Consumer: "session_context", QueueCapacity: 1})
		if snapshotErr != nil {
			return a.publish(ctx, Event{Kind: EventError, Err: snapshotErr, Message: "refresh durable context", NonTerminal: true})
		}
		contextState, contextErr := DurableContext(snapshot)
		if contextErr != nil {
			return a.publish(ctx, Event{Kind: EventError, Err: contextErr, Message: "invalid durable context", NonTerminal: true})
		}
		event.Context = contextState
	}
	return a.publish(ctx, event)
}

func (a *App) modelConfigured(selection domain.ModelSelection) bool {
	return slices.Contains(a.runtimeSet.Models, selection)
}

func (a *App) selectionConfigured(selection domain.ModelSelection) bool {
	if a.runtimeSet.unchecked && len(a.runtimeSet.Models) == 0 {
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
	internalCallID := prompt.Call.Request.CallID
	internalKey := permissionPendingKey(prompt.SessionID, internalCallID)
	var redactor secret.Redacting = secret.New()
	if a.redactors != nil {
		redactor = a.redactors.Snapshot()
	}
	published, sanitizeErr := sanitizePublishedEvent(redactor, Event{Kind: EventPermissionRequested, Permission: &prompt})
	if sanitizeErr != nil {
		a.enqueuePublishedEvent(ctx, published)
		if err := ctx.Err(); err != nil {
			return domain.PermissionDecision{}, err
		}
		return domain.PermissionDecision{}, sanitizeErr
	}
	a.pendingMu.Lock()
	if _, exists := a.pendingInternal[internalKey]; exists {
		a.pendingMu.Unlock()
		return domain.PermissionDecision{}, errors.New("duplicate permission call is already pending")
	}
	displayedCallID, err := a.newDisplayedPermissionCallID(redactor)
	if err != nil {
		a.pendingMu.Unlock()
		failure := Event{Kind: EventError, Message: err.Error(), Err: err}
		a.enqueuePublishedEvent(ctx, failure)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return domain.PermissionDecision{}, ctxErr
		}
		return domain.PermissionDecision{}, err
	}
	published = permissionEventWithCallID(published, displayedCallID)
	pending := &pendingPermission{
		internalKey:     internalKey,
		displayedCallID: displayedCallID,
		decision:        make(chan domain.PermissionDecision, 1),
		internalScope:   permissionApprovalScope(prompt.Call),
		displayedScope:  permissionApprovalScope(published.Permission.Call),
	}
	a.pending[displayedCallID] = pending
	a.pendingInternal[internalKey] = pending
	a.pendingMu.Unlock()
	defer a.removePending(pending)

	if !a.enqueuePublishedEvent(ctx, published) {
		return domain.PermissionDecision{}, ctx.Err()
	}
	select {
	case resolved := <-pending.decision:
		return resolved, nil
	case <-ctx.Done():
		return domain.PermissionDecision{}, ctx.Err()
	}
}

func permissionPendingKey(sessionID, callID string) string { return sessionID + "\x00" + callID }

func (a *App) newDisplayedPermissionCallID(redactor secret.Redacting) (string, error) {
	for range 32 {
		if a.permissionCallSequence == ^uint64(0) {
			return "", errPermissionCorrelationUnavailable
		}
		a.permissionCallSequence++
		callID := fmt.Sprintf("permission-%d-%s", a.permissionCallSequence, rand.Text())
		if redactor.String(callID) != callID {
			continue
		}
		return callID, nil
	}
	return "", errPermissionCorrelationUnavailable
}

func permissionEventWithCallID(event Event, callID string) Event {
	prompt := *event.Permission
	call := prompt.Call
	request := call.Request
	request.CallID = callID
	call.Request = request
	if call.FilePlan != nil {
		plan := *call.FilePlan
		plan.CallID = callID
		plan.ArtifactIDs = append([]string(nil), plan.ArtifactIDs...)
		call.FilePlan = &plan
	}
	prompt.Call = call
	event.Permission = &prompt
	return event
}

func permissionApprovalScope(request domain.PreparedToolRequest) string {
	if request.ApprovalScope != "" {
		return request.ApprovalScope
	}
	return request.CanonicalScope
}

func (a *App) validatePermissionResponse(callID string, decision domain.PermissionDecision) permissionResolution {
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()

	pending, exists := a.pending[callID]
	if !exists {
		return permissionStale
	}
	if decision.Scope != pending.displayedScope {
		return permissionInvalidScope
	}
	return permissionResolved
}

func (a *App) resolvePermission(callID string, decision domain.PermissionDecision) permissionResolution {
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()

	pending, exists := a.pending[callID]
	if !exists {
		return permissionStale
	}
	if decision.Scope != pending.displayedScope {
		return permissionInvalidScope
	}
	decision.Scope = pending.internalScope
	select {
	case pending.decision <- decision:
		delete(a.pending, callID)
		delete(a.pendingInternal, pending.internalKey)
		return permissionResolved
	default:
		return permissionStale
	}
}

func (a *App) removePending(pending *pendingPermission) {
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	if a.pending[pending.displayedCallID] == pending {
		delete(a.pending, pending.displayedCallID)
	}
	if a.pendingInternal[pending.internalKey] == pending {
		delete(a.pendingInternal, pending.internalKey)
	}
}

func (a *App) clearPending() {
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	clear(a.pending)
	clear(a.pendingInternal)
}

func (a *App) publish(ctx context.Context, event Event) bool {
	event, _ = a.preparePublishedEvent(event)
	return a.enqueuePublishedEvent(ctx, event)
}

func (a *App) preparePublishedEvent(event Event) (Event, error) {
	if a.redactors != nil {
		return sanitizePublishedEvent(a.redactors.Snapshot(), event)
	}
	return event, nil
}

func (a *App) enqueuePublishedEvent(ctx context.Context, event Event) bool {
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
	limit := a.legacyQueueCapacity
	if legacyTerminalEvent(event.Kind) {
		limit++ // one reserved terminal item
	}
	if len(a.legacyQueue) >= limit {
		a.eventMu.Unlock()
		a.overflowOnce.Do(func() {
			if a.nonReconnectableCancel != nil {
				a.nonReconnectableCancel()
			}
		})
		return false
	}
	a.legacyQueue = append(a.legacyQueue, event)
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
		if len(a.legacyQueue) == 0 {
			a.eventMu.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-a.eventWake:
			}
			continue
		}
		event := a.legacyQueue[0]
		a.eventMu.Unlock()

		select {
		case <-ctx.Done():
			return
		case a.events <- event:
			a.eventMu.Lock()
			a.legacyQueue[0] = Event{}
			a.legacyQueue = a.legacyQueue[1:]
			a.eventMu.Unlock()
		}
	}
}

func legacyTerminalEvent(kind EventKind) bool {
	switch kind {
	case EventTurnCompleted, EventTurnInterrupted, EventReloadCompleted, EventError, EventRejected:
		return true
	default:
		return false
	}
}

func consumeLegacyProtocolEvents(ctx context.Context, subscription Subscription, adapter *LegacyAdapter, destination chan<- Event, suppressTerminal bool, compactTerminals chan<- Event) {
	defer subscription.Close()
	if compactTerminals != nil {
		defer close(compactTerminals)
	}
	for {
		item, err := subscription.Next(ctx)
		if err != nil {
			if compactTerminals != nil {
				return
			}
			if ctx.Err() == nil {
				select {
				case destination <- Event{Kind: EventError, Err: err, Message: err.Error()}:
				case <-ctx.Done():
				}
			}
			return
		}
		if item.Terminal != nil {
			if compactTerminals != nil {
				return
			}
			err := errors.New(item.Terminal.Message)
			select {
			case destination <- Event{Kind: EventError, Code: item.Terminal.Code, Err: err, Message: err.Error()}:
			case <-ctx.Done():
			}
			return
		}
		if item.Event == nil {
			continue
		}
		// The compatibility TUI receives the admitted execution error from the
		// command result path. Publishing the durable generic turn failure first
		// would make legacy consumers stop before that redacted diagnostic arrives.
		if item.Event.Kind == protocol.EventTurnFailed {
			return
		}
		event, err := adapter.Event(*item.Event)
		if err != nil {
			if compactTerminals != nil {
				return
			}
			select {
			case destination <- Event{Kind: EventError, Err: err, Message: err.Error()}:
			case <-ctx.Done():
			}
			return
		}
		if event.Kind == "" {
			continue
		}
		if compactTerminals != nil && (event.Kind == EventCompactionCompleted || event.Kind == EventCompactionFailed) {
			select {
			case compactTerminals <- event:
			case <-ctx.Done():
			}
			return
		}
		// Automatic compaction is nested inside an active turn. Its lifecycle
		// terminal updates durable context but must not make the TUI idle before
		// the owning turn result arrives.
		if compactTerminals == nil && event.Compaction != nil && event.Compaction.Trigger == "automatic" && (event.Kind == EventCompactionCompleted || event.Kind == EventCompactionFailed) {
			if event.Context != nil {
				select {
				case destination <- Event{Kind: EventState, Context: event.Context}:
				case <-ctx.Done():
				}
			}
			continue
		}
		if suppressTerminal && legacyTerminalEvent(event.Kind) {
			return
		}
		select {
		case destination <- event:
		case <-ctx.Done():
			return
		}
		if legacyTerminalEvent(event.Kind) {
			return
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
