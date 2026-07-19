package tui

import (
	"charm.land/bubbles/v2/spinner"
	tea "charm.land/bubbletea/v2"
	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/tui/components"
)

type Screen string

const (
	ScreenConversation Screen = "conversation"
	ScreenSessions     Screen = "sessions"
	ScreenHelp         Screen = "help"
	ScreenMode         Screen = "mode"
	ScreenModel        Screen = "model"
	ScreenSkills       Screen = "skills"
)

type Layout string

const (
	LayoutStream      Layout = "stream"
	LayoutSplit       Layout = "split"
	LayoutContextOnly Layout = "context_only"
)

type ModalKind string

type turnProgress string

const (
	ModalNone           ModalKind = ""
	ModalPermission     ModalKind = "permission"
	ModalCommandPalette ModalKind = "command_palette"
	ModalConfirmExit    ModalKind = "confirm_exit"
)

const (
	progressIdle      turnProgress = ""
	progressWaiting   turnProgress = "waiting"
	progressReceiving turnProgress = "receiving"
	progressTool      turnProgress = "tool"
)

const (
	contextBreakpoint = 100
	maxDraftID        = ^uint64(0)
	focusComposer     = "composer"
	focusContext      = "context"
	focusModal        = "modal"
)

type Options struct {
	Commands           chan<- app.Command
	Events             <-chan app.Event
	SessionTitle       string
	Mode               domain.PermissionMode
	Selection          domain.ModelSelection
	CanonicalWorkspace string
	Sessions           []domain.SessionSummary
	Models             []domain.ModelSelection
	Skills             app.SkillSnapshot
}

type Model struct {
	width              int
	height             int
	screen             Screen
	contextOpen        bool
	modal              ModalKind
	turnActive         bool
	turnProgress       turnProgress
	queuedMode         domain.PermissionMode
	queuedSelection    domain.ModelSelection
	sessionTitle       string
	effectiveMode      domain.PermissionMode
	profile            string
	model              string
	canonicalWorkspace string
	commands           chan<- app.Command
	events             <-chan app.Event
	focusedComponent   string
	cancelSent         bool
	pendingCommand     tea.Cmd
	modeCursor         int
	modelCursor        int
	pendingDraft       string
	pendingDraftID     uint64
	nextDraftID        uint64

	conversation    components.Conversation
	composer        components.Composer
	context         components.Context
	permission      components.Permission
	sessions        components.Sessions
	palette         components.Palette
	spinner         spinner.Model
	models          []domain.ModelSelection
	skillSnapshot   app.SkillSnapshot
	displayedSkills app.SkillSnapshot
	skills          components.Skills
}

type appEventMsg struct {
	event app.Event
	ok    bool
}

func NewModel(options Options) Model {
	title := options.SessionTitle
	if title == "" {
		title = "New session"
	}
	mode := options.Mode
	if mode == "" {
		mode = domain.ModeAsk
	}
	profile := options.Selection.Profile
	if profile == "" {
		profile = "default"
	}
	modelName := options.Selection.Model
	if modelName == "" {
		modelName = "unconfigured"
	}
	workspace := options.CanonicalWorkspace
	if workspace == "" {
		workspace = "."
	}

	model := Model{
		screen:             ScreenConversation,
		sessionTitle:       title,
		effectiveMode:      mode,
		profile:            profile,
		model:              modelName,
		canonicalWorkspace: workspace,
		commands:           options.Commands,
		events:             options.Events,
		focusedComponent:   focusComposer,
		conversation:       components.NewConversation(),
		composer:           components.NewComposer(nil),
		context:            components.NewContext(),
		sessions:           components.NewSessions(options.Sessions),
		palette:            components.NewPalette(),
		spinner:            spinner.New(spinner.WithSpinner(spinner.MiniDot)),
		models:             append([]domain.ModelSelection(nil), options.Models...),
		skillSnapshot:      options.Skills.Clone(),
		nextDraftID:        1,
	}
	model.skills = components.NewSkills(skillScreenOptions(model.skillSnapshot, false, false))
	for index, configured := range model.models {
		if configured == options.Selection {
			model.modelCursor = index
			break
		}
	}
	switch mode {
	case domain.ModeSafe:
		model.modeCursor = 0
	case domain.ModeAuto:
		model.modeCursor = 2
	default:
		model.modeCursor = 1
	}
	return model
}

func (model Model) Init() tea.Cmd {
	return model.waitForEvent()
}

func (model Model) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	updated := model.updateMessage(message)
	command := updated.pendingCommand
	updated.pendingCommand = nil
	if event, ok := message.(appEventMsg); ok && event.ok {
		return updated, tea.Batch(command, updated.waitForEvent())
	}
	return updated, command
}

func (model Model) layout() Layout {
	if !model.contextOpen {
		return LayoutStream
	}
	if model.width < contextBreakpoint {
		return LayoutContextOnly
	}
	return LayoutSplit
}

func (model Model) waitForEvent() tea.Cmd {
	return func() tea.Msg {
		event, ok := <-model.events
		return appEventMsg{event: event, ok: ok}
	}
}
