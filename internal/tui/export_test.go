package tui

import (
	"strconv"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/tui/components"
)

func OptionsForTest() Options {
	commands := make(chan app.Command, 8)
	events := make(chan app.Event, 8)
	return Options{Commands: commands, Events: events}
}

func UpdateForTest(model Model, message tea.Msg) Model { return model.updateMessage(message) }

func OpenContextForTest(model Model) Model {
	model.contextOpen = true
	model.context.OpenPanel()
	return model
}

func (model Model) LayoutForTest() string { return string(model.layout()) }

func ModelWithModalContextAndActiveTurnForTest() Model {
	model := NewModel(OptionsForTest())
	model.modal = ModalPermission
	model.contextOpen = true
	model.turnActive = true
	return model
}

func PressForTest(model Model, key string) Model { return model.handleKey(key) }

func (model Model) HasModalForTest() bool { return model.modal != ModalNone }

func (model Model) ContextOpenForTest() bool { return model.contextOpen }

func (model Model) ContextExpandedForTest() bool { return model.context.Expanded() }

func (model Model) TurnActiveForTest() bool { return model.turnActive }

func (model Model) CancelSentForTest() bool { return model.cancelSent }

func ModelAndCommandsForTest() (Model, <-chan app.Command) {
	commands := make(chan app.Command, 8)
	events := make(chan app.Event, 8)
	return NewModel(Options{Commands: commands, Events: events}), commands
}

func NavigationModelForTest() (Model, <-chan app.Command) {
	commands := make(chan app.Command, 16)
	events := make(chan app.Event, 8)
	return NewModel(Options{
		Commands: commands,
		Events:   events,
		Mode:     domain.ModeAsk,
		Selection: domain.ModelSelection{
			Profile: "primary",
			Model:   "model-a",
		},
		Sessions: []domain.SessionSummary{
			{ID: "older", Title: "Older", UpdatedAt: time.Unix(1, 0)},
			{ID: "newer", Title: "Newer", UpdatedAt: time.Unix(2, 0)},
		},
		Models: []domain.ModelSelection{
			{Profile: "primary", Model: "model-a"},
			{Profile: "primary", Model: "model-b"},
		},
	}), commands
}

func ModelAndEventsForTest() (Model, chan<- app.Event) {
	commands := make(chan app.Command, 8)
	events := make(chan app.Event, 8)
	return NewModel(Options{Commands: commands, Events: events}), events
}

func SetTurnActiveForTest(model Model, active bool) Model {
	model.turnActive = active
	model.composer.SetActiveTurn(active)
	return model
}

func SetModalForTest(model Model, modal ModalKind) Model {
	model.modal = modal
	model.focusedComponent = focusModal
	return model
}

func (model Model) ModalForTest() string { return string(model.modal) }

func (model Model) FocusForTest() string { return model.focusedComponent }

func (model Model) ScreenForTest() string { return string(model.screen) }

func SubmitForTest(model Model, value string) Model { return model.routeSubmission(value) }

func SpinnerTickForTest(model Model) tea.Msg { return model.spinner.Tick() }

func NextAppEventForTest(command tea.Cmd) tea.Msg {
	if command == nil {
		return nil
	}
	message := command()
	batch, ok := message.(tea.BatchMsg)
	if !ok {
		return message
	}
	for _, child := range batch {
		if event, ok := child().(appEventMsg); ok {
			return event
		}
	}
	return nil
}

func SetComposerValueForTest(model Model, value string) Model {
	model.composer.SetValue(value)
	return model
}

func (model Model) ComposerValueForTest() string { return model.composer.Value() }

func (model Model) ComposerActiveForTest() bool { return model.composer.ActiveTurn() }

func (model Model) StatusSelectionForTest() domain.ModelSelection {
	return domain.ModelSelection{Profile: model.profile, Model: model.model}
}

func (model Model) ModelCountForTest() int { return len(model.models) }

func (model Model) ConversationBlocksForTest() []components.Block { return model.conversation.Blocks() }

func (model Model) ConversationAtBottomForTest() bool { return model.conversation.AtBottom() }

func (model Model) ComponentWidthsForTest() (int, int) {
	return model.conversation.Width(), model.composer.Width()
}

func (model Model) ComponentHeightsForTest() (int, int) {
	return model.conversation.Height(), model.composer.Height()
}

func ApplyAppEventForTest(model Model, event app.Event) Model { return model.handleAppEvent(event) }

func CommandKindsForTest(commands <-chan app.Command) []app.CommandKind {
	commandsList := CommandsForTest(commands)
	var kinds []app.CommandKind
	for _, command := range commandsList {
		kinds = append(kinds, command.Kind)
	}
	return kinds
}

func CommandsForTest(commands <-chan app.Command) []app.Command {
	var result []app.Command
	for {
		select {
		case command := <-commands:
			result = append(result, command)
		default:
			return result
		}
	}
}

func SetModeForTest(model Model, mode domain.PermissionMode) Model {
	model.effectiveMode = mode
	return model
}

func PermissionPromptForTest(tool, scope string, inside bool) ports.PermissionPrompt {
	return ports.PermissionPrompt{Call: domain.PreparedToolRequest{
		Request:         domain.ToolRequest{CallID: "call-1", Name: tool},
		CanonicalScope:  scope,
		InsideWorkspace: inside,
		Summary:         "summary",
		ProposedDiff:    "proposed diff",
	}}
}

func AppendConversationForTest(model Model, kind components.BlockKind, content string) Model {
	model.conversation.Append(kind, content)
	return model
}

func GoldenModelForTest(width, height int) Model {
	model := NewModel(OptionsForTest())
	model.width = width
	model.height = height
	model.contextOpen = true
	model.sessionTitle = "Command bridge"
	model.effectiveMode = domain.ModeAsk
	model.profile = "local"
	model.model = "gpt-5"
	model.canonicalWorkspace = "/Users/murat/oss/tui-yordam-v0.1/workspaces/command-bridge-demo"
	model.conversation.Append(components.BlockUser, "Inspect internal/app/app.go.")
	model.conversation.AppendAssistantDelta("The command bridge keeps UI and runtime separate.")
	model.conversation.CompleteTool("call-1", "read", components.ToolSucceeded, "internal/app/app.go:1-40", 0, false)
	model.conversation.HandleKey("ctrl+e")
	model.context.ShowDiff("internal/app/app.go", "@@ -1,3 +1,3 @@\n-old bridge\n+typed command bridge")
	model = model.resizeComponents()
	return model
}

func WidthStringForTest(width int) string { return strconv.Itoa(width) }
