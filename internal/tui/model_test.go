package tui_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/tui"
	"github.com/muratmirgun/yordam/internal/tui/components"
	"github.com/muratmirgun/yordam/internal/workspace"
)

func TestAdaptiveContextLayout(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.UpdateForTest(model, tea.WindowSizeMsg{Width: 120, Height: 40})
	model = tui.OpenContextForTest(model)
	if got := model.LayoutForTest(); got != "split" {
		t.Fatalf("layout=%s", got)
	}
	model = tui.UpdateForTest(model, tea.WindowSizeMsg{Width: 80, Height: 30})
	if got := model.LayoutForTest(); got != "context_only" {
		t.Fatalf("layout=%s", got)
	}
}

func TestClosingContextRestoresStreamComponentWidth(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.UpdateForTest(model, tea.WindowSizeMsg{Width: 120, Height: 40})
	fullConversationWidth, fullComposerWidth := model.ComponentWidthsForTest()
	model = tui.PressForTest(model, "ctrl+o")
	conversationWidth, composerWidth := model.ComponentWidthsForTest()
	if conversationWidth >= fullConversationWidth || composerWidth >= fullComposerWidth {
		t.Fatalf("split component widths=%d,%d", conversationWidth, composerWidth)
	}

	model = tui.PressForTest(model, "esc")
	conversationWidth, composerWidth = model.ComponentWidthsForTest()
	if conversationWidth != fullConversationWidth || composerWidth != fullComposerWidth {
		t.Fatalf("restored component widths=%d,%d want=%d,%d", conversationWidth, composerWidth, fullConversationWidth, fullComposerWidth)
	}
}

func TestContextRootEnterTogglesExpansion(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.PressForTest(model, "ctrl+o")
	if !model.ContextOpenForTest() || !model.ContextExpandedForTest() {
		t.Fatal("context did not open expanded")
	}
	model = tui.PressForTest(model, "enter")
	if model.ContextExpandedForTest() {
		t.Fatal("enter did not collapse focused context")
	}
	model = tui.PressForTest(model, "enter")
	if !model.ContextExpandedForTest() {
		t.Fatal("second enter did not expand focused context")
	}
}

func TestNarrowStatusPreservesModeAndModel(t *testing.T) {
	model := tui.GoldenModelForTest(50, 20)
	status := strings.SplitN(model.View().Content, "\n", 2)[0]
	if !strings.Contains(status, "mode: ask") || !strings.Contains(status, "local/gpt-5") {
		t.Fatalf("status hid mode/model: %q", status)
	}
}

func TestConfigurationErrorKeepsComposerFocused(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.ApplyAppEventForTest(model, app.Event{
		Kind: app.EventError,
		Err: &domain.TypedError{
			Kind:    domain.ErrorConfigurationInvalid,
			Message: "edit config and run /reload",
		},
		NonTerminal: true,
	})
	if model.FocusForTest() != "composer" || model.ContextOpenForTest() {
		t.Fatalf("focus=%q context=%t", model.FocusForTest(), model.ContextOpenForTest())
	}
}

func TestEscPriorityModalThenContextThenTurn(t *testing.T) {
	model := tui.ModelWithModalContextAndActiveTurnForTest()
	model = tui.PressForTest(model, "esc")
	if model.HasModalForTest() || !model.ContextOpenForTest() || !model.TurnActiveForTest() {
		t.Fatal("first esc priority wrong")
	}
	if got := model.FocusForTest(); got != "context" {
		t.Fatalf("focus after modal=%q", got)
	}
	model = tui.PressForTest(model, "esc")
	if model.ContextOpenForTest() || !model.TurnActiveForTest() {
		t.Fatal("second esc priority wrong")
	}
	if got := model.FocusForTest(); got != "composer" {
		t.Fatalf("focus after context=%q", got)
	}
	model = tui.PressForTest(model, "esc")
	if !model.CancelSentForTest() {
		t.Fatal("third esc did not cancel")
	}
}

func TestRootGlobalKeys(t *testing.T) {
	tests := []struct {
		name         string
		active       bool
		presses      []string
		wantModal    string
		wantContext  bool
		wantFocus    string
		wantCommands []app.CommandKind
	}{
		{name: "command palette", presses: []string{"ctrl+p"}, wantModal: "command_palette", wantFocus: "modal"},
		{name: "context toggles twice", presses: []string{"ctrl+o", "ctrl+o"}, wantFocus: "composer"},
		{name: "idle exit", presses: []string{"ctrl+c"}, wantFocus: "composer", wantCommands: []app.CommandKind{app.CommandShutdown}},
		{name: "active exit requires confirmation", active: true, presses: []string{"ctrl+c"}, wantModal: "confirm_exit", wantFocus: "modal"},
		{name: "active exit ignores enter", active: true, presses: []string{"ctrl+c", "enter"}, wantModal: "confirm_exit", wantFocus: "modal"},
		{name: "active exit can be declined", active: true, presses: []string{"ctrl+c", "n"}, wantFocus: "composer"},
		{name: "active exit confirms", active: true, presses: []string{"ctrl+c", "y"}, wantFocus: "composer", wantCommands: []app.CommandKind{app.CommandCancelTurn, app.CommandShutdown}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model, commands := tui.ModelAndCommandsForTest()
			model = tui.SetTurnActiveForTest(model, test.active)
			for _, key := range test.presses {
				model = tui.PressForTest(model, key)
			}

			if got := model.ModalForTest(); got != test.wantModal {
				t.Fatalf("modal=%q want=%q", got, test.wantModal)
			}
			if got := model.ContextOpenForTest(); got != test.wantContext {
				t.Fatalf("contextOpen=%t want=%t", got, test.wantContext)
			}
			if got := model.FocusForTest(); got != test.wantFocus {
				t.Fatalf("focus=%q want=%q", got, test.wantFocus)
			}
			if got := tui.CommandKindsForTest(commands); !reflect.DeepEqual(got, test.wantCommands) {
				t.Fatalf("commands=%v want=%v", got, test.wantCommands)
			}
		})
	}
}

func TestModalRoutesBeforeGlobalKeys(t *testing.T) {
	model, commands := tui.ModelAndCommandsForTest()
	model = tui.SetModalForTest(model, tui.ModalPermission)
	model = tui.PressForTest(model, "ctrl+c")
	model = tui.PressForTest(model, "ctrl+o")
	model = tui.PressForTest(model, "ctrl+p")

	if got := model.ModalForTest(); got != "permission" {
		t.Fatalf("modal=%q", got)
	}
	if model.ContextOpenForTest() {
		t.Fatal("global context toggle bypassed modal")
	}
	if got := tui.CommandKindsForTest(commands); len(got) != 0 {
		t.Fatalf("global exit bypassed modal: %v", got)
	}
}

func TestAppEventWaitLoop(t *testing.T) {
	model, events := tui.ModelAndEventsForTest()
	events <- app.Event{Kind: app.EventTextDelta}

	firstWait := model.Init()
	if firstWait == nil {
		t.Fatal("Init returned no app event wait command")
	}
	message := firstWait()
	updated, nextWait := model.Update(message)
	if _, ok := updated.(tui.Model); !ok {
		t.Fatalf("updated model type=%T", updated)
	}
	if nextWait == nil {
		t.Fatal("received event did not schedule the next wait")
	}

	events <- app.Event{Kind: app.EventTurnCompleted}
	message = tui.NextAppEventForTest(nextWait)
	_, nextWait = updated.Update(message)
	if nextWait == nil {
		t.Fatal("second event did not schedule another wait")
	}
}

func TestComposerSubmitStartsTurnAndAppendsUserBlock(t *testing.T) {
	model, commands := tui.ModelAndCommandsForTest()
	model = tui.SetComposerValueForTest(model, "\n  inspect this  \n\n")
	model = tui.UpdateForTest(model, keyMessage("enter"))

	command := <-commands
	if command.Kind != app.CommandStartTurn || command.Prompt != "  inspect this  " {
		t.Fatalf("command=%+v", command)
	}
	if !model.TurnActiveForTest() {
		t.Fatal("submitted turn is not active")
	}
	if got := model.ComposerValueForTest(); got != "" {
		t.Fatalf("composer value=%q", got)
	}
	if blocks := model.ConversationBlocksForTest(); len(blocks) != 0 {
		t.Fatalf("unaccepted blocks=%+v", blocks)
	}
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTurnAccepted, Draft: command.Prompt})
	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 1 || blocks[0].Kind != components.BlockUser || blocks[0].Content != "  inspect this  " {
		t.Fatalf("blocks=%+v", blocks)
	}

	model = tui.SetComposerValueForTest(model, "blocked")
	model = tui.UpdateForTest(model, keyMessage("enter"))
	if got := tui.CommandKindsForTest(commands); len(got) != 0 {
		t.Fatalf("active composer sent commands: %v", got)
	}
	if got := model.ComposerValueForTest(); got != "blocked" {
		t.Fatalf("active composer value=%q", got)
	}
}

func TestConfiguredSecretPromptIsRedactedBeforeConversationRendering(t *testing.T) {
	const configuredSecret = "configured-tui-secret-sentinel"
	runtime := tuiRuntimeFunc(func(context.Context, agent.RunInput) error { return nil })
	selection := domain.ModelSelection{Profile: "profile", Model: "model"}
	application := app.New(app.Options{
		RuntimeSet: app.RuntimeSet{
			Runtime:          runtime,
			Models:           []domain.ModelSelection{selection},
			DefaultSelection: selection,
			Credentials:      map[string]string{selection.Profile: configuredSecret},
			CredentialEnvs:   map[string]string{selection.Profile: "TEST_KEY"},
		},
		Redactors: secret.NewBinding(secret.New(configuredSecret)),
		Session:   domain.Session{Selection: selection},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	events := application.Events()
	model := tui.NewModel(tui.Options{Commands: application.Commands(), Events: events})
	model = tui.SubmitForTest(model, configuredSecret)
	for {
		event := <-events
		model = tui.ApplyAppEventForTest(model, event)
		if event.Kind == app.EventTurnCompleted {
			break
		}
	}
	if strings.Contains(model.View().Content, configuredSecret) {
		t.Fatal("rendered TUI contains configured secret")
	}
	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 1 || blocks[0].Content != "[REDACTED]" {
		t.Fatal("conversation did not contain the redacted prompt")
	}
}

type tuiRuntimeFunc func(context.Context, agent.RunInput) error

func (run tuiRuntimeFunc) RunTurn(ctx context.Context, input agent.RunInput) error {
	return run(ctx, input)
}

func TestRootDelegatesPasteToComposer(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.UpdateForTest(model, tea.PasteMsg{Content: "first\nsecond"})

	if got := model.ComposerValueForTest(); got != "first\nsecond" {
		t.Fatalf("composer value=%q", got)
	}
}

func TestRootComposerUsesOneCompactRowWhenEmpty(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.UpdateForTest(model, tea.WindowSizeMsg{Width: 80, Height: 40})
	view := model.View()

	if !strings.Contains(view.Content, "Ask Yordam") {
		t.Fatalf("view missing composer:\n%s", view.Content)
	}
	if got := len(strings.Split(strings.TrimSuffix(view.Content, "\n"), "\n")); got != 5 {
		t.Fatalf("rendered rows=%d want=5:\n%s", got, view.Content)
	}
}

func TestRootComposerUsesTerminalCursorWithoutEmbeddedANSI(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.UpdateForTest(model, tea.WindowSizeMsg{Width: 80, Height: 20})
	view := model.View()

	if strings.Contains(view.Content, "\x1b[") {
		t.Fatalf("view contains embedded cursor styles: %q", view.Content)
	}
	if view.Cursor == nil {
		t.Fatal("focused composer has no terminal cursor")
	}
	if view.Cursor.Position.X != 2 || view.Cursor.Position.Y != 4 {
		t.Fatalf("composer cursor position=%+v want={X:2 Y:4}", view.Cursor.Position)
	}
}

func TestRootResizesConversationWhenComposerGrows(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.UpdateForTest(model, tea.WindowSizeMsg{Width: 80, Height: 10})
	model = tui.UpdateForTest(model, tea.PasteMsg{Content: "one\ntwo\nthree\nfour"})

	conversationHeight, composerHeight := model.ComponentHeightsForTest()
	if conversationHeight != 3 || composerHeight != 4 {
		t.Fatalf("component heights=%d,%d want=3,4", conversationHeight, composerHeight)
	}
}

func TestAppEventsBuildConversationAndFinishTurn(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventState, Runtime: agent.RuntimeEvent{State: "streaming_model"}})
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTextDelta, Runtime: agent.RuntimeEvent{Text: "hel"}})
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTextDelta, Runtime: agent.RuntimeEvent{Text: "lo"}})
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventToolOutput, Runtime: agent.RuntimeEvent{Progress: &domain.ToolProgress{CallID: "call-1", Text: "first"}}})
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventToolOutput, Runtime: agent.RuntimeEvent{Progress: &domain.ToolProgress{CallID: "call-1", Text: "latest", Truncated: true}}})
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventToolCompleted, Runtime: agent.RuntimeEvent{Result: &domain.ToolResult{CallID: "call-1", Status: domain.ToolSucceeded, Content: "done", Duration: 1500 * time.Millisecond}}})
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTextDelta, Runtime: agent.RuntimeEvent{Text: "after"}})

	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 3 {
		t.Fatalf("blocks=%+v", blocks)
	}
	if block := blocks[0]; block.Kind != components.BlockAssistant || block.Content != "hello" {
		t.Fatalf("assistant block=%+v", block)
	}
	if block := blocks[1]; block.Kind != components.BlockTool || block.CallID != "call-1" || block.Name != "shell" || block.Status != components.ToolSucceeded || block.Content != "done" || block.Duration != 1500*time.Millisecond || block.Truncated {
		t.Fatalf("tool block=%+v", block)
	}
	if block := blocks[2]; block.Kind != components.BlockAssistant || block.Content != "after" || block.ID == blocks[0].ID {
		t.Fatalf("post-tool assistant block=%+v", block)
	}
	if !model.TurnActiveForTest() {
		t.Fatal("state/text events did not activate turn")
	}

	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTurnCompleted})
	if model.TurnActiveForTest() || model.ComposerActiveForTest() {
		t.Fatal("terminal event did not reactivate composer")
	}
}

func TestTurnProgressOnlyShowsTheCurrentActivePhase(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.UpdateForTest(model, tea.WindowSizeMsg{Width: 80, Height: 20})
	if view := model.View().Content; strings.Contains(view, "Waiting for response") || strings.Contains(view, "Receiving response") || strings.Contains(view, "Running tool") {
		t.Fatalf("idle view contains progress:\n%s", view)
	}

	model = tui.SubmitForTest(model, "hello")
	if view := model.View().Content; !strings.Contains(view, "Waiting for response") || strings.Contains(view, "Running tool") {
		t.Fatalf("submitted view has wrong progress:\n%s", view)
	}

	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTextDelta, Runtime: agent.RuntimeEvent{Text: "hello"}})
	if view := model.View().Content; !strings.Contains(view, "Receiving response") || strings.Contains(view, "Running tool") {
		t.Fatalf("streaming view has wrong progress:\n%s", view)
	}

	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventToolStarted, Runtime: agent.RuntimeEvent{Kind: agent.RuntimeToolStarted}})
	if view := model.View().Content; !strings.Contains(view, "Running tool") {
		t.Fatalf("tool view missing progress:\n%s", view)
	}

	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventToolCompleted, Runtime: agent.RuntimeEvent{Result: &domain.ToolResult{CallID: "call-1", Status: domain.ToolSucceeded}}})
	if view := model.View().Content; !strings.Contains(view, "Waiting for response") || strings.Contains(view, "Running tool") {
		t.Fatalf("post-tool view has wrong progress:\n%s", view)
	}

	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTurnCompleted})
	if view := model.View().Content; strings.Contains(view, "Waiting for response") || strings.Contains(view, "Receiving response") || strings.Contains(view, "Running tool") {
		t.Fatalf("completed view contains progress:\n%s", view)
	}
}

func TestActiveTurnProgressAnimates(t *testing.T) {
	model := tui.SubmitForTest(tui.NewModel(tui.OptionsForTest()), "hello")
	before := model.View().Content
	model = tui.UpdateForTest(model, tui.SpinnerTickForTest(model))
	after := model.View().Content
	if before == after || !strings.Contains(after, "Waiting for response") {
		t.Fatalf("spinner did not animate:\nbefore=%q\nafter=%q", before, after)
	}
}

func TestErrorAndInterruptedEventsAppendTerminalBlocks(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.SetTurnActiveForTest(model, true)
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventError, Message: "provider failed"})
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTurnInterrupted, Message: "cancelled"})

	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 2 || blocks[0].Kind != components.BlockError || blocks[1].Kind != components.BlockNotice {
		t.Fatalf("blocks=%+v", blocks)
	}
	if blocks[0].Content != "provider failed" || blocks[1].Content != "cancelled" {
		t.Fatalf("terminal block contents=%+v", blocks)
	}
}

func TestDurableReplayRendersRecoveryReadOnlyToolsAndTerminalState(t *testing.T) {
	replay := domain.SessionReplay{
		Session:      domain.Session{ID: "session", Title: "Recovered", Mode: domain.ModeAsk, Selection: domain.ModelSelection{Profile: "p", Model: "m"}},
		RecoveryNote: "incomplete final line recovered",
		ReadOnly:     true,
		Events: []domain.DurableEvent{
			tuiDurableEvent(1, domain.EventUserMessage, domain.MessagePayload{Content: "inspect"}),
			tuiDurableEvent(2, domain.EventToolRequested, domain.PreparedToolRequest{Request: domain.ToolRequest{CallID: "c1", Name: "read"}}),
			tuiDurableEvent(3, domain.EventToolResult, domain.ToolResultPayload{Result: domain.ToolResult{CallID: "c1", Status: domain.ToolSucceeded, Content: "1: content"}}),
			tuiDurableEvent(4, domain.EventTurnInterrupted, domain.TurnTerminalPayload{Reason: "cancelled", ErrorKind: domain.ErrorCancelled}),
		},
	}
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventState, Session: replay.Session, Replay: replay, Mode: replay.Session.Mode, Selection: replay.Session.Selection})
	view := model.View().Content
	for _, want := range []string{"RECOVERY", "incomplete final line recovered", "READ-ONLY", "TOOL read [completed]", "cancelled"} {
		if !strings.Contains(view, want) {
			t.Fatalf("replay view missing %q:\n%s", want, view)
		}
	}
	if strings.Contains(view, "1: content") {
		t.Fatalf("replayed tool detail should start collapsed:\n%s", view)
	}
}

func TestConversationScrollAndToolExpansionKeys(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.UpdateForTest(model, tea.WindowSizeMsg{Width: 80, Height: 8})
	for index := range 20 {
		model = tui.AppendConversationForTest(model, components.BlockNotice, fmt.Sprintf("notice-%d", index))
	}
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventToolCompleted, Runtime: agent.RuntimeEvent{Result: &domain.ToolResult{CallID: "c1", Status: domain.ToolSucceeded, Content: "detail"}}})
	model = tui.PressForTest(model, "pgup")
	if model.ConversationAtBottomForTest() {
		t.Fatal("page up did not scroll conversation")
	}
	model = tui.PressForTest(model, "ctrl+e")
	blocks := model.ConversationBlocksForTest()
	if blocks[len(blocks)-1].Collapsed {
		t.Fatal("first expansion key did not expand completed tool")
	}
	model = tui.PressForTest(model, "ctrl+e")
	blocks = model.ConversationBlocksForTest()
	if !blocks[len(blocks)-1].Collapsed {
		t.Fatal("second expansion key did not collapse completed tool")
	}
}

func TestTerminalReplayPreservesManualConversationScroll(t *testing.T) {
	replay := domain.SessionReplay{Session: domain.Session{ID: "session"}}
	for index := range 20 {
		replay.Events = append(replay.Events, tuiDurableEvent(uint64(index+1), domain.EventUserMessage, domain.MessagePayload{Content: fmt.Sprintf("message-%d", index)}))
	}
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.UpdateForTest(model, tea.WindowSizeMsg{Width: 80, Height: 8})
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventState, Session: replay.Session, Replay: replay})
	model = tui.PressForTest(model, "pgup")
	if model.ConversationAtBottomForTest() {
		t.Fatal("page up did not move before replay")
	}

	replay.Events = append(replay.Events, tuiDurableEvent(21, domain.EventAssistantMessage, domain.MessagePayload{Content: "latest"}))
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTurnCompleted, Replay: replay})
	if model.ConversationAtBottomForTest() {
		t.Fatal("terminal replay discarded manual scroll position")
	}
}

func TestReplayReplacementPreservesViewportAndAllowsAnyToolToggle(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.UpdateForTest(model, tea.WindowSizeMsg{Width: 80, Height: 8})
	wantWidth, wantComposerWidth := model.ComponentWidthsForTest()
	wantConversationHeight, _ := model.ComponentHeightsForTest()
	replay := domain.SessionReplay{Session: domain.Session{ID: "session"}, Events: []domain.DurableEvent{
		tuiDurableEvent(1, domain.EventToolRequested, domain.PreparedToolRequest{Request: domain.ToolRequest{CallID: "c1", Name: "read"}}),
		tuiDurableEvent(2, domain.EventToolResult, domain.ToolResultPayload{Result: domain.ToolResult{CallID: "c1", Status: domain.ToolSucceeded, Content: "first detail"}}),
		tuiDurableEvent(3, domain.EventToolRequested, domain.PreparedToolRequest{Request: domain.ToolRequest{CallID: "c2", Name: "search"}}),
		tuiDurableEvent(4, domain.EventToolResult, domain.ToolResultPayload{Result: domain.ToolResult{CallID: "c2", Status: domain.ToolSucceeded, Content: "second detail"}}),
	}}
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTurnCompleted, Replay: replay})
	width, composerWidth := model.ComponentWidthsForTest()
	conversationHeight, _ := model.ComponentHeightsForTest()
	if width != wantWidth || composerWidth != wantComposerWidth || conversationHeight != wantConversationHeight {
		t.Fatalf("viewport dimensions=%d,%d,%d want=%d,%d,%d", width, composerWidth, conversationHeight, wantWidth, wantComposerWidth, wantConversationHeight)
	}
	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 2 || !blocks[0].Collapsed || !blocks[1].Collapsed {
		t.Fatalf("replayed tools=%+v", blocks)
	}
	model = tui.PressForTest(model, "alt+up")
	model = tui.PressForTest(model, "ctrl+e")
	blocks = model.ConversationBlocksForTest()
	if blocks[0].Collapsed || !blocks[1].Collapsed {
		t.Fatalf("selected tool toggle=%+v", blocks)
	}
}

func TestConversationNavigationKeyIsNotInsertedIntoComposer(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.UpdateForTest(model, tea.WindowSizeMsg{Width: 80, Height: 8})
	for index := range 20 {
		model = tui.AppendConversationForTest(model, components.BlockNotice, fmt.Sprintf("notice-%d", index))
	}
	model = tui.UpdateForTest(model, tea.KeyPressMsg(tea.Key{Code: tea.KeyPgUp}))
	if model.ConversationAtBottomForTest() {
		t.Fatal("page up did not scroll conversation")
	}
	if got := model.ComposerValueForTest(); got != "" {
		t.Fatalf("navigation key inserted into composer: %q", got)
	}
}

func TestPermissionEscapeSendsExactDenyWithoutCancellingTurn(t *testing.T) {
	model, commands := tui.ModelAndCommandsForTest()
	model = tui.SetTurnActiveForTest(model, true)
	prompt := tui.PermissionPromptForTest("edit", "/workspace/file.go", true)
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventPermissionRequested, Permission: &prompt})

	view := model.View().Content
	for _, want := range []string{"edit", "/workspace/file.go", "inside workspace", "summary", "proposed diff"} {
		if !strings.Contains(view, want) {
			t.Fatalf("permission view missing %q:\n%s", want, view)
		}
	}

	model = tui.PressForTest(model, "esc")
	got := tui.CommandsForTest(commands)
	if len(got) != 1 || got[0].Kind != app.CommandResolvePermission || got[0].CallID != "call-1" {
		t.Fatalf("commands=%+v", got)
	}
	decision := got[0].Decision
	if decision.Action != domain.PermissionDeny || decision.Lifetime != domain.PermissionOnce || decision.Scope != "/workspace/file.go" {
		t.Fatalf("decision=%+v", decision)
	}
	if model.HasModalForTest() || !model.TurnActiveForTest() || model.CancelSentForTest() {
		t.Fatalf("modal=%t active=%t cancelled=%t", model.HasModalForTest(), model.TurnActiveForTest(), model.CancelSentForTest())
	}
}

func TestPermissionEscapeResolvesAppBroker(t *testing.T) {
	const configuredSecret = "tui-permission-internal-scope-secret"
	rawScope := "/workspace/" + configuredSecret + "/file.go"
	displayedScope := "/workspace/[REDACTED]/file.go"
	application := app.New(app.Options{Redactors: secret.NewBinding(secret.New(configuredSecret))})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = application.Run(ctx) }()

	prompt := tui.PermissionPromptForTest("edit", displayedScope, true)
	prompt.Call.ApprovalScope = rawScope
	resolved := make(chan domain.PermissionDecision, 1)
	errors := make(chan error, 1)
	go func() {
		decision, err := application.Resolve(ctx, prompt)
		if err != nil {
			errors <- err
			return
		}
		resolved <- decision
	}()

	var event app.Event
	select {
	case event = <-application.Events():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for permission event")
	}
	if event.Permission == nil || event.Permission.Call.ApprovalScope != displayedScope || strings.Contains(event.Permission.Call.ApprovalScope, configuredSecret) {
		t.Fatal("app did not publish only the displayed approval scope")
	}
	model := tui.NewModel(tui.Options{Commands: application.Commands(), Events: application.Events()})
	model = tui.SetTurnActiveForTest(model, true)
	model = tui.ApplyAppEventForTest(model, event)
	model = tui.PressForTest(model, "esc")

	select {
	case decision := <-resolved:
		if decision.Action != domain.PermissionDeny || decision.Scope != rawScope {
			t.Fatalf("decision=%+v", decision)
		}
	case err := <-errors:
		t.Fatalf("resolve error=%v", err)
	case <-time.After(time.Second):
		t.Fatal("escape left app permission request unresolved")
	}
	if !model.TurnActiveForTest() || model.CancelSentForTest() {
		t.Fatal("permission escape cancelled the turn")
	}
}

func TestPermissionEscapeRoundTripsOpaqueCallIDThroughAppBroker(t *testing.T) {
	const configuredSecret = "tui-permission-call-id-secret"
	rawCallID := "provider-" + configuredSecret
	application := app.New(app.Options{Redactors: secret.NewBinding(secret.New(configuredSecret))})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = application.Run(ctx) }()

	prompt := tui.PermissionPromptForTest("edit", "/workspace/file.go", true)
	prompt.Call.Request.CallID = rawCallID
	resolved := make(chan domain.PermissionDecision, 1)
	errors := make(chan error, 1)
	go func() {
		decision, err := application.Resolve(ctx, prompt)
		if err != nil {
			errors <- err
			return
		}
		resolved <- decision
	}()

	var event app.Event
	select {
	case event = <-application.Events():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for permission event")
	}
	if event.Permission == nil {
		t.Fatal("permission request was not published")
	}
	displayedCallID := event.Permission.Call.Request.CallID
	if displayedCallID == rawCallID || displayedCallID == "provider-[REDACTED]" || strings.Contains(displayedCallID, configuredSecret) {
		t.Fatal("TUI received a raw or derived permission call ID")
	}
	model := tui.NewModel(tui.Options{Commands: application.Commands(), Events: application.Events()})
	model = tui.SetTurnActiveForTest(model, true)
	model = tui.ApplyAppEventForTest(model, event)
	model = tui.PressForTest(model, "esc")

	select {
	case decision := <-resolved:
		if decision.Action != domain.PermissionDeny || prompt.Call.Request.CallID != rawCallID {
			t.Fatal("TUI response did not preserve the runner-owned internal call ID")
		}
	case err := <-errors:
		t.Fatalf("resolve error=%v", err)
	case <-time.After(time.Second):
		t.Fatal("opaque TUI call ID left app permission request unresolved")
	}
}

func TestAutoShellAcceptAcknowledgesBeforeResolution(t *testing.T) {
	model, commands := tui.ModelAndCommandsForTest()
	model = tui.SetModeForTest(model, domain.ModeAuto)
	prompt := tui.PermissionPromptForTest("shell", "/workspace\x00go test ./...", true)
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventPermissionRequested, Permission: &prompt})

	view := model.View().Content
	for _, want := range []string{
		components.AutoShellWarningTitle,
		components.AutoShellSandboxWarning,
		components.AutoShellAccessWarning,
		components.AutoShellEnvironmentWarning,
	} {
		if !strings.Contains(view, want) {
			t.Fatalf("auto shell view missing %q:\n%s", want, view)
		}
	}

	model = tui.PressForTest(model, "y")
	got := tui.CommandsForTest(commands)
	if len(got) != 1 || got[0].Kind != app.CommandAcknowledgeAutoShell || got[0].CallID != "call-1" {
		t.Fatalf("commands=%+v", got)
	}
	if got[0].Decision.Action != domain.PermissionAllow || got[0].Decision.Scope != prompt.Call.CanonicalScope {
		t.Fatalf("resolution=%+v", got[0])
	}
}

func TestToolWorkspaceChangesOpenContext(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventToolCompleted, Runtime: agent.RuntimeEvent{Result: &domain.ToolResult{
		CallID: "call-1",
		Status: domain.ToolSucceeded,
		WorkspaceChanges: &domain.WorkspaceChanges{
			IsGit:  true,
			Status: " M file.go\n",
			Diff:   "+changed\n",
		},
	}}})

	if !model.ContextOpenForTest() {
		t.Fatal("workspace changes did not open context")
	}
	view := model.View().Content
	for _, want := range []string{"Git status", "M file.go", "Git diff", "+changed"} {
		if !strings.Contains(view, want) {
			t.Fatalf("context missing %q:\n%s", want, view)
		}
	}

	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventToolCompleted, Runtime: agent.RuntimeEvent{Result: &domain.ToolResult{
		CallID: "call-2",
		Status: domain.ToolSucceeded,
		WorkspaceChanges: &domain.WorkspaceChanges{
			Notice: workspace.NonGitNotice,
		},
	}}})
	if view := model.View().Content; !strings.Contains(view, workspace.NonGitNotice) {
		t.Fatalf("context missing exact non-Git notice:\n%s", view)
	}
}

func TestTypedContextTooLargeErrorOpensCompactActionWithoutTrimmingConversation(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.AppendConversationForTest(model, components.BlockUser, "keep this history")
	typed := &domain.TypedError{Kind: domain.ErrorContextTooLarge, Message: "redacted context limit"}
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventError, Err: fmt.Errorf("provider: %w", typed)})

	if !model.ContextOpenForTest() {
		t.Fatal("typed context error did not open context")
	}
	view := model.View().Content
	for _, want := range []string{"context_too_large", "redacted context limit", "Run /compact"} {
		if !strings.Contains(view, want) {
			t.Fatalf("view missing %q:\n%s", want, view)
		}
	}
	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 2 || blocks[0].Content != "keep this history" {
		t.Fatalf("history changed=%+v", blocks)
	}
}

func TestOtherTypedErrorShowsCategoryAndRedactedMessage(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	typed := &domain.TypedError{Kind: domain.ErrorProviderFatal, Message: "redacted provider failure"}
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventError, Err: typed})

	view := model.View().Content
	if !strings.Contains(view, "provider_fatal") || !strings.Contains(view, "redacted provider failure") {
		t.Fatalf("typed error not visible:\n%s", view)
	}
}

func TestGoldenViews(t *testing.T) {
	tests := []struct {
		width      int
		height     int
		wantLayout string
	}{
		{width: 80, height: 30, wantLayout: "context_only"},
		{width: 120, height: 40, wantLayout: "split"},
		{width: 160, height: 50, wantLayout: "split"},
	}

	for _, test := range tests {
		t.Run(test.wantLayout+"-"+filepath.Base(goldenPath(test.width)), func(t *testing.T) {
			model := tui.GoldenModelForTest(test.width, test.height)
			if got := model.LayoutForTest(); got != test.wantLayout {
				t.Fatalf("layout=%q want=%q", got, test.wantLayout)
			}

			view := model.View()
			if !view.AltScreen {
				t.Fatal("View did not request the alternate screen")
			}
			if !strings.Contains(view.Content, "mode: ask") || !strings.Contains(view.Content, "local/gpt-5") {
				t.Fatalf("status row hid mode/model:\n%s", view.Content)
			}
			status := strings.SplitN(view.Content, "\n", 2)[0]
			if len(status) > test.width {
				t.Fatalf("status width=%d exceeds terminal width=%d: %q", len(status), test.width, status)
			}
			if test.width == 80 && !strings.Contains(status, "~") {
				t.Fatalf("narrow status did not middle-truncate workspace: %q", status)
			}
			if !strings.Contains(view.Content, "DIFF internal/app/app.go") {
				t.Fatalf("view missing open diff context:\n%s", view.Content)
			}
			if test.wantLayout == "split" {
				for _, required := range []string{"USER", "ASSISTANT", "TOOL read [completed]"} {
					if !strings.Contains(view.Content, required) {
						t.Fatalf("split view missing %q:\n%s", required, view.Content)
					}
				}
			}

			want, err := os.ReadFile(goldenPath(test.width))
			if err != nil {
				t.Fatal(err)
			}
			if view.Content != string(want) {
				t.Fatalf("view does not match golden %s\n--- want ---\n%s\n--- got ---\n%s", goldenPath(test.width), want, view.Content)
			}
		})
	}
}

func goldenPath(width int) string {
	return filepath.Join("testdata", "view-"+tui.WidthStringForTest(width)+".golden")
}

func keyMessage(value string) tea.KeyPressMsg {
	switch value {
	case "enter":
		return tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter})
	default:
		return tea.KeyPressMsg(tea.Key{Code: []rune(value)[0], Text: value})
	}
}
