package tui_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	lipgloss "charm.land/lipgloss/v2"
	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/tui"
	"github.com/muratmirgun/yordam/internal/tui/components"
)

func TestSlashCommandTable(t *testing.T) {
	tests := []struct {
		input       string
		wantCommand app.CommandKind
		wantScreen  string
	}{
		{input: "/new", wantCommand: app.CommandNewSession, wantScreen: "conversation"},
		{input: "/sessions", wantScreen: "sessions"},
		{input: "/mode", wantScreen: "mode"},
		{input: "/model", wantScreen: "model"},
		{input: "/skills", wantScreen: "skills"},
		{input: "/reload", wantCommand: app.CommandReloadConfig, wantScreen: "conversation"},
		{input: "/compact", wantCommand: app.CommandCompact, wantScreen: "conversation"},
		{input: "/help", wantScreen: "help"},
		{input: "/quit", wantCommand: app.CommandShutdown, wantScreen: "conversation"},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			model, commands := tui.NavigationModelForTest()
			model = tui.SetComposerValueForTest(model, test.input)
			model = tui.UpdateForTest(model, keyMessage("enter"))

			if got := model.ScreenForTest(); got != test.wantScreen {
				t.Fatalf("screen=%q want=%q", got, test.wantScreen)
			}
			got := tui.CommandsForTest(commands)
			if test.wantCommand == "" {
				if len(got) != 0 {
					t.Fatalf("commands=%v", got)
				}
			} else if len(got) != 1 || got[0].Kind != test.wantCommand {
				t.Fatalf("commands=%v want=%q", got, test.wantCommand)
			}
			if blocks := model.ConversationBlocksForTest(); len(blocks) != 0 {
				t.Fatalf("slash command rendered as conversation: %v", blocks)
			}
		})
	}
}

func TestDraftRendersOnlyAfterTurnAcceptanceAndRestoresOnConfigError(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	model = tui.SubmitForTest(model, "draft prompt")
	got := tui.CommandsForTest(commands)
	if len(got) != 1 || got[0].Kind != app.CommandStartTurn || len(model.ConversationBlocksForTest()) != 0 {
		t.Fatalf("commands=%v blocks=%v", got, model.ConversationBlocksForTest())
	}
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTurnAccepted, DraftID: got[0].DraftID, Draft: "draft prompt"})
	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 1 || blocks[0].Content != "draft prompt" {
		t.Fatalf("accepted blocks=%v", blocks)
	}

	failed, failedCommands := tui.NavigationModelForTest()
	failed = tui.SubmitForTest(failed, "retry me")
	failedCommand := tui.CommandsForTest(failedCommands)[0]
	failed = tui.ApplyAppEventForTest(failed, app.Event{
		Kind:    app.EventError,
		DraftID: failedCommand.DraftID,
		Err:     &domain.TypedError{Kind: domain.ErrorConfigurationInvalid, Message: "edit config and run /reload"},
		Draft:   "retry me",
	})
	failedBlocks := failed.ConversationBlocksForTest()
	if failed.ComposerValueForTest() != "retry me" || failed.TurnActiveForTest() || len(failedBlocks) != 1 || failedBlocks[0].Kind != components.BlockError || !strings.Contains(failedBlocks[0].Content, "edit config and run /reload") {
		t.Fatalf("composer=%q active=%t blocks=%v", failed.ComposerValueForTest(), failed.TurnActiveForTest(), failed.ConversationBlocksForTest())
	}
	if failed.ContextOpenForTest() {
		t.Fatal("configuration error moved focus away from the restored draft")
	}
}

func TestTurnAcceptanceAppendsPendingDraftExactlyOnce(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	model = tui.SubmitForTest(model, "configured secret")
	command := tui.CommandsForTest(commands)[0]
	accepted := app.Event{Kind: app.EventTurnAccepted, DraftID: command.DraftID, Draft: "[REDACTED]"}
	model = tui.ApplyAppEventForTest(model, accepted)
	model = tui.ApplyAppEventForTest(model, accepted)

	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 1 || blocks[0].Kind != components.BlockUser || blocks[0].Content != "[REDACTED]" {
		t.Fatalf("accepted blocks=%v", blocks)
	}
}

func TestRejectedTurnRestoresPendingDraftAndUnlocksComposer(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	model = tui.SubmitForTest(model, "retry this draft")
	got := tui.CommandsForTest(commands)
	if len(got) != 1 || got[0].Kind != app.CommandStartTurn {
		t.Fatalf("commands=%v", got)
	}
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventRejected, DraftID: got[0].DraftID, Message: "session context is unavailable", Draft: "retry this draft"})
	if model.TurnActiveForTest() || model.ComposerValueForTest() != "retry this draft" {
		t.Fatalf("active=%t composer=%q", model.TurnActiveForTest(), model.ComposerValueForTest())
	}
	model = tui.SubmitForTest(model, model.ComposerValueForTest())
	if got := tui.CommandsForTest(commands); len(got) != 1 || got[0].Prompt != "retry this draft" {
		t.Fatalf("retry commands=%v", got)
	}
}

func TestDraftCorrelationIgnoresDelayedAcceptanceFromPriorTurn(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	model = tui.SubmitForTest(model, "draft A")
	commandA := tui.CommandsForTest(commands)[0]
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTurnAccepted, DraftID: commandA.DraftID, Draft: "draft A"})
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTurnCompleted})

	model = tui.SubmitForTest(model, "draft B")
	commandB := tui.CommandsForTest(commands)[0]
	if commandA.DraftID == 0 || commandB.DraftID == 0 || commandA.DraftID == commandB.DraftID {
		t.Fatalf("draft IDs A=%d B=%d", commandA.DraftID, commandB.DraftID)
	}
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTurnAccepted, DraftID: commandA.DraftID, Draft: "draft A"})
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTurnAccepted, DraftID: commandB.DraftID, Draft: "draft B"})
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTurnAccepted, DraftID: commandB.DraftID, Draft: "draft B"})

	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 2 || blocks[0].Content != "draft A" || blocks[1].Content != "draft B" {
		t.Fatalf("blocks=%v", blocks)
	}
}

func TestMatchingTerminalErrorRestoresLocalDraftAndClearsCorrelation(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	model = tui.SubmitForTest(model, "draft B")
	command := tui.CommandsForTest(commands)[0]
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventError, DraftID: command.DraftID, Err: errors.New("prepare failed")})

	if model.TurnActiveForTest() || model.ComposerValueForTest() != "draft B" {
		t.Fatalf("active=%t composer=%q", model.TurnActiveForTest(), model.ComposerValueForTest())
	}
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTurnAccepted, DraftID: command.DraftID, Draft: "draft B"})
	for _, block := range model.ConversationBlocksForTest() {
		if block.Kind == components.BlockUser {
			t.Fatalf("stale acceptance rendered after terminal error: %v", model.ConversationBlocksForTest())
		}
	}
}

func TestStaleDraftFailuresDoNotConsumeCurrentDraft(t *testing.T) {
	tests := []struct {
		name  string
		event app.Event
	}{
		{name: "error", event: app.Event{Kind: app.EventError, Err: errors.New("stale error")}},
		{name: "rejection", event: app.Event{Kind: app.EventRejected, Message: "stale rejection"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model, commands := tui.NavigationModelForTest()
			model = tui.SubmitForTest(model, "draft B")
			command := tui.CommandsForTest(commands)[0]
			test.event.DraftID = command.DraftID + 1
			test.event.Draft = "stale draft"
			model = tui.ApplyAppEventForTest(model, test.event)
			if !model.TurnActiveForTest() || model.ComposerValueForTest() != "" {
				t.Fatalf("stale event consumed draft: active=%t composer=%q", model.TurnActiveForTest(), model.ComposerValueForTest())
			}
			model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTurnAccepted, DraftID: command.DraftID, Draft: "draft B"})
			blocks := model.ConversationBlocksForTest()
			if blocks[len(blocks)-1].Kind != components.BlockUser || blocks[len(blocks)-1].Content != "draft B" {
				t.Fatalf("real acceptance not rendered: %v", blocks)
			}
		})
	}
}

func TestDraftIDExhaustionFailsClosedWithoutReusingAnID(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	model = tui.SetNextDraftIDForTest(model, ^uint64(0))
	model = tui.SubmitForTest(model, "preserve this draft")

	if got := tui.CommandsForTest(commands); len(got) != 0 {
		t.Fatalf("exhausted command=%v", got)
	}
	blocks := model.ConversationBlocksForTest()
	if model.TurnActiveForTest() || model.ComposerValueForTest() != "preserve this draft" || len(blocks) != 1 || !strings.Contains(blocks[0].Content, "restart Yordam") {
		t.Fatalf("active=%t composer=%q blocks=%v", model.TurnActiveForTest(), model.ComposerValueForTest(), blocks)
	}
}

func TestDraftIDZeroValueStartsAtOne(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	model = tui.SetNextDraftIDForTest(model, 0)
	model = tui.SubmitForTest(model, "draft")

	if got := tui.CommandsForTest(commands); len(got) != 1 || got[0].DraftID != 1 {
		t.Fatalf("commands=%v", got)
	}
}

func TestReloadCompletionRefreshesModelsAndUnlocksComposer(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	model = tui.SubmitForTest(model, "/reload")
	if got := tui.CommandsForTest(commands); len(got) != 1 || got[0].Kind != app.CommandReloadConfig || !model.TurnActiveForTest() {
		t.Fatalf("commands=%v active=%t", got, model.TurnActiveForTest())
	}
	selection := domain.ModelSelection{Profile: "new", Model: "b"}
	model = tui.ApplyAppEventForTest(model, app.Event{
		Kind:      app.EventReloadCompleted,
		Applied:   true,
		Message:   "configuration reloaded",
		Models:    []domain.ModelSelection{{Profile: "new", Model: "a"}, selection},
		Selection: selection,
	})
	if model.TurnActiveForTest() || model.StatusSelectionForTest() != selection {
		t.Fatalf("active=%t selection=%+v", model.TurnActiveForTest(), model.StatusSelectionForTest())
	}
	model = tui.SubmitForTest(model, "/model")
	if got := model.ModelCountForTest(); got != 2 {
		t.Fatalf("model count=%d", got)
	}
	model = tui.PressForTest(model, "enter")
	if got := tui.CommandsForTest(commands); len(got) != 1 || got[0].Kind != app.CommandChangeModel || got[0].Selection != selection {
		t.Fatalf("refreshed picker commands=%v", got)
	}
	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 1 || blocks[0].Kind != components.BlockNotice || blocks[0].Content != "configuration reloaded" {
		t.Fatalf("reload blocks=%v", blocks)
	}
}

func TestReloadFailurePreservesModelsAndSelectionWhileUnlocking(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	preservedSelection := domain.ModelSelection{Profile: "primary", Model: "model-b"}
	model = tui.SubmitForTest(model, "/model")
	model = tui.PressForTest(model, "down")
	model = tui.PressForTest(model, "enter")
	if got := tui.CommandsForTest(commands); len(got) != 1 || got[0].Kind != app.CommandChangeModel || got[0].Selection != preservedSelection {
		t.Fatalf("model change commands=%v", got)
	}
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventState, Selection: preservedSelection})
	model = tui.SubmitForTest(model, "/reload")
	if got := tui.CommandsForTest(commands); len(got) != 1 || got[0].Kind != app.CommandReloadConfig {
		t.Fatalf("reload commands=%v", got)
	}
	model = tui.ApplyAppEventForTest(model, app.Event{
		Kind:      app.EventReloadCompleted,
		Applied:   false,
		Models:    []domain.ModelSelection{{Profile: "discarded", Model: "model"}},
		Selection: domain.ModelSelection{Profile: "discarded", Model: "model"},
		Err:       errors.New("invalid reload"),
	})

	if model.TurnActiveForTest() || model.ComposerActiveForTest() || model.StatusSelectionForTest() != preservedSelection || model.ModelCountForTest() != 2 {
		t.Fatalf("active=%t composerActive=%t selection=%+v modelCount=%d", model.TurnActiveForTest(), model.ComposerActiveForTest(), model.StatusSelectionForTest(), model.ModelCountForTest())
	}
	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 1 || blocks[0].Kind != components.BlockError || blocks[0].Content != "invalid reload" {
		t.Fatalf("reload failure blocks=%v", blocks)
	}
	model = tui.SubmitForTest(model, "/model")
	model = tui.PressForTest(model, "enter")
	if got := tui.CommandsForTest(commands); len(got) != 1 || got[0].Kind != app.CommandChangeModel || got[0].Selection != preservedSelection {
		t.Fatalf("preserved picker commands=%v", got)
	}
}

func TestReloadWithMissingCredentialShowsLoadedAndRestartNotices(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	selection := domain.ModelSelection{Profile: "new", Model: "b"}
	model = tui.ApplyAppEventForTest(model, app.Event{
		Kind:      app.EventReloadCompleted,
		Applied:   true,
		Message:   "configuration reloaded",
		Models:    []domain.ModelSelection{selection},
		Selection: selection,
		Err: &domain.TypedError{
			Kind:    domain.ErrorConfigurationInvalid,
			Message: `API key environment variable "NEW_KEY" is empty; export it and restart Yordam`,
		},
	})
	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 2 || blocks[0].Content != "configuration reloaded" || !strings.Contains(blocks[1].Content, "restart Yordam") {
		t.Fatalf("blocks=%+v", blocks)
	}
	if model.StatusSelectionForTest() != selection || model.ModelCountForTest() != 1 {
		t.Fatalf("selection=%+v modelCount=%d", model.StatusSelectionForTest(), model.ModelCountForTest())
	}
	model = tui.SubmitForTest(model, "/model")
	model = tui.PressForTest(model, "enter")
	if got := tui.CommandsForTest(commands); len(got) != 1 || got[0].Selection != selection {
		t.Fatalf("applied picker commands=%v", got)
	}
}

func TestNoticeAppendsWithoutChangingTurnState(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle", true: "active"}[active], func(t *testing.T) {
			model, _ := tui.NavigationModelForTest()
			model = tui.SetTurnActiveForTest(model, active)
			model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventNotice, Message: "Created config.jsonc"})
			blocks := model.ConversationBlocksForTest()
			if len(blocks) != 1 || blocks[0].Content != "Created config.jsonc" || model.TurnActiveForTest() != active {
				t.Fatalf("blocks=%v active=%t wantActive=%t", blocks, model.TurnActiveForTest(), active)
			}
		})
	}
}

func TestReloadDuringActiveOperationIsRejectedLocally(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	model = tui.SetTurnActiveForTest(model, true)
	model = tui.SubmitForTest(model, "/reload")

	if got := tui.CommandsForTest(commands); len(got) != 0 {
		t.Fatalf("commands=%v", got)
	}
	blocks := model.ConversationBlocksForTest()
	if !model.TurnActiveForTest() || len(blocks) != 1 || blocks[0].Kind != components.BlockNotice || blocks[0].Content != "an operation is already active" {
		t.Fatalf("active=%t blocks=%v", model.TurnActiveForTest(), blocks)
	}
}

func TestStateModelsDistinguishesNilFromEmpty(t *testing.T) {
	model, _ := tui.NavigationModelForTest()
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventState, Models: nil})
	if got := model.ModelCountForTest(); got != 2 {
		t.Fatalf("nil models changed picker count=%d", got)
	}

	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventState, Models: []domain.ModelSelection{}})
	if got := model.ModelCountForTest(); got != 0 {
		t.Fatalf("empty models did not clear picker count=%d", got)
	}
}

func TestHelpListsReloadCommand(t *testing.T) {
	model, _ := tui.NavigationModelForTest()
	model = tui.SubmitForTest(model, "/help")
	if view := model.View().Content; !strings.Contains(view, "/new /sessions /mode /model /skills /reload /compact /help /quit") {
		t.Fatalf("help missing reload command:\n%s", view)
	}
}

func TestSessionModeAndModelPickerFlows(t *testing.T) {
	model, commands := tui.NavigationModelForTest()

	model = tui.SubmitForTest(model, "/sessions")
	model = tui.PressForTest(model, "enter")
	if got := tui.CommandsForTest(commands); len(got) != 1 || got[0].Kind != app.CommandOpenSession || got[0].SessionID != "newer" {
		t.Fatalf("session commands=%v", got)
	}

	model = tui.SubmitForTest(model, "/mode")
	model = tui.PressForTest(model, "down")
	model = tui.PressForTest(model, "enter")
	if got := tui.CommandsForTest(commands); len(got) != 1 || got[0].Kind != app.CommandChangeMode || got[0].Mode != domain.ModeAuto {
		t.Fatalf("mode commands=%v", got)
	}

	model = tui.SubmitForTest(model, "/model")
	model = tui.PressForTest(model, "down")
	model = tui.PressForTest(model, "enter")
	if got := tui.CommandsForTest(commands); len(got) != 1 || got[0].Kind != app.CommandChangeModel || got[0].Selection.Model != "model-b" {
		t.Fatalf("model commands=%v", got)
	}
}

func TestSessionAndPaletteFiltersRouteTypedInput(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	model = tui.SubmitForTest(model, "/sessions")
	for _, key := range []string{"o", "l", "d"} {
		model = tui.PressForTest(model, key)
	}
	model = tui.PressForTest(model, "enter")
	if got := tui.CommandsForTest(commands); len(got) != 1 || got[0].SessionID != "older" {
		t.Fatalf("filtered session commands=%v", got)
	}

	model = tui.PressForTest(model, "ctrl+p")
	for _, key := range []string{"m", "o", "d", "e", "l"} {
		model = tui.PressForTest(model, key)
	}
	model = tui.PressForTest(model, "enter")
	if got := model.ScreenForTest(); got != "model" {
		t.Fatalf("filtered palette screen=%q", got)
	}
}

func TestPaletteNoMatchEnterDoesNotSubmitOrClose(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	model = tui.PressForTest(model, "ctrl+p")
	for _, key := range []string{"n", "o", "m", "a", "t", "c", "h"} {
		model = tui.PressForTest(model, key)
	}
	model = tui.PressForTest(model, "enter")
	if model.ModalForTest() != "command_palette" || model.ScreenForTest() != "conversation" {
		t.Fatalf("modal=%q screen=%q", model.ModalForTest(), model.ScreenForTest())
	}
	if got := tui.CommandsForTest(commands); len(got) != 0 {
		t.Fatalf("commands=%+v", got)
	}
	if blocks := model.ConversationBlocksForTest(); len(blocks) != 0 {
		t.Fatalf("conversation=%+v", blocks)
	}
}

func TestRequiredGlobalKeysWorkFromSecondaryScreens(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	model = tui.SubmitForTest(model, "/help")
	model = tui.PressForTest(model, "ctrl+p")
	if got := model.ModalForTest(); got != "command_palette" {
		t.Fatalf("palette modal=%q", got)
	}
	model = tui.PressForTest(model, "esc")
	model = tui.PressForTest(model, "ctrl+c")
	if got := tui.CommandKindsForTest(commands); !reflect.DeepEqual(got, []app.CommandKind{app.CommandShutdown}) {
		t.Fatalf("commands=%v", got)
	}

	active, _ := tui.NavigationModelForTest()
	active = tui.SetTurnActiveForTest(active, true)
	active = tui.SubmitForTest(active, "/mode")
	active = tui.PressForTest(active, "ctrl+c")
	if got := active.ModalForTest(); got != "confirm_exit" {
		t.Fatalf("active modal=%q", got)
	}
}

func TestClosingPaletteOverSecondaryScreenDoesNotEditHiddenComposer(t *testing.T) {
	model, _ := tui.NavigationModelForTest()
	model = tui.SubmitForTest(model, "/help")
	model = tui.PressForTest(model, "ctrl+p")
	model = tui.PressForTest(model, "esc")
	model = tui.UpdateForTest(model, keyMessage("x"))
	if got := model.ComposerValueForTest(); got != "" {
		t.Fatalf("hidden composer value=%q", got)
	}
	if got := model.ScreenForTest(); got != "help" {
		t.Fatalf("screen=%q", got)
	}
}

func TestPaletteModeFlowWorksDuringActiveTurn(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	model = tui.SetTurnActiveForTest(model, true)
	model = tui.PressForTest(model, "ctrl+p")
	model = tui.PressForTest(model, "down")
	model = tui.PressForTest(model, "down")
	model = tui.PressForTest(model, "enter")
	if got := model.ScreenForTest(); got != "mode" {
		t.Fatalf("screen=%q", got)
	}
	model = tui.PressForTest(model, "down")
	model = tui.PressForTest(model, "enter")
	if got := tui.CommandsForTest(commands); len(got) != 1 || got[0].Kind != app.CommandChangeMode || got[0].Mode != domain.ModeAuto {
		t.Fatalf("commands=%v", got)
	}
}

func TestActiveTurnComposerStillRoutesSlashSettings(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	model = tui.SetTurnActiveForTest(model, true)
	model = tui.SetComposerValueForTest(model, "/mode")
	model = tui.UpdateForTest(model, keyMessage("enter"))
	if got := model.ScreenForTest(); got != "mode" {
		t.Fatalf("screen=%q", got)
	}
	if got := tui.CommandsForTest(commands); len(got) != 0 {
		t.Fatalf("commands=%v", got)
	}
}

func TestDurableSettingsMovePickerCursors(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	model = tui.ApplyAppEventForTest(model, app.Event{
		Kind:      app.EventState,
		Mode:      domain.ModeSafe,
		Selection: domain.ModelSelection{Profile: "primary", Model: "model-b"},
	})

	model = tui.SubmitForTest(model, "/mode")
	model = tui.PressForTest(model, "enter")
	got := tui.CommandsForTest(commands)
	if len(got) != 1 || got[0].Mode != domain.ModeSafe {
		t.Fatalf("mode commands=%v", got)
	}

	model = tui.SubmitForTest(model, "/model")
	model = tui.PressForTest(model, "enter")
	got = tui.CommandsForTest(commands)
	if len(got) != 1 || got[0].Selection.Model != "model-b" {
		t.Fatalf("model commands=%v", got)
	}
}

func TestSlashQuitSharesActiveTurnConfirmation(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	model = tui.SetTurnActiveForTest(model, true)
	model = tui.SubmitForTest(model, "/quit")
	if got := model.ModalForTest(); got != "confirm_exit" {
		t.Fatalf("modal=%q", got)
	}
	if got := tui.CommandsForTest(commands); len(got) != 0 {
		t.Fatalf("commands before confirmation=%v", got)
	}
	model = tui.PressForTest(model, "y")
	if got := tui.CommandKindsForTest(commands); !reflect.DeepEqual(got, []app.CommandKind{app.CommandCancelTurn, app.CommandShutdown}) {
		t.Fatalf("confirmed commands=%v", got)
	}
}

func TestDurableStateEventUpdatesStatusAndProjectsOpenedSession(t *testing.T) {
	model, _ := tui.NavigationModelForTest()
	replay := domain.SessionReplay{
		Session: domain.Session{
			ID:        "opened",
			Title:     "Opened session",
			Mode:      domain.ModeSafe,
			Selection: domain.ModelSelection{Profile: "primary", Model: "model-b"},
		},
		Events: []domain.DurableEvent{
			tuiDurableEvent(1, domain.EventUserMessage, domain.MessagePayload{Content: "persisted user"}),
			tuiDurableEvent(2, domain.EventAssistantMessage, domain.MessagePayload{Content: "persisted assistant"}),
		},
	}
	model = tui.ApplyAppEventForTest(model, app.Event{
		Kind:      app.EventState,
		Mode:      domain.ModeSafe,
		Selection: domain.ModelSelection{Profile: "primary", Model: "model-b"},
		Session:   replay.Session,
		Replay:    replay,
	})

	if model.TurnActiveForTest() {
		t.Fatal("durable state event marked turn active")
	}
	if got := model.ScreenForTest(); got != "conversation" {
		t.Fatalf("screen=%q", got)
	}
	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 2 || blocks[0].Content != "persisted user" || blocks[1].Content != "persisted assistant" {
		t.Fatalf("projected blocks=%v", blocks)
	}
}

func TestDurableSubagentSnapshotRendersCardAndNavigatesChildAndParent(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	card := protocol.SubagentCardV1{AttemptID: "attempt", ParentSessionID: "parent", ChildSessionID: "child", Task: "inspect recovery", State: protocol.SubagentStageRunning, Attempt: 1, StartedAt: time.Unix(1, 0).UTC(), Deadline: time.Unix(11, 0).UTC(), ElapsedNanos: int64(time.Second), MaxToolCalls: 4}
	rawCard, _ := json.Marshal(card)
	lineage := protocol.SubagentLineageV1{SessionID: "parent", Children: []protocol.SessionID{"child"}}
	rawLineage, _ := json.Marshal(lineage)
	durable := protocol.DurableProjection{Subagents: []protocol.ProjectionView{{ID: "attempt", Kind: "subagent", Status: "running", State: protocol.ValueKnown, Data: rawCard}}, Lineage: &protocol.ProjectionView{ID: "parent", Kind: "lineage", Status: "ready", State: protocol.ValueKnown, Data: rawLineage}}
	parent := domain.Session{ID: "parent", Title: "Parent", Mode: domain.ModeAsk, Selection: domain.ModelSelection{Profile: "primary", Model: "model-a"}}
	parentEvent := app.Event{Kind: app.EventState, Session: parent, Replay: domain.SessionReplay{Session: parent}, Durable: &durable}
	model = tui.ApplyAppEventForTest(model, parentEvent)
	if view := model.View().Content; !strings.Contains(view, "inspect recovery") || !strings.Contains(view, "child") {
		t.Fatalf("child card absent:\n%s", view)
	}
	model = tui.PressForTest(model, "alt+enter")
	got := tui.CommandsForTest(commands)
	if len(got) != 1 || got[0].Kind != app.CommandOpenSession || got[0].SessionID != "child" {
		t.Fatalf("open child commands=%+v", got)
	}

	childLineage := protocol.SubagentLineageV1{SessionID: "child", ParentSessionID: "parent", DelegationAttemptID: "attempt", Children: []protocol.SessionID{}}
	rawChildLineage, _ := json.Marshal(childLineage)
	childDurable := protocol.DurableProjection{Lineage: &protocol.ProjectionView{ID: "child", Kind: "lineage", Status: "ready", State: protocol.ValueKnown, Data: rawChildLineage}}
	child := domain.Session{ID: "child", Title: "Child", Mode: domain.ModeAsk, Selection: parent.Selection}
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventState, Session: child, Replay: domain.SessionReplay{Session: child}, Durable: &childDurable})
	if view := model.View().Content; !strings.Contains(view, "Parent: parent") {
		t.Fatalf("parent backlink absent:\n%s", view)
	}
	model = tui.PressForTest(model, "alt+left")
	got = tui.CommandsForTest(commands)
	if len(got) != 1 || got[0].SessionID != "parent" {
		t.Fatalf("return parent commands=%+v", got)
	}

	restarted, restartedCommands := tui.NavigationModelForTest()
	restarted = tui.ApplyAppEventForTest(restarted, parentEvent)
	if view := restarted.View().Content; !strings.Contains(view, "inspect recovery") || !strings.Contains(view, "child") {
		t.Fatalf("restart did not reconstruct child card:\n%s", view)
	}
	restarted = tui.PressForTest(restarted, "alt+enter")
	if got := tui.CommandsForTest(restartedCommands); len(got) != 1 || got[0].SessionID != "child" {
		t.Fatalf("restart child navigation=%+v", got)
	}
}

func TestRealModelBoundsSelectedChildCardWithHugeHistoryAndCardList(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTextDelta, Runtime: agent.RuntimeEvent{Text: strings.Repeat("huge history line\n", 1000)}})
	long := strings.Repeat("long-task-value-", 120)
	views := make([]protocol.ProjectionView, 20)
	for index := range views {
		card := protocol.SubagentCardV1{
			AttemptID: protocol.DelegationAttemptID(fmt.Sprintf("attempt-%02d", index+1)), ParentSessionID: "parent",
			ChildSessionID: protocol.SessionID(fmt.Sprintf("123e4567-e89b-12d3-a456-%012d", index)), Task: long,
			State: protocol.SubagentStageSucceeded, Attempt: index%protocol.MaxSubagentAttemptsPerTurn + 1,
			StartedAt: time.Unix(1, 0).UTC(), Deadline: time.Unix(3, 0).UTC(), ElapsedNanos: int64(time.Second), ToolCalls: 1, MaxToolCalls: 4,
		}
		if index%2 == 0 {
			card.ReceiptSummary = long
			card.ChangedFiles = []string{"a-" + long, "b-" + long, "c-" + long}
			card.CommandsAndTests = []string{"a-" + long, "b-" + long, "c-" + long}
		}
		raw, err := json.Marshal(card)
		if err != nil {
			t.Fatal(err)
		}
		views[index] = protocol.ProjectionView{ID: string(card.AttemptID), Kind: "subagent", Status: string(card.State), State: protocol.ValueKnown, Data: raw}
	}
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventState, Durable: &protocol.DurableProjection{Subagents: views}})
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTurnCompleted})
	for range 12 {
		model = tui.PressForTest(model, "alt+]")
	}
	for _, width := range []int{40, 120} {
		model = tui.UpdateForTest(model, tea.WindowSizeMsg{Width: width, Height: 24})
		view := model.View()
		rendered := view.Content
		if lines := strings.Count(strings.TrimSuffix(rendered, "\n"), "\n") + 1; lines > 24 {
			t.Fatalf("width %d terminal overflow: lines=%d height=24\n%s", width, lines, rendered)
		}
		if view.Cursor == nil || view.Cursor.Position.Y < 0 || view.Cursor.Position.Y >= 24 {
			t.Fatalf("width %d composer cursor is not visible: %+v", width, view.Cursor)
		}
		start := strings.Index(rendered, "card 13/20")
		endMarker := "Alt+[/Alt+] cards | Alt+Enter open"
		end := strings.Index(rendered[start:], endMarker)
		if start < 0 || end < 0 {
			t.Fatalf("width %d selected card missing:\n%s", width, rendered)
		}
		end += start + len(endMarker)
		cardView := rendered[start:end]
		lines := strings.Split(cardView, "\n")
		if len(lines) > 12 || strings.Contains(cardView, "attempt-12") || strings.Contains(cardView, "attempt-14") {
			t.Fatalf("width %d unbounded/non-selected card view (%d lines):\n%s", width, len(lines), cardView)
		}
		for _, line := range lines {
			if lipgloss.Width(line) > width {
				t.Fatalf("width %d card line width=%d: %q", width, lipgloss.Width(line), line)
			}
		}
		model = tui.PressForTest(model, "alt+]")
		transition := model.View()
		if lines := strings.Count(strings.TrimSuffix(transition.Content, "\n"), "\n") + 1; lines > 24 || transition.Cursor == nil || transition.Cursor.Position.Y >= 24 || !strings.Contains(transition.Content, "card 14/20") {
			t.Fatalf("width %d card-height transition overflow/cursor loss: cursor=%+v\n%s", width, transition.Cursor, transition.Content)
		}
		model = tui.PressForTest(model, "alt+[")
	}
}

func TestLiveSubagentStageUsesFreshDurableSnapshotAndEscCancelsParent(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	model = tui.SetTurnActiveForTest(model, true)
	card := protocol.SubagentCardV1{AttemptID: "attempt", ParentSessionID: "parent", ChildSessionID: "child", Task: "child task", State: protocol.SubagentStageRunning, Attempt: 1, StartedAt: time.Unix(1, 0).UTC(), Deadline: time.Unix(11, 0).UTC(), ElapsedNanos: int64(time.Second), MaxToolCalls: 4}
	raw, _ := json.Marshal(card)
	durable := protocol.DurableProjection{Subagents: []protocol.ProjectionView{{ID: "attempt", Kind: "subagent", Status: "running", State: protocol.ValueKnown, Data: raw}}}
	stage := protocol.SubagentStageV1{AttemptID: "attempt", ParentSessionID: "parent", ChildSessionID: "child", Stage: protocol.SubagentStageWaiting}
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventSubagentStage, Subagent: &stage, Durable: &durable})
	view := model.View().Content
	if !strings.Contains(view, "[running]") || strings.Contains(view, "[waiting]") {
		t.Fatalf("transient stage overrode durable snapshot:\n%s", view)
	}
	model = tui.PressForTest(model, "esc")
	got := tui.CommandsForTest(commands)
	if len(got) != 1 || got[0].Kind != app.CommandCancelTurn || !model.CancelSentForTest() {
		t.Fatalf("Esc did not cancel owning parent turn: commands=%+v", got)
	}
}

func TestOpenedSessionIsImmediatelyAvailableInPicker(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	session := domain.Session{ID: "latest", Title: "Latest", Mode: domain.ModeAsk, Selection: domain.ModelSelection{Profile: "primary", Model: "model-a"}, UpdatedAt: time.Unix(3, 0)}
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventState, Session: session, Replay: domain.SessionReplay{Session: session}})
	model = tui.SubmitForTest(model, "/sessions")
	model = tui.PressForTest(model, "enter")
	if got := tui.CommandsForTest(commands); len(got) != 1 || got[0].SessionID != "latest" {
		t.Fatalf("commands=%v", got)
	}
}

func TestUnknownSlashCommandIsRejectedLocally(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	model = tui.SubmitForTest(model, "/unknown")
	if got := tui.CommandsForTest(commands); len(got) != 0 {
		t.Fatalf("commands=%v", got)
	}
	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 1 || blocks[0].Content != "unknown command: /unknown" {
		t.Fatalf("blocks=%v", blocks)
	}
}

func TestNonTerminalCommandErrorDoesNotUnlockActiveComposer(t *testing.T) {
	model, _ := tui.NavigationModelForTest()
	model = tui.SetTurnActiveForTest(model, true)
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventError, Message: "mode append failed", NonTerminal: true})
	if !model.TurnActiveForTest() || !model.ComposerActiveForTest() {
		t.Fatal("command error ended active turn")
	}
	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 1 || blocks[0].Content != "mode append failed" {
		t.Fatalf("blocks=%v", blocks)
	}
}

func TestCompactLocksComposerUntilTerminalEvent(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	model = tui.SubmitForTest(model, "/compact")
	if !model.TurnActiveForTest() || !model.ComposerActiveForTest() {
		t.Fatal("compaction did not lock composer")
	}
	model = tui.SetComposerValueForTest(model, "blocked prompt")
	model = tui.UpdateForTest(model, keyMessage("enter"))
	got := tui.CommandsForTest(commands)
	if len(got) != 1 || got[0].Kind != app.CommandCompact {
		t.Fatalf("commands=%v", got)
	}
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTurnCompleted})
	if model.TurnActiveForTest() || model.ComposerActiveForTest() {
		t.Fatal("compaction terminal did not unlock composer")
	}
}

func TestCompactDuringTurnIsRejectedLocally(t *testing.T) {
	model, commands := tui.NavigationModelForTest()
	model = tui.SetTurnActiveForTest(model, true)
	model = tui.SubmitForTest(model, "/compact")
	if got := tui.CommandsForTest(commands); len(got) != 0 {
		t.Fatalf("commands=%v", got)
	}
	if !model.TurnActiveForTest() {
		t.Fatal("compact rejection ended active turn")
	}
	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 1 || blocks[0].Content != "an operation is already active" {
		t.Fatalf("blocks=%v", blocks)
	}
}

func tuiDurableEvent(sequence uint64, kind domain.EventKind, payload any) domain.DurableEvent {
	raw, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return domain.DurableEvent{Seq: sequence, Kind: kind, Payload: raw, Time: time.Unix(int64(sequence), 0)}
}
