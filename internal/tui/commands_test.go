package tui_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/domain"
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
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventTurnAccepted, Draft: "draft prompt"})
	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 1 || blocks[0].Content != "draft prompt" {
		t.Fatalf("accepted blocks=%v", blocks)
	}

	failed, _ := tui.NavigationModelForTest()
	failed = tui.SubmitForTest(failed, "retry me")
	failed = tui.ApplyAppEventForTest(failed, app.Event{
		Kind:  app.EventError,
		Err:   &domain.TypedError{Kind: domain.ErrorConfigurationInvalid, Message: "edit config and run /reload"},
		Draft: "retry me",
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
	model, _ := tui.NavigationModelForTest()
	model = tui.SubmitForTest(model, "configured secret")
	accepted := app.Event{Kind: app.EventTurnAccepted, Draft: "[REDACTED]"}
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
	if got := tui.CommandsForTest(commands); len(got) != 1 || got[0].Kind != app.CommandStartTurn {
		t.Fatalf("commands=%v", got)
	}
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventRejected, Message: "session context is unavailable", Draft: "retry this draft"})
	if model.TurnActiveForTest() || model.ComposerValueForTest() != "retry this draft" {
		t.Fatalf("active=%t composer=%q", model.TurnActiveForTest(), model.ComposerValueForTest())
	}
	model = tui.SubmitForTest(model, model.ComposerValueForTest())
	if got := tui.CommandsForTest(commands); len(got) != 1 || got[0].Prompt != "retry this draft" {
		t.Fatalf("retry commands=%v", got)
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

	wantSelection := domain.ModelSelection{Profile: "primary", Model: "model-a"}
	if model.TurnActiveForTest() || model.ComposerActiveForTest() || model.StatusSelectionForTest() != wantSelection || model.ModelCountForTest() != 2 {
		t.Fatalf("active=%t composerActive=%t selection=%+v modelCount=%d", model.TurnActiveForTest(), model.ComposerActiveForTest(), model.StatusSelectionForTest(), model.ModelCountForTest())
	}
	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 1 || blocks[0].Kind != components.BlockError || blocks[0].Content != "invalid reload" {
		t.Fatalf("reload failure blocks=%v", blocks)
	}
	model = tui.SubmitForTest(model, "/model")
	model = tui.PressForTest(model, "enter")
	if got := tui.CommandsForTest(commands); len(got) != 1 || got[0].Kind != app.CommandChangeModel || got[0].Selection != wantSelection {
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
	if view := model.View().Content; !strings.Contains(view, "/new /sessions /mode /model /reload /compact /help /quit") {
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
