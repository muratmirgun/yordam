package tui

import (
	"encoding/json"
	"errors"
	"strings"

	"charm.land/bubbles/v2/spinner"
	tea "charm.land/bubbletea/v2"
	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/tui/components"
)

func (model Model) updateMessage(message tea.Msg) Model {
	switch message := message.(type) {
	case tea.WindowSizeMsg:
		model.width = message.Width
		model.height = message.Height
		model = model.resizeComponents()
	case tea.KeyPressMsg:
		modalOpen := model.modal != ModalNone
		model = model.handleKey(message.String())
		if !modalOpen && !isRootKey(message.String()) && model.focusedComponent == focusComposer {
			if message.String() == "enter" && model.turnActive && strings.HasPrefix(strings.TrimSpace(model.composer.Value()), "/") {
				submitted := model.composer.Value()
				model.composer.SetValue("")
				model = model.routeSubmission(submitted)
			} else {
				model = model.updateComposer(message)
			}
		}
	case appEventMsg:
		if message.ok {
			model = model.handleAppEvent(message.event)
		}
	case spinner.TickMsg:
		if model.turnProgress != progressIdle {
			var command tea.Cmd
			model.spinner, command = model.spinner.Update(message)
			model.pendingCommand = command
		}
	default:
		if model.modal == ModalNone && model.focusedComponent == focusComposer {
			model = model.updateComposer(message)
		}
	}
	return model
}

func (model Model) handleAppEvent(event app.Event) Model {
	if event.Context != nil {
		model.context.SetCompactionContext(*event.Context)
	}
	switch event.Kind {
	case app.EventCompactionStarted, app.EventCompactionProgress:
		if event.Compaction != nil {
			model.context.SetCompactionProgress(*event.Compaction)
		}
		model = model.setTurnActive(true)
		model = model.setTurnProgress(progressWaiting)
	case app.EventCompactionCompleted, app.EventCompactionFailed:
		if event.Compaction != nil {
			model.context.SetCompactionProgress(*event.Compaction)
		}
		model = model.setTurnActive(false)
		if event.Compaction != nil && event.Compaction.Error != nil {
			model.conversation.Append(components.BlockError, event.Compaction.Error.Message)
		}
		model.context.ClearCompactionProgress()
	case app.EventState:
		if event.Runtime.Kind == "" {
			model.context.ClearCompactionProgress()
		}
		if event.Runtime.Kind != "" {
			model = model.setTurnActive(true)
			model = model.setTurnProgress(progressWaiting)
		} else {
			model = model.applyDurableState(event)
		}
		if event.Models != nil {
			model = model.replaceModels(event.Models, event.Selection)
		}
	case app.EventTurnAccepted:
		model.context.ClearCompactionProgress()
		if model.matchesPendingDraft(event) {
			draft := event.Draft
			if draft == "" {
				draft = model.pendingDraft
			}
			model.conversation.Append(components.BlockUser, draft)
			model = model.clearPendingDraft()
		}
	case app.EventReloadCompleted:
		model = model.setTurnActive(false)
		if event.Applied {
			model = model.replaceModels(event.Models, event.Selection)
			if event.Message != "" {
				model.conversation.Append(components.BlockNotice, event.Message)
			}
		}
		if event.Err != nil {
			model = model.renderErrorEvent(event)
		} else if !event.Applied && event.Message != "" {
			model.conversation.Append(components.BlockNotice, event.Message)
		}
	case app.EventNotice:
		if event.Message != "" {
			model.conversation.Append(components.BlockNotice, event.Message)
		}
	case app.EventTextDelta:
		model = model.setTurnActive(true)
		model = model.setTurnProgress(progressReceiving)
		model.conversation.AppendAssistantDelta(event.Runtime.Text)
	case app.EventToolStarted:
		model = model.setTurnActive(true)
		model = model.setTurnProgress(progressTool)
	case app.EventToolOutput:
		model = model.setTurnActive(true)
		model = model.setTurnProgress(progressTool)
		if progress := event.Runtime.Progress; progress != nil {
			model.conversation.ReplaceToolOutput(progress.CallID, "shell", progress.Text, progress.Truncated)
		}
	case app.EventToolCompleted:
		model = model.setTurnActive(true)
		model = model.setTurnProgress(progressWaiting)
		if result := event.Runtime.Result; result != nil {
			model.conversation.CompleteTool(result.CallID, "", toolStatus(result.Status), result.Content, result.Duration, result.Truncated)
			if result.FileChange != nil {
				model.context.ShowDiff(result.FileChange.Path, result.FileChange.Diff)
				model = model.openContext()
			}
			if result.WorkspaceChanges != nil {
				model.context.ShowWorkspaceChanges(result.WorkspaceChanges)
				model = model.openContext()
			}
		}
	case app.EventPermissionRequested:
		if event.Permission != nil {
			warning := model.effectiveMode == domain.ModeAuto && event.Permission.Call.Request.Name == "shell"
			commands := model.commands
			callID := event.Permission.Call.Request.CallID
			model.permission = components.NewPermission(event.Permission.Call, func(decision domain.PermissionDecision) {
				if commands == nil {
					return
				}
				if warning && decision.Action == domain.PermissionAllow {
					commands <- app.Command{Kind: app.CommandAcknowledgeAutoShell, CallID: callID, Decision: decision}
					return
				}
				commands <- permissionResolutionCommand(callID, decision)
			})
			model.permission.SetAutoShellWarning(warning)
			model.modal = ModalPermission
			model.focusedComponent = focusModal
			model = model.resizeComponents()
		}
	case app.EventTurnCompleted:
		model = model.applyTerminalReplay(event)
		model = model.setTurnActive(false)
		model = model.finishPendingExit()
	case app.EventTurnInterrupted:
		model = model.applyTerminalReplay(event)
		model = model.setTurnActive(false)
		if message := eventMessage(event); message != "" {
			model.conversation.Append(components.BlockNotice, message)
		}
		model = model.finishPendingExit()
	case app.EventError:
		model = model.applyTerminalReplay(event)
		terminalTransition := false
		if !event.NonTerminal {
			switch {
			case model.matchesPendingDraft(event):
				model = model.restorePendingDraft(event)
				terminalTransition = true
			case model.pendingDraftID == 0:
				model = model.setTurnActive(false)
				if event.Draft != "" {
					model.composer.SetValue(event.Draft)
				}
				terminalTransition = true
			}
		}
		model = model.renderErrorEvent(event)
		if terminalTransition {
			model = model.finishPendingExit()
		}
	case app.EventRejected:
		switch {
		case model.matchesPendingDraft(event):
			model = model.restorePendingDraft(event)
		case model.pendingDraftID == 0 && event.Draft != "":
			model.composer.SetValue(event.Draft)
			model = model.setTurnActive(false)
		}
		if message := eventMessage(event); message != "" {
			model.conversation.Append(components.BlockNotice, message)
		}
	}
	return model
}

func (model Model) applyTerminalReplay(event app.Event) Model {
	if event.Replay.Session.ID != "" {
		model = model.replaceConversation(event.Replay)
	}
	return model
}

func (model Model) finishPendingExit() Model {
	if model.modal != ModalConfirmExit {
		return model
	}
	model.modal = ModalNone
	model.focusedComponent = focusComposer
	model.sendCommand(app.CommandShutdown)
	return model.resizeComponents()
}

func (model Model) openContext() Model {
	model.contextOpen = true
	model.context.OpenPanel()
	model.focusedComponent = focusContext
	return model.resizeComponents()
}

func (model Model) updateComposer(message tea.Msg) Model {
	var submitted string
	model.composer.SetSubmit(func(value string) {
		submitted = value
	})
	var command tea.Cmd
	model.composer, command = model.composer.Update(message)
	model.composer.SetSubmit(nil)
	model.pendingCommand = command
	model = model.resizeComponents()
	if submitted == "" {
		return model
	}

	return model.routeSubmission(submitted)
}

func (model Model) routeSubmission(submitted string) Model {
	command := strings.TrimSpace(submitted)
	if !strings.HasPrefix(command, "/") {
		if model.nextDraftID == maxDraftID {
			model.composer.SetValue(submitted)
			model.conversation.Append(components.BlockNotice, "draft correlation exhausted; restart Yordam")
			return model
		}
		draftID := model.nextDraftID
		if draftID == 0 {
			draftID = 1
		}
		model.nextDraftID = draftID + 1
		model.pendingDraft = submitted
		model.pendingDraftID = draftID
		model = model.setTurnActive(true)
		model = model.setTurnProgress(progressWaiting)
		model.sendStartTurn(submitted, draftID)
		return model
	}

	switch command {
	case "/new":
		model.sendCommand(app.CommandNewSession)
	case "/sessions":
		model.screen = ScreenSessions
		model.focusedComponent = focusModal
	case "/mode":
		model.screen = ScreenMode
		model.focusedComponent = focusModal
	case "/model":
		model.screen = ScreenModel
		model.focusedComponent = focusModal
	case "/reload":
		if model.turnActive {
			model.conversation.Append(components.BlockNotice, "an operation is already active")
			return model
		}
		model = model.setTurnActive(true)
		model.sendCommand(app.CommandReloadConfig)
	case "/compact":
		if model.turnActive {
			model.conversation.Append(components.BlockNotice, "an operation is already active")
			return model
		}
		model = model.setTurnActive(true)
		model.sendCommand(app.CommandCompact)
	case "/help":
		model.screen = ScreenHelp
		model.focusedComponent = focusModal
	case "/quit":
		if model.turnActive {
			model.modal = ModalConfirmExit
			model.focusedComponent = focusModal
		} else {
			model.sendCommand(app.CommandShutdown)
		}
	default:
		model.conversation.Append(components.BlockNotice, "unknown command: "+command)
	}
	return model
}

func (model Model) matchesPendingDraft(event app.Event) bool {
	return model.pendingDraftID != 0 && event.DraftID == model.pendingDraftID
}

func (model Model) clearPendingDraft() Model {
	model.pendingDraft = ""
	model.pendingDraftID = 0
	return model
}

func (model Model) restorePendingDraft(event app.Event) Model {
	draft := event.Draft
	if draft == "" {
		draft = model.pendingDraft
	}
	model.composer.SetValue(draft)
	model = model.clearPendingDraft()
	return model.setTurnActive(false)
}

func (model Model) renderErrorEvent(event app.Event) Model {
	var typed *domain.TypedError
	if errors.As(event.Err, &typed) {
		message := "[" + string(typed.Kind) + "] " + typed.Message
		model.conversation.Append(components.BlockError, message)
		if typed.Kind == domain.ErrorConfigurationInvalid {
			return model
		}
		action := ""
		if typed.Kind == domain.ErrorContextTooLarge {
			action = components.RunCompactAction
		}
		model.context.ShowError(typed.Kind, typed.Message, action)
		return model.openContext()
	}
	if message := eventMessage(event); message != "" {
		model.conversation.Append(components.BlockError, message)
	}
	return model
}

func (model Model) replaceModels(models []domain.ModelSelection, selection domain.ModelSelection) Model {
	model.models = append([]domain.ModelSelection(nil), models...)
	if selection.Profile != "" {
		model.profile = selection.Profile
	}
	if selection.Model != "" {
		model.model = selection.Model
	}
	model.modelCursor = 0
	for index, configured := range model.models {
		if configured == selection {
			model.modelCursor = index
			break
		}
	}
	return model
}

func (model Model) applyDurableState(event app.Event) Model {
	if event.Mode != "" {
		model.effectiveMode = event.Mode
		switch event.Mode {
		case domain.ModeSafe:
			model.modeCursor = 0
		case domain.ModeAsk:
			model.modeCursor = 1
		case domain.ModeAuto:
			model.modeCursor = 2
		}
	}
	if event.Selection.Profile != "" {
		model.profile = event.Selection.Profile
	}
	if event.Selection.Model != "" {
		model.model = event.Selection.Model
	}
	for index, configured := range model.models {
		if configured == event.Selection {
			model.modelCursor = index
			break
		}
	}
	if event.Session.ID == "" {
		return model
	}
	model.sessionTitle = event.Session.Title
	model.sessions.Upsert(domain.SessionSummary{
		ID:        event.Session.ID,
		Title:     event.Session.Title,
		Mode:      event.Session.Mode,
		UpdatedAt: event.Session.UpdatedAt,
	})
	model.screen = ScreenConversation
	model.modal = ModalNone
	model.focusedComponent = focusComposer
	model = model.replaceConversation(event.Replay)
	return model
}

func (model Model) replaceConversation(replay domain.SessionReplay) Model {
	width, height := model.conversation.Width(), model.conversation.Height()
	wasAtBottom := model.conversation.AtBottom()
	scrollOffset := model.conversation.ScrollOffset()
	model.conversation = projectConversation(replay)
	model.conversation.SetBottomAligned(model.layout() == LayoutStream && model.modal == ModalNone)
	model.conversation.SetSize(width, height)
	if !wasAtBottom {
		model.conversation.SetScrollOffset(scrollOffset)
	}
	return model
}

func projectConversation(replay domain.SessionReplay) components.Conversation {
	conversation := components.NewConversation()
	if replay.RecoveryNote != "" {
		conversation.Append(components.BlockNotice, "RECOVERY\n"+replay.RecoveryNote)
	}
	if replay.ReadOnly {
		conversation.Append(components.BlockError, "READ-ONLY\nSession storage is corrupt; new durable events are disabled.")
	}
	toolNames := make(map[string]string)
	for _, event := range replay.Events {
		var payload domain.MessagePayload
		switch event.Kind {
		case domain.EventUserMessage:
			if json.Unmarshal(event.Payload, &payload) == nil {
				conversation.Append(components.BlockUser, payload.Content)
			}
		case domain.EventAssistantMessage:
			if json.Unmarshal(event.Payload, &payload) == nil {
				conversation.Append(components.BlockAssistant, payload.Content)
			}
		case domain.EventToolRequested:
			var requested domain.PreparedToolRequest
			if json.Unmarshal(event.Payload, &requested) == nil {
				toolNames[requested.Request.CallID] = requested.Request.Name
			}
		case domain.EventToolResult:
			var result domain.ToolResultPayload
			if json.Unmarshal(event.Payload, &result) == nil {
				conversation.CompleteTool(result.Result.CallID, toolNames[result.Result.CallID], toolStatus(result.Result.Status), result.Result.Content, result.Result.Duration, result.Result.Truncated)
			}
		case domain.EventTurnFailed, domain.EventTurnInterrupted:
			var terminal domain.TurnTerminalPayload
			if json.Unmarshal(event.Payload, &terminal) == nil {
				kind := components.BlockNotice
				if event.Kind == domain.EventTurnFailed {
					kind = components.BlockError
				}
				conversation.Append(kind, terminal.Reason)
			}
		}
	}
	return conversation
}

func (model Model) setTurnActive(active bool) Model {
	model.turnActive = active
	model.composer.SetActiveTurn(active)
	if !active {
		model = model.setTurnProgress(progressIdle)
	}
	return model
}

func (model Model) setTurnProgress(progress turnProgress) Model {
	wasIdle := model.turnProgress == progressIdle
	model.turnProgress = progress
	if progress != progressIdle && wasIdle {
		tick := model.spinner.Tick
		if model.pendingCommand == nil {
			model.pendingCommand = tick
		} else {
			model.pendingCommand = tea.Batch(model.pendingCommand, tick)
		}
	}
	return model.resizeComponents()
}

func (model Model) resizeComponents() Model {
	streamWidth := model.width
	if model.layout() == LayoutSplit {
		streamWidth = (model.width - splitGap) * 2 / 3
	}
	model.composer.SetWidth(max(1, streamWidth))
	conversationHeight := max(1, model.height-3-model.composer.Height())
	if model.turnProgress != progressIdle {
		conversationHeight = max(1, conversationHeight-1)
	}
	if model.modal != ModalNone {
		conversationHeight = max(1, conversationHeight-renderedLineCount(model.renderModal())-2)
	}
	model.conversation.SetBottomAligned(model.layout() == LayoutStream && model.modal == ModalNone)
	model.conversation.SetSize(max(1, streamWidth), conversationHeight)
	return model
}

func isRootKey(key string) bool {
	switch key {
	case "esc", "ctrl+p", "ctrl+o", "ctrl+c":
		return true
	default:
		return false
	}
}

func eventMessage(event app.Event) string {
	if event.Message != "" {
		return event.Message
	}
	if event.Err != nil {
		return event.Err.Error()
	}
	return ""
}

func toolStatus(status domain.ToolStatus) components.ToolStatus {
	switch status {
	case domain.ToolSucceeded:
		return components.ToolSucceeded
	case domain.ToolDenied:
		return components.ToolDenied
	case domain.ToolCancelled:
		return components.ToolCancelled
	default:
		return components.ToolFailed
	}
}
