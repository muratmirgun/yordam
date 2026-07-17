package tui

import (
	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/tui/components"
)

func (model Model) handleKey(key string) Model {
	if model.modal != ModalNone {
		return model.handleModalKey(key)
	}

	switch key {
	case "ctrl+p":
		model.palette = components.NewPalette()
		model.modal = ModalCommandPalette
		model.focusedComponent = focusModal
		return model.resizeComponents()
	case "ctrl+o":
		model.screen = ScreenConversation
		model.contextOpen = !model.contextOpen
		if model.contextOpen {
			model.context.OpenPanel()
			model.focusedComponent = focusContext
		} else {
			model.context.Close()
			model.focusedComponent = focusComposer
		}
		model = model.resizeComponents()
		return model
	case "ctrl+c":
		if model.turnActive {
			model.modal = ModalConfirmExit
			model.focusedComponent = focusModal
			return model.resizeComponents()
		}
		model.sendCommand(app.CommandShutdown)
		return model
	case "ctrl+up", "ctrl+down", "pgup", "pgdown", "ctrl+home", "ctrl+end", "ctrl+e", "alt+up", "alt+down":
		if model.screen == ScreenConversation && !model.contextOpen {
			model.conversation.HandleKey(key)
			return model
		}
	}
	if model.screen != ScreenConversation {
		return model.handleScreenKey(key)
	}

	switch key {
	case "esc":
		if model.contextOpen {
			return model.closeContext()
		}
		if model.turnActive {
			return model.cancelTurn()
		}
	default:
		if model.contextOpen && model.focusedComponent == focusContext {
			model.context.HandleKey(key)
		}
	}
	return model
}

func (model Model) handleModalKey(key string) Model {
	if model.modal == ModalPermission && model.permission.HandleKey(key) {
		model.modal = ModalNone
		return model.restoreFocus().resizeComponents()
	}
	if model.modal == ModalConfirmExit && key == "y" {
		model = model.cancelTurn()
		model.sendCommand(app.CommandShutdown)
		model.modal = ModalNone
		model.focusedComponent = focusComposer
		return model.resizeComponents()
	}
	if model.modal == ModalCommandPalette {
		switch key {
		case "up":
			model.palette.Move(-1)
			return model
		case "down":
			model.palette.Move(1)
			return model
		case "enter":
			selected := model.palette.Select()
			if selected.Name == "" {
				return model
			}
			model.modal = ModalNone
			model = model.restoreFocus()
			return model.routeSubmission(selected.Name).resizeComponents()
		case "esc":
		default:
			model.palette.SetFilter(editPickerFilter(model.palette.Filter(), key))
			return model
		}
	}
	if key == "esc" || (model.modal == ModalConfirmExit && key == "n") {
		model.modal = ModalNone
		model = model.restoreFocus()
		model = model.resizeComponents()
	}
	return model
}

func (model Model) handleScreenKey(key string) Model {
	if key == "esc" {
		model.screen = ScreenConversation
		model.focusedComponent = focusComposer
		return model
	}

	switch model.screen {
	case ScreenSessions:
		switch key {
		case "up":
			model.sessions.Move(-1)
		case "down":
			model.sessions.Move(1)
		case "enter":
			if sessionID := model.sessions.Select(); sessionID != "" {
				model.sendAppCommand(app.Command{Kind: app.CommandOpenSession, SessionID: sessionID})
				model.screen = ScreenConversation
				model.focusedComponent = focusComposer
			}
		default:
			model.sessions.SetFilter(editPickerFilter(model.sessions.Filter(), key))
		}
	case ScreenMode:
		switch key {
		case "up":
			model.modeCursor = (model.modeCursor + 2) % 3
		case "down":
			model.modeCursor = (model.modeCursor + 1) % 3
		case "enter":
			modes := [...]domain.PermissionMode{domain.ModeSafe, domain.ModeAsk, domain.ModeAuto}
			model.sendAppCommand(app.Command{Kind: app.CommandChangeMode, Mode: modes[model.modeCursor]})
			model.screen = ScreenConversation
			model.focusedComponent = focusComposer
		}
	case ScreenModel:
		if len(model.models) == 0 {
			return model
		}
		switch key {
		case "up":
			model.modelCursor = (model.modelCursor - 1 + len(model.models)) % len(model.models)
		case "down":
			model.modelCursor = (model.modelCursor + 1) % len(model.models)
		case "enter":
			model.sendAppCommand(app.Command{Kind: app.CommandChangeModel, Selection: model.models[model.modelCursor]})
			model.screen = ScreenConversation
			model.focusedComponent = focusComposer
		}
	case ScreenHelp:
		if key == "enter" {
			model.screen = ScreenConversation
			model.focusedComponent = focusComposer
		}
	}
	return model
}

func editPickerFilter(filter, key string) string {
	if key == "backspace" {
		runes := []rune(filter)
		if len(runes) > 0 {
			return string(runes[:len(runes)-1])
		}
		return filter
	}
	if key == "space" {
		return filter + " "
	}
	if len([]rune(key)) == 1 {
		return filter + key
	}
	return filter
}

func (model Model) restoreFocus() Model {
	if model.screen != ScreenConversation {
		model.focusedComponent = focusModal
		return model
	}
	if model.contextOpen {
		model.focusedComponent = focusContext
	} else {
		model.focusedComponent = focusComposer
	}
	return model
}

func (model Model) closeContext() Model {
	model.contextOpen = false
	model.context.Close()
	model.focusedComponent = focusComposer
	return model.resizeComponents()
}

func (model Model) cancelTurn() Model {
	model.sendCommand(app.CommandCancelTurn)
	model.cancelSent = true
	return model
}

func (model Model) sendCommand(kind app.CommandKind) {
	model.sendAppCommand(app.Command{Kind: kind})
}

func (model Model) sendAppCommand(command app.Command) {
	if model.commands != nil {
		model.commands <- command
	}
}

func (model Model) sendStartTurn(prompt string) {
	if model.commands != nil {
		model.commands <- app.Command{Kind: app.CommandStartTurn, Prompt: prompt}
	}
}

func permissionResolutionCommand(callID string, decision domain.PermissionDecision) app.Command {
	return app.Command{Kind: app.CommandResolvePermission, CallID: callID, Decision: decision}
}
