package tui

import (
	"strings"

	tea "charm.land/bubbletea/v2"
	lipgloss "charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
)

const splitGap = 3

func (model Model) View() tea.View {
	rendered := model.render()
	view := tea.NewView(rendered)
	view.AltScreen = true
	if model.screen == ScreenConversation && model.layout() != LayoutContextOnly && model.modal == ModalNone && model.focusedComponent == focusComposer {
		if cursor := model.composer.Cursor(); cursor != nil {
			cursor.Position.Y += 3 + renderedLineCount(model.renderConversation(model.renderWidth())) + model.progressHeight()
			view.Cursor = cursor
		}
	}
	return view
}

func (model Model) render() string {
	width := model.renderWidth()

	status := model.renderStatus(width)
	separator := strings.Repeat("-", width)
	body := model.renderBody(width)

	if model.modal != ModalNone {
		body += "\n\n" + model.renderModal()
	}
	return status + "\n" + separator + "\n" + body + "\n"
}

func (model Model) renderWidth() int {
	if model.width > 0 {
		return model.width
	}
	return 80
}

func (model Model) renderBody(width int) string {
	if model.screen != ScreenConversation {
		return renderPanel(model.renderScreen(), width)
	}
	separator := strings.Repeat("-", width)
	body := model.renderConversation(width)
	if model.layout() != LayoutContextOnly {
		if progress := model.renderProgress(); progress != "" {
			body += "\n" + progress
		}
		body += "\n" + separator + "\n" + model.composer.View()
	}
	return body
}

func (model Model) renderProgress() string {
	if stage := model.context.CompactionStage(); stage != "" && model.turnActive {
		return model.spinner.View() + " Compacting context: " + string(stage) + "..."
	}
	var label string
	switch model.turnProgress {
	case progressWaiting:
		label = "Waiting for response..."
	case progressReceiving:
		label = "Receiving response..."
	case progressTool:
		label = "Running tool..."
	default:
		return ""
	}
	return model.spinner.View() + " " + label
}

func (model Model) progressHeight() int {
	if model.renderProgress() == "" || model.layout() == LayoutContextOnly {
		return 0
	}
	return 1
}

func (model Model) renderScreen() string {
	switch model.screen {
	case ScreenSessions:
		lines := []string{"SESSIONS", "Filter: " + model.sessions.Filter()}
		selected := model.sessions.Select()
		for _, session := range model.sessions.Visible() {
			marker := "  "
			if session.ID == selected {
				marker = "> "
			}
			lines = append(lines, marker+session.Title+" ["+session.ID+"]")
		}
		return strings.Join(append(lines, "Enter: open | Esc: cancel"), "\n")
	case ScreenMode:
		lines := []string{"MODE"}
		for index, mode := range []string{"safe", "ask", "auto"} {
			marker := "  "
			if index == model.modeCursor {
				marker = "> "
			}
			lines = append(lines, marker+mode)
		}
		return strings.Join(append(lines, "Enter: select | Esc: cancel"), "\n")
	case ScreenModel:
		lines := []string{"MODEL"}
		for index, selection := range model.models {
			marker := "  "
			if index == model.modelCursor {
				marker = "> "
			}
			lines = append(lines, marker+selection.Profile+"/"+selection.Model)
		}
		return strings.Join(append(lines, "Enter: select | Esc: cancel"), "\n")
	case ScreenHelp:
		return "HELP\n/new /sessions /mode /model /reload /compact /help /quit\nCtrl+P: commands | Ctrl+O: context | Ctrl+C: quit | Esc: back"
	default:
		return ""
	}
}

func (model Model) renderConversation(width int) string {
	stream := model.conversation.View()
	if len(model.conversation.Blocks()) == 0 {
		stream = "Conversation"
	}
	var body string
	switch model.layout() {
	case LayoutContextOnly:
		body = renderPanel(model.context.View(), width)
	case LayoutSplit:
		leftWidth := (width - splitGap) * 2 / 3
		rightWidth := width - splitGap - leftWidth
		body = joinPanels(stream, leftWidth, model.context.View(), rightWidth)
	default:
		body = renderPanel(stream, width)
	}
	return body
}

func renderedLineCount(value string) int { return strings.Count(value, "\n") + 1 }

func (model Model) renderStatus(width int) string {
	prefix := model.sessionTitle + " | "
	suffix := " | mode: " + string(model.effectiveMode) + " | " + model.profile + "/" + model.model
	workspaceWidth := width - lipgloss.Width(prefix) - lipgloss.Width(suffix)
	workspace := truncateMiddle(model.canonicalWorkspace, workspaceWidth)
	return truncateEnd(prefix+workspace+suffix, width)
}

func (model Model) renderModal() string {
	switch model.modal {
	case ModalPermission:
		return model.permission.View()
	case ModalCommandPalette:
		lines := []string{"COMMANDS", "Filter: " + model.palette.Filter()}
		selected := model.palette.Select()
		for _, item := range model.palette.Visible() {
			marker := "  "
			if item.Name == selected.Name {
				marker = "> "
			}
			lines = append(lines, marker+item.Name+" - "+item.Description)
		}
		return strings.Join(append(lines, "Enter: select | Esc: close"), "\n")
	case ModalConfirmExit:
		return "[confirm exit] Cancel active turn and exit? y/N"
	default:
		return ""
	}
}

func joinPanels(left string, leftWidth int, right string, rightWidth int) string {
	leftLines := renderLines(left, leftWidth)
	rightLines := renderLines(right, rightWidth)
	height := max(len(leftLines), len(rightLines))
	rows := make([]string, height)
	for index := range height {
		var leftLine, rightLine string
		if index < len(leftLines) {
			leftLine = leftLines[index]
		}
		if index < len(rightLines) {
			rightLine = rightLines[index]
		}
		rows[index] = strings.TrimRight(fitLine(leftLine, leftWidth)+" | "+truncateEnd(rightLine, rightWidth), " ")
	}
	return strings.Join(rows, "\n")
}

func renderPanel(content string, width int) string {
	return strings.Join(renderLines(content, width), "\n")
}

func renderLines(content string, width int) []string {
	lines := strings.Split(content, "\n")
	for index, line := range lines {
		lines[index] = truncateEnd(line, width)
	}
	return lines
}

func fitLine(value string, width int) string {
	value = truncateEnd(value, width)
	return value + strings.Repeat(" ", max(0, width-lipgloss.Width(value)))
}

func truncateMiddle(value string, width int) string {
	if width <= 0 {
		return ""
	}
	if lipgloss.Width(value) <= width {
		return value
	}
	if width == 1 {
		return "."
	}
	left := (width - 1) / 2
	right := width - 1 - left
	rightValue := ansi.TruncateLeft(value, lipgloss.Width(value)-right, "")
	return ansi.Truncate(value, left, "") + "~" + rightValue
}

func truncateEnd(value string, width int) string {
	if width <= 0 {
		return ""
	}
	if lipgloss.Width(value) <= width {
		return value
	}
	return ansi.Truncate(value, width, "~")
}
