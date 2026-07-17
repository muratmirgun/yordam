package components

import (
	"strings"

	"charm.land/bubbles/v2/textarea"
	tea "charm.land/bubbletea/v2"
)

type SubmitFunc func(string)

type Composer struct {
	input      textarea.Model
	submit     SubmitFunc
	activeTurn bool
}

func NewComposer(submit SubmitFunc) Composer {
	input := textarea.New()
	input.Placeholder = "Ask Yordam"
	input.ShowLineNumbers = false
	input.DynamicHeight = true
	input.MinHeight = 1
	input.MaxHeight = 8
	input.SetHeight(input.MinHeight)
	input.SetStyles(textarea.Styles{
		Focused: textarea.StyleState{},
		Blurred: textarea.StyleState{},
		Cursor:  textarea.CursorStyle{Shape: tea.CursorBar, Blink: true},
	})
	input.SetVirtualCursor(false)
	input.Focus()
	return Composer{input: input, submit: submit}
}

func (c *Composer) SetValue(value string) {
	c.input.SetValue(value)
}

func (c Composer) Value() string {
	return c.input.Value()
}

func (c *Composer) SetActiveTurn(active bool) {
	c.activeTurn = active
}

func (c Composer) ActiveTurn() bool {
	return c.activeTurn
}

func (c *Composer) SetSubmit(submit SubmitFunc) {
	c.submit = submit
}

func (c *Composer) SetWidth(width int) {
	c.input.SetWidth(width)
}

func (c Composer) Height() int {
	return c.input.Height()
}

func (c Composer) Width() int {
	return c.input.Width()
}

func (c Composer) Cursor() *tea.Cursor {
	return c.input.Cursor()
}

func (c *Composer) Insert(value string) {
	c.input.InsertString(value)
}

func (c Composer) Update(message tea.Msg) (Composer, tea.Cmd) {
	if key, ok := message.(tea.KeyPressMsg); ok {
		switch key.String() {
		case "ctrl+j":
			c.input.InsertString("\n")
			return c, nil
		case "enter":
			if c.activeTurn {
				return c, nil
			}
			value := trimOuterBlankLines(c.input.Value())
			if value == "" {
				return c, nil
			}
			if c.submit != nil {
				c.submit(value)
			}
			c.input.Reset()
			return c, nil
		}
	}

	var command tea.Cmd
	c.input, command = c.input.Update(message)
	return c, command
}

func (c Composer) View() string {
	lines := strings.Split(strings.TrimSuffix(c.input.View(), "\n"), "\n")
	for index := range lines {
		lines[index] = strings.TrimRight(lines[index], " ")
	}
	return strings.Join(lines, "\n")
}

func trimOuterBlankLines(value string) string {
	lines := strings.Split(value, "\n")
	start := 0
	for start < len(lines) && strings.TrimSpace(lines[start]) == "" {
		start++
	}
	end := len(lines)
	for end > start && strings.TrimSpace(lines[end-1]) == "" {
		end--
	}
	return strings.Join(lines[start:end], "\n")
}
