package components

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/muratmirgun/yordam/internal/domain"
)

type ContextTab string

const (
	ContextDiff  ContextTab = "diff"
	ContextTool  ContextTab = "tool"
	ContextError ContextTab = "error"

	RunCompactAction = "Run /compact"
)

var contextTabs = [...]ContextTab{ContextDiff, ContextTool, ContextError}

type Context struct {
	active   ContextTab
	expanded bool
	open     bool
	diff     string
	tool     string
	error    string
}

func NewContext() Context {
	return Context{
		active:   ContextDiff,
		expanded: true,
	}
}

func (c Context) Open() bool {
	return c.open
}

func (c *Context) OpenPanel() {
	c.open = true
}

func (c *Context) Close() {
	c.open = false
}

func (c Context) ActiveTab() ContextTab {
	return c.active
}

func (c Context) Expanded() bool {
	return c.expanded
}

func (c *Context) ShowDiff(name, diff string) {
	c.diff = strings.TrimSpace("DIFF " + name + "\n" + diff)
	c.show(ContextDiff)
}

func (c *Context) ShowTool(name, detail string) {
	c.tool = strings.TrimSpace("TOOL " + name + "\n" + detail)
	c.show(ContextTool)
}

func (c *Context) ShowError(kind domain.ErrorKind, message, action string) {
	body := fmt.Sprintf("ERROR %s\n%s", kind, message)
	if action != "" {
		body += "\n\n" + action
	}
	c.error = strings.TrimSpace(body)
	c.show(ContextError)
}

func (c *Context) ShowWorkspaceChanges(changes *domain.WorkspaceChanges) {
	if changes == nil {
		return
	}
	if !changes.IsGit {
		c.ShowTool("shell changes", changes.Notice)
		return
	}

	status := changes.Status
	if strings.TrimSpace(status) == "" {
		status = "(clean)"
	}
	diff := changes.Diff
	if strings.TrimSpace(diff) == "" {
		diff = "(no unstaged diff)"
	}
	c.ShowTool("shell changes", "Git status\n"+status+"\nGit diff\n"+diff)
}

func (c *Context) show(tab ContextTab) {
	c.active = tab
	c.expanded = true
	c.open = true
}

func (c *Context) Update(message tea.Msg) {
	key, ok := message.(tea.KeyPressMsg)
	if !ok {
		return
	}
	c.HandleKey(key.String())
}

func (c *Context) HandleKey(key string) bool {
	switch key {
	case "esc":
		c.Close()
		return true
	case "enter":
		c.expanded = !c.expanded
		return true
	case "left", "shift+tab":
		c.moveTab(-1)
		return true
	case "right", "tab":
		c.moveTab(1)
		return true
	case "d":
		c.active = ContextDiff
		return true
	case "t":
		c.active = ContextTool
		return true
	case "e":
		c.active = ContextError
		return true
	default:
		return false
	}
}

func (c *Context) moveTab(delta int) {
	for index, tab := range contextTabs {
		if tab == c.active {
			c.active = contextTabs[(index+delta+len(contextTabs))%len(contextTabs)]
			return
		}
	}
	c.active = ContextDiff
}

func (c Context) View() string {
	labels := make([]string, len(contextTabs))
	for index, tab := range contextTabs {
		labels[index] = string(tab)
		if tab == c.active {
			labels[index] = "[" + labels[index] + "]"
		}
	}
	header := strings.Join(labels, " | ")
	if !c.expanded {
		return header + "\nEnter: expand | Esc: close"
	}
	var body string
	switch c.active {
	case ContextDiff:
		body = c.diff
	case ContextTool:
		body = c.tool
	case ContextError:
		body = c.error
	}
	if body == "" {
		body = "Context"
	}
	return header + "\n" + body + "\nEnter: collapse | Esc: close"
}
