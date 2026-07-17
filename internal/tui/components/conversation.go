package components

import (
	"fmt"
	"strings"
	"time"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
)

type BlockKind string

const (
	BlockUser      BlockKind = "user"
	BlockAssistant BlockKind = "assistant"
	BlockTool      BlockKind = "tool"
	BlockError     BlockKind = "error"
	BlockNotice    BlockKind = "notice"
)

type ToolStatus string

const (
	ToolRunning   ToolStatus = "running"
	ToolSucceeded ToolStatus = "completed"
	ToolFailed    ToolStatus = "failed"
	ToolDenied    ToolStatus = "denied"
	ToolCancelled ToolStatus = "cancelled"
)

type Block struct {
	ID        string
	Kind      BlockKind
	Content   string
	CallID    string
	Name      string
	Status    ToolStatus
	Duration  time.Duration
	Truncated bool
	Collapsed bool
}

type Conversation struct {
	blocks              []Block
	nextID              uint64
	viewport            viewport.Model
	selectedTool        int
	toolSelectionActive bool
	bottomAligned       bool
}

func NewConversation() Conversation {
	return Conversation{viewport: viewport.New(), selectedTool: -1, bottomAligned: true}
}

func (c *Conversation) Append(kind BlockKind, content string) string {
	block := Block{ID: c.newID(), Kind: kind, Content: content}
	c.blocks = append(c.blocks, block)
	c.refresh()
	return block.ID
}

func (c *Conversation) AppendAssistantDelta(delta string) string {
	if len(c.blocks) > 0 && c.blocks[len(c.blocks)-1].Kind == BlockAssistant {
		index := len(c.blocks) - 1
		c.blocks[index].Content += delta
		c.refresh()
		return c.blocks[index].ID
	}
	return c.Append(BlockAssistant, delta)
}

func (c *Conversation) ReplaceToolOutput(callID, name, content string, truncated bool) string {
	index := c.toolIndex(callID)
	if index < 0 {
		block := Block{
			ID:        c.newID(),
			Kind:      BlockTool,
			CallID:    callID,
			Name:      name,
			Status:    ToolRunning,
			Content:   content,
			Truncated: truncated,
			Collapsed: true,
		}
		c.blocks = append(c.blocks, block)
		c.refresh()
		return block.ID
	}

	block := &c.blocks[index]
	block.Status = ToolRunning
	if name != "" {
		block.Name = name
	}
	block.Content = content
	block.Truncated = truncated
	c.refresh()
	return block.ID
}

func (c *Conversation) CompleteTool(callID, name string, status ToolStatus, content string, duration time.Duration, truncated bool) string {
	index := c.toolIndex(callID)
	if index < 0 {
		block := Block{
			ID:        c.newID(),
			Kind:      BlockTool,
			CallID:    callID,
			Name:      name,
			Status:    status,
			Content:   content,
			Duration:  duration,
			Truncated: truncated,
			Collapsed: true,
		}
		c.blocks = append(c.blocks, block)
		c.refresh()
		return block.ID
	}

	block := &c.blocks[index]
	if name != "" {
		block.Name = name
	}
	block.Status = status
	block.Content = content
	block.Duration = duration
	block.Truncated = truncated
	c.refresh()
	return block.ID
}

func (c Conversation) Blocks() []Block {
	return append([]Block(nil), c.blocks...)
}

func (c *Conversation) SetSize(width, height int) {
	wasAtBottom := c.viewport.AtBottom()
	c.viewport.SetWidth(width)
	c.viewport.SetHeight(height)
	c.refresh()
	if wasAtBottom {
		c.viewport.GotoBottom()
	}
}

func (c Conversation) Width() int {
	return c.viewport.Width()
}

func (c Conversation) Height() int {
	return c.viewport.Height()
}

func (c Conversation) AtBottom() bool { return c.viewport.AtBottom() }

func (c Conversation) ScrollOffset() int { return c.viewport.YOffset() }

func (c *Conversation) SetScrollOffset(offset int) { c.viewport.SetYOffset(offset) }

func (c *Conversation) SetBottomAligned(enabled bool) {
	if c.bottomAligned == enabled {
		return
	}
	c.bottomAligned = enabled
	c.refresh()
}

func (c *Conversation) HandleKey(key string) bool {
	switch key {
	case "ctrl+up":
		c.viewport.ScrollUp(1)
	case "ctrl+down":
		c.viewport.ScrollDown(1)
	case "pgup":
		c.viewport.PageUp()
	case "pgdown":
		c.viewport.PageDown()
	case "ctrl+home":
		c.viewport.GotoTop()
	case "ctrl+end":
		c.viewport.GotoBottom()
	case "ctrl+e":
		index := c.selectedTool
		if !c.toolSelectionActive || index < 0 || index >= len(c.blocks) || c.blocks[index].Kind != BlockTool {
			index = c.lastToolIndex()
		}
		if index >= 0 {
			c.blocks[index].Collapsed = !c.blocks[index].Collapsed
			c.refresh()
			return true
		}
		return false
	case "alt+up", "alt+down":
		return c.moveToolSelection(key == "alt+down")
	default:
		return false
	}
	return true
}

func (c Conversation) Update(message tea.Msg) (Conversation, tea.Cmd) {
	var command tea.Cmd
	c.viewport, command = c.viewport.Update(message)
	return c, command
}

func (c Conversation) View() string {
	if c.viewport.Width() <= 0 || c.viewport.Height() <= 0 {
		return c.render()
	}
	return strings.TrimRight(c.viewport.View(), " \n")
}

func (c *Conversation) newID() string {
	c.nextID++
	return fmt.Sprintf("block-%d", c.nextID)
}

func (c Conversation) toolIndex(callID string) int {
	for index := len(c.blocks) - 1; index >= 0; index-- {
		if c.blocks[index].Kind == BlockTool && c.blocks[index].CallID == callID {
			return index
		}
	}
	return -1
}

func (c *Conversation) refresh() {
	wasAtBottom := c.viewport.AtBottom()
	content := strings.TrimRight(c.render(), "\r\n")
	lineCount := strings.Count(content, "\n") + 1
	if c.bottomAligned && content != "" && lineCount < c.viewport.Height() {
		content = strings.Repeat("\n", c.viewport.Height()-lineCount) + content
	}
	c.viewport.SetContent(content)
	if wasAtBottom {
		c.viewport.GotoBottom()
	}
}

func (c Conversation) render() string {
	blocks := make([]string, len(c.blocks))
	for index, block := range c.blocks {
		selected := c.toolSelectionActive && index == c.selectedTool && block.Kind == BlockTool
		blocks[index] = renderBlock(block, selected)
	}
	return strings.Join(blocks, "\n\n")
}

func renderBlock(block Block, selected bool) string {
	heading := strings.ToUpper(string(block.Kind))
	if block.Kind != BlockTool {
		return heading + "\n" + block.Content
	}

	name := block.Name
	if name == "" {
		name = "tool"
	}
	summary := fmt.Sprintf("TOOL %s [%s]", name, block.Status)
	if selected {
		summary = "> " + summary
	}
	if block.Duration > 0 {
		summary += " " + block.Duration.String()
	}
	if block.Truncated {
		summary += " truncated"
	}
	if block.Content == "" || block.Collapsed {
		return summary
	}
	return summary + "\n" + block.Content
}

func (c Conversation) lastToolIndex() int {
	for index := len(c.blocks) - 1; index >= 0; index-- {
		if c.blocks[index].Kind == BlockTool {
			return index
		}
	}
	return -1
}

func (c *Conversation) moveToolSelection(forward bool) bool {
	tools := make([]int, 0)
	for index := range c.blocks {
		if c.blocks[index].Kind == BlockTool {
			tools = append(tools, index)
		}
	}
	if len(tools) == 0 {
		return false
	}
	position := len(tools) - 1
	if c.toolSelectionActive {
		for index, toolIndex := range tools {
			if toolIndex == c.selectedTool {
				position = index
				break
			}
		}
	}
	if forward {
		position = (position + 1) % len(tools)
	} else {
		position = (position - 1 + len(tools)) % len(tools)
	}
	c.selectedTool = tools[position]
	c.toolSelectionActive = true
	c.refresh()
	return true
}
