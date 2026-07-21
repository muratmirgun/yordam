package components

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/x/ansi"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type ChildCards struct {
	cards  []protocol.SubagentCardV1
	cursor int
}

func NewChildCards(cards []protocol.SubagentCardV1) ChildCards {
	return ChildCards{cards: protocol.DeepCopy(cards)}
}

func (c *ChildCards) Set(cards []protocol.SubagentCardV1) {
	selected := c.SelectedSession()
	c.cards = protocol.DeepCopy(cards)
	c.cursor = 0
	for index := range c.cards {
		if c.cards[index].ChildSessionID == selected {
			c.cursor = index
			break
		}
	}
}

func (c *ChildCards) Move(delta int) {
	if len(c.cards) == 0 {
		c.cursor = 0
		return
	}
	c.cursor = (c.cursor + delta + len(c.cards)) % len(c.cards)
}

func (c ChildCards) SelectedSession() protocol.SessionID {
	if len(c.cards) == 0 {
		return ""
	}
	return c.cards[min(max(c.cursor, 0), len(c.cards)-1)].ChildSessionID
}

func (c ChildCards) Cards() []protocol.SubagentCardV1 { return protocol.DeepCopy(c.cards) }

func (c *ChildCards) ApplyStage(stage protocol.SubagentStageV1) {
	for index := range c.cards {
		if c.cards[index].AttemptID == stage.AttemptID && c.cards[index].ChildSessionID == stage.ChildSessionID && c.cards[index].ParentSessionID == stage.ParentSessionID {
			if stage.Stage != protocol.SubagentStageAttached {
				c.cards[index].State = stage.Stage
			}
			return
		}
	}
}

func (c ChildCards) View(width int) string {
	if len(c.cards) == 0 {
		return ""
	}
	width = max(width, 1)
	index := min(max(c.cursor, 0), len(c.cards)-1)
	card := c.cards[index]
	budget := time.Duration(card.ElapsedNanos).Round(time.Millisecond).String() + "/" + card.Deadline.Sub(card.StartedAt).Round(time.Millisecond).String()
	lines := []string{
		fmt.Sprintf("card %d/%d", index+1, len(c.cards)),
		fmt.Sprintf("CHILD %s [%s]", card.AttemptID, card.State),
		"session: " + string(card.ChildSessionID),
		fmt.Sprintf("attempt %d | %s | tools %d/%d", card.Attempt, budget, card.ToolCalls, card.MaxToolCalls),
		"task: " + card.Task,
	}
	lines = append(lines, childDetails(card)...)
	lines = append(lines, "Alt+[/Alt+] cards | Alt+Enter open")
	for lineIndex := range lines {
		lines[lineIndex] = truncateChildLine(lines[lineIndex], width)
	}
	return strings.Join(lines, "\n")
}

func childDetails(card protocol.SubagentCardV1) []string {
	lines := make([]string, 0, 6)
	if card.ReceiptSummary != "" {
		lines = append(lines, "summary: "+card.ReceiptSummary)
	}
	for index, value := range card.ChangedFiles {
		if index == 2 {
			break
		}
		lines = append(lines, "changed: "+value)
	}
	for index, value := range card.CommandsAndTests {
		if index == 2 {
			break
		}
		lines = append(lines, "tests: "+value)
	}
	if card.Warning != "" {
		lines = append(lines, "WARNING: "+card.Warning)
	}
	return lines
}

func truncateChildLine(value string, width int) string {
	return ansi.Truncate(value, width, "…")
}
