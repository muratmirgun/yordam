package components

import (
	"fmt"
	"strings"
	"time"

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
	blocks := make([]string, 0, len(c.cards))
	for index, card := range c.cards {
		marker := "  "
		if index == c.cursor {
			marker = "> "
		}
		budget := time.Duration(card.ElapsedNanos).Round(time.Millisecond).String() + "/" + card.Deadline.Sub(card.StartedAt).Round(time.Millisecond).String()
		heading := fmt.Sprintf("%sCHILD %s [%s]", marker, card.AttemptID, card.State)
		if width >= 72 {
			heading += fmt.Sprintf(" | %s | session: %s | attempt %d | %s | tools %d/%d", card.Task, card.ChildSessionID, card.Attempt, budget, card.ToolCalls, card.MaxToolCalls)
			blocks = append(blocks, strings.Join(append([]string{heading}, childDetails(card)...), "\n"))
			continue
		}
		lines := []string{heading, "task: " + card.Task, "session: " + string(card.ChildSessionID), fmt.Sprintf("attempt %d | %s | tools %d/%d", card.Attempt, budget, card.ToolCalls, card.MaxToolCalls)}
		blocks = append(blocks, strings.Join(append(lines, childDetails(card)...), "\n"))
	}
	if len(blocks) == 0 {
		return ""
	}
	return strings.Join(blocks, "\n\n") + "\nopen: Alt+Enter"
}

func childDetails(card protocol.SubagentCardV1) []string {
	lines := make([]string, 0, 5)
	if card.ReceiptSummary != "" {
		lines = append(lines, "summary: "+card.ReceiptSummary)
	}
	if len(card.ChangedFiles) != 0 {
		lines = append(lines, "changed: "+strings.Join(card.ChangedFiles, ", "))
	}
	if len(card.CommandsAndTests) != 0 {
		lines = append(lines, "tests: "+strings.Join(card.CommandsAndTests, ", "))
	}
	if card.Warning != "" {
		lines = append(lines, "WARNING: "+card.Warning)
	}
	return lines
}
