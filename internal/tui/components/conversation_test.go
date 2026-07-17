package components_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/tui/components"
)

func TestConversationStreamingDeltasAppendOneAssistantBlock(t *testing.T) {
	conversation := components.NewConversation()
	userID := conversation.Append(components.BlockUser, "hello")
	assistantID := conversation.AppendAssistantDelta("hel")
	if got := conversation.AppendAssistantDelta("lo"); got != assistantID {
		t.Fatalf("second delta id=%q want=%q", got, assistantID)
	}

	blocks := conversation.Blocks()
	if len(blocks) != 2 {
		t.Fatalf("blocks=%+v", blocks)
	}
	if blocks[0].ID != userID || blocks[0].Kind != components.BlockUser {
		t.Fatalf("user block=%+v", blocks[0])
	}
	if blocks[1].ID != assistantID || blocks[1].Kind != components.BlockAssistant || blocks[1].Content != "hello" {
		t.Fatalf("assistant block=%+v", blocks[1])
	}
}

func TestConversationToolBlocksKeepStableIdentityAndStartCollapsed(t *testing.T) {
	conversation := components.NewConversation()
	id := conversation.ReplaceToolOutput("call-1", "shell", "first", false)
	if got := conversation.ReplaceToolOutput("call-1", "shell", "second", true); got != id {
		t.Fatalf("updated tool id=%q want=%q", got, id)
	}

	blocks := conversation.Blocks()
	if len(blocks) != 1 {
		t.Fatalf("blocks=%+v", blocks)
	}
	if block := blocks[0]; block.ID != id || block.Kind != components.BlockTool || !block.Collapsed || block.Name != "shell" || block.Status != components.ToolRunning || block.Content != "second" || !block.Truncated {
		t.Fatalf("live tool block=%+v", block)
	}

	if got := conversation.CompleteTool("call-1", "shell", components.ToolSucceeded, "done", 1500*time.Millisecond, false); got != id {
		t.Fatalf("completed tool id=%q want=%q", got, id)
	}
	block := conversation.Blocks()[0]
	if block.Status != components.ToolSucceeded || block.Content != "done" || block.Duration != 1500*time.Millisecond || block.Truncated {
		t.Fatalf("completed tool block=%+v", block)
	}
}

func TestConversationNewAssistantBlockAfterTool(t *testing.T) {
	conversation := components.NewConversation()
	first := conversation.AppendAssistantDelta("before")
	conversation.CompleteTool("call-1", "read", components.ToolSucceeded, "result", time.Second, false)
	second := conversation.AppendAssistantDelta("after")

	if first == second {
		t.Fatalf("assistant block reused across tool: %q", first)
	}
	blocks := conversation.Blocks()
	if len(blocks) != 3 || blocks[2].Content != "after" {
		t.Fatalf("blocks=%+v", blocks)
	}
}

func TestConversationCompletedToolExpandsToRenderDetail(t *testing.T) {
	conversation := components.NewConversation()
	conversation.CompleteTool("call-1", "read", components.ToolSucceeded, "internal/app/app.go:1-40", 1500*time.Millisecond, true)
	conversation.HandleKey("ctrl+e")

	view := conversation.View()
	for _, required := range []string{
		"TOOL read [completed] 1.5s truncated",
		"internal/app/app.go:1-40",
	} {
		if !strings.Contains(view, required) {
			t.Fatalf("collapsed completed tool missing %q:\n%s", required, view)
		}
	}
}

func TestConversationBlocksReturnsSnapshot(t *testing.T) {
	conversation := components.NewConversation()
	conversation.Append(components.BlockNotice, "notice")
	blocks := conversation.Blocks()
	blocks[0].Content = "changed"

	if got := conversation.Blocks()[0].Content; got != "notice" {
		t.Fatalf("internal block changed through snapshot: %q", got)
	}
}

func TestConversationBottomAlignsShortContent(t *testing.T) {
	conversation := components.NewConversation()
	conversation.SetSize(40, 6)
	conversation.Append(components.BlockNotice, "latest")

	lines := strings.Split(conversation.View(), "\n")
	if len(lines) != 6 || strings.Join(lines[4:], "\n") != "NOTICE                                  \nlatest" {
		t.Fatalf("bottom-aligned lines=%q", lines)
	}
	for index, line := range lines[:4] {
		if strings.TrimSpace(line) != "" {
			t.Fatalf("padding line %d=%q", index, line)
		}
	}
}

func TestConversationBottomAlignmentIgnoresTrailingNewlines(t *testing.T) {
	conversation := components.NewConversation()
	conversation.SetSize(40, 6)
	conversation.Append(components.BlockNotice, "latest\n")

	lines := strings.Split(conversation.View(), "\n")
	if len(lines) != 6 || strings.TrimSpace(lines[4]) != "NOTICE" || lines[5] != "latest" {
		t.Fatalf("bottom-aligned lines=%q", lines)
	}
}

func TestConversationStaysAtBottomWhenViewportShrinks(t *testing.T) {
	conversation := components.NewConversation()
	conversation.SetSize(40, 6)
	for index := range 10 {
		conversation.Append(components.BlockNotice, fmt.Sprintf("notice-%d", index))
	}
	if !conversation.AtBottom() {
		t.Fatal("conversation did not start at bottom")
	}

	conversation.SetSize(40, 5)
	if !conversation.AtBottom() || !strings.Contains(conversation.View(), "notice-9") {
		t.Fatalf("resized conversation lost bottom: %q", conversation.View())
	}
}
