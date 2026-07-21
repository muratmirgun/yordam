package components_test

import (
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/tui/components"
)

func TestChildCardsRenderLifecycleBudgetsAndBoundedTerminalEvidence(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	cards := components.NewChildCards([]protocol.SubagentCardV1{
		{AttemptID: "attempt-1", ParentSessionID: "parent", ChildSessionID: "child-1", Task: "inspect recovery", State: protocol.SubagentStageRunning, Attempt: 1, StartedAt: now.Add(-3 * time.Second), Deadline: now.Add(7 * time.Second), ElapsedNanos: int64(3 * time.Second), ToolCalls: 2, MaxToolCalls: 4},
		{AttemptID: "attempt-2", ParentSessionID: "parent", ChildSessionID: "child-2", Task: "run checks", State: protocol.SubagentStageUncertain, Attempt: 2, StartedAt: now.Add(-8 * time.Second), Deadline: now, ElapsedNanos: int64(8 * time.Second), ToolCalls: 4, MaxToolCalls: 4, ReceiptSummary: "bounded summary", ChangedFiles: []string{"internal/app/runtime.go"}, CommandsAndTests: []string{"go test ./internal/app"}, Warning: "effect may have occurred"},
	})

	wide := cards.View(100)
	for _, want := range []string{"card 1/2", "CHILD attempt-1 [running]", "inspect recovery", "child-1", "attempt 1", "3s/10s", "tools 2/4"} {
		if !strings.Contains(wide, want) {
			t.Fatalf("wide child cards missing %q:\n%s", want, wide)
		}
	}
	cards.Move(1)
	wide = cards.View(100)
	for _, want := range []string{"card 2/2", "CHILD attempt-2 [uncertain]", "bounded summary", "internal/app/runtime.go", "go test ./internal/app", "WARNING: effect may have occurred"} {
		if !strings.Contains(wide, want) {
			t.Fatalf("wide terminal child card missing %q:\n%s", want, wide)
		}
	}
	narrow := cards.View(38)
	if !strings.Contains(narrow, "session: child-2") || !strings.Contains(narrow, "Alt+Enter open") {
		t.Fatalf("narrow child cards did not stack:\n%s", narrow)
	}
}

func TestChildCardsCoverEveryDurableStateAndSelectNavigationTarget(t *testing.T) {
	states := []protocol.SubagentStage{
		protocol.SubagentStageRequested, protocol.SubagentStageWaiting, protocol.SubagentStageRunning,
		protocol.SubagentStageSucceeded, protocol.SubagentStageFailed, protocol.SubagentStageCancelled,
		protocol.SubagentStageUncertain,
	}
	cards := make([]protocol.SubagentCardV1, 0, len(states))
	for index, state := range states {
		cards = append(cards, protocol.SubagentCardV1{AttemptID: protocol.DelegationAttemptID("attempt-" + string(rune('a'+index))), ParentSessionID: "parent", ChildSessionID: protocol.SessionID("child-" + string(rune('a'+index))), Task: "task", State: state, Attempt: index + 1, StartedAt: time.Unix(1, 0).UTC(), Deadline: time.Unix(2, 0).UTC(), MaxToolCalls: 1})
	}
	view := components.NewChildCards(cards)
	for index, state := range states {
		if !strings.Contains(view.View(80), "["+string(state)+"]") {
			t.Fatalf("missing state %q", state)
		}
		if index+1 < len(states) {
			view.Move(1)
		}
	}
	view.Move(2)
	if got := view.SelectedSession(); got != "child-b" {
		t.Fatalf("selected child=%q want child-b", got)
	}
}

func TestChildCardsRenderOnlySelectedCardWithinWidthAndHeightBounds(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	long := strings.Repeat("very-long-value-", 200)
	cards := make([]protocol.SubagentCardV1, 20)
	for index := range cards {
		cards[index] = protocol.SubagentCardV1{
			AttemptID: protocol.DelegationAttemptID(fmt.Sprintf("attempt-%02d", index+1)), ParentSessionID: "parent",
			ChildSessionID: protocol.SessionID(fmt.Sprintf("123e4567-e89b-12d3-a456-%012d", index)), Task: long,
			State: protocol.SubagentStageSucceeded, Attempt: index%protocol.MaxSubagentAttemptsPerTurn + 1,
			StartedAt: now.Add(-time.Second), Deadline: now.Add(time.Second), ElapsedNanos: int64(time.Second), ToolCalls: 1, MaxToolCalls: 4,
			ReceiptSummary: long, ChangedFiles: []string{long, long + "b", long + "c", long + "d"}, CommandsAndTests: []string{long, long + "2", long + "3", long + "4"},
		}
	}
	view := components.NewChildCards(cards)
	view.Move(12)
	for _, width := range []int{38, 100} {
		rendered := view.View(width)
		lines := strings.Split(rendered, "\n")
		if len(lines) > 12 {
			t.Fatalf("width %d rendered %d lines:\n%s", width, len(lines), rendered)
		}
		for _, line := range lines {
			if utf8.RuneCountInString(line) > width {
				t.Fatalf("width %d line has %d runes: %q", width, utf8.RuneCountInString(line), line)
			}
		}
		for _, want := range []string{"card 13/20", "attempt-13", "session:", "attempt 1", "1s/2s", "tools 1/4", "Alt+Enter", "Alt+[/Alt+]"} {
			if !strings.Contains(rendered, want) {
				t.Fatalf("width %d missing %q:\n%s", width, want, rendered)
			}
		}
		if strings.Contains(rendered, "attempt-12") || strings.Contains(rendered, "attempt-14") {
			t.Fatalf("width %d rendered non-selected card:\n%s", width, rendered)
		}
	}
}

func TestChildCardsWideViewKeepsIdentityWithLongTask(t *testing.T) {
	card := protocol.SubagentCardV1{AttemptID: "attempt-identity", ParentSessionID: "parent", ChildSessionID: "123e4567-e89b-12d3-a456-426614174000", Task: strings.Repeat("task ", 100), State: protocol.SubagentStageRunning, Attempt: 4, StartedAt: time.Unix(1, 0).UTC(), Deadline: time.Unix(3, 0).UTC(), ElapsedNanos: int64(time.Second), ToolCalls: 3, MaxToolCalls: 4}
	rendered := components.NewChildCards([]protocol.SubagentCardV1{card}).View(120)
	for _, want := range []string{"attempt-identity", "123e4567-e89b-12d3-a456-426614174000", "attempt 4", "1s/2s", "tools 3/4"} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q:\n%s", want, rendered)
		}
	}
}
