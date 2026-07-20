package components_test

import (
	"strings"
	"testing"
	"time"

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
	for _, want := range []string{"CHILD attempt-1 [running]", "inspect recovery", "child-1", "attempt 1", "3s/10s", "tools 2/4", "CHILD attempt-2 [uncertain]", "bounded summary", "internal/app/runtime.go", "go test ./internal/app", "WARNING: effect may have occurred"} {
		if !strings.Contains(wide, want) {
			t.Fatalf("wide child cards missing %q:\n%s", want, wide)
		}
	}
	narrow := cards.View(38)
	if narrow == wide || !strings.Contains(narrow, "session: child-1") || !strings.Contains(narrow, "open: Alt+Enter") {
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
	for _, state := range states {
		if !strings.Contains(view.View(80), "["+string(state)+"]") {
			t.Fatalf("missing state %q", state)
		}
	}
	view.Move(1)
	if got := view.SelectedSession(); got != "child-b" {
		t.Fatalf("selected child=%q want child-b", got)
	}
}
