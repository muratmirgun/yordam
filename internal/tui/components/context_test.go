package components_test

import (
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/tui/components"
	"github.com/muratmirgun/yordam/internal/workspace"
)

func TestContextTabsAndExpansion(t *testing.T) {
	panel := components.NewContext()
	panel.ShowDiff("file.go", "@@ -1 +1 @@\n-old\n+new")
	panel.ShowTool("shell", "command output")
	panel.ShowError(domain.ErrorToolFailed, "tool failed", "")

	if got := panel.ActiveTab(); got != components.ContextError {
		t.Fatalf("active tab=%q", got)
	}
	for _, want := range []string{"diff", "tool", "error", "tool_failed", "tool failed"} {
		if view := panel.View(); !strings.Contains(view, want) {
			t.Fatalf("view missing %q:\n%s", want, view)
		}
	}

	panel.Update(permissionKey("enter"))
	if panel.Expanded() || strings.Contains(panel.View(), "tool failed") {
		t.Fatalf("context did not collapse:\n%s", panel.View())
	}
	panel.Update(permissionKey("enter"))
	if !panel.Expanded() || !strings.Contains(panel.View(), "tool failed") {
		t.Fatalf("context did not expand:\n%s", panel.View())
	}

	panel.Update(permissionKey("left"))
	if got := panel.ActiveTab(); got != components.ContextTool || !strings.Contains(panel.View(), "command output") {
		t.Fatalf("active tab=%q view:\n%s", got, panel.View())
	}
	panel.Update(permissionKey("left"))
	if got := panel.ActiveTab(); got != components.ContextDiff || !strings.Contains(panel.View(), "+new") {
		t.Fatalf("active tab=%q view:\n%s", got, panel.View())
	}

	panel.Update(permissionKey("esc"))
	if panel.Open() {
		t.Fatal("escape did not close context")
	}
}

func TestContextShellGitStatusAndDiff(t *testing.T) {
	panel := components.NewContext()
	panel.ShowWorkspaceChanges(&domain.WorkspaceChanges{
		IsGit:  true,
		Status: " M file.go\n",
		Diff:   "diff --git a/file.go b/file.go\n+changed\n",
	})

	view := panel.View()
	for _, want := range []string{"Git status", "M file.go", "Git diff", "+changed"} {
		if !strings.Contains(view, want) {
			t.Fatalf("view missing %q:\n%s", want, view)
		}
	}
}

func TestContextShellNonGitNoticeIsExact(t *testing.T) {
	panel := components.NewContext()
	panel.ShowWorkspaceChanges(&domain.WorkspaceChanges{Notice: workspace.NonGitNotice})

	if view := panel.View(); !strings.Contains(view, workspace.NonGitNotice) {
		t.Fatalf("view missing exact notice:\n%s", view)
	}
}

func TestContextTooLargeShowsCompactAction(t *testing.T) {
	panel := components.NewContext()
	panel.ShowError(domain.ErrorContextTooLarge, "request exceeded context", components.RunCompactAction)

	view := panel.View()
	for _, want := range []string{"context_too_large", "request exceeded context", "Run /compact"} {
		if !strings.Contains(view, want) {
			t.Fatalf("view missing %q:\n%s", want, view)
		}
	}
}

func TestContextShowsCompactionHintWhenNoCompactionStateIsKnown(t *testing.T) {
	panel := components.NewContext()

	view := panel.View()
	for _, want := range []string{"CONTEXT", "Auto compaction: unavailable", "/compact"} {
		if !strings.Contains(view, want) {
			t.Fatalf("view missing %q:\n%s", want, view)
		}
	}
}
