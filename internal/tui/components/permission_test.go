package components_test

import (
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/tui/components"
)

func TestPermissionModalResolutions(t *testing.T) {
	tests := []struct {
		key      string
		action   domain.PermissionAction
		lifetime domain.PermissionLifetime
	}{
		{key: "y", action: domain.PermissionAllow, lifetime: domain.PermissionOnce},
		{key: "s", action: domain.PermissionAllow, lifetime: domain.PermissionSession},
		{key: "n", action: domain.PermissionDeny, lifetime: domain.PermissionOnce},
		{key: "esc", action: domain.PermissionDeny, lifetime: domain.PermissionOnce},
	}

	for _, test := range tests {
		t.Run(test.key, func(t *testing.T) {
			var got domain.PermissionDecision
			calls := 0
			modal := components.NewPermission(preparedEdit(), func(decision domain.PermissionDecision) {
				got = decision
				calls++
			})

			modal.Update(permissionKey(test.key))
			modal.Update(permissionKey("y"))

			if got.Action != test.action || got.Lifetime != test.lifetime || got.Scope != "/workspace/file.go" {
				t.Fatalf("got=%#v", got)
			}
			if calls != 1 || modal.Open() {
				t.Fatalf("calls=%d open=%t", calls, modal.Open())
			}
		})
	}
}

func TestPermissionModalRendersPreparedRequest(t *testing.T) {
	modal := components.NewPermission(preparedEdit(), nil)
	view := modal.View()

	for _, want := range []string{
		"edit",
		"/workspace/file.go",
		"inside workspace",
		"replace greeting",
		"-old",
		"+new",
		"allow once",
		"allow session",
		"deny",
	} {
		if !strings.Contains(view, want) {
			t.Fatalf("view missing %q:\n%s", want, view)
		}
	}
}

func TestPermissionModalMarksOutsideScope(t *testing.T) {
	request := preparedEdit()
	request.InsideWorkspace = false
	modal := components.NewPermission(request, nil)

	if view := modal.View(); !strings.Contains(view, "outside workspace") {
		t.Fatalf("view missing outside marker:\n%s", view)
	}
}

func TestPermissionAutoShellWarningUsesSeparateSecurityCopy(t *testing.T) {
	request := preparedEdit()
	request.Request.Name = "shell"
	request.Summary = "run go test"
	request.ProposedDiff = ""
	modal := components.NewPermission(request, nil)
	modal.SetAutoShellWarning(true)

	view := modal.View()
	for _, want := range []string{
		components.AutoShellWarningTitle,
		components.AutoShellSandboxWarning,
		components.AutoShellAccessWarning,
		components.AutoShellEnvironmentWarning,
	} {
		if !strings.Contains(view, want) {
			t.Fatalf("warning missing %q:\n%s", want, view)
		}
	}
}

func preparedEdit() domain.PreparedToolRequest {
	return domain.PreparedToolRequest{
		Request:         domain.ToolRequest{CallID: "call-1", Name: "edit"},
		CanonicalScope:  "/workspace/file.go",
		InsideWorkspace: true,
		Summary:         "replace greeting",
		ProposedDiff:    "@@ -1 +1 @@\n-old\n+new",
	}
}

func permissionKey(value string) tea.KeyPressMsg {
	switch value {
	case "esc":
		return tea.KeyPressMsg{Code: tea.KeyEscape}
	case "enter":
		return tea.KeyPressMsg{Code: tea.KeyEnter}
	case "left":
		return tea.KeyPressMsg{Code: tea.KeyLeft}
	case "right":
		return tea.KeyPressMsg{Code: tea.KeyRight}
	}
	return tea.KeyPressMsg{Code: rune(value[0]), Text: value}
}
