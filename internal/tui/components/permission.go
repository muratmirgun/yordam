package components

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/muratmirgun/yordam/internal/domain"
)

const (
	AutoShellWarningTitle       = "AUTO SHELL WARNING"
	AutoShellSandboxWarning     = "Shell execution is not OS-sandboxed."
	AutoShellAccessWarning      = "The command may read or write outside the workspace and may access the network."
	AutoShellEnvironmentWarning = "Shell inherits non-provider environment variables."
)

type PermissionResolveFunc func(domain.PermissionDecision)

type Permission struct {
	request          domain.PreparedToolRequest
	resolve          PermissionResolveFunc
	open             bool
	resolved         bool
	autoShellWarning bool
}

func NewPermission(request domain.PreparedToolRequest, resolve PermissionResolveFunc) Permission {
	return Permission{request: request, resolve: resolve, open: true}
}

func (p *Permission) SetAutoShellWarning(enabled bool) {
	p.autoShellWarning = enabled
}

func (p Permission) Open() bool {
	return p.open
}

func (p *Permission) Update(message tea.Msg) {
	key, ok := message.(tea.KeyPressMsg)
	if !ok {
		return
	}
	p.HandleKey(key.String())
}

func (p *Permission) HandleKey(key string) bool {
	if !p.open || p.resolved {
		return false
	}

	scope := p.request.ApprovalScope
	if scope == "" {
		scope = p.request.CanonicalScope
	}
	decision := domain.PermissionDecision{Scope: scope, Lifetime: domain.PermissionOnce}
	switch key {
	case "y":
		decision.Action = domain.PermissionAllow
		decision.Reason = "allowed once by user"
	case "s":
		decision.Action = domain.PermissionAllow
		decision.Lifetime = domain.PermissionSession
		decision.Reason = "allowed for session by user"
	case "n", "esc":
		decision.Action = domain.PermissionDeny
		decision.Reason = "denied by user"
	default:
		return false
	}

	p.resolved = true
	p.open = false
	if p.resolve != nil {
		p.resolve(decision)
	}
	return true
}

func (p Permission) View() string {
	marker := "outside workspace"
	if p.request.InsideWorkspace {
		marker = "inside workspace"
	}

	sections := []string{}
	if p.autoShellWarning {
		sections = append(sections, strings.Join([]string{
			AutoShellWarningTitle,
			AutoShellSandboxWarning,
			AutoShellAccessWarning,
			AutoShellEnvironmentWarning,
		}, "\n"))
	}
	sections = append(sections, fmt.Sprintf(
		"PERMISSION\nTool: %s\nScope: %s\nLocation: %s\nSummary: %s",
		p.request.Request.Name,
		p.request.CanonicalScope,
		marker,
		p.request.Summary,
	))
	if p.request.ProposedDiff != "" {
		sections = append(sections, "Proposed diff\n"+p.request.ProposedDiff)
	}
	sections = append(sections, "y: allow once | s: allow session | n/Esc: deny")
	return strings.Join(sections, "\n\n")
}
