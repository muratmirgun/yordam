package components

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
)

const (
	AutoShellWarningTitle       = "AUTO SHELL WARNING"
	AutoShellSandboxWarning     = "Shell execution is not OS-sandboxed."
	AutoShellAccessWarning      = "The command may read or write outside the workspace and may access the network."
	AutoShellEnvironmentWarning = "Shell inherits non-provider environment variables."
)

type PermissionResolveFunc func(domain.PermissionDecision)
type AuthorizationResolveFunc func(protocol.ApprovalResponse)

type Permission struct {
	request              domain.PreparedToolRequest
	resolve              PermissionResolveFunc
	open                 bool
	resolved             bool
	autoShellWarning     bool
	authorization        *ports.AuthorizationPrompt
	authorizationActor   protocol.ActorRef
	authorizationResolve AuthorizationResolveFunc
}

func NewPermission(request domain.PreparedToolRequest, resolve PermissionResolveFunc) Permission {
	return Permission{request: request, resolve: resolve, open: true}
}

func NewAuthorizationPermission(prompt ports.AuthorizationPrompt, actor protocol.ActorRef, resolve AuthorizationResolveFunc) Permission {
	cloned := protocol.DeepCopy(prompt)
	return Permission{authorization: &cloned, authorizationActor: actor, authorizationResolve: resolve, open: true}
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

	if p.authorization != nil {
		response := protocol.ApprovalResponse{RequestID: p.authorization.Request.RequestID, Lifetime: protocol.AuthorizationLifetimeOnce, ScopeDigest: p.authorization.ScopeDigest, Actor: p.authorizationActor}
		switch key {
		case "y":
			response.Action = "allow"
			response.Reason = "allowed once by user"
		case "s":
			response.Action = "allow"
			response.Lifetime = protocol.AuthorizationLifetimeSession
			response.Reason = "allowed for session by user"
		case "n", "esc":
			response.Action = "deny"
			response.Reason = "denied by user"
		default:
			return false
		}
		p.resolved = true
		p.open = false
		if p.authorizationResolve != nil {
			p.authorizationResolve(response)
		}
		return true
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
	if p.authorization != nil {
		return p.authorizationView()
	}
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

func (p Permission) authorizationView() string {
	request := p.authorization.Request
	scope := p.authorization.Scope
	resourceLines := make([]string, 0, len(scope.Resources))
	for _, resource := range scope.Resources {
		facts := []string{resource.Kind, resource.CanonicalID}
		if resource.ParentID != "" {
			facts = append(facts, "parent="+resource.ParentID)
		}
		if resource.Digest != "" {
			facts = append(facts, "digest="+resource.Digest)
		}
		for _, attribute := range resource.Attributes {
			facts = append(facts, attribute.Name+"="+attribute.Value)
		}
		resourceLines = append(resourceLines, "- "+strings.Join(facts, " | "))
	}
	if len(resourceLines) == 0 {
		resourceLines = append(resourceLines, "- none")
	}
	constraintLines := make([]string, 0, len(scope.Constraints))
	for _, constraint := range scope.Constraints {
		constraintLines = append(constraintLines, fmt.Sprintf("- %s %s %s", constraint.Name, constraint.Operator, string(constraint.Value)))
	}
	if len(constraintLines) == 0 {
		constraintLines = append(constraintLines, "- none")
	}
	sections := []string{fmt.Sprintf(
		"PERMISSION\nSource: %s/%s/%s\nCapability: %s\nResources:\n%s\nConstraints:\n%s\nExecution locus: %s\nBoundary: %s\nEffect: %s\nProfile: %s -> %s\nScope digest: %s:%s\nSummary: %s",
		scope.Source.Source, scope.Source.Authority, scope.Source.Name, scope.Capability, strings.Join(resourceLines, "\n"), strings.Join(constraintLines, "\n"),
		request.ExecutionLocus, request.Boundary, request.Effect, request.RequestedProfile, request.EffectiveProfile,
		p.authorization.ScopeDigest.Algorithm, p.authorization.ScopeDigest.Value, p.authorization.Summary,
	)}
	if p.authorization.ProposedDiff != "" {
		sections = append(sections, "Proposed diff\n"+p.authorization.ProposedDiff)
	}
	sections = append(sections, "y: allow once | s: allow session | n/Esc: deny")
	return strings.Join(sections, "\n\n")
}
