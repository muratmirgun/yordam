package permission

import (
	"context"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
)

func (p *SessionPolicy) Evaluate(_ context.Context, _ ports.PermissionContext, call domain.PreparedToolRequest) domain.PermissionDecision {
	p.mu.RLock()
	defer p.mu.RUnlock()

	mutation := call.Mutation
	if mutation != mutationFor(call.Request.Name) {
		return decision(domain.PermissionDeny, call.CanonicalScope, "invalid built-in mutation classification")
	}
	if p.mode == domain.ModeSafe {
		if mutation == domain.MutationReadOnly && call.InsideWorkspace {
			return decision(domain.PermissionAllow, call.CanonicalScope, "safe read inside workspace")
		}
		return decision(domain.PermissionDeny, call.CanonicalScope, "safe mode")
	}
	if _, ok := p.grants[grantKey(call.Request.Name, call.CanonicalScope)]; ok {
		if p.mode == domain.ModeAuto && mutation == domain.MutationProcess && !p.autoShell {
			return decision(domain.PermissionAsk, call.CanonicalScope, "shell acknowledgement required")
		}
		return domain.PermissionDecision{Action: domain.PermissionAllow, Lifetime: domain.PermissionSession, Scope: call.CanonicalScope, Reason: "session grant"}
	}
	if mutation == domain.MutationReadOnly {
		if call.InsideWorkspace {
			return decision(domain.PermissionAllow, call.CanonicalScope, "read inside workspace")
		}
		return decision(domain.PermissionAsk, call.CanonicalScope, "outside workspace")
	}
	if p.mode == domain.ModeAsk {
		return decision(domain.PermissionAsk, call.CanonicalScope, "ask mode mutation")
	}
	if mutation == domain.MutationFile {
		if call.InsideWorkspace {
			return decision(domain.PermissionAllow, call.CanonicalScope, "auto file inside workspace")
		}
		return decision(domain.PermissionAsk, call.CanonicalScope, "outside workspace")
	}
	if p.autoShell {
		return decision(domain.PermissionAllow, call.CanonicalScope, "trusted shell acknowledged")
	}
	return decision(domain.PermissionAsk, call.CanonicalScope, "shell acknowledgement required")
}

func decision(action domain.PermissionAction, scope, reason string) domain.PermissionDecision {
	return domain.PermissionDecision{Action: action, Lifetime: domain.PermissionOnce, Scope: scope, Reason: reason}
}

func mutationFor(name string) domain.MutationKind {
	switch name {
	case "read", "search":
		return domain.MutationReadOnly
	case "edit":
		return domain.MutationFile
	default:
		return domain.MutationProcess
	}
}
