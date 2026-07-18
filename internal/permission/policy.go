package permission

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type PolicyRule struct {
	Action   domain.PermissionAction
	Source   string
	HardDeny bool
}

type PolicyInput struct {
	Platform     PolicyRule
	User         PolicyRule
	Project      PolicyRule
	SessionGrant PolicyRule
	Interactive  PolicyRule
}

func ResolvePolicy(input PolicyInput) (domain.PermissionDecision, error) {
	platform := defaultAction(input.Platform.Action, domain.PermissionAllow)
	user := defaultAction(input.User.Action, domain.PermissionAsk)
	project := defaultAction(input.Project.Action, domain.PermissionAllow)
	for _, action := range []domain.PermissionAction{platform, user, project, input.SessionGrant.Action, input.Interactive.Action} {
		if action != "" && action != domain.PermissionAllow && action != domain.PermissionAsk && action != domain.PermissionDeny {
			return domain.PermissionDecision{}, fmt.Errorf("invalid policy action %q", action)
		}
	}
	if input.Platform.HardDeny || platform == domain.PermissionDeny {
		return policyDecision(domain.PermissionDeny, input.Platform.Source, "platform deny"), nil
	}
	resolved := narrower(user, project)
	source := input.User.Source
	if resolved == project && project != domain.PermissionAllow {
		source = input.Project.Source
	}
	if resolved == domain.PermissionAsk && input.SessionGrant.Action != "" {
		resolved = input.SessionGrant.Action
		source = input.SessionGrant.Source
	}
	if resolved == domain.PermissionAsk && input.Interactive.Action != "" {
		resolved = input.Interactive.Action
		source = input.Interactive.Source
	}
	return policyDecision(resolved, source, "monotonic policy resolution"), nil
}

func policyDecision(action domain.PermissionAction, source, reason string) domain.PermissionDecision {
	return domain.PermissionDecision{Action: action, Lifetime: domain.PermissionOnce, Reason: reason, PolicySource: source}
}

func defaultAction(action, fallback domain.PermissionAction) domain.PermissionAction {
	if action == "" {
		return fallback
	}
	return action
}

func narrower(left, right domain.PermissionAction) domain.PermissionAction {
	rank := func(action domain.PermissionAction) int {
		switch action {
		case domain.PermissionDeny:
			return 0
		case domain.PermissionAsk:
			return 1
		default:
			return 2
		}
	}
	if rank(right) < rank(left) {
		return right
	}
	return left
}

func (p *SessionPolicy) Evaluate(_ context.Context, _ ports.PermissionContext, call domain.PreparedToolRequest) domain.PermissionDecision {
	p.mu.RLock()
	defer p.mu.RUnlock()

	mutation := call.Mutation
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

func (p *SessionPolicy) EvaluateAuthorization(_ context.Context, permissionContext ports.PermissionContext, request protocol.AuthorizationRequest) (protocol.AuthorizationDecision, error) {
	if err := request.Validate(); err != nil {
		return protocol.AuthorizationDecision{}, err
	}
	if permissionContext.SessionID != "" && request.SessionID != protocol.SessionID(permissionContext.SessionID) {
		return protocol.AuthorizationDecision{}, fmt.Errorf("authorization session mismatch")
	}
	p.mu.RLock()
	mode := p.mode
	autoShell := p.autoShell
	grantKey, keyErr := authorizationGrantKey(request, nil)
	_, granted := p.authorizationGrants[grantKey]
	p.mu.RUnlock()
	if keyErr != nil {
		return protocol.AuthorizationDecision{}, keyErr
	}
	userAction, reason := modeAuthorizationAction(mode, request, autoShell)
	policySource := "user"
	compatibility := permissionContext.ConfiguredProvider && request.Source.Source == "provider" && request.Effect == "egress"
	if compatibility {
		userAction = domain.PermissionAllow
		policySource = "compatibility"
		reason = "configured provider compatibility policy"
	}
	input := PolicyInput{
		Platform: PolicyRule{Action: permissionContext.PlatformAction, Source: "platform", HardDeny: requestHasHardDeny(request)},
		User:     PolicyRule{Action: userAction, Source: policySource},
		Project:  PolicyRule{Action: permissionContext.ProjectAction, Source: "project"},
	}
	if granted {
		input.SessionGrant = PolicyRule{Action: domain.PermissionAllow, Source: "session"}
	}
	resolved, err := ResolvePolicy(input)
	if err != nil {
		return protocol.AuthorizationDecision{}, err
	}
	nonce, err := policyNonce()
	if err != nil {
		return protocol.AuthorizationDecision{}, err
	}
	lifetime := protocol.AuthorizationLifetimeOnce
	if resolved.PolicySource == "session" || resolved.PolicySource == "compatibility" {
		lifetime = protocol.AuthorizationLifetimeSession
	}
	result := protocol.AuthorizationDecision{
		Request: protocol.DeepCopy(request), Action: string(resolved.Action), Scope: protocol.CanonicalAuthorizationScope{
			Capability: request.Action, Source: request.Source, Resources: protocol.DeepCopy(request.Resources), Constraints: []protocol.AuthorizationConstraint{},
		}, Constraints: []protocol.AuthorizationConstraint{}, Lifetime: lifetime, PolicySource: resolved.PolicySource,
		PolicyGeneration: request.PolicyGeneration, Reason: reason, DecidedAt: time.Now().UTC(), PlanDigest: request.PlanDigest, DecisionNonce: nonce,
	}
	if err := result.Validate(); err != nil {
		return protocol.AuthorizationDecision{}, err
	}
	return result, nil
}

func modeAuthorizationAction(mode domain.PermissionMode, request protocol.AuthorizationRequest, autoShell bool) (domain.PermissionAction, string) {
	inside := request.Boundary == "workspace"
	observation := request.Effect == "observation"
	process := request.ExecutionLocus == "process" || request.Boundary == "process" || request.EffectiveProfile == "unsandboxed"
	builtinFileMutation := request.Source.Source == "builtin" && request.Source.Authority == "yordam" && request.Effect == "mutation" && inside && !process
	switch mode {
	case domain.ModeSafe:
		if observation && inside {
			return domain.PermissionAllow, "safe observation inside workspace"
		}
		return domain.PermissionDeny, "safe mode"
	case domain.ModeAsk:
		if observation && inside {
			return domain.PermissionAllow, "observation inside workspace"
		}
		return domain.PermissionAsk, "ask mode"
	case domain.ModeAuto:
		if observation && inside {
			return domain.PermissionAllow, "observation inside workspace"
		}
		if builtinFileMutation {
			return domain.PermissionAllow, "auto built-in file mutation inside workspace"
		}
		if process && autoShell {
			return domain.PermissionAllow, "trusted shell acknowledged"
		}
		return domain.PermissionAsk, "auto boundary or process acknowledgement required"
	default:
		return domain.PermissionDeny, "invalid permission mode"
	}
}

type authorizationGrantBinding struct {
	SessionID        protocol.SessionID
	Capability       string
	Source           protocol.ToolIdentity
	SourceRevision   string
	DescriptorDigest protocol.Digest
	PolicyGeneration string
	Resources        []protocol.ResourceTarget
	Constraints      []protocol.AuthorizationConstraint
}

func authorizationGrantKey(request protocol.AuthorizationRequest, constraints []protocol.AuthorizationConstraint) (string, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	digest, err := canonicaljson.Digest(authorizationGrantBinding{
		SessionID: request.SessionID, Capability: request.Action, Source: request.Source, SourceRevision: request.SourceRevision,
		DescriptorDigest: request.DescriptorDigest, PolicyGeneration: request.PolicyGeneration, Resources: request.Resources, Constraints: constraints,
	})
	if err != nil {
		return "", err
	}
	return digest.Algorithm + ":" + digest.Value, nil
}

func requestHasHardDeny(request protocol.AuthorizationRequest) bool {
	for _, provenance := range request.PolicyProvenance {
		if provenance.HardDeny {
			return true
		}
	}
	return false
}

func policyNonce() (protocol.DecisionNonce, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return protocol.DecisionNonce(hex.EncodeToString(raw)), nil
}
