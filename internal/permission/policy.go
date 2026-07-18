package permission

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type PolicyRule struct {
	Action   domain.PermissionAction
	Source   string
	Reason   string
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
	platform := defaultRule(input.Platform, domain.PermissionAllow, "platform", "platform policy")
	user := defaultRule(input.User, domain.PermissionAsk, "user", "user policy")
	project := defaultRule(input.Project, domain.PermissionAllow, "project", "project policy")
	for _, action := range []domain.PermissionAction{platform.Action, user.Action, project.Action, input.SessionGrant.Action, input.Interactive.Action} {
		if action != "" && action != domain.PermissionAllow && action != domain.PermissionAsk && action != domain.PermissionDeny {
			return domain.PermissionDecision{}, fmt.Errorf("invalid policy action %q", action)
		}
	}
	if input.Platform.HardDeny || platform.Action == domain.PermissionDeny {
		platform.Action = domain.PermissionDeny
		if input.Platform.HardDeny && input.Platform.Reason == "" {
			platform.Reason = "platform hard deny"
		}
		return policyDecision(platform.Action, platform.Source, platform.Reason), nil
	}
	resolved := user
	if narrower(user.Action, project.Action) == project.Action && project.Action != user.Action {
		resolved = project
	}
	if resolved.Action == domain.PermissionAsk && input.SessionGrant.Action != "" {
		resolved = defaultRule(input.SessionGrant, domain.PermissionAsk, "session", "session grant")
	}
	if resolved.Action == domain.PermissionAsk && input.Interactive.Action != "" {
		resolved = defaultRule(input.Interactive, domain.PermissionAsk, "interactive", "interactive decision")
	}
	return policyDecision(resolved.Action, resolved.Source, resolved.Reason), nil
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

func defaultRule(rule PolicyRule, fallback domain.PermissionAction, source, reason string) PolicyRule {
	rule.Action = defaultAction(rule.Action, fallback)
	if rule.Source == "" {
		rule.Source = source
	}
	if rule.Reason == "" {
		rule.Reason = reason
	}
	return rule
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

func (p *SessionPolicy) EvaluateAuthorization(_ context.Context, evaluation ports.EvaluationInput) (protocol.AuthorizationDecision, error) {
	permissionContext := evaluation.Permission
	request := evaluation.Request
	if err := request.Validate(); err != nil {
		return protocol.AuthorizationDecision{}, err
	}
	if permissionContext.SessionID != "" && request.SessionID != protocol.SessionID(permissionContext.SessionID) {
		return protocol.AuthorizationDecision{}, fmt.Errorf("authorization session mismatch")
	}
	constraints, err := canonicalConstraints(evaluation.Constraints)
	if err != nil {
		return protocol.AuthorizationDecision{}, err
	}
	p.mu.RLock()
	mode := p.mode
	autoShell := p.autoShell
	grantKey, keyErr := authorizationGrantKey(request, constraints)
	_, granted := p.authorizationGrants[grantKey]
	p.mu.RUnlock()
	if keyErr != nil {
		return protocol.AuthorizationDecision{}, keyErr
	}
	userAction, reason := modeAuthorizationAction(mode, evaluation, autoShell)
	policySource := "user"
	compatibility := configuredProviderMatches(permissionContext.ConfiguredProvider, request)
	if compatibility {
		userAction = domain.PermissionAllow
		policySource = "compatibility"
		reason = "configured provider compatibility policy"
	}
	input := PolicyInput{
		Platform: PolicyRule{Action: permissionContext.PlatformAction, Source: "platform", Reason: platformReason(request, permissionContext.PlatformAction), HardDeny: requestHasHardDeny(request)},
		User:     PolicyRule{Action: userAction, Source: policySource, Reason: reason},
		Project:  PolicyRule{Action: permissionContext.ProjectAction, Source: "project", Reason: "project policy"},
	}
	if granted {
		input.SessionGrant = PolicyRule{Action: domain.PermissionAllow, Source: "session", Reason: "exact session grant"}
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
			Capability: request.Action, Source: request.Source, Resources: protocol.DeepCopy(request.Resources), Constraints: protocol.DeepCopy(constraints),
		}, Constraints: protocol.DeepCopy(constraints), Lifetime: lifetime, PolicySource: resolved.PolicySource,
		PolicyGeneration: request.PolicyGeneration, Reason: resolved.Reason, DecidedAt: time.Now().UTC(), PlanDigest: request.PlanDigest, DecisionNonce: nonce,
	}
	if err := result.Validate(); err != nil {
		return protocol.AuthorizationDecision{}, err
	}
	return result, nil
}

func modeAuthorizationAction(mode domain.PermissionMode, evaluation ports.EvaluationInput, autoShell bool) (domain.PermissionAction, string) {
	facts := deriveAuthorizationFacts(evaluation)
	switch mode {
	case domain.ModeSafe:
		if facts.process || facts.outside {
			return domain.PermissionDeny, "safe process or outside boundary"
		}
		if facts.observation {
			return domain.PermissionAllow, "safe observation inside workspace"
		}
		return domain.PermissionDeny, "safe mode"
	case domain.ModeAsk:
		if facts.process || facts.outside {
			return domain.PermissionAsk, "process or outside boundary requires approval"
		}
		if facts.observation {
			return domain.PermissionAllow, "observation inside workspace"
		}
		return domain.PermissionAsk, "ask mode"
	case domain.ModeAuto:
		if facts.outside {
			return domain.PermissionAsk, "outside boundary requires approval"
		}
		if facts.process {
			if autoShell && facts.trustedLocalProcess {
				return domain.PermissionAllow, "trusted local process acknowledged"
			}
			return domain.PermissionAsk, "process acknowledgement required"
		}
		if facts.observation {
			return domain.PermissionAllow, "observation inside workspace"
		}
		if facts.trustedBuiltinFileMutation {
			return domain.PermissionAllow, "auto built-in file mutation inside workspace"
		}
		return domain.PermissionAsk, "auto boundary or process acknowledgement required"
	default:
		return domain.PermissionDeny, "invalid permission mode"
	}
}

type authorizationFacts struct {
	process                    bool
	outside                    bool
	observation                bool
	trustedLocalProcess        bool
	trustedBuiltinFileMutation bool
}

func deriveAuthorizationFacts(evaluation ports.EvaluationInput) authorizationFacts {
	request := evaluation.Request
	descriptor := evaluation.Descriptor
	descriptorMatches := descriptorMatchesRequest(descriptor, request)
	trustedDescriptor := descriptorMatches && descriptor.Body.ClassificationSource == "trusted_adapter"
	process := request.ExecutionLocus == "process" || request.EffectiveProfile == "unsandboxed"
	if descriptorMatches && descriptor.Body.Mutation == "process" {
		process = true
	}
	outside := request.Boundary == "filesystem_external" || request.Boundary == "network" || request.Boundary == "remote" || request.Boundary == "remote_or_unknown"
	trustedSource := request.Source.Source == "builtin" && request.Source.Authority == "yordam"
	filesystemInside, filesystemOnly := filesystemResourcesInside(evaluation.Permission.Workspace, request.Resources)
	shellInside, shellShape := localProcessResourcesInside(evaluation.Permission.Workspace, request.Resources)
	if process && (!shellShape || !shellInside) {
		outside = true
	}
	observation := trustedDescriptor && descriptor.Body.Effect == "observation" && descriptor.Body.Mutation == "read_only" && request.Effect == "observation" && request.Boundary == "workspace" && filesystemInside
	trustedLocalProcess := trustedDescriptor && trustedSource && descriptor.Body.Effect == "mutation" && descriptor.Body.Mutation == "process" && request.Effect == "mutation" && request.ExecutionLocus == "process" && request.RequestedProfile == "unsandboxed" && request.EffectiveProfile == "unsandboxed" && shellShape && shellInside
	trustedBuiltinFileMutation := trustedDescriptor && trustedSource && descriptor.Body.Effect == "mutation" && descriptor.Body.Mutation == "file" && request.Effect == "mutation" && (request.ExecutionLocus == "builtin" || request.ExecutionLocus == "local") && request.Boundary == "workspace" && filesystemOnly && filesystemInside
	return authorizationFacts{process: process, outside: outside, observation: observation, trustedLocalProcess: trustedLocalProcess, trustedBuiltinFileMutation: trustedBuiltinFileMutation}
}

func descriptorMatchesRequest(descriptor protocol.ToolDescriptor, request protocol.AuthorizationRequest) bool {
	if descriptor.Body.Identity == (protocol.ToolIdentity{}) || descriptor.Body.Validate() != nil || descriptor.DescriptorDigest.Validate() != nil {
		return false
	}
	if canonicaljson.ValidateDigest(descriptor.Body, descriptor.DescriptorDigest) != nil {
		return false
	}
	if descriptor.Body.Identity != request.Source || descriptor.Body.SourceRevision != request.SourceRevision || descriptor.DescriptorDigest != request.DescriptorDigest || descriptor.Body.Effect != request.Effect {
		return false
	}
	for _, locus := range descriptor.Body.ExecutionLoci {
		if locus == request.ExecutionLocus {
			return true
		}
	}
	return false
}

func filesystemResourcesInside(workspace string, resources []protocol.ResourceTarget) (inside, filesystemOnly bool) {
	if workspace == "" || len(resources) == 0 {
		return false, false
	}
	filesystemOnly = true
	for _, resource := range resources {
		if resource.Kind != "file" && resource.Kind != "directory" {
			filesystemOnly = false
			continue
		}
		if !pathInside(workspace, resource.CanonicalID) {
			return false, filesystemOnly
		}
	}
	return filesystemOnly, filesystemOnly
}

func localProcessResourcesInside(workspace string, resources []protocol.ResourceTarget) (inside, validShape bool) {
	if workspace == "" || len(resources) == 0 {
		return false, false
	}
	seenDirectory, seenExecutable := false, false
	for _, resource := range resources {
		switch resource.Kind {
		case "directory":
			seenDirectory = true
			if !pathInside(workspace, resource.CanonicalID) {
				return false, true
			}
		case "executable":
			seenExecutable = true
		default:
			return false, false
		}
	}
	return seenDirectory && seenExecutable, seenDirectory && seenExecutable
}

func pathInside(workspace, target string) bool {
	if !filepath.IsAbs(workspace) || !filepath.IsAbs(target) {
		return false
	}
	relative, err := filepath.Rel(filepath.Clean(workspace), filepath.Clean(target))
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !filepath.IsAbs(relative)
}

func configuredProviderMatches(configured *ports.ConfiguredProviderBinding, request protocol.AuthorizationRequest) bool {
	return configured != nil && request.Source.Source == "provider" && request.Effect == "egress" && request.Boundary == "network" && configured.Identity == request.Source && configured.SourceRevision == request.SourceRevision && configured.DescriptorDigest == request.DescriptorDigest
}

func platformReason(request protocol.AuthorizationRequest, action domain.PermissionAction) string {
	if requestHasHardDeny(request) {
		return "platform hard deny"
	}
	if action == domain.PermissionDeny {
		return "platform deny"
	}
	return "platform policy"
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
	canonical, err := canonicalConstraints(constraints)
	if err != nil {
		return "", err
	}
	digest, err := canonicaljson.Digest(authorizationGrantBinding{
		SessionID: request.SessionID, Capability: request.Action, Source: request.Source, SourceRevision: request.SourceRevision,
		DescriptorDigest: request.DescriptorDigest, PolicyGeneration: request.PolicyGeneration, Resources: request.Resources, Constraints: canonical,
	})
	if err != nil {
		return "", err
	}
	return digest.Algorithm + ":" + digest.Value, nil
}

func canonicalConstraints(constraints []protocol.AuthorizationConstraint) ([]protocol.AuthorizationConstraint, error) {
	result := protocol.DeepCopy(constraints)
	previous := ""
	for index := range result {
		constraint := &result[index]
		if constraint.Name == "" || constraint.Operator == "" {
			return nil, fmt.Errorf("authorization constraint is incomplete")
		}
		canonicalValue, err := canonicaljson.Marshal(constraint.Value)
		if err != nil {
			return nil, fmt.Errorf("authorization constraint %q: %w", constraint.Name, err)
		}
		constraint.Value = canonicalValue
		key := constraint.Name + "\x00" + constraint.Operator
		if index > 0 && key <= previous {
			return nil, fmt.Errorf("authorization constraints must be sorted and unique")
		}
		previous = key
	}
	return result, nil
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
