package permission_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/permission"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
	skilltool "github.com/muratmirgun/yordam/internal/tools/skill"
	subagenttool "github.com/muratmirgun/yordam/internal/tools/subagent"
)

func TestAuthorizationPolicyPrecedenceCrossProduct(t *testing.T) {
	tests := []struct {
		name        string
		platform    domain.PermissionAction
		hardDeny    bool
		user        domain.PermissionAction
		project     domain.PermissionAction
		grant       domain.PermissionAction
		interactive domain.PermissionAction
		want        domain.PermissionAction
	}{
		{"platform hard deny terminal", domain.PermissionDeny, true, domain.PermissionAllow, domain.PermissionAllow, domain.PermissionAllow, domain.PermissionAllow, domain.PermissionDeny},
		{"ordinary platform deny terminal", domain.PermissionDeny, false, domain.PermissionAllow, domain.PermissionAllow, domain.PermissionAllow, domain.PermissionAllow, domain.PermissionDeny},
		{"user deny terminal", domain.PermissionAllow, false, domain.PermissionDeny, domain.PermissionAllow, domain.PermissionAllow, domain.PermissionAllow, domain.PermissionDeny},
		{"project deny terminal", domain.PermissionAllow, false, domain.PermissionAllow, domain.PermissionDeny, domain.PermissionAllow, domain.PermissionAllow, domain.PermissionDeny},
		{"project cannot broaden user ask", domain.PermissionAllow, false, domain.PermissionAsk, domain.PermissionAllow, "", "", domain.PermissionAsk},
		{"project narrows user allow", domain.PermissionAllow, false, domain.PermissionAllow, domain.PermissionAsk, "", "", domain.PermissionAsk},
		{"grant resolves ask", domain.PermissionAllow, false, domain.PermissionAsk, domain.PermissionAllow, domain.PermissionAllow, "", domain.PermissionAllow},
		{"grant cannot change allow", domain.PermissionAllow, false, domain.PermissionAllow, domain.PermissionAllow, domain.PermissionDeny, "", domain.PermissionAllow},
		{"interactive resolves remaining ask", domain.PermissionAllow, false, domain.PermissionAllow, domain.PermissionAsk, "", domain.PermissionDeny, domain.PermissionDeny},
		{"interactive cannot change allow", domain.PermissionAllow, false, domain.PermissionAllow, domain.PermissionAllow, "", domain.PermissionDeny, domain.PermissionAllow},
		{"grant wins before interactive", domain.PermissionAllow, false, domain.PermissionAsk, domain.PermissionAllow, domain.PermissionAllow, domain.PermissionDeny, domain.PermissionAllow},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := permission.ResolvePolicy(permission.PolicyInput{
				Platform:     permission.PolicyRule{Action: test.platform, HardDeny: test.hardDeny, Source: "platform"},
				User:         permission.PolicyRule{Action: test.user, Source: "user"},
				Project:      permission.PolicyRule{Action: test.project, Source: "project"},
				SessionGrant: permission.PolicyRule{Action: test.grant, Source: "session"},
				Interactive:  permission.PolicyRule{Action: test.interactive, Source: "interactive"},
			})
			if err != nil {
				t.Fatal(err)
			}
			if got.Action != test.want {
				t.Fatalf("action=%s want=%s result=%#v", got.Action, test.want, got)
			}
		})
	}
}

func TestPermissionAllowsOnlyCanonicalTrustedSubagentOrchestration(t *testing.T) {
	descriptor := subagenttool.BuiltinDescriptor()
	request := structuredRequest("subagent", descriptor.Body.Identity)
	request.SourceRevision = descriptor.Body.SourceRevision
	request.DescriptorDigest = descriptor.DescriptorDigest
	request.Effect = descriptor.Body.Effect
	request.ExecutionLocus = descriptor.Body.ExecutionLoci[0]
	request.RequestedProfile = "configured"
	request.EffectiveProfile = "configured"
	request.Boundary = "runtime"
	request.Reversibility = "not_applicable"
	request.VerificationCoverage = "full"
	request.Resources = nil
	input := ports.EvaluationInput{Permission: ports.PermissionContext{SessionID: string(request.SessionID), Mode: domain.ModeSafe, Workspace: "/workspace"}, Request: request, Descriptor: descriptor}
	decision, err := permission.NewSession(domain.ModeSafe).EvaluateAuthorization(context.Background(), input)
	if err != nil || decision.Action != string(domain.PermissionAllow) {
		t.Fatalf("decision=%#v err=%v", decision, err)
	}
	for name, mutate := range map[string]func(*ports.EvaluationInput){
		"alias action": func(in *ports.EvaluationInput) { in.Request.Action = "other" },
		"source":       func(in *ports.EvaluationInput) { in.Request.Source.Name = "other" },
		"digest":       func(in *ports.EvaluationInput) { in.Request.DescriptorDigest = permissionDigest("0") },
		"descriptor":   func(in *ports.EvaluationInput) { in.Descriptor.Body.Description = "forged" },
	} {
		t.Run(name, func(t *testing.T) {
			forged := protocol.DeepCopy(input)
			mutate(&forged)
			got, evaluateErr := permission.NewSession(domain.ModeSafe).EvaluateAuthorization(context.Background(), forged)
			if evaluateErr != nil || got.Action != string(domain.PermissionDeny) {
				t.Fatalf("decision=%#v err=%v", got, evaluateErr)
			}
		})
	}
}

func TestPermissionAllowsOnlyExactCatalogBoundSkillResource(t *testing.T) {
	descriptor := skilltool.BuiltinDescriptor()
	request := structuredRequest("skill", descriptor.Body.Identity)
	request.SourceRevision, request.DescriptorDigest = descriptor.Body.SourceRevision, descriptor.DescriptorDigest
	request.Effect, request.ExecutionLocus, request.RequestedProfile, request.EffectiveProfile = "observation", "builtin", "restricted", "restricted"
	request.Boundary, request.Reversibility, request.VerificationCoverage = "workspace", "not_applicable", "full"
	request.Resources = []protocol.ResourceTarget{{Kind: "skill", CanonicalID: "go-testing", Digest: "sha256:" + strings.Repeat("a", 64), Attributes: []protocol.ResourceAttribute{{Name: "runtime_generation", Value: "generation-1"}, {Name: "source", Value: "global"}, {Name: "workspace_id", Value: ""}}}}
	base := ports.EvaluationInput{Permission: ports.PermissionContext{SessionID: "session-1", Mode: domain.ModeAsk, Workspace: "/workspace"}, Request: request, Descriptor: descriptor}
	for _, mode := range []domain.PermissionMode{domain.ModeSafe, domain.ModeAsk, domain.ModeAuto} {
		input := protocol.DeepCopy(base)
		input.Permission.Mode = mode
		decision, err := permission.NewSession(mode).EvaluateAuthorization(context.Background(), input)
		if err != nil || decision.Action != string(domain.PermissionAllow) {
			t.Fatalf("mode=%s decision=%#v err=%v", mode, decision, err)
		}
	}
	project := protocol.DeepCopy(base)
	workspaceSum := sha256.Sum256([]byte(project.Permission.Workspace))
	project.Request.Resources[0].Attributes[1].Value = "project"
	project.Request.Resources[0].Attributes[2].Value = hex.EncodeToString(workspaceSum[:])
	if decision, err := permission.NewSession(domain.ModeSafe).EvaluateAuthorization(context.Background(), project); err != nil || decision.Action != string(domain.PermissionAllow) {
		t.Fatalf("project decision=%#v err=%v", decision, err)
	}
	for name, mutate := range map[string]func(*ports.EvaluationInput){
		"parent":       func(in *ports.EvaluationInput) { in.Request.Resources[0].ParentID = "forged" },
		"upper digest": func(in *ports.EvaluationInput) { in.Request.Resources[0].Digest = "sha256:" + strings.Repeat("A", 64) },
		"source":       func(in *ports.EvaluationInput) { in.Request.Resources[0].Attributes[1].Value = "project" },
		"workspace":    func(in *ports.EvaluationInput) { in.Request.Resources[0].Attributes[2].Value = "workspace" },
		"generation":   func(in *ports.EvaluationInput) { in.Request.Resources[0].Attributes[0].Value = "other" },
		"name":         func(in *ports.EvaluationInput) { in.Request.Resources[0].CanonicalID = "../go-testing" },
		"mixed": func(in *ports.EvaluationInput) {
			in.Request.Resources = append(in.Request.Resources, protocol.ResourceTarget{Kind: "file", CanonicalID: "/workspace/a"})
		},
		"descriptor": func(in *ports.EvaluationInput) { in.Descriptor.Body.Description = "forged" },
	} {
		t.Run(name, func(t *testing.T) {
			input := protocol.DeepCopy(base)
			mutate(&input)
			decision, err := permission.NewSession(domain.ModeAsk).EvaluateAuthorization(context.Background(), input)
			if err == nil && decision.Action != string(domain.PermissionAsk) {
				t.Fatalf("decision=%#v err=%v", decision, err)
			}
		})
	}
}

func TestAuthorizationResolvedReasonAndSourceFollowTerminalRule(t *testing.T) {
	decision, err := permission.ResolvePolicy(permission.PolicyInput{
		Platform: permission.PolicyRule{Action: domain.PermissionDeny, Source: "platform-policy", Reason: "platform network prohibition"},
		User:     permission.PolicyRule{Action: domain.PermissionAllow, Source: "compatibility", Reason: "configured provider compatibility policy"},
		Project:  permission.PolicyRule{Action: domain.PermissionAllow, Source: "project", Reason: "project allow"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Action != domain.PermissionDeny || decision.PolicySource != "platform-policy" || decision.Reason != "platform network prohibition" {
		t.Fatalf("terminal rule provenance was lost: %#v", decision)
	}
}

func TestPermissionStructuredFactCrossProductDoesNotBroadenAuthority(t *testing.T) {
	tests := []struct {
		name       string
		mode       domain.PermissionMode
		mutation   string
		effect     string
		locus      string
		boundary   string
		source     protocol.ToolIdentity
		resources  []protocol.ResourceTarget
		ackShell   bool
		wantAction string
	}{
		{name: "safe process observation stays denied", mode: domain.ModeSafe, mutation: "process", effect: "observation", locus: "process", boundary: "workspace", source: builtinIdentity("renamed_process"), resources: shellResources("/workspace"), wantAction: "deny"},
		{name: "ask process observation still asks", mode: domain.ModeAsk, mutation: "process", effect: "observation", locus: "process", boundary: "workspace", source: builtinIdentity("renamed_process"), resources: shellResources("/workspace"), wantAction: "ask"},
		{name: "auto process observation is not shell acknowledgement", mode: domain.ModeAuto, mutation: "process", effect: "observation", locus: "process", boundary: "workspace", source: builtinIdentity("renamed_process"), resources: shellResources("/workspace"), ackShell: true, wantAction: "ask"},
		{name: "trusted local process inside workspace", mode: domain.ModeAuto, mutation: "process", effect: "mutation", locus: "process", boundary: "process", source: builtinIdentity("renamed_process"), resources: shellResources("/workspace"), ackShell: true, wantAction: "allow"},
		{name: "trusted process outside workspace never broadens", mode: domain.ModeAuto, mutation: "process", effect: "mutation", locus: "process", boundary: "process", source: builtinIdentity("renamed_process"), resources: shellResources("/outside"), ackShell: true, wantAction: "ask"},
		{name: "built-in file mutation inside workspace", mode: domain.ModeAuto, mutation: "file", effect: "mutation", locus: "builtin", boundary: "workspace", source: builtinIdentity("renamed_file_mutator"), resources: []protocol.ResourceTarget{{Kind: "file", CanonicalID: "/workspace/a"}}, wantAction: "allow"},
		{name: "built-in mutation must target filesystem", mode: domain.ModeAuto, mutation: "file", effect: "mutation", locus: "builtin", boundary: "workspace", source: builtinIdentity("renamed_file_mutator"), resources: []protocol.ResourceTarget{{Kind: "record", CanonicalID: "record-1"}}, wantAction: "ask"},
		{name: "built-in file mutation outside cannot spoof boundary", mode: domain.ModeAuto, mutation: "file", effect: "mutation", locus: "builtin", boundary: "workspace", source: builtinIdentity("renamed_file_mutator"), resources: []protocol.ResourceTarget{{Kind: "file", CanonicalID: "/outside/a"}}, wantAction: "ask"},
		{name: "untrusted source cannot claim built-in file facts", mode: domain.ModeAuto, mutation: "file", effect: "mutation", locus: "builtin", boundary: "workspace", source: protocol.ToolIdentity{Source: "mcp", Authority: "server", Name: "edit"}, resources: []protocol.ResourceTarget{{Kind: "file", CanonicalID: "/workspace/a"}}, wantAction: "ask"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := structuredRequest("capability", test.source)
			request.Effect, request.ExecutionLocus, request.Boundary, request.Resources = test.effect, test.locus, test.boundary, test.resources
			input := structuredEvaluationInput(t, request, test.mutation, "trusted_adapter", nil)
			policy := permission.NewSession(test.mode)
			if test.ackShell {
				policy.AcknowledgeAutoShell()
			}
			decision, err := policy.EvaluateAuthorization(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			if decision.Action != test.wantAction {
				t.Fatalf("action=%s want=%s decision=%#v", decision.Action, test.wantAction, decision)
			}
		})
	}
}

func TestPermissionAllowsBoundTrustedMutationPreviewWithoutMutationGrant(t *testing.T) {
	request := structuredRequest("shell.preview", builtinIdentity("shell"))
	request.Effect = "mutation"
	request.ExecutionLocus = "process"
	request.Boundary = "process"
	request.Resources = shellResources("/workspace")
	input := structuredEvaluationInput(t, request, "process", "trusted_adapter", nil)
	input.Permission.Mode = domain.ModeAuto
	input.Request.Action = "shell.preview"
	input.Request.Effect = "observation"
	input.Request.Reversibility = "not_applicable"

	decision, err := permission.NewSession(domain.ModeAuto).EvaluateAuthorization(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Action != "allow" {
		t.Fatalf("trusted preview decision=%#v", decision)
	}

	spoofed := protocol.DeepCopy(input)
	spoofed.Request.Action = "shell.inspect"
	decision, err = permission.NewSession(domain.ModeAuto).EvaluateAuthorization(context.Background(), spoofed)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Action != "ask" {
		t.Fatalf("spoofed preview decision=%#v", decision)
	}
}

func TestPermissionConfiguredProviderCompatibilityBindsExactModelDescriptor(t *testing.T) {
	request := structuredRequest("model_egress", protocol.ToolIdentity{Source: "provider", Authority: "openai", Name: "model-a"})
	request.Effect, request.Boundary, request.ExecutionLocus = "egress", "network", "remote"
	configured := ports.ConfiguredProviderBinding{Identity: request.Source, SourceRevision: request.SourceRevision, DescriptorDigest: request.DescriptorDigest}
	base := ports.EvaluationInput{Permission: ports.PermissionContext{SessionID: "session-1", Mode: domain.ModeAsk, ConfiguredProvider: &configured}, Request: request}
	decision, err := permission.NewSession(domain.ModeAsk).EvaluateAuthorization(context.Background(), base)
	if err != nil || decision.Action != "allow" || decision.Lifetime != protocol.AuthorizationLifetimeSession || decision.PolicySource != "compatibility" {
		t.Fatalf("matching configured provider decision=%#v err=%v", decision, err)
	}
	mutations := map[string]func(*ports.EvaluationInput){
		"provider source":    func(in *ports.EvaluationInput) { in.Request.Source.Source = "extension" },
		"provider authority": func(in *ports.EvaluationInput) { in.Request.Source.Authority = "other" },
		"model":              func(in *ports.EvaluationInput) { in.Request.Source.Name = "model-b" },
		"source revision":    func(in *ports.EvaluationInput) { in.Request.SourceRevision = "revision-2" },
		"descriptor digest":  func(in *ports.EvaluationInput) { in.Request.DescriptorDigest = permissionDigest("d") },
		"effect":             func(in *ports.EvaluationInput) { in.Request.Effect = "observation" },
		"boundary":           func(in *ports.EvaluationInput) { in.Request.Boundary = "workspace" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := protocol.DeepCopy(base)
			mutate(&changed)
			got, evaluateErr := permission.NewSession(domain.ModeAsk).EvaluateAuthorization(context.Background(), changed)
			if evaluateErr != nil {
				t.Fatal(evaluateErr)
			}
			if got.Action != "ask" || got.PolicySource == "compatibility" {
				t.Fatalf("changed identity inherited compatibility allow: %#v", got)
			}
		})
	}
}

func TestSessionGrantConstraintsUseExactApplicableCanonicalSet(t *testing.T) {
	policy := permission.NewSession(domain.ModeAsk)
	request := structuredRequest("write", protocol.ToolIdentity{Source: "mcp", Authority: "server", Name: "write"})
	constraints := []protocol.AuthorizationConstraint{
		{Name: "path_prefix", Operator: "equals", Value: json.RawMessage(`"/workspace"`)},
		{Name: "size", Operator: "less_than", Value: json.RawMessage(`1024`)},
	}
	if err := policy.GrantAuthorizationSession(request, constraints); err != nil {
		t.Fatal(err)
	}
	input := ports.EvaluationInput{Permission: ports.PermissionContext{SessionID: "session-1", Mode: domain.ModeAsk}, Request: request, Constraints: protocol.DeepCopy(constraints)}
	allowed, err := policy.EvaluateAuthorization(context.Background(), input)
	if err != nil || allowed.Action != "allow" || !reflect.DeepEqual(allowed.Constraints, constraints) || !reflect.DeepEqual(allowed.Scope.Constraints, constraints) {
		t.Fatalf("exact constraints decision=%#v err=%v", allowed, err)
	}
	for name, changedConstraints := range map[string][]protocol.AuthorizationConstraint{
		"missing": constraints[:1],
		"changed": {{Name: "path_prefix", Operator: "equals", Value: json.RawMessage(`"/other"`)}, {Name: "size", Operator: "less_than", Value: json.RawMessage(`1024`)}},
	} {
		t.Run(name, func(t *testing.T) {
			changed := input
			changed.Constraints = protocol.DeepCopy(changedConstraints)
			decision, evaluateErr := policy.EvaluateAuthorization(context.Background(), changed)
			if evaluateErr != nil || decision.Action != "ask" {
				t.Fatalf("changed constraints decision=%#v err=%v", decision, evaluateErr)
			}
		})
	}
	unsorted := []protocol.AuthorizationConstraint{constraints[1], constraints[0]}
	input.Constraints = unsorted
	if _, err := policy.EvaluateAuthorization(context.Background(), input); err == nil {
		t.Fatal("unsorted applicable constraints were accepted")
	}
	if err := policy.GrantAuthorizationSession(request, unsorted); err == nil {
		t.Fatal("unsorted grant constraints were accepted")
	}
}

func builtinIdentity(name string) protocol.ToolIdentity {
	return protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: name}
}

func shellResources(cwd string) []protocol.ResourceTarget {
	return []protocol.ResourceTarget{{Kind: "directory", CanonicalID: cwd}, {Kind: "executable", CanonicalID: "/bin/sh"}}
}

func structuredEvaluationInput(t *testing.T, request protocol.AuthorizationRequest, mutation, classificationSource string, constraints []protocol.AuthorizationConstraint) ports.EvaluationInput {
	t.Helper()
	if mutation == "process" {
		request.RequestedProfile = "unsandboxed"
		request.EffectiveProfile = "unsandboxed"
	}
	body := protocol.ToolDescriptorBody{
		Identity: request.Source, SourceRevision: request.SourceRevision, DisplayName: "capability", Description: "test capability",
		InputSchema: json.RawMessage(`{"type":"object"}`), Effect: request.Effect, Mutation: mutation, ExecutionLoci: []string{request.ExecutionLocus},
		ClassificationSource: classificationSource, Idempotency: "unknown", Retry: "never_after_dispatch",
	}
	digest, err := canonicaljson.Digest(body)
	if err != nil {
		t.Fatal(err)
	}
	request.DescriptorDigest = digest
	return ports.EvaluationInput{
		Permission: ports.PermissionContext{SessionID: string(request.SessionID), Mode: domain.ModeAsk, Workspace: "/workspace"},
		Request:    request, Descriptor: protocol.ToolDescriptor{Body: body, DescriptorDigest: digest}, Constraints: protocol.DeepCopy(constraints),
	}
}

func TestPermissionModeUsesStructuredDescriptorAndResourceFactsNotNames(t *testing.T) {
	policy := permission.NewSession(domain.ModeAuto)
	builtinRenamed := structuredRequest("not_edit", protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "renamed_mutator"})
	builtinRenamed.Effect = "mutation"
	builtinRenamed.Boundary = "workspace"
	externalSpoof := structuredRequest("edit", protocol.ToolIdentity{Source: "mcp", Authority: "server", Name: "edit"})
	externalSpoof.Effect = "mutation"
	externalSpoof.Boundary = "workspace"

	builtinInput := structuredEvaluationInput(t, builtinRenamed, "file", "trusted_adapter", nil)
	builtinInput.Permission.Mode = domain.ModeAuto
	allowed, err := policy.EvaluateAuthorization(context.Background(), builtinInput)
	if err != nil {
		t.Fatal(err)
	}
	externalInput := structuredEvaluationInput(t, externalSpoof, "file", "trusted_adapter", nil)
	externalInput.Permission.Mode = domain.ModeAuto
	asked, err := policy.EvaluateAuthorization(context.Background(), externalInput)
	if err != nil {
		t.Fatal(err)
	}
	if allowed.Action != "allow" || asked.Action != "ask" {
		t.Fatalf("builtin=%s external-spoof=%s", allowed.Action, asked.Action)
	}
}

func TestSessionGrantBindsStructuredAuthorizationScopeAndResolvesOnlyAsk(t *testing.T) {
	policy := permission.NewSession(domain.ModeAsk)
	request := structuredRequest("write", protocol.ToolIdentity{Source: "mcp", Authority: "server", Name: "write"})
	if err := policy.GrantAuthorizationSession(request, nil); err != nil {
		t.Fatal(err)
	}
	allowed, err := policy.EvaluateAuthorization(context.Background(), ports.EvaluationInput{Permission: ports.PermissionContext{SessionID: "session-1", Mode: domain.ModeAsk, Workspace: "/workspace"}, Request: request})
	if err != nil || allowed.Action != "allow" || allowed.Lifetime != protocol.AuthorizationLifetimeSession {
		t.Fatalf("allowed=%#v err=%v", allowed, err)
	}
	mutations := map[string]func(*protocol.AuthorizationRequest){
		"session":         func(r *protocol.AuthorizationRequest) { r.SessionID = "session-2" },
		"capability":      func(r *protocol.AuthorizationRequest) { r.Action = "other" },
		"source":          func(r *protocol.AuthorizationRequest) { r.Source.Authority = "other" },
		"source revision": func(r *protocol.AuthorizationRequest) { r.SourceRevision = "revision-2" },
		"descriptor": func(r *protocol.AuthorizationRequest) {
			r.DescriptorDigest = protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("d", 64)}
		},
		"policy generation": func(r *protocol.AuthorizationRequest) { r.PolicyGeneration = "policy-2" },
		"resource":          func(r *protocol.AuthorizationRequest) { r.Resources[0].CanonicalID = "/workspace/other" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := protocol.DeepCopy(request)
			mutate(&changed)
			decision, evaluateErr := policy.EvaluateAuthorization(context.Background(), ports.EvaluationInput{Permission: ports.PermissionContext{SessionID: string(changed.SessionID), Mode: domain.ModeAsk, Workspace: "/workspace"}, Request: changed})
			if evaluateErr != nil {
				t.Fatal(evaluateErr)
			}
			if decision.Action != "ask" {
				t.Fatalf("changed binding reused grant: %#v", decision)
			}
		})
	}

	deniedRequest := protocol.DeepCopy(request)
	deniedRequest.PolicyProvenance[0].HardDeny = true
	denied, err := policy.EvaluateAuthorization(context.Background(), ports.EvaluationInput{Permission: ports.PermissionContext{SessionID: "session-1", Mode: domain.ModeAsk, Workspace: "/workspace", PlatformAction: domain.PermissionDeny}, Request: deniedRequest})
	if err != nil || denied.Action != "deny" {
		t.Fatalf("grant overrode deny: %#v err=%v", denied, err)
	}
}

func structuredRequest(action string, source protocol.ToolIdentity) protocol.AuthorizationRequest {
	return protocol.AuthorizationRequest{
		RequestID: "request-1", Principal: protocol.ActorRef{ID: "user-1", Kind: protocol.ActorUser}, Actor: protocol.ActorRef{ID: "agent-1", Kind: protocol.ActorAgent},
		SessionID: "session-1", TaskID: "task-1", TurnID: "turn-1", ActivityID: "activity-1", CallID: "call-1", QueueID: "queue-1",
		Source: source, SourceRevision: "revision-1", DescriptorDigest: permissionDigest("a"), Action: action,
		Resources: []protocol.ResourceTarget{{Kind: "file", CanonicalID: "/workspace/file"}}, ExecutionLocus: "local", RequestedProfile: "default", EffectiveProfile: "default",
		Effect: "mutation", Boundary: "workspace", Reversibility: "reversible", VerificationCoverage: "full", RuntimeGenerationID: "generation-1", PolicyGeneration: "policy-1",
		PolicyProvenance: []protocol.PolicyProvenance{{Source: "user", Revision: "revision-1", Generation: "policy-1"}},
		PlanDigest:       permissionDigest("b"), RequestDigest: permissionDigest("c"), DispatchDigest: permissionDigest("e"),
	}
}

func permissionDigest(fill string) protocol.Digest {
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat(fill, 64)}
}

func TestPermissionMatrix(t *testing.T) {
	cases := []struct {
		name     string
		mode     domain.PermissionMode
		tool     string
		mutation domain.MutationKind
		inside   bool
		want     domain.PermissionAction
	}{
		{"safe read inside", domain.ModeSafe, "read", domain.MutationReadOnly, true, domain.PermissionAllow},
		{"safe skill inside", domain.ModeSafe, "skill", domain.MutationReadOnly, true, domain.PermissionAllow},
		{"safe read outside", domain.ModeSafe, "read", domain.MutationReadOnly, false, domain.PermissionDeny},
		{"safe edit", domain.ModeSafe, "edit", domain.MutationFile, true, domain.PermissionDeny},
		{"safe shell", domain.ModeSafe, "shell", domain.MutationProcess, true, domain.PermissionDeny},
		{"ask read inside", domain.ModeAsk, "read", domain.MutationReadOnly, true, domain.PermissionAllow},
		{"ask skill inside", domain.ModeAsk, "skill", domain.MutationReadOnly, true, domain.PermissionAllow},
		{"ask read outside", domain.ModeAsk, "read", domain.MutationReadOnly, false, domain.PermissionAsk},
		{"ask edit", domain.ModeAsk, "edit", domain.MutationFile, true, domain.PermissionAsk},
		{"ask shell", domain.ModeAsk, "shell", domain.MutationProcess, true, domain.PermissionAsk},
		{"auto read inside", domain.ModeAuto, "read", domain.MutationReadOnly, true, domain.PermissionAllow},
		{"auto skill inside", domain.ModeAuto, "skill", domain.MutationReadOnly, true, domain.PermissionAllow},
		{"auto edit inside", domain.ModeAuto, "edit", domain.MutationFile, true, domain.PermissionAllow},
		{"auto edit outside", domain.ModeAuto, "edit", domain.MutationFile, false, domain.PermissionAsk},
		{"auto shell unacknowledged", domain.ModeAuto, "shell", domain.MutationProcess, true, domain.PermissionAsk},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			policy := permission.NewSession(tc.mode)
			decision := policy.Evaluate(context.Background(), ports.PermissionContext{SessionID: "s", Mode: tc.mode, Workspace: "/w"}, domain.PreparedToolRequest{Request: domain.ToolRequest{Name: tc.tool}, Mutation: tc.mutation, CanonicalScope: "/w/a", InsideWorkspace: tc.inside, Summary: tc.name})
			if decision.Action != tc.want {
				t.Fatalf("got=%s want=%s", decision.Action, tc.want)
			}
		})
	}
}

func prepared(name, scope string, inside bool) domain.PreparedToolRequest {
	mutation := domain.MutationProcess
	if name == "read" || name == "search" || name == "skill" {
		mutation = domain.MutationReadOnly
	} else if name == "edit" {
		mutation = domain.MutationFile
	}
	return domain.PreparedToolRequest{Request: domain.ToolRequest{Name: name}, Mutation: mutation, CanonicalScope: scope, InsideWorkspace: inside}
}

func TestSessionGrantMatchesToolAndExactScope(t *testing.T) {
	policy := permission.NewSession(domain.ModeAsk)
	policy.GrantSession("edit", "/w/a.go")
	allowed := policy.Evaluate(context.Background(), ports.PermissionContext{}, prepared("edit", "/w/a.go", true))
	otherPath := policy.Evaluate(context.Background(), ports.PermissionContext{}, prepared("edit", "/w/b.go", true))
	otherTool := policy.Evaluate(context.Background(), ports.PermissionContext{}, prepared("shell", "/w/a.go", true))
	if allowed.Action != domain.PermissionAllow || allowed.Lifetime != domain.PermissionSession {
		t.Fatalf("allowed=%#v", allowed)
	}
	if otherPath.Action != domain.PermissionAsk || otherTool.Action != domain.PermissionAsk {
		t.Fatalf("path=%#v tool=%#v", otherPath, otherTool)
	}
}

func TestSafeOverridesGrantsAndAutoStillRequiresShellAck(t *testing.T) {
	policy := permission.NewSession(domain.ModeAsk)
	policy.GrantSession("edit", "/w/a.go")
	policy.GrantSession("shell", "/w\x00echo ok")
	policy.SetMode(domain.ModeSafe)
	if got := policy.Evaluate(context.Background(), ports.PermissionContext{}, prepared("edit", "/w/a.go", true)); got.Action != domain.PermissionDeny {
		t.Fatalf("safe edit=%#v", got)
	}
	policy.SetMode(domain.ModeAuto)
	if got := policy.Evaluate(context.Background(), ports.PermissionContext{}, prepared("shell", "/w\x00echo ok", true)); got.Action != domain.PermissionAsk {
		t.Fatalf("auto shell=%#v", got)
	}
}

func TestLeavingAutoClearsShellAcknowledgement(t *testing.T) {
	policy := permission.NewSession(domain.ModeAuto)
	shell := prepared("shell", "/w\x00echo ok", true)
	policy.AcknowledgeAutoShell()
	if got := policy.Evaluate(context.Background(), ports.PermissionContext{}, shell); got.Action != domain.PermissionAllow {
		t.Fatalf("acknowledged=%#v", got)
	}
	policy.SetMode(domain.ModeAsk)
	policy.SetMode(domain.ModeAuto)
	if got := policy.Evaluate(context.Background(), ports.PermissionContext{}, shell); got.Action != domain.PermissionAsk {
		t.Fatalf("re-entered=%#v", got)
	}
}

func event(kind domain.EventKind, payload any) domain.DurableEvent {
	raw, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return domain.DurableEvent{Kind: kind, Payload: raw}
}

func replayWith(mode domain.PermissionMode, events ...domain.DurableEvent) domain.SessionReplay {
	return domain.SessionReplay{Session: domain.Session{Mode: mode}, Events: events}
}

func TestRestoreReplaysGrantAndAutoAcknowledgement(t *testing.T) {
	replay := replayWith(domain.ModeAuto,
		event(domain.EventPermissionResolved, domain.PermissionPayload{Tool: "edit", Decision: domain.PermissionDecision{Action: domain.PermissionAllow, Lifetime: domain.PermissionSession, Scope: "/w/a.go"}}),
		event(domain.EventTrustedExecutionAcknowledged, domain.TrustedExecutionPayload{Enabled: true}),
	)
	policy := permission.Restore(replay)
	if got := policy.Evaluate(context.Background(), ports.PermissionContext{}, prepared("edit", "/w/a.go", true)); got.Action != domain.PermissionAllow || got.Lifetime != domain.PermissionSession {
		t.Fatalf("grant=%#v", got)
	}
	if got := policy.Evaluate(context.Background(), ports.PermissionContext{}, prepared("shell", "/w\x00echo ok", true)); got.Action != domain.PermissionAllow {
		t.Fatalf("shell=%#v", got)
	}
}

func TestRestoreClearsAutoAcknowledgementAfterLaterExit(t *testing.T) {
	replay := replayWith(domain.ModeAuto,
		event(domain.EventTrustedExecutionAcknowledged, domain.TrustedExecutionPayload{Enabled: true}),
		event(domain.EventModeChanged, domain.ModeChangedPayload{Mode: domain.ModeAsk}),
		event(domain.EventModeChanged, domain.ModeChangedPayload{Mode: domain.ModeAuto}),
	)
	policy := permission.Restore(replay)
	if got := policy.Evaluate(context.Background(), ports.PermissionContext{}, prepared("shell", "/w\x00echo ok", true)); got.Action != domain.PermissionAsk {
		t.Fatalf("restored shell=%#v", got)
	}
}
