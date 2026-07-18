package permission_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/permission"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
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

func TestPermissionModeUsesStructuredDescriptorAndResourceFactsNotNames(t *testing.T) {
	policy := permission.NewSession(domain.ModeAuto)
	builtinRenamed := structuredRequest("not_edit", protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "renamed_mutator"})
	builtinRenamed.Effect = "mutation"
	builtinRenamed.Boundary = "workspace"
	externalSpoof := structuredRequest("edit", protocol.ToolIdentity{Source: "mcp", Authority: "server", Name: "edit"})
	externalSpoof.Effect = "mutation"
	externalSpoof.Boundary = "workspace"

	allowed, err := policy.EvaluateAuthorization(context.Background(), ports.PermissionContext{SessionID: "session-1", Mode: domain.ModeAuto}, builtinRenamed)
	if err != nil {
		t.Fatal(err)
	}
	asked, err := policy.EvaluateAuthorization(context.Background(), ports.PermissionContext{SessionID: "session-1", Mode: domain.ModeAuto}, externalSpoof)
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
	allowed, err := policy.EvaluateAuthorization(context.Background(), ports.PermissionContext{SessionID: "session-1", Mode: domain.ModeAsk}, request)
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
			decision, evaluateErr := policy.EvaluateAuthorization(context.Background(), ports.PermissionContext{SessionID: string(changed.SessionID), Mode: domain.ModeAsk}, changed)
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
	denied, err := policy.EvaluateAuthorization(context.Background(), ports.PermissionContext{SessionID: "session-1", Mode: domain.ModeAsk, PlatformAction: domain.PermissionDeny}, deniedRequest)
	if err != nil || denied.Action != "deny" {
		t.Fatalf("grant overrode deny: %#v err=%v", denied, err)
	}
}

func TestPermissionConfiguredProviderCompatibilityPolicyIsDurableAllow(t *testing.T) {
	request := structuredRequest("model_egress", protocol.ToolIdentity{Source: "provider", Authority: "openai", Name: "model-a"})
	request.Effect = "egress"
	request.Boundary = "network"
	decision, err := permission.NewSession(domain.ModeAsk).EvaluateAuthorization(context.Background(), ports.PermissionContext{
		SessionID: "session-1", Mode: domain.ModeAsk, ConfiguredProvider: true,
	}, request)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Action != "allow" || decision.Lifetime != protocol.AuthorizationLifetimeSession || decision.PolicySource != "compatibility" {
		t.Fatalf("decision=%#v", decision)
	}
	request.Source.Authority = "unconfigured"
	unconfigured, err := permission.NewSession(domain.ModeAsk).EvaluateAuthorization(context.Background(), ports.PermissionContext{SessionID: "session-1", Mode: domain.ModeAsk}, request)
	if err != nil || unconfigured.Action != "ask" {
		t.Fatalf("unconfigured=%#v err=%v", unconfigured, err)
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
		{"safe read outside", domain.ModeSafe, "read", domain.MutationReadOnly, false, domain.PermissionDeny},
		{"safe edit", domain.ModeSafe, "edit", domain.MutationFile, true, domain.PermissionDeny},
		{"safe shell", domain.ModeSafe, "shell", domain.MutationProcess, true, domain.PermissionDeny},
		{"ask read inside", domain.ModeAsk, "read", domain.MutationReadOnly, true, domain.PermissionAllow},
		{"ask read outside", domain.ModeAsk, "read", domain.MutationReadOnly, false, domain.PermissionAsk},
		{"ask edit", domain.ModeAsk, "edit", domain.MutationFile, true, domain.PermissionAsk},
		{"ask shell", domain.ModeAsk, "shell", domain.MutationProcess, true, domain.PermissionAsk},
		{"auto read inside", domain.ModeAuto, "read", domain.MutationReadOnly, true, domain.PermissionAllow},
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
	if name == "read" || name == "search" {
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
