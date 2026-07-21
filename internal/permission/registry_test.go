package permission

import (
	"context"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestSessionPolicyRegistryChildStartsWithOnlyParentMode(t *testing.T) {
	parent := NewSession(domain.ModeAuto)
	parent.AcknowledgeAutoShell()
	parent.GrantSession("shell", "/workspace")
	registry := NewSessionPolicyRegistry()
	if err := registry.RegisterParent("parent", parent); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterChild("child", domain.ModeAuto); err != nil {
		t.Fatal(err)
	}

	call := domain.PreparedToolRequest{Request: domain.ToolRequest{Name: "shell"}, Mutation: domain.MutationProcess, CanonicalScope: "/workspace", InsideWorkspace: true}
	if got := registry.Evaluate(context.Background(), ports.PermissionContext{SessionID: "child"}, call); got.Action != domain.PermissionAsk {
		t.Fatalf("child inherited mutable parent grant or acknowledgement: %+v", got)
	}
	if got := registry.Evaluate(context.Background(), ports.PermissionContext{SessionID: "parent"}, call); got.Action != domain.PermissionAllow {
		t.Fatalf("parent grant changed: %+v", got)
	}
}

func TestSessionPolicyRegistryGrantsAuthorizationToExactSession(t *testing.T) {
	registry := NewSessionPolicyRegistry()
	if err := registry.RegisterParent("parent", NewSession(domain.ModeAsk)); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterChild("child", domain.ModeAsk); err != nil {
		t.Fatal(err)
	}
	request := registryAuthorizationRequest("child")
	if err := registry.GrantAuthorizationSession(request, nil); err != nil {
		t.Fatal(err)
	}
	if got, err := registry.EvaluateAuthorization(context.Background(), ports.EvaluationInput{Permission: ports.PermissionContext{SessionID: "child"}, Request: request}); err != nil || got.Action != "allow" || got.PolicySource != "session" {
		t.Fatalf("child grant = %+v, %v", got, err)
	}
	parentRequest := registryAuthorizationRequest("parent")
	if got, err := registry.EvaluateAuthorization(context.Background(), ports.EvaluationInput{Permission: ports.PermissionContext{SessionID: "parent"}, Request: parentRequest}); err != nil || got.Action != "ask" {
		t.Fatalf("parent received child grant = %+v, %v", got, err)
	}
}

func registryAuthorizationRequest(session string) protocol.AuthorizationRequest {
	return protocol.AuthorizationRequest{RequestID: "request-" + session, SessionID: protocol.SessionID(session), ActivityID: protocol.ActivityID("activity-" + session), CallID: "call-" + session, QueueID: "queue-" + session, Principal: protocol.ActorRef{ID: "user", Kind: protocol.ActorUser}, Actor: protocol.ActorRef{ID: "runtime", Kind: protocol.ActorSystem}, Source: protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "edit"}, SourceRevision: "v1", DescriptorDigest: registryDigest("a"), Action: "file.write", Resources: []protocol.ResourceTarget{{Kind: "path", CanonicalID: "/workspace/a.txt"}}, ExecutionLocus: "local", RequestedProfile: "restricted", EffectiveProfile: "restricted", Effect: "mutation", Boundary: "workspace", Reversibility: "exact", VerificationCoverage: "exact", RuntimeGenerationID: "runtime", PolicyGeneration: "policy", PolicyProvenance: []protocol.PolicyProvenance{{Source: "runtime", Revision: "v1", Generation: "policy"}}, PlanDigest: registryDigest("b"), RequestDigest: registryDigest("c"), DispatchDigest: registryDigest("d")}
}

func registryDigest(value string) protocol.Digest {
	return protocol.Digest{Algorithm: "sha256", Value: strings.Repeat(value, 64)}
}
