package components_test

import (
	"encoding/json"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/tui/components"
)

func TestPermissionStructuredScopeRendersExactlyAndReturnsMinimalResponse(t *testing.T) {
	request := protocol.AuthorizationRequest{
		RequestID: "authorization-1", Principal: protocol.ActorRef{ID: "user-1", Kind: protocol.ActorUser}, Actor: protocol.ActorRef{ID: "agent-1", Kind: protocol.ActorAgent},
		SessionID: "session-1", ActivityID: "activity-1", CallID: "call-1", QueueID: "queue-1", Source: protocol.ToolIdentity{Source: "mcp", Authority: "server-a", Name: "write_record"},
		SourceRevision: "source-r1", DescriptorDigest: componentDigest("a"), Action: "write_record", Resources: []protocol.ResourceTarget{{Kind: "record", CanonicalID: "records/42", ParentID: "records", Digest: strings.Repeat("b", 64), Attributes: []protocol.ResourceAttribute{{Name: "field", Value: "status"}}}},
		ExecutionLocus: "remote", RequestedProfile: "networked", EffectiveProfile: "networked", Effect: "mutation", Boundary: "remote", Reversibility: "unknown", VerificationCoverage: "provider_reported",
		RuntimeGenerationID: "generation-1", PolicyGeneration: "policy-1", PolicyProvenance: []protocol.PolicyProvenance{{Source: "user", Revision: "1", Generation: "policy-1"}}, PlanDigest: componentDigest("c"), RequestDigest: componentDigest("d"), DispatchDigest: componentDigest("e"),
	}
	scopeDigest := componentDigest("f")
	constraint := protocol.AuthorizationConstraint{Name: "fields", Operator: "subset", Value: json.RawMessage(`["status"]`)}
	prompt := ports.AuthorizationPrompt{Request: request, Scope: protocol.CanonicalAuthorizationScope{Capability: request.Action, Source: request.Source, Resources: request.Resources, Constraints: []protocol.AuthorizationConstraint{constraint}}, ScopeDigest: scopeDigest, Summary: "update status"}
	actor := protocol.ActorRef{ID: "user-1", Kind: protocol.ActorUser}
	var response protocol.ApprovalResponse
	modal := components.NewAuthorizationPermission(prompt, actor, func(value protocol.ApprovalResponse) { response = value })
	view := modal.View()
	for _, exactFact := range []string{"mcp/server-a/write_record", "write_record", "record", "records/42", "records", strings.Repeat("b", 64), "field=status", "fields subset [\"status\"]", "remote", "networked", scopeDigest.Value} {
		if !strings.Contains(view, exactFact) {
			t.Fatalf("view missing exact structured fact %q:\n%s", exactFact, view)
		}
	}
	modal.HandleKey("s")
	if response.RequestID != request.RequestID || response.Action != "allow" || response.Lifetime != protocol.AuthorizationLifetimeSession || response.Actor != actor || response.ScopeDigest != scopeDigest || response.Reason == "" {
		t.Fatalf("response=%#v", response)
	}
	raw, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 6 {
		t.Fatalf("UI response authored extra binding fields: %s", raw)
	}
}

func TestPermissionStructuredDenialCannotReplaceDisplayedScopeDigest(t *testing.T) {
	request := protocol.AuthorizationRequest{RequestID: "authorization-1", Source: protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "read"}, Action: "read", Resources: []protocol.ResourceTarget{{Kind: "file", CanonicalID: "/workspace/a"}}}
	prompt := ports.AuthorizationPrompt{Request: request, Scope: protocol.CanonicalAuthorizationScope{Capability: request.Action, Source: request.Source, Resources: request.Resources}, ScopeDigest: componentDigest("f")}
	var response protocol.ApprovalResponse
	modal := components.NewAuthorizationPermission(prompt, protocol.ActorRef{ID: "user", Kind: protocol.ActorUser}, func(value protocol.ApprovalResponse) { response = value })
	modal.HandleKey("n")
	if response.ScopeDigest != prompt.ScopeDigest || response.Action != "deny" || response.Lifetime != protocol.AuthorizationLifetimeOnce {
		t.Fatalf("response=%#v", response)
	}
}

func componentDigest(fill string) protocol.Digest {
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat(fill, 64)}
}

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
