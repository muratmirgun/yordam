package permission_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/permission"
	"github.com/muratmirgun/yordam/internal/ports"
)

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
