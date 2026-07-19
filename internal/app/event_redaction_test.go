package app

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/secret"
)

func TestPublishRedactsEquivalentTextFieldsWithActiveBinding(t *testing.T) {
	const configuredSecret = "configured-publish-secret-sentinel"
	application := New(Options{Redactors: secret.NewBinding(secret.New(configuredSecret))})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.publishEvents(ctx)

	event := Event{
		Kind:    EventError,
		Message: configuredSecret,
		Draft:   configuredSecret,
		Err:     errors.Join(errors.New(configuredSecret), errors.New("ordinary error")),
		Runtime: agent.RuntimeEvent{
			Kind:  agent.RuntimeToolCompleted,
			State: configuredSecret,
			Text:  configuredSecret,
			Progress: &domain.ToolProgress{
				CallID: configuredSecret,
				Text:   configuredSecret,
			},
			Result: &domain.ToolResult{
				CallID:      configuredSecret,
				Content:     configuredSecret,
				ArtifactIDs: []string{configuredSecret},
				FileChange: &domain.FileChange{
					CallID:      configuredSecret,
					Path:        configuredSecret,
					Diff:        configuredSecret,
					ArtifactIDs: []string{configuredSecret},
				},
				WorkspaceChanges: &domain.WorkspaceChanges{
					Status:      configuredSecret,
					Diff:        configuredSecret,
					Notice:      configuredSecret,
					ArtifactIDs: []string{configuredSecret},
				},
			},
		},
		Permission: &ports.PermissionPrompt{
			SessionID: configuredSecret,
			Call: domain.PreparedToolRequest{
				Request: domain.ToolRequest{
					CallID:    configuredSecret,
					Name:      configuredSecret,
					Input:     json.RawMessage(`{"value":"` + configuredSecret + `"}`),
					Workspace: configuredSecret,
				},
				CanonicalScope: configuredSecret,
				ProposedDiff:   configuredSecret,
				Summary:        configuredSecret,
				ApprovalScope:  configuredSecret,
			},
		},
		Selection: domain.ModelSelection{Profile: configuredSecret, Model: configuredSecret},
		Models:    []domain.ModelSelection{{Profile: configuredSecret, Model: configuredSecret}},
		Session: domain.Session{
			ID:        configuredSecret,
			Workspace: domain.Workspace{ID: configuredSecret, CanonicalPath: configuredSecret},
			Title:     configuredSecret,
		},
		Replay: domain.SessionReplay{
			Session:      domain.Session{ID: configuredSecret, Title: configuredSecret},
			RecoveryNote: configuredSecret,
			Events: []domain.DurableEvent{{
				EventID:   configuredSecret,
				SessionID: configuredSecret,
				Time:      time.Unix(1, 0),
				Kind:      domain.EventUserMessage,
				Payload:   json.RawMessage(`{"content":"` + configuredSecret + `"}`),
			}},
		},
	}
	if !application.publish(ctx, event) {
		t.Fatal("publish rejected event")
	}
	published := <-application.Events()
	raw, err := json.Marshal(published)
	if err != nil {
		t.Fatal("marshal published event")
	}
	if strings.Contains(string(raw), configuredSecret) {
		t.Fatal("published event contains a configured secret in a text field")
	}
	if published.Err == nil || strings.Contains(published.Err.Error(), configuredSecret) {
		t.Fatal("published event error was not safely redacted")
	}
	assertErrorTreeOmits(t, published.Err, configuredSecret)
	if published.Permission == nil || strings.Contains(published.Permission.Call.ApprovalScope, configuredSecret) {
		t.Fatal("published permission approval scope was not safely redacted")
	}
}

func TestPublishRedactsSecretFoundOnlyInApprovalScope(t *testing.T) {
	const configuredSecret = "approval-scope-only-secret-sentinel"
	application := New(Options{Redactors: secret.NewBinding(secret.New(configuredSecret))})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.publishEvents(ctx)

	prompt := ports.PermissionPrompt{Call: domain.PreparedToolRequest{
		Request:        domain.ToolRequest{CallID: "scope-only", Name: "shell"},
		CanonicalScope: "/workspace/[REDACTED]",
		Summary:        "ordinary summary",
		ApprovalScope:  "/workspace/" + configuredSecret,
	}}
	if !application.publish(ctx, Event{Kind: EventPermissionRequested, Permission: &prompt}) {
		t.Fatal("publish rejected event")
	}
	published := <-application.Events()
	if published.Permission == nil {
		t.Fatal("published permission is nil")
	}
	if strings.Contains(published.Permission.Call.ApprovalScope, configuredSecret) {
		t.Fatal("published in-memory permission contains the raw approval scope")
	}
	if published.Permission.Call.ApprovalScope != "/workspace/[REDACTED]" {
		t.Fatal("published permission does not contain the displayed approval scope")
	}
}

func TestPublishUsesGenerationLeaseForEncodedVariants(t *testing.T) {
	registry := secret.NewRegistry()
	lease, err := registry.Acquire("generation-app", [][]byte{[]byte("publish-secret")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Close() }()
	application := New(Options{Redactors: secret.NewBinding(lease)})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.publishEvents(ctx)
	encoded := "cHVibGlzaC1zZWNyZXQ="
	if !application.publish(ctx, Event{Kind: EventNotice, Message: encoded}) {
		t.Fatal("publish rejected event")
	}
	if published := <-application.Events(); strings.Contains(published.Message, encoded) || published.Message != "[REDACTED]" {
		t.Fatalf("published=%+v", published)
	}
}

func assertErrorTreeOmits(t *testing.T, err error, forbidden string) {
	t.Helper()
	if err == nil {
		return
	}
	if strings.Contains(err.Error(), forbidden) {
		t.Fatal("published event error tree contains a configured secret")
	}
	if wrapped, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range wrapped.Unwrap() {
			assertErrorTreeOmits(t, cause, forbidden)
		}
		return
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		assertErrorTreeOmits(t, wrapped.Unwrap(), forbidden)
	}
}
