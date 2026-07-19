package jsonl_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
	"github.com/muratmirgun/yordam/internal/tools/output"
)

func TestSanitizedStoreAndArtifactsContainNoConfiguredSecret(t *testing.T) {
	const sentinel = "synthetic-profile-secret"
	root := t.TempDir()
	redactor := secret.New(sentinel)
	store := jsonl.New(root, jsonl.Options{Sanitize: redactor.JSON})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}

	payload := map[string]any{
		"message": "failed with " + sentinel,
		"nested":  []any{map[string]any{"detail": sentinel}},
	}
	event, err := store.WriteLegacyFixture(context.Background(), session.ID, domain.EventTurnFailed, payload)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(event.Payload, []byte(sentinel)) {
		t.Fatalf("returned event payload leaked secret: %s", event.Payload)
	}

	buffer := output.New(output.Options{SessionID: session.ID, Artifacts: store, Redact: redactor})
	toolOutput := strings.Repeat("x", output.ModelExcerptBytes) + sentinel
	if _, err := buffer.Write([]byte(toolOutput)); err != nil {
		t.Fatal(err)
	}
	result, err := buffer.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.WriteLegacyFixture(context.Background(), session.ID, domain.EventToolResult, domain.ToolResultPayload{Result: result}); err != nil {
		t.Fatal(err)
	}

	sessionRoot := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	err = filepath.Walk(sessionRoot, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(raw, []byte(sentinel)) {
			t.Errorf("configured secret found in %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
