package evidence_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/muratmirgun/yordam/internal/evidence"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
)

func TestLegacyArtifactMigrationCreatesDurableAlias(t *testing.T) {
	root, store := newEvidenceStore(t, secret.NewAdmissionScanner())
	legacy := createLegacyArtifact(t, root, "workspace-a", "session-a", "legacy-a", []byte("legacy-content"))
	record, err := store.MigrateLegacyArtifact(context.Background(), "session-a", "legacy-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Body.LegacyArtifactAliases) != 1 || record.Body.LegacyArtifactAliases[0] != "legacy-a" || record.Body.SessionID != "session-a" {
		t.Fatalf("record=%+v", record)
	}
	if err := os.Remove(legacy); err != nil {
		t.Fatal(err)
	}
	restarted, err := evidence.New(root, secret.NewAdmissionScanner())
	if err != nil {
		t.Fatal(err)
	}
	aliased, err := restarted.MigrateLegacyArtifact(context.Background(), "session-a", "legacy-a")
	if err != nil {
		t.Fatalf("durable alias was not resolved after source removal: %v", err)
	}
	if aliased.Body.ID != record.Body.ID || aliased.Body.Blob == nil || aliased.Body.Blob.Digest != record.Body.Blob.Digest {
		t.Fatalf("alias changed: first=%+v second=%+v", record, aliased)
	}
	opened, err := restarted.Open(context.Background(), aliased.Body.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	if raw, err := io.ReadAll(opened); err != nil || string(raw) != "legacy-content" {
		t.Fatalf("content=(%q, %v)", raw, err)
	}
}

func TestLegacyArtifactMigrationRejectsTraversalAndSymlink(t *testing.T) {
	root, store := newEvidenceStore(t, secret.NewAdmissionScanner())
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := createLegacyArtifact(t, root, "workspace-a", "session-a", "legacy-a", []byte("inside"))
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, path); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"../outside", "legacy-a"} {
		if _, err := store.MigrateLegacyArtifact(context.Background(), protocol.SessionID("session-a"), id); !errors.Is(err, evidence.ErrUnsafePath) {
			t.Fatalf("id=%q error=%v", id, err)
		}
	}
}

func createLegacyArtifact(t *testing.T, root, workspace, session, id string, content []byte) string {
	t.Helper()
	directory := filepath.Join(root, "workspaces", workspace, "sessions", session, "artifacts")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, id+".bin")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
