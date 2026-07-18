package evidence_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/evidence"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
)

func TestLegacyArtifactMigrationCreatesDurableAlias(t *testing.T) {
	resolver := &legacyResolverFixture{artifacts: map[string][]byte{"session-a/legacy-a": []byte("legacy-content")}}
	root, store := newEvidenceStoreWithResolver(t, resolver)
	record, err := store.MigrateLegacyArtifact(context.Background(), "session-a", "legacy-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Body.LegacyArtifactAliases) != 1 || record.Body.LegacyArtifactAliases[0] != "legacy-a" || record.Body.SessionID != "session-a" {
		t.Fatalf("record=%+v", record)
	}
	resolver.remove("session-a", "legacy-a")
	registry := secret.NewRegistry()
	lease, err := registry.Acquire("legacy-restart", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	restarted, err := evidence.New(root, lease, evidence.WithLegacyResolver(resolver))
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
	_, store := newEvidenceStoreWithResolver(t, &legacyResolverFixture{err: evidence.ErrUnsafePath})
	for _, id := range []string{"../outside", "legacy-a"} {
		if _, err := store.MigrateLegacyArtifact(context.Background(), protocol.SessionID("session-a"), id); !errors.Is(err, evidence.ErrUnsafePath) {
			t.Fatalf("id=%q error=%v", id, err)
		}
	}
}

func TestLegacyAliasAdmitsFinalBodyAndDigestBeforeAnyPublication(t *testing.T) {
	root := filepath.Join(t.TempDir(), "not-created")
	sessionID := protocol.SessionID("session-a")
	artifactID := "legacy-a"
	sum := sha256.Sum256([]byte("legacy-artifact\x00" + string(sessionID) + "\x00" + artifactID))
	evidenceID := protocol.EvidenceID("legacy-" + hex.EncodeToString(sum[:]))
	body := struct {
		SessionID   protocol.SessionID   `json:"session_id"`
		ArtifactID  string               `json:"artifact_id"`
		WorkspaceID protocol.WorkspaceID `json:"workspace_id"`
		EvidenceID  protocol.EvidenceID  `json:"evidence_id"`
	}{sessionID, artifactID, "workspace-a", evidenceID}
	digest, err := canonicaljson.Digest(body)
	if err != nil {
		t.Fatal(err)
	}
	registry := secret.NewRegistry()
	lease, err := registry.Acquire("legacy-final-alias", [][]byte{[]byte(digest.Value)})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	resolver := &legacyResolverFixture{artifacts: map[string][]byte{"session-a/legacy-a": []byte("legacy-content")}}
	store, err := evidence.New(root, lease, evidence.WithLegacyResolver(resolver))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.MigrateLegacyArtifact(context.Background(), sessionID, artifactID); !errors.Is(err, secret.ErrSecretDetected) {
		t.Fatalf("alias admission error=%v", err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("rejected final alias created storage: %v", err)
	}
}

func TestLegacyAliasRejectsInRootSymlinkSwapForExistingAndFreshStores(t *testing.T) {
	resolver := &legacyResolverFixture{artifacts: map[string][]byte{"session-a/legacy-a": []byte("legacy-content")}}
	root, store := newEvidenceStoreWithResolver(t, resolver)
	if _, err := store.MigrateLegacyArtifact(context.Background(), "session-a", "legacy-a"); err != nil {
		t.Fatal(err)
	}
	aliases := filepath.Join(root, "evidence", "aliases")
	retained := aliases + "-retained"
	if err := os.Rename(aliases, retained); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Base(retained), aliases); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MigrateLegacyArtifact(context.Background(), "session-a", "legacy-a"); !errors.Is(err, evidence.ErrUnsafePath) {
		t.Fatalf("existing store alias swap error=%v", err)
	}
	registry := secret.NewRegistry()
	lease, err := registry.Acquire("legacy-fresh-swap", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	fresh, err := evidence.New(root, lease, evidence.WithLegacyResolver(resolver))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if _, err := fresh.MigrateLegacyArtifact(context.Background(), "session-a", "legacy-a"); !errors.Is(err, evidence.ErrUnsafePath) {
		t.Fatalf("fresh store alias swap error=%v", err)
	}
}

func newEvidenceStoreWithResolver(t *testing.T, resolver evidence.LegacyResolver) (string, evidence.Store) {
	t.Helper()
	root := t.TempDir()
	registry := secret.NewRegistry()
	lease, err := registry.Acquire("legacy-evidence", nil)
	if err != nil {
		t.Fatal(err)
	}
	store, err := evidence.New(root, lease, evidence.WithLegacyResolver(resolver))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = lease.Close() })
	return root, store
}

type legacyResolverFixture struct {
	mu        sync.Mutex
	artifacts map[string][]byte
	err       error
}

func (r *legacyResolverFixture) ResolveLegacyArtifact(_ context.Context, sessionID protocol.SessionID, artifactID string) (protocol.WorkspaceID, []byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return "", nil, r.err
	}
	raw, ok := r.artifacts[fmt.Sprintf("%s/%s", sessionID, artifactID)]
	if !ok {
		return "", nil, errors.New("legacy source must not be opened on alias hit")
	}
	return "workspace-a", append([]byte(nil), raw...), nil
}

func (r *legacyResolverFixture) remove(sessionID, artifactID string) {
	r.mu.Lock()
	delete(r.artifacts, fmt.Sprintf("%s/%s", sessionID, artifactID))
	r.mu.Unlock()
}
