package jsonl_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
)

func TestAppendBatchUsesGenerationLeaseBeforeJournalWrite(t *testing.T) {
	registry := secret.NewRegistry()
	registration, err := registry.Acquire("generation-a", [][]byte{[]byte("journal-secret")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registration.Close() }()
	fixture := newV2JournalWithOptions(t, jsonl.Options{Encoder: passthroughEncoder{}, Secrets: registry})
	event := proposed("evt-admission", protocol.EventTaskCreated)
	event.SessionID = protocol.SessionID(fixture.ref.ID)
	event.RuntimeGenerationID = "generation-a"
	event.Payload = []byte(`{"goal":"am91cm5hbC1zZWNyZXQ=","outcome_contract_id":"contract-1","contract_version":1}`)
	result, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: fixture.head, TransactionID: "txn-admission", Events: []protocol.ProposedEvent{event},
	})
	if err != nil || result.Status != journal.AppendCommitted {
		t.Fatalf("append=(%+v, %v)", result, err)
	}
	raw, err := os.ReadFile(fixture.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("am91cm5hbC1zZWNyZXQ=")) {
		t.Fatalf("encoded secret remained in journal: %s", raw)
	}
}

func TestArtifactAdmissionRejectsSecretBeforeTemporaryWrite(t *testing.T) {
	registry := secret.NewRegistry()
	lease, err := registry.Acquire("generation-a", [][]byte{[]byte("artifact-secret")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Close() }()
	fixture := newV2JournalWithOptions(t, jsonl.Options{Encoder: passthroughEncoder{}, ArtifactAdmission: lease})
	if _, err := fixture.repo.Put(context.Background(), string(fixture.ref.ID), "text/plain", bytes.NewReader([]byte("YXJ0aWZhY3Qtc2VjcmV0")), 1024); !errors.Is(err, secret.ErrSecretDetected) {
		t.Fatalf("put error=%v", err)
	}
	artifacts := fixtureArtifactDirectory(t, fixture)
	entries, err := os.ReadDir(artifacts)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("artifact admission wrote files: %v", entries)
	}
}

func fixtureArtifactDirectory(t *testing.T, fixture v2JournalFixture) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(fixture.root, "workspaces", "*", "sessions", string(fixture.ref.ID), "artifacts"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("artifact roots=%v err=%v", matches, err)
	}
	return matches[0]
}
