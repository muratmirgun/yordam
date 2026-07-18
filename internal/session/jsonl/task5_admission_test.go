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

func TestAppendBatchRejectsSecretInFullEnvelopeMetadataBeforeWrite(t *testing.T) {
	registry := secret.NewRegistry()
	registration, err := registry.Acquire("generation-envelope", [][]byte{[]byte("identity-secret")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registration.Close() }()
	for name, mutate := range map[string]func(*protocol.ProposedEvent){
		"event_id": func(event *protocol.ProposedEvent) { event.EventID = "aWRlbnRpdHktc2VjcmV0" },
		"kind":     func(event *protocol.ProposedEvent) { event.Kind = "aWRlbnRpdHktc2VjcmV0" },
		"actor_id": func(event *protocol.ProposedEvent) {
			event.Actor = &protocol.ActorRef{ID: "aWRlbnRpdHktc2VjcmV0", Kind: protocol.ActorSystem, Source: "test"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newV2JournalWithOptions(t, jsonl.Options{Encoder: passthroughEncoder{}, Secrets: registry})
			before, err := os.ReadFile(fixture.eventsPath)
			if err != nil {
				t.Fatal(err)
			}
			event := proposed("event-envelope", protocol.EventTaskCreated)
			event.SessionID = protocol.SessionID(fixture.ref.ID)
			event.RuntimeGenerationID = "generation-envelope"
			mutate(&event)
			if _, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
				Journal: fixture.ref, ExpectedHead: fixture.head, TransactionID: "txn-envelope", Events: []protocol.ProposedEvent{event},
			}); !errors.Is(err, secret.ErrSecretDetected) {
				t.Fatalf("append metadata error=%v", err)
			}
			after, err := os.ReadFile(fixture.eventsPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(after, before) {
				t.Fatal("rejected envelope metadata changed journal")
			}
		})
	}
}

func TestRecoveryPinsGenerationAcrossFaultAndRestartAdmission(t *testing.T) {
	root := t.TempDir()
	setup, workspace, session := createTestSession(t, root)
	_ = setup
	eventsPath := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "events.jsonl")
	appendFile(t, eventsPath, []byte(`{"incomplete":`))
	registry := secret.NewRegistry()
	lease, err := registry.Acquire("generation-recovery", [][]byte{[]byte("recovery-secret")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Close() }()
	store := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}, Secrets: registry})
	request := recoveryRequestFromInspection(t, store, session.ID, "operation-admitted-recovery", "txn-admitted-recovery")
	request.RuntimeGenerationID = "generation-recovery"
	pause := errors.New("pause admitted recovery")
	paused := false
	faulting := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}, Secrets: registry, Fault: func(point jsonl.FaultPoint) error {
		if point == jsonl.FaultRecoveryDiagnosticCommit && !paused {
			paused = true
			return pause
		}
		return nil
	}})
	if _, err := faulting.RecoverSession(context.Background(), request); !errors.Is(err, pause) {
		t.Fatalf("faulted recovery error=%v", err)
	}
	restarted := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}, Secrets: registry})
	result, err := restarted.RecoverSession(context.Background(), request)
	if err != nil || (result.Status != "recovered" && result.Status != "already_recovered") {
		t.Fatalf("restart recovery result=%+v err=%v", result, err)
	}
	raw, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"runtime_generation_id":"generation-recovery"`)) {
		t.Fatalf("recovery did not retain pinned generation: %s", raw)
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

func TestNewAdmittedRejectsMissingProductionAdmission(t *testing.T) {
	registry := secret.NewRegistry()
	if _, err := jsonl.NewAdmitted(t.TempDir(), jsonl.Options{Secrets: registry}); err == nil {
		t.Fatal("production store accepted missing generation binding")
	}
	lease, err := registry.Acquire("production-generation", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Close() }()
	binding := secret.NewBinding(lease)
	if _, err := jsonl.NewAdmitted(t.TempDir(), jsonl.Options{Secrets: registry, Admission: binding}); err != nil {
		t.Fatalf("admitted production store: %v", err)
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
