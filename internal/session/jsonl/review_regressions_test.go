package jsonl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestMalformedRecognizedV1PayloadStopsValidatedPrefixAndBlocksAppend(t *testing.T) {
	fixture := copyFixture(t, "invalid-known-payload")
	store := openFixtureStore(fixture)
	inspection, err := store.InspectSession(context.Background(), fixtureSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Journal.Writable || len(inspection.Journal.Events) != 1 || !hasDiagnostic(inspection.Journal.Diagnostics, "invalid_known_payload") || hasDiagnostic(inspection.Journal.Diagnostics, "recovery.available") {
		t.Fatalf("malformed v1 inspection=%+v", inspection)
	}
	before := snapshotTree(t, fixture.root)
	result, err := store.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: inspection.Journal.Journal, ExpectedHead: inspection.Journal.Head, TransactionID: "txn-after-malformed-v1",
		Events: []protocol.ProposedEvent{{
			EventID: "event-after-malformed-v1", Time: inspection.Session.UpdatedAt, PayloadVersion: 1,
			Kind: protocol.EventSessionTitleChanged, SessionID: fixtureSessionID, Payload: json.RawMessage(`{"title":"must not append"}`),
		}},
	})
	if err == nil && result.Status == journal.AppendCommitted {
		t.Fatal("append committed to a journal with malformed recognized v1 payload")
	}
	if after := snapshotTree(t, fixture.root); !reflect.DeepEqual(after, before) {
		t.Fatal("rejected append mutated malformed v1 journal")
	}
}

func TestV1ToolLifecycleRequiresRequestedThenStartedBeforeSuccess(t *testing.T) {
	for _, test := range []struct {
		name    string
		sources []protocol.LegacySource
	}{
		{
			name: "orphan start then success",
			sources: []protocol.LegacySource{
				legacySource("u-orphan", 1, domain.EventUserMessage, `{"content":"turn"}`),
				legacySource("s-orphan", 2, domain.EventToolStarted, `{"call_id":"orphan"}`),
				legacySource("r-orphan", 3, domain.EventToolResult, `{"result":{"call_id":"orphan","status":"succeeded","content":"done","duration":1,"truncated":false}}`),
			},
		},
		{
			name: "request then success without start",
			sources: []protocol.LegacySource{
				legacySource("u-no-start", 1, domain.EventUserMessage, `{"content":"turn"}`),
				legacySource("q-no-start", 2, domain.EventToolRequested, `{"request":{"call_id":"no-start","name":"read","input":{},"workspace":"/tmp"},"mutation":"read_only","canonical_scope":"x","inside_workspace":true,"summary":"read"}`),
				legacySource("r-no-start", 3, domain.EventToolResult, `{"result":{"call_id":"no-start","status":"succeeded","content":"done","duration":1,"truncated":false}}`),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := UpcastState{SessionID: fixtureSessionID}
			var records []protocol.EventRecord
			var diagnostics []protocol.Diagnostic
			for _, source := range test.sources {
				record, next, emitted := UpcastV1(source, state)
				state = next
				records = append(records, record)
				diagnostics = append(diagnostics, emitted...)
			}
			terminal := records[len(records)-1]
			if terminal.Envelope.Kind != protocol.EventActivityUncertain {
				t.Fatalf("terminal kind=%q records=%+v diagnostics=%+v", terminal.Envelope.Kind, records, diagnostics)
			}
		})
	}
}

func TestRecoveryDiagnosticCommitRetriesEveryAppendFaultWindow(t *testing.T) {
	appendFaults := []FaultPoint{
		FaultEventWrite,
		FaultEventSync,
		FaultMarkerWrite,
		FaultMarkerSync,
		FaultMetadataWrite,
		FaultMetadataRename,
		FaultDirectorySync,
	}
	for _, point := range appendFaults {
		t.Run(string(point), func(t *testing.T) {
			fixture := copyFixture(t, "incomplete-batch")
			setup := openFixtureStore(fixture)
			_, request := recoveryRequestForFixture(t, setup, protocol.ControlOperationID("operation-diagnostic-"+string(point)), protocol.TransactionID("txn-diagnostic-"+string(point)))
			faultErr := errors.New("diagnostic append fault at " + string(point))
			inDiagnosticCommit := false
			injected := false
			faulting := New(fixture.root, Options{Encoder: fixturePassthroughEncoder{}, Fault: func(got FaultPoint) error {
				if got == FaultRecoveryDiagnosticCommit {
					inDiagnosticCommit = !inDiagnosticCommit
					return nil
				}
				if inDiagnosticCommit && got == point && !injected {
					injected = true
					return faultErr
				}
				return nil
			}})
			if _, err := faulting.RecoverSession(context.Background(), request); !errors.Is(err, faultErr) {
				t.Fatalf("first recovery err=%v want %v", err, faultErr)
			}
			if !injected {
				t.Fatalf("append fault point %q was not reached", point)
			}
			restarted := openFixtureStore(fixture)
			result, err := restarted.RecoverSession(context.Background(), request)
			if err != nil || (result.Status != "recovered" && result.Status != "already_recovered") {
				t.Fatalf("restart result=%+v err=%v", result, err)
			}
			inspection, err := restarted.InspectSession(context.Background(), fixtureSessionID)
			if err != nil {
				t.Fatal(err)
			}
			if !inspection.Journal.Writable || inspection.Session.LastSeq != inspection.Journal.Head.CommitSeq || countEventKind(inspection.Journal.Events, protocol.EventRecoveryDiagnostic) != 1 {
				t.Fatalf("restart inspection=%+v", inspection)
			}
		})
	}
}

func TestPersistedRecoveryManifestMustExactlyMatchDerivedObservationAndNames(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*explicitRecoveryManifest)
	}{
		{name: "candidate reserved events leaf", mutate: func(manifest *explicitRecoveryManifest) { manifest.CandidateName = "events.jsonl" }},
		{name: "metadata reserved metadata leaf", mutate: func(manifest *explicitRecoveryManifest) { manifest.MetadataName = "metadata.json" }},
		{name: "quarantine forged leaf", mutate: func(manifest *explicitRecoveryManifest) { manifest.QuarantineName = "events.jsonl" }},
		{name: "request temporary reserved leaf", mutate: func(manifest *explicitRecoveryManifest) { manifest.RequestTemporary = "metadata.json" }},
		{name: "prefix digest", mutate: func(manifest *explicitRecoveryManifest) { manifest.PrefixDigest.Value = strings.Repeat("0", 64) }},
		{name: "source digest", mutate: func(manifest *explicitRecoveryManifest) { manifest.SourceDigest.Value = strings.Repeat("0", 64) }},
		{name: "valid prefix size", mutate: func(manifest *explicitRecoveryManifest) { manifest.Observation.ValidPrefixBytes++ }},
		{name: "source size", mutate: func(manifest *explicitRecoveryManifest) { manifest.Observation.SourceBytes++ }},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			fixture := copyFixture(t, "incomplete-batch")
			setup := openFixtureStore(fixture)
			_, request := recoveryRequestForFixture(t, setup, protocol.ControlOperationID("operation-manifest-"+strings.ReplaceAll(test.name, " ", "-")), "txn-manifest")
			pause := errors.New("pause after manifest")
			faulting := New(fixture.root, Options{Encoder: fixturePassthroughEncoder{}, Fault: func(point FaultPoint) error {
				if point == FaultQuarantineWrite {
					return pause
				}
				return nil
			}})
			if _, err := faulting.RecoverSession(context.Background(), request); !errors.Is(err, pause) {
				t.Fatalf("initial recovery err=%v", err)
			}
			manifestPath := onlyGlob(t, filepath.Join(fixture.sessionDir, ".recovery-request-*.json"))
			raw, err := os.ReadFile(manifestPath)
			if err != nil {
				t.Fatal(err)
			}
			var manifest explicitRecoveryManifest
			if err := json.Unmarshal(raw, &manifest); err != nil {
				t.Fatal(err)
			}
			test.mutate(&manifest)
			forged, err := canonicaljson.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(manifestPath, forged, 0o600); err != nil {
				t.Fatal(err)
			}
			before := snapshotTree(t, fixture.root)
			result, recoverErr := openFixtureStore(fixture).RecoverSession(context.Background(), request)
			if recoverErr == nil && result.Status != "conflict" {
				t.Fatalf("forged manifest accepted: result=%+v", result)
			}
			if after := snapshotTree(t, fixture.root); !reflect.DeepEqual(after, before) {
				t.Fatal("rejected forged manifest mutated storage")
			}
		})
	}
}

func TestRecoveryRevalidatesAllLeavesAndArtifactsDirectoryAtActivation(t *testing.T) {
	for _, target := range []string{"journal candidate", "metadata candidate", "quarantine", "artifacts directory"} {
		t.Run(target, func(t *testing.T) {
			fixture := copyFixture(t, "incomplete-batch")
			setup := openFixtureStore(fixture)
			_, request := recoveryRequestForFixture(t, setup, protocol.ControlOperationID("operation-activate-"+strings.ReplaceAll(target, " ", "-")), "txn-activate")
			beforeEvents, err := os.ReadFile(filepath.Join(fixture.sessionDir, "events.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			replaced := false
			store := New(fixture.root, Options{Encoder: fixturePassthroughEncoder{}, Fault: func(point FaultPoint) error {
				if point != FaultCandidateActivate || replaced {
					return nil
				}
				replaced = true
				switch target {
				case "journal candidate":
					return substituteOnlyGlob(filepath.Join(fixture.sessionDir, ".recovery-candidate-*.jsonl"), []byte("substituted journal candidate"))
				case "metadata candidate":
					return substituteOnlyGlob(filepath.Join(fixture.sessionDir, ".recovery-metadata-*.json"), []byte(`{"id":"substituted"}`))
				case "quarantine":
					return substituteOnlyGlob(filepath.Join(fixture.sessionDir, "artifacts", "recovery-tail-*.bin"), []byte("substituted quarantine"))
				case "artifacts directory":
					artifacts := filepath.Join(fixture.sessionDir, "artifacts")
					if err := os.Rename(artifacts, artifacts+".opened"); err != nil {
						return err
					}
					return os.Mkdir(artifacts, 0o700)
				default:
					return fmt.Errorf("unknown target %q", target)
				}
			}})
			if _, err := store.RecoverSession(context.Background(), request); err == nil {
				t.Fatal("recovery accepted activation-time substitution")
			}
			if !replaced {
				t.Fatal("activation substitution boundary was not reached")
			}
			afterEvents, err := os.ReadFile(filepath.Join(fixture.sessionDir, "events.jsonl"))
			if err != nil || !bytes.Equal(afterEvents, beforeEvents) {
				t.Fatalf("activation failure changed active journal: err=%v", err)
			}
		})
	}
}

func TestUnsupportedJournalDataIsNeverAdvertisedOrAcceptedForRecovery(t *testing.T) {
	for _, fixtureName := range []string{"unsupported-envelope-version", "unsupported-payload-version", "unknown-future-kind"} {
		t.Run(fixtureName, func(t *testing.T) {
			fixture := copyFixture(t, fixtureName)
			eventsPath := filepath.Join(fixture.sessionDir, "events.jsonl")
			if fixtureName != "unsupported-envelope-version" {
				raw, err := os.ReadFile(eventsPath)
				if err != nil {
					t.Fatal(err)
				}
				last := bytes.LastIndex(raw[:len(raw)-1], []byte("\n"))
				if last < 0 || os.WriteFile(eventsPath, raw[:last+1], 0o600) != nil {
					t.Fatal("could not remove marker from unsupported fixture")
				}
			}
			store := openFixtureStore(fixture)
			inspection, err := store.InspectSession(context.Background(), fixtureSessionID)
			if err != nil {
				t.Fatal(err)
			}
			if hasDiagnostic(inspection.Journal.Diagnostics, "recovery.available") {
				t.Fatalf("unsupported data advertised recovery: %+v", inspection.Journal.Diagnostics)
			}
			raw, err := os.ReadFile(eventsPath)
			if err != nil {
				t.Fatal(err)
			}
			prefixEnd := bytes.IndexByte(raw, '\n') + 1
			if prefixEnd <= 0 || prefixEnd >= len(raw) {
				t.Fatalf("invalid unsupported fixture bytes=%d prefix=%d", len(raw), prefixEnd)
			}
			request := journal.RecoveryRequest{
				OperationID: protocol.ControlOperationID("operation-unsupported-" + fixtureName),
				Journal:     inspection.Journal.Journal, ExpectedHead: inspection.Journal.Head,
				ObservedTailDigest: digestBytes(raw[prefixEnd:]), TransactionID: protocol.TransactionID("txn-unsupported-recovery-" + fixtureName),
			}
			before := snapshotTree(t, fixture.root)
			result, recoverErr := store.RecoverSession(context.Background(), request)
			if recoverErr == nil && result.Status != "conflict" {
				t.Fatalf("unsupported recovery accepted: result=%+v", result)
			}
			if after := snapshotTree(t, fixture.root); !reflect.DeepEqual(after, before) {
				t.Fatal("rejected unsupported recovery mutated storage")
			}
		})
	}
}

func TestMixedJournalRequiresExactFirstCompatibilityTransition(t *testing.T) {
	tests := []struct {
		name  string
		build func(*testing.T, fixtureMaterialization, protocol.CommittedCursor) int
	}{
		{name: "missing", build: func(t *testing.T, fixture fixtureMaterialization, legacyHead protocol.CommittedCursor) int {
			appendForgedTransition(t, fixture, legacyHead.CommitSeq, "txn-missing", []forgedTransitionEvent{titleTransition("missing")})
			return 13
		}},
		{name: "later", build: func(t *testing.T, fixture fixtureMaterialization, legacyHead protocol.CommittedCursor) int {
			appendForgedTransition(t, fixture, legacyHead.CommitSeq, "txn-later", []forgedTransitionEvent{titleTransition("later"), compatibilityTransition(t, legacyHead)})
			return 13
		}},
		{name: "duplicate", build: func(t *testing.T, fixture fixtureMaterialization, legacyHead protocol.CommittedCursor) int {
			appendForgedTransition(t, fixture, legacyHead.CommitSeq, "txn-duplicate", []forgedTransitionEvent{compatibilityTransition(t, legacyHead), compatibilityTransition(t, legacyHead)})
			return 13
		}},
		{name: "wrong reader", build: wrongCompatibilityBuilder(func(payload *protocol.MigrationCompatibilityDeclaredV1) { payload.ReaderVersion = 3 })},
		{name: "wrong writer", build: wrongCompatibilityBuilder(func(payload *protocol.MigrationCompatibilityDeclaredV1) { payload.WriterVersion = 3 })},
		{name: "wrong legacy head", build: wrongCompatibilityBuilder(func(payload *protocol.MigrationCompatibilityDeclaredV1) { payload.LegacyHead.CommitSeq-- })},
		{name: "wrong downgrade", build: wrongCompatibilityBuilder(func(payload *protocol.MigrationCompatibilityDeclaredV1) { payload.DowngradeStatus = "writable" })},
		{name: "repeated", build: func(t *testing.T, fixture fixtureMaterialization, legacyHead protocol.CommittedCursor) int {
			appendForgedTransition(t, fixture, legacyHead.CommitSeq, "txn-first-valid", []forgedTransitionEvent{compatibilityTransition(t, legacyHead), titleTransition("valid")})
			appendForgedTransition(t, fixture, legacyHead.CommitSeq+3, "txn-repeated", []forgedTransitionEvent{compatibilityTransition(t, legacyHead)})
			return 15
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := copyFixture(t, "v1-valid")
			before := inspectFixtureMaterialization(t, fixture)
			wantCommittedEvents := test.build(t, fixture, before.Journal.Head)
			inspection := inspectFixtureMaterialization(t, fixture)
			if inspection.Journal.Writable || !hasDiagnostic(inspection.Journal.Diagnostics, "invalid_transition") || len(inspection.Journal.Events) != wantCommittedEvents {
				t.Fatalf("forged transition inspection writable=%v events=%d want=%d diagnostics=%+v", inspection.Journal.Writable, len(inspection.Journal.Events), wantCommittedEvents, inspection.Journal.Diagnostics)
			}
		})
	}
}

type forgedTransitionEvent struct {
	kind    string
	payload json.RawMessage
}

func titleTransition(title string) forgedTransitionEvent {
	payload, _ := canonicaljson.Marshal(protocol.SessionTitleChangedV1{Title: title})
	return forgedTransitionEvent{kind: protocol.EventSessionTitleChanged, payload: payload}
}

func compatibilityTransition(t *testing.T, legacyHead protocol.CommittedCursor) forgedTransitionEvent {
	t.Helper()
	payload, err := canonicaljson.Marshal(protocol.MigrationCompatibilityDeclaredV1{
		ReaderVersion: protocol.EnvelopeVersion, WriterVersion: protocol.EnvelopeVersion,
		LegacyHead: legacyHead, DowngradeStatus: "v0.1_read_only_after_v2",
	})
	if err != nil {
		t.Fatal(err)
	}
	return forgedTransitionEvent{kind: protocol.EventMigrationCompatibilityDeclared, payload: payload}
}

func wrongCompatibilityBuilder(mutate func(*protocol.MigrationCompatibilityDeclaredV1)) func(*testing.T, fixtureMaterialization, protocol.CommittedCursor) int {
	return func(t *testing.T, fixture fixtureMaterialization, legacyHead protocol.CommittedCursor) int {
		payload := protocol.MigrationCompatibilityDeclaredV1{
			ReaderVersion: protocol.EnvelopeVersion, WriterVersion: protocol.EnvelopeVersion,
			LegacyHead: legacyHead, DowngradeStatus: "v0.1_read_only_after_v2",
		}
		mutate(&payload)
		raw, err := canonicaljson.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		appendForgedTransition(t, fixture, legacyHead.CommitSeq, "txn-wrong", []forgedTransitionEvent{{kind: protocol.EventMigrationCompatibilityDeclared, payload: raw}})
		return 13
	}
}

func appendForgedTransition(t *testing.T, fixture fixtureMaterialization, startSeq uint64, transactionID protocol.TransactionID, proposed []forgedTransitionEvent) int {
	t.Helper()
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(fixtureSessionID)}
	envelopes := make([]protocol.EventEnvelope, 0, len(proposed))
	for index, event := range proposed {
		seq := startSeq + uint64(index) + 1
		envelopes = append(envelopes, protocol.EventEnvelope{
			SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1,
			JournalKind: ref.Kind, JournalID: ref.ID, EventID: protocol.EventID(fmt.Sprintf("forged-%s-%d", transactionID, seq)), SessionID: fixtureSessionID,
			Seq: seq, Time: time.Date(2026, 7, 18, 12, 0, int(seq%60), 0, time.UTC), Kind: event.kind,
			TransactionID: transactionID, Payload: event.payload,
		})
	}
	digest, err := canonicaljson.TransactionDigest(envelopes)
	if err != nil {
		t.Fatal(err)
	}
	markerSeq := startSeq + uint64(len(envelopes)) + 1
	markerPayload, err := canonicaljson.Marshal(protocol.TransactionCommittedV1{
		TransactionID: transactionID, FirstSeq: envelopes[0].Seq, LastSeq: envelopes[len(envelopes)-1].Seq,
		EventCount: uint32(len(envelopes)), Digest: digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	marker := protocol.EventEnvelope{
		SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1,
		JournalKind: ref.Kind, JournalID: ref.ID, EventID: protocol.EventID(fmt.Sprintf("forged-%s-marker", transactionID)), SessionID: fixtureSessionID,
		Seq: markerSeq, Time: time.Date(2026, 7, 18, 12, 1, int(markerSeq%60), 0, time.UTC), Kind: protocol.EventTransactionCommitted,
		TransactionID: transactionID, Payload: markerPayload,
	}
	var raw bytes.Buffer
	for _, envelope := range envelopes {
		line, err := encodeLine(envelope)
		if err != nil {
			t.Fatal(err)
		}
		raw.Write(line)
	}
	line, err := encodeLine(marker)
	if err != nil {
		t.Fatal(err)
	}
	raw.Write(line)
	path := filepath.Join(fixture.sessionDir, "events.jsonl")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(raw.Bytes()); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return int(startSeq + uint64(len(envelopes)))
}

func inspectFixtureMaterialization(t *testing.T, fixture fixtureMaterialization) journal.SessionInspection {
	t.Helper()
	inspection, err := openFixtureStore(fixture).InspectSession(context.Background(), fixtureSessionID)
	if err != nil {
		t.Fatal(err)
	}
	return inspection
}

func onlyGlob(t *testing.T, pattern string) string {
	t.Helper()
	matches, err := filepath.Glob(pattern)
	if err != nil || len(matches) != 1 {
		t.Fatalf("glob %q matches=%v err=%v", pattern, matches, err)
	}
	return matches[0]
}

func substituteOnlyGlob(pattern string, contents []byte) error {
	matches, err := filepath.Glob(pattern)
	if err != nil || len(matches) != 1 {
		return fmt.Errorf("glob %q matches=%v err=%v", pattern, matches, err)
	}
	if err := os.Rename(matches[0], matches[0]+".opened"); err != nil {
		return err
	}
	return os.WriteFile(matches[0], contents, 0o600)
}
