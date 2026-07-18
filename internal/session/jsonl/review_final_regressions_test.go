package jsonl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type deterministicDiagnosticMaterial struct {
	raw         []byte
	lineEnds    []int
	markerStart int
}

func TestRecoveryDiagnosticResumesEveryDeterministicBytePrefix(t *testing.T) {
	cut := func(name string, material deterministicDiagnosticMaterial) int {
		switch name {
		case "before transaction id":
			index := bytes.Index(material.raw[:material.lineEnds[0]], []byte(`"transaction_id"`))
			if index <= 0 {
				t.Fatalf("first line has no transaction_id boundary: %q", material.raw[:material.lineEnds[0]])
			}
			return index
		case "mid first line":
			return material.lineEnds[0] / 2
		case "between lines":
			return material.lineEnds[0]
		case "mid marker":
			return material.markerStart + (len(material.raw)-material.markerStart)/2
		default:
			t.Fatalf("unknown cut %q", name)
			return 0
		}
	}
	for _, name := range []string{"before transaction id", "mid first line", "between lines", "mid marker"} {
		t.Run(name, func(t *testing.T) {
			fixture, request, manifest, material := pausedDeterministicDiagnosticRecovery(t, "operation-short-"+strings.ReplaceAll(name, " ", "-"))
			cutAt := cut(name, material)
			if cutAt <= 0 || cutAt >= len(material.raw) {
				t.Fatalf("cut=%d transaction bytes=%d", cutAt, len(material.raw))
			}
			eventsPath := filepath.Join(fixture.sessionDir, "events.jsonl")
			file, err := os.OpenFile(eventsPath, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.Write(material.raw[:cutAt]); err != nil {
				_ = file.Close()
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}

			result, err := openFixtureStore(fixture).RecoverSession(context.Background(), request)
			if err != nil || (result.Status != "recovered" && result.Status != "already_recovered") {
				t.Fatalf("restart result=%+v err=%v", result, err)
			}
			active, err := os.ReadFile(eventsPath)
			if err != nil {
				t.Fatal(err)
			}
			prefixSize := manifest.Observation.ValidPrefixBytes
			if int64(len(active)) < prefixSize || !bytes.Equal(active[prefixSize:], material.raw) {
				t.Fatalf("durable diagnostic transaction differs from deterministic bytes\ngot:  %q\nwant: %q", active[prefixSize:], material.raw)
			}
			partials, err := filepath.Glob(filepath.Join(fixture.sessionDir, "artifacts", "recovery-diagnostic-tail-*.bin"))
			if err != nil || len(partials) != 1 {
				t.Fatalf("partial diagnostic artifacts=%v err=%v", partials, err)
			}
			preserved, err := os.ReadFile(partials[0])
			if err != nil || !bytes.Equal(preserved, material.raw[:cutAt]) {
				t.Fatalf("preserved partial=%q err=%v want=%q", preserved, err, material.raw[:cutAt])
			}
			inspection, err := openFixtureStore(fixture).InspectSession(context.Background(), fixtureSessionID)
			if err != nil || !inspection.Journal.Writable || countEventKind(inspection.Journal.Events, protocol.EventRecoveryDiagnostic) != 1 {
				t.Fatalf("inspection=%+v err=%v", inspection, err)
			}
		})
	}
}

func TestRecoveryActivationChecksIdentityAtEachFinalRenameBoundary(t *testing.T) {
	for _, target := range []struct {
		name  string
		point FaultPoint
		glob  string
	}{
		{name: "metadata", point: FaultRecoveryMetadataRenameBoundary, glob: ".recovery-metadata-*.json"},
		{name: "candidate", point: FaultRecoveryCandidateRenameBoundary, glob: ".recovery-candidate-*.jsonl"},
	} {
		t.Run(target.name, func(t *testing.T) {
			fixture := copyFixture(t, "incomplete-batch")
			setup := openFixtureStore(fixture)
			_, request := recoveryRequestForFixture(t, setup, protocol.ControlOperationID("operation-final-normal-"+target.name), "txn-final-normal")
			beforeEvents, err := os.ReadFile(filepath.Join(fixture.sessionDir, "events.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			replaced := false
			store := New(fixture.root, Options{Encoder: fixturePassthroughEncoder{}, Fault: func(point FaultPoint) error {
				if point != target.point || replaced {
					return nil
				}
				replaced = true
				return substituteOnlyGlob(filepath.Join(fixture.sessionDir, target.glob), []byte("final-boundary substitute"))
			}})
			if _, err := store.RecoverSession(context.Background(), request); err == nil {
				t.Fatal("normal activation accepted final-boundary substitution")
			}
			if !replaced {
				t.Fatal("normal final-rename boundary was not reached")
			}
			afterEvents, err := os.ReadFile(filepath.Join(fixture.sessionDir, "events.jsonl"))
			if err != nil || !bytes.Equal(afterEvents, beforeEvents) {
				t.Fatalf("normal boundary failure changed active journal: err=%v", err)
			}
		})
	}
}

func TestRecoveryDiagnosticResetChecksCandidateAndMetadataAtFinalRenameBoundary(t *testing.T) {
	for _, target := range []string{"candidate", "metadata"} {
		t.Run(target, func(t *testing.T) {
			fixture, request, _, material := pausedDeterministicDiagnosticRecovery(t, "operation-final-reset-"+target)
			eventsPath := filepath.Join(fixture.sessionDir, "events.jsonl")
			file, err := os.OpenFile(eventsPath, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				t.Fatal(err)
			}
			// A complete first event is recognized by the pre-fix reset path, so
			// this test isolates the missing final Rename boundary check.
			if _, err := file.Write(material.raw[:material.lineEnds[0]]); err != nil {
				_ = file.Close()
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			beforeEvents, err := os.ReadFile(eventsPath)
			if err != nil {
				t.Fatal(err)
			}
			replaced := false
			store := New(fixture.root, Options{Encoder: fixturePassthroughEncoder{}, Fault: func(point FaultPoint) error {
				if point != FaultRecoveryDiagnosticResetBoundary || replaced {
					return nil
				}
				replaced = true
				if target == "candidate" {
					return substituteOnlyGlob(filepath.Join(fixture.sessionDir, ".recovery-candidate-*.jsonl"), []byte("reset candidate substitute"))
				}
				metadata := filepath.Join(fixture.sessionDir, "metadata.json")
				if err := os.Rename(metadata, metadata+".opened"); err != nil {
					return err
				}
				return os.WriteFile(metadata, []byte(`{"id":"reset-metadata-substitute"}`), 0o600)
			}})
			if _, err := store.RecoverSession(context.Background(), request); err == nil {
				t.Fatal("diagnostic reset accepted final-boundary substitution")
			}
			if !replaced {
				t.Fatal("diagnostic reset final-rename boundary was not reached")
			}
			afterEvents, err := os.ReadFile(eventsPath)
			if err != nil || !bytes.Equal(afterEvents, beforeEvents) {
				t.Fatalf("reset boundary failure changed active journal: err=%v", err)
			}
		})
	}
}

func pausedDeterministicDiagnosticRecovery(
	t *testing.T,
	operationID string,
) (fixtureMaterialization, journal.RecoveryRequest, explicitRecoveryManifest, deterministicDiagnosticMaterial) {
	t.Helper()
	fixture := copyFixture(t, "incomplete-batch")
	setup := openFixtureStore(fixture)
	_, request := recoveryRequestForFixture(t, setup, protocol.ControlOperationID(operationID), protocol.TransactionID("txn-"+operationID))
	pause := errors.New("pause before diagnostic append")
	store := New(fixture.root, Options{Encoder: fixturePassthroughEncoder{}, Fault: func(point FaultPoint) error {
		if point == FaultRecoveryDiagnosticCommit {
			return pause
		}
		return nil
	}})
	if _, err := store.RecoverSession(context.Background(), request); !errors.Is(err, pause) {
		t.Fatalf("pause recovery err=%v", err)
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
	inspection, err := openFixtureStore(fixture).InspectSession(context.Background(), fixtureSessionID)
	if err != nil {
		t.Fatal(err)
	}
	material := deterministicDiagnosticBytesForTest(t, request, manifest, inspection)
	return fixture, request, manifest, material
}

func deterministicDiagnosticBytesForTest(
	t *testing.T,
	request journal.RecoveryRequest,
	manifest explicitRecoveryManifest,
	inspection journal.SessionInspection,
) deterministicDiagnosticMaterial {
	t.Helper()
	hash := recoveryOperationHash(request.OperationID)
	eventTime := inspection.Session.UpdatedAt.UTC()
	seq := request.ExpectedHead.CommitSeq + 1
	var envelopes []protocol.EventEnvelope
	legacy, committedV2 := false, false
	for _, record := range inspection.Journal.Events {
		legacy = legacy || record.Legacy != nil
		committedV2 = committedV2 || record.Legacy == nil
	}
	if legacy && !committedV2 {
		payload, err := canonicaljson.Marshal(protocol.MigrationCompatibilityDeclaredV1{
			ReaderVersion: protocol.EnvelopeVersion, WriterVersion: protocol.EnvelopeVersion,
			LegacyHead: request.ExpectedHead, DowngradeStatus: "v0.1_read_only_after_v2",
		})
		if err != nil {
			t.Fatal(err)
		}
		envelopes = append(envelopes, protocol.EventEnvelope{
			SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1,
			JournalKind: request.Journal.Kind, JournalID: request.Journal.ID,
			EventID: protocol.EventID("recovery:" + hash + ":compatibility"), SessionID: protocol.SessionID(request.Journal.ID),
			Seq: seq, Time: eventTime, Kind: protocol.EventMigrationCompatibilityDeclared,
			TransactionID: request.TransactionID, Payload: payload,
		})
		seq++
	}
	diagnosticPayload, err := canonicaljson.Marshal(protocol.DiagnosticV1{Diagnostic: recoveryCompletedDiagnostic(request, manifest)})
	if err != nil {
		t.Fatal(err)
	}
	envelopes = append(envelopes, protocol.EventEnvelope{
		SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1,
		JournalKind: request.Journal.Kind, JournalID: request.Journal.ID,
		EventID: protocol.EventID("recovery:" + hash + ":diagnostic"), SessionID: protocol.SessionID(request.Journal.ID),
		Seq: seq, Time: eventTime, Kind: protocol.EventRecoveryDiagnostic,
		TransactionID: request.TransactionID, Payload: diagnosticPayload,
	})
	digest, err := canonicaljson.TransactionDigest(envelopes)
	if err != nil {
		t.Fatal(err)
	}
	markerPayload, err := canonicaljson.Marshal(protocol.TransactionCommittedV1{
		TransactionID: request.TransactionID, FirstSeq: envelopes[0].Seq, LastSeq: envelopes[len(envelopes)-1].Seq,
		EventCount: uint32(len(envelopes)), Digest: digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	marker := protocol.EventEnvelope{
		SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1,
		JournalKind: request.Journal.Kind, JournalID: request.Journal.ID,
		EventID: protocol.EventID("recovery:" + hash + ":marker"), SessionID: protocol.SessionID(request.Journal.ID),
		Seq: seq + 1, Time: eventTime, Kind: protocol.EventTransactionCommitted,
		TransactionID: request.TransactionID, Payload: markerPayload,
	}
	material := deterministicDiagnosticMaterial{}
	for _, envelope := range envelopes {
		line, err := encodeLine(envelope)
		if err != nil {
			t.Fatal(err)
		}
		material.raw = append(material.raw, line...)
		material.lineEnds = append(material.lineEnds, len(material.raw))
	}
	material.markerStart = len(material.raw)
	line, err := encodeLine(marker)
	if err != nil {
		t.Fatal(err)
	}
	material.raw = append(material.raw, line...)
	material.lineEnds = append(material.lineEnds, len(material.raw))
	if material.markerStart <= 0 || len(material.lineEnds) < 2 {
		t.Fatal(fmt.Errorf("invalid deterministic recovery material"))
	}
	return material
}
