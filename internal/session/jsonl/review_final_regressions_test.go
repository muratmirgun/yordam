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

type deterministicDiagnosticMaterial struct {
	raw         []byte
	lineEnds    []int
	markerStart int
}

type recoveryDiagnosticAppendTestView struct {
	DiagnosticPayload []byte `json:"diagnostic_payload"`
	TransactionBytes  []byte `json:"transaction_bytes"`
}

type recoveryAdmissionEncoder struct {
	calls     []string
	reject    error
	transform bool
	message   string
}

func (e *recoveryAdmissionEncoder) EncodeProposed(event protocol.ProposedEvent) (json.RawMessage, error) {
	e.calls = append(e.calls, event.Kind)
	if e.reject != nil {
		return nil, e.reject
	}
	if !e.transform || event.Kind != protocol.EventRecoveryDiagnostic {
		return protocol.CloneRawMessage(event.Payload), nil
	}
	var payload protocol.DiagnosticV1
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return nil, err
	}
	payload.Diagnostic.Message = e.message
	if payload.Diagnostic.Message == "" {
		payload.Diagnostic.Message = "admitted redacted recovery diagnostic"
	}
	return canonicaljson.Marshal(payload)
}

func TestRecoveryAdmissionUsesValidatedHeadTimeAcrossStaleMetadataRetry(t *testing.T) {
	fixture := copyFixture(t, "incomplete-batch")
	metadataPath := filepath.Join(fixture.sessionDir, "metadata.json")
	metadataRaw, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	var stale domain.Session
	if err := json.Unmarshal(metadataRaw, &stale); err != nil {
		t.Fatal(err)
	}
	validatedHeadTime := stale.CreatedAt.UTC()
	stale.UpdatedAt = stale.CreatedAt.Add(6 * time.Hour)
	staleRaw, err := json.MarshalIndent(stale, "", "  ")
	if err != nil || os.WriteFile(metadataPath, staleRaw, 0o600) != nil {
		t.Fatalf("write stale metadata err=%v", err)
	}
	setup := openFixtureStore(fixture)
	_, request := recoveryRequestForFixture(t, setup, "operation-stale-head-time", "txn-stale-head-time")
	pause := errors.New("pause after stale-metadata recovery activation")
	paused := false
	store := New(fixture.root, Options{Encoder: fixturePassthroughEncoder{}, Fault: func(point FaultPoint) error {
		if point == FaultRecoveryDiagnosticCommit && !paused {
			paused = true
			return pause
		}
		return nil
	}})
	if _, err := store.RecoverSession(context.Background(), request); !errors.Is(err, pause) {
		t.Fatalf("initial recovery err=%v", err)
	}
	manifest := readOnlyRecoveryManifest(t, fixture)
	if !manifest.DiagnosticAppend.EventTime.Equal(validatedHeadTime) {
		t.Fatalf("admission time=%s want validated head time %s", manifest.DiagnosticAppend.EventTime, validatedHeadTime)
	}
	result, err := store.RecoverSession(context.Background(), request)
	if err != nil || result.Status != "recovered" {
		t.Fatalf("stale-metadata retry result=%+v err=%v", result, err)
	}
}

func TestCommittedRecoveryClearsOnlyExactFullMarkerUncertainty(t *testing.T) {
	for _, faultPoint := range []FaultPoint{FaultMarkerWrite, FaultMarkerSync} {
		t.Run(string(faultPoint), func(t *testing.T) {
			fixture := copyFixture(t, "incomplete-batch")
			setup := openFixtureStore(fixture)
			_, request := recoveryRequestForFixture(t, setup, protocol.ControlOperationID("operation-full-marker-"+string(faultPoint)), protocol.TransactionID("txn-full-marker-"+string(faultPoint)))
			markerFault := errors.New("full marker fault")
			failed := false
			store := New(fixture.root, Options{Encoder: fixturePassthroughEncoder{}, Fault: func(point FaultPoint) error {
				if point == faultPoint && !failed {
					failed = true
					return markerFault
				}
				return nil
			}})
			if _, err := store.RecoverSession(context.Background(), request); !errors.Is(err, markerFault) {
				t.Fatalf("initial recovery err=%v", err)
			}
			exactKey := markerUncertaintyKey(request.Journal, request.TransactionID)
			if _, exists := store.state.markerUncertainty.Load(exactKey); !exists {
				t.Fatal("full marker fault did not retain exact uncertainty")
			}
			eventsInfo, err := os.Stat(filepath.Join(fixture.sessionDir, "events.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			otherRef := protocol.JournalRef{Kind: protocol.JournalSession, ID: "01ARZ3NDEKTSV4RRFFQ69G5FB1"}
			otherKey := markerUncertaintyKey(otherRef, "txn-other-full-marker")
			store.state.markerUncertainty.Store(otherKey, eventsInfo)

			recovered, err := store.RecoverSession(context.Background(), request)
			if err != nil || recovered.Status != "already_recovered" {
				t.Fatalf("committed retry result=%+v err=%v", recovered, err)
			}
			if _, exists := store.state.markerUncertainty.Load(exactKey); exists {
				t.Fatal("already-recovered path retained exact uncertainty")
			}
			if _, exists := store.state.markerUncertainty.Load(otherKey); !exists {
				t.Fatal("already-recovered path cleared unrelated uncertainty")
			}
			inspection, err := store.InspectSession(context.Background(), fixtureSessionID)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := canonicaljson.Marshal(protocol.TaskCreatedV1{
				Goal: "append after resolved marker", OutcomeContractID: "contract-after-marker", ContractVersion: 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			appendResult, err := store.AppendBatch(context.Background(), journal.AppendRequest{
				Journal: request.Journal, ExpectedHead: recovered.Cursor, TransactionID: "txn-after-full-marker",
				Events: []protocol.ProposedEvent{{
					EventID: "event-after-full-marker", Time: inspection.Session.UpdatedAt.Add(time.Second), PayloadVersion: 1,
					Kind: protocol.EventTaskCreated, SessionID: fixtureSessionID, TaskID: "task-after-full-marker", Payload: payload,
				}},
			})
			if err != nil || appendResult.Status != journal.AppendCommitted {
				t.Fatalf("append after already-recovered result=%+v err=%v", appendResult, err)
			}
		})
	}
}

func TestLargeAdmittedRecoveryManifestLoadsAfterPersistedFailure(t *testing.T) {
	fixture := copyFixture(t, "incomplete-batch")
	setup := openFixtureStore(fixture)
	_, request := recoveryRequestForFixture(t, setup, "operation-large-admission", "txn-large-admission")
	pause := errors.New("pause after large recovery manifest persistence")
	paused := false
	largeMessage := strings.Repeat("m", 128<<10)
	encoder := &recoveryAdmissionEncoder{transform: true, message: largeMessage}
	store := New(fixture.root, Options{Encoder: encoder, Fault: func(point FaultPoint) error {
		if point == FaultRecoveryDiagnosticCommit && !paused {
			paused = true
			return pause
		}
		return nil
	}})
	if _, err := store.RecoverSession(context.Background(), request); !errors.Is(err, pause) {
		t.Fatalf("initial recovery err=%v", err)
	}
	manifestPath := onlyGlob(t, filepath.Join(fixture.sessionDir, ".recovery-request-*.json"))
	manifestInfo, err := os.Stat(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if manifestInfo.Size() <= maxSessionMetadataBytes {
		t.Fatalf("manifest size=%d does not exercise the dedicated loader bound", manifestInfo.Size())
	}
	retryEncoder := &recoveryAdmissionEncoder{reject: errors.New("large manifest retry must not re-admit")}
	retry := New(fixture.root, Options{Encoder: retryEncoder})
	result, err := retry.RecoverSession(context.Background(), request)
	if err != nil || result.Status != "recovered" {
		t.Fatalf("large manifest retry result=%+v err=%v", result, err)
	}
	if len(retryEncoder.calls) != 0 || result.Diagnostic.Message != largeMessage {
		t.Fatalf("large manifest retry calls=%v diagnostic message bytes=%d", retryEncoder.calls, len(result.Diagnostic.Message))
	}
}

func TestRecoveryAdmissionRejectsBeforeAnyPersistence(t *testing.T) {
	fixture := copyFixture(t, "incomplete-batch")
	setup := openFixtureStore(fixture)
	_, request := recoveryRequestForFixture(t, setup, "operation-admission-reject", "txn-admission-reject")
	before := snapshotTree(t, fixture.root)
	rejected := errors.New("reject recovery diagnostic admission")
	encoder := &recoveryAdmissionEncoder{reject: rejected}
	store := New(fixture.root, Options{Encoder: encoder})
	if _, err := store.RecoverSession(context.Background(), request); !errors.Is(err, rejected) {
		t.Fatalf("recovery admission err=%v", err)
	}
	if len(encoder.calls) != 1 || encoder.calls[0] != protocol.EventMigrationCompatibilityDeclared {
		t.Fatalf("encoder calls=%v, want one rejected compatibility admission", encoder.calls)
	}
	if after := snapshotTree(t, fixture.root); !reflect.DeepEqual(after, before) {
		t.Fatal("rejected recovery admission wrote storage")
	}
}

func TestRecoveryPersistsAdmittedPayloadAndReusesItsExactBytes(t *testing.T) {
	fixture := copyFixture(t, "incomplete-batch")
	setup := openFixtureStore(fixture)
	_, request := recoveryRequestForFixture(t, setup, "operation-admission-transform", "txn-admission-transform")
	pause := errors.New("pause after admitted recovery material is persisted")
	paused := false
	encoder := &recoveryAdmissionEncoder{transform: true}
	store := New(fixture.root, Options{Encoder: encoder, Fault: func(point FaultPoint) error {
		if point == FaultRecoveryDiagnosticCommit && !paused {
			paused = true
			return pause
		}
		return nil
	}})
	if _, err := store.RecoverSession(context.Background(), request); !errors.Is(err, pause) {
		t.Fatalf("initial recovery err=%v", err)
	}
	if fmt.Sprint(encoder.calls) != fmt.Sprint([]string{
		protocol.EventMigrationCompatibilityDeclared, protocol.EventRecoveryDiagnostic,
	}) {
		t.Fatalf("encoder calls=%v", encoder.calls)
	}
	manifest := readOnlyRecoveryManifest(t, fixture)
	appendMaterial := recoveryDiagnosticAppendView(t, manifest)
	admitted := []byte("admitted redacted recovery diagnostic")
	original := []byte("journal recovery preserved the observed tail and activated the validated prefix")
	if !bytes.Contains(appendMaterial.DiagnosticPayload, admitted) ||
		bytes.Contains(appendMaterial.DiagnosticPayload, original) {
		t.Fatalf("manifest diagnostic payload=%s", appendMaterial.DiagnosticPayload)
	}
	if !bytes.Contains(appendMaterial.TransactionBytes, admitted) ||
		bytes.Contains(appendMaterial.TransactionBytes, original) {
		t.Fatalf("manifest transaction bytes=%s", appendMaterial.TransactionBytes)
	}
	eventsPath := filepath.Join(fixture.sessionDir, "events.jsonl")
	cut := len(appendMaterial.TransactionBytes) / 3
	file, err := os.OpenFile(eventsPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(appendMaterial.TransactionBytes[:cut]); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	retryEncoder := &recoveryAdmissionEncoder{reject: errors.New("re-admission is forbidden")}
	retry := New(fixture.root, Options{Encoder: retryEncoder})
	result, err := retry.RecoverSession(context.Background(), request)
	if err != nil || result.Status != "recovered" {
		t.Fatalf("retry result=%+v err=%v", result, err)
	}
	if len(retryEncoder.calls) != 0 {
		t.Fatalf("retry re-admitted recovery events: %v", retryEncoder.calls)
	}
	active, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if prefix := manifest.Observation.ValidPrefixBytes; !bytes.Equal(active[prefix:], appendMaterial.TransactionBytes) {
		t.Fatalf("retried bytes differ from admitted manifest bytes\ngot:  %q\nwant: %q", active[prefix:], appendMaterial.TransactionBytes)
	}
}

func TestRecoveryResetClearsOnlyExactMarkerUncertaintyForSameStoreRetry(t *testing.T) {
	fixture := copyFixture(t, "incomplete-batch")
	setup := openFixtureStore(fixture)
	_, request := recoveryRequestForFixture(t, setup, "operation-exact-uncertainty", "txn-exact-uncertainty")
	pause := errors.New("pause before diagnostic append")
	paused := false
	store := New(fixture.root, Options{Encoder: fixturePassthroughEncoder{}, Fault: func(point FaultPoint) error {
		if point == FaultRecoveryDiagnosticCommit && !paused {
			paused = true
			return pause
		}
		return nil
	}})
	if _, err := store.RecoverSession(context.Background(), request); !errors.Is(err, pause) {
		t.Fatalf("initial recovery err=%v", err)
	}
	manifest := readOnlyRecoveryManifest(t, fixture)
	raw := recoveryDiagnosticAppendView(t, manifest).TransactionBytes
	markerStart := bytes.LastIndex(raw[:len(raw)-1], []byte{'\n'}) + 1
	cut := markerStart + (len(raw)-markerStart)/2
	eventsPath := filepath.Join(fixture.sessionDir, "events.jsonl")
	file, err := os.OpenFile(eventsPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(raw[:cut]); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	eventsInfo, err := os.Stat(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	exactKey := markerUncertaintyKey(request.Journal, request.TransactionID)
	otherRef := protocol.JournalRef{Kind: protocol.JournalSession, ID: "01ARZ3NDEKTSV4RRFFQ69G5FB0"}
	otherKey := markerUncertaintyKey(otherRef, "txn-unrelated-uncertainty")
	store.state.markerUncertainty.Store(exactKey, eventsInfo)
	store.state.markerUncertainty.Store(otherKey, eventsInfo)

	result, err := store.RecoverSession(context.Background(), request)
	if err != nil || result.Status != "recovered" {
		t.Fatalf("same-store retry result=%+v err=%v", result, err)
	}
	if _, exists := store.state.markerUncertainty.Load(exactKey); exists {
		t.Fatal("successful retry retained exact marker uncertainty")
	}
	if _, exists := store.state.markerUncertainty.Load(otherKey); !exists {
		t.Fatal("recovery broadly cleared unrelated marker uncertainty")
	}
}

func readOnlyRecoveryManifest(t *testing.T, fixture fixtureMaterialization) explicitRecoveryManifest {
	t.Helper()
	manifestPath := onlyGlob(t, filepath.Join(fixture.sessionDir, ".recovery-request-*.json"))
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest explicitRecoveryManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func recoveryDiagnosticAppendView(t *testing.T, manifest explicitRecoveryManifest) recoveryDiagnosticAppendTestView {
	t.Helper()
	raw, err := canonicaljson.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var persisted struct {
		DiagnosticAppend recoveryDiagnosticAppendTestView `json:"diagnostic_append"`
	}
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	if len(persisted.DiagnosticAppend.DiagnosticPayload) == 0 || len(persisted.DiagnosticAppend.TransactionBytes) == 0 {
		t.Fatal("recovery manifest has no admitted diagnostic append material")
	}
	return persisted.DiagnosticAppend
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
