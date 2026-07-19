package jsonl

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
)

var explicitRecoveryFaultPoints = []FaultPoint{
	FaultQuarantineWrite,
	FaultQuarantineSync,
	FaultCandidateWrite,
	FaultCandidateSync,
	FaultRecoveryMetadataWrite,
	FaultRecoveryMetadataSync,
	FaultCandidateValidate,
	FaultCandidateActivate,
	FaultRecoveryDirectorySync,
	FaultRecoveryDiagnosticCommit,
}

type recoveryInspectionDetails struct {
	ObservedTailDigest protocol.Digest `json:"observed_tail_digest"`
	ValidPrefixBytes   int64           `json:"valid_prefix_bytes"`
	SourceBytes        int64           `json:"source_bytes"`
}

func TestExplicitRecoveryPreservesTailAndCommitsDiagnostic(t *testing.T) {
	fixture := copyFixture(t, "v1-truncated-final")
	store := openFixtureStore(fixture)
	beforeSource := snapshotTree(t, fixture.sourceRoot)
	inspection, request := recoveryRequestForFixture(t, store, "operation-recover-tail", "txn-recover-tail")
	tail := readObservedTail(t, fixture, inspection)

	result, err := store.RecoverSession(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "recovered" || result.QuarantineDigest != request.ObservedTailDigest || result.Diagnostic.Code != "recovery.completed" {
		t.Fatalf("result=%+v", result)
	}
	artifacts, err := filepath.Glob(filepath.Join(fixture.sessionDir, "artifacts", "recovery-tail-*.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 1 {
		t.Fatalf("recovery artifacts=%v", artifacts)
	}
	quarantined, err := os.ReadFile(artifacts[0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(quarantined, tail) {
		t.Fatalf("quarantine=%q want exact tail %q", quarantined, tail)
	}
	after, err := store.InspectSession(context.Background(), fixtureSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if !after.Journal.Writable || !containsEventKind(after.Journal.Events, protocol.EventRecoveryDiagnostic) || !containsEventKind(after.Journal.Events, protocol.EventMigrationCompatibilityDeclared) {
		t.Fatalf("recovered inspection=%+v", after)
	}
	if got := snapshotTree(t, fixture.sourceRoot); !reflect.DeepEqual(got, beforeSource) {
		t.Fatal("explicit recovery changed immutable source fixtures")
	}
}

func TestExplicitRecoveryIsRestartIdempotentAcrossEveryFaultProbe(t *testing.T) {
	for _, point := range explicitRecoveryFaultPoints {
		for _, occurrence := range []int{1, 2} {
			t.Run(string(point)+"/occurrence-"+string(rune('0'+occurrence)), func(t *testing.T) {
				fixture := copyFixture(t, "incomplete-batch")
				setup := openFixtureStore(fixture)
				_, request := recoveryRequestForFixture(t, setup, protocol.ControlOperationID("operation-"+string(point)+"-"+string(rune('0'+occurrence))), protocol.TransactionID("txn-"+string(point)+"-"+string(rune('0'+occurrence))))
				faultErr := errors.New("injected " + string(point))
				seen := 0
				faulting := New(fixture.root, Options{
					Encoder: fixturePassthroughEncoder{},
					Fault: func(got FaultPoint) error {
						if got == point {
							seen++
							if seen == occurrence {
								return faultErr
							}
						}
						return nil
					},
				})
				first, firstErr := faulting.RecoverSession(context.Background(), request)
				if !errors.Is(firstErr, faultErr) {
					t.Fatalf("first result=%+v err=%v want injected fault", first, firstErr)
				}
				restarted := openFixtureStore(fixture)
				result, err := restarted.RecoverSession(context.Background(), request)
				if err != nil {
					t.Fatalf("restart recovery: %v", err)
				}
				if result.Status != "recovered" && result.Status != "already_recovered" {
					t.Fatalf("restart result=%+v", result)
				}
				again, err := restarted.RecoverSession(context.Background(), request)
				if err != nil || again.Status != "already_recovered" || again.Cursor != result.Cursor || again.QuarantineDigest != result.QuarantineDigest {
					t.Fatalf("idempotent result=%+v err=%v first durable=%+v", again, err, result)
				}
				inspection, err := restarted.InspectSession(context.Background(), fixtureSessionID)
				if err != nil {
					t.Fatal(err)
				}
				if !inspection.Journal.Writable || countEventKind(inspection.Journal.Events, protocol.EventRecoveryDiagnostic) != 1 {
					t.Fatalf("restart inspection=%+v", inspection)
				}
			})
		}
	}
}

func TestExplicitRecoveryResumesPartialOperationOwnedFiles(t *testing.T) {
	for _, test := range []struct {
		name  string
		point FaultPoint
		glob  func(fixtureMaterialization) string
	}{
		{name: "quarantine", point: FaultQuarantineSync, glob: func(f fixtureMaterialization) string {
			return filepath.Join(f.sessionDir, "artifacts", "recovery-tail-*.bin")
		}},
		{name: "journal candidate", point: FaultCandidateSync, glob: func(f fixtureMaterialization) string {
			return filepath.Join(f.sessionDir, ".recovery-candidate-*.jsonl")
		}},
		{name: "metadata candidate", point: FaultRecoveryMetadataSync, glob: func(f fixtureMaterialization) string {
			return filepath.Join(f.sessionDir, ".recovery-metadata-*.json")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := copyFixture(t, "incomplete-batch")
			setup := openFixtureStore(fixture)
			_, request := recoveryRequestForFixture(t, setup, protocol.ControlOperationID("operation-partial-"+strings.ReplaceAll(test.name, " ", "-")), "txn-partial")
			faultErr := errors.New("pause before sync")
			store := New(fixture.root, Options{Encoder: fixturePassthroughEncoder{}, Fault: func(point FaultPoint) error {
				if point == test.point {
					return faultErr
				}
				return nil
			}})
			if _, err := store.RecoverSession(context.Background(), request); !errors.Is(err, faultErr) {
				t.Fatalf("first recovery err=%v", err)
			}
			matches, err := filepath.Glob(test.glob(fixture))
			if err != nil || len(matches) != 1 {
				t.Fatalf("operation-owned files=%v err=%v", matches, err)
			}
			info, err := os.Stat(matches[0])
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Truncate(matches[0], max(1, info.Size()/2)); err != nil {
				t.Fatal(err)
			}
			result, err := openFixtureStore(fixture).RecoverSession(context.Background(), request)
			if err != nil || result.Status != "recovered" {
				t.Fatalf("resumed result=%+v err=%v", result, err)
			}
		})
	}
}

func TestExplicitRecoveryPersistsCompleteRequestBeforeFirstPhysicalAction(t *testing.T) {
	fixture := copyFixture(t, "v1-truncated-final")
	setup := openFixtureStore(fixture)
	_, request := recoveryRequestForFixture(t, setup, "operation-persist-first", "txn-persist-first")
	beforeEvents, err := os.ReadFile(filepath.Join(fixture.sessionDir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	faultErr := errors.New("before quarantine write")
	store := New(fixture.root, Options{
		Encoder: fixturePassthroughEncoder{},
		Fault: func(point FaultPoint) error {
			if point == FaultQuarantineWrite {
				return faultErr
			}
			return nil
		},
	})
	if _, err := store.RecoverSession(context.Background(), request); !errors.Is(err, faultErr) {
		t.Fatalf("err=%v want %v", err, faultErr)
	}
	manifests, err := filepath.Glob(filepath.Join(fixture.sessionDir, ".recovery-request-*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(manifests) != 1 {
		t.Fatalf("request manifests=%v", manifests)
	}
	raw, err := os.ReadFile(manifests[0])
	if err != nil {
		t.Fatal(err)
	}
	var persisted struct {
		Request journal.RecoveryRequest `json:"request"`
	}
	if err := json.Unmarshal(raw, &persisted); err != nil || !reflect.DeepEqual(persisted.Request, request) {
		t.Fatalf("persisted request=%+v err=%v want %+v", persisted.Request, err, request)
	}
	afterEvents, err := os.ReadFile(filepath.Join(fixture.sessionDir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(afterEvents, beforeEvents) {
		t.Fatal("fault before first recovery action changed active journal")
	}
}

func TestExplicitRecoveryConflictsDoNotMutate(t *testing.T) {
	for _, mutate := range []struct {
		name string
		fn   func(*journal.RecoveryRequest)
	}{
		{name: "expected head", fn: func(request *journal.RecoveryRequest) { request.ExpectedHead.CommitSeq++ }},
		{name: "observed tail", fn: func(request *journal.RecoveryRequest) {
			request.ObservedTailDigest = protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("0", sha256.Size*2)}
		}},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			fixture := copyFixture(t, "v1-truncated-final")
			store := openFixtureStore(fixture)
			_, request := recoveryRequestForFixture(t, store, protocol.ControlOperationID("operation-conflict-"+strings.ReplaceAll(mutate.name, " ", "-")), "txn-conflict")
			mutate.fn(&request)
			abandoned := filepath.Join(fixture.sessionDir, ".metadata-"+strings.Repeat("a", 32)+".tmp")
			if err := os.WriteFile(abandoned, []byte("unrelated temporary"), 0o600); err != nil {
				t.Fatal(err)
			}
			before := snapshotTree(t, fixture.root)
			result, err := store.RecoverSession(context.Background(), request)
			if err != nil || result.Status != "conflict" {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if after := snapshotTree(t, fixture.root); !reflect.DeepEqual(after, before) {
				t.Fatal("conflicting recovery mutated storage")
			}
		})
	}
}

func TestExplicitRecoveryRejectsChangedRequestForPersistedOperation(t *testing.T) {
	fixture := copyFixture(t, "v1-truncated-final")
	setup := openFixtureStore(fixture)
	_, request := recoveryRequestForFixture(t, setup, "operation-stable", "txn-stable")
	faultErr := errors.New("pause after request persistence")
	store := New(fixture.root, Options{Encoder: fixturePassthroughEncoder{}, Fault: func(point FaultPoint) error {
		if point == FaultQuarantineWrite {
			return faultErr
		}
		return nil
	}})
	if _, err := store.RecoverSession(context.Background(), request); !errors.Is(err, faultErr) {
		t.Fatal(err)
	}
	changed := request
	changed.TransactionID = "txn-replacement-not-allowed"
	before := snapshotTree(t, fixture.root)
	result, err := openFixtureStore(fixture).RecoverSession(context.Background(), changed)
	if err != nil || result.Status != "conflict" {
		t.Fatalf("changed request result=%+v err=%v", result, err)
	}
	if after := snapshotTree(t, fixture.root); !reflect.DeepEqual(after, before) {
		t.Fatal("changed operation request mutated storage")
	}
}

func TestExplicitRecoveryRevalidatesCandidateLeavesBeforeActivation(t *testing.T) {
	for _, target := range []struct {
		name   string
		prefix string
	}{
		{name: "journal candidate", prefix: ".recovery-candidate-"},
		{name: "metadata candidate", prefix: ".recovery-metadata-"},
	} {
		t.Run(target.name, func(t *testing.T) {
			fixture := copyFixture(t, "incomplete-batch")
			setup := openFixtureStore(fixture)
			_, request := recoveryRequestForFixture(t, setup, protocol.ControlOperationID("operation-substitute-"+strings.ReplaceAll(target.name, " ", "-")), "txn-substitute")
			beforeEvents, err := os.ReadFile(filepath.Join(fixture.sessionDir, "events.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			replaced := false
			store := New(fixture.root, Options{Encoder: fixturePassthroughEncoder{}, Fault: func(point FaultPoint) error {
				if point != FaultCandidateValidate || replaced {
					return nil
				}
				matches, err := filepath.Glob(filepath.Join(fixture.sessionDir, target.prefix+"*"))
				if err != nil || len(matches) != 1 {
					return fmt.Errorf("candidate matches=%v err=%v", matches, err)
				}
				replaced = true
				if err := os.Rename(matches[0], matches[0]+".opened"); err != nil {
					return err
				}
				return os.WriteFile(matches[0], []byte("substituted recovery candidate"), 0o600)
			}})
			if _, err := store.RecoverSession(context.Background(), request); err == nil {
				t.Fatal("recovery accepted a substituted candidate leaf")
			}
			if !replaced {
				t.Fatal("candidate substitution boundary was not reached")
			}
			afterEvents, err := os.ReadFile(filepath.Join(fixture.sessionDir, "events.jsonl"))
			if err != nil || !bytes.Equal(afterEvents, beforeEvents) {
				t.Fatalf("failed candidate validation changed active journal: err=%v", err)
			}
		})
	}
}

func TestExplicitRecoveryKeepsOpenedSessionRootAcrossDirectorySubstitution(t *testing.T) {
	fixture := copyFixture(t, "incomplete-batch")
	setup := openFixtureStore(fixture)
	_, request := recoveryRequestForFixture(t, setup, "operation-root-substitution", "txn-root-substitution")
	originalEvents, err := os.ReadFile(filepath.Join(fixture.sessionDir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	originalMetadata, err := os.ReadFile(filepath.Join(fixture.sessionDir, "metadata.json"))
	if err != nil {
		t.Fatal(err)
	}
	moved := fixture.sessionDir + "-opened"
	replaced := false
	store := New(fixture.root, Options{Encoder: fixturePassthroughEncoder{}, Fault: func(point FaultPoint) error {
		if point != FaultCandidateActivate || replaced {
			return nil
		}
		replaced = true
		if err := os.Rename(fixture.sessionDir, moved); err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Join(fixture.sessionDir, "artifacts"), 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(fixture.sessionDir, "events.jsonl"), originalEvents, 0o600); err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(fixture.sessionDir, "metadata.json"), originalMetadata, 0o600)
	}})
	if _, err := store.RecoverSession(context.Background(), request); err == nil {
		t.Fatal("recovery continued through a substituted session path")
	}
	if !replaced {
		t.Fatal("session-root substitution boundary was not reached")
	}
	afterEvents, err := os.ReadFile(filepath.Join(fixture.sessionDir, "events.jsonl"))
	if err != nil || !bytes.Equal(afterEvents, originalEvents) {
		t.Fatalf("recovery mutated substituted events: err=%v", err)
	}
	afterMetadata, err := os.ReadFile(filepath.Join(fixture.sessionDir, "metadata.json"))
	if err != nil || !bytes.Equal(afterMetadata, originalMetadata) {
		t.Fatalf("recovery mutated substituted metadata: err=%v", err)
	}
}

func TestExplicitRecoveryRepositoryMethodUsesSameOperation(t *testing.T) {
	fixture := copyFixture(t, "incomplete-batch")
	store := openFixtureStore(fixture)
	_, request := recoveryRequestForFixture(t, store, "operation-repository", "txn-repository")
	result, err := store.Recover(context.Background(), request)
	if err != nil || result.Status != "recovered" {
		t.Fatalf("repository recovery result=%+v err=%v", result, err)
	}
	again, err := store.RecoverSession(context.Background(), request)
	if err != nil || again.Status != "already_recovered" || again.Cursor != result.Cursor {
		t.Fatalf("session retry result=%+v err=%v", again, err)
	}
}

func recoveryRequestForFixture(t *testing.T, store *Store, operationID protocol.ControlOperationID, transactionID protocol.TransactionID) (journal.SessionInspection, journal.RecoveryRequest) {
	t.Helper()
	inspection, err := store.InspectSession(context.Background(), fixtureSessionID)
	if err != nil {
		t.Fatal(err)
	}
	var details recoveryInspectionDetails
	found := false
	for _, diagnostic := range inspection.Journal.Diagnostics {
		if diagnostic.Details == nil {
			continue
		}
		var candidate recoveryInspectionDetails
		if json.Unmarshal(diagnostic.Details, &candidate) == nil && !candidate.ObservedTailDigest.IsZero() {
			details, found = candidate, true
			break
		}
	}
	if !found || details.ObservedTailDigest.Validate() != nil || details.ValidPrefixBytes < 0 || details.SourceBytes <= details.ValidPrefixBytes {
		t.Fatalf("inspection does not expose a complete recovery request: %+v", inspection)
	}
	return inspection, journal.RecoveryRequest{
		OperationID: operationID, Journal: inspection.Journal.Journal, ExpectedHead: inspection.Journal.Head,
		ObservedTailDigest: details.ObservedTailDigest, TransactionID: transactionID,
	}
}

func readObservedTail(t *testing.T, fixture fixtureMaterialization, inspection journal.SessionInspection) []byte {
	t.Helper()
	var details recoveryInspectionDetails
	for _, diagnostic := range inspection.Journal.Diagnostics {
		if json.Unmarshal(diagnostic.Details, &details) == nil && !details.ObservedTailDigest.IsZero() {
			break
		}
	}
	raw, err := os.ReadFile(filepath.Join(fixture.sessionDir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if details.ValidPrefixBytes < 0 || details.ValidPrefixBytes >= int64(len(raw)) {
		t.Fatalf("invalid valid prefix size %d for %d bytes", details.ValidPrefixBytes, len(raw))
	}
	tail := bytes.Clone(raw[details.ValidPrefixBytes:])
	digest := sha256.Sum256(tail)
	if details.ObservedTailDigest.Value != hex.EncodeToString(digest[:]) {
		t.Fatalf("tail digest=%x inspection=%+v", digest, details.ObservedTailDigest)
	}
	return tail
}

func containsEventKind(events []protocol.EventRecord, kind string) bool {
	return countEventKind(events, kind) > 0
}

func countEventKind(events []protocol.EventRecord, kind string) int {
	count := 0
	for _, event := range events {
		if event.Envelope.Kind == kind {
			count++
		}
	}
	return count
}

func recoveryArtifacts(t *testing.T, sessionDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(sessionDir, "artifacts"))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "recovery-tail-") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names
}
