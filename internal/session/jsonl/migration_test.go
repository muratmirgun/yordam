package jsonl

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
)

const fixtureSessionID = protocol.SessionID("01ARZ3NDEKTSV4RRFFQ69G5FAV")

var foundationFixtureNames = []string{
	"v1-valid",
	"v1-truncated-final",
	"v1-unmatched-tool-start",
	"v1-stale-edit-recovery",
	"mixed-v1-v2",
	"unknown-future-kind",
	"unsupported-envelope-version",
	"unsupported-payload-version",
	"invalid-known-payload",
	"invalid-sequence",
	"incomplete-batch",
}

type fixtureWant struct {
	CommittedEvents int      `json:"committed_events"`
	Writable        bool     `json:"writable"`
	Diagnostics     []string `json:"diagnostics"`
}

type fixtureMaterialization struct {
	root       string
	sessionDir string
	want       fixtureWant
}

type fixturePassthroughEncoder struct{}

func (fixturePassthroughEncoder) EncodeProposed(event protocol.ProposedEvent) (json.RawMessage, error) {
	return protocol.CloneRawMessage(event.Payload), nil
}

func TestFoundationFixtureInventory(t *testing.T) {
	const fixtureRoot = "testdata/foundation"
	manifestRaw, err := os.ReadFile(filepath.Join(fixtureRoot, "fixtures.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[string]string)
	for lineNumber, line := range strings.Split(strings.TrimSpace(string(manifestRaw)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || len(fields[0]) != sha256.Size*2 || fields[0] != strings.ToLower(fields[0]) {
			t.Fatalf("manifest line %d is invalid: %q", lineNumber+1, line)
		}
		if _, err := hex.DecodeString(fields[0]); err != nil {
			t.Fatalf("manifest line %d digest: %v", lineNumber+1, err)
		}
		if _, duplicate := want[fields[1]]; duplicate {
			t.Fatalf("duplicate fixture manifest path %q", fields[1])
		}
		want[filepath.ToSlash(fields[1])] = fields[0]
	}

	got := make(map[string]string)
	err = filepath.WalkDir(fixtureRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() == "fixtures.sha256" {
			return nil
		}
		relative, err := filepath.Rel(fixtureRoot, path)
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(raw)
		got[filepath.ToSlash(relative)] = hex.EncodeToString(digest[:])
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fixture inventory mismatch\ngot:  %v\nwant: %v", got, want)
	}

	entries, err := os.ReadDir(fixtureRoot)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	wantNames := append([]string(nil), foundationFixtureNames...)
	sort.Strings(wantNames)
	if !reflect.DeepEqual(names, wantNames) {
		t.Fatalf("fixture directories=%v want %v", names, wantNames)
	}
}

func TestFoundationFixtureInspectionMatrix(t *testing.T) {
	for _, name := range foundationFixtureNames {
		t.Run(name, func(t *testing.T) {
			fixture := copyFixture(t, name)
			inspection, err := openFixtureStore(fixture).InspectSession(context.Background(), fixtureSessionID)
			if err != nil {
				t.Fatal(err)
			}
			if inspection.Journal.Writable != fixture.want.Writable || len(inspection.Journal.Events) != fixture.want.CommittedEvents {
				t.Fatalf("inspection writable=%v events=%d want writable=%v events=%d", inspection.Journal.Writable, len(inspection.Journal.Events), fixture.want.Writable, fixture.want.CommittedEvents)
			}
			for _, code := range fixture.want.Diagnostics {
				if !hasDiagnostic(inspection.Journal.Diagnostics, code) {
					t.Fatalf("inspection diagnostics=%+v want code %q", inspection.Journal.Diagnostics, code)
				}
			}
		})
	}
}

func TestInspectSessionNeverMutatesFixture(t *testing.T) {
	fixture := copyFixture(t, "v1-truncated-final")
	before := snapshotTree(t, fixture.root)
	inspection, err := openFixtureStore(fixture).InspectSession(context.Background(), fixtureSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Journal.Writable || !hasDiagnostic(inspection.Journal.Diagnostics, "incomplete_final_fragment") {
		t.Fatalf("inspection=%+v", inspection)
	}
	if after := snapshotTree(t, fixture.root); !reflect.DeepEqual(after, before) {
		t.Fatalf("inspection mutated source\nbefore=%v\nafter=%v", before, after)
	}
}

func TestLoadNeverMutatesFixture(t *testing.T) {
	fixture := copyFixture(t, "v1-truncated-final")
	eventsPath := filepath.Join(fixture.sessionDir, "events.jsonl")
	eventsRaw, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(eventsPath, bytes.TrimSuffix(eventsRaw, []byte("\n")), 0o600); err != nil {
		t.Fatal(err)
	}
	before := snapshotTree(t, fixture.root)
	replay, err := openFixtureStore(fixture).Load(context.Background(), string(fixtureSessionID))
	if err != nil {
		t.Fatal(err)
	}
	if !replay.ReadOnly || !strings.Contains(replay.RecoveryNote, "incomplete_final_fragment") {
		t.Fatalf("replay=%+v", replay)
	}
	if after := snapshotTree(t, fixture.root); !reflect.DeepEqual(after, before) {
		t.Fatal("legacy load mutated source")
	}
}

func TestV1UpcastPreservesOrderingAndDerivesTurnScopedCalls(t *testing.T) {
	inspection := inspectFixture(t, "v1-valid")
	if len(inspection.Journal.Events) != 13 {
		t.Fatalf("events=%d", len(inspection.Journal.Events))
	}
	for index, record := range inspection.Journal.Events {
		if record.Legacy == nil || record.Envelope.EventID != record.Legacy.EventID || record.Envelope.Seq != uint64(index+1) {
			t.Fatalf("record %d lost original identity: %+v", index, record)
		}
		if !bytes.Equal(record.Legacy.RawEnvelope, record.RawEnvelope) {
			t.Fatalf("record %d did not preserve its original line", index)
		}
	}
	firstTurn := inspection.Journal.Events[1].Envelope.TurnID
	secondTurn := inspection.Journal.Events[8].Envelope.TurnID
	if firstTurn == "" || secondTurn == "" || firstTurn == secondTurn || !strings.HasPrefix(string(firstTurn), "derived:") || !strings.HasPrefix(string(secondTurn), "derived:") {
		t.Fatalf("derived turns first=%q second=%q", firstTurn, secondTurn)
	}
	firstActivity := inspection.Journal.Events[3].Envelope.ActivityID
	secondActivity := inspection.Journal.Events[9].Envelope.ActivityID
	if firstActivity == "" || secondActivity == "" || firstActivity == secondActivity {
		t.Fatalf("same legacy call ID was not scoped by turn: first=%q second=%q", firstActivity, secondActivity)
	}
	wantTask := derivedFixtureID("task", fixtureSessionID, "legacy-user-1", 1, 0)
	if inspection.Journal.Events[1].Envelope.TaskID != protocol.TaskID(wantTask) {
		t.Fatalf("task ID=%q want %q", inspection.Journal.Events[1].Envelope.TaskID, wantTask)
	}
}

func TestV1UpcastLeavesMissingFactsUnknown(t *testing.T) {
	inspection := inspectFixture(t, "v1-valid")
	usageState, verificationState := protocol.UsageUnknown, protocol.ValueUnknown
	for _, record := range inspection.Journal.Events {
		if record.Envelope.Kind == protocol.EventContextUsageRecorded || record.Envelope.Kind == protocol.EventVerificationReceiptRecorded {
			t.Fatalf("v1 upcast invented usage or verification event: %q", record.Envelope.Kind)
		}
	}
	if usageState != protocol.UsageUnknown || verificationState != protocol.ValueUnknown || !hasDiagnostic(inspection.Journal.Diagnostics, "migration.lossy") {
		t.Fatalf("missing facts were not kept unknown: diagnostics=%+v", inspection.Journal.Diagnostics)
	}
}

func TestV1UpcastDuplicateStartsAndTerminalsBecomeUncertain(t *testing.T) {
	state := UpcastState{SessionID: fixtureSessionID}
	sources := []protocol.LegacySource{
		legacySource("u", 1, domain.EventUserMessage, `{"content":"turn"}`),
		legacySource("r", 2, domain.EventToolRequested, `{"request":{"call_id":"dup","name":"read","input":{},"workspace":"/tmp"},"mutation":"read_only","canonical_scope":"x","inside_workspace":true,"summary":"read"}`),
		legacySource("s1", 3, domain.EventToolStarted, `{"call_id":"dup"}`),
		legacySource("s2", 4, domain.EventToolStarted, `{"call_id":"dup"}`),
		legacySource("t1", 5, domain.EventToolResult, `{"result":{"call_id":"dup","status":"succeeded","content":"one","duration":1,"truncated":false}}`),
		legacySource("t2", 6, domain.EventToolResult, `{"result":{"call_id":"dup","status":"succeeded","content":"two","duration":1,"truncated":false}}`),
	}
	var records []protocol.EventRecord
	var diagnostics []protocol.Diagnostic
	for _, source := range sources {
		record, next, emitted := UpcastV1(source, state)
		state = next
		records = append(records, record)
		diagnostics = append(diagnostics, emitted...)
	}
	if records[3].Envelope.Kind != protocol.EventActivityUncertain || records[5].Envelope.Kind != protocol.EventActivityUncertain || !hasDiagnostic(diagnostics, "migration.duplicate_start") || !hasDiagnostic(diagnostics, "migration.duplicate_terminal") {
		t.Fatalf("records=%+v diagnostics=%+v", records, diagnostics)
	}
}

func TestV1UpcastUnmatchedStartsNeverBecomeSuccess(t *testing.T) {
	inspection := inspectFixture(t, "v1-unmatched-tool-start")
	for _, event := range inspection.Journal.Events {
		if event.Envelope.Kind == protocol.EventActivitySucceeded {
			t.Fatalf("unmatched start became success: %+v", event)
		}
	}
	if !hasDiagnostic(inspection.Journal.Diagnostics, "migration.unmatched_activity") {
		t.Fatalf("diagnostics=%+v", inspection.Journal.Diagnostics)
	}
}

func TestV1UpcastAcceptsRecoveryGeneratedInterruptionAndLegacyEvidence(t *testing.T) {
	stale := inspectFixture(t, "v1-stale-edit-recovery")
	last := stale.Journal.Events[len(stale.Journal.Events)-1]
	payload, ok := last.Decoded.(*protocol.TurnTerminalV1)
	if !ok || last.Envelope.Kind != protocol.EventTurnInterrupted || payload.Status != "interrupted" || payload.Reason != "unmatched tool.started" {
		t.Fatalf("recovery interruption=%+v decoded=%T", last, last.Decoded)
	}
	valid := inspectFixture(t, "v1-valid")
	if !hasDiagnostic(valid.Journal.Diagnostics, "migration.legacy_evidence") {
		t.Fatalf("legacy tool output was not classified as evidence: %+v", valid.Journal.Diagnostics)
	}
	for _, event := range valid.Journal.Events {
		if event.Envelope.Kind == protocol.EventSessionForked {
			t.Fatalf("legacy root acquired invented lineage: %+v", event)
		}
	}
}

func TestMixedV1V2KeepsOneOrderedCursorModel(t *testing.T) {
	inspection := inspectFixture(t, "mixed-v1-v2")
	if len(inspection.Journal.Events) != 3 || inspection.Journal.Events[0].Legacy == nil || inspection.Journal.Events[1].Legacy == nil || inspection.Journal.Events[2].Envelope.Kind != protocol.EventMigrationCompatibilityDeclared {
		t.Fatalf("mixed inspection=%+v", inspection)
	}
	if inspection.Journal.Head.CommitSeq != 4 || inspection.Journal.Head.TransactionID != "txn-mixed" {
		t.Fatalf("mixed head=%+v", inspection.Journal.Head)
	}
}

func TestV1UpcastIsDeterministicAndDoesNotAliasState(t *testing.T) {
	source := legacySource("event-1", 1, domain.EventSessionTitleChanged, `{"title":"hello"}`)
	initial := UpcastState{SessionID: fixtureSessionID, OpenCalls: map[string][]protocol.ActivityID{"kept": {"activity"}}}
	first, nextA, diagnosticsA := UpcastV1(source, initial)
	second, nextB, diagnosticsB := UpcastV1(source, initial)
	if !reflect.DeepEqual(first, second) || !reflect.DeepEqual(nextA, nextB) || !reflect.DeepEqual(diagnosticsA, diagnosticsB) {
		t.Fatal("upcast was not deterministic")
	}
	nextA.OpenCalls["kept"][0] = "mutated"
	if initial.OpenCalls["kept"][0] != "activity" || nextB.OpenCalls["kept"][0] != "activity" {
		t.Fatal("upcast state aliases its input or another result")
	}
}

func TestV1UpcastFirstV2AppendDeclaresCompatibilityOnce(t *testing.T) {
	fixture := copyFixture(t, "v1-valid")
	store := openFixtureStore(fixture)
	inspection, err := store.InspectSession(context.Background(), fixtureSessionID)
	if err != nil {
		t.Fatal(err)
	}
	request := journal.AppendRequest{
		Journal: inspection.Journal.Journal, ExpectedHead: inspection.Journal.Head, TransactionID: "txn-first-v2",
		Compatibility: &journal.CompatibilityDeclaration{ReaderVersion: 2, WriterVersion: 2, LegacyHead: inspection.Journal.Head},
		Events: []protocol.ProposedEvent{{
			EventID: "v2-title", Time: inspection.Session.UpdatedAt, PayloadVersion: 1, Kind: protocol.EventSessionTitleChanged,
			SessionID: fixtureSessionID, Payload: json.RawMessage(`{"title":"v2"}`),
		}},
	}
	result, err := store.AppendBatch(context.Background(), request)
	if err != nil || result.Status != journal.AppendCommitted || len(result.Events) != 2 || result.Events[0].Kind != protocol.EventMigrationCompatibilityDeclared {
		t.Fatalf("first v2 append result=%+v err=%v", result, err)
	}
	var declaration protocol.MigrationCompatibilityDeclaredV1
	if err := json.Unmarshal(result.Events[0].Payload, &declaration); err != nil {
		t.Fatal(err)
	}
	if declaration.ReaderVersion != 2 || declaration.WriterVersion != 2 || declaration.LegacyHead != request.ExpectedHead || declaration.DowngradeStatus != "v0.1_read_only_after_v2" || result.Events[0].TransactionID != result.Events[1].TransactionID {
		t.Fatalf("compatibility declaration=%+v events=%+v", declaration, result.Events)
	}
	request.ExpectedHead = result.Cursor
	request.TransactionID = "txn-second-declaration"
	request.Events[0].EventID = "v2-title-again"
	before := snapshotTree(t, fixture.root)
	if _, err := store.AppendBatch(context.Background(), request); err == nil {
		t.Fatal("second compatibility declaration was accepted")
	}
	if after := snapshotTree(t, fixture.root); !reflect.DeepEqual(after, before) {
		t.Fatal("rejected second declaration mutated storage")
	}
}

func copyFixture(t *testing.T, name string) fixtureMaterialization {
	t.Helper()
	source := filepath.Join("testdata", "foundation", name)
	workspacePath := t.TempDir()
	workspace, err := WorkspaceFromPath(workspacePath)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", string(fixtureSessionID))
	if err := os.MkdirAll(filepath.Join(sessionDir, "artifacts"), 0o700); err != nil {
		t.Fatal(err)
	}
	workspaceRaw, err := json.Marshal(workspace)
	if err != nil {
		t.Fatal(err)
	}
	workspaceDir := filepath.Join(root, "workspaces", workspace.ID)
	if err := os.WriteFile(filepath.Join(workspaceDir, "workspace.json"), workspaceRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	replacements := strings.NewReplacer("${WORKSPACE_ID}", workspace.ID, "${WORKSPACE}", workspace.CanonicalPath)
	for _, fileName := range []string{"metadata.json", "events.jsonl"} {
		raw, err := os.ReadFile(filepath.Join(source, fileName))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(sessionDir, fileName), []byte(replacements.Replace(string(raw))), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var want fixtureWant
	wantRaw, err := os.ReadFile(filepath.Join(source, "want.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(wantRaw, &want); err != nil {
		t.Fatal(err)
	}
	return fixtureMaterialization{root: root, sessionDir: sessionDir, want: want}
}

func openFixtureStore(fixture fixtureMaterialization) *Store {
	return New(fixture.root, Options{Encoder: fixturePassthroughEncoder{}})
}

func inspectFixture(t *testing.T, name string) journal.SessionInspection {
	t.Helper()
	fixture := copyFixture(t, name)
	inspection, err := openFixtureStore(fixture).InspectSession(context.Background(), fixtureSessionID)
	if err != nil {
		t.Fatal(err)
	}
	return inspection
}

func snapshotTree(t *testing.T, root string) map[string]string {
	t.Helper()
	snapshot := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			snapshot[filepath.ToSlash(relative)+"/"] = "dir"
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(raw)
		snapshot[filepath.ToSlash(relative)] = fmt.Sprintf("%s:%o", hex.EncodeToString(digest[:]), entry.Type().Perm())
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func hasDiagnostic(diagnostics []protocol.Diagnostic, code string) bool {
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == code {
			return true
		}
	}
	return false
}

func legacySource(eventID string, seq uint64, kind domain.EventKind, payload string) protocol.LegacySource {
	raw := json.RawMessage(fmt.Sprintf(`{"schema_version":1,"event_id":%q,"session_id":%q,"seq":%d,"time":"2026-07-18T10:00:00Z","kind":%q,"payload":%s}`, eventID, fixtureSessionID, seq, kind, payload))
	return protocol.LegacySource{
		SchemaVersion: 1, EventID: protocol.EventID(eventID), SessionID: fixtureSessionID, Seq: seq,
		Kind: string(kind), Payload: json.RawMessage(payload), RawEnvelope: raw,
	}
}

func derivedFixtureID(kind string, sessionID protocol.SessionID, anchor protocol.EventID, turnOrdinal, activityOrdinal uint64) string {
	raw := fmt.Sprintf("yordam-v0.2-upcast\x00%s\x00%s\x00%s\x00%d\x00%d", kind, sessionID, anchor, turnOrdinal, activityOrdinal)
	digest := sha256.Sum256([]byte(raw))
	return "derived:" + hex.EncodeToString(digest[:])
}
