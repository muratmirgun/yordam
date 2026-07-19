package jsonl_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
	"github.com/oklog/ulid/v2"
)

type passthroughEncoder struct{}

func (passthroughEncoder) EncodeProposed(event protocol.ProposedEvent) (json.RawMessage, error) {
	return protocol.CloneRawMessage(event.Payload), nil
}

type v2JournalFixture struct {
	repo       *jsonl.Store
	root       string
	ref        protocol.JournalRef
	head       protocol.CommittedCursor
	eventsPath string
	metaPath   string
}

func newV2Journal(t *testing.T) v2JournalFixture {
	return newV2JournalWithFault(t, nil)
}

func newV2JournalWithFault(t *testing.T, fault jsonl.FaultInjector) v2JournalFixture {
	return newV2JournalWithOptions(t, jsonl.Options{Encoder: passthroughEncoder{}, Fault: fault})
}

func newV2JournalWithOptions(t *testing.T, opts jsonl.Options) v2JournalFixture {
	t.Helper()
	root := t.TempDir()
	opts.Clock = func() time.Time { return time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC) }
	opts.Entropy = strings.NewReader(strings.Repeat("e", 4096))
	repo := jsonl.New(root, opts)
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	session, err := repo.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(session.ID)}
	transactionID := protocol.TransactionID("txn-bootstrap")
	payload, err := canonicaljson.Marshal(protocol.SessionCreatedV1{
		WorkspaceID: protocol.WorkspaceID(workspace.ID), CanonicalPath: workspace.CanonicalPath,
		Title: session.Title, Mode: string(session.Mode), ProviderID: "p", ModelID: "m",
	})
	if err != nil {
		t.Fatal(err)
	}
	event := protocol.EventEnvelope{
		SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1,
		JournalKind: ref.Kind, JournalID: ref.ID, SessionID: protocol.SessionID(session.ID),
		EventID: "evt-bootstrap", Seq: 1, Time: session.CreatedAt,
		Kind: protocol.EventSessionCreated, TransactionID: transactionID, Payload: payload,
	}
	digest, err := canonicaljson.TransactionDigest([]protocol.EventEnvelope{event})
	if err != nil {
		t.Fatal(err)
	}
	markerPayload, err := canonicaljson.Marshal(protocol.TransactionCommittedV1{
		TransactionID: transactionID, FirstSeq: 1, LastSeq: 1, EventCount: 1, Digest: digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	marker := protocol.EventEnvelope{
		SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1,
		JournalKind: ref.Kind, JournalID: ref.ID, SessionID: protocol.SessionID(session.ID),
		EventID: "evt-bootstrap-marker", Seq: 2, Time: session.CreatedAt,
		Kind: protocol.EventTransactionCommitted, TransactionID: transactionID, Payload: markerPayload,
	}
	eventLine, err := canonicaljson.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	markerLine, err := canonicaljson.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	eventsPath := filepath.Join(sessionDir, "events.jsonl")
	if err := os.WriteFile(eventsPath, append(append(eventLine, '\n'), append(markerLine, '\n')...), 0o600); err != nil {
		t.Fatal(err)
	}
	metaPath := filepath.Join(sessionDir, "metadata.json")
	session.LastSeq = 2
	metadata, err := json.MarshalIndent(session, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metaPath, metadata, 0o600); err != nil {
		t.Fatal(err)
	}
	return v2JournalFixture{
		repo: repo, root: root, ref: ref,
		head:       protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 2, TransactionID: transactionID},
		eventsPath: eventsPath, metaPath: metaPath,
	}
}

func proposed(eventID protocol.EventID, kind string) protocol.ProposedEvent {
	payload, err := canonicaljson.Marshal(protocol.TaskCreatedV1{
		Goal: "test committed batch", OutcomeContractID: "contract-1", ContractVersion: 1,
	})
	if err != nil {
		panic(err)
	}
	return protocol.ProposedEvent{
		EventID: eventID, Time: time.Date(2026, 7, 18, 10, 1, 0, 0, time.UTC),
		PayloadVersion: 1, Kind: kind, SessionID: "session-from-request",
		TaskID: "task-1", Payload: payload,
	}
}

func appendCommitted(t *testing.T, fixture v2JournalFixture, head protocol.CommittedCursor, transactionID protocol.TransactionID, eventID protocol.EventID) journal.AppendResult {
	t.Helper()
	event := proposed(eventID, protocol.EventTaskCreated)
	event.SessionID = protocol.SessionID(fixture.ref.ID)
	result, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: head, TransactionID: transactionID, Events: []protocol.ProposedEvent{event},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != journal.AppendCommitted {
		t.Fatalf("append status=%q result=%+v", result.Status, result)
	}
	return result
}

func TestAppendBatchConflictsAtExpectedHeadWithoutPartialWrite(t *testing.T) {
	fixture := newV2Journal(t)
	requestA := journal.AppendRequest{Journal: fixture.ref, ExpectedHead: fixture.head, TransactionID: "txn-a", Events: []protocol.ProposedEvent{proposed("evt-a", protocol.EventTaskCreated)}}
	requestB := journal.AppendRequest{Journal: fixture.ref, ExpectedHead: fixture.head, TransactionID: "txn-b", Events: []protocol.ProposedEvent{proposed("evt-b", protocol.EventTaskCreated)}}
	requestA.Events[0].SessionID = protocol.SessionID(fixture.ref.ID)
	requestB.Events[0].SessionID = protocol.SessionID(fixture.ref.ID)
	results := make(chan journal.AppendResult, 2)
	go func() { result, _ := fixture.repo.AppendBatch(context.Background(), requestA); results <- result }()
	go func() { result, _ := fixture.repo.AppendBatch(context.Background(), requestB); results <- result }()
	counts := map[journal.AppendStatus]int{}
	counts[(<-results).Status]++
	counts[(<-results).Status]++
	if counts[journal.AppendCommitted] != 1 || counts[journal.AppendConflict] != 1 {
		t.Fatalf("counts=%v", counts)
	}
	lines := readJournalLines(t, fixture.eventsPath)
	if len(lines) != 4 {
		t.Fatalf("physical line count=%d want 4", len(lines))
	}
}

func TestAppendBatchWritesMarkerAndAdvancesMetadataAfterCommit(t *testing.T) {
	fixture := newV2Journal(t)
	result := appendCommitted(t, fixture, fixture.head, "txn-a", "evt-a")
	lines := readJournalLines(t, fixture.eventsPath)
	var marker protocol.EventEnvelope
	if err := json.Unmarshal(lines[len(lines)-1], &marker); err != nil {
		t.Fatal(err)
	}
	if marker.Kind != protocol.EventTransactionCommitted || marker.Seq != result.Cursor.CommitSeq {
		t.Fatalf("marker=%+v cursor=%+v", marker, result.Cursor)
	}
	if marker.Time.Before(result.Events[len(result.Events)-1].Time) {
		t.Fatalf("marker time %s precedes committed event time %s", marker.Time, result.Events[len(result.Events)-1].Time)
	}
	var metadata domain.Session
	raw, err := os.ReadFile(fixture.metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.LastSeq != result.Cursor.CommitSeq {
		t.Fatalf("metadata last_seq=%d cursor=%d", metadata.LastSeq, result.Cursor.CommitSeq)
	}
}

func TestAppendBatchRejectsTransactionTooLargeForRangeWithoutWriting(t *testing.T) {
	fixture := newV2Journal(t)
	events := make([]protocol.ProposedEvent, 1001)
	for index := range events {
		events[index] = proposed(protocol.EventID(fmt.Sprintf("evt-%04d", index)), protocol.EventTaskCreated)
		events[index].SessionID = protocol.SessionID(fixture.ref.ID)
	}
	before, err := os.ReadFile(fixture.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: fixture.head, TransactionID: "txn-too-large", Events: events,
	}); err == nil {
		t.Fatal("AppendBatch accepted a transaction that no valid ReadRange limit can return")
	}
	after, err := os.ReadFile(fixture.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("rejected oversized transaction changed the journal")
	}
}

func TestIncompleteBatchIsInvisibleAndRequiresRecovery(t *testing.T) {
	fixture := newV2Journal(t)
	incomplete := proposed("evt-incomplete", protocol.EventTaskCreated)
	incomplete.SessionID = protocol.SessionID(fixture.ref.ID)
	payload := incomplete.Payload
	envelope := protocol.EventEnvelope{
		SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: incomplete.PayloadVersion,
		JournalKind: fixture.ref.Kind, JournalID: fixture.ref.ID, SessionID: incomplete.SessionID,
		EventID: incomplete.EventID, Seq: fixture.head.CommitSeq + 1, Time: incomplete.Time,
		Kind: incomplete.Kind, TaskID: incomplete.TaskID, TransactionID: "txn-incomplete", Payload: payload,
	}
	line, err := canonicaljson.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(fixture.eventsPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(append(line, '\n')); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	page, err := fixture.repo.ReadRange(context.Background(), journal.ReadRangeRequest{Journal: fixture.ref, After: fixture.head, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 0 || page.Head != fixture.head {
		t.Fatalf("incomplete event became visible: %+v", page)
	}
	requestEvent := proposed("evt-next", protocol.EventTaskCreated)
	requestEvent.SessionID = protocol.SessionID(fixture.ref.ID)
	result, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: fixture.head, TransactionID: "txn-next", Events: []protocol.ProposedEvent{requestEvent},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != journal.AppendRecoveryRequired {
		t.Fatalf("status=%q want %q", result.Status, journal.AppendRecoveryRequired)
	}
}

func TestPostMarkerFaultsReturnCommitUnknownAndResolveByLookup(t *testing.T) {
	points := []jsonl.FaultPoint{
		jsonl.FaultMarkerWrite,
		jsonl.FaultMarkerSync,
		jsonl.FaultMetadataWrite,
		jsonl.FaultMetadataRename,
		jsonl.FaultDirectorySync,
	}
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			faultErr := errors.New("injected " + string(point))
			fixture := newV2JournalWithFault(t, func(got jsonl.FaultPoint) error {
				if got == point {
					return faultErr
				}
				return nil
			})
			event := proposed("evt-fault", protocol.EventTaskCreated)
			event.SessionID = protocol.SessionID(fixture.ref.ID)
			result, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
				Journal: fixture.ref, ExpectedHead: fixture.head,
				TransactionID: "txn-fault", Events: []protocol.ProposedEvent{event},
			})
			if !errors.Is(err, faultErr) || result.Status != journal.AppendCommitUnknown {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if !zeroCommittedCursor(result.Cursor) || result.CurrentHead != fixture.head {
				t.Fatalf("commit_unknown exposed an unverified cursor/head: %+v", result)
			}
			lookup, err := fixture.repo.LookupTransaction(context.Background(), fixture.ref, "txn-fault")
			if err != nil {
				t.Fatal(err)
			}
			if lookup.State != journal.TransactionCommitted {
				t.Fatalf("lookup=%+v after %s", lookup, point)
			}
		})
	}
}

func TestPreMarkerFaultsRequireRecoveryAndRemainUnknown(t *testing.T) {
	for _, point := range []jsonl.FaultPoint{jsonl.FaultEventWrite, jsonl.FaultEventSync} {
		t.Run(string(point), func(t *testing.T) {
			faultErr := errors.New("injected " + string(point))
			fixture := newV2JournalWithFault(t, func(got jsonl.FaultPoint) error {
				if got == point {
					return faultErr
				}
				return nil
			})
			event := proposed("evt-fault", protocol.EventTaskCreated)
			event.SessionID = protocol.SessionID(fixture.ref.ID)
			result, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
				Journal: fixture.ref, ExpectedHead: fixture.head,
				TransactionID: "txn-fault", Events: []protocol.ProposedEvent{event},
			})
			if !errors.Is(err, faultErr) || result.Status != journal.AppendRecoveryRequired {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if !zeroCommittedCursor(result.Cursor) || result.CurrentHead != fixture.head {
				t.Fatalf("recovery_required exposed an unverified cursor/head: %+v", result)
			}
			lookup, err := fixture.repo.LookupTransaction(context.Background(), fixture.ref, "txn-fault")
			if err != nil {
				t.Fatal(err)
			}
			if lookup.State != journal.TransactionUnknown {
				t.Fatalf("lookup=%+v after %s", lookup, point)
			}
		})
	}
}

func zeroCommittedCursor(cursor protocol.CommittedCursor) bool {
	return cursor == (protocol.CommittedCursor{})
}

type callbackEncoder struct {
	callback func() error
}

func (e *callbackEncoder) EncodeProposed(event protocol.ProposedEvent) (json.RawMessage, error) {
	if e.callback != nil {
		if err := e.callback(); err != nil {
			return nil, err
		}
	}
	return protocol.CloneRawMessage(event.Payload), nil
}

func TestAppendBatchRejectsOpenedEventsLeafSubstitution(t *testing.T) {
	encoder := new(callbackEncoder)
	fixture := newV2JournalWithOptions(t, jsonl.Options{Encoder: encoder})
	openedPath := fixture.eventsPath + ".opened"
	replacement := []byte("replacement must stay untouched")
	encoder.callback = func() error {
		if err := os.Rename(fixture.eventsPath, openedPath); err != nil {
			return err
		}
		return os.WriteFile(fixture.eventsPath, replacement, 0o600)
	}
	event := proposed("evt-substitution", protocol.EventTaskCreated)
	event.SessionID = protocol.SessionID(fixture.ref.ID)
	if _, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: fixture.head,
		TransactionID: "txn-substitution", Events: []protocol.ProposedEvent{event},
	}); err == nil {
		t.Fatal("AppendBatch wrote through an opened events leaf substitution")
	}
	after, err := os.ReadFile(fixture.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, replacement) {
		t.Fatalf("substituted leaf changed: %q", after)
	}
	opened, err := os.ReadFile(openedPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(readJournalLines(t, openedPath)) != 2 || bytes.Contains(opened, []byte("txn-substitution")) {
		t.Fatal("AppendBatch mutated the detached opened journal")
	}
}

func TestAppendBatchRequiresEncoderBeforeAnyWrite(t *testing.T) {
	fixture := newV2JournalWithOptions(t, jsonl.Options{})
	before, err := os.ReadFile(fixture.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	event := proposed("evt-no-encoder", protocol.EventTaskCreated)
	event.SessionID = protocol.SessionID(fixture.ref.ID)
	if _, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: fixture.head,
		TransactionID: "txn-no-encoder", Events: []protocol.ProposedEvent{event},
	}); err == nil {
		t.Fatal("AppendBatch accepted an unconfigured admission/encoding boundary")
	}
	after, err := os.ReadFile(fixture.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("missing encoder changed the journal")
	}
}

func TestAppendBatchAdaptsExistingJSONRedactorBoundary(t *testing.T) {
	const sentinel = "synthetic-batch-secret"
	redactor := secret.New(sentinel)
	fixture := newV2JournalWithOptions(t, jsonl.Options{Sanitize: redactor.JSON})
	event := proposed("evt-redacted", protocol.EventTaskCreated)
	event.SessionID = protocol.SessionID(fixture.ref.ID)
	payload, err := canonicaljson.Marshal(protocol.TaskCreatedV1{
		Goal: "must hide " + sentinel, OutcomeContractID: "contract-1", ContractVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	event.Payload = payload
	result, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: fixture.head,
		TransactionID: "txn-redacted", Events: []protocol.ProposedEvent{event},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != journal.AppendCommitted || bytes.Contains(result.Events[0].Payload, []byte(sentinel)) {
		t.Fatalf("result leaked configured secret: %+v", result)
	}
	raw, err := os.ReadFile(fixture.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(sentinel)) {
		t.Fatal("journal leaked configured secret")
	}
}

func appendRawV2Transaction(
	t *testing.T,
	fixture v2JournalFixture,
	transactionID protocol.TransactionID,
	event protocol.EventEnvelope,
	mutateMarker func(*protocol.TransactionCommittedV1, *protocol.EventEnvelope),
) (json.RawMessage, protocol.CommittedCursor) {
	t.Helper()
	if event.SchemaVersion == 0 {
		event.SchemaVersion = protocol.EnvelopeVersion
	}
	if event.PayloadVersion == 0 {
		event.PayloadVersion = 1
	}
	event.JournalKind, event.JournalID = fixture.ref.Kind, fixture.ref.ID
	if fixture.ref.Kind == protocol.JournalSession {
		event.SessionID = protocol.SessionID(fixture.ref.ID)
	}
	event.Seq = fixture.head.CommitSeq + 1
	event.TransactionID = transactionID
	digest, err := canonicaljson.TransactionDigest([]protocol.EventEnvelope{event})
	if err != nil {
		t.Fatal(err)
	}
	markerPayload := protocol.TransactionCommittedV1{
		TransactionID: transactionID, FirstSeq: event.Seq, LastSeq: event.Seq,
		EventCount: 1, Digest: digest,
	}
	marker := protocol.EventEnvelope{
		SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1,
		JournalKind: fixture.ref.Kind, JournalID: fixture.ref.ID,
		EventID: protocol.EventID("marker-" + transactionID), Seq: event.Seq + 1,
		Time: event.Time.Add(time.Second), Kind: protocol.EventTransactionCommitted,
		TransactionID: transactionID,
	}
	if fixture.ref.Kind == protocol.JournalSession {
		marker.SessionID = protocol.SessionID(fixture.ref.ID)
	}
	if mutateMarker != nil {
		mutateMarker(&markerPayload, &marker)
	}
	marker.Payload, err = canonicaljson.Marshal(markerPayload)
	if err != nil {
		t.Fatal(err)
	}
	eventRaw, err := canonicaljson.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	markerRaw, err := canonicaljson.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(fixture.eventsPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(append(append(eventRaw, '\n'), append(markerRaw, '\n')...)); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return eventRaw, protocol.CommittedCursor{
		JournalKind: fixture.ref.Kind, JournalID: fixture.ref.ID,
		CommitSeq: marker.Seq, TransactionID: transactionID,
	}
}

func TestUnknownKindAndPayloadVersionPreserveExactRawAndMakePrefixReadOnly(t *testing.T) {
	tests := []struct {
		name    string
		kind    string
		version uint32
		payload json.RawMessage
	}{
		{name: "unknown kind", kind: "future.kind", version: 1, payload: json.RawMessage(`{"future":true}`)},
		{name: "unsupported payload version", kind: protocol.EventTaskCreated, version: 99, payload: json.RawMessage(`{"contract_version":1,"goal":"future","outcome_contract_id":"contract-1"}`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newV2Journal(t)
			event := protocol.EventEnvelope{
				PayloadVersion: test.version, EventID: "evt-future",
				Time: time.Date(2026, 7, 18, 10, 2, 0, 0, time.UTC),
				Kind: test.kind, TaskID: "task-1", Payload: test.payload,
			}
			raw, cursor := appendRawV2Transaction(t, fixture, "txn-future", event, nil)
			preservedRaw := raw
			if test.name == "unknown kind" {
				preservedRaw = append(append(json.RawMessage(" \t"), raw...), ' ', ' ')
				lines := readJournalLines(t, fixture.eventsPath)
				lines[len(lines)-2] = preservedRaw
				rewritten := append(bytes.Join(lines, []byte{'\n'}), '\n')
				if err := os.WriteFile(fixture.eventsPath, rewritten, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			inspection, err := fixture.repo.Inspect(context.Background(), fixture.ref)
			if err != nil {
				t.Fatal(err)
			}
			if inspection.Writable || inspection.Head != cursor {
				t.Fatalf("inspection=%+v", inspection)
			}
			last := inspection.Events[len(inspection.Events)-1]
			if !bytes.Equal(last.RawEnvelope, preservedRaw) {
				t.Fatalf("raw envelope=%s want exact %s", last.RawEnvelope, preservedRaw)
			}
			last.RawEnvelope[0] ^= 1
			again, err := fixture.repo.Inspect(context.Background(), fixture.ref)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(again.Events[len(again.Events)-1].RawEnvelope, preservedRaw) {
				t.Fatal("inspection raw envelope aliased a prior result")
			}
		})
	}
}

func TestInvalidKnownPayloadAndMarkerCorruptionFailClosed(t *testing.T) {
	tests := []struct {
		name         string
		event        protocol.EventEnvelope
		mutateMarker func(*protocol.TransactionCommittedV1, *protocol.EventEnvelope)
	}{
		{
			name: "invalid known payload",
			event: protocol.EventEnvelope{
				EventID: "evt-invalid", Time: time.Date(2026, 7, 18, 10, 2, 0, 0, time.UTC),
				Kind: protocol.EventTaskCreated, TaskID: "task-1", Payload: json.RawMessage(`{}`),
			},
		},
		{
			name: "invalid marker count",
			event: protocol.EventEnvelope{
				EventID: "evt-invalid", Time: time.Date(2026, 7, 18, 10, 2, 0, 0, time.UTC),
				Kind: protocol.EventTaskCreated, TaskID: "task-1", Payload: proposed("ignored", protocol.EventTaskCreated).Payload,
			},
			mutateMarker: func(marker *protocol.TransactionCommittedV1, _ *protocol.EventEnvelope) { marker.EventCount = 2 },
		},
		{
			name: "invalid marker digest",
			event: protocol.EventEnvelope{
				EventID: "evt-invalid", Time: time.Date(2026, 7, 18, 10, 2, 0, 0, time.UTC),
				Kind: protocol.EventTaskCreated, TaskID: "task-1", Payload: proposed("ignored", protocol.EventTaskCreated).Payload,
			},
			mutateMarker: func(marker *protocol.TransactionCommittedV1, _ *protocol.EventEnvelope) {
				marker.Digest.Value = strings.Repeat("0", 64)
			},
		},
		{
			name: "invalid marker sequence",
			event: protocol.EventEnvelope{
				EventID: "evt-invalid", Time: time.Date(2026, 7, 18, 10, 2, 0, 0, time.UTC),
				Kind: protocol.EventTaskCreated, TaskID: "task-1", Payload: proposed("ignored", protocol.EventTaskCreated).Payload,
			},
			mutateMarker: func(_ *protocol.TransactionCommittedV1, marker *protocol.EventEnvelope) { marker.Seq++ },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newV2Journal(t)
			appendRawV2Transaction(t, fixture, "txn-invalid", test.event, test.mutateMarker)
			if _, err := fixture.repo.Inspect(context.Background(), fixture.ref); err == nil {
				t.Fatal("corrupt known journal data was accepted")
			}
		})
	}
}

func TestInspectScansMixedV1AndV2CommittedTransactions(t *testing.T) {
	root := t.TempDir()
	repo := jsonl.New(root, jsonl.Options{
		Clock:   func() time.Time { return time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC) },
		Entropy: strings.NewReader(strings.Repeat("m", 4096)), Encoder: passthroughEncoder{},
	})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	session, err := repo.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	eventsPath := filepath.Join(sessionDir, "events.jsonl")
	legacyLines := readJournalLines(t, eventsPath)
	if len(legacyLines) != 1 {
		t.Fatalf("legacy lines=%d", len(legacyLines))
	}
	var legacy domain.DurableEvent
	if err := json.Unmarshal(legacyLines[0], &legacy); err != nil {
		t.Fatal(err)
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(session.ID)}
	fixture := v2JournalFixture{
		repo: repo, root: root, ref: ref,
		head: protocol.CommittedCursor{
			JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 1,
			TransactionID: protocol.TransactionID("legacy:" + legacy.EventID),
		},
		eventsPath: eventsPath, metaPath: filepath.Join(sessionDir, "metadata.json"),
	}
	event := proposed("evt-v2", protocol.EventTaskCreated)
	event.SessionID = protocol.SessionID(ref.ID)
	result, err := repo.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: fixture.head, TransactionID: "txn-v2",
		Compatibility: &journal.CompatibilityDeclaration{
			ReaderVersion: protocol.EnvelopeVersion, WriterVersion: protocol.EnvelopeVersion, LegacyHead: fixture.head,
		},
		Events: []protocol.ProposedEvent{event},
	})
	if err != nil || result.Status != journal.AppendCommitted {
		t.Fatalf("mixed append result=%+v err=%v", result, err)
	}
	cursor := result.Cursor
	inspection, err := repo.Inspect(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Head != cursor || len(inspection.Events) != 3 || inspection.Events[0].Legacy == nil || inspection.Events[1].Envelope.Kind != protocol.EventMigrationCompatibilityDeclared || inspection.Events[2].Envelope.EventID != "evt-v2" {
		t.Fatalf("mixed inspection=%+v", inspection)
	}
	if !bytes.Equal(inspection.Events[0].Legacy.RawEnvelope, legacyLines[0]) {
		t.Fatal("legacy raw line was not preserved exactly")
	}
}

func TestExpectedHeadRejectsWrongIdentityAndDuplicateIDsWithoutWrite(t *testing.T) {
	fixture := newV2Journal(t)
	wrong := fixture.head
	wrong.JournalID = "other"
	event := proposed("evt-a", protocol.EventTaskCreated)
	event.SessionID = protocol.SessionID(fixture.ref.ID)
	if _, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: wrong, TransactionID: "txn-wrong", Events: []protocol.ProposedEvent{event},
	}); err == nil {
		t.Fatal("wrong expected-head identity was accepted")
	}
	first := appendCommitted(t, fixture, fixture.head, "txn-a", "evt-a")
	before, err := os.ReadFile(fixture.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []journal.AppendRequest{
		{Journal: fixture.ref, ExpectedHead: first.Cursor, TransactionID: "txn-a", Events: []protocol.ProposedEvent{proposed("evt-b", protocol.EventTaskCreated)}},
		{Journal: fixture.ref, ExpectedHead: first.Cursor, TransactionID: "txn-b", Events: []protocol.ProposedEvent{proposed("evt-a", protocol.EventTaskCreated)}},
	} {
		request.Events[0].SessionID = protocol.SessionID(fixture.ref.ID)
		if _, err := fixture.repo.AppendBatch(context.Background(), request); err == nil {
			t.Fatalf("duplicate request accepted: %+v", request)
		}
	}
	after, err := os.ReadFile(fixture.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("duplicate rejection partially wrote")
	}
}

func TestRecoverRemainsFailClosedTask3Boundary(t *testing.T) {
	fixture := newV2Journal(t)
	before, err := os.ReadFile(fixture.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.repo.Recover(context.Background(), journal.RecoveryRequest{
		OperationID: "recovery-operation", Journal: fixture.ref,
		ExpectedHead: fixture.head, TransactionID: "txn-recovery",
	}); err == nil {
		t.Fatal("Task 2 exposed recovery mutation before Task 3 quarantine/replacement")
	}
	after, err := os.ReadFile(fixture.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("fail-closed recovery boundary mutated the journal")
	}
}

func newLegacyJournalForRegression(t *testing.T) (*jsonl.Store, protocol.JournalRef, protocol.CommittedCursor, string) {
	t.Helper()
	root := t.TempDir()
	repo := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	session, err := repo.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(session.ID)}
	path := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "events.jsonl")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var event domain.DurableEvent
	if err := json.Unmarshal(bytes.TrimSuffix(raw, []byte("\n")), &event); err != nil {
		t.Fatal(err)
	}
	head := protocol.CommittedCursor{
		JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: event.Seq,
		TransactionID: protocol.TransactionID("legacy:" + event.EventID),
	}
	return repo, ref, head, path
}

func legacyLineForRegression(ref protocol.JournalRef, eventID string, seq uint64, payload string) []byte {
	return []byte(fmt.Sprintf(
		`{"schema_version":1,"event_id":%q,"session_id":%q,"seq":%d,"time":"2026-07-18T10:00:00Z","kind":"user.message","payload":%s}`+"\n",
		eventID, ref.ID, seq, payload,
	))
}

func appendPhysicalEnvelopes(t *testing.T, path string, envelopes ...protocol.EventEnvelope) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, envelope := range envelopes {
		line, err := canonicaljson.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write(append(line, '\n')); err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyJournalRegressionAcceptsV01NumbersAndTwoMiBPhysicalLimit(t *testing.T) {
	repo, ref, _, path := newLegacyJournalForRegression(t)
	floatLine := legacyLineForRegression(ref, "legacy-float", 2, `{"content":"numbers","decimal":1.5,"exponent":1e3}`)
	largeLine := legacyLineForRegression(ref, "legacy-large", 3, `{"content":"`+strings.Repeat("x", (1<<20)+4096)+`"}`)
	if len(largeLine) <= 1<<20 || len(largeLine) >= 2<<20 {
		t.Fatalf("legacy line size=%d does not exercise the 1-2 MiB window", len(largeLine))
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(append(floatLine, largeLine...)); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	inspection, err := repo.Inspect(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(inspection.Events) != 3 || !inspection.Writable {
		t.Fatalf("legacy inspection events=%d writable=%v", len(inspection.Events), inspection.Writable)
	}
}

func TestUnsupportedCommitMarkerRegressionRemainsStructuralAndAmbiguous(t *testing.T) {
	fixture := newV2Journal(t)
	event := protocol.EventEnvelope{
		SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1,
		JournalKind: fixture.ref.Kind, JournalID: fixture.ref.ID, SessionID: protocol.SessionID(fixture.ref.ID),
		EventID: "evt-future-data", Seq: 3, Time: time.Date(2026, 7, 18, 10, 2, 0, 0, time.UTC),
		Kind: protocol.EventTaskCreated, TaskID: "task-future", TransactionID: "txn-future",
		Payload: proposed("unused", protocol.EventTaskCreated).Payload,
	}
	unknownPayload, err := canonicaljson.Marshal(protocol.TransactionCommittedV1{
		TransactionID: "txn-future", FirstSeq: 3, LastSeq: 3, EventCount: 1,
		Digest: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("a", 64)},
	})
	if err != nil {
		t.Fatal(err)
	}
	unknownMarker := protocol.EventEnvelope{
		SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 99,
		JournalKind: fixture.ref.Kind, JournalID: fixture.ref.ID, SessionID: protocol.SessionID(fixture.ref.ID),
		EventID: "evt-future-marker", Seq: 4, Time: event.Time,
		Kind: protocol.EventTransactionCommitted, TransactionID: "txn-future", Payload: unknownPayload,
	}
	digest, err := canonicaljson.TransactionDigest([]protocol.EventEnvelope{event, unknownMarker})
	if err != nil {
		t.Fatal(err)
	}
	laterPayload, err := canonicaljson.Marshal(protocol.TransactionCommittedV1{
		TransactionID: "txn-future", FirstSeq: 3, LastSeq: 4, EventCount: 2, Digest: digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	laterMarker := unknownMarker
	laterMarker.PayloadVersion = 1
	laterMarker.EventID = "evt-later-marker"
	laterMarker.Seq = 5
	laterMarker.Payload = laterPayload
	appendPhysicalEnvelopes(t, fixture.eventsPath, event, unknownMarker, laterMarker)
	inspection, err := fixture.repo.Inspect(context.Background(), fixture.ref)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Writable || inspection.IncompleteTransaction != "txn-future" || len(inspection.Events) != 1 || inspection.Head != fixture.head {
		t.Fatalf("unsupported marker changed domain visibility: %+v", inspection)
	}
}

func TestPhysicalTransactionRegressionRejectsMoreThanOneThousandEvents(t *testing.T) {
	fixture := newV2Journal(t)
	events := make([]protocol.EventEnvelope, 1001)
	for index := range events {
		events[index] = protocol.EventEnvelope{
			SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1,
			JournalKind: fixture.ref.Kind, JournalID: fixture.ref.ID, SessionID: protocol.SessionID(fixture.ref.ID),
			EventID: protocol.EventID(fmt.Sprintf("evt-physical-%04d", index)), Seq: uint64(index + 3),
			Time: time.Date(2026, 7, 18, 10, 3, 0, 0, time.UTC), Kind: protocol.EventTaskCreated,
			TaskID: protocol.TaskID(fmt.Sprintf("task-%04d", index)), TransactionID: "txn-oversized",
			Payload: proposed("unused", protocol.EventTaskCreated).Payload,
		}
	}
	digest, err := canonicaljson.TransactionDigest(events)
	if err != nil {
		t.Fatal(err)
	}
	markerPayload, err := canonicaljson.Marshal(protocol.TransactionCommittedV1{
		TransactionID: "txn-oversized", FirstSeq: 3, LastSeq: 1003, EventCount: 1001, Digest: digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	marker := protocol.EventEnvelope{
		SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1,
		JournalKind: fixture.ref.Kind, JournalID: fixture.ref.ID, SessionID: protocol.SessionID(fixture.ref.ID),
		EventID: "evt-oversized-marker", Seq: 1004, Time: events[0].Time,
		Kind: protocol.EventTransactionCommitted, TransactionID: "txn-oversized", Payload: markerPayload,
	}
	appendPhysicalEnvelopes(t, fixture.eventsPath, append(events, marker)...)
	if _, err := fixture.repo.Inspect(context.Background(), fixture.ref); err == nil || !strings.Contains(err.Error(), "1000") {
		t.Fatalf("physical 1001-event transaction accepted: %v", err)
	}
}

func TestGeneratedMarkerIDRegressionRejectsProposedCollisionBeforeWrite(t *testing.T) {
	fixture := newV2Journal(t)
	clock := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	markerID, err := ulid.New(ulid.Timestamp(clock), strings.NewReader(strings.Repeat("e", 16)))
	if err != nil {
		t.Fatal(err)
	}
	markerID[15] += 2
	event := proposed(protocol.EventID(markerID.String()), protocol.EventTaskCreated)
	event.SessionID = protocol.SessionID(fixture.ref.ID)
	before, err := os.ReadFile(fixture.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: fixture.head, TransactionID: "txn-collision", Events: []protocol.ProposedEvent{event},
	}); err == nil {
		t.Fatal("generated marker ID collision accepted")
	}
	after, err := os.ReadFile(fixture.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("marker ID collision wrote journal bytes")
	}
}

func TestGeneratedMarkerIDRegressionRejectsDurableCollisionBeforeWrite(t *testing.T) {
	fixture := newV2Journal(t)
	clock := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	markerID, err := ulid.New(ulid.Timestamp(clock), strings.NewReader(strings.Repeat("e", 16)))
	if err != nil {
		t.Fatal(err)
	}
	markerID[15] += 2
	lines := readJournalLines(t, fixture.eventsPath)
	var event protocol.EventEnvelope
	var marker protocol.EventEnvelope
	if err := json.Unmarshal(lines[0], &event); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(lines[1], &marker); err != nil {
		t.Fatal(err)
	}
	event.EventID = protocol.EventID(markerID.String())
	digest, err := canonicaljson.TransactionDigest([]protocol.EventEnvelope{event})
	if err != nil {
		t.Fatal(err)
	}
	marker.Payload, err = canonicaljson.Marshal(protocol.TransactionCommittedV1{
		TransactionID: event.TransactionID, FirstSeq: event.Seq, LastSeq: event.Seq, EventCount: 1, Digest: digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	eventLine, err := canonicaljson.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	markerLine, err := canonicaljson.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	before := append(append(eventLine, '\n'), append(markerLine, '\n')...)
	if err := os.WriteFile(fixture.eventsPath, before, 0o600); err != nil {
		t.Fatal(err)
	}
	proposedEvent := proposed("evt-unique-collision-regression", protocol.EventTaskCreated)
	proposedEvent.SessionID = protocol.SessionID(fixture.ref.ID)
	if _, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: fixture.head, TransactionID: "txn-durable-collision", Events: []protocol.ProposedEvent{proposedEvent},
	}); err == nil {
		t.Fatal("generated marker ID durable collision accepted")
	}
	after, err := os.ReadFile(fixture.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("durable marker ID collision wrote journal bytes")
	}
}

func TestUnsyncedMarkerRegressionRequiresOriginalRootedIdentity(t *testing.T) {
	wantFault := errors.New("marker write result unknown")
	fixture := newV2JournalWithFault(t, func(point jsonl.FaultPoint) error {
		if point == jsonl.FaultMarkerWrite {
			return wantFault
		}
		return nil
	})
	event := proposed("evt-unsynced-regression", protocol.EventTaskCreated)
	event.SessionID = protocol.SessionID(fixture.ref.ID)
	result, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: fixture.head, TransactionID: "txn-unsynced-regression", Events: []protocol.ProposedEvent{event},
	})
	if !errors.Is(err, wantFault) || result.Status != journal.AppendCommitUnknown {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	raw, err := os.ReadFile(fixture.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(fixture.eventsPath, fixture.eventsPath+".detached"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixture.eventsPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	lookup, err := fixture.repo.LookupTransaction(context.Background(), fixture.ref, "txn-unsynced-regression")
	if err != nil {
		t.Fatal(err)
	}
	if lookup.State != journal.TransactionUnknown {
		t.Fatalf("substituted bytes proved unsynced marker: %+v", lookup)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(fixture.eventsPath), "journal.index.json")); !os.IsNotExist(err) {
		t.Fatalf("unresolved marker uncertainty published a disposable index: %v", err)
	}
}

func TestDirectorySyncHookRegressionReverifiesEventsBeforeCommitted(t *testing.T) {
	var armed atomic.Bool
	var eventsPath string
	var substitutionErr error
	fixture := newV2JournalWithFault(t, func(point jsonl.FaultPoint) error {
		if point != jsonl.FaultDirectorySync || !armed.Load() {
			return nil
		}
		raw, err := os.ReadFile(eventsPath)
		if err == nil {
			err = os.Rename(eventsPath, eventsPath+".detached")
		}
		if err == nil {
			err = os.WriteFile(eventsPath, raw, 0o600)
		}
		substitutionErr = err
		return nil
	})
	eventsPath = fixture.eventsPath
	armed.Store(true)
	event := proposed("evt-directory-regression", protocol.EventTaskCreated)
	event.SessionID = protocol.SessionID(fixture.ref.ID)
	result, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: fixture.head, TransactionID: "txn-directory-regression", Events: []protocol.ProposedEvent{event},
	})
	if substitutionErr != nil {
		t.Fatal(substitutionErr)
	}
	if err == nil || result.Status != journal.AppendCommitUnknown {
		t.Fatalf("substituted events leaf returned committed: result=%+v err=%v", result, err)
	}
}

func TestPostSyncFailureRetainsMarkerInodeUntilLookupResolution(t *testing.T) {
	for _, point := range []jsonl.FaultPoint{
		jsonl.FaultMarkerSync,
		jsonl.FaultMetadataWrite,
		jsonl.FaultMetadataRename,
		jsonl.FaultDirectorySync,
	} {
		t.Run(string(point), func(t *testing.T) {
			wantFault := errors.New("post-sync append failure")
			fixture := newV2JournalWithFault(t, func(got jsonl.FaultPoint) error {
				if got == point {
					return wantFault
				}
				return nil
			})
			event := proposed("evt-post-sync-"+protocol.EventID(point), protocol.EventTaskCreated)
			event.SessionID = protocol.SessionID(fixture.ref.ID)
			result, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
				Journal: fixture.ref, ExpectedHead: fixture.head,
				TransactionID: protocol.TransactionID("txn-post-sync-" + point), Events: []protocol.ProposedEvent{event},
			})
			if !errors.Is(err, wantFault) || result.Status != journal.AppendCommitUnknown {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			raw, err := os.ReadFile(fixture.eventsPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(fixture.eventsPath, fixture.eventsPath+".detached"); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(fixture.eventsPath, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			lookup, err := fixture.repo.LookupTransaction(context.Background(), fixture.ref, protocol.TransactionID("txn-post-sync-"+point))
			if err != nil {
				t.Fatal(err)
			}
			if lookup.State != journal.TransactionUnknown {
				t.Fatalf("substituted inode resolved post-sync uncertainty: %+v", lookup)
			}
		})
	}
}

func TestInspectAndHeadDoNotExposeMarkerWhenDurabilityResolutionFails(t *testing.T) {
	appendFault := errors.New("marker write unknown")
	viewFault := errors.New("committed view sync unknown")
	fixture := newV2JournalWithFault(t, func(point jsonl.FaultPoint) error {
		switch point {
		case jsonl.FaultMarkerWrite:
			return appendFault
		case jsonl.FaultCommittedViewSync:
			return viewFault
		default:
			return nil
		}
	})
	event := proposed("evt-inspect-uncertain", protocol.EventTaskCreated)
	event.SessionID = protocol.SessionID(fixture.ref.ID)
	result, err := fixture.repo.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: fixture.head,
		TransactionID: "txn-inspect-uncertain", Events: []protocol.ProposedEvent{event},
	})
	if !errors.Is(err, appendFault) || result.Status != journal.AppendCommitUnknown {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if inspection, err := fixture.repo.Inspect(context.Background(), fixture.ref); !errors.Is(err, viewFault) {
		t.Fatalf("Inspect exposed unresolved marker: inspection=%+v err=%v", inspection, err)
	}
	if head, err := fixture.repo.Head(context.Background(), fixture.ref); !errors.Is(err, viewFault) {
		t.Fatalf("Head exposed unresolved marker: head=%+v err=%v", head, err)
	}
}

func TestLegacyAndV2MutatorsShareJournalLockBeforeTemporaryCleanup(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	legacyDone := make(chan error, 1)
	fixture := newV2JournalWithOptions(t, jsonl.Options{
		Encoder: passthroughEncoder{},
		Sanitize: func(value any) (json.RawMessage, error) {
			entered <- struct{}{}
			<-release
			return json.Marshal(value)
		},
	})
	go func() {
		_, err := fixture.repo.WriteLegacyFixture(context.Background(), string(fixture.ref.ID), domain.EventUserMessage, map[string]any{"text": "legacy"})
		legacyDone <- err
	}()
	<-entered
	defer func() {
		close(release)
		<-legacyDone
	}()
	temporary := filepath.Join(filepath.Dir(fixture.eventsPath), ".metadata-"+strings.Repeat("2", 32)+".tmp")
	if err := os.WriteFile(temporary, []byte("live legacy writer"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	event := proposed("evt-lock-domain", protocol.EventTaskCreated)
	event.SessionID = protocol.SessionID(fixture.ref.ID)
	result, err := fixture.repo.AppendBatch(ctx, journal.AppendRequest{
		Journal: fixture.ref, ExpectedHead: fixture.head,
		TransactionID: "txn-lock-domain", Events: []protocol.ProposedEvent{event},
	})
	if !errors.Is(err, context.DeadlineExceeded) || result.Status != "" {
		t.Fatalf("AppendBatch bypassed active legacy mutator: result=%+v err=%v", result, err)
	}
	raw, err := os.ReadFile(temporary)
	if err != nil || string(raw) != "live legacy writer" {
		t.Fatalf("competing mutator deleted live temporary: contents=%q err=%v", raw, err)
	}
}

func TestLoadNeverEntersMutationSanitizerOrTemporaryCleanup(t *testing.T) {
	root := t.TempDir()
	setup := jsonl.New(root, jsonl.Options{
		Clock:   func() time.Time { return time.Date(2026, 7, 18, 11, 0, 0, 0, time.UTC) },
		Entropy: strings.NewReader(strings.Repeat("l", 4096)),
	})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	session, err := setup.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	entered := false
	loading := jsonl.New(root, jsonl.Options{
		Sanitize: func(value any) (json.RawMessage, error) {
			entered = true
			return json.Marshal(value)
		},
	})
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	temporary := filepath.Join(sessionDir, ".metadata-"+strings.Repeat("3", 32)+".tmp")
	contents := []byte("stale mutation temporary")
	if err := os.WriteFile(temporary, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loading.Load(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	if entered {
		t.Fatal("pure Load entered the mutation sanitizer")
	}
	raw, err := os.ReadFile(temporary)
	if err != nil || !bytes.Equal(raw, contents) {
		t.Fatalf("pure Load changed mutation temporary: contents=%q err=%v", raw, err)
	}
}

func readJournalLines(t *testing.T, path string) [][]byte {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var lines [][]byte
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), protocol.MaxEventBytes)
	for scanner.Scan() {
		lines = append(lines, append([]byte(nil), scanner.Bytes()...))
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return lines
}
