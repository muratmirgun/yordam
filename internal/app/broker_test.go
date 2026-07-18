package app_test

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestSnapshotAndSubscribeHasNoCommitGap(t *testing.T) {
	source := newBrokerSource()
	entered, release := source.pauseSnapshot()
	broker := mustBroker(t, source, nil)
	opened := make(chan struct {
		snapshot protocol.ApplicationSnapshot
		sub      app.Subscription
		err      error
	}, 1)
	go func() {
		snapshot, sub, err := broker.SnapshotAndSubscribe(t.Context(), snapshotRequest(4))
		opened <- struct {
			snapshot protocol.ApplicationSnapshot
			sub      app.Subscription
			err      error
		}{snapshot, sub, err}
	}()
	<-entered

	committed := make(chan error, 1)
	go func() {
		events, cursor := source.commit(source.session, "event-during-snapshot", time.Unix(3, 0).UTC())
		committed <- broker.PublishCommitted(t.Context(), source.session, cursor, events)
	}()
	source.waitCommitted()
	close(release)
	if err := <-committed; err != nil {
		t.Fatal(err)
	}
	result := <-opened
	if result.err != nil {
		t.Fatal(result.err)
	}
	item, err := result.sub.Next(t.Context())
	if err != nil || item.Event == nil || item.Event.StreamEventID != "event-during-snapshot" {
		t.Fatalf("item=%+v err=%v snapshot=%+v", item, err, result.snapshot)
	}
}

func TestSubscriptionCatchUpMergesByTimestampThenEventID(t *testing.T) {
	source := newBrokerSource()
	source.commit(source.session, "z-session", time.Unix(2, 0).UTC())
	source.commit(source.workspace, "b-workspace", time.Unix(1, 0).UTC())
	source.commit(source.session, "a-session", time.Unix(1, 0).UTC())
	broker := mustBroker(t, source, nil)
	sub, err := broker.Subscribe(t.Context(), protocol.SubscriptionRequest{
		ProtocolVersion:   protocol.ApplicationProtocolVersion,
		SelectedSessionID: "session-1",
		After: protocol.ApplicationCursor{
			WorkspaceControl: source.initialWorkspace,
			SelectedSession:  ptrCursor(source.initialSession),
			Stream:           protocol.StreamCursor{Epoch: broker.Epoch()},
		},
		Consumer: "interactive", QueueCapacity: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a-session", "b-workspace", "z-session"}
	for _, id := range want {
		item, err := sub.Next(t.Context())
		if err != nil || item.Event == nil || item.Event.StreamEventID != id {
			t.Fatalf("got=%+v err=%v want=%s", item, err, id)
		}
	}
}

func TestSubscriptionSelectedSessionSwitchStartsNewSessionAtOrigin(t *testing.T) {
	source := newBrokerSource()
	other := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session-2"}
	source.ensure(other)
	source.commit(other, "new-session-event", time.Unix(2, 0).UTC())
	broker := mustBroker(t, source, nil)
	sub, err := broker.Subscribe(t.Context(), protocol.SubscriptionRequest{
		ProtocolVersion:   protocol.ApplicationProtocolVersion,
		SelectedSessionID: "session-2",
		After: protocol.ApplicationCursor{
			WorkspaceControl: source.initialWorkspace,
			SelectedSession:  ptrCursor(source.initialSession),
			Stream:           protocol.StreamCursor{Epoch: broker.Epoch()},
		},
		Consumer: "interactive", QueueCapacity: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	item, err := sub.Next(t.Context())
	if err != nil || item.Event == nil || item.Event.StreamEventID != "new-session-event" || item.Event.Correlation.SessionID != "session-2" {
		t.Fatalf("item=%+v err=%v", item, err)
	}
}

func TestSubscriptionDeliversEveryEventFromCommittedTransaction(t *testing.T) {
	source := newBrokerSource()
	broker := mustBroker(t, source, nil)
	_, sub, err := broker.SnapshotAndSubscribe(t.Context(), snapshotRequest(4))
	if err != nil {
		t.Fatal(err)
	}
	events, cursor := source.commitBatch(source.session, []string{"batch-one", "batch-two"}, time.Unix(2, 0).UTC())
	if err := broker.PublishCommitted(t.Context(), source.session, cursor, events); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"batch-one", "batch-two"} {
		item, err := sub.Next(t.Context())
		if err != nil || item.Event == nil || item.Event.StreamEventID != id {
			t.Fatalf("item=%+v err=%v want=%s", item, err, id)
		}
	}
}

func TestTransientEpochGapRequiresFreshSnapshot(t *testing.T) {
	source := newBrokerSource()
	broker := mustBroker(t, source, nil)
	sub, err := broker.Subscribe(t.Context(), protocol.SubscriptionRequest{
		ProtocolVersion:   protocol.ApplicationProtocolVersion,
		SelectedSessionID: "session-1",
		After:             protocol.ApplicationCursor{WorkspaceControl: source.initialWorkspace, SelectedSession: ptrCursor(source.initialSession), Stream: protocol.StreamCursor{Epoch: "previous-process", Seq: 8}},
		Consumer:          "interactive", QueueCapacity: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	item, err := sub.Next(t.Context())
	if err != nil || item.Terminal == nil || item.Terminal.Code != "transient.gap" || !item.Terminal.RequiresSnapshot {
		t.Fatalf("item=%+v err=%v", item, err)
	}
}

func TestSlowConsumerOverflowIsBoundedAndResumable(t *testing.T) {
	source := newBrokerSource()
	broker := mustBroker(t, source, nil)
	_, sub, err := broker.SnapshotAndSubscribe(t.Context(), snapshotRequest(1))
	if err != nil {
		t.Fatal(err)
	}
	for index, id := range []string{"one", "two", "three"} {
		events, cursor := source.commit(source.session, id, time.Unix(int64(index+2), 0).UTC())
		if err := broker.PublishCommitted(t.Context(), source.session, cursor, events); err != nil {
			t.Fatal(err)
		}
	}
	first, err := sub.Next(t.Context())
	if err != nil || first.Event == nil || first.Event.StreamEventID != "one" {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	terminal, err := sub.Next(t.Context())
	if err != nil || terminal.Terminal == nil || terminal.Terminal.Code != "slow_consumer" || terminal.Terminal.RequiresSnapshot {
		t.Fatalf("terminal=%+v err=%v", terminal, err)
	}
	if _, err := sub.Next(t.Context()); err != io.EOF {
		t.Fatalf("closed subscription err=%v", err)
	}
	resumed, err := broker.Subscribe(t.Context(), protocol.SubscriptionRequest{ProtocolVersion: 1, SelectedSessionID: "session-1", After: terminal.Terminal.ResumeAfter, Consumer: "interactive", QueueCapacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"two", "three"} {
		item, err := resumed.Next(t.Context())
		if err != nil || item.Event == nil || item.Event.StreamEventID != id {
			t.Fatalf("resumed=%+v err=%v want=%s", item, err, id)
		}
	}
}

func TestSlowConsumerFakeHeadlessCancellationRunsOnce(t *testing.T) {
	source := newBrokerSource()
	var cancellations atomic.Int32
	broker := mustBroker(t, source, func(string, protocol.SubscriptionTerminal) { cancellations.Add(1) })
	_, _, err := broker.SnapshotAndSubscribe(t.Context(), protocol.SnapshotRequest{ProtocolVersion: 1, SelectedSessionID: "session-1", Consumer: app.ConsumerFakeHeadless, QueueCapacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	for index, id := range []string{"one", "two", "three"} {
		events, cursor := source.commit(source.session, id, time.Unix(int64(index+2), 0).UTC())
		if err := broker.PublishCommitted(t.Context(), source.session, cursor, events); err != nil {
			t.Fatal(err)
		}
	}
	if got := cancellations.Load(); got != 1 {
		t.Fatalf("cancellations=%d", got)
	}
}

func TestTransientOverflowFakeHeadlessCancellationRunsOnceOutsideBrokerLock(t *testing.T) {
	source := newBrokerSource()
	var cancellations atomic.Int32
	callbackReturned := make(chan struct{}, 1)
	var broker *app.Broker
	broker = mustBroker(t, source, func(string, protocol.SubscriptionTerminal) {
		cancellations.Add(1)
		_ = broker.Epoch() // deadlocks when the callback is invoked under the broker lock
		callbackReturned <- struct{}{}
	})
	_, _, err := broker.SnapshotAndSubscribe(t.Context(), protocol.SnapshotRequest{
		ProtocolVersion: 1, SelectedSessionID: "session-1", Consumer: app.ConsumerFakeHeadless, QueueCapacity: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"one", "two", "three"} {
		if err := broker.PublishTransient(protocol.ApplicationEvent{
			StreamEventID: id,
			Correlation: protocol.EventCorrelation{
				JournalKind: protocol.JournalSession, JournalID: "session-1", SessionID: "session-1",
			},
			Time: time.Now().UTC(), Kind: app.ApplicationEventNotice, PayloadVersion: 1, Payload: json.RawMessage(`{}`),
		}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-callbackReturned:
	case <-time.After(time.Second):
		t.Fatal("transient overflow cancellation callback did not return")
	}
	if got := cancellations.Load(); got != 1 {
		t.Fatalf("cancellations=%d", got)
	}
}

type brokerSource struct {
	mu               sync.Mutex
	workspace        protocol.JournalRef
	session          protocol.JournalRef
	initialWorkspace protocol.CommittedCursor
	initialSession   protocol.CommittedCursor
	heads            map[protocol.JournalRef]protocol.CommittedCursor
	records          map[protocol.JournalRef][]protocol.EventRecord
	pauseEntered     chan struct{}
	pauseRelease     chan struct{}
	durable          chan struct{}
}

func newBrokerSource() *brokerSource {
	workspace := protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace-1"}
	session := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session-1"}
	s := &brokerSource{workspace: workspace, session: session, heads: make(map[protocol.JournalRef]protocol.CommittedCursor), records: make(map[protocol.JournalRef][]protocol.EventRecord), durable: make(chan struct{}, 32)}
	s.initialWorkspace = committedCursor(workspace, 1, "workspace-initial")
	s.initialSession = committedCursor(session, 1, "session-initial")
	s.heads[workspace], s.heads[session] = s.initialWorkspace, s.initialSession
	return s
}

func (s *brokerSource) WorkspaceControl() protocol.JournalRef { return s.workspace }
func (s *brokerSource) Session(id protocol.SessionID) protocol.JournalRef {
	return protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(id)}
}
func (s *brokerSource) Head(_ context.Context, ref protocol.JournalRef) (protocol.CommittedCursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.heads[ref], nil
}
func (s *brokerSource) ReadRange(_ context.Context, request journal.ReadRangeRequest) (journal.EventPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var events []protocol.EventRecord
	for _, record := range s.records[request.Journal] {
		if record.Envelope.Seq > request.After.CommitSeq {
			events = append(events, protocol.CloneEventRecord(record))
		}
	}
	return journal.EventPage{Events: events, Cursor: s.heads[request.Journal], Head: s.heads[request.Journal]}, nil
}
func (s *brokerSource) Project(_ context.Context, vector app.SnapshotVector) (protocol.DurableProjection, protocol.RuntimeProjection, error) {
	if s.pauseEntered != nil {
		close(s.pauseEntered)
		<-s.pauseRelease
	}
	data, _ := json.Marshal(vector)
	return protocol.DurableProjection{Workspace: protocol.ProjectionView{ID: string(s.workspace.ID), Kind: "workspace", Status: "ready", State: protocol.ValueKnown, Data: data}}, protocol.RuntimeProjection{}, nil
}
func (s *brokerSource) pauseSnapshot() (<-chan struct{}, chan struct{}) {
	s.pauseEntered, s.pauseRelease = make(chan struct{}), make(chan struct{})
	return s.pauseEntered, s.pauseRelease
}
func (s *brokerSource) ensure(ref protocol.JournalRef) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.heads[ref]; !ok {
		s.heads[ref] = committedCursor(ref, 1, "initial")
	}
}
func (s *brokerSource) commit(ref protocol.JournalRef, id string, at time.Time) ([]protocol.EventEnvelope, protocol.CommittedCursor) {
	return s.commitBatch(ref, []string{id}, at)
}

func (s *brokerSource) commitBatch(ref protocol.JournalRef, ids []string, at time.Time) ([]protocol.EventEnvelope, protocol.CommittedCursor) {
	s.mu.Lock()
	head := s.heads[ref]
	sessionID := protocol.SessionID("")
	if ref.Kind == protocol.JournalSession {
		sessionID = protocol.SessionID(ref.ID)
	}
	transactionID := protocol.TransactionID("tx-" + ids[0])
	events := make([]protocol.EventEnvelope, 0, len(ids))
	for index, id := range ids {
		envelope := protocol.EventEnvelope{SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1, JournalKind: ref.Kind, JournalID: ref.ID, SessionID: sessionID, EventID: protocol.EventID(id), Seq: head.CommitSeq + 1 + uint64(index), Time: at, Kind: "test.event", TransactionID: transactionID, Payload: json.RawMessage(`{}`)}
		events = append(events, envelope)
		s.records[ref] = append(s.records[ref], protocol.EventRecord{Envelope: envelope})
	}
	cursor := committedCursor(ref, head.CommitSeq+uint64(len(ids))+1, transactionID)
	s.heads[ref] = cursor
	s.mu.Unlock()
	s.durable <- struct{}{}
	return events, cursor
}
func (s *brokerSource) waitCommitted() { <-s.durable }

func mustBroker(t *testing.T, source app.BrokerSource, callback func(string, protocol.SubscriptionTerminal)) *app.Broker {
	t.Helper()
	broker, err := app.NewBroker(app.BrokerOptions{Source: source, Epoch: "epoch-1", DefaultQueueCapacity: 4, MaxQueueCapacity: 32, NonReconnectableOverflow: callback})
	if err != nil {
		t.Fatal(err)
	}
	return broker
}
func snapshotRequest(capacity int) protocol.SnapshotRequest {
	return protocol.SnapshotRequest{ProtocolVersion: 1, SelectedSessionID: "session-1", Consumer: "interactive", QueueCapacity: capacity}
}
func committedCursor(ref protocol.JournalRef, seq uint64, tx protocol.TransactionID) protocol.CommittedCursor {
	return protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: seq, TransactionID: tx}
}
func ptrCursor(cursor protocol.CommittedCursor) *protocol.CommittedCursor { return &cursor }
