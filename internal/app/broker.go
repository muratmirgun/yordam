package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
)

const ConsumerFakeHeadless = "fake_headless"

type SnapshotVector struct {
	WorkspaceControl protocol.CommittedCursor  `json:"workspace_control"`
	SelectedSession  *protocol.CommittedCursor `json:"selected_session,omitempty"`
}

type BrokerSource interface {
	WorkspaceControl() protocol.JournalRef
	Session(protocol.SessionID) protocol.JournalRef
	Head(context.Context, protocol.JournalRef) (protocol.CommittedCursor, error)
	ReadRange(context.Context, journal.ReadRangeRequest) (journal.EventPage, error)
	Project(context.Context, SnapshotVector) (protocol.DurableProjection, protocol.RuntimeProjection, error)
}

type BrokerOptions struct {
	Source                   BrokerSource
	Epoch                    string
	DefaultQueueCapacity     int
	MaxQueueCapacity         int
	NonReconnectableOverflow func(string, protocol.SubscriptionTerminal)
}

type Broker struct {
	mu                       sync.Mutex
	source                   BrokerSource
	epoch                    string
	streamSeq                uint64
	defaultCapacity          int
	maxCapacity              int
	nextSubscription         uint64
	subscriptions            map[uint64]*brokerSubscription
	nonReconnectableOverflow func(string, protocol.SubscriptionTerminal)
}

type brokerSubscription struct {
	broker        *Broker
	id            uint64
	consumer      string
	selected      protocol.SessionID
	capacity      int
	queue         []protocol.SubscriptionItem
	cursor        protocol.ApplicationCursor
	wake          chan struct{}
	closed        bool
	terminal      bool
	overflowFired bool
}

type catchUpEvent struct {
	envelope protocol.EventEnvelope
	cursor   protocol.CommittedCursor
}

func NewBroker(options BrokerOptions) (*Broker, error) {
	if options.Source == nil {
		return nil, fmt.Errorf("broker source is required")
	}
	if options.Epoch == "" {
		return nil, fmt.Errorf("broker process epoch is required")
	}
	if options.DefaultQueueCapacity <= 0 {
		options.DefaultQueueCapacity = 64
	}
	if options.MaxQueueCapacity <= 0 {
		options.MaxQueueCapacity = options.DefaultQueueCapacity
	}
	if options.MaxQueueCapacity < options.DefaultQueueCapacity {
		return nil, fmt.Errorf("broker maximum queue capacity is smaller than default")
	}
	workspace := options.Source.WorkspaceControl()
	if err := workspace.Validate(); err != nil || workspace.Kind != protocol.JournalWorkspaceControl {
		return nil, fmt.Errorf("broker workspace-control journal is invalid")
	}
	return &Broker{
		source: options.Source, epoch: options.Epoch, defaultCapacity: options.DefaultQueueCapacity,
		maxCapacity: options.MaxQueueCapacity, subscriptions: make(map[uint64]*brokerSubscription),
		nonReconnectableOverflow: options.NonReconnectableOverflow,
	}, nil
}

func (b *Broker) Epoch() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.epoch
}

func (b *Broker) Snapshot(ctx context.Context, request protocol.SnapshotRequest) (protocol.ApplicationSnapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.validateSnapshotRequest(request); err != nil {
		return protocol.ApplicationSnapshot{}, err
	}
	return b.snapshotLocked(ctx, request.SelectedSessionID)
}

func (b *Broker) SnapshotAndSubscribe(ctx context.Context, request protocol.SnapshotRequest) (protocol.ApplicationSnapshot, Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.validateSnapshotRequest(request); err != nil {
		return protocol.ApplicationSnapshot{}, nil, err
	}
	snapshot, err := b.snapshotLocked(ctx, request.SelectedSessionID)
	if err != nil {
		return protocol.ApplicationSnapshot{}, nil, err
	}
	subscription := b.newSubscriptionLocked(request.Consumer, request.SelectedSessionID, request.QueueCapacity, snapshot.Cursor)
	b.subscriptions[subscription.id] = subscription
	return snapshot, subscription, nil
}

func (b *Broker) snapshotLocked(ctx context.Context, selected protocol.SessionID) (protocol.ApplicationSnapshot, error) {
	workspaceRef := b.source.WorkspaceControl()
	workspaceHead, err := b.source.Head(ctx, workspaceRef)
	if err != nil {
		return protocol.ApplicationSnapshot{}, err
	}
	if err := validateCursorForJournal(workspaceHead, workspaceRef); err != nil {
		return protocol.ApplicationSnapshot{}, err
	}
	vector := SnapshotVector{WorkspaceControl: workspaceHead}
	if selected != "" {
		ref := b.source.Session(selected)
		if ref != (protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(selected)}) {
			return protocol.ApplicationSnapshot{}, fmt.Errorf("selected-session journal identity mismatch")
		}
		head, headErr := b.source.Head(ctx, ref)
		if headErr != nil {
			return protocol.ApplicationSnapshot{}, headErr
		}
		if err := validateCursorForJournal(head, ref); err != nil {
			return protocol.ApplicationSnapshot{}, err
		}
		vector.SelectedSession = &head
	}
	durable, runtime, err := b.source.Project(ctx, vector)
	if err != nil {
		return protocol.ApplicationSnapshot{}, err
	}
	return protocol.ApplicationSnapshot{
		ProtocolVersion: protocol.ApplicationProtocolVersion,
		Cursor: protocol.ApplicationCursor{
			WorkspaceControl: workspaceHead, SelectedSession: protocol.DeepCopy(vector.SelectedSession),
			Stream: protocol.StreamCursor{Epoch: b.epoch, Seq: b.streamSeq},
		},
		Durable: protocol.DeepCopy(durable), Runtime: protocol.DeepCopy(runtime),
	}, nil
}

func (b *Broker) Subscribe(ctx context.Context, request protocol.SubscriptionRequest) (Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.validateSubscriptionRequest(request); err != nil {
		return nil, err
	}
	subscription := b.newSubscriptionLocked(request.Consumer, request.SelectedSessionID, request.QueueCapacity, protocol.DeepCopy(request.After))
	if request.After.Stream.Epoch != b.epoch {
		terminal := protocol.SubscriptionTerminal{
			Code: "transient.gap", Message: "transient process epoch changed",
			ResumeAfter: protocol.DeepCopy(request.After), RequiresSnapshot: true,
		}
		terminal.ResumeAfter.Stream = protocol.StreamCursor{Epoch: b.epoch, Seq: b.streamSeq}
		subscription.queueTerminalLocked(terminal)
		return subscription, nil
	}

	workspaceRef := b.source.WorkspaceControl()
	workspaceEvents, workspaceHead, err := b.readCatchUpLocked(ctx, workspaceRef, request.After.WorkspaceControl)
	if err != nil {
		return nil, err
	}
	events := workspaceEvents
	subscription.cursor.WorkspaceControl = request.After.WorkspaceControl
	if request.SelectedSessionID != "" {
		ref := b.source.Session(request.SelectedSessionID)
		after := protocol.CommittedCursor{}
		if request.After.SelectedSession != nil && cursorJournal(*request.After.SelectedSession) == ref {
			after = *request.After.SelectedSession
		}
		sessionEvents, sessionHead, readErr := b.readCatchUpLocked(ctx, ref, after)
		if readErr != nil {
			return nil, readErr
		}
		events = append(events, sessionEvents...)
		if len(sessionEvents) == 0 {
			subscription.cursor.SelectedSession = &sessionHead
		} else {
			subscription.cursor.SelectedSession = nil
			if after != (protocol.CommittedCursor{}) {
				subscription.cursor.SelectedSession = ptrCommittedCursor(after)
			}
		}
	}
	if len(workspaceEvents) == 0 {
		subscription.cursor.WorkspaceControl = workspaceHead
	}
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].envelope.Time.Equal(events[j].envelope.Time) {
			return events[i].envelope.EventID < events[j].envelope.EventID
		}
		return events[i].envelope.Time.Before(events[j].envelope.Time)
	})
	for _, candidate := range events {
		if !subscription.enqueueCatchUpEnvelopeLocked(candidate.envelope, candidate.cursor, b.epoch, b.streamSeq) {
			break
		}
	}
	if !subscription.terminal {
		b.subscriptions[subscription.id] = subscription
	}
	return subscription, nil
}

func (b *Broker) readCatchUpLocked(ctx context.Context, ref protocol.JournalRef, after protocol.CommittedCursor) ([]catchUpEvent, protocol.CommittedCursor, error) {
	if after != (protocol.CommittedCursor{}) {
		if err := validateCursorForJournal(after, ref); err != nil {
			return nil, protocol.CommittedCursor{}, err
		}
	}
	result := make([]catchUpEvent, 0)
	cursor := after
	var head protocol.CommittedCursor
	for {
		page, err := b.source.ReadRange(ctx, journal.ReadRangeRequest{Journal: ref, After: cursor, Limit: protocol.MaxCollectionMembers})
		if err != nil {
			return nil, protocol.CommittedCursor{}, err
		}
		head = page.Head
		for start := 0; start < len(page.Events); {
			end := start + 1
			for end < len(page.Events) && sameApplicationTransaction(page.Events[start], page.Events[end]) {
				end++
			}
			transactionCursor, cursorErr := applicationTransactionCursor(ref, page.Events[start:end])
			if cursorErr != nil {
				return nil, protocol.CommittedCursor{}, cursorErr
			}
			for _, record := range page.Events[start:end] {
				result = append(result, catchUpEvent{envelope: protocol.CloneEventEnvelope(record.Envelope), cursor: transactionCursor})
			}
			start = end
		}
		if !page.More {
			break
		}
		if len(page.Events) == 0 || page.Cursor == cursor {
			return nil, protocol.CommittedCursor{}, fmt.Errorf("subscription catch-up did not advance")
		}
		cursor = page.Cursor
	}
	if err := validateCursorForJournal(head, ref); err != nil {
		return nil, protocol.CommittedCursor{}, err
	}
	return result, head, nil
}

func (b *Broker) PublishCommitted(_ context.Context, ref protocol.JournalRef, cursor protocol.CommittedCursor, events []protocol.EventEnvelope) error {
	if err := validateCursorForJournal(cursor, ref); err != nil {
		return fmt.Errorf("publish cursor %+v for journal %+v: %w", cursor, ref, err)
	}
	for _, event := range events {
		if event.JournalKind != ref.Kind || event.JournalID != ref.ID || event.TransactionID != cursor.TransactionID {
			return fmt.Errorf("published committed event does not match transaction")
		}
	}
	b.mu.Lock()
	callbacks := make([]struct {
		consumer string
		terminal protocol.SubscriptionTerminal
	}, 0)
	for _, subscription := range b.subscriptions {
		if subscription.closed || subscription.terminal || !subscription.accepts(ref) || subscription.hasCursor(cursor) {
			continue
		}
		if !subscription.enqueueTransactionLocked(events, cursor, b.epoch, b.streamSeq) {
			if subscription.consumer == ConsumerFakeHeadless && !subscription.overflowFired && b.nonReconnectableOverflow != nil {
				subscription.overflowFired = true
				callbacks = append(callbacks, struct {
					consumer string
					terminal protocol.SubscriptionTerminal
				}{subscription.consumer, *subscription.queue[len(subscription.queue)-1].Terminal})
			}
			delete(b.subscriptions, subscription.id)
		}
	}
	b.mu.Unlock()
	for _, callback := range callbacks {
		b.nonReconnectableOverflow(callback.consumer, protocol.DeepCopy(callback.terminal))
	}
	return nil
}

func (b *Broker) PublishTransient(event protocol.ApplicationEvent) error {
	b.mu.Lock()
	b.streamSeq++
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	if event.StreamEventID == "" {
		event.StreamEventID = fmt.Sprintf("%s:%d", b.epoch, b.streamSeq)
	}
	event.ProtocolVersion = protocol.ApplicationProtocolVersion
	event.Classification = "transient"
	event.JournalCursor = nil
	if event.PayloadVersion == 0 {
		event.PayloadVersion = 1
	}
	if len(event.Payload) == 0 {
		event.Payload = json.RawMessage(`{}`)
	}
	callbacks := make([]struct {
		consumer string
		terminal protocol.SubscriptionTerminal
	}, 0)
	for _, subscription := range b.subscriptions {
		if subscription.closed || subscription.terminal {
			continue
		}
		copyEvent := protocol.CloneApplicationEvent(event)
		copyEvent.Cursor = protocol.DeepCopy(subscription.cursor)
		copyEvent.Cursor.Stream = protocol.StreamCursor{Epoch: b.epoch, Seq: b.streamSeq}
		if err := copyEvent.Validate(); err != nil {
			b.mu.Unlock()
			return err
		}
		if !subscription.enqueueEventLocked(copyEvent) {
			if subscription.consumer == ConsumerFakeHeadless && !subscription.overflowFired && b.nonReconnectableOverflow != nil {
				subscription.overflowFired = true
				callbacks = append(callbacks, struct {
					consumer string
					terminal protocol.SubscriptionTerminal
				}{subscription.consumer, *subscription.queue[len(subscription.queue)-1].Terminal})
			}
			delete(b.subscriptions, subscription.id)
		}
	}
	b.mu.Unlock()
	for _, callback := range callbacks {
		b.nonReconnectableOverflow(callback.consumer, protocol.DeepCopy(callback.terminal))
	}
	return nil
}

func (b *Broker) RotateEpoch(epoch string) error {
	if epoch == "" {
		return fmt.Errorf("transient epoch is required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if epoch == b.epoch {
		return nil
	}
	b.epoch, b.streamSeq = epoch, 0
	for id, subscription := range b.subscriptions {
		resume := protocol.DeepCopy(subscription.cursor)
		resume.Stream = protocol.StreamCursor{Epoch: epoch}
		subscription.queueTerminalLocked(protocol.SubscriptionTerminal{Code: "transient.gap", Message: "transient process epoch changed", ResumeAfter: resume, RequiresSnapshot: true})
		delete(b.subscriptions, id)
	}
	return nil
}

func (b *Broker) validateSnapshotRequest(request protocol.SnapshotRequest) error {
	if request.ProtocolVersion != protocol.ApplicationProtocolVersion {
		return requestError(codeInvalidProtocolVersion, "unsupported application protocol version", nil)
	}
	if request.Consumer == "" {
		return requestError(codeInvalidCommand, "snapshot consumer is required", nil)
	}
	_, err := b.capacity(request.QueueCapacity)
	return err
}

func (b *Broker) validateSubscriptionRequest(request protocol.SubscriptionRequest) error {
	if request.ProtocolVersion != protocol.ApplicationProtocolVersion {
		return requestError(codeInvalidProtocolVersion, "unsupported application protocol version", nil)
	}
	if request.Consumer == "" || request.After.Stream.Epoch == "" {
		return requestError(codeInvalidCommand, "subscription consumer and stream epoch are required", nil)
	}
	if err := validateCursorForJournal(request.After.WorkspaceControl, b.source.WorkspaceControl()); err != nil {
		return requestError(codeInvalidCommand, "invalid workspace-control cursor", err)
	}
	if request.SelectedSessionID == "" && request.After.SelectedSession != nil {
		return requestError(codeInvalidCommand, "subscription without selection carries a session cursor", nil)
	}
	_, err := b.capacity(request.QueueCapacity)
	return err
}

func (b *Broker) capacity(requested int) (int, error) {
	if requested == 0 {
		return b.defaultCapacity, nil
	}
	if requested < 0 || requested > b.maxCapacity {
		return 0, requestError(codeInvalidCommand, "subscription queue capacity is invalid", nil)
	}
	return requested, nil
}

func (b *Broker) newSubscriptionLocked(consumer string, selected protocol.SessionID, requested int, cursor protocol.ApplicationCursor) *brokerSubscription {
	capacity, _ := b.capacity(requested)
	b.nextSubscription++
	return &brokerSubscription{broker: b, id: b.nextSubscription, consumer: consumer, selected: selected, capacity: capacity, cursor: cursor, wake: make(chan struct{}, 1)}
}

func (s *brokerSubscription) accepts(ref protocol.JournalRef) bool {
	return ref.Kind == protocol.JournalWorkspaceControl || (ref.Kind == protocol.JournalSession && protocol.SessionID(ref.ID) == s.selected)
}

func (s *brokerSubscription) hasCursor(cursor protocol.CommittedCursor) bool {
	if cursor.JournalKind == protocol.JournalWorkspaceControl {
		return s.cursor.WorkspaceControl.CommitSeq >= cursor.CommitSeq
	}
	return s.cursor.SelectedSession != nil && s.cursor.SelectedSession.JournalID == cursor.JournalID && s.cursor.SelectedSession.CommitSeq >= cursor.CommitSeq
}

func (s *brokerSubscription) enqueueEnvelopeLocked(envelope protocol.EventEnvelope, cursor protocol.CommittedCursor, epoch string, streamSeq uint64) bool {
	if s.hasCursor(cursor) {
		return true
	}
	return s.enqueueEnvelope(envelope, cursor, epoch, streamSeq)
}

func (s *brokerSubscription) enqueueTransactionLocked(events []protocol.EventEnvelope, cursor protocol.CommittedCursor, epoch string, streamSeq uint64) bool {
	if s.closed || s.terminal || s.hasCursor(cursor) {
		return !s.closed && !s.terminal
	}
	if len(s.queue)+len(events) > s.capacity {
		s.queueTerminalLocked(protocol.SubscriptionTerminal{Code: "slow_consumer", Message: "subscription queue capacity exceeded", ResumeAfter: protocol.DeepCopy(s.cursor)})
		return false
	}
	next := s.cursorAfter(cursor, epoch, streamSeq)
	items := make([]protocol.SubscriptionItem, 0, len(events))
	for _, envelope := range events {
		event, err := applicationEvent(envelope, cursor, next)
		if err != nil {
			s.queueTerminalLocked(protocol.SubscriptionTerminal{Code: "invalid_event", Message: "committed event could not be projected", ResumeAfter: protocol.DeepCopy(s.cursor), RequiresSnapshot: true})
			return false
		}
		copyEvent := protocol.CloneApplicationEvent(event)
		items = append(items, protocol.SubscriptionItem{Event: &copyEvent})
	}
	s.queue = append(s.queue, items...)
	s.cursor = next
	s.signalLocked()
	return true
}

func (s *brokerSubscription) enqueueCatchUpEnvelopeLocked(envelope protocol.EventEnvelope, cursor protocol.CommittedCursor, epoch string, streamSeq uint64) bool {
	return s.enqueueEnvelope(envelope, cursor, epoch, streamSeq)
}

func (s *brokerSubscription) enqueueEnvelope(envelope protocol.EventEnvelope, cursor protocol.CommittedCursor, epoch string, streamSeq uint64) bool {
	next := s.cursorAfter(cursor, epoch, streamSeq)
	event, err := applicationEvent(envelope, cursor, next)
	if err != nil {
		s.queueTerminalLocked(protocol.SubscriptionTerminal{Code: "invalid_event", Message: "committed event could not be projected", ResumeAfter: protocol.DeepCopy(s.cursor), RequiresSnapshot: true})
		return false
	}
	if !s.enqueueEventLocked(event) {
		return false
	}
	s.cursor = next
	return true
}

func (s *brokerSubscription) cursorAfter(cursor protocol.CommittedCursor, epoch string, streamSeq uint64) protocol.ApplicationCursor {
	next := protocol.DeepCopy(s.cursor)
	if cursor.JournalKind == protocol.JournalWorkspaceControl {
		if next.WorkspaceControl.CommitSeq < cursor.CommitSeq {
			next.WorkspaceControl = cursor
		}
	} else {
		if next.SelectedSession == nil || next.SelectedSession.JournalID != cursor.JournalID || next.SelectedSession.CommitSeq < cursor.CommitSeq {
			next.SelectedSession = ptrCommittedCursor(cursor)
		}
	}
	next.Stream = protocol.StreamCursor{Epoch: epoch, Seq: streamSeq}
	return next
}

func (s *brokerSubscription) enqueueEventLocked(event protocol.ApplicationEvent) bool {
	if s.closed || s.terminal {
		return false
	}
	if len(s.queue) >= s.capacity {
		s.queueTerminalLocked(protocol.SubscriptionTerminal{Code: "slow_consumer", Message: "subscription queue capacity exceeded", ResumeAfter: protocol.DeepCopy(s.cursor)})
		return false
	}
	copyEvent := protocol.CloneApplicationEvent(event)
	s.queue = append(s.queue, protocol.SubscriptionItem{Event: &copyEvent})
	s.signalLocked()
	return true
}

func (s *brokerSubscription) queueTerminalLocked(terminal protocol.SubscriptionTerminal) {
	if s.closed || s.terminal {
		return
	}
	copyTerminal := protocol.DeepCopy(terminal)
	s.queue = append(s.queue, protocol.SubscriptionItem{Terminal: &copyTerminal})
	s.terminal = true
	s.signalLocked()
}

func (s *brokerSubscription) signalLocked() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *brokerSubscription) Next(ctx context.Context) (protocol.SubscriptionItem, error) {
	for {
		s.broker.mu.Lock()
		if len(s.queue) > 0 {
			item := protocol.DeepCopy(s.queue[0])
			s.queue[0] = protocol.SubscriptionItem{}
			s.queue = s.queue[1:]
			if item.Terminal != nil {
				s.closed = true
				delete(s.broker.subscriptions, s.id)
			}
			s.broker.mu.Unlock()
			return item, nil
		}
		if s.closed {
			s.broker.mu.Unlock()
			return protocol.SubscriptionItem{}, io.EOF
		}
		s.broker.mu.Unlock()
		select {
		case <-ctx.Done():
			return protocol.SubscriptionItem{}, ctx.Err()
		case <-s.wake:
		}
	}
}

func (s *brokerSubscription) Close() error {
	s.broker.mu.Lock()
	defer s.broker.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.queue = nil
	delete(s.broker.subscriptions, s.id)
	s.signalLocked()
	return nil
}

func applicationEvent(envelope protocol.EventEnvelope, journalCursor protocol.CommittedCursor, cursor protocol.ApplicationCursor) (protocol.ApplicationEvent, error) {
	correlation := protocol.EventCorrelation{JournalKind: envelope.JournalKind, JournalID: envelope.JournalID, SessionID: envelope.SessionID, TaskID: envelope.TaskID, TurnID: envelope.TurnID, ActivityID: envelope.ActivityID}
	if isApplicationControlEvent(envelope.Kind) {
		var identity struct {
			ControlOperationID protocol.ControlOperationID `json:"control_operation_id"`
		}
		if err := json.Unmarshal(envelope.Payload, &identity); err != nil {
			return protocol.ApplicationEvent{}, err
		}
		correlation.ControlOperationID = identity.ControlOperationID
	}
	event := protocol.ApplicationEvent{
		ProtocolVersion: protocol.ApplicationProtocolVersion, StreamEventID: string(envelope.EventID), Cursor: protocol.DeepCopy(cursor),
		Correlation: correlation, Time: envelope.Time, Kind: envelope.Kind, Classification: "durable",
		JournalCursor: ptrCommittedCursor(journalCursor), PayloadVersion: envelope.PayloadVersion, Payload: protocol.CloneRawMessage(envelope.Payload),
	}
	if err := event.Validate(); err != nil {
		return protocol.ApplicationEvent{}, err
	}
	return event, nil
}

func isApplicationControlEvent(kind string) bool {
	switch kind {
	case protocol.EventControlOperationPlanned, protocol.EventControlOperationAuthorized, protocol.EventControlOperationStarted,
		protocol.EventControlOperationCompleted, protocol.EventControlOperationFailed, protocol.EventControlOperationInterrupted:
		return true
	default:
		return false
	}
}

func sameApplicationTransaction(left, right protocol.EventRecord) bool {
	if left.Legacy != nil || right.Legacy != nil {
		return left.Legacy != nil && right.Legacy != nil && left.Legacy.EventID == right.Legacy.EventID
	}
	return left.Envelope.TransactionID != "" && left.Envelope.TransactionID == right.Envelope.TransactionID
}

func applicationTransactionCursor(ref protocol.JournalRef, records []protocol.EventRecord) (protocol.CommittedCursor, error) {
	if len(records) == 0 {
		return protocol.CommittedCursor{}, fmt.Errorf("empty committed transaction")
	}
	first, last := records[0], records[len(records)-1]
	if first.Legacy != nil {
		if len(records) != 1 {
			return protocol.CommittedCursor{}, fmt.Errorf("legacy transaction contains multiple events")
		}
		return protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: first.Legacy.Seq, TransactionID: protocol.TransactionID("legacy:" + first.Legacy.EventID)}, nil
	}
	return protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: last.Envelope.Seq + 1, TransactionID: first.Envelope.TransactionID}, nil
}

func validateCursorForJournal(cursor protocol.CommittedCursor, ref protocol.JournalRef) error {
	if err := cursor.Validate(); err != nil {
		return err
	}
	if cursorJournal(cursor) != ref {
		return fmt.Errorf("cursor journal mismatch")
	}
	return nil
}

func ptrCommittedCursor(cursor protocol.CommittedCursor) *protocol.CommittedCursor {
	copyCursor := cursor
	return &copyCursor
}

var _ Subscription = (*brokerSubscription)(nil)
