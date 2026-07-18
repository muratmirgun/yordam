package projection

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/eventcodec"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
)

var ErrReadOnlyPrefix = errors.New("projection source is a read-only validated prefix")

type Projector[T any] interface {
	Version() uint32
	Zero(protocol.JournalRef) T
	Apply(T, protocol.EventRecord) (T, error)
}

type Snapshot[T any] struct {
	ProjectionVersion uint32                   `json:"projection_version"`
	Head              protocol.CommittedCursor `json:"head"`
	State             T                        `json:"state"`
	Digest            protocol.Digest          `json:"digest"`
}

type Cache[T any] interface {
	Load(context.Context, protocol.JournalRef) (Snapshot[T], bool, error)
	Store(context.Context, protocol.JournalRef, Snapshot[T]) error
	Delete(context.Context, protocol.JournalRef) error
}

type Store[T any] struct {
	repository journal.Repository
	projector  Projector[T]
	cache      Cache[T]
}

var foundationDescriptors = func() map[string]map[uint32]eventcodec.Descriptor {
	result := make(map[string]map[uint32]eventcodec.Descriptor)
	for _, descriptor := range eventcodec.FoundationDescriptors() {
		if result[descriptor.Kind] == nil {
			result[descriptor.Kind] = make(map[uint32]eventcodec.Descriptor)
		}
		result[descriptor.Kind][descriptor.Version] = descriptor
	}
	return result
}()

func ValidateFoundationEvent(record protocol.EventRecord) error {
	versions, known := foundationDescriptors[record.Envelope.Kind]
	if !known {
		return fmt.Errorf("unknown stateful event %q", record.Envelope.Kind)
	}
	descriptor, supported := versions[record.Envelope.PayloadVersion]
	if !supported {
		return fmt.Errorf("unsupported payload version %d for event %q", record.Envelope.PayloadVersion, record.Envelope.Kind)
	}
	if record.Decoded == nil || reflect.TypeOf(record.Decoded) != reflect.TypeOf(descriptor.New()) {
		return fmt.Errorf("decoded payload for %q does not match its foundation descriptor", record.Envelope.Kind)
	}
	return nil
}

func New[T any](repository journal.Repository, projector Projector[T], cache Cache[T]) *Store[T] {
	return &Store[T]{repository: repository, projector: projector, cache: cache}
}

func (s *Store[T]) Rebuild(ctx context.Context, ref protocol.JournalRef) (Snapshot[T], error) {
	if s == nil || s.repository == nil || s.projector == nil {
		return Snapshot[T]{}, fmt.Errorf("projection store is not configured")
	}
	if err := ref.Validate(); err != nil {
		return Snapshot[T]{}, err
	}
	inspection, err := s.repository.Inspect(ctx, ref)
	if err != nil {
		return Snapshot[T]{}, err
	}
	if inspection.Journal != ref {
		return Snapshot[T]{}, fmt.Errorf("projection inspection journal mismatch")
	}
	state, projectedHead, projectionErr := s.applyCommittedTransactions(ref, s.projector.Zero(ref), protocol.CommittedCursor{}, inspection.Events)
	if projectionErr != nil {
		snapshot, digestErr := s.snapshot(projectedHead, state)
		if digestErr != nil {
			return Snapshot[T]{}, errors.Join(projectionErr, digestErr)
		}
		return snapshot, errors.Join(ErrReadOnlyPrefix, projectionErr)
	}
	if len(inspection.Events) > 0 && projectedHead != inspection.Head {
		return Snapshot[T]{}, fmt.Errorf("projection rebuild head mismatch: projected=%+v inspected=%+v", projectedHead, inspection.Head)
	}
	snapshot, err := s.snapshot(inspection.Head, state)
	if err != nil {
		return Snapshot[T]{}, err
	}
	if !inspection.Writable {
		return snapshot, ErrReadOnlyPrefix
	}
	if s.cache != nil {
		if err := s.cache.Store(ctx, ref, snapshot); err != nil {
			return Snapshot[T]{}, err
		}
	}
	return cloneSnapshot(snapshot), nil
}

func (s *Store[T]) Load(ctx context.Context, ref protocol.JournalRef) (Snapshot[T], error) {
	if s == nil || s.repository == nil || s.projector == nil {
		return Snapshot[T]{}, fmt.Errorf("projection store is not configured")
	}
	if err := ref.Validate(); err != nil {
		return Snapshot[T]{}, err
	}
	if s.cache == nil {
		return s.Rebuild(ctx, ref)
	}
	snapshot, ok, err := s.cache.Load(ctx, ref)
	if err != nil {
		return Snapshot[T]{}, err
	}
	if !ok || snapshot.ProjectionVersion != s.projector.Version() || validateSnapshot(snapshot) != nil || cursorJournal(snapshot.Head) != ref {
		return s.Rebuild(ctx, ref)
	}
	head, err := s.repository.Head(ctx, ref)
	if err != nil {
		return Snapshot[T]{}, err
	}
	if head == snapshot.Head {
		return cloneSnapshot(snapshot), nil
	}
	return s.Update(ctx, snapshot)
}

func (s *Store[T]) Update(ctx context.Context, current Snapshot[T]) (Snapshot[T], error) {
	if s == nil || s.repository == nil || s.projector == nil {
		return current, fmt.Errorf("projection store is not configured")
	}
	if current.ProjectionVersion != s.projector.Version() {
		return current, fmt.Errorf("projection version mismatch")
	}
	if err := validateSnapshot(current); err != nil {
		return current, err
	}
	ref := cursorJournal(current.Head)
	if err := ref.Validate(); err != nil {
		return current, err
	}
	updated := cloneSnapshot(current)
	for {
		page, err := s.repository.ReadRange(ctx, journal.ReadRangeRequest{Journal: ref, After: updated.Head, Limit: 1000})
		if err != nil {
			return current, err
		}
		candidate, projectedHead, projectionErr := s.applyCommittedTransactions(ref, updated.State, updated.Head, page.Events)
		if projectionErr != nil {
			prefix, digestErr := s.snapshot(projectedHead, candidate)
			if digestErr != nil {
				return current, errors.Join(projectionErr, digestErr)
			}
			return prefix, errors.Join(ErrReadOnlyPrefix, projectionErr)
		}
		if len(page.Events) > 0 {
			if err := page.Cursor.Validate(); err != nil {
				return current, fmt.Errorf("invalid projection page cursor: %w", err)
			}
			if cursorJournal(page.Cursor) != ref || page.Cursor.CommitSeq <= updated.Head.CommitSeq || projectedHead != page.Cursor {
				return current, fmt.Errorf("projection page cursor did not advance")
			}
			updated, err = s.snapshot(page.Cursor, candidate)
			if err != nil {
				return current, err
			}
		}
		if !page.More {
			if page.Head != updated.Head {
				return current, fmt.Errorf("projection final page did not reach reported head")
			}
			break
		}
		if len(page.Events) == 0 {
			return current, fmt.Errorf("projection range reported more without advancing")
		}
	}
	if s.cache != nil {
		if err := s.cache.Store(ctx, ref, updated); err != nil {
			return current, err
		}
	}
	return cloneSnapshot(updated), nil
}

func (s *Store[T]) applyCommittedTransactions(
	ref protocol.JournalRef,
	initial T,
	initialHead protocol.CommittedCursor,
	records []protocol.EventRecord,
) (T, protocol.CommittedCursor, error) {
	state, head := protocol.DeepCopy(initial), initialHead
	for start := 0; start < len(records); {
		end := start + 1
		for end < len(records) && sameCommittedTransaction(records[start], records[end]) {
			end++
		}
		candidate := protocol.DeepCopy(state)
		for _, record := range records[start:end] {
			var err error
			candidate, err = s.projector.Apply(candidate, protocol.CloneEventRecord(record))
			if err != nil {
				return state, head, fmt.Errorf("project event %q: %w", record.Envelope.Kind, err)
			}
		}
		cursor, err := committedTransactionCursor(ref, records[start:end])
		if err != nil {
			return state, head, err
		}
		if head != (protocol.CommittedCursor{}) && cursor.CommitSeq <= head.CommitSeq {
			return state, head, fmt.Errorf("projection transaction cursor did not advance")
		}
		state, head = candidate, cursor
		start = end
	}
	return state, head, nil
}

func sameCommittedTransaction(left, right protocol.EventRecord) bool {
	if left.Legacy != nil || right.Legacy != nil {
		return left.Legacy != nil && right.Legacy != nil && left.Legacy.EventID == right.Legacy.EventID
	}
	return left.Envelope.TransactionID != "" && left.Envelope.TransactionID == right.Envelope.TransactionID
}

func committedTransactionCursor(ref protocol.JournalRef, records []protocol.EventRecord) (protocol.CommittedCursor, error) {
	if len(records) == 0 {
		return protocol.CommittedCursor{}, fmt.Errorf("projection transaction is empty")
	}
	first, last := records[0], records[len(records)-1]
	if first.Legacy != nil {
		if len(records) != 1 || first.Legacy.EventID == "" || first.Legacy.Seq == 0 {
			return protocol.CommittedCursor{}, fmt.Errorf("invalid legacy projection transaction")
		}
		return protocol.CommittedCursor{
			JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: first.Legacy.Seq,
			TransactionID: protocol.TransactionID("legacy:" + first.Legacy.EventID),
		}, nil
	}
	if first.Envelope.TransactionID == "" || first.Envelope.Seq == 0 || last.Envelope.Seq < first.Envelope.Seq {
		return protocol.CommittedCursor{}, fmt.Errorf("invalid v2 projection transaction identity")
	}
	for index, record := range records {
		if record.Legacy != nil || record.Envelope.TransactionID != first.Envelope.TransactionID || record.Envelope.Seq != first.Envelope.Seq+uint64(index) {
			return protocol.CommittedCursor{}, fmt.Errorf("noncontiguous projection transaction")
		}
	}
	return protocol.CommittedCursor{
		JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: last.Envelope.Seq + 1, TransactionID: first.Envelope.TransactionID,
	}, nil
}

func (s *Store[T]) snapshot(head protocol.CommittedCursor, state T) (Snapshot[T], error) {
	snapshot := Snapshot[T]{ProjectionVersion: s.projector.Version(), Head: head, State: protocol.DeepCopy(state)}
	digest, err := snapshotDigest(snapshot)
	if err != nil {
		return Snapshot[T]{}, fmt.Errorf("digest projection snapshot: %w", err)
	}
	snapshot.Digest = digest
	return snapshot, nil
}

func Canonical[T any](snapshot Snapshot[T]) ([]byte, error) {
	if err := validateSnapshot(snapshot); err != nil {
		return nil, err
	}
	return canonicaljson.Marshal(snapshot)
}

func validateSnapshot[T any](snapshot Snapshot[T]) error {
	if snapshot.ProjectionVersion == 0 {
		return fmt.Errorf("projection version is required")
	}
	if err := snapshot.Head.Validate(); err != nil {
		return err
	}
	want, err := snapshotDigest(snapshot)
	if err != nil {
		return err
	}
	if snapshot.Digest != want {
		return fmt.Errorf("projection snapshot digest mismatch")
	}
	return nil
}

func snapshotDigest[T any](snapshot Snapshot[T]) (protocol.Digest, error) {
	body := struct {
		ProjectionVersion uint32                   `json:"projection_version"`
		Head              protocol.CommittedCursor `json:"head"`
		State             T                        `json:"state"`
	}{ProjectionVersion: snapshot.ProjectionVersion, Head: snapshot.Head, State: snapshot.State}
	return canonicaljson.Digest(body)
}

func cursorJournal(cursor protocol.CommittedCursor) protocol.JournalRef {
	return protocol.JournalRef{Kind: cursor.JournalKind, ID: cursor.JournalID}
}

func cloneSnapshot[T any](snapshot Snapshot[T]) Snapshot[T] {
	return protocol.DeepCopy(snapshot)
}

type MemoryCache[T any] struct {
	mu        sync.RWMutex
	snapshots map[string]Snapshot[T]
}

func NewMemoryCache[T any]() *MemoryCache[T] {
	return &MemoryCache[T]{snapshots: make(map[string]Snapshot[T])}
}

func (c *MemoryCache[T]) Load(_ context.Context, ref protocol.JournalRef) (Snapshot[T], bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	snapshot, ok := c.snapshots[cacheKey(ref)]
	return cloneSnapshot(snapshot), ok, nil
}

func (c *MemoryCache[T]) Store(_ context.Context, ref protocol.JournalRef, snapshot Snapshot[T]) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshots[cacheKey(ref)] = cloneSnapshot(snapshot)
	return nil
}

func (c *MemoryCache[T]) Delete(_ context.Context, ref protocol.JournalRef) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.snapshots, cacheKey(ref))
	return nil
}

func cacheKey(ref protocol.JournalRef) string { return string(ref.Kind) + "\x00" + string(ref.ID) }
