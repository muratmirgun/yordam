package jsonl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
)

type keyedJournalLock struct {
	token chan struct{}
}

func newKeyedJournalLock() *keyedJournalLock {
	lock := &keyedJournalLock{token: make(chan struct{}, 1)}
	lock.token <- struct{}{}
	return lock
}

func (l *keyedJournalLock) lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.token:
		if err := ctx.Err(); err != nil {
			l.unlock()
			return err
		}
		return nil
	}
}

func (l *keyedJournalLock) unlock() { l.token <- struct{}{} }

func (s *Store) journalLock(ref protocol.JournalRef) *keyedJournalLock {
	key := journalLockKey(ref)
	loaded, _ := s.state.journalLocks.LoadOrStore(key, newKeyedJournalLock())
	return loaded.(*keyedJournalLock)
}

func journalLockKey(ref protocol.JournalRef) string {
	return string(ref.Kind) + "\x00" + string(ref.ID)
}

func markerUncertaintyKey(ref protocol.JournalRef, transactionID protocol.TransactionID) string {
	return journalLockKey(ref) + "\x00" + string(transactionID)
}

func (s *Store) markMarkerUncertain(transaction *sessionTransaction, ref protocol.JournalRef, transactionID protocol.TransactionID) {
	s.state.markerUncertainty.Store(markerUncertaintyKey(ref, transactionID), transaction.eventsInfo)
}

func (s *Store) clearMarkerUncertainty(ref protocol.JournalRef, transactionID protocol.TransactionID) {
	s.state.markerUncertainty.Delete(markerUncertaintyKey(ref, transactionID))
}

func (s *Store) journalHasMarkerUncertainty(ref protocol.JournalRef) bool {
	prefix := journalLockKey(ref) + "\x00"
	found := false
	s.state.markerUncertainty.Range(func(key, _ any) bool {
		value, ok := key.(string)
		if ok && strings.HasPrefix(value, prefix) {
			found = true
			return false
		}
		return true
	})
	return found
}

var errUnresolvedMarkerDurability = errors.New("unresolved marker durability")

func (s *Store) syncCommittedView(
	ctx context.Context,
	transaction *sessionTransaction,
	ref protocol.JournalRef,
	scan journalScan,
) error {
	prefix := journalLockKey(ref) + "\x00"
	var uncertaintyKeys []string
	var uncertaintyErr error
	s.state.markerUncertainty.Range(func(key, value any) bool {
		encodedKey, keyOK := key.(string)
		if !keyOK || !strings.HasPrefix(encodedKey, prefix) {
			return true
		}
		eventsInfo, infoOK := value.(os.FileInfo)
		transactionID := protocol.TransactionID(strings.TrimPrefix(encodedKey, prefix))
		_, committed := scan.transactions[transactionID]
		if !infoOK || eventsInfo == nil || transaction.eventsInfo == nil || !os.SameFile(eventsInfo, transaction.eventsInfo) || !committed {
			uncertaintyErr = errUnresolvedMarkerDurability
			return false
		}
		uncertaintyKeys = append(uncertaintyKeys, encodedKey)
		return true
	})
	if uncertaintyErr != nil {
		return uncertaintyErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := transaction.events.Sync(); err != nil {
		return err
	}
	if err := s.injectFault(FaultCommittedViewSync); err != nil {
		return err
	}
	if err := errors.Join(ctx.Err(), transaction.verifyEvents()); err != nil {
		return err
	}
	for _, key := range uncertaintyKeys {
		s.state.markerUncertainty.Delete(key)
	}
	return nil
}

func (s *Store) AppendBatch(ctx context.Context, request journal.AppendRequest) (journal.AppendResult, error) {
	if err := validateAppendRequest(request); err != nil {
		return journal.AppendResult{}, err
	}
	admission, err := s.acquireAppendAdmission(request)
	if err != nil {
		return journal.AppendResult{}, err
	}
	defer admission.Close()
	if err := s.admitAppendRequestMetadata(request, admission); err != nil {
		return journal.AppendResult{}, err
	}
	if s.encoder == nil {
		return journal.AppendResult{}, fmt.Errorf("journal encoder is required")
	}
	lock := s.journalLock(request.Journal)
	if err := lock.lock(ctx); err != nil {
		return journal.AppendResult{}, err
	}
	defer lock.unlock()
	guard, err := s.acquireJournalMutation(ctx, request.Journal)
	if err != nil {
		return journal.AppendResult{}, err
	}

	transaction, session, err := s.openJournal(ctx, request.Journal, os.O_RDWR|os.O_APPEND)
	if err != nil {
		return journal.AppendResult{}, errors.Join(err, guard.release())
	}
	if err := guard.bind(ctx, transaction); err != nil {
		return journal.AppendResult{}, errors.Join(err, transaction.close(), guard.release())
	}
	result, operationErr := s.appendBatchLocked(ctx, transaction, session, request, admission)
	closeErr := transaction.close()
	resultErr := errors.Join(operationErr, closeErr, guard.release())
	if resultErr == nil && result.Status == journal.AppendCommitted {
		s.clearMarkerUncertainty(request.Journal, request.TransactionID)
	}
	return result, resultErr
}

func (s *Store) acquireAppendAdmission(request journal.AppendRequest) (*secret.Lease, error) {
	if s.secrets == nil {
		return nil, nil
	}
	generationID := request.Events[0].RuntimeGenerationID
	if generationID == "" {
		return nil, fmt.Errorf("runtime generation is required for secret admission")
	}
	for _, event := range request.Events[1:] {
		if event.RuntimeGenerationID != generationID {
			return nil, fmt.Errorf("one pinned runtime generation is required for the entire transaction")
		}
	}
	lease, err := s.secrets.AcquireExisting(generationID)
	if err != nil {
		return nil, fmt.Errorf("acquire secret admission lease: %w", err)
	}
	return lease, nil
}

func (s *Store) admitAppendRequestMetadata(request journal.AppendRequest, admission *secret.Lease) error {
	if admission == nil {
		return nil
	}
	type appendMetadata struct {
		Journal       protocol.JournalRef
		ExpectedHead  protocol.CommittedCursor
		TransactionID protocol.TransactionID
		Events        []protocol.ProposedEvent
	}
	metadata := appendMetadata{
		Journal: request.Journal, ExpectedHead: request.ExpectedHead,
		TransactionID: request.TransactionID, Events: make([]protocol.ProposedEvent, len(request.Events)),
	}
	for index, event := range request.Events {
		metadata.Events[index] = protocol.CloneProposedEvent(event)
		metadata.Events[index].Payload = nil
	}
	raw, err := canonicaljson.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("marshal proposed envelope metadata: %w", err)
	}
	if err := admission.Admit(raw); err != nil {
		return fmt.Errorf("admit proposed envelope metadata: %w", err)
	}
	return nil
}

func validateAppendRequest(request journal.AppendRequest) error {
	if err := request.Journal.Validate(); err != nil {
		return err
	}
	if zeroCursor(request.ExpectedHead) {
		if request.Journal.Kind != protocol.JournalWorkspaceControl {
			return fmt.Errorf("only a new workspace-control journal accepts an empty expected head")
		}
	} else {
		if err := request.ExpectedHead.Validate(); err != nil {
			return fmt.Errorf("invalid expected head: %w", err)
		}
		if request.ExpectedHead.JournalKind != request.Journal.Kind || request.ExpectedHead.JournalID != request.Journal.ID {
			return fmt.Errorf("expected head journal identity mismatch")
		}
	}
	if request.TransactionID == "" {
		return fmt.Errorf("transaction ID is required")
	}
	if len(request.Events) == 0 || len(request.Events) > 1000 {
		return fmt.Errorf("append event count must be between 1 and 1000 so every committed transaction is range-readable")
	}
	return nil
}

func (s *Store) appendBatchLocked(
	ctx context.Context,
	transaction *sessionTransaction,
	session domain.Session,
	request journal.AppendRequest,
	admission *secret.Lease,
) (journal.AppendResult, error) {
	return s.appendBatchLockedWithIdentity(ctx, transaction, session, request, appendGeneratedIdentity{}, admission)
}

type appendGeneratedIdentity struct {
	compatibilityEventID protocol.EventID
	compatibilityTime    time.Time
	markerEventID        protocol.EventID
	markerTime           time.Time
	admittedPayloads     map[protocol.EventID]json.RawMessage
	expectedBytes        []byte
}

func (s *Store) appendBatchLockedWithIdentity(
	ctx context.Context,
	transaction *sessionTransaction,
	session domain.Session,
	request journal.AppendRequest,
	identity appendGeneratedIdentity,
	admission *secret.Lease,
) (journal.AppendResult, error) {
	if s.journalHasMarkerUncertainty(request.Journal) {
		return journal.AppendResult{Status: journal.AppendCommitUnknown}, fmt.Errorf("journal has unresolved marker durability uncertainty")
	}
	scan, _, err := s.loadJournalScan(ctx, transaction, request.Journal)
	if err != nil {
		return journal.AppendResult{}, err
	}
	base := journal.AppendResult{CurrentHead: scan.head}
	if recoveryEligible(scan) {
		base.Status = journal.AppendRecoveryRequired
		return base, nil
	}
	if scan.incompleteTail || !scan.writable {
		return base, fmt.Errorf("journal is read-only because data beyond its validated prefix is not eligible for recovery")
	}
	if request.ExpectedHead != scan.head {
		base.Status = journal.AppendConflict
		return base, nil
	}
	for _, event := range request.Events {
		if event.Kind == protocol.EventMigrationCompatibilityDeclared {
			return base, fmt.Errorf("caller must not propose a compatibility declaration")
		}
	}
	if scan.hasLegacy && !scan.hasV2 {
		if request.Compatibility == nil {
			return base, fmt.Errorf("first v2 append to a legacy journal requires a compatibility declaration")
		}
		if request.Compatibility.ReaderVersion != protocol.EnvelopeVersion || request.Compatibility.WriterVersion != protocol.EnvelopeVersion || request.Compatibility.LegacyHead != scan.head {
			return base, fmt.Errorf("invalid legacy compatibility declaration")
		}
		if len(request.Events) >= 1000 {
			return base, fmt.Errorf("first v2 compatibility transaction supports at most 999 caller events")
		}
		compatibilityID := identity.compatibilityEventID
		if compatibilityID == "" {
			generated, err := s.nextID()
			if err != nil {
				return base, err
			}
			compatibilityID = protocol.EventID(generated)
		}
		compatibilityTime := identity.compatibilityTime.UTC()
		if identity.compatibilityTime.IsZero() {
			compatibilityTime = s.clock().UTC()
		}
		if compatibilityTime.Before(session.UpdatedAt) {
			compatibilityTime = session.UpdatedAt
		}
		payload, err := canonicaljson.Marshal(protocol.MigrationCompatibilityDeclaredV1{
			ReaderVersion: request.Compatibility.ReaderVersion, WriterVersion: request.Compatibility.WriterVersion,
			LegacyHead: request.Compatibility.LegacyHead, DowngradeStatus: "v0.1_read_only_after_v2",
		})
		if err != nil {
			return base, err
		}
		declaration := protocol.ProposedEvent{
			EventID: compatibilityID, Time: compatibilityTime, PayloadVersion: 1,
			Kind: protocol.EventMigrationCompatibilityDeclared, SessionID: protocol.SessionID(request.Journal.ID), Payload: payload,
			RuntimeGenerationID: request.Events[0].RuntimeGenerationID,
		}
		request.Events = append([]protocol.ProposedEvent{declaration}, request.Events...)
	} else if request.Compatibility != nil {
		return base, fmt.Errorf("compatibility declaration is invalid after the first v2 transaction")
	}
	if _, duplicate := scan.transactions[request.TransactionID]; duplicate {
		return base, fmt.Errorf("duplicate transaction ID %q", request.TransactionID)
	}
	if session.LastSeq > scan.head.CommitSeq {
		return base, fmt.Errorf("session metadata leads committed journal head: metadata=%d head=%d", session.LastSeq, scan.head.CommitSeq)
	}

	seen := make(map[protocol.EventID]struct{}, len(request.Events))
	events := make([]protocol.EventEnvelope, 0, len(request.Events))
	eventLines := bytes.NewBuffer(nil)
	for index, proposed := range request.Events {
		proposed = protocol.CloneProposedEvent(proposed)
		if proposed.Kind == protocol.EventTransactionCommitted {
			return base, fmt.Errorf("caller must not propose a transaction marker")
		}
		if proposed.EventID == "" {
			return base, fmt.Errorf("event ID is required")
		}
		if _, duplicate := seen[proposed.EventID]; duplicate {
			return base, fmt.Errorf("duplicate proposed event ID %q", proposed.EventID)
		}
		if _, duplicate := scan.eventIDs[proposed.EventID]; duplicate {
			return base, fmt.Errorf("duplicate durable event ID %q", proposed.EventID)
		}
		seen[proposed.EventID] = struct{}{}
		var admitted json.RawMessage
		if identity.admittedPayloads != nil {
			persisted, ok := identity.admittedPayloads[proposed.EventID]
			if !ok {
				return base, fmt.Errorf("event %q has no persisted admission payload", proposed.EventID)
			}
			admitted = protocol.CloneRawMessage(persisted)
		} else {
			if s.encoder == nil {
				return base, fmt.Errorf("journal encoder is required")
			}
			admitted, err = s.admitProposedWithLease(protocol.CloneProposedEvent(proposed), admission)
			if err != nil {
				return base, fmt.Errorf("encode proposed event %q: %w", proposed.EventID, err)
			}
		}
		canonicalPayload, err := canonicaljson.Marshal(admitted)
		if err != nil {
			return base, fmt.Errorf("canonicalize proposed event %q: %w", proposed.EventID, err)
		}
		envelope := protocol.EventEnvelope{
			SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: proposed.PayloadVersion,
			JournalKind: request.Journal.Kind, JournalID: request.Journal.ID,
			EventID: proposed.EventID, SessionID: proposed.SessionID,
			Seq: scan.physicalSeq + uint64(index) + 1, Time: proposed.Time,
			Kind: proposed.Kind, TaskID: proposed.TaskID, TurnID: proposed.TurnID,
			ActivityID: proposed.ActivityID, ParentActivityID: proposed.ParentActivityID,
			CausationEventID: proposed.CausationEventID, Actor: protocol.DeepCopy(proposed.Actor),
			RuntimeGenerationID: proposed.RuntimeGenerationID, TransactionID: request.TransactionID,
			Payload: protocol.CloneRawMessage(canonicalPayload),
		}
		line, err := encodeLine(envelope)
		if err != nil {
			return base, err
		}
		record, err := s.registry.Decode(protocol.CloneRawMessage(line[:len(line)-1]))
		if err != nil {
			return base, fmt.Errorf("decode proposed event %q: %w", proposed.EventID, err)
		}
		if err := s.registry.Validate(record); err != nil {
			return base, fmt.Errorf("validate proposed event %q: %w", proposed.EventID, err)
		}
		events = append(events, protocol.CloneEventEnvelope(envelope))
		_, _ = eventLines.Write(line)
	}
	digest, err := canonicaljson.TransactionDigest(events)
	if err != nil {
		return base, err
	}
	markerPayload, err := canonicaljson.Marshal(protocol.TransactionCommittedV1{
		TransactionID: request.TransactionID,
		FirstSeq:      events[0].Seq, LastSeq: events[len(events)-1].Seq,
		EventCount: uint32(len(events)), Digest: digest,
	})
	if err != nil {
		return base, err
	}
	generatedMarkerID := identity.markerEventID
	if generatedMarkerID == "" {
		markerID, err := s.nextID()
		if err != nil {
			return base, err
		}
		generatedMarkerID = protocol.EventID(markerID)
	}
	if _, duplicate := scan.eventIDs[generatedMarkerID]; duplicate {
		return base, fmt.Errorf("generated marker event ID %q duplicates a durable event", generatedMarkerID)
	}
	if _, duplicate := seen[generatedMarkerID]; duplicate {
		return base, fmt.Errorf("generated marker event ID %q duplicates a proposed event", generatedMarkerID)
	}
	markerTime := identity.markerTime.UTC()
	if identity.markerTime.IsZero() {
		markerTime = s.clock().UTC()
	}
	if markerTime.Before(events[len(events)-1].Time) {
		markerTime = events[len(events)-1].Time
	}
	if markerTime.Before(session.UpdatedAt) {
		markerTime = session.UpdatedAt
	}
	marker := protocol.EventEnvelope{
		SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1,
		JournalKind: request.Journal.Kind, JournalID: request.Journal.ID,
		EventID: generatedMarkerID, Seq: events[len(events)-1].Seq + 1,
		Time: markerTime, Kind: protocol.EventTransactionCommitted,
		RuntimeGenerationID: events[0].RuntimeGenerationID,
		TransactionID:       request.TransactionID, Payload: markerPayload,
	}
	if request.Journal.Kind == protocol.JournalSession {
		marker.SessionID = protocol.SessionID(request.Journal.ID)
	}
	markerLine, err := encodeLine(marker)
	if err != nil {
		return base, err
	}
	markerRecord, err := s.registry.Decode(protocol.CloneRawMessage(markerLine[:len(markerLine)-1]))
	if err != nil {
		return base, err
	}
	if err := s.registry.Validate(markerRecord); err != nil {
		return base, err
	}
	actual := make([]byte, 0, eventLines.Len()+len(markerLine))
	actual = append(actual, eventLines.Bytes()...)
	actual = append(actual, markerLine...)
	if identity.expectedBytes != nil {
		if !bytes.Equal(actual, identity.expectedBytes) {
			return base, fmt.Errorf("prepared append differs from persisted admitted transaction bytes")
		}
	}
	if admission != nil {
		if err := admission.Admit(actual); err != nil {
			return base, fmt.Errorf("admit complete generated transaction: %w", err)
		}
	}
	cursor := protocol.CommittedCursor{
		JournalKind: request.Journal.Kind, JournalID: request.Journal.ID,
		CommitSeq: marker.Seq, TransactionID: request.TransactionID,
	}

	if err := transaction.verifyEvents(); err != nil {
		return base, err
	}
	if err := writeFull(transaction.events, eventLines.Bytes()); err != nil {
		return appendFailure(journal.AppendRecoveryRequired, scan.head, err)
	}
	if err := s.injectFault(FaultEventWrite); err != nil {
		return appendFailure(journal.AppendRecoveryRequired, scan.head, err)
	}
	if err := transaction.events.Sync(); err != nil {
		return appendFailure(journal.AppendRecoveryRequired, scan.head, err)
	}
	if err := s.injectFault(FaultEventSync); err != nil {
		return appendFailure(journal.AppendRecoveryRequired, scan.head, err)
	}
	if err := transaction.verifyEvents(); err != nil {
		return appendFailure(journal.AppendRecoveryRequired, scan.head, err)
	}
	s.markMarkerUncertain(transaction, request.Journal, request.TransactionID)
	if err := writeFull(transaction.events, markerLine); err != nil {
		return appendFailure(journal.AppendCommitUnknown, scan.head, err)
	}
	if err := s.injectFault(FaultMarkerWrite); err != nil {
		return appendFailure(journal.AppendCommitUnknown, scan.head, err)
	}
	if err := transaction.events.Sync(); err != nil {
		return appendFailure(journal.AppendCommitUnknown, scan.head, err)
	}
	if err := s.injectFault(FaultMarkerSync); err != nil {
		return appendFailure(journal.AppendCommitUnknown, scan.head, err)
	}
	if err := transaction.verifyEvents(); err != nil {
		return appendFailure(journal.AppendCommitUnknown, scan.head, err)
	}

	session.LastSeq = cursor.CommitSeq
	session.UpdatedAt = marker.Time
	if err := s.injectFault(FaultMetadataWrite); err != nil {
		return appendFailure(journal.AppendCommitUnknown, scan.head, err)
	}
	metadata := any(session)
	if transaction.control {
		transaction.controlState.LastSeq = cursor.CommitSeq
		transaction.controlState.UpdatedAt = marker.Time
		metadata = transaction.controlState
	}
	if err := writeJSONAtomicRooted(ctx, transaction, metadata); err != nil {
		return appendFailure(journal.AppendCommitUnknown, scan.head, err)
	}
	if err := s.injectFault(FaultMetadataRename); err != nil {
		return appendFailure(journal.AppendCommitUnknown, scan.head, err)
	}
	committedScan, accelerated := s.loadVerifiedJournalScan(ctx, transaction, request.Journal, true)
	if !accelerated {
		committedScan, err = s.scanJournal(ctx, transaction, request.Journal)
		if err != nil {
			return appendFailure(journal.AppendCommitUnknown, scan.head, err)
		}
	}
	// The index is a disposable accelerator. A failure leaves the committed
	// marker authoritative and merely forces the next operation to rescan.
	_ = writeJournalIndex(ctx, transaction, committedScan)
	s.rememberVerifiedJournalScan(transaction, request.Journal, committedScan)
	if err := syncRootDir(transaction.sessionRoot, "."); err != nil {
		return appendFailure(journal.AppendCommitUnknown, scan.head, err)
	}
	if err := s.injectFault(FaultDirectorySync); err != nil {
		return appendFailure(journal.AppendCommitUnknown, scan.head, err)
	}
	if err := errors.Join(ctx.Err(), transaction.verifyEvents()); err != nil {
		return appendFailure(journal.AppendCommitUnknown, scan.head, err)
	}
	return journal.AppendResult{
		Status: journal.AppendCommitted, Cursor: cursor, CurrentHead: cursor,
		Events: cloneEnvelopes(events),
	}, nil
}

func appendFailure(status journal.AppendStatus, current protocol.CommittedCursor, err error) (journal.AppendResult, error) {
	return journal.AppendResult{Status: status, CurrentHead: current}, err
}

func writeFull(writer io.Writer, raw []byte) error {
	for len(raw) > 0 {
		written, err := writer.Write(raw)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrNoProgress
		}
		raw = raw[written:]
	}
	return nil
}

func encodeLine(value any) ([]byte, error) {
	raw, err := canonicaljson.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(raw) > protocol.MaxEventBytes {
		return nil, fmt.Errorf("journal event exceeds %d bytes", protocol.MaxEventBytes)
	}
	return append(raw, '\n'), nil
}
