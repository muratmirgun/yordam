package jsonl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
)

// WriteLegacyFixture retains the v0.1 writer only inside the jsonl test
// binary. Recovery and migration tests use it to construct legacy journals;
// production code cannot link this API.
func (s *Store) WriteLegacyFixture(ctx context.Context, sessionID string, kind domain.EventKind, payload any) (domain.DurableEvent, error) {
	if err := validateSessionID(sessionID); err != nil {
		return domain.DurableEvent{}, err
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(sessionID)}
	lock := s.journalLock(ref)
	if err := lock.lock(ctx); err != nil {
		return domain.DurableEvent{}, err
	}
	defer lock.unlock()
	guard, err := s.acquireJournalMutation(ctx, ref)
	if err != nil {
		return domain.DurableEvent{}, err
	}
	event, appendErr := s.appendLegacyFixtureLocked(ctx, domain.Session{ID: sessionID}, kind, payload, guard)
	return event, errors.Join(appendErr, guard.release())
}

func (s *Store) appendLegacyFixtureLocked(ctx context.Context, session domain.Session, kind domain.EventKind, payload any, guard *journalMutationGuard) (domain.DurableEvent, error) {
	transaction, durableSession, err := s.openSessionTransaction(ctx, session.ID, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		return domain.DurableEvent{}, err
	}
	if err := guard.bind(ctx, transaction); err != nil {
		return domain.DurableEvent{}, errors.Join(err, transaction.close())
	}
	event, operationErr := s.appendLegacyFixtureInTransaction(ctx, transaction, durableSession, kind, payload)
	return event, errors.Join(operationErr, transaction.close())
}

func (s *Store) appendLegacyFixtureInTransaction(ctx context.Context, transaction *sessionTransaction, session domain.Session, kind domain.EventKind, payload any) (domain.DurableEvent, error) {
	raw, err := s.sanitizeLegacyFixture(payload)
	if err != nil {
		return domain.DurableEvent{}, err
	}
	id, err := s.nextID()
	if err != nil {
		return domain.DurableEvent{}, err
	}
	event := domain.DurableEvent{SchemaVersion: 1, EventID: id, SessionID: session.ID, Seq: session.LastSeq + 1, Time: s.clock().UTC(), Kind: kind, Payload: raw}
	if err := event.Validate(); err != nil {
		return domain.DurableEvent{}, err
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return domain.DurableEvent{}, err
	}
	if len(encoded) > maxEventSize {
		return domain.DurableEvent{}, fmt.Errorf("session event exceeds 2 MiB")
	}
	summary, err := validateEventLog(ctx, transaction.events, session.ID, true)
	if err != nil {
		return domain.DurableEvent{}, err
	}
	if summary.lastSeq != session.LastSeq {
		return domain.DurableEvent{}, fmt.Errorf("session sequence mismatch: metadata=%d log=%d", session.LastSeq, summary.lastSeq)
	}
	if err := transaction.verifyEvents(); err != nil {
		return domain.DurableEvent{}, err
	}
	_, err = transaction.events.Write(append(encoded, '\n'))
	if err == nil {
		err = transaction.events.Sync()
	}
	if err == nil {
		err = transaction.verifyEvents()
	}
	if err != nil {
		return domain.DurableEvent{}, err
	}
	session.LastSeq, session.UpdatedAt = event.Seq, event.Time
	if err := writeJSONAtomicRooted(ctx, transaction, session); err != nil {
		return domain.DurableEvent{}, err
	}
	return event, nil
}

func (s *Store) sanitizeLegacyFixture(value any) (json.RawMessage, error) {
	raw, err := s.sanitize(value)
	if err != nil || !s.requireAdmission {
		return raw, err
	}
	lease, err := s.admission.AcquireLease()
	if err != nil {
		return nil, fmt.Errorf("acquire journal admission: %w", err)
	}
	defer lease.Close()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var tree any
	if err := decoder.Decode(&tree); err != nil {
		return nil, err
	}
	return lease.JSON(tree)
}
