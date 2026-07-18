package jsonl

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
)

var errJournalLockInitializationRequired = errors.New("journal lock initialization required")

type journalMutationGuard struct {
	store       *Store
	ref         protocol.JournalRef
	transaction *sessionTransaction
	lock        *rootedFlock
	lockSet     *openedLockSet
	fallback    bool
}

func (s *Store) acquireJournalMutation(ctx context.Context, ref protocol.JournalRef) (*journalMutationGuard, error) {
	transaction, _, err := s.openJournal(ctx, ref, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	fail := func(operationErr error) (*journalMutationGuard, error) {
		return nil, errors.Join(operationErr, transaction.close())
	}
	lockSet, lockErr := s.openValidatedLockSet(ctx, transaction, ref)
	if lockErr != nil {
		if _, stateErr := transaction.sessionRoot.Lstat(lockSetStateName); os.IsNotExist(stateErr) {
			lockErr = errors.Join(errJournalLockInitializationRequired, lockErr)
		}
		return fail(fmt.Errorf("journal is read-only because its durable lock set is unavailable: %w", lockErr))
	}
	lock, err := waitOpenedFlock(ctx, lockSet, journalLockName)
	if err != nil {
		return fail(errors.Join(err, lockSet.close()))
	}
	return &journalMutationGuard{store: s, ref: ref, transaction: transaction, lock: lock, lockSet: lockSet}, nil
}

func (s *Store) InitializeJournalLocks(ctx context.Context, ref protocol.JournalRef) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	lock := s.journalLock(ref)
	if err := lock.lock(ctx); err != nil {
		return err
	}
	defer lock.unlock()
	transaction, _, err := s.openJournal(ctx, ref, os.O_RDONLY)
	if err != nil {
		return err
	}
	legacyLock, err := waitLegacyFlock(ctx, transaction)
	if err != nil {
		return errors.Join(err, transaction.close())
	}
	scan, scanErr := s.scanJournal(ctx, transaction, ref)
	if scanErr == nil && scan.incompleteTail {
		scanErr = errJournalLockInitializationRequired
	}
	if scanErr == nil {
		scanErr = s.initializeLegacyLocks(ctx, transaction, ref)
	}
	return errors.Join(scanErr, legacyLock.release(), transaction.close())
}

func (s *Store) initializeLegacyLocks(ctx context.Context, transaction *sessionTransaction, ref protocol.JournalRef) error {
	if _, stateErr := transaction.sessionRoot.Lstat(lockSetStateName); stateErr == nil {
		existing, err := s.openValidatedLockSet(ctx, transaction, ref)
		if err != nil {
			return err
		}
		return existing.close()
	} else if !os.IsNotExist(stateErr) {
		return stateErr
	}
	journalInfo, journalErr := transaction.sessionRoot.Lstat(journalLockName)
	turnInfo, turnErr := transaction.sessionRoot.Lstat(turnLockName)
	if transaction.control {
		turnErr = os.ErrNotExist
	}
	if journalErr != nil && !os.IsNotExist(journalErr) {
		return journalErr
	}
	if turnErr != nil && !os.IsNotExist(turnErr) {
		return turnErr
	}
	if os.IsNotExist(journalErr) && turnErr == nil {
		return fmt.Errorf("partial lock initialization has turn.lock without journal.lock")
	}
	if journalErr == nil && (!journalInfo.Mode().IsRegular() || journalInfo.Mode()&os.ModeSymlink != 0 || journalInfo.Mode().Perm() != 0o600) {
		return fmt.Errorf("existing journal.lock is not a mode-0600 regular file")
	}
	if turnErr == nil && (!turnInfo.Mode().IsRegular() || turnInfo.Mode()&os.ModeSymlink != 0 || turnInfo.Mode().Perm() != 0o600) {
		return fmt.Errorf("existing turn.lock is not a mode-0600 regular file")
	}
	if err := ensureDurableRootedFile(ctx, transaction.sessionRoot, journalLockName, nil); err != nil {
		return err
	}
	if err := s.injectFault(FaultLockInitializationAfterJournalSync); err != nil {
		return err
	}
	if !transaction.control {
		if err := ensureDurableRootedFile(ctx, transaction.sessionRoot, turnLockName, nil); err != nil {
			return err
		}
	}
	if err := writeDurableLockSetState(ctx, transaction.sessionRoot, ref); err != nil {
		if existing, openErr := s.openValidatedLockSet(ctx, transaction, ref); openErr == nil {
			return existing.close()
		}
		return err
	}
	return syncRootDir(transaction.sessionRoot, ".")
}

func (s *Store) acquireLegacyRecoveryMutation(ctx context.Context, ref protocol.JournalRef) (*journalMutationGuard, error) {
	transaction, _, err := s.openJournal(ctx, ref, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	lock, err := waitLegacyFlock(ctx, transaction)
	if err != nil {
		return nil, errors.Join(err, transaction.close())
	}
	return &journalMutationGuard{store: s, ref: ref, transaction: transaction, lock: lock, fallback: true}, nil
}

func (g *journalMutationGuard) ensureInitialized(ctx context.Context) error {
	if !g.fallback {
		return g.lock.verify()
	}
	if err := g.store.initializeLegacyLocks(ctx, g.transaction, g.ref); err != nil {
		return err
	}
	lockSet, err := g.store.openValidatedLockSet(ctx, g.transaction, g.ref)
	if err != nil {
		return err
	}
	journalLock, err := waitOpenedFlock(ctx, lockSet, journalLockName)
	if err != nil {
		return errors.Join(err, lockSet.close())
	}
	if err := g.lock.release(); err != nil {
		return errors.Join(err, journalLock.release(), lockSet.close())
	}
	g.lock = journalLock
	g.lockSet = lockSet
	g.fallback = false
	return nil
}

func (g *journalMutationGuard) bind(ctx context.Context, transaction *sessionTransaction) error {
	if err := g.lock.verify(); err != nil {
		return err
	}
	if g.lockSet == nil {
		return fmt.Errorf("durable lock set is not held")
	}
	if err := g.lockSet.verify(); err != nil {
		return err
	}
	if !os.SameFile(g.lockSet.journalInfo, g.lock.info) {
		return fmt.Errorf("opened mutation transaction uses a substituted journal lock")
	}
	g.lockSet.bind(transaction)
	return transaction.verifyEvents()
}

func (g *journalMutationGuard) release() error {
	if g == nil {
		return nil
	}
	return errors.Join(g.lock.release(), g.lockSet.close(), g.transaction.close())
}

type turnLease struct {
	store       *Store
	sessionID   protocol.SessionID
	turnID      protocol.TurnID
	transaction *sessionTransaction
	lock        *rootedFlock
	lockSet     *openedLockSet
	mu          sync.Mutex
	released    bool
}

func (l *turnLease) SessionID() protocol.SessionID { return l.sessionID }
func (l *turnLease) TurnID() protocol.TurnID       { return l.turnID }

func (s *Store) AcquireTurnLease(ctx context.Context, sessionID protocol.SessionID, turnID protocol.TurnID, expected protocol.CommittedCursor) (journal.TurnLease, error) {
	return s.acquireTurnLease(ctx, sessionID, turnID, expected, false)
}

func (s *Store) AcquireTurnRecoveryLease(ctx context.Context, sessionID protocol.SessionID, turnID protocol.TurnID, expected protocol.CommittedCursor) (journal.TurnLease, error) {
	return s.acquireTurnLease(ctx, sessionID, turnID, expected, true)
}

func (s *Store) acquireTurnLease(ctx context.Context, sessionID protocol.SessionID, turnID protocol.TurnID, expected protocol.CommittedCursor, recovery bool) (journal.TurnLease, error) {
	if err := validateSessionID(string(sessionID)); err != nil {
		return nil, err
	}
	if turnID == "" {
		return nil, fmt.Errorf("turn ID is required")
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(sessionID)}
	if err := expected.Validate(); err != nil || expected.JournalKind != ref.Kind || expected.JournalID != ref.ID {
		return nil, fmt.Errorf("invalid turn lease expected head")
	}
	transaction, _, err := s.openJournal(ctx, ref, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	fail := func(operationErr error, lock *rootedFlock) (journal.TurnLease, error) {
		if lock != nil {
			operationErr = errors.Join(operationErr, lock.release())
		}
		return nil, errors.Join(operationErr, transaction.close())
	}
	lockSet, err := s.openValidatedLockSet(ctx, transaction, ref)
	if err != nil {
		return fail(fmt.Errorf("turn lease denied because durable lock set is unavailable: %w", err), nil)
	}
	lock, err := tryOpenedFlock(ctx, lockSet, turnLockName, journal.ErrTurnLeaseHeld)
	if err != nil {
		return fail(errors.Join(err, lockSet.close()), nil)
	}
	lockSet.bind(transaction)
	active, terminal, err := s.activeTurnInTransaction(ctx, transaction, ref, expected)
	if err != nil {
		return fail(errors.Join(err, lockSet.close()), lock)
	}
	if recovery {
		if active == "" || active != turnID || terminal {
			return fail(errors.Join(journal.ErrTurnRecoveryRequired, lockSet.close()), lock)
		}
	} else if active != "" && !terminal {
		return fail(errors.Join(journal.ErrTurnRecoveryRequired, lockSet.close()), lock)
	}
	lease := &turnLease{store: s, sessionID: sessionID, turnID: turnID, transaction: transaction, lock: lock, lockSet: lockSet}
	if done := ctx.Done(); done != nil {
		go func() {
			<-done
			_ = lease.abandon()
		}()
	}
	return lease, nil
}

func (s *Store) ActiveTurn(ctx context.Context, sessionID protocol.SessionID, at protocol.CommittedCursor) (protocol.TurnID, bool, error) {
	if err := validateSessionID(string(sessionID)); err != nil {
		return "", false, err
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(sessionID)}
	transaction, _, err := s.openJournal(ctx, ref, os.O_RDONLY)
	if err != nil {
		return "", false, err
	}
	lockSet, lockErr := s.openValidatedLockSet(ctx, transaction, ref)
	if lockErr != nil {
		return "", false, errors.Join(lockErr, transaction.close())
	}
	lockSet.bind(transaction)
	turnID, terminal, operationErr := s.activeTurnInTransaction(ctx, transaction, ref, at)
	return turnID, terminal, errors.Join(operationErr, lockSet.close(), transaction.close())
}

func (s *Store) activeTurnInTransaction(ctx context.Context, transaction *sessionTransaction, ref protocol.JournalRef, at protocol.CommittedCursor) (protocol.TurnID, bool, error) {
	scan, err := s.scanJournal(ctx, transaction, ref)
	if err != nil {
		return "", false, err
	}
	if scan.head != at || !scanHasCursor(scan, at) {
		return "", false, journal.ErrTurnHeadConflict
	}
	records, _ := upcastScannedEvents(scan, protocol.SessionID(ref.ID))
	var active protocol.TurnID
	terminal := true
	for _, record := range records {
		turnID := record.Envelope.TurnID
		switch record.Envelope.Kind {
		case protocol.EventTurnAccepted:
			active, terminal = turnID, false
		case protocol.EventTurnStateChanged:
			if turnID == active {
				if payload, ok := record.Decoded.(*protocol.StateChangedV1); ok {
					switch protocol.TurnState(payload.To) {
					case protocol.TurnCompleted, protocol.TurnFailed, protocol.TurnInterrupted:
						terminal = true
					default:
						terminal = false
					}
				}
			}
		case protocol.EventTurnCompleted, protocol.EventTurnFailed, protocol.EventTurnInterrupted:
			if turnID == active {
				terminal = true
			}
		}
	}
	return active, terminal, nil
}

func (l *turnLease) Release(ctx context.Context, cursor protocol.CommittedCursor) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return nil
	}
	if err := l.lock.verify(); err != nil {
		return err
	}
	if err := l.lockSet.verify(); err != nil {
		return err
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(l.sessionID)}
	transaction, _, err := l.store.openJournal(ctx, ref, os.O_RDONLY)
	if err != nil {
		return err
	}
	l.lockSet.bind(transaction)
	terminal, operationErr := l.store.transactionTerminalizesTurn(ctx, transaction, ref, cursor, l.turnID)
	err = errors.Join(operationErr, transaction.close())
	if err != nil {
		return err
	}
	if !terminal {
		return journal.ErrTurnNotTerminal
	}
	return l.releaseLocked()
}

func (s *Store) transactionTerminalizesTurn(
	ctx context.Context,
	transaction *sessionTransaction,
	ref protocol.JournalRef,
	cursor protocol.CommittedCursor,
	turnID protocol.TurnID,
) (bool, error) {
	if err := cursor.Validate(); err != nil || cursor.JournalKind != ref.Kind || cursor.JournalID != ref.ID {
		return false, nil
	}
	scan, err := s.scanJournal(ctx, transaction, ref)
	if err != nil {
		return false, err
	}
	for _, commit := range scan.commits {
		if commit.cursor != cursor {
			continue
		}
		for _, record := range commit.events {
			if record.Envelope.TurnID != turnID {
				continue
			}
			switch record.Envelope.Kind {
			case protocol.EventTurnCompleted, protocol.EventTurnFailed, protocol.EventTurnInterrupted:
				return true, nil
			case protocol.EventTurnStateChanged:
				payload, ok := record.Decoded.(*protocol.StateChangedV1)
				if !ok {
					continue
				}
				switch protocol.TurnState(payload.To) {
				case protocol.TurnCompleted, protocol.TurnFailed, protocol.TurnInterrupted:
					return true, nil
				}
			}
		}
		return false, nil
	}
	return false, nil
}

func (l *turnLease) abandon() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return nil
	}
	return l.releaseLocked()
}

func (l *turnLease) releaseLocked() error {
	l.released = true
	return errors.Join(l.lock.release(), l.lockSet.close(), l.transaction.close())
}
