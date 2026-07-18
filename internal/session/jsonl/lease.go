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
	file, info, lockErr := s.openStableLock(ctx, transaction, ref, journalLockName)
	if lockErr == nil {
		lockErr = errors.Join(verifyRootedRegularFile(transaction.sessionRoot, journalLockName, info), file.Close())
	} else if os.IsNotExist(lockErr) {
		if turnFile, _, turnErr := openRootedRegularFile(ctx, transaction.sessionRoot, turnLockName, os.O_RDONLY, 0); turnErr == nil {
			return fail(errors.Join(fmt.Errorf("journal is read-only because its initialized lock is missing"), turnFile.Close()))
		} else if !os.IsNotExist(turnErr) {
			return fail(fmt.Errorf("journal is read-only because its lock set is invalid: %w", turnErr))
		}
		scan, scanErr := s.scanJournal(ctx, transaction, ref)
		if scanErr != nil {
			return fail(scanErr)
		}
		if scan.incompleteTail {
			return fail(errJournalLockInitializationRequired)
		}
		lockErr = s.initializeLegacyLocks(ctx, transaction)
	}
	if lockErr != nil {
		return fail(fmt.Errorf("journal is read-only because its durable lock is unavailable: %w", lockErr))
	}
	lock, err := s.waitFlock(ctx, transaction, ref, journalLockName)
	if err != nil {
		return fail(err)
	}
	return &journalMutationGuard{store: s, ref: ref, transaction: transaction, lock: lock}, nil
}

func (s *Store) initializeLegacyLocks(ctx context.Context, transaction *sessionTransaction) error {
	if err := ensureDurableRootedFile(ctx, transaction.sessionRoot, journalLockName, nil); err != nil {
		return err
	}
	if !transaction.control {
		if err := ensureDurableRootedFile(ctx, transaction.sessionRoot, turnLockName, nil); err != nil {
			return err
		}
	}
	return syncRootDir(transaction.sessionRoot, ".")
}

func (s *Store) acquireLegacyRecoveryMutation(ctx context.Context, ref protocol.JournalRef) (*journalMutationGuard, error) {
	transaction, _, err := s.openJournal(ctx, ref, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	lock, err := s.waitFlock(ctx, transaction, ref, "events.jsonl")
	if err != nil {
		return nil, errors.Join(err, transaction.close())
	}
	return &journalMutationGuard{store: s, ref: ref, transaction: transaction, lock: lock, fallback: true}, nil
}

func (g *journalMutationGuard) ensureInitialized(ctx context.Context) error {
	if !g.fallback {
		return g.lock.verify()
	}
	if err := g.store.initializeLegacyLocks(ctx, g.transaction); err != nil {
		return err
	}
	journalLock, err := g.store.waitFlock(ctx, g.transaction, g.ref, journalLockName)
	if err != nil {
		return err
	}
	if err := g.lock.release(); err != nil {
		return errors.Join(err, journalLock.release())
	}
	g.lock = journalLock
	g.fallback = false
	return nil
}

func (g *journalMutationGuard) bind(ctx context.Context, transaction *sessionTransaction) error {
	if err := g.lock.verify(); err != nil {
		return err
	}
	if transaction.journalLock != nil {
		if !os.SameFile(transaction.journalLockInfo, g.lock.info) {
			return fmt.Errorf("opened mutation transaction uses a substituted journal lock")
		}
		return transaction.verifyEvents()
	}
	file, info, err := g.store.openStableLock(ctx, transaction, g.ref, journalLockName)
	if err != nil {
		return err
	}
	transaction.journalLock = file
	transaction.journalLockInfo = info
	return transaction.verifyEvents()
}

func (g *journalMutationGuard) release() error {
	if g == nil {
		return nil
	}
	return errors.Join(g.lock.release(), g.transaction.close())
}

type turnLease struct {
	store       *Store
	sessionID   protocol.SessionID
	turnID      protocol.TurnID
	transaction *sessionTransaction
	lock        *rootedFlock
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
	lock, err := s.tryFlock(ctx, transaction, ref, turnLockName, journal.ErrTurnLeaseHeld)
	if os.IsNotExist(err) {
		_, journalErr := transaction.sessionRoot.Lstat(journalLockName)
		if os.IsNotExist(journalErr) {
			if closeErr := transaction.close(); closeErr != nil {
				return nil, errors.Join(err, closeErr)
			}
			guard, initializationErr := s.acquireLegacyRecoveryMutation(ctx, ref)
			if initializationErr == nil {
				initializationErr = guard.ensureInitialized(ctx)
			}
			if guard != nil {
				initializationErr = errors.Join(initializationErr, guard.release())
			}
			if initializationErr != nil {
				return nil, initializationErr
			}
			return s.acquireTurnLease(ctx, sessionID, turnID, expected, recovery)
		}
	}
	if err != nil {
		return fail(err, nil)
	}
	active, terminal, err := s.activeTurnInTransaction(ctx, transaction, ref, expected)
	if err != nil {
		return fail(err, lock)
	}
	if recovery {
		if active == "" || active != turnID || terminal {
			return fail(journal.ErrTurnRecoveryRequired, lock)
		}
	} else if active != "" && !terminal {
		return fail(journal.ErrTurnRecoveryRequired, lock)
	}
	lease := &turnLease{store: s, sessionID: sessionID, turnID: turnID, transaction: transaction, lock: lock}
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
	turnID, terminal, operationErr := s.activeTurnInTransaction(ctx, transaction, ref, at)
	return turnID, terminal, errors.Join(operationErr, transaction.close())
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
	turnID, terminal, err := l.store.ActiveTurn(ctx, l.sessionID, cursor)
	if err != nil {
		return err
	}
	if turnID != l.turnID || !terminal {
		return journal.ErrTurnNotTerminal
	}
	return l.releaseLocked()
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
	return errors.Join(l.lock.release(), l.transaction.close())
}
