//go:build darwin || linux

package jsonl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"golang.org/x/sys/unix"
)

const (
	journalLockName                     = "journal.lock"
	turnLockName                        = "turn.lock"
	lockSetStateName                    = "lock-set.json"
	workspaceCoordinationLockName       = "workspace.lock"
	maxLockSetStateBytes          int64 = 16 << 10
)

type durableLockIdentity struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
}

type durableLockSetState struct {
	Version     uint32               `json:"version"`
	Journal     protocol.JournalRef  `json:"journal"`
	JournalLock durableLockIdentity  `json:"journal_lock"`
	TurnLock    *durableLockIdentity `json:"turn_lock,omitempty"`
}

type openedLockSet struct {
	root        *os.Root
	control     bool
	stateFile   *os.File
	stateInfo   os.FileInfo
	journalFile *os.File
	journalInfo os.FileInfo
	turnFile    *os.File
	turnInfo    os.FileInfo
}

type rootedFlock struct {
	file *os.File
	info os.FileInfo
	root *os.Root
	name string
}

func waitWorkspaceCoordination(ctx context.Context, root *os.Root) (*rootedFlock, error) {
	if err := ensureDurableRootedFile(ctx, root, workspaceCoordinationLockName, nil); err != nil {
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		file, info, err := openRootedRegularFile(ctx, root, workspaceCoordinationLockName, os.O_RDWR, 0)
		if err != nil {
			return nil, err
		}
		if info.Mode().Perm() != 0o600 {
			return nil, errors.Join(fmt.Errorf("%s mode is %04o, want 0600", workspaceCoordinationLockName, info.Mode().Perm()), file.Close())
		}
		if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err == nil {
			lock := &rootedFlock{file: file, info: info, root: root, name: workspaceCoordinationLockName}
			if err := lock.verify(); err != nil {
				return nil, errors.Join(err, lock.release())
			}
			return lock, nil
		} else {
			closeErr := file.Close()
			if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
				return nil, errors.Join(err, closeErr)
			}
			if closeErr != nil {
				return nil, closeErr
			}
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func lockIdentity(info os.FileInfo) (durableLockIdentity, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return durableLockIdentity{}, fmt.Errorf("lock identity is unavailable")
	}
	return durableLockIdentity{Device: uint64(stat.Dev), Inode: uint64(stat.Ino)}, nil
}

func openLockMember(ctx context.Context, root *os.Root, name string) (*os.File, os.FileInfo, durableLockIdentity, error) {
	file, info, err := openRootedRegularFile(ctx, root, name, os.O_RDWR, 0)
	if err != nil {
		return nil, nil, durableLockIdentity{}, err
	}
	if info.Mode().Perm() != 0o600 {
		return nil, nil, durableLockIdentity{}, errors.Join(fmt.Errorf("%s mode is %04o, want 0600", name, info.Mode().Perm()), file.Close())
	}
	identity, err := lockIdentity(info)
	if err != nil {
		return nil, nil, durableLockIdentity{}, errors.Join(err, file.Close())
	}
	return file, info, identity, nil
}

func writeDurableLockSetState(ctx context.Context, root *os.Root, ref protocol.JournalRef) error {
	journalFile, _, journalIdentity, err := openLockMember(ctx, root, journalLockName)
	if err != nil {
		return err
	}
	defer journalFile.Close()
	state := durableLockSetState{Version: 1, Journal: ref, JournalLock: journalIdentity}
	if ref.Kind == protocol.JournalSession {
		turnFile, _, turnIdentity, err := openLockMember(ctx, root, turnLockName)
		if err != nil {
			return err
		}
		state.TurnLock = &turnIdentity
		defer turnFile.Close()
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return writeStagedFile(ctx, root, lockSetStateName, raw)
}

func (s *Store) openValidatedLockSet(ctx context.Context, transaction *sessionTransaction, ref protocol.JournalRef) (*openedLockSet, error) {
	set, err := openValidatedLockSetAtRoot(ctx, transaction.sessionRoot, transaction.control, ref)
	if err != nil {
		return nil, err
	}
	if err := transaction.verifySession(); err != nil {
		return nil, errors.Join(err, set.close())
	}
	return set, nil
}

func openValidatedLockSetAtRoot(ctx context.Context, root *os.Root, control bool, ref protocol.JournalRef) (*openedLockSet, error) {
	set := &openedLockSet{root: root, control: control}
	fail := func(operationErr error) (*openedLockSet, error) {
		return nil, errors.Join(operationErr, set.close())
	}
	var err error
	set.stateFile, set.stateInfo, err = openRootedRegularFile(ctx, root, lockSetStateName, os.O_RDONLY, 0)
	if err != nil {
		return fail(err)
	}
	if set.stateInfo.Mode().Perm() != 0o600 {
		return fail(fmt.Errorf("%s mode is %04o, want 0600", lockSetStateName, set.stateInfo.Mode().Perm()))
	}
	raw, err := readOpenedFile(ctx, set.stateFile, maxLockSetStateBytes)
	if err != nil {
		return fail(err)
	}
	var state durableLockSetState
	if err := json.Unmarshal(raw, &state); err != nil {
		return fail(fmt.Errorf("decode durable lock set: %w", err))
	}
	if state.Version != 1 || state.Journal != ref {
		return fail(fmt.Errorf("durable lock set journal identity mismatch"))
	}
	wantTurn := ref.Kind == protocol.JournalSession
	if wantTurn != (state.TurnLock != nil) {
		return fail(fmt.Errorf("durable lock set membership mismatch"))
	}
	var journalIdentity durableLockIdentity
	set.journalFile, set.journalInfo, journalIdentity, err = openLockMember(ctx, root, journalLockName)
	if err != nil {
		return fail(err)
	}
	if journalIdentity != state.JournalLock {
		return fail(fmt.Errorf("%s identity does not match durable lock set", journalLockName))
	}
	if wantTurn {
		var turnIdentity durableLockIdentity
		set.turnFile, set.turnInfo, turnIdentity, err = openLockMember(ctx, root, turnLockName)
		if err != nil {
			return fail(err)
		}
		if turnIdentity != *state.TurnLock {
			return fail(fmt.Errorf("%s identity does not match durable lock set", turnLockName))
		}
	}
	if err := set.verify(); err != nil {
		return fail(err)
	}
	return set, nil
}

func (s *openedLockSet) bind(transaction *sessionTransaction) {
	transaction.lockSetBound = true
	transaction.lockSetInfo = s.stateInfo
	transaction.journalLockInfo = s.journalInfo
	transaction.turnLockInfo = s.turnInfo
}

func (s *openedLockSet) verify() error {
	if s == nil || s.stateFile == nil || s.journalInfo == nil {
		return fmt.Errorf("durable lock set is not open")
	}
	err := errors.Join(
		verifyRootedRegularFile(s.root, lockSetStateName, s.stateInfo),
		verifyRootedRegularFile(s.root, journalLockName, s.journalInfo),
	)
	if !s.control {
		err = errors.Join(err, verifyRootedRegularFile(s.root, turnLockName, s.turnInfo))
	}
	return err
}

func (s *openedLockSet) close() error {
	if s == nil {
		return nil
	}
	var closeErrs []error
	if s.turnFile != nil {
		closeErrs = append(closeErrs, s.turnFile.Close())
		s.turnFile = nil
	}
	if s.journalFile != nil {
		closeErrs = append(closeErrs, s.journalFile.Close())
		s.journalFile = nil
	}
	if s.stateFile != nil {
		closeErrs = append(closeErrs, s.stateFile.Close())
		s.stateFile = nil
	}
	return errors.Join(closeErrs...)
}

func tryOpenedFlock(ctx context.Context, set *openedLockSet, name string, held error) (*rootedFlock, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var file *os.File
	var info os.FileInfo
	switch name {
	case journalLockName:
		file, info = set.journalFile, set.journalInfo
	case turnLockName:
		file, info = set.turnFile, set.turnInfo
	default:
		return nil, fmt.Errorf("unsupported lock-set member %q", name)
	}
	if file == nil || info == nil {
		return nil, fmt.Errorf("lock-set member %q is not open", name)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, held
		}
		return nil, err
	}
	if name == journalLockName {
		set.journalFile = nil
	} else {
		set.turnFile = nil
	}
	lock := &rootedFlock{file: file, info: info, root: set.root, name: name}
	if err := errors.Join(lock.verify(), set.verify()); err != nil {
		return nil, errors.Join(err, lock.release())
	}
	return lock, nil
}

func waitOpenedFlock(ctx context.Context, set *openedLockSet, name string) (*rootedFlock, error) {
	for {
		lock, err := tryOpenedFlock(ctx, set, name, journal.ErrTurnLeaseHeld)
		if !errors.Is(err, journal.ErrTurnLeaseHeld) {
			return lock, err
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func waitLegacyFlock(ctx context.Context, transaction *sessionTransaction) (*rootedFlock, error) {
	file, info, err := openRootedRegularFile(ctx, transaction.sessionRoot, "events.jsonl", os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, file.Close())
		}
		if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err == nil {
			return &rootedFlock{file: file, info: info, root: transaction.sessionRoot, name: "events.jsonl"}, nil
		} else if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return nil, errors.Join(err, file.Close())
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, errors.Join(ctx.Err(), file.Close())
		case <-timer.C:
		}
	}
}

func (l *rootedFlock) verify() error {
	if l == nil || l.file == nil {
		return fmt.Errorf("lock is not held")
	}
	return verifyRootedRegularFile(l.root, l.name, l.info)
}

func (l *rootedFlock) release() error {
	if l == nil || l.file == nil {
		return nil
	}
	verifyErr := l.verify()
	unlockErr := unix.Flock(int(l.file.Fd()), unix.LOCK_UN)
	closeErr := l.file.Close()
	l.file = nil
	return errors.Join(verifyErr, unlockErr, closeErr)
}
