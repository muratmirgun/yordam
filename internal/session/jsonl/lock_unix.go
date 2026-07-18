//go:build darwin || linux

package jsonl

import (
	"context"
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
	journalLockName = "journal.lock"
	turnLockName    = "turn.lock"
)

type rootedFlock struct {
	file *os.File
	info os.FileInfo
	root *os.Root
	name string
}

func (s *Store) rememberLockIdentity(ref protocol.JournalRef, name string, info os.FileInfo) error {
	key := journalLockKey(ref) + "\x00" + name
	loaded, exists := s.state.lockIdentities.LoadOrStore(key, info)
	if exists {
		known, ok := loaded.(os.FileInfo)
		if !ok || known == nil || !os.SameFile(known, info) {
			return fmt.Errorf("%s inode was substituted", name)
		}
	}
	return nil
}

func (s *Store) openStableLock(ctx context.Context, transaction *sessionTransaction, ref protocol.JournalRef, name string) (*os.File, os.FileInfo, error) {
	file, info, err := openRootedRegularFile(ctx, transaction.sessionRoot, name, os.O_RDWR, 0)
	if err != nil {
		return nil, nil, err
	}
	if info.Mode().Perm() != 0o600 {
		return nil, nil, errors.Join(fmt.Errorf("%s mode is %04o, want 0600", name, info.Mode().Perm()), file.Close())
	}
	if err := errors.Join(transaction.verifySession(), verifyRootedRegularFile(transaction.sessionRoot, name, info), s.rememberLockIdentity(ref, name, info)); err != nil {
		return nil, nil, errors.Join(err, file.Close())
	}
	return file, info, nil
}

func (s *Store) tryFlock(ctx context.Context, transaction *sessionTransaction, ref protocol.JournalRef, name string, held error) (*rootedFlock, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, info, err := s.openStableLock(ctx, transaction, ref, name)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		closeErr := file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, errors.Join(held, closeErr)
		}
		return nil, errors.Join(err, closeErr)
	}
	lock := &rootedFlock{file: file, info: info, root: transaction.sessionRoot, name: name}
	if err := errors.Join(transaction.verifySession(), verifyRootedRegularFile(transaction.sessionRoot, name, info), s.rememberLockIdentity(ref, name, info)); err != nil {
		return nil, errors.Join(err, lock.release())
	}
	return lock, nil
}

func (s *Store) waitFlock(ctx context.Context, transaction *sessionTransaction, ref protocol.JournalRef, name string) (*rootedFlock, error) {
	for {
		lock, err := s.tryFlock(ctx, transaction, ref, name, journal.ErrTurnLeaseHeld)
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
