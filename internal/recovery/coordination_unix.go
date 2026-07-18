//go:build darwin || linux

package recovery

import (
	"context"
	"errors"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

const recoveryCoordinationName = ".recovery.lock"

type recoveryCoordination struct {
	file *os.File
}

func acquireRecoveryCoordination(ctx context.Context, root *os.Root) (*recoveryCoordination, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	created := false
	if info, err := root.Lstat(recoveryCoordinationName); err == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return nil, ErrUnsafePath
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	} else {
		created = true
	}
	file, err := root.OpenFile(recoveryCoordinationName, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	cleanup := func(operationErr error) (*recoveryCoordination, error) {
		return nil, errors.Join(operationErr, file.Close())
	}
	info, err := root.Lstat(recoveryCoordinationName)
	opened, openErr := file.Stat()
	if err != nil || openErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || !os.SameFile(info, opened) {
		return cleanup(errors.Join(ErrUnsafePath, err, openErr))
	}
	if err := file.Chmod(0o600); err != nil {
		return cleanup(err)
	}
	if err := file.Sync(); err != nil {
		return cleanup(err)
	}
	if created {
		if err := syncRootDir(root, "."); err != nil {
			return cleanup(err)
		}
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return &recoveryCoordination{file: file}, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			return cleanup(err)
		}
		select {
		case <-ctx.Done():
			return cleanup(errors.Join(ErrRecoveryBusy, ctx.Err()))
		case <-ticker.C:
		}
	}
}

func (c *recoveryCoordination) release() error {
	if c == nil || c.file == nil {
		return nil
	}
	err := unix.Flock(int(c.file.Fd()), unix.LOCK_UN)
	return errors.Join(err, c.file.Close())
}
