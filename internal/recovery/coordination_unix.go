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
	root       *os.Root
	directory  *os.File
	marker     *os.File
	markerInfo os.FileInfo
}

func acquireRecoveryCoordination(ctx context.Context, root *os.Root) (*recoveryCoordination, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	retainedRoot, err := root.OpenRoot(".")
	if err != nil {
		return nil, err
	}
	closeRoot := func(operationErr error) (*recoveryCoordination, error) {
		return nil, errors.Join(operationErr, retainedRoot.Close())
	}
	created := false
	if info, err := retainedRoot.Lstat(recoveryCoordinationName); err == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return closeRoot(ErrUnsafePath)
		}
	} else if !os.IsNotExist(err) {
		return closeRoot(err)
	} else {
		created = true
	}
	marker, err := retainedRoot.OpenFile(recoveryCoordinationName, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return closeRoot(err)
	}
	cleanup := func(operationErr error) (*recoveryCoordination, error) {
		return nil, errors.Join(operationErr, marker.Close(), retainedRoot.Close())
	}
	info, err := retainedRoot.Lstat(recoveryCoordinationName)
	opened, openErr := marker.Stat()
	if err != nil || openErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || !os.SameFile(info, opened) {
		return cleanup(errors.Join(ErrUnsafePath, err, openErr))
	}
	if err := marker.Chmod(0o600); err != nil {
		return cleanup(err)
	}
	if err := marker.Sync(); err != nil {
		return cleanup(err)
	}
	if created {
		if err := syncRootDir(retainedRoot, "."); err != nil {
			return cleanup(err)
		}
	}
	directory, err := retainedRoot.Open(".")
	if err != nil {
		return cleanup(err)
	}
	directoryInfo, err := directory.Stat()
	rootInfo, rootErr := retainedRoot.Stat(".")
	if err != nil || rootErr != nil || !directoryInfo.IsDir() || !os.SameFile(directoryInfo, rootInfo) {
		return nil, errors.Join(ErrUnsafePath, err, rootErr, directory.Close(), marker.Close(), retainedRoot.Close())
	}
	cleanupLocked := func(operationErr error) (*recoveryCoordination, error) {
		return nil, errors.Join(operationErr, directory.Close(), marker.Close(), retainedRoot.Close())
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := unix.Flock(int(directory.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			if err := verifyRecoveryMarker(retainedRoot, marker, info); err != nil {
				_ = unix.Flock(int(directory.Fd()), unix.LOCK_UN)
				return cleanupLocked(err)
			}
			return &recoveryCoordination{root: retainedRoot, directory: directory, marker: marker, markerInfo: info}, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			return cleanupLocked(err)
		}
		select {
		case <-ctx.Done():
			return cleanupLocked(errors.Join(ErrRecoveryBusy, ctx.Err()))
		case <-ticker.C:
		}
	}
}

func (c *recoveryCoordination) release() error {
	if c == nil || c.directory == nil {
		return nil
	}
	identityErr := verifyRecoveryMarker(c.root, c.marker, c.markerInfo)
	unlockErr := unix.Flock(int(c.directory.Fd()), unix.LOCK_UN)
	return errors.Join(identityErr, unlockErr, c.directory.Close(), c.marker.Close(), c.root.Close())
}

func verifyRecoveryMarker(root *os.Root, marker *os.File, expected os.FileInfo) error {
	info, err := root.Lstat(recoveryCoordinationName)
	opened, openErr := marker.Stat()
	if err != nil || openErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != 0o600 || !os.SameFile(expected, info) || !os.SameFile(expected, opened) {
		return errors.Join(ErrUnsafePath, err, openErr)
	}
	return nil
}
