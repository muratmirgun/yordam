//go:build darwin || linux

package skills

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// openRootedRegularNoFollow opens a leaf relative to the retained directory
// descriptor. O_NOFOLLOW and O_NONBLOCK make a replacement by a symlink or a
// FIFO fail closed without following or blocking before identity verification.
func openRootedRegularNoFollow(root *os.Root, name string, expected os.FileInfo) (*os.File, error) {
	directory, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	fd, openErr := unix.Openat(int(directory.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	closeErr := directory.Close()
	if openErr != nil || closeErr != nil {
		if openErr == nil {
			openErr = closeErr
		}
		return nil, openErr
	}
	file := os.NewFile(uintptr(fd), name)
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || !os.SameFile(info, expected) {
		_ = file.Close()
		if err == nil {
			err = errors.New("opened leaf is not the checked regular file")
		}
		return nil, err
	}
	if err := unix.SetNonblock(fd, false); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}
