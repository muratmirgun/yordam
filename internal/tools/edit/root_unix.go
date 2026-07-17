//go:build darwin || linux

package edit

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func createTemporaryAt(directory *os.File, name string) (*os.File, error) {
	fd, err := unix.Openat(int(directory.Fd()), name, unix.O_CREAT|unix.O_EXCL|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

func openAt(directory *os.File, name string) (*os.File, error) {
	fd, err := unix.Openat(int(directory.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

func removeAt(directory *os.File, name string) error {
	if name == "" {
		return nil
	}
	err := unix.Unlinkat(int(directory.Fd()), name, 0)
	if err == unix.ENOENT {
		return nil
	}
	return err
}

func validateDirectoryIdentity(directory *os.File, expected os.FileInfo) error {
	actual, err := directory.Stat()
	if err != nil {
		return err
	}
	if expected == nil || !os.SameFile(expected, actual) {
		return fmt.Errorf("edit parent directory identity changed after authorization")
	}
	return nil
}
