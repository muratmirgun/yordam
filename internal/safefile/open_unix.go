//go:build darwin || linux

package safefile

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// OpenRegular walks path from an opened root directory without following
// symlinks, then rejects non-regular leaves before returning a file handle.
func OpenRegular(ctx context.Context, path string) (*os.File, error) {
	return open(ctx, path, false)
}

func OpenDirectory(ctx context.Context, path string) (*os.File, error) {
	return open(ctx, path, true)
}

func open(ctx context.Context, path string, directory bool) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	clean := filepath.Clean(abs)
	parts := strings.Split(strings.TrimPrefix(clean, string(filepath.Separator)), string(filepath.Separator))
	if len(parts) == 0 || parts[0] == "" {
		return nil, fmt.Errorf("path %q has no regular-file leaf", path)
	}

	fd, err := unix.Open(string(filepath.Separator), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	for index, part := range parts {
		if err := ctx.Err(); err != nil {
			_ = unix.Close(fd)
			return nil, err
		}
		flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK
		if index < len(parts)-1 || directory {
			flags |= unix.O_DIRECTORY
		}
		next, openErr := unix.Openat(fd, part, flags, 0)
		closeErr := unix.Close(fd)
		if openErr != nil {
			return nil, openErr
		}
		if closeErr != nil {
			_ = unix.Close(next)
			return nil, closeErr
		}
		fd = next
	}

	file := os.NewFile(uintptr(fd), clean)
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if directory && !info.IsDir() {
		_ = file.Close()
		return nil, fmt.Errorf("path %q is not a directory", clean)
	}
	if !directory && !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, fmt.Errorf("path %q is not a regular file", clean)
	}
	if err := unix.SetNonblock(fd, false); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}
