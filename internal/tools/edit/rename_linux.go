//go:build linux

package edit

import (
	"os"

	"golang.org/x/sys/unix"
)

func renameNoReplace(directory *os.File, from, to string) error {
	return unix.Renameat2(int(directory.Fd()), from, int(directory.Fd()), to, unix.RENAME_NOREPLACE)
}

func exchangeNames(directory *os.File, left, right string) error {
	return unix.Renameat2(int(directory.Fd()), left, int(directory.Fd()), right, unix.RENAME_EXCHANGE)
}
