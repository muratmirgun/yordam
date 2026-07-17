//go:build darwin

package edit

import (
	"os"

	"golang.org/x/sys/unix"
)

func renameNoReplace(directory *os.File, from, to string) error {
	return unix.RenameatxNp(int(directory.Fd()), from, int(directory.Fd()), to, unix.RENAME_EXCL)
}

func exchangeNames(directory *os.File, left, right string) error {
	return unix.RenameatxNp(int(directory.Fd()), left, int(directory.Fd()), right, unix.RENAME_SWAP)
}
