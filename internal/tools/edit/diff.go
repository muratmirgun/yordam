package edit

import (
	"io"
	"path/filepath"

	"github.com/pmezard/go-difflib/difflib"
)

func writeUnifiedDiff(destination io.Writer, before, after []byte, relative string) error {
	return difflib.WriteUnifiedDiff(destination, difflib.UnifiedDiff{
		A:        difflib.SplitLines(string(before)),
		B:        difflib.SplitLines(string(after)),
		FromFile: "a/" + filepath.ToSlash(relative),
		ToFile:   "b/" + filepath.ToSlash(relative),
		Context:  3,
	})
}
