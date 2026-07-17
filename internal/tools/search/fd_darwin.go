//go:build darwin

package search

import "fmt"

func inheritedFilePath(fd int) string { return fmt.Sprintf("/dev/fd/%d", fd) }
