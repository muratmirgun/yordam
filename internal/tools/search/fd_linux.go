//go:build linux

package search

import "fmt"

func inheritedFilePath(fd int) string { return fmt.Sprintf("/proc/self/fd/%d", fd) }
