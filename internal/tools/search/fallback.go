package search

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"unicode/utf8"

	"github.com/muratmirgun/yordam/internal/tools/output"
)

const probeBytes = 8 << 10

var errMatchLimit = errors.New("search match limit reached")

func runFallback(ctx context.Context, destination io.Writer, root *os.Root, input Input, maxMatches int) (bool, error) {
	matches := 0
	err := fs.WalkDir(root.FS(), ".", func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			if entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		slashed := strings.TrimPrefix(current, "./")
		if !included(slashed, input.Include, input.Exclude) {
			return nil
		}

		file, err := root.Open(current)
		if err != nil {
			return fmt.Errorf("open search file %s: %w", slashed, err)
		}
		truncated, nextMatches, err := scanFallbackFile(ctx, destination, file, slashed, []byte(input.Query), matches, maxMatches)
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return fmt.Errorf("close search file %s: %w", slashed, closeErr)
		}
		matches = nextMatches
		if truncated {
			return errMatchLimit
		}
		return nil
	})
	if errors.Is(err, errMatchLimit) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("walk search path: %w", err)
	}
	return false, nil
}

func scanFallbackFile(ctx context.Context, destination io.Writer, file *os.File, relative string, query []byte, matches, maxMatches int) (bool, int, error) {
	info, err := file.Stat()
	if err != nil {
		return false, matches, fmt.Errorf("stat search file %s: %w", relative, err)
	}
	if !info.Mode().IsRegular() {
		return false, matches, nil
	}
	text, err := isSearchableText(file)
	if err != nil {
		return false, matches, fmt.Errorf("inspect search file %s: %w", relative, err)
	}
	if !text {
		return false, matches, nil
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return false, matches, fmt.Errorf("rewind search file %s: %w", relative, err)
	}

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), output.RetainedBytes)
	lineNumber := 0
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return false, matches, err
		}
		lineNumber++
		line := scanner.Bytes()
		if !bytes.Contains(line, query) {
			continue
		}
		if matches == maxMatches {
			return true, matches, nil
		}
		if matches > 0 {
			if _, err := destination.Write([]byte{'\n'}); err != nil {
				return false, matches, err
			}
		}
		if _, err := fmt.Fprintf(destination, "%s:%d:", relative, lineNumber); err != nil {
			return false, matches, err
		}
		if _, err := destination.Write(line); err != nil {
			return false, matches, err
		}
		matches++
	}
	if err := scanner.Err(); err != nil {
		return false, matches, fmt.Errorf("search file %s: %w", relative, err)
	}
	return false, matches, nil
}

func validUTF8Prefix(value []byte) bool {
	for len(value) > 0 {
		if !utf8.FullRune(value) {
			return true
		}
		r, size := utf8.DecodeRune(value)
		if r == utf8.RuneError && size == 1 {
			return false
		}
		value = value[size:]
	}
	return true
}

func validateGlobs(patterns []string) error {
	for _, pattern := range patterns {
		if pattern == "" {
			return fmt.Errorf("glob must not be empty")
		}
		for _, part := range strings.Split(strings.ReplaceAll(pattern, "\\", "/"), "/") {
			if part == "**" {
				continue
			}
			if _, err := path.Match(part, ""); err != nil {
				return fmt.Errorf("invalid glob %q: %w", pattern, err)
			}
		}
	}
	return nil
}

func included(relative string, includes, excludes []string) bool {
	if len(includes) > 0 && !matchesAny(includes, relative) {
		return false
	}
	return !matchesAny(excludes, relative)
}

func matchesAny(patterns []string, relative string) bool {
	for _, pattern := range patterns {
		pattern = strings.ReplaceAll(pattern, "\\", "/")
		if !strings.Contains(pattern, "/") {
			matched, _ := path.Match(pattern, path.Base(relative))
			if matched {
				return true
			}
			continue
		}
		if matchParts(strings.Split(pattern, "/"), strings.Split(relative, "/"), 0, 0, make(map[[2]int]bool), make(map[[2]int]bool)) {
			return true
		}
	}
	return false
}

func matchParts(pattern, value []string, patternIndex, valueIndex int, seen, memo map[[2]int]bool) bool {
	state := [2]int{patternIndex, valueIndex}
	if seen[state] {
		return memo[state]
	}
	seen[state] = true
	if patternIndex == len(pattern) {
		memo[state] = valueIndex == len(value)
		return memo[state]
	}
	if pattern[patternIndex] == "**" {
		if matchParts(pattern, value, patternIndex+1, valueIndex, seen, memo) || (valueIndex < len(value) && matchParts(pattern, value, patternIndex, valueIndex+1, seen, memo)) {
			memo[state] = true
		}
		return memo[state]
	}
	if valueIndex == len(value) {
		return false
	}
	matched, _ := path.Match(pattern[patternIndex], value[valueIndex])
	if matched {
		memo[state] = matchParts(pattern, value, patternIndex+1, valueIndex+1, seen, memo)
	}
	return memo[state]
}
