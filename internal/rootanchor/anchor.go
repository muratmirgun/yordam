package rootanchor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Anchor retains the deepest existing ancestor of a storage root from
// construction onward. Missing root components are created through retained
// directory handles, never by resolving the original absolute path again.
type Anchor struct {
	target        string
	unsafe        error
	ancestorPath  string
	ancestor      *os.Root
	ancestorChain []identity
	relative      string
	root          *os.Root
	rootInfo      os.FileInfo
	creation      []*os.Root
	descendants   map[string]pinnedDirectory
}

type identity struct {
	path string
	info os.FileInfo
}

type pinnedDirectory struct {
	info   os.FileInfo
	handle *os.Root
}

func New(path string, unsafe error) (*Anchor, error) {
	if unsafe == nil {
		return nil, fmt.Errorf("unsafe-path sentinel is required")
	}
	target, err := normalize(path)
	if err != nil {
		return nil, err
	}
	ancestorPath, relative, err := deepestExisting(target)
	if err != nil {
		return nil, err
	}
	chain, err := captureChain(ancestorPath, unsafe)
	if err != nil {
		return nil, err
	}
	ancestor, err := os.OpenRoot(ancestorPath)
	if err != nil {
		return nil, err
	}
	ancestorInfo, err := os.Lstat(ancestorPath)
	if err != nil {
		_ = ancestor.Close()
		return nil, err
	}
	opened, err := ancestor.Stat(".")
	if err != nil || !os.SameFile(ancestorInfo, opened) {
		_ = ancestor.Close()
		return nil, errors.Join(unsafe, err)
	}
	anchor := &Anchor{
		target: target, unsafe: unsafe, ancestorPath: ancestorPath, ancestor: ancestor,
		ancestorChain: chain, relative: relative, descendants: make(map[string]pinnedDirectory),
	}
	if relative == "." {
		anchor.root = ancestor
		anchor.rootInfo = ancestorInfo
	}
	return anchor, nil
}

func (a *Anchor) Target() string { return a.target }

func (a *Anchor) Open(ctx context.Context, create bool) (*os.Root, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := a.Verify(); err != nil {
		return nil, err
	}
	if a.root != nil {
		return a.root, nil
	}
	current := a.ancestor
	parts := strings.Split(a.relative, string(filepath.Separator))
	createdHandles := make([]*os.Root, 0, len(parts))
	for _, part := range parts {
		if !safeName(part) {
			closeRoots(createdHandles)
			return nil, a.unsafe
		}
		info, err := current.Lstat(part)
		if os.IsNotExist(err) && create {
			if err := current.Mkdir(part, 0o700); err != nil && !os.IsExist(err) {
				closeRoots(createdHandles)
				return nil, err
			}
			parent, syncErr := current.Open(".")
			if syncErr != nil {
				closeRoots(createdHandles)
				return nil, syncErr
			}
			if syncErr := errors.Join(parent.Sync(), parent.Close()); syncErr != nil {
				closeRoots(createdHandles)
				return nil, syncErr
			}
			info, err = current.Lstat(part)
		}
		if err != nil {
			closeRoots(createdHandles)
			return nil, err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			closeRoots(createdHandles)
			return nil, a.unsafe
		}
		next, err := current.OpenRoot(part)
		if err != nil {
			closeRoots(createdHandles)
			return nil, err
		}
		opened, err := next.Stat(".")
		if err != nil || !os.SameFile(info, opened) {
			_ = next.Close()
			closeRoots(createdHandles)
			return nil, errors.Join(a.unsafe, err)
		}
		createdHandles = append(createdHandles, next)
		current = next
	}
	if err := a.verifyAncestor(); err != nil {
		closeRoots(createdHandles)
		return nil, err
	}
	a.creation = createdHandles
	a.root = current
	a.rootInfo, _ = current.Stat(".")
	if err := a.Verify(); err != nil {
		return nil, err
	}
	return a.root, nil
}

func (a *Anchor) Verify() error {
	if a == nil || a.ancestor == nil {
		return a.unsafe
	}
	if err := a.verifyAncestor(); err != nil {
		return err
	}
	if a.root == nil {
		return nil
	}
	info, err := os.Lstat(a.target)
	opened, openErr := a.root.Stat(".")
	if err != nil || openErr != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
		a.rootInfo == nil || !os.SameFile(a.rootInfo, info) || !os.SameFile(a.rootInfo, opened) {
		return errors.Join(a.unsafe, err, openErr)
	}
	for name, pinned := range a.descendants {
		info, err := a.root.Lstat(name)
		opened, openErr := pinned.handle.Stat(".")
		if err != nil || openErr != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
			!os.SameFile(pinned.info, info) || !os.SameFile(pinned.info, opened) {
			return errors.Join(a.unsafe, err, openErr)
		}
	}
	return nil
}

func (a *Anchor) PinDirectory(relative string) error {
	if a.root == nil || !safeRelative(relative) {
		return a.unsafe
	}
	if err := a.VerifyRelative(relative); err != nil {
		return err
	}
	info, err := a.root.Lstat(relative)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(a.unsafe, err)
	}
	if pinned, ok := a.descendants[relative]; ok {
		if !os.SameFile(pinned.info, info) {
			return a.unsafe
		}
		return nil
	}
	handle, err := a.root.OpenRoot(relative)
	if err != nil {
		return err
	}
	opened, err := handle.Stat(".")
	if err != nil || !os.SameFile(info, opened) {
		_ = handle.Close()
		return errors.Join(a.unsafe, err)
	}
	a.descendants[relative] = pinnedDirectory{info: info, handle: handle}
	return a.Verify()
}

// VerifyRelative rejects a symbolic-link directory component, including for a
// freshly constructed store that has not pinned the descendant before.
func (a *Anchor) VerifyRelative(relative string) error {
	if a.root == nil || !safeRelative(relative) {
		return a.unsafe
	}
	parts := strings.Split(filepath.Clean(relative), string(filepath.Separator))
	cursor := ""
	for index, part := range parts {
		cursor = filepath.Join(cursor, part)
		info, err := a.root.Lstat(cursor)
		if err != nil {
			return err
		}
		if index < len(parts)-1 && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
			return a.unsafe
		}
	}
	return nil
}

func (a *Anchor) Close() error {
	if a == nil {
		return nil
	}
	var errs []error
	for _, pinned := range a.descendants {
		errs = append(errs, pinned.handle.Close())
	}
	a.descendants = nil
	if a.root != nil && a.root != a.ancestor {
		// The last creation handle is the root and is closed below with the rest.
		a.root = nil
	}
	for _, handle := range a.creation {
		errs = append(errs, handle.Close())
	}
	a.creation = nil
	if a.ancestor != nil {
		errs = append(errs, a.ancestor.Close())
		a.ancestor = nil
	}
	a.root = nil
	return errors.Join(errs...)
}

func (a *Anchor) verifyAncestor() error {
	for _, pinned := range a.ancestorChain {
		info, err := os.Lstat(pinned.path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !os.SameFile(pinned.info, info) {
			return errors.Join(a.unsafe, err)
		}
	}
	opened, err := a.ancestor.Stat(".")
	last := a.ancestorChain[len(a.ancestorChain)-1]
	if err != nil || !os.SameFile(last.info, opened) {
		return errors.Join(a.unsafe, err)
	}
	return nil
}

func normalize(path string) (string, error) {
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	cursor := absolute
	var missing []string
	for {
		canonical, resolveErr := filepath.EvalSymlinks(cursor)
		if resolveErr == nil {
			for index := len(missing) - 1; index >= 0; index-- {
				canonical = filepath.Join(canonical, missing[index])
			}
			return canonical, nil
		}
		parent := filepath.Dir(cursor)
		if parent == cursor {
			return "", resolveErr
		}
		missing = append(missing, filepath.Base(cursor))
		cursor = parent
	}
}

func deepestExisting(target string) (string, string, error) {
	cursor := target
	var missing []string
	for {
		info, err := os.Lstat(cursor)
		if err == nil {
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				return "", "", fmt.Errorf("deepest storage ancestor is not a directory")
			}
			relative := "."
			for index := len(missing) - 1; index >= 0; index-- {
				relative = filepath.Join(relative, missing[index])
			}
			return cursor, relative, nil
		}
		if !os.IsNotExist(err) {
			return "", "", err
		}
		parent := filepath.Dir(cursor)
		if parent == cursor {
			return "", "", err
		}
		missing = append(missing, filepath.Base(cursor))
		cursor = parent
	}
}

func captureChain(path string, unsafe error) ([]identity, error) {
	var paths []string
	for cursor := filepath.Clean(path); ; cursor = filepath.Dir(cursor) {
		paths = append(paths, cursor)
		if parent := filepath.Dir(cursor); parent == cursor {
			break
		}
	}
	chain := make([]identity, 0, len(paths))
	for index := len(paths) - 1; index >= 0; index-- {
		info, err := os.Lstat(paths[index])
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.Join(unsafe, err)
		}
		chain = append(chain, identity{path: paths[index], info: info})
	}
	return chain, nil
}

func safeRelative(path string) bool {
	if path == "" || filepath.IsAbs(path) || filepath.Clean(path) != path {
		return false
	}
	for _, part := range strings.Split(path, string(filepath.Separator)) {
		if !safeName(part) {
			return false
		}
	}
	return true
}

func safeName(value string) bool {
	return value != "" && value != "." && value != ".." && filepath.Base(value) == value && !strings.ContainsAny(value, "/\\\x00")
}

func closeRoots(roots []*os.Root) {
	for _, root := range roots {
		_ = root.Close()
	}
}
