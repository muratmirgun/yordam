package scope

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Resolved struct {
	Workspace string
	Path      string
	Inside    bool
}

func Resolve(workspace, target string, allowMissingLeaf bool) (Resolved, error) {
	root, err := filepath.Abs(workspace)
	if err != nil {
		return Resolved{}, err
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return Resolved{}, err
	}

	candidate := target
	if !filepath.IsAbs(candidate) {
		candidate = filepath.Join(root, candidate)
	}
	candidate, err = filepath.Abs(candidate)
	if err != nil {
		return Resolved{}, err
	}

	canonical, err := filepath.EvalSymlinks(candidate)
	if err != nil && allowMissingLeaf && os.IsNotExist(err) {
		if _, leafErr := os.Lstat(candidate); leafErr == nil || !os.IsNotExist(leafErr) {
			return Resolved{}, err
		}
		parent, parentErr := filepath.EvalSymlinks(filepath.Dir(candidate))
		if parentErr != nil {
			return Resolved{}, parentErr
		}
		canonical = filepath.Join(parent, filepath.Base(candidate))
	} else if err != nil {
		return Resolved{}, err
	}
	if canonical == "" {
		return Resolved{}, fmt.Errorf("empty canonical path")
	}

	relative, err := filepath.Rel(root, canonical)
	if err != nil {
		return Resolved{}, err
	}
	inside := relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator)) && !filepath.IsAbs(relative)
	return Resolved{Workspace: root, Path: canonical, Inside: inside}, nil
}
