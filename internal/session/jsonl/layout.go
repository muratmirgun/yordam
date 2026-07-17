package jsonl

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/muratmirgun/yordam/internal/domain"
)

func WorkspaceFromPath(path string) (domain.Workspace, error) {
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return domain.Workspace{}, err
	}
	canonical, err = filepath.Abs(canonical)
	if err != nil {
		return domain.Workspace{}, err
	}
	sum := sha256.Sum256([]byte(canonical))
	return domain.Workspace{ID: hex.EncodeToString(sum[:]), CanonicalPath: canonical}, nil
}

func validateWorkspace(workspace domain.Workspace) error {
	canonical, err := filepath.EvalSymlinks(workspace.CanonicalPath)
	if err != nil {
		return fmt.Errorf("invalid workspace: %w", err)
	}
	canonical, err = filepath.Abs(canonical)
	if err != nil {
		return fmt.Errorf("invalid workspace: %w", err)
	}
	if canonical != workspace.CanonicalPath {
		return fmt.Errorf("invalid workspace canonical path %q", workspace.CanonicalPath)
	}
	sum := sha256.Sum256([]byte(canonical))
	if workspace.ID != hex.EncodeToString(sum[:]) {
		return fmt.Errorf("invalid workspace ID %q", workspace.ID)
	}
	return nil
}

func validWorkspaceID(id string) bool {
	if len(id) != sha256.Size*2 || id != strings.ToLower(id) {
		return false
	}
	decoded, err := hex.DecodeString(id)
	return err == nil && len(decoded) == sha256.Size
}

func ensureDir(path string) error {
	clean := filepath.Clean(path)
	missing := []string{}
	for cursor := clean; ; cursor = filepath.Dir(cursor) {
		if info, err := os.Stat(cursor); err == nil {
			if !info.IsDir() {
				return fmt.Errorf("%q is not a directory", cursor)
			}
			break
		} else if !os.IsNotExist(err) {
			return err
		}
		missing = append(missing, cursor)
		if filepath.Dir(cursor) == cursor {
			return fmt.Errorf("no existing ancestor for %q", clean)
		}
	}
	for index := len(missing) - 1; index >= 0; index-- {
		if err := os.Mkdir(missing[index], 0o700); err != nil {
			return err
		}
		if err := syncDir(filepath.Dir(missing[index])); err != nil {
			return err
		}
	}
	return nil
}

func syncDir(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
