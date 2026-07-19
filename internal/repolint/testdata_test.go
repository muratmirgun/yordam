package repolint_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func TestRepositoryContainsNoTestdataDirectories(t *testing.T) {
	root := repositoryRoot(t)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() {
			return nil
		}
		switch entry.Name() {
		case ".git", ".worktrees", "dist":
			return filepath.SkipDir
		case "testdata":
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			t.Errorf("repository contains testdata directory: %s", filepath.ToSlash(relative))
			return filepath.SkipDir
		default:
			return nil
		}
	})
	if err != nil {
		t.Fatal(err)
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(workingDirectory, "..", ".."))
}
