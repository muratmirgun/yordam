package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureGlobalLinkCollisionRemovesTemporaryAndSyncsDirectory(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	winner := []byte("winner")
	var temporaryPath string
	syncCalls := 0

	path, created, err := ensureGlobal(
		func(oldPath, newPath string) error {
			temporaryPath = oldPath
			if err := os.WriteFile(newPath, winner, 0o600); err != nil {
				return err
			}
			return os.Link(oldPath, newPath)
		},
		os.Remove,
		func(directory string) error {
			syncCalls++
			if _, err := os.Lstat(temporaryPath); !os.IsNotExist(err) {
				return fmt.Errorf("temporary still exists before directory sync: %v", err)
			}
			return syncDirectory(directory)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(home, ".config", "yordam", "config.jsonc"); path != want || created {
		t.Fatalf("path=%q want=%q created=%t", path, want, created)
	}
	if syncCalls != 1 {
		t.Fatalf("directory sync calls=%d want=1", syncCalls)
	}
	if _, err := os.Lstat(temporaryPath); !os.IsNotExist(err) {
		t.Fatalf("losing temporary remains: %v", err)
	}
	if raw, err := os.ReadFile(path); err != nil || string(raw) != string(winner) {
		t.Fatalf("winner changed: raw=%q err=%v", raw, err)
	}
}

func TestEnsureGlobalPropagatesTemporaryCleanupErrors(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	removeErr := errors.New("remove temporary")
	syncErr := errors.New("sync directory")

	_, created, err := ensureGlobal(
		func(oldPath, newPath string) error {
			if err := os.WriteFile(newPath, []byte("winner"), 0o600); err != nil {
				return err
			}
			return os.Link(oldPath, newPath)
		},
		func(string) error { return removeErr },
		func(string) error { return syncErr },
	)
	if created {
		t.Fatal("link collision reported config creation")
	}
	if !errors.Is(err, removeErr) || !errors.Is(err, syncErr) {
		t.Fatalf("EnsureGlobal() error=%v, want removal and sync errors", err)
	}
}
