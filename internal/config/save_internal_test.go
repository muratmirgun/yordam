package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestCleanupConfigTemporariesSyncsPartialRemovalBeforeReturningError(t *testing.T) {
	directory := t.TempDir()
	infoErr := errors.New("inspect temporary")
	syncErr := errors.New("sync partial cleanup")
	removedPath := ""
	syncCalls := 0
	old := time.Now().Add(-25 * time.Hour)

	err := cleanupConfigTemporaryEntries(
		directory,
		[]os.DirEntry{
			cleanupDirEntry{name: ".config-abandoned.tmp", info: cleanupFileInfo{name: ".config-abandoned.tmp", modified: old}},
			cleanupDirEntry{name: ".config-broken.tmp", err: infoErr},
		},
		func(path string) error {
			removedPath = path
			return nil
		},
		func(string) error {
			syncCalls++
			return syncErr
		},
	)
	if want := filepath.Join(directory, ".config-abandoned.tmp"); removedPath != want {
		t.Fatalf("removed path=%q want=%q", removedPath, want)
	}
	if syncCalls != 1 {
		t.Fatalf("directory sync calls=%d want=1", syncCalls)
	}
	if !errors.Is(err, infoErr) || !errors.Is(err, syncErr) {
		t.Fatalf("cleanup error=%v, want info and sync errors", err)
	}
}

func TestCleanupConfigTemporariesIgnoresInfoNotExist(t *testing.T) {
	directory := t.TempDir()
	disappeared := &os.PathError{Op: "stat", Path: ".config-disappeared.tmp", Err: os.ErrNotExist}
	removedPath := ""
	syncCalls := 0
	old := time.Now().Add(-25 * time.Hour)

	err := cleanupConfigTemporaryEntries(
		directory,
		[]os.DirEntry{
			cleanupDirEntry{name: ".config-disappeared.tmp", err: disappeared},
			cleanupDirEntry{name: ".config-stale.tmp", info: cleanupFileInfo{name: ".config-stale.tmp", modified: old}},
		},
		func(path string) error {
			removedPath = path
			return nil
		},
		func(string) error {
			syncCalls++
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(directory, ".config-stale.tmp"); removedPath != want {
		t.Fatalf("removed path=%q want=%q", removedPath, want)
	}
	if syncCalls != 1 {
		t.Fatalf("directory sync calls=%d want=1", syncCalls)
	}
}

type cleanupDirEntry struct {
	name string
	info os.FileInfo
	err  error
}

func (entry cleanupDirEntry) Name() string               { return entry.name }
func (entry cleanupDirEntry) IsDir() bool                { return entry.Type().IsDir() }
func (entry cleanupDirEntry) Type() os.FileMode          { return entry.infoMode().Type() }
func (entry cleanupDirEntry) Info() (os.FileInfo, error) { return entry.info, entry.err }

func (entry cleanupDirEntry) infoMode() os.FileMode {
	if entry.info == nil {
		return 0
	}
	return entry.info.Mode()
}

type cleanupFileInfo struct {
	name     string
	mode     os.FileMode
	modified time.Time
}

func (info cleanupFileInfo) Name() string       { return info.name }
func (cleanupFileInfo) Size() int64             { return 0 }
func (info cleanupFileInfo) Mode() os.FileMode  { return info.mode }
func (info cleanupFileInfo) ModTime() time.Time { return info.modified }
func (info cleanupFileInfo) IsDir() bool        { return info.mode.IsDir() }
func (cleanupFileInfo) Sys() any                { return nil }
