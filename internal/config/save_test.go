package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/config"
)

func TestEnsureGlobalCreatesSecureEditableTemplate(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "ignored"))
	t.Setenv("YORDAM_API_KEY", "actual-secret")

	path, created, err := config.EnsureGlobal()
	if err != nil {
		t.Fatal(err)
	}
	if !created || path != filepath.Join(home, ".config", "yordam", "config.jsonc") {
		t.Fatalf("path=%q created=%t", path, created)
	}
	assertMode(t, filepath.Dir(path), 0o700)
	assertMode(t, path, 0o600)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"$schema": "` + config.SchemaURL + `"`,
		`"model": "openai/your-model-id"`,
		`"baseURL": "https://api.openai.com/v1"`,
		`"apiKey": "your-api-key"`,
		`// Format: provider/model`,
	} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("template missing %q:\n%s", want, raw)
		}
	}
	if strings.Contains(string(raw), "actual-secret") {
		t.Fatal("template persisted API key value")
	}
}

func TestEnsureGlobalNeverOverwritesExistingFile(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	path := filepath.Join(home, ".config", "yordam", "config.jsonc")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}

	got, created, err := config.EnsureGlobal()
	if err != nil {
		t.Fatal(err)
	}
	if got != path || created {
		t.Fatalf("path=%q created=%t", got, created)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "unchanged" {
		t.Fatalf("existing config changed to %q", raw)
	}
}

func TestEnsureGlobalRemovesOnlyRecognizedTemporary(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	directory := filepath.Join(home, ".config", "yordam")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	abandoned := filepath.Join(directory, ".config-abandoned.tmp")
	lookalike := filepath.Join(directory, ".config-keep.txt")
	if err := os.WriteFile(abandoned, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-25 * time.Hour)
	if err := os.Chtimes(abandoned, old, old); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lookalike, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := config.EnsureGlobal(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(abandoned); !os.IsNotExist(err) {
		t.Fatalf("abandoned temporary remains: %v", err)
	}
	if _, err := os.Stat(lookalike); err != nil {
		t.Fatalf("lookalike removed: %v", err)
	}
}

func TestEnsureGlobalPreservesRecentTemporaryFromConcurrentWriter(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	directory := filepath.Join(home, ".config", "yordam")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	live := filepath.Join(directory, ".config-live.tmp")
	if err := os.WriteFile(live, []byte("in progress"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := config.EnsureGlobal(); err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(live); err != nil || string(raw) != "in progress" {
		t.Fatalf("live temporary changed: raw=%q err=%v", raw, err)
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode=%o want=%o", path, got, want)
	}
}
