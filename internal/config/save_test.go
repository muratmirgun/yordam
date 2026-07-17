package config_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/domain"
)

func TestEnsureGlobalCreatesSecureEnvironmentOnlyTemplate(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "ignored"))
	t.Setenv("OPENAI_API_KEY", "actual-secret")

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
		`"apiKeyEnv": "OPENAI_API_KEY"`,
		`// Format: provider/model`,
	} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("template missing %q:\n%s", want, raw)
		}
	}
	if strings.Contains(string(raw), `"apiKey"`) || strings.Contains(string(raw), "actual-secret") {
		t.Fatal("template persisted a literal API key")
	}
}

func TestSaveGlobalWritesStrictJSONAndRoundTrips(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "actual-secret")
	path := filepath.Join(t.TempDir(), "nested", "config.jsonc")
	cfg := savedConfig()
	if err := config.SaveGlobal(path, cfg); err != nil {
		t.Fatal(err)
	}
	assertMode(t, filepath.Dir(path), 0o700)
	assertMode(t, path, 0o600)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"model"`, `"provider"`, `"baseURL"`, `"apiKeyEnv"`, `"models"`, `"limits"`} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("saved config missing %s:\n%s", want, raw)
		}
	}
	if strings.Contains(string(raw), `"apiKey"`) || strings.Contains(string(raw), "actual-secret") {
		t.Fatalf("saved config persisted an API key:\n%s", raw)
	}
	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("saved output is not strict JSON: %v", err)
	}

	loaded, err := config.Load(config.LoadOptions{ConfigPath: path, LookupEnv: os.LookupEnv})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.DefaultSelection() != (domain.ModelSelection{Profile: "primary", Model: "model-a"}) ||
		!slices.Equal(loaded.Models(), []domain.ModelSelection{{Profile: "primary", Model: "model-a"}, {Profile: "primary", Model: "model-b"}}) ||
		loaded.MaxToolCalls != 64 || loaded.ShellTimeoutSeconds != 300 {
		t.Fatalf("round-tripped config=%+v models=%v", loaded, loaded.Models())
	}
	resolved, err := loaded.Resolve(config.ResolveOptions{})
	if err != nil || resolved.APIKey != "actual-secret" {
		t.Fatalf("round-tripped resolved=%+v err=%v", resolved, err)
	}
}

func TestSaveGlobalDoesNotReplaceExistingFileWhenConfigIsInvalid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.jsonc")
	if err := os.WriteFile(path, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := savedConfig()
	cfg.MaxToolCalls = 0
	if err := config.SaveGlobal(path, cfg); err == nil {
		t.Fatal("SaveGlobal() accepted invalid config")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "unchanged" {
		t.Fatalf("existing config changed to %q", raw)
	}
}

func TestSaveGlobalRemovesAbandonedTemporary(t *testing.T) {
	directory := t.TempDir()
	abandoned := filepath.Join(directory, ".config-abandoned.tmp")
	live := filepath.Join(directory, ".config-live.tmp")
	if err := os.WriteFile(abandoned, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-25 * time.Hour)
	if err := os.Chtimes(abandoned, old, old); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(live, []byte("in progress"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := config.SaveGlobal(filepath.Join(directory, "config.jsonc"), savedConfig()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(abandoned); !os.IsNotExist(err) {
		t.Fatalf("abandoned temporary remains: %v", err)
	}
	if raw, err := os.ReadFile(live); err != nil || string(raw) != "in progress" {
		t.Fatalf("live temporary changed: raw=%q err=%v", raw, err)
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

func savedConfig() config.Config {
	return config.Config{
		ActiveProfile: "primary",
		Profiles: map[string]config.Profile{
			"primary": {
				Name:         "Primary",
				BaseURL:      "https://llm.example/v1",
				APIKeyEnv:    "PRIMARY_KEY",
				Models:       []string{"model-b", "model-a"},
				DefaultModel: "model-a",
			},
		},
		MaxToolCalls:        64,
		ShellTimeoutSeconds: 300,
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
