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
		`// Project skills may come from the repository and need your trust.`,
		`// Supported values: ask, allow, deny.`,
		`"projectPolicy": "ask"`,
		`// Enable sequential subagents for bounded delegated work.`,
		`// maxPerTurn, maxToolCalls, and timeoutSeconds remain enforced when disabled.`,
	} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("template missing %q:\n%s", want, raw)
		}
	}
	if strings.Contains(string(raw), `"apiKey"`) || strings.Contains(string(raw), "actual-secret") {
		t.Fatal("template persisted a literal API key")
	}
	if strings.Contains(string(raw), `"projectPolicy": "allow"`) {
		t.Fatal("template implicitly enables project skills")
	}
}

func TestGeneratedTemplateCoversV030StrictDecoderSurface(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	path, _, err := config.EnsureGlobal()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		`// "contextWindow": 128000`,
		`"autoCompact": true`,
		`// "compactReserveTokens": 8192`,
		`"projectPolicy": "ask"`,
		`"enabled": true`,
		`"maxPerTurn": 4`,
		`"maxToolCalls": 16`,
		`"timeoutSeconds": 600`,
	} {
		if strings.Count(string(raw), required) != 1 {
			t.Errorf("generated template must contain exactly one %q", required)
		}
	}

	configured := strings.ReplaceAll(string(raw), "your-model-id", "model-a")
	configured = strings.Replace(configured, `// "contextWindow": 128000`, `"contextWindow": 128000`, 1)
	configured = strings.Replace(configured, `// "compactReserveTokens": 8192`, `"compactReserveTokens": 8192`, 1)
	if err := os.WriteFile(path, []byte(configured), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(config.LoadOptions{ConfigPath: path})
	if err != nil {
		t.Fatalf("generated v0.3 template does not pass strict decoder after placeholders are configured: %v", err)
	}
	if loaded.Profiles["openai"].ModelContextWindows["model-a"] != 128000 ||
		!loaded.Context.AutoCompact || loaded.Context.CompactReserveTokens == nil || *loaded.Context.CompactReserveTokens != 8192 ||
		loaded.Skills.ProjectPolicy != config.ProjectSkillsAsk ||
		loaded.Subagents != (config.SubagentConfig{Enabled: true, MaxPerTurn: 4, MaxToolCalls: 16, TimeoutSeconds: 600}) {
		t.Fatalf("generated v0.3 template decoded incompletely: %+v", loaded)
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
		loaded.MaxToolCalls != 64 || loaded.ShellTimeoutSeconds != 300 || loaded.Subagents != (config.SubagentConfig{Enabled: true, MaxPerTurn: 3, MaxToolCalls: 12, TimeoutSeconds: 300}) || !loaded.Context.AutoCompact || loaded.Context.CompactReserveTokens == nil || *loaded.Context.CompactReserveTokens != 8192 || loaded.Profiles["primary"].ModelContextWindows["model-a"] != 128000 {
		t.Fatalf("round-tripped config=%+v models=%v", loaded, loaded.Models())
	}
	resolved, err := loaded.Resolve(config.ResolveOptions{})
	if err != nil || resolved.APIKey != "actual-secret" {
		t.Fatalf("round-tripped resolved=%+v err=%v", resolved, err)
	}
}

func TestSaveGlobalDefaultsOmittedProjectPolicyToAsk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.jsonc")
	cfg := savedConfig()
	cfg.Skills.ProjectPolicy = ""
	if err := config.SaveGlobal(path, cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(config.LoadOptions{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Skills.ProjectPolicy != config.ProjectSkillsAsk {
		t.Fatalf("project policy=%q want ask", loaded.Skills.ProjectPolicy)
	}
}

func TestSaveGlobalDefaultsOmittedSubagentConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.jsonc")
	cfg := savedConfig()
	cfg.Subagents = config.SubagentConfig{}
	if err := config.SaveGlobal(path, cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(config.LoadOptions{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	want := config.SubagentConfig{Enabled: true, MaxPerTurn: 4, MaxToolCalls: 16, TimeoutSeconds: 600}
	if loaded.Subagents != want {
		t.Fatalf("subagents=%+v want=%+v", loaded.Subagents, want)
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

func TestEnsureGlobalRemovesOnlyAbandonedTemporary(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	directory := filepath.Join(home, ".config", "yordam")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	abandoned := filepath.Join(directory, ".config-abandoned.tmp")
	recent := filepath.Join(directory, ".config-recent.tmp")
	nonmatching := filepath.Join(directory, ".config-keep.txt")
	matchingDirectory := filepath.Join(directory, ".config-directory.tmp")
	matchingSymlink := filepath.Join(directory, ".config-symlink.tmp")
	if err := os.WriteFile(abandoned, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-25 * time.Hour)
	if err := os.Chtimes(abandoned, old, old); err != nil {
		t.Fatal(err)
	}
	for path, contents := range map[string]string{
		recent:      "in progress",
		nonmatching: "keep",
	} {
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(matchingDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(matchingDirectory, old, old); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(nonmatching, matchingSymlink); err != nil {
		t.Fatal(err)
	}
	if _, _, err := config.EnsureGlobal(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(abandoned); !os.IsNotExist(err) {
		t.Fatalf("abandoned temporary remains: %v", err)
	}
	for path, want := range map[string]string{
		recent:      "in progress",
		nonmatching: "keep",
	} {
		if raw, err := os.ReadFile(path); err != nil || string(raw) != want {
			t.Fatalf("preserved file %q changed: raw=%q err=%v", path, raw, err)
		}
	}
	for _, path := range []string{matchingDirectory, matchingSymlink} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("matching nonregular entry %q changed: %v", path, err)
		}
	}
}

func TestEnsureGlobalNeverOverwritesExistingFile(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	path, created, err := config.EnsureGlobal()
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("first EnsureGlobal() did not create config")
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
	reserve := int64(8192)
	return config.Config{
		ActiveProfile: "primary",
		Profiles: map[string]config.Profile{
			"primary": {
				Name:                "Primary",
				BaseURL:             "https://llm.example/v1",
				APIKeyEnv:           "PRIMARY_KEY",
				Models:              []string{"model-b", "model-a"},
				DefaultModel:        "model-a",
				ModelContextWindows: map[string]int64{"model-a": 128000},
			},
		},
		MaxToolCalls:        64,
		ShellTimeoutSeconds: 300,
		Subagents:           config.SubagentConfig{Enabled: true, MaxPerTurn: 3, MaxToolCalls: 12, TimeoutSeconds: 300},
		Context:             config.ContextConfig{AutoCompact: true, CompactReserveTokens: &reserve},
		Skills:              config.SkillConfig{ProjectPolicy: config.ProjectSkillsAsk},
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
