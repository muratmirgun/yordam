package config_test

import (
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/domain"
)

const validConfig = `{
  "$schema": "https://raw.githubusercontent.com/muratmirgun/yordam/main/schema/config.json",
  // JSONC comments and trailing commas are accepted.
  "model": "primary/model-a",
  "provider": {
    "primary": {
      "name": "Primary",
      "options": {
        "baseURL": "https://llm.example/v1",
        "apiKeyEnv": "PRIMARY_KEY",
      },
      "models": {
        "model-b": {},
        "model-a": {"name": "Model A"},
      },
    },
  },
  "limits": {"maxToolCalls": 32, "shellTimeoutSeconds": 120},
}`

func TestLoadJSONCAndResolveProfile(t *testing.T) {
	path := writeConfig(t, validConfig)
	t.Setenv("PRIMARY_KEY", "secret-value")

	cfg, err := config.Load(config.LoadOptions{ConfigPath: path, LookupEnv: os.LookupEnv})
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.DefaultSelection(); got != (domain.ModelSelection{Profile: "primary", Model: "model-a"}) {
		t.Fatalf("default selection=%+v", got)
	}
	if got := cfg.Models(); !slices.Equal(got, []domain.ModelSelection{{Profile: "primary", Model: "model-a"}, {Profile: "primary", Model: "model-b"}}) {
		t.Fatalf("models=%+v", got)
	}
	resolved, err := cfg.Resolve(config.ResolveOptions{Model: "model-b"})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Name != "primary" || resolved.Label != "Primary" || resolved.Model != "model-b" || resolved.BaseURL != "https://llm.example/v1" || resolved.APIKey != "secret-value" {
		t.Fatalf("resolved=%+v", resolved)
	}
	if strings.Contains(string(cfg.RawForTest()), "secret-value") {
		t.Fatal("secret copied into config")
	}
}

func TestLoadAcceptsDirectAPIKey(t *testing.T) {
	body := `{
  "model": "primary/model-a",
  "provider": {
    "primary": {
      "options": {
        "baseURL": "https://llm.example/v1",
        "apiKey": "direct-secret"
      },
      "models": {"model-a": {}}
    }
  }
}`
	cfg, err := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, body)})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := cfg.Resolve(config.ResolveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.APIKey != "direct-secret" {
		t.Fatalf("API key=%q", resolved.APIKey)
	}
}

func TestLoadAppliesOmittedLimitDefaults(t *testing.T) {
	body := strings.Replace(validConfig, `,
  "limits": {"maxToolCalls": 32, "shellTimeoutSeconds": 120}`, "", 1)
	cfg, err := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, body)})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxToolCalls != 32 || cfg.ShellTimeoutSeconds != 120 {
		t.Fatalf("limits=%d/%d", cfg.MaxToolCalls, cfg.ShellTimeoutSeconds)
	}
}

func TestLoadRejectsExplicitNullValues(t *testing.T) {
	tests := map[string]string{
		"schema":        strings.Replace(validConfig, `"$schema": "https://raw.githubusercontent.com/muratmirgun/yordam/main/schema/config.json"`, `"$schema": null`, 1),
		"provider name": strings.Replace(validConfig, `"name": "Primary"`, `"name": null`, 1),
		"model name":    strings.Replace(validConfig, `"name": "Model A"`, `"name": null`, 1),
		"model entry":   strings.Replace(validConfig, `"model-b": {}`, `"model-b": null`, 1),
		"limits":        strings.Replace(validConfig, `"limits": {"maxToolCalls": 32, "shellTimeoutSeconds": 120}`, `"limits": null`, 1),
		"limit field":   strings.Replace(validConfig, `"maxToolCalls": 32`, `"maxToolCalls": null`, 1),
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, body)})
			if err == nil || !strings.Contains(err.Error(), "null values are not allowed") {
				t.Fatalf("Load() error=%v", err)
			}
		})
	}
}

func TestLoadReportsJSONCLineAndColumn(t *testing.T) {
	_, err := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, "{\n  \"model\":,\n}")})
	if err == nil || !strings.Contains(err.Error(), "line 2") || !strings.Contains(err.Error(), "column") {
		t.Fatalf("syntax error=%v", err)
	}
}

func TestLoadAcceptsMissingCredential(t *testing.T) {
	cfg, err := config.Load(config.LoadOptions{
		ConfigPath: writeConfig(t, validConfig),
		LookupEnv:  func(string) (string, bool) { return "", false },
	})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := cfg.Resolve(config.ResolveOptions{})
	if err != nil || resolved.APIKey != "" || resolved.APIKeyEnv != "PRIMARY_KEY" {
		t.Fatalf("resolved=%+v err=%v", resolved, err)
	}
}

func TestResolveOverridePrecedence(t *testing.T) {
	body := `{
  "model": "primary/model-a",
  "provider": {
    "primary": {
      "options": {"baseURL": "https://llm.example/v1", "apiKeyEnv": "PRIMARY_KEY"},
      "models": {"model-a": {}, "model-b": {}}
    },
    "secondary": {
      "options": {"baseURL": "https://secondary.example/v1", "apiKeyEnv": "SECONDARY_KEY"},
      "models": {"model-c": {}}
    }
  }
}`
	env := map[string]string{
		"YORDAM_PROFILE":  "secondary",
		"YORDAM_MODEL":    "model-c",
		"YORDAM_BASE_URL": "https://override.example/v1/",
		"YORDAM_API_KEY":  "override-key",
		"SECONDARY_KEY":   "secondary-key",
	}
	cfg, err := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, body), LookupEnv: func(name string) (string, bool) {
		value, ok := env[name]
		return value, ok
	}})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := cfg.Resolve(config.ResolveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Name != "secondary" || resolved.Model != "model-c" || resolved.BaseURL != "https://override.example/v1" || resolved.APIKey != "override-key" {
		t.Fatalf("resolved=%+v", resolved)
	}

	resolved, err = cfg.Resolve(config.ResolveOptions{Profile: "primary", Model: "model-b", BaseURL: "https://cli.example/v1/"})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Name != "primary" || resolved.Model != "model-b" || resolved.BaseURL != "https://cli.example/v1" || resolved.APIKey != "override-key" {
		t.Fatalf("CLI resolved=%+v", resolved)
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "unknown top-level field", body: strings.Replace(validConfig, `"model":`, `"extra": true, "model":`, 1), want: `unknown field "extra"`},
		{name: "unknown provider field", body: strings.Replace(validConfig, `"name": "Primary",`, `"name": "Primary", "extra": true,`, 1), want: `unknown field "extra"`},
		{name: "unknown options field", body: strings.Replace(validConfig, `"baseURL":`, `"extra": true, "baseURL":`, 1), want: `unknown field "extra"`},
		{name: "unknown model field", body: strings.Replace(validConfig, `{"name": "Model A"}`, `{"name": "Model A", "extra": true}`, 1), want: `unknown field "extra"`},
		{name: "unknown limits field", body: strings.Replace(validConfig, `"maxToolCalls": 32`, `"extra": true, "maxToolCalls": 32`, 1), want: `unknown field "extra"`},
		{name: "missing provider map", body: strings.Replace(validConfig, `"provider": {`, `"providers": {`, 1), want: `unknown field "providers"`},
		{name: "malformed root model", body: strings.Replace(validConfig, `primary/model-a`, `model-a`, 1), want: `model must use provider/model format`},
		{name: "missing referenced provider", body: strings.Replace(validConfig, `primary/model-a`, `missing/model-a`, 1), want: `provider "missing" not found`},
		{name: "missing referenced model", body: strings.Replace(validConfig, `primary/model-a`, `primary/model-c`, 1), want: `model "model-c" not configured for "primary"`},
		{name: "reserved model", body: strings.ReplaceAll(validConfig, `model-a`, `your-model-id`), want: `model ID "your-model-id" is reserved`},
		{name: "credentialed URL", body: strings.Replace(validConfig, `https://llm.example/v1`, `https://user:pass@llm.example/v1`, 1), want: `provider "primary" has invalid baseURL`},
		{name: "non HTTP URL", body: strings.Replace(validConfig, `https://llm.example/v1`, `ftp://llm.example/v1`, 1), want: `provider "primary" has invalid baseURL`},
		{name: "invalid environment name", body: strings.Replace(validConfig, `PRIMARY_KEY`, `primary-key`, 1), want: `provider "primary" has invalid apiKeyEnv`},
		{name: "zero tool limit", body: strings.Replace(validConfig, `"maxToolCalls": 32`, `"maxToolCalls": 0`, 1), want: `maxToolCalls must be 1..128`},
		{name: "negative tool limit", body: strings.Replace(validConfig, `"maxToolCalls": 32`, `"maxToolCalls": -1`, 1), want: `maxToolCalls must be 1..128`},
		{name: "high tool limit", body: strings.Replace(validConfig, `"maxToolCalls": 32`, `"maxToolCalls": 129`, 1), want: `maxToolCalls must be 1..128`},
		{name: "zero timeout", body: strings.Replace(validConfig, `"shellTimeoutSeconds": 120`, `"shellTimeoutSeconds": 0`, 1), want: `shellTimeoutSeconds must be 1..1800`},
		{name: "negative timeout", body: strings.Replace(validConfig, `"shellTimeoutSeconds": 120`, `"shellTimeoutSeconds": -1`, 1), want: `shellTimeoutSeconds must be 1..1800`},
		{name: "high timeout", body: strings.Replace(validConfig, `"shellTimeoutSeconds": 120`, `"shellTimeoutSeconds": 1801`, 1), want: `shellTimeoutSeconds must be 1..1800`},
		{name: "empty provider ID", body: strings.Replace(validConfig, `"primary": {`, `"": {`, 1), want: `provider ID is empty`},
		{name: "empty model ID", body: strings.Replace(validConfig, `"model-b": {}`, `"": {}`, 1), want: `model ID is empty`},
		{name: "ambiguous API key", body: strings.Replace(validConfig, `"apiKeyEnv": "PRIMARY_KEY",`, `"apiKeyEnv": "PRIMARY_KEY", "apiKey": "secret",`, 1), want: `must configure only one of apiKey or apiKeyEnv`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, test.body)})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load() error=%v want containing %q", err, test.want)
			}
		})
	}
}

func TestLoadParseErrorDoesNotExposeMalformedLiteral(t *testing.T) {
	const secret = "sk-malformed-secret-value"
	body := strings.Replace(validConfig, `"apiKeyEnv": "PRIMARY_KEY"`, `"apiKeyEnv": `+secret, 1)
	_, err := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, body)})
	if err == nil {
		t.Fatal("Load() accepted malformed JSONC")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("parse error exposed config literal: %v", err)
	}
}

func TestResolveRejectsUnknownModel(t *testing.T) {
	cfg, err := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, validConfig)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.Resolve(config.ResolveOptions{Model: "unknown"}); err == nil || !strings.Contains(err.Error(), `model "unknown" not configured for "primary"`) {
		t.Fatalf("unknown model error=%v", err)
	}
}

func TestDefaultPaths(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skipf("unsupported test platform %s", runtime.GOOS)
	}
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "ignored-config"))

	gotConfig, err := config.DefaultConfigPath()
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(home, ".config", "yordam", "config.jsonc"); gotConfig != want {
		t.Fatalf("DefaultConfigPath()=%q want=%q", gotConfig, want)
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.jsonc")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
