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
  // comments and trailing commas are accepted
  "model": "primary/model-a",
  "provider": {
    "primary": {
      "name": "Primary",
      "options": {
        "baseURL": "https://llm.example/v1",
        "apiKeyEnv": "PRIMARY_KEY",
      },
      "models": {
        "model-a": {"name": "Model A"},
        "model-b": {},
      },
    },
  },
  "limits": {"maxToolCalls": 32, "shellTimeoutSeconds": 120},
}`

const (
	exactModelObject    = `{"name":"Model A"}`
	exactModelsObject   = `{"model-a":` + exactModelObject + `}`
	exactOptionsObject  = `{"baseURL":"https://llm.example/v1","apiKeyEnv":"PRIMARY_KEY"}`
	exactProviderObject = `{"name":"Primary","options":` + exactOptionsObject + `,"models":` + exactModelsObject + `}`
	exactProviderMap    = `{"primary":` + exactProviderObject + `}`
	exactLimitsObject   = `{"maxToolCalls":32,"shellTimeoutSeconds":120}`
	exactConfig         = `{"$schema":"` + config.SchemaURL + `","model":"primary/model-a","provider":` + exactProviderMap + `,"limits":` + exactLimitsObject + `}`
)

func TestLoadJSONCAndResolveProvider(t *testing.T) {
	path := writeConfig(t, validConfig)
	t.Setenv("PRIMARY_KEY", "secret-value")

	cfg, err := config.Load(config.LoadOptions{ConfigPath: path, LookupEnv: os.LookupEnv})
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.DefaultSelection(); got != (domain.ModelSelection{Profile: "primary", Model: "model-a"}) {
		t.Fatalf("default selection=%+v", got)
	}
	if got := cfg.Models(); !slices.Equal(got, []domain.ModelSelection{
		{Profile: "primary", Model: "model-a"},
		{Profile: "primary", Model: "model-b"},
	}) {
		t.Fatalf("models=%+v", got)
	}
	if got := cfg.ProviderKeyEnvironmentNames(); !slices.Equal(got, []string{"PRIMARY_KEY"}) {
		t.Fatalf("provider key environment names=%v", got)
	}
	if got := cfg.APIKeys(); got["primary"] != "secret-value" {
		t.Fatalf("API keys=%v", got)
	}
	resolved, err := cfg.Resolve(config.ResolveOptions{Model: "model-b"})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Name != "primary" || resolved.Model != "model-b" || resolved.BaseURL != "https://llm.example/v1" || resolved.APIKey != "secret-value" {
		t.Fatalf("resolved=%+v", resolved)
	}
	if strings.Contains(string(cfg.RawForTest()), "secret-value") {
		t.Fatal("secret copied into config")
	}
}

func TestLoadReportsJSONCLineAndColumn(t *testing.T) {
	_, err := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, "{\n  \"model\":,\n}")})
	if err == nil || !strings.Contains(err.Error(), "line 2") || !strings.Contains(err.Error(), "column") {
		t.Fatalf("syntax error=%v", err)
	}
}

func TestLoadAcceptsBlockComments(t *testing.T) {
	body := strings.Replace(validConfig, "// comments and trailing commas are accepted", "/* block comments are accepted */", 1)
	if _, err := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, body)}); err != nil {
		t.Fatal(err)
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

func TestLoadAppliesDefaultsOnlyToOmittedLimits(t *testing.T) {
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

func TestResolveOverridePrecedence(t *testing.T) {
	body := strings.Replace(validConfig, `
    },
  },`, `
    },
    "secondary": {
      "options": {"baseURL": "https://secondary.example/v1", "apiKeyEnv": "SECONDARY_KEY"},
      "models": {"model-c": {}}
    },
  },`, 1)
	environment := map[string]string{
		"YORDAM_PROFILE":  "secondary",
		"YORDAM_MODEL":    "model-c",
		"YORDAM_BASE_URL": "https://override.example/v1/",
		"YORDAM_API_KEY":  "override-key",
		"PRIMARY_KEY":     "primary-key",
		"SECONDARY_KEY":   "secondary-key",
	}
	lookup := func(name string) (string, bool) {
		value, ok := environment[name]
		return value, ok
	}
	cfg, err := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, body), LookupEnv: lookup})
	if err != nil {
		t.Fatal(err)
	}

	resolved, err := cfg.Resolve(config.ResolveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Name != "secondary" || resolved.Model != "model-c" || resolved.BaseURL != "https://override.example/v1" || resolved.APIKey != "override-key" {
		t.Fatalf("environment resolved=%+v", resolved)
	}
	resolved, err = cfg.Resolve(config.ResolveOptions{
		Profile:       "primary",
		Model:         "model-b",
		BaseURL:       "https://cli.example/v1/",
		ProcessAPIKey: "process-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Name != "primary" || resolved.Model != "model-b" || resolved.BaseURL != "https://cli.example/v1" || resolved.APIKey != "process-key" {
		t.Fatalf("CLI resolved=%+v", resolved)
	}
	if got := cfg.APIKeys(); got["primary"] != "primary-key" || got["secondary"] != "secondary-key" {
		t.Fatalf("provider keys incorrectly used selected override: %v", got)
	}

	delete(environment, "YORDAM_PROFILE")
	delete(environment, "YORDAM_MODEL")
	delete(environment, "YORDAM_BASE_URL")
	delete(environment, "YORDAM_API_KEY")
	resolved, err = cfg.Resolve(config.ResolveOptions{DefaultProfile: "secondary", DefaultModel: "model-c"})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Name != "secondary" || resolved.Model != "model-c" || resolved.APIKey != "secondary-key" {
		t.Fatalf("resumed selection=%+v", resolved)
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "top-level unknown field", body: strings.Replace(validConfig, `"model":`, `"extra": true, "model":`, 1), want: `unknown field "extra"`},
		{name: "provider unknown field", body: strings.Replace(validConfig, `"name": "Primary",`, `"name": "Primary", "extra": true,`, 1), want: `unknown field "extra"`},
		{name: "options unknown field", body: strings.Replace(validConfig, `"apiKeyEnv": "PRIMARY_KEY",`, `"apiKeyEnv": "PRIMARY_KEY", "apiKey": "literal-secret",`, 1), want: `unknown field "apiKey"`},
		{name: "model unknown field", body: strings.Replace(validConfig, `{"name": "Model A"}`, `{"name": "Model A", "extra": true}`, 1), want: `unknown field "extra"`},
		{name: "limits unknown field", body: strings.Replace(validConfig, `"maxToolCalls": 32`, `"extra": true, "maxToolCalls": 32`, 1), want: `unknown field "extra"`},
		{name: "absent provider", body: `{"model":"primary/model-a"}`, want: "provider must contain at least one entry"},
		{name: "malformed root model", body: strings.Replace(validConfig, `primary/model-a`, `model-a`, 1), want: "model must use provider/model format"},
		{name: "missing referenced provider", body: strings.Replace(validConfig, `primary/model-a`, `missing/model-a`, 1), want: `provider "missing" not found`},
		{name: "missing referenced model", body: strings.Replace(validConfig, `primary/model-a`, `primary/model-c`, 1), want: `model "model-c" not configured for "primary"`},
		{name: "reserved your-model-id", body: strings.ReplaceAll(validConfig, `model-a`, `your-model-id`), want: `model ID "your-model-id" is reserved`},
		{name: "user-info URL", body: strings.Replace(validConfig, `https://llm.example/v1`, `https://user:pass@llm.example/v1`, 1), want: `provider "primary" has invalid baseURL`},
		{name: "non-HTTP URL", body: strings.Replace(validConfig, `https://llm.example/v1`, `ftp://llm.example/v1`, 1), want: `provider "primary" has invalid baseURL`},
		{name: "invalid environment name", body: strings.Replace(validConfig, `PRIMARY_KEY`, `primary-key`, 1), want: `provider "primary" has invalid apiKeyEnv`},
		{name: "zero tool-call limit", body: strings.Replace(validConfig, `"maxToolCalls": 32`, `"maxToolCalls": 0`, 1), want: "maxToolCalls must be 1..128"},
		{name: "negative tool-call limit", body: strings.Replace(validConfig, `"maxToolCalls": 32`, `"maxToolCalls": -1`, 1), want: "maxToolCalls must be 1..128"},
		{name: "129 tool-call limit", body: strings.Replace(validConfig, `"maxToolCalls": 32`, `"maxToolCalls": 129`, 1), want: "maxToolCalls must be 1..128"},
		{name: "zero shell timeout", body: strings.Replace(validConfig, `"shellTimeoutSeconds": 120`, `"shellTimeoutSeconds": 0`, 1), want: "shellTimeoutSeconds must be 1..1800"},
		{name: "negative shell timeout", body: strings.Replace(validConfig, `"shellTimeoutSeconds": 120`, `"shellTimeoutSeconds": -1`, 1), want: "shellTimeoutSeconds must be 1..1800"},
		{name: "1801 shell timeout", body: strings.Replace(validConfig, `"shellTimeoutSeconds": 120`, `"shellTimeoutSeconds": 1801`, 1), want: "shellTimeoutSeconds must be 1..1800"},
		{name: "empty provider ID", body: strings.Replace(validConfig, `"primary": {`, `"": {`, 1), want: "provider ID is empty"},
		{name: "empty model ID", body: strings.Replace(validConfig, `"model-b": {}`, `"": {}`, 1), want: "model ID is empty"},
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

func TestLoadRejectsNullAtSchemaConstrainedLocations(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "schema", body: strings.Replace(exactConfig, `"$schema":"`+config.SchemaURL+`"`, `"$schema":null`, 1)},
		{name: "root model", body: strings.Replace(exactConfig, `"model":"primary/model-a"`, `"model":null`, 1)},
		{name: "provider map", body: strings.Replace(exactConfig, `"provider":`+exactProviderMap, `"provider":null`, 1)},
		{name: "provider object", body: strings.Replace(exactConfig, exactProviderObject, `null`, 1)},
		{name: "provider name", body: strings.Replace(exactConfig, `"name":"Primary"`, `"name":null`, 1)},
		{name: "options object", body: strings.Replace(exactConfig, exactOptionsObject, `null`, 1)},
		{name: "base URL", body: strings.Replace(exactConfig, `"baseURL":"https://llm.example/v1"`, `"baseURL":null`, 1)},
		{name: "API key environment", body: strings.Replace(exactConfig, `"apiKeyEnv":"PRIMARY_KEY"`, `"apiKeyEnv":null`, 1)},
		{name: "models object", body: strings.Replace(exactConfig, exactModelsObject, `null`, 1)},
		{name: "model object", body: strings.Replace(exactConfig, exactModelObject, `null`, 1)},
		{name: "model name", body: strings.Replace(exactConfig, `"name":"Model A"`, `"name":null`, 1)},
		{name: "limits object", body: strings.Replace(exactConfig, exactLimitsObject, `null`, 1)},
		{name: "max tool calls", body: strings.Replace(exactConfig, `"maxToolCalls":32`, `"maxToolCalls":null`, 1)},
		{name: "shell timeout", body: strings.Replace(exactConfig, `"shellTimeoutSeconds":120`, `"shellTimeoutSeconds":null`, 1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, test.body)})
			if err == nil || !strings.Contains(err.Error(), "must not be null") {
				t.Fatalf("Load() error=%v", err)
			}
		})
	}
}

func TestLoadRejectsCaseVariantKeys(t *testing.T) {
	tests := []struct {
		name string
		key  string
		body string
	}{
		{name: "schema", key: "$SCHEMA", body: strings.Replace(exactConfig, `"$schema":`, `"$SCHEMA":`, 1)},
		{name: "model", key: "MODEL", body: strings.Replace(exactConfig, `"model":`, `"MODEL":`, 1)},
		{name: "provider", key: "PROVIDER", body: strings.Replace(exactConfig, `"provider":`, `"PROVIDER":`, 1)},
		{name: "limits", key: "LIMITS", body: strings.Replace(exactConfig, `"limits":`, `"LIMITS":`, 1)},
		{name: "provider name", key: "NAME", body: strings.Replace(exactConfig, `"name":"Primary"`, `"NAME":"Primary"`, 1)},
		{name: "options", key: "OPTIONS", body: strings.Replace(exactConfig, `"options":`, `"OPTIONS":`, 1)},
		{name: "models", key: "MODELS", body: strings.Replace(exactConfig, `"models":`, `"MODELS":`, 1)},
		{name: "base URL", key: "BASEURL", body: strings.Replace(exactConfig, `"baseURL":`, `"BASEURL":`, 1)},
		{name: "API key environment", key: "APIKEYENV", body: strings.Replace(exactConfig, `"apiKeyEnv":`, `"APIKEYENV":`, 1)},
		{name: "model name", key: "NAME", body: strings.Replace(exactConfig, `"name":"Model A"`, `"NAME":"Model A"`, 1)},
		{name: "max tool calls", key: "MAXTOOLCALLS", body: strings.Replace(exactConfig, `"maxToolCalls":`, `"MAXTOOLCALLS":`, 1)},
		{name: "shell timeout", key: "SHELLTIMEOUTSECONDS", body: strings.Replace(exactConfig, `"shellTimeoutSeconds":`, `"SHELLTIMEOUTSECONDS":`, 1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, test.body)})
			if err == nil || !strings.Contains(err.Error(), `unknown field "`+test.key+`"`) {
				t.Fatalf("Load() error=%v", err)
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
