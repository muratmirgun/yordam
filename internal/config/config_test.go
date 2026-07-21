package config_test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/domain"
)

func TestResolveOptionsDoesNotAcceptProcessOnlyCredential(t *testing.T) {
	if _, exists := reflect.TypeOf(config.ResolveOptions{}).FieldByName("ProcessAPIKey"); exists {
		t.Fatal("ResolveOptions still accepts a process-only API key")
	}
}

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

func withSubagents(body, subagents string) string {
	return strings.Replace(body, "\n}", "\n  \"subagents\": "+subagents+"\n}", 1)
}

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

func TestLoadSubagentDefaultsAndExplicitDisable(t *testing.T) {
	tests := []struct {
		name string
		body string
		want config.SubagentConfig
	}{
		{name: "omitted", body: validConfig, want: config.SubagentConfig{Enabled: true, MaxPerTurn: 4, MaxToolCalls: 16, TimeoutSeconds: 600}},
		{name: "disabled", body: withSubagents(validConfig, `{"enabled": false, "maxPerTurn": 4, "maxToolCalls": 16, "timeoutSeconds": 600}`), want: config.SubagentConfig{Enabled: false, MaxPerTurn: 4, MaxToolCalls: 16, TimeoutSeconds: 600}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, test.body)})
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Subagents != test.want {
				t.Fatalf("subagents=%+v want=%+v", cfg.Subagents, test.want)
			}
		})
	}
}

func TestLoadRejectsInvalidSubagentConfiguration(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "unknown field", body: withSubagents(validConfig, `{"extra": true}`), want: `unknown field "extra"`},
		{name: "null object", body: withSubagents(validConfig, `null`), want: "subagents must not be null"},
		{name: "null enabled", body: withSubagents(validConfig, `{"enabled": null}`), want: "enabled must not be null"},
		{name: "null max per turn", body: withSubagents(validConfig, `{"maxPerTurn": null}`), want: "maxPerTurn must not be null"},
		{name: "null max tool calls", body: withSubagents(validConfig, `{"maxToolCalls": null}`), want: "maxToolCalls must not be null"},
		{name: "null timeout", body: withSubagents(validConfig, `{"timeoutSeconds": null}`), want: "timeoutSeconds must not be null"},
		{name: "wrong type", body: withSubagents(validConfig, `{"enabled": "true"}`), want: "cannot unmarshal string into Go value of type bool"},
		{name: "below max per turn", body: withSubagents(validConfig, `{"enabled": false, "maxPerTurn": 0}`), want: "maxPerTurn must be 1..4"},
		{name: "above max per turn", body: withSubagents(validConfig, `{"enabled": false, "maxPerTurn": 5}`), want: "maxPerTurn must be 1..4"},
		{name: "below max tool calls", body: withSubagents(validConfig, `{"enabled": false, "maxToolCalls": 0}`), want: "subagent maxToolCalls must be 1..64"},
		{name: "above max tool calls", body: withSubagents(validConfig, `{"enabled": false, "maxToolCalls": 65}`), want: "subagent maxToolCalls must be 1..64"},
		{name: "below timeout", body: withSubagents(validConfig, `{"enabled": false, "timeoutSeconds": 0}`), want: "subagent timeoutSeconds must be 1..1800"},
		{name: "above timeout", body: withSubagents(validConfig, `{"enabled": false, "timeoutSeconds": 1801}`), want: "subagent timeoutSeconds must be 1..1800"},
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

func TestLoadContextCompactionDefaults(t *testing.T) {
	cfg, err := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, validConfig)})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Context.AutoCompact || cfg.Context.CompactReserveTokens != nil {
		t.Fatalf("context defaults=%+v", cfg.Context)
	}
}

func TestLoadSkillProjectPolicy(t *testing.T) {
	tests := []struct {
		name string
		body string
		want config.ProjectSkillPolicy
	}{
		{name: "omitted defaults to ask", body: validConfig, want: config.ProjectSkillsAsk},
		{name: "ask", body: withSkills(validConfig, `{"projectPolicy": "ask"}`), want: config.ProjectSkillsAsk},
		{name: "allow", body: withSkills(validConfig, `{"projectPolicy": "allow"}`), want: config.ProjectSkillsAllow},
		{name: "deny", body: withSkills(validConfig, `{"projectPolicy": "deny"}`), want: config.ProjectSkillsDeny},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, test.body)})
			if err != nil {
				t.Fatal(err)
			}
			if got := cfg.Skills.ProjectPolicy; got != test.want {
				t.Fatalf("project policy=%q want=%q", got, test.want)
			}
		})
	}
}

func TestLoadRejectsInvalidSkillProjectPolicy(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "empty", body: withSkills(validConfig, `{"projectPolicy": ""}`), want: "projectPolicy must be ask, allow, or deny"},
		{name: "mixed case", body: withSkills(validConfig, `{"projectPolicy": "Allow"}`), want: "projectPolicy must be ask, allow, or deny"},
		{name: "unknown", body: withSkills(validConfig, `{"projectPolicy": "always"}`), want: "projectPolicy must be ask, allow, or deny"},
		{name: "null", body: withSkills(validConfig, `{"projectPolicy": null}`), want: "projectPolicy must not be null"},
		{name: "unknown skills key", body: withSkills(validConfig, `{"extra": true}`), want: `unknown field "extra"`},
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

func TestLoadConfiguredContextWindow(t *testing.T) {
	cfg, err := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, withModelContextWindow(validConfig, "model-a", 128000))})
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Profiles["primary"].ModelContextWindows["model-a"]; got != 128000 {
		t.Fatalf("context window=%d", got)
	}
}

func TestLoadConfiguredContextCompaction(t *testing.T) {
	reserve := int64(8192)
	cfg, err := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, withContext(validConfig, `{"autoCompact": false, "compactReserveTokens": 8192}`))})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Context.AutoCompact || cfg.Context.CompactReserveTokens == nil || *cfg.Context.CompactReserveTokens != reserve {
		t.Fatalf("context=%+v", cfg.Context)
	}
}

func TestLoadRejectsInvalidContextConfiguration(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "context unknown field", body: withContext(validConfig, `{"extra": true}`), want: `unknown field "extra"`},
		{name: "zero context window", body: withModelContextWindow(validConfig, "model-a", 0), want: "contextWindow must be positive"},
		{name: "negative context window", body: withModelContextWindow(validConfig, "model-a", -1), want: "contextWindow must be positive"},
		{name: "zero compact reserve", body: withContext(validConfig, `{"compactReserveTokens": 0}`), want: "compactReserveTokens must be positive"},
		{name: "negative compact reserve", body: withContext(validConfig, `{"compactReserveTokens": -1}`), want: "compactReserveTokens must be positive"},
		{name: "reserve equals context window", body: withContext(withModelContextWindow(validConfig, "model-a", 128000), `{"compactReserveTokens": 128000}`), want: "compactReserveTokens must be less than configured contextWindow"},
		{name: "reserve exceeds context window", body: withContext(withModelContextWindow(validConfig, "model-a", 128000), `{"compactReserveTokens": 128001}`), want: "compactReserveTokens must be less than configured contextWindow"},
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
		Profile: "primary",
		Model:   "model-b",
		BaseURL: "https://cli.example/v1/",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Name != "primary" || resolved.Model != "model-b" || resolved.BaseURL != "https://cli.example/v1" || resolved.APIKey != "override-key" {
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

func withContext(body, context string) string {
	return strings.Replace(body, `"limits":`, `"context": `+context+`, "limits":`, 1)
}

func withSkills(body, skills string) string {
	return strings.Replace(body, `"limits":`, `"skills": `+skills+`, "limits":`, 1)
}

func withModelContextWindow(body, model string, window int64) string {
	return strings.Replace(body, `"`+model+`": {"name": "Model A"}`, `"`+model+`": {"name": "Model A", "contextWindow": `+fmt.Sprint(window)+`}`, 1)
}
