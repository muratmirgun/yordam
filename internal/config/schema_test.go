package config_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/config"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/tailscale/hujson"
)

func TestPublishedSchemaMatchesRuntimeValidation(t *testing.T) {
	schemaPath := filepath.Join("..", "..", "schema", "config.json")
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	schema, err := compiler.Compile(schemaPath)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name         string
		body         string
		schemaValid  bool
		runtimeValid bool
	}{
		{name: "valid JSONC", body: validConfig, schemaValid: true, runtimeValid: true},
		{name: "top-level unknown field", body: strings.Replace(validConfig, `"model":`, `"extra": true, "model":`, 1)},
		{name: "provider unknown field", body: strings.Replace(validConfig, `"name": "Primary",`, `"name": "Primary", "extra": true,`, 1)},
		{name: "options unknown field", body: strings.Replace(validConfig, `"apiKeyEnv": "PRIMARY_KEY",`, `"apiKeyEnv": "PRIMARY_KEY", "apiKey": "literal",`, 1)},
		{name: "model unknown field", body: strings.Replace(validConfig, `{"name": "Model A"}`, `{"name": "Model A", "extra": true}`, 1)},
		{name: "limits unknown field", body: strings.Replace(validConfig, `"maxToolCalls": 32`, `"maxToolCalls": 32, "extra": true`, 1)},
		{name: "skills default", body: validConfig, schemaValid: true, runtimeValid: true},
		{name: "skills ask", body: withSkills(validConfig, `{"projectPolicy": "ask"}`), schemaValid: true, runtimeValid: true},
		{name: "skills allow", body: withSkills(validConfig, `{"projectPolicy": "allow"}`), schemaValid: true, runtimeValid: true},
		{name: "skills deny", body: withSkills(validConfig, `{"projectPolicy": "deny"}`), schemaValid: true, runtimeValid: true},
		{name: "skills empty", body: withSkills(validConfig, `{"projectPolicy": ""}`)},
		{name: "skills mixed case", body: withSkills(validConfig, `{"projectPolicy": "Allow"}`)},
		{name: "skills unknown policy", body: withSkills(validConfig, `{"projectPolicy": "always"}`)},
		{name: "skills null policy", body: withSkills(validConfig, `{"projectPolicy": null}`)},
		{name: "skills unknown field", body: withSkills(validConfig, `{"extra": true}`)},
		{name: "tool-call lower boundary", body: strings.Replace(validConfig, `"maxToolCalls": 32`, `"maxToolCalls": 1`, 1), schemaValid: true, runtimeValid: true},
		{name: "tool-call upper boundary", body: strings.Replace(validConfig, `"maxToolCalls": 32`, `"maxToolCalls": 128`, 1), schemaValid: true, runtimeValid: true},
		{name: "shell timeout lower boundary", body: strings.Replace(validConfig, `"shellTimeoutSeconds": 120`, `"shellTimeoutSeconds": 1`, 1), schemaValid: true, runtimeValid: true},
		{name: "shell timeout upper boundary", body: strings.Replace(validConfig, `"shellTimeoutSeconds": 120`, `"shellTimeoutSeconds": 1800`, 1), schemaValid: true, runtimeValid: true},
		{name: "tool-call below range", body: strings.Replace(validConfig, `"maxToolCalls": 32`, `"maxToolCalls": 0`, 1)},
		{name: "tool-call above range", body: strings.Replace(validConfig, `"maxToolCalls": 32`, `"maxToolCalls": 129`, 1)},
		{name: "shell timeout below range", body: strings.Replace(validConfig, `"shellTimeoutSeconds": 120`, `"shellTimeoutSeconds": 0`, 1)},
		{name: "shell timeout above range", body: strings.Replace(validConfig, `"shellTimeoutSeconds": 120`, `"shellTimeoutSeconds": 1801`, 1)},
		{name: "invalid URL", body: strings.Replace(validConfig, `https://llm.example/v1`, `ftp://llm.example/v1`, 1)},
		{name: "invalid environment name", body: strings.Replace(validConfig, `PRIMARY_KEY`, `primary-key`, 1)},
		{name: "malformed root model", body: strings.Replace(validConfig, `primary/model-a`, `model-a`, 1)},
		{name: "reserved sentinel", body: strings.ReplaceAll(validConfig, `model-a`, `your-model-id`)},
		{name: "missing referenced provider", body: strings.Replace(validConfig, `primary/model-a`, `missing/model-a`, 1), schemaValid: true, runtimeValid: false},
		{name: "missing referenced model", body: strings.Replace(validConfig, `primary/model-a`, `primary/model-c`, 1), schemaValid: true, runtimeValid: false},
		{name: "null schema", body: strings.Replace(exactConfig, `"$schema":"`+config.SchemaURL+`"`, `"$schema":null`, 1)},
		{name: "null root model", body: strings.Replace(exactConfig, `"model":"primary/model-a"`, `"model":null`, 1)},
		{name: "null provider map", body: strings.Replace(exactConfig, `"provider":`+exactProviderMap, `"provider":null`, 1)},
		{name: "null provider object", body: strings.Replace(exactConfig, exactProviderObject, `null`, 1)},
		{name: "null provider name", body: strings.Replace(exactConfig, `"name":"Primary"`, `"name":null`, 1)},
		{name: "null options object", body: strings.Replace(exactConfig, exactOptionsObject, `null`, 1)},
		{name: "null base URL", body: strings.Replace(exactConfig, `"baseURL":"https://llm.example/v1"`, `"baseURL":null`, 1)},
		{name: "null API key environment", body: strings.Replace(exactConfig, `"apiKeyEnv":"PRIMARY_KEY"`, `"apiKeyEnv":null`, 1)},
		{name: "null models object", body: strings.Replace(exactConfig, exactModelsObject, `null`, 1)},
		{name: "null model object", body: strings.Replace(exactConfig, exactModelObject, `null`, 1)},
		{name: "null model name", body: strings.Replace(exactConfig, `"name":"Model A"`, `"name":null`, 1)},
		{name: "null limits object", body: strings.Replace(exactConfig, exactLimitsObject, `null`, 1)},
		{name: "null max tool calls", body: strings.Replace(exactConfig, `"maxToolCalls":32`, `"maxToolCalls":null`, 1)},
		{name: "null shell timeout", body: strings.Replace(exactConfig, `"shellTimeoutSeconds":120`, `"shellTimeoutSeconds":null`, 1)},
		{name: "case-variant schema", body: strings.Replace(exactConfig, `"$schema":`, `"$SCHEMA":`, 1)},
		{name: "case-variant model", body: strings.Replace(exactConfig, `"model":`, `"MODEL":`, 1)},
		{name: "case-variant provider", body: strings.Replace(exactConfig, `"provider":`, `"PROVIDER":`, 1)},
		{name: "case-variant limits", body: strings.Replace(exactConfig, `"limits":`, `"LIMITS":`, 1)},
		{name: "case-variant provider name", body: strings.Replace(exactConfig, `"name":"Primary"`, `"NAME":"Primary"`, 1)},
		{name: "case-variant options", body: strings.Replace(exactConfig, `"options":`, `"OPTIONS":`, 1)},
		{name: "case-variant models", body: strings.Replace(exactConfig, `"models":`, `"MODELS":`, 1)},
		{name: "case-variant base URL", body: strings.Replace(exactConfig, `"baseURL":`, `"BASEURL":`, 1)},
		{name: "case-variant API key environment", body: strings.Replace(exactConfig, `"apiKeyEnv":`, `"APIKEYENV":`, 1)},
		{name: "case-variant model name", body: strings.Replace(exactConfig, `"name":"Model A"`, `"NAME":"Model A"`, 1)},
		{name: "case-variant max tool calls", body: strings.Replace(exactConfig, `"maxToolCalls":`, `"MAXTOOLCALLS":`, 1)},
		{name: "case-variant shell timeout", body: strings.Replace(exactConfig, `"shellTimeoutSeconds":`, `"SHELLTIMEOUTSECONDS":`, 1)},
		{name: "case-variant skill policy", body: withSkills(exactConfig, `{"projectPolicy":"ASK"}`)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, err := hujson.Parse([]byte(test.body))
			if err != nil {
				t.Fatal(err)
			}
			value.Standardize()
			var instance any
			if err := json.Unmarshal(value.Pack(), &instance); err != nil {
				t.Fatal(err)
			}
			if err := schema.Validate(instance); (err == nil) != test.schemaValid {
				t.Fatalf("schema error=%v want valid=%t", err, test.schemaValid)
			}

			_, runtimeErr := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, test.body)})
			if (runtimeErr == nil) != test.runtimeValid {
				t.Fatalf("runtime error=%v want valid=%t", runtimeErr, test.runtimeValid)
			}
		})
	}
}

func TestSchemaContextParity(t *testing.T) {
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	schema, err := compiler.Compile(filepath.Join("..", "..", "schema", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name         string
		body         string
		schemaValid  bool
		runtimeValid bool
	}{
		{name: "context defaults", body: validConfig, schemaValid: true, runtimeValid: true},
		{name: "configured context", body: withContext(withModelContextWindow(validConfig, "model-a", 128000), `{"autoCompact": false, "compactReserveTokens": 8192}`), schemaValid: true, runtimeValid: true},
		{name: "unknown context field", body: withContext(validConfig, `{"extra": true}`)},
		{name: "zero context window", body: withModelContextWindow(validConfig, "model-a", 0)},
		{name: "negative context window", body: withModelContextWindow(validConfig, "model-a", -1)},
		{name: "zero reserve", body: withContext(validConfig, `{"compactReserveTokens": 0}`)},
		{name: "negative reserve", body: withContext(validConfig, `{"compactReserveTokens": -1}`)},
		{name: "reserve equals context window", body: withContext(withModelContextWindow(validConfig, "model-a", 128000), `{"compactReserveTokens": 128000}`), schemaValid: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, err := hujson.Parse([]byte(test.body))
			if err != nil {
				t.Fatal(err)
			}
			value.Standardize()
			var instance any
			if err := json.Unmarshal(value.Pack(), &instance); err != nil {
				t.Fatal(err)
			}
			if err := schema.Validate(instance); (err == nil) != test.schemaValid {
				t.Fatalf("schema error=%v want valid=%t", err, test.schemaValid)
			}
			_, runtimeErr := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, test.body)})
			if (runtimeErr == nil) != test.runtimeValid {
				t.Fatalf("runtime error=%v want valid=%t", runtimeErr, test.runtimeValid)
			}
		})
	}
}

func TestSchemaSubagentParity(t *testing.T) {
	compiler := jsonschema.NewCompiler()
	schema, err := compiler.Compile(filepath.Join("..", "..", "schema", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name         string
		body         string
		schemaValid  bool
		runtimeValid bool
	}{
		{name: "defaults", body: validConfig, schemaValid: true, runtimeValid: true},
		{name: "enabled bounds", body: withSubagents(validConfig, `{"enabled": true, "maxPerTurn": 4, "maxToolCalls": 64, "timeoutSeconds": 1800}`), schemaValid: true, runtimeValid: true},
		{name: "disabled bounds", body: withSubagents(validConfig, `{"enabled": false, "maxPerTurn": 1, "maxToolCalls": 1, "timeoutSeconds": 1}`), schemaValid: true, runtimeValid: true},
		{name: "unknown", body: withSubagents(validConfig, `{"extra": true}`)},
		{name: "null", body: withSubagents(validConfig, `null`)},
		{name: "wrong type", body: withSubagents(validConfig, `{"enabled": "true"}`)},
		{name: "max per turn low", body: withSubagents(validConfig, `{"maxPerTurn": 0}`)},
		{name: "max per turn high", body: withSubagents(validConfig, `{"maxPerTurn": 5}`)},
		{name: "tool calls low", body: withSubagents(validConfig, `{"maxToolCalls": 0}`)},
		{name: "tool calls high", body: withSubagents(validConfig, `{"maxToolCalls": 65}`)},
		{name: "timeout low", body: withSubagents(validConfig, `{"timeoutSeconds": 0}`)},
		{name: "timeout high", body: withSubagents(validConfig, `{"timeoutSeconds": 1801}`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, err := hujson.Parse([]byte(test.body))
			if err != nil {
				t.Fatal(err)
			}
			value.Standardize()
			var instance any
			if err := json.Unmarshal(value.Pack(), &instance); err != nil {
				t.Fatal(err)
			}
			if err := schema.Validate(instance); (err == nil) != test.schemaValid {
				t.Fatalf("schema error=%v want valid=%t", err, test.schemaValid)
			}
			_, runtimeErr := config.Load(config.LoadOptions{ConfigPath: writeConfig(t, test.body)})
			if (runtimeErr == nil) != test.runtimeValid {
				t.Fatalf("runtime error=%v want valid=%t", runtimeErr, test.runtimeValid)
			}
		})
	}
}
