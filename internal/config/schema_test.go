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
