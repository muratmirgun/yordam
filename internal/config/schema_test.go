package config_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/tailscale/hujson"
)

func TestPublishedSchemaMatchesRuntimeShape(t *testing.T) {
	schemaPath := filepath.Join("..", "..", "schema", "config.json")
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	schema, err := compiler.Compile(schemaPath)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name        string
		body        string
		schemaValid bool
	}{
		{name: "valid JSONC", body: validConfig, schemaValid: true},
		{name: "direct API key", body: strings.Replace(validConfig, `"apiKeyEnv": "PRIMARY_KEY"`, `"apiKey": "direct-secret"`, 1), schemaValid: true},
		{name: "unknown field", body: strings.Replace(validConfig, `"model":`, `"extra": true, "model":`, 1)},
		{name: "invalid URL", body: strings.Replace(validConfig, `https://llm.example/v1`, `ftp://llm.example/v1`, 1)},
		{name: "user-info URL", body: strings.Replace(validConfig, `https://llm.example/v1`, `https://user:password@llm.example/v1`, 1)},
		{name: "invalid environment", body: strings.Replace(validConfig, `PRIMARY_KEY`, `primary-key`, 1)},
		{name: "ambiguous API key", body: strings.Replace(validConfig, `"apiKeyEnv": "PRIMARY_KEY"`, `"apiKeyEnv": "PRIMARY_KEY", "apiKey": "direct-secret"`, 1)},
		{name: "reserved sentinel", body: strings.ReplaceAll(validConfig, `model-a`, `your-model-id`)},
		{name: "out of range limit", body: strings.Replace(validConfig, `"maxToolCalls": 32`, `"maxToolCalls": 129`, 1)},
		{name: "null limits", body: strings.Replace(validConfig, `"limits": {"maxToolCalls": 32, "shellTimeoutSeconds": 120}`, `"limits": null`, 1)},
		{name: "null model entry", body: strings.Replace(validConfig, `"model-b": {}`, `"model-b": null`, 1)},
		{name: "missing referenced model is semantic", body: strings.Replace(validConfig, `primary/model-a`, `primary/model-c`, 1), schemaValid: true},
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
		})
	}

	if _, err := os.Stat(schemaPath); err != nil {
		t.Fatal(err)
	}
}
