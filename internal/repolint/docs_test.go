package repolint_test

import (
	"os"
	"strings"
	"testing"
)

func TestRequiredRepositoryDocuments(t *testing.T) {
	required := map[string][]string{
		"LICENSE":         {"Apache License", "Version 2.0, January 2004"},
		"README.md":       {"# Yordam", "a small, hackable agent for your terminal", "Shell execution is not sandboxed"},
		"CONTRIBUTING.md": {"go test ./...", "go test -race ./...", "go vet ./..."},
		"SECURITY.md":     {"Private vulnerability reporting", "Do not open a public issue"},
		"CHANGELOG.md":    {"## [Unreleased]", "Semantic Versioning"},
	}
	for path, needles := range required {
		raw, err := os.ReadFile("../../" + path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		for _, needle := range needles {
			if !strings.Contains(string(raw), needle) {
				t.Errorf("%s missing %q", path, needle)
			}
		}
	}
}
