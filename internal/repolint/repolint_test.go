package repolint_test

import (
	"os"
	"strings"
	"testing"
)

func TestV030CompactionDocumentation(t *testing.T) {
	for path, required := range map[string][]string{
		"README.md": {
			"## Context compaction",
			"/compact",
			"automatic compaction",
			"context too large",
			"summary evidence",
		},
		"SECURITY.md": {
			"Context compaction summaries",
			"not original evidence",
			"provider credentials",
		},
		"docs/releases/v0.3.0-compaction-acceptance.md": {
			"# Yordam v0.3.0 Context Compaction Acceptance",
			"Evidence provenance",
			"Exact verification commands",
		},
	} {
		raw, err := os.ReadFile("../../" + path)
		if err != nil {
			t.Errorf("%s: %v", path, err)
			continue
		}
		for _, needle := range required {
			if !strings.Contains(string(raw), needle) {
				t.Errorf("%s missing %q", path, needle)
			}
		}
	}
}
