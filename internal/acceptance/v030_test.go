//go:build acceptance

package acceptance_test

import (
	"slices"
	"testing"
)

var v030SelfHostedRuntimeCases = []struct {
	name string
	run  func(*testing.T)
}{
	{name: "compaction", run: TestV030Compaction},
	{name: "skills", run: TestV030Skills},
	{name: "subagent", run: TestV030Subagent},
	{name: "self_hosting", run: TestV030SelfHosting},
	{name: "failure_matrix", run: TestV030FailureMatrix},
	{name: "Foundation", run: func(t *testing.T) {
		assertV020NestsV010(t)
		TestV020Foundation(t)
	}},
}

// TestV030SelfHostedRuntime is the release-facing cumulative v0.3 umbrella.
func TestV030SelfHostedRuntime(t *testing.T) {
	want := []string{
		"compaction",
		"skills",
		"subagent",
		"self_hosting",
		"failure_matrix",
		"Foundation",
	}
	got := make([]string, 0, len(v030SelfHostedRuntimeCases))
	seen := make(map[string]struct{}, len(v030SelfHostedRuntimeCases))
	for _, test := range v030SelfHostedRuntimeCases {
		if test.name == "" || test.run == nil {
			t.Fatalf("incomplete v0.3 umbrella case: %+v", test)
		}
		if _, duplicate := seen[test.name]; duplicate {
			t.Fatalf("duplicate v0.3 umbrella case %q", test.name)
		}
		seen[test.name] = struct{}{}
		got = append(got, test.name)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("v0.3 umbrella inventory=%q want=%q", got, want)
	}
	for _, test := range v030SelfHostedRuntimeCases {
		t.Run(test.name, test.run)
	}
}
