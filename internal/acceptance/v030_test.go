//go:build acceptance

package acceptance_test

import "testing"

// TestV030SelfHostedRuntime is the release-facing v0.3 success umbrella.
// The failure matrix and cumulative v0.1/v0.2 gates are added by their owning
// release tasks so this task cannot silently pretend unfinished work passed.
func TestV030SelfHostedRuntime(t *testing.T) {
	t.Run("self_hosting_success", TestV030SelfHosting)
}
