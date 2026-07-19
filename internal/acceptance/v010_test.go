//go:build acceptance

package acceptance_test

import "testing"

func TestV010Acceptance(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{"single_binary_startup", acceptSingleBinaryStartup},
		{"secret_free_config_reload", acceptSecretFreeConfigReload},
		{"scripted_four_tool_turn", acceptFourToolTurn},
		{"ask_allow_and_deny", acceptAskPaths},
		{"safe_blocks_mutations", acceptSafeMode},
		{"auto_scope_and_shell_ack", acceptAutoMode},
		{"exact_diff_and_stale_preimage", acceptEditSafety},
		{"cancel_kills_process_group", acceptShellCancellation},
		{"continue_reconstructs_session", acceptResume},
		{"truncated_tail_recovery", acceptRecovery},
		{"model_change_next_turn", acceptModelChange},
		{"git_and_non_git", acceptWorkspaceKinds},
		{"four_release_targets", acceptReleaseTargets},
		{"secret_hygiene", acceptSecretHygiene},
	}
	for _, test := range tests {
		t.Run(test.name, test.run)
	}
}
