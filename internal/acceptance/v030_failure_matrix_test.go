//go:build acceptance

package acceptance_test

import (
	"fmt"
	"slices"
	"testing"
)

type v030FailureCoverage struct {
	pkg  string
	test string
}

type v030FailureCase struct {
	name     string
	coverage []v030FailureCoverage
}

// v030FailureMatrix is the release inventory for the failure boundaries named
// in the v0.3 self-hosting plan. Each entry points at the smallest existing
// deterministic test that owns the relevant production boundary. This keeps
// the release gate named and auditable without cloning fault-only mechanisms
// into the acceptance package.
var v030FailureMatrix = []v030FailureCase{
	{
		name: "project_trust_denied",
		coverage: []v030FailureCoverage{
			{pkg: "./internal/skills", test: "TestBuildResolvesPolicyTrustAndShadowing"},
			{pkg: "./internal/skills", test: "TestTrustProjectorProjectsExactHistoryAndResolvesNewestMatch"},
		},
	},
	{
		name: "malformed_oversized_and_symlinked_project_skill",
		coverage: []v030FailureCoverage{
			{pkg: "./internal/skills", test: "TestParseRejectsOversizedRawInput"},
			{pkg: "./internal/skills", test: "TestDiscoverRejectsLinksAndSpecialFilesWithoutLeakingContent"},
			{pkg: "./internal/skills", test: "TestDiscoverRejectsFileInspectOpenFIFOAndSymlinkReplacement"},
		},
	},
	{
		name: "compaction_provider_failure_and_context_too_large_without_retry",
		coverage: []v030FailureCoverage{
			{pkg: "./internal/orchestrator", test: "TestRunCompactionFaultsDoNotLeaveTheLaneHeldOrRepeatProviderEgress"},
			{pkg: "./internal/orchestrator", test: "TestAutomaticCompactionFailuresAreTerminalAndNeverRetry"},
			{pkg: "./internal/tui/components", test: "TestContextTooLargeShowsCompactAction"},
		},
	},
	{
		name: "evidence_persistence_failure_with_inactive_orphan_summary",
		coverage: []v030FailureCoverage{
			{pkg: "./internal/orchestrator", test: "TestRunCompactionFaultsDoNotLeaveTheLaneHeldOrRepeatProviderEgress"},
			{pkg: "./internal/context", test: "TestEvidenceSummaryResolverFailsClosedToEarlierValidCompaction"},
		},
	},
	{
		name: "child_timeout_during_provider_stream",
		coverage: []v030FailureCoverage{
			{pkg: "./internal/orchestrator", test: "TestSubagentCancelDuringChildProviderStreamCommitsOneCancelledReceipt"},
		},
	},
	{
		name: "esc_cancellation_during_child_shell_process_group",
		coverage: []v030FailureCoverage{
			{pkg: "./internal/ptytest", test: "TestEscCancellationTerminatesShellProcessGroup"},
			{pkg: "./internal/orchestrator", test: "TestSubagentCancelDuringChildShellProcessStopsProcessAndWritesOneReceipt"},
		},
	},
	{
		name: "crash_after_parent_wait_before_child_creation",
		coverage: []v030FailureCoverage{
			{pkg: "./internal/session/jsonl", test: "TestCrossProcessTurnLeaseAllowsOneProcess"},
			{pkg: "./internal/subagent", test: "TestSubagentRecoverRestartAndCancellationMatrix"},
			{pkg: "./internal/orchestrator", test: "TestSubagentRecoveryCreatesReservedChildOnceAndContinuesParent"},
		},
	},
	{
		name: "crash_during_child_read_only_activity",
		coverage: []v030FailureCoverage{
			{pkg: "./internal/session/jsonl", test: "TestCrossProcessTurnLeaseAllowsOneProcess"},
			{pkg: "./internal/subagent", test: "TestSubagentRecoverNoEffectChildIsCancelledButUnprovenEffectIsUncertain"},
			{pkg: "./internal/orchestrator", test: "TestSubagentRecoverNoEffectChildWritesCancelledReceipt"},
		},
	},
	{
		name: "crash_during_unproven_child_mutation",
		coverage: []v030FailureCoverage{
			{pkg: "./internal/session/jsonl", test: "TestCrossProcessTurnLeaseAllowsOneProcess"},
			{pkg: "./internal/subagent", test: "TestSubagentRecoverNoEffectChildIsCancelledButUnprovenEffectIsUncertain"},
			{pkg: "./internal/orchestrator", test: "TestRecoveryTerminalizesUncertainSubagentWithStructuredNonRetryableDiagnostic"},
		},
	},
	{
		name: "crash_after_child_receipt_before_parent_attachment",
		coverage: []v030FailureCoverage{
			{pkg: "./internal/session/jsonl", test: "TestCrossProcessTurnLeaseAllowsOneProcess"},
			{pkg: "./internal/subagent", test: "TestSubagentRecoverTerminalReceiptNeedsAttachmentBeforeParentCanResume"},
			{pkg: "./internal/orchestrator", test: "TestSubagentRecoveryCreatesReservedChildOnceAndContinuesParent"},
		},
	},
	{
		name: "crash_after_attachment_before_parent_continuation",
		coverage: []v030FailureCoverage{
			{pkg: "./internal/session/jsonl", test: "TestCrossProcessTurnLeaseAllowsOneProcess"},
			{pkg: "./internal/subagent", test: "TestSubagentRecoverAttachmentIsTheOnlyResumableState"},
			{pkg: "./internal/orchestrator", test: "TestRecoveryReconstructsExactSubagentToolResultContinuation"},
		},
	},
	{
		name: "parent_attachment_commit_unknown",
		coverage: []v030FailureCoverage{
			{pkg: "./internal/orchestrator", test: "TestCommitUnknownResolutionAdoptsProvenCommitForEveryCaller"},
			{pkg: "./internal/orchestrator", test: "TestCommitUnknownResolutionReturnsTypedUncertaintyWithoutResend"},
		},
	},
	{
		name: "restart_with_changed_project_skill_digest_awaiting_new_trust",
		coverage: []v030FailureCoverage{
			{pkg: "./internal/skills", test: "TestBuildDigestRevisionValidationAndImmutability"},
			{pkg: "./internal/skills", test: "TestTrustProjectorProjectsExactHistoryAndResolvesNewestMatch"},
		},
	},
}

func TestV030FailureMatrix(t *testing.T) {
	want := []string{
		"project_trust_denied",
		"malformed_oversized_and_symlinked_project_skill",
		"compaction_provider_failure_and_context_too_large_without_retry",
		"evidence_persistence_failure_with_inactive_orphan_summary",
		"child_timeout_during_provider_stream",
		"esc_cancellation_during_child_shell_process_group",
		"crash_after_parent_wait_before_child_creation",
		"crash_during_child_read_only_activity",
		"crash_during_unproven_child_mutation",
		"crash_after_child_receipt_before_parent_attachment",
		"crash_after_attachment_before_parent_continuation",
		"parent_attachment_commit_unknown",
		"restart_with_changed_project_skill_digest_awaiting_new_trust",
	}
	got := make([]string, 0, len(v030FailureMatrix))
	seen := make(map[string]struct{}, len(v030FailureMatrix))
	for _, test := range v030FailureMatrix {
		if _, duplicate := seen[test.name]; duplicate {
			t.Fatalf("duplicate failure matrix case %q", test.name)
		}
		seen[test.name] = struct{}{}
		got = append(got, test.name)
		if len(test.coverage) == 0 {
			t.Fatalf("failure matrix case %q has no production coverage", test.name)
		}
		for index, coverage := range test.coverage {
			if coverage.pkg == "" || coverage.test == "" {
				t.Fatalf("failure matrix case %q coverage[%d] is incomplete: %+v", test.name, index, coverage)
			}
			if coverage.test[0] == '^' || coverage.test[len(coverage.test)-1] == '$' {
				t.Fatalf("failure matrix case %q must name an exact top-level test, got %q", test.name, coverage.test)
			}
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("failure matrix inventory=%q want=%q", got, want)
	}

	for _, test := range v030FailureMatrix {
		t.Run(test.name, func(t *testing.T) {
			for _, coverage := range test.coverage {
				t.Run(fmt.Sprintf("%s/%s", coverage.pkg, coverage.test), func(t *testing.T) {
					runV030GoTest(t, coverage.pkg, coverage.test)
				})
			}
		})
	}
}
