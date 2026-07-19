package acceptance_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/muratmirgun/yordam/internal/compaction"
	"github.com/muratmirgun/yordam/internal/protocol"
)

// TestV030Compaction is intentionally runnable without build tags so release
// operators can execute the command documented in the v0.3 acceptance brief.
// Its subprocesses exercise production packages with deterministic provider
// streams and fault injection; none needs an external credential.
func TestV030Compaction(t *testing.T) {
	t.Run("manual_success_replay_and_immutable_source_prefix", func(t *testing.T) {
		assertV030SourcePrefixImmutable(t)
		runV030GoTest(t, "./internal/app", `^TestProductionLegacyCommandUsesApplicationProtocolAndRealCursorProjection$`)
	})
	t.Run("automatic_threshold_disabled_and_unknown_window", func(t *testing.T) {
		runV030GoTest(t, "./internal/orchestrator", `^TestAutomaticCompactionPolicyThresholds$`)
		runV030GoTest(t, "./internal/compaction", `^TestEvaluatePolicy$`)
	})
	t.Run("prior_summary_suffix_and_large_tool_references", func(t *testing.T) {
		runV030GoTest(t, "./internal/compaction", `^TestSelectRetainsSafetyFactsAndBoundsInputDeterministically$`)
		runV030GoTest(t, "./internal/context", `^TestContextPlan(ReconstructsLatestValidNativeSummary|AdaptsEventTranscriptAndLegacyCompaction)$`)
	})
	t.Run("provider_refusal_and_context_too_large_are_terminal_without_retry", func(t *testing.T) {
		runV030GoTest(t, "./internal/orchestrator", `^TestRunCompactionFaultsDoNotLeaveTheLaneHeldOrRepeatProviderEgress/provider_refusal$`)
		runV030GoTest(t, "./internal/orchestrator", `^TestAutomaticCompactionFailuresAreTerminalAndNeverRetry$`)
		runV030GoTest(t, "./internal/tui/components", `^TestContextTooLargeShowsCompactAction$`)
	})
	t.Run("cancellation_evidence_failure_and_commit_recovery_boundaries", func(t *testing.T) {
		runV030GoTest(t, "./internal/orchestrator", `^TestRunCompactionCancellationBeforeAndDuringStream$`)
		runV030GoTest(t, "./internal/orchestrator", `^TestRunCompactionFaultsDoNotLeaveTheLaneHeldOrRepeatProviderEgress/(evidence_failure|known_non_commit|commit_unknown)$`)
		runV030GoTest(t, "./internal/orchestrator", `^TestCommitUnknownResolution(ReturnsTypedUncertaintyWithoutResend|AdoptsProvenCommitForEveryCaller)$`)
	})
	t.Run("restart_reconstructs_verified_evidence_and_preserves_suffix", func(t *testing.T) {
		runV030GoTest(t, "./internal/context", `^Test(EvidenceSummaryResolverFailsClosedToEarlierValidCompaction|ContextPlanKeepsEarlierResolvableSummaryWhenLaterEvidenceIsMissing)$`)
		runV030GoTest(t, "./internal/session/jsonl", `^TestExplicitRecoveryPreservesTailAndCommitsDiagnostic$`)
	})
	t.Run("secret_absence_across_provider_evidence_events_tui_logs_and_public_errors", func(t *testing.T) {
		runV030GoTest(t, "./internal/orchestrator", `^TestProviderSplitSecretNeverAppearsInDurableEvents$`)
		runV030GoTest(t, "./internal/app", `^Test(PublishRedactsEquivalentTextFieldsWithActiveBinding|ProductionCompactProtocolSerializesCancelledContext)$`)
		runV030GoTest(t, "./internal/tui", `^TestConfiguredSecretPromptIsRedactedBeforeConversationRendering$`)
	})
	t.Run("public_documentation_contract", func(t *testing.T) {
		runV030GoTest(t, "./internal/repolint", `^TestV030CompactionDocumentation$`)
	})
}

func runV030GoTest(t *testing.T, pkg, pattern string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), "go", "test", pkg, "-run", pattern, "-count=1")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("package=%s pattern=%s: %v\n%s", pkg, pattern, err, output)
	}
}

func assertV030SourcePrefixImmutable(t *testing.T) {
	t.Helper()
	events := make([]protocol.EventRecord, 0, 6)
	for sequence := uint64(1); sequence <= 6; sequence++ {
		message := protocol.UserMessageV1{Content: fmt.Sprintf("journal message %d", sequence)}
		payload, err := json.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, protocol.EventRecord{
			Envelope: protocol.EventEnvelope{SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1, JournalKind: protocol.JournalSession, JournalID: "v030-session", SessionID: "v030-session", EventID: protocol.EventID(fmt.Sprintf("event-%d", sequence)), Seq: sequence, Kind: protocol.EventUserMessage, TransactionID: protocol.TransactionID(fmt.Sprintf("tx-%d", sequence)), Payload: payload},
			Decoded:  &message,
		})
	}
	before, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	beforeDigest := sha256.Sum256(before)
	head := protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "v030-session", CommitSeq: 6, TransactionID: "tx-6"}
	selection, err := compaction.Select(events, head, compaction.TriggerManual, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.SummarizedEventIDs) == 0 || len(selection.RetainedEventIDs) == 0 {
		t.Fatalf("selection does not preserve a compacted prefix and suffix: %+v", selection)
	}
	after, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	afterDigest := sha256.Sum256(after)
	if beforeDigest != afterDigest {
		t.Fatalf("source journal prefix changed: before=%x after=%x", beforeDigest, afterDigest)
	}
}
