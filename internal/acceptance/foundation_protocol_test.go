//go:build acceptance

package acceptance_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
	"github.com/muratmirgun/yordam/internal/testsupport/foundationfixture"
)

const (
	traceMigration   = "PRD-FR-11; Capability 4.5; Foundation 9; Acceptance 6.1"
	traceConcurrency = "PRD-FR-03; Capability 4.2; Foundation 8.2-8.4; Acceptance 6.2"
	traceOrdering    = "PRD-FR-03; Capability 4.2; Foundation 12.3; Acceptance 6.3"
	traceEvidence    = "PRD-FR-03/04/05; Capability 3.2/3.4; Foundation 10; Acceptance 6.1"
	traceCatalog     = "PRD-FR-06/07; Capability 3.3/3.5; Foundation 14-15"
	traceApplication = "PRD-FR-02/03/10; Capability 3.9/4.3; Foundation 16; Acceptance 6.2"
)

func acceptFoundationMigration(t *testing.T) {
	t.Logf("trace=%s", traceMigration)
	if protocol.EnvelopeVersion != 2 {
		t.Fatalf("[%s] journal envelope version=%d want=2", traceMigration, protocol.EnvelopeVersion)
	}
	validateFoundationFixtures(t)
	runFoundationGoTest(t, traceMigration, "./internal/session/jsonl", `^Test(FoundationFixtureInventory|FoundationFixtureInspectionMatrix|InspectSessionNeverMutatesFixture|LoadNeverMutatesFixture|V1UpcastPreservesOrderingAndDerivesTurnScopedCalls|V1UpcastLeavesMissingFactsUnknown|V1UpcastDuplicateStartsAndTerminalsBecomeUncertain|V1UpcastUnmatchedStartsNeverBecomeSuccess|V1UpcastAcceptsRecoveryGeneratedInterruptionAndLegacyEvidence|MixedV1V2KeepsOneOrderedCursorModel)$`)
}

func acceptFoundationConcurrency(t *testing.T) {
	t.Logf("trace=%s", traceConcurrency)
	acceptCrossProcessSameHeadWriters(t)
	runFoundationGoTest(t, traceConcurrency, "./internal/session/jsonl", `^Test(CrossProcessTurnLeaseAllowsOneProcess|UnrelatedSessionsAppendConcurrently|ReadRangeStartsStrictlyAfterCursor|LookupTransactionSurvivesLaterCommits)$`)
	runFoundationGoTest(t, traceConcurrency, "./internal/orchestrator", `^Test(RunTurnConcurrentDuplicateExecutesProviderExactlyOnce|RunControlConcurrentDuplicateDispatchesExactlyOnce|ConcurrentSameCommandPureAndSessionChangesCommitOneLifecycle)$`)
	runFoundationGoTest(t, traceConcurrency, "./internal/authorization", `^Test(DispatchGateRejectsZeroRepeatedRevokedAndConcurrentUseBeforeCallback|DispatchRevokeRacingStartHasOneLinearizedOutcome)$`)
}

func acceptFoundationOrdering(t *testing.T) {
	t.Logf("trace=%s", traceOrdering)
	turnBarriers := []string{
		"command_accepted", "goal_draft_committed", "contract_frozen", "provider_authorization_committed",
		"action_plan_committed", "checkpoint_ready", "resources_revalidated", "authorization_committed",
		"activity_started_committed", "effect_dispatch", "action_terminal_committed", "provider_continuation",
		"verification_committed", "turn_terminal_committed", "command_completed",
	}
	for _, barrier := range turnBarriers {
		for _, phase := range []string{"before", "after"} {
			t.Run("logical/"+barrier+"/"+phase, func(t *testing.T) {
				runFoundationGoTest(t, traceOrdering, "./internal/orchestrator", "^TestEveryTurnBarrierBeforeAndAfterLeavesNoDurableOrphan/"+barrier+"_"+phase+"$")
			})
		}
	}
	for _, phase := range []string{"before", "after"} {
		t.Run("logical/recovery_turn_terminal_committed/"+phase, func(t *testing.T) {
			runFoundationGoTest(t, traceOrdering, "./internal/orchestrator", "^TestEveryRecoveryDispatchAndTerminalBarrierLeavesDurableControlTerminal/recovery_turn_terminal_committed_"+phase+"$")
		})
	}

	appendWindows := []struct {
		point  string
		before string
		after  string
	}{
		{"event_write", `^TestAppendBatchRequiresEncoderBeforeAnyWrite$`, `^TestPreMarkerFaultsRequireRecoveryAndRemainUnknown/event_write$`},
		{"event_sync", `^TestPreMarkerFaultsRequireRecoveryAndRemainUnknown/event_write$`, `^TestPreMarkerFaultsRequireRecoveryAndRemainUnknown/event_sync$`},
		{"marker_write", `^TestPreMarkerFaultsRequireRecoveryAndRemainUnknown/event_sync$`, `^TestPostMarkerFaultsReturnCommitUnknownAndResolveByLookup/marker_write$`},
		{"marker_sync", `^TestPostMarkerFaultsReturnCommitUnknownAndResolveByLookup/marker_write$`, `^TestPostMarkerFaultsReturnCommitUnknownAndResolveByLookup/marker_sync$`},
		{"metadata_write", `^TestPostMarkerFaultsReturnCommitUnknownAndResolveByLookup/marker_sync$`, `^TestPostMarkerFaultsReturnCommitUnknownAndResolveByLookup/metadata_write$`},
		{"metadata_rename", `^TestPostMarkerFaultsReturnCommitUnknownAndResolveByLookup/metadata_write$`, `^TestPostMarkerFaultsReturnCommitUnknownAndResolveByLookup/metadata_rename$`},
		{"directory_sync", `^TestPostMarkerFaultsReturnCommitUnknownAndResolveByLookup/metadata_rename$`, `^TestPostMarkerFaultsReturnCommitUnknownAndResolveByLookup/directory_sync$`},
		{"committed_view_sync", `^TestPostMarkerFaultsReturnCommitUnknownAndResolveByLookup/directory_sync$`, `^TestInspectAndHeadDoNotExposeMarkerWhenDurabilityResolutionFails$`},
	}
	for _, window := range appendWindows {
		for phase, pattern := range map[string]string{"before": window.before, "after": window.after} {
			t.Run("append/"+window.point+"/"+phase, func(t *testing.T) {
				runFoundationGoTest(t, traceOrdering, "./internal/session/jsonl", pattern)
			})
		}
	}

	recoveryPoints := []string{
		"quarantine_write", "quarantine_sync", "candidate_write", "candidate_sync", "recovery_metadata_write",
		"recovery_metadata_sync", "candidate_validate", "candidate_activate", "recovery_directory_sync", "recovery_diagnostic_commit",
	}
	for _, point := range recoveryPoints {
		for occurrence, phase := range []string{"before", "after"} {
			t.Run("recovery/"+point+"/"+phase, func(t *testing.T) {
				pattern := "^TestExplicitRecoveryIsRestartIdempotentAcrossEveryFaultProbe/" + point + "/occurrence-" + strconv.Itoa(occurrence+1) + "$"
				runFoundationGoTest(t, traceOrdering, "./internal/session/jsonl", pattern)
			})
		}
	}
	runFoundationGoTest(t, traceOrdering, "./internal/orchestrator", `^Test(DurableOrderingMutationPreviewCheckpointRevalidationExecutionAndContinuation|EvidenceMetadataMismatchTerminalizesBeforeContinuation|RecoveryMaterialFailureCommitsCheckpointFailedAndPreventsDispatch|StartedCommitFaultPreventsTokenIssuanceAndProviderDispatch)$`)
}

func acceptFoundationEvidence(t *testing.T) {
	t.Logf("trace=%s", traceEvidence)
	validateEvidenceLineageFixtureExpectations(t)
	runFoundationGoTest(t, traceEvidence, "./internal/evidence", `^Test(TwoSessionsResolveOneImmutableBlob|EvidencePreservesCandidateProvenance|EvidenceProjectsMissingBlobAndCannotVerify|EvidenceDetectsDigestMismatchAndProjectsCorrupt|EvidenceWithholdsEveryRegisteredVariantAndWritesNoSecret)$`)
	runFoundationGoTest(t, traceEvidence, "./internal/session/jsonl", `^Test(LineageComposesOnlyAnchoredParentPrefix|LineageRejectsMissingParentCycleAndDepth65|ExplicitRecoveryPreservesTailAndCommitsDiagnostic)$`)
}

func acceptFoundationCatalogAuthorization(t *testing.T) {
	t.Logf("trace=%s", traceCatalog)
	runFoundationGoTest(t, traceCatalog, "./internal/provider", `^Test(RequiredUnknownCapabilityFailsBeforeProvider|CatalogRejectsLowerPrecedenceCapabilityContradiction|CatalogRejectsContradictoryOrUnprovenancedCandidateFacts|DispatchProviderConcurrentDoubleDispatchSendsOneAttempt)$`)
	runFoundationGoTest(t, traceCatalog, "./internal/tooling", `^Test(CatalogRejectsAliasTrustSpoof|CatalogRejectsAliasCanonicalIdentityAndDescriptorDigestCollisions|DispatchToolConcurrentDoubleExecuteProducesOneEffect|RevalidateReportsChangedEditCanonicalScopeWithoutDispatch)$`)
	runFoundationGoTest(t, traceCatalog, "./internal/authorization", `^Test(DecisionBindingRejectsEveryRequestFieldMutation|AuthorizationIssuerRejectsExpiredAndRepeatedDecision|DispatchGateRejectsZeroRepeatedRevokedAndConcurrentUseBeforeCallback)$`)
}

func acceptFoundationApplicationProtocol(t *testing.T) {
	t.Logf("trace=%s", traceApplication)
	if protocol.ApplicationProtocolVersion != 1 {
		t.Fatalf("[%s] application protocol version=%d want=1", traceApplication, protocol.ApplicationProtocolVersion)
	}
	runFoundationGoTest(t, traceApplication, "./internal/app", `^Test(SnapshotAndSubscribeHasNoCommitGap|SubscriptionCatchUpMergesByTimestampThenEventID|SubscriptionSelectedSessionSwitchStartsNewSessionAtOrigin|TransientEpochGapRequiresFreshSnapshot|SlowConsumerOverflowIsBoundedAndResumable|SlowConsumerFakeHeadlessCancellationRunsOnce|ApplicationLegacyAdapterPreservesCommandAndRedactedEventSemantics|CommandIdempotencyReplaysDurableResultAndConflictsOnChangedPrincipal)$`)
	runFoundationGoTest(t, traceApplication, "./internal/projection", `^TestRebuildAfterSnapshotDeletionOrVersionMismatchIsCanonicalAndHeadExact$`)
	runFoundationGoTest(t, traceApplication, "./internal/orchestrator", `^TestProviderSplitSecretNeverAppearsInDurableEvents$`)
	runFoundationGoTest(t, traceApplication, "./internal/app", `^Test(PublishRedactsEquivalentTextFieldsWithActiveBinding|PublishUsesGenerationLeaseForEncodedVariants|RuntimeGenerationBootstrapBindingAdvancesStoreWhileOldOutputIsImmutable)$`)
	runFoundationGoTest(t, traceApplication, "./internal/secret", `^Test(AdmissionScannerDetectsRawAndExactEncodedVariants|AdmissionStreamDetectsSecretAcrossChunkBoundary|RegistryRetirementRejectsNewProducersButRetainsBufferedStreams|RegistryLeaseRedactsEncodedVariants)$`)
	runFoundationGoTest(t, traceApplication, "./internal/tui", `^Test(DurableStateEventUpdatesStatusAndProjectsOpenedSession|DurableReplayRendersRecoveryReadOnlyToolsAndTerminalState|ConfiguredSecretPromptIsRedactedBeforeConversationRendering)$`)
	assertSyntheticSecretSplit(t)
	assertApplicationConsumerEquivalence(t)
}

func runFoundationGoTest(t *testing.T, trace, pkg, pattern string) {
	t.Helper()
	root := foundationRepositoryRoot(t)
	command := exec.CommandContext(t.Context(), "go", "test", pkg, "-run", pattern, "-count=1", "-v")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("[%s] subprocess %q %q failed: %v\n%s", trace, pkg, pattern, err, output)
	}
	t.Logf("trace=%s subprocess=%s pattern=%s status=pass", trace, pkg, pattern)
}

func foundationRepositoryRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	return root
}

func validateFoundationFixtures(t *testing.T) {
	t.Helper()
	names := foundationfixture.Names()
	if len(names) != 14 {
		t.Fatalf("[%s] fixture count=%d want=14", traceMigration, len(names))
	}
	const wantDigest = "c20a9601d4a87b7105c2938d57abd9591bef932bd157d563f3cc5d5dd39ccaeb"
	if got := foundationfixture.Digest(); got != wantDigest {
		t.Fatalf("[%s] fixture digest=%s want=%s", traceMigration, got, wantDigest)
	}

	root := t.TempDir()
	if err := foundationfixture.Materialize(root, names...); err != nil {
		t.Fatalf("[%s] materialize fixtures: %v", traceMigration, err)
	}
	if got := foundationFixtureDirectories(t, root); !slices.Equal(got, names) {
		t.Fatalf("[%s] materialized fixture inventory=%v want=%v", traceMigration, got, names)
	}
	before := snapshotFixtureTree(t, root)
	validateMaterializedFoundationFixtures(t, root, before)
	after := snapshotFixtureTree(t, root)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("[%s] materialized fixture bytes changed during validation", traceMigration)
	}
	t.Logf("trace=%s fixtures=%d digest=%s files=%d", traceMigration, len(names), wantDigest, len(before))
}

func foundationFixtureDirectories(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("[%s] read materialized fixture inventory: %v", traceMigration, err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	slices.Sort(names)
	return names
}

func validateMaterializedFoundationFixtures(t *testing.T, root string, tree map[string][]byte) {
	t.Helper()
	for path, got := range tree {
		rel, err := filepath.Rel(root, path)
		if err != nil {
			t.Fatalf("[%s] resolve materialized fixture path %q: %v", traceMigration, path, err)
		}
		parts := strings.SplitN(filepath.ToSlash(rel), "/", 2)
		if len(parts) != 2 {
			t.Fatalf("[%s] malformed materialized fixture path %q", traceMigration, rel)
		}
		want, ok := foundationfixture.Read(parts[0], parts[1])
		if !ok {
			t.Fatalf("[%s] shared fixture bytes unavailable for %q", traceMigration, rel)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("[%s] materialized fixture bytes differ for %q", traceMigration, rel)
		}
	}
}

func snapshotFixtureTree(t *testing.T, root string) map[string][]byte {
	t.Helper()
	result := make(map[string][]byte)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		result[path] = raw
		return nil
	})
	if err != nil {
		t.Fatalf("[%s] snapshot fixtures: %v", traceMigration, err)
	}
	return result
}

func validateEvidenceLineageFixtureExpectations(t *testing.T) {
	t.Helper()
	for _, fixture := range []struct {
		name, field, want string
	}{
		{"missing-evidence-blob", "availability", "missing"},
		{"evidence-digest-mismatch", "availability", "corrupt"},
		{"lineage-cycle", "accepted", "false"},
	} {
		raw, ok := foundationfixture.Read(fixture.name, "want.json")
		if !ok {
			t.Fatalf("[%s] shared fixture expectation unavailable for %s", traceEvidence, fixture.name)
		}
		var decoded map[string]any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("[%s] decode %s expectation: %v", traceEvidence, fixture.name, err)
		}
		got := fmt.Sprint(decoded[fixture.field])
		if got != fixture.want || len(decoded["diagnostics"].([]any)) == 0 {
			t.Fatalf("[%s] expectation %s=%s diagnostics=%v", traceEvidence, fixture.name, got, decoded["diagnostics"])
		}
	}
}

type acceptanceEncoder struct{}

func (acceptanceEncoder) EncodeProposed(event protocol.ProposedEvent) (json.RawMessage, error) {
	return protocol.CloneRawMessage(event.Payload), nil
}

func acceptCrossProcessSameHeadWriters(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	store := jsonl.New(root, jsonl.Options{Encoder: acceptanceEncoder{}})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatalf("[%s] workspace: %v", traceConcurrency, err)
	}
	session, err := store.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "acceptance", Model: "fixture"})
	if err != nil {
		t.Fatalf("[%s] create session: %v", traceConcurrency, err)
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(session.ID)}
	head, err := store.Head(context.Background(), ref)
	if err != nil {
		t.Fatalf("[%s] head: %v", traceConcurrency, err)
	}
	commands := []*exec.Cmd{
		foundationWriterCommand(root, protocol.SessionID(session.ID), head, "a"),
		foundationWriterCommand(root, protocol.SessionID(session.ID), head, "b"),
	}
	type outcome struct {
		output []byte
		err    error
	}
	outcomes := make(chan outcome, len(commands))
	for _, command := range commands {
		go func() {
			output, runErr := command.CombinedOutput()
			outcomes <- outcome{output: output, err: runErr}
		}()
	}
	counts := map[string]int{}
	for range commands {
		result := <-outcomes
		if result.err != nil {
			t.Fatalf("[%s] writer subprocess: %v output=%q", traceConcurrency, result.err, result.output)
		}
		for _, field := range strings.Fields(string(result.output)) {
			if field == string(journal.AppendCommitted) || field == string(journal.AppendConflict) {
				counts[field]++
			}
		}
	}
	if counts[string(journal.AppendCommitted)] != 1 || counts[string(journal.AppendConflict)] != 1 {
		t.Fatalf("[%s] cross-process same-head results=%v", traceConcurrency, counts)
	}
	inspection, err := store.Inspect(context.Background(), ref)
	// The first v2 append contains the compatibility declaration, task event,
	// and commit marker, so exactly one winning writer advances three slots.
	if err != nil || inspection.Head.CommitSeq != head.CommitSeq+3 {
		t.Fatalf("[%s] durable writer result head=%+v initial=%+v err=%v", traceConcurrency, inspection.Head, head, err)
	}
	t.Logf("trace=%s cross_process_writers committed=1 conflict=1 head=%d", traceConcurrency, inspection.Head.CommitSeq)
}

func foundationWriterCommand(root string, sessionID protocol.SessionID, head protocol.CommittedCursor, suffix string) *exec.Cmd {
	command := exec.Command(os.Args[0], "-test.run=^TestFoundationWriterProcess$", "-test.v=false")
	command.Env = append(os.Environ(),
		"YORDAM_FOUNDATION_WRITER=1",
		"YORDAM_FOUNDATION_ROOT="+root,
		"YORDAM_FOUNDATION_SESSION="+string(sessionID),
		"YORDAM_FOUNDATION_SEQ="+strconv.FormatUint(head.CommitSeq, 10),
		"YORDAM_FOUNDATION_TXN="+string(head.TransactionID),
		"YORDAM_FOUNDATION_SUFFIX="+suffix,
	)
	return command
}

func TestFoundationWriterProcess(t *testing.T) {
	if os.Getenv("YORDAM_FOUNDATION_WRITER") != "1" {
		return
	}
	seq, err := strconv.ParseUint(os.Getenv("YORDAM_FOUNDATION_SEQ"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	sessionID := protocol.SessionID(os.Getenv("YORDAM_FOUNDATION_SESSION"))
	suffix := os.Getenv("YORDAM_FOUNDATION_SUFFIX")
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(sessionID)}
	head := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: seq, TransactionID: protocol.TransactionID(os.Getenv("YORDAM_FOUNDATION_TXN"))}
	payload, err := canonicaljson.Marshal(protocol.TaskCreatedV1{Goal: "cross-process acceptance", OutcomeContractID: "contract-" + protocol.OutcomeContractID(suffix), ContractVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	result, err := jsonl.New(os.Getenv("YORDAM_FOUNDATION_ROOT"), jsonl.Options{Encoder: acceptanceEncoder{}}).AppendBatch(context.Background(), journal.AppendRequest{
		Journal: ref, ExpectedHead: head, TransactionID: protocol.TransactionID("txn-writer-" + suffix),
		Compatibility: &journal.CompatibilityDeclaration{ReaderVersion: protocol.EnvelopeVersion, WriterVersion: protocol.EnvelopeVersion, LegacyHead: head},
		Events: []protocol.ProposedEvent{{
			EventID: "event-writer-" + protocol.EventID(suffix), Time: time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC),
			PayloadVersion: 1, Kind: protocol.EventTaskCreated, SessionID: sessionID, TaskID: "task-writer-" + protocol.TaskID(suffix), Payload: payload,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(result.Status)
}

func assertSyntheticSecretSplit(t *testing.T) {
	t.Helper()
	const sentinel = "gate-one-synthetic-secret"
	registry := secret.NewRegistry()
	lease, err := registry.Acquire("generation-secret-acceptance", [][]byte{[]byte(sentinel)})
	if err != nil {
		t.Fatalf("[%s] acquire secret generation: %v", traceApplication, err)
	}
	stream, err := lease.RedactionStream()
	if err != nil {
		t.Fatalf("[%s] open redaction stream: %v", traceApplication, err)
	}
	output := stream.Write("gate-one-synthetic-")
	if err := registry.Retire("generation-secret-acceptance"); err != nil {
		t.Fatalf("[%s] retire generation: %v", traceApplication, err)
	}
	output += stream.Write("secret")
	output += stream.Close()
	variants := []string{
		sentinel,
		base64.StdEncoding.EncodeToString([]byte(sentinel)),
		base64.RawStdEncoding.EncodeToString([]byte(sentinel)),
		base64.URLEncoding.EncodeToString([]byte(sentinel)),
		base64.RawURLEncoding.EncodeToString([]byte(sentinel)),
		hex.EncodeToString([]byte(sentinel)),
		strings.ToUpper(hex.EncodeToString([]byte(sentinel))),
	}
	surfaceNames := []string{"session_journal", "control_journal", "evidence", "recovery_diagnostics", "logs", "application_events", "tui", "fake_headless", "public_errors", "model_visible_tool_results"}
	var surfaces []string
	for index, name := range surfaceNames {
		variant := variants[index%len(variants)]
		surfaces = append(surfaces, name+":"+lease.String(variant))
	}
	surfaces = append(surfaces, "retired_buffered_stream:"+output)
	joined := strings.Join(surfaces, "\n")
	for _, variant := range variants {
		if strings.Contains(joined, variant) {
			t.Fatalf("[%s] synthetic secret variant leaked across acceptance surfaces", traceApplication)
		}
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("[%s] close secret lease: %v", traceApplication, err)
	}
	t.Logf("trace=%s synthetic_secret surfaces=%d encodings=%d occurrences=0 retired_stream=redacted", traceApplication, len(surfaceNames), len(variants))
}

func assertApplicationConsumerEquivalence(t *testing.T) {
	t.Helper()
	runFoundationGoTest(t, traceApplication, "./internal/app", `^TestProductionLegacyCommandUsesApplicationProtocolAndRealCursorProjection$`)
	t.Logf("trace=%s consumers=tui,fake_headless source=production_journals cursor=exact", traceApplication)
}
