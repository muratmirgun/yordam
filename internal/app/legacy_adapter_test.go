package app_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
	skilltool "github.com/muratmirgun/yordam/internal/tools/skill"
)

func TestApplicationLegacyAdapterPreservesCommandAndRedactedEventSemantics(t *testing.T) {
	adapter := app.NewLegacyAdapter(app.LegacyAdapterOptions{Actor: protocol.ActorRef{ID: "user-1", Kind: protocol.ActorUser}, SelectedSessionID: "session-1"})
	command, err := adapter.Command(app.Command{Kind: app.CommandChangeModel, Selection: domain.ModelSelection{Profile: "openai", Model: "gpt"}})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := app.DecodeCommandPayload(command)
	if err != nil {
		t.Fatal(err)
	}
	payload, ok := decoded.(*protocol.ChangeModelCommandV1)
	if !ok || payload.ProviderID != "openai" || payload.ModelID != "gpt" {
		t.Fatalf("payload=%T %+v", decoded, decoded)
	}

	applicationEvent := protocol.ApplicationEvent{
		ProtocolVersion: 1,
		StreamEventID:   "legacy-error",
		Correlation:     protocol.EventCorrelation{JournalKind: protocol.JournalSession, JournalID: "session-1", SessionID: "session-1"},
		Time:            time.Unix(1, 0).UTC(), Kind: app.ApplicationEventError, Classification: "transient", PayloadVersion: 1,
		Payload: json.RawMessage(`{"message":"safe message"}`), Error: &protocol.PublicError{Code: "runtime_failed", Message: "safe message"},
	}
	legacy, err := adapter.Event(applicationEvent)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Kind != app.EventError || legacy.Message != "safe message" || legacy.Err != nil {
		t.Fatalf("legacy=%+v", legacy)
	}
}

func TestApplicationLegacyAdapterProjectsManualCompactionLifecycleWithoutProviderContent(t *testing.T) {
	cursor := protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "session-1", CommitSeq: 9, TransactionID: "tx-9"}
	adapter := app.NewLegacyAdapter(app.LegacyAdapterOptions{Actor: protocol.ActorRef{ID: "user-1", Kind: protocol.ActorUser}, RuntimeGenerationID: "runtime-a", Cursor: adapterCursor("session-1", cursor)})
	if _, err := adapter.Command(app.Command{Kind: app.CommandCompact}); err != nil {
		t.Fatal(err)
	}

	activityID := protocol.ActivityID("compact-activity")
	steps := []struct {
		kind string
		body string
		want app.EventKind
	}{
		{protocol.EventActivityPlanned, `{"kind":"provider","purpose":"summarize stable context","compaction_trigger":"manual"}`, app.EventKind("compaction_started")},
		{protocol.EventActivityStarted, `{}`, app.EventKind("compaction_progress")},
		{protocol.EventActivitySucceeded, `{"status":"succeeded"}`, app.EventKind("compaction_progress")},
		{protocol.EventContextCompacted, `{"from":{"journal_kind":"session","journal_id":"session-1","commit_seq":1,"transaction_id":"tx-1"},"through":{"journal_kind":"session","journal_id":"session-1","commit_seq":4,"transaction_id":"tx-4"},"summary_evidence_id":"summary-1","revision":"r1","summary":"provider-secret-body"}`, app.EventKind("compaction_completed")},
	}
	for _, step := range steps {
		legacy, err := adapter.Event(compactionApplicationEvent(step.kind, activityID, step.body))
		if err != nil {
			t.Fatalf("%s: %v", step.kind, err)
		}
		if legacy.Kind != step.want {
			t.Fatalf("%s legacy kind=%q want=%q", step.kind, legacy.Kind, step.want)
		}
		raw, err := json.Marshal(legacy)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "provider-secret-body") {
			t.Fatalf("%s leaked provider content: %s", step.kind, raw)
		}
	}
}

func TestApplicationLegacyAdapterProjectsAutomaticCompactionCancelledAndUncertain(t *testing.T) {
	for _, terminal := range []struct {
		kind string
		want string
	}{
		{protocol.EventActivityCancelled, "cancelled"},
		{protocol.EventActivityUncertain, "uncertain"},
	} {
		t.Run(terminal.want, func(t *testing.T) {
			adapter := app.NewLegacyAdapter(app.LegacyAdapterOptions{Actor: protocol.ActorRef{ID: "user-1", Kind: protocol.ActorUser}})
			activityID := protocol.ActivityID("auto-" + terminal.want)
			if _, err := adapter.Event(compactionApplicationEvent(protocol.EventActivityPlanned, activityID, `{"kind":"provider","purpose":"summarize stable context","compaction_trigger":"automatic"}`)); err != nil {
				t.Fatal(err)
			}
			legacy, err := adapter.Event(compactionApplicationEvent(terminal.kind, activityID, `{}`))
			if err != nil {
				t.Fatal(err)
			}
			if legacy.Kind != app.EventKind("compaction_failed") || !strings.Contains(legacy.Message, terminal.want) {
				t.Fatalf("terminal=%+v", legacy)
			}
		})
	}
}

func TestApplicationLegacyAdapterCompactionFactsStayBoundToActivityID(t *testing.T) {
	adapter := app.NewLegacyAdapter(app.LegacyAdapterOptions{Actor: protocol.ActorRef{ID: "user", Kind: protocol.ActorUser}})
	for _, item := range []struct {
		id      protocol.ActivityID
		trigger string
	}{{"A", "manual"}, {"B", "automatic"}, {"C", ""}} {
		body := `{"kind":"provider","purpose":"summarize stable context"}`
		if item.trigger != "" {
			body = body[:len(body)-1] + `,"compaction_trigger":"` + item.trigger + `"}`
		}
		if got, err := adapter.Event(compactionApplicationEvent(protocol.EventActivityPlanned, item.id, body)); err != nil || (item.trigger == "" && got.Kind != "") || (item.trigger != "" && got.Kind != app.EventCompactionStarted) {
			t.Fatalf("plan %s=%+v err=%v", item.id, got, err)
		}
	}
	for _, item := range []struct {
		id           protocol.ActivityID
		usage, bytes int64
		trigger      string
	}{{"B", 22, 222, "automatic"}, {"A", 11, 111, "manual"}} {
		outcome := fmt.Sprintf(`{"status":"succeeded","output_bytes":%d,"usage":{"input":{"state":"provider_reported","value":%d},"output":{"state":"unknown"},"cached":{"state":"unknown"},"cache_write":{"state":"unknown"},"reasoning":{"state":"unknown"}}}`, item.bytes, item.usage)
		if _, err := adapter.Event(compactionApplicationEvent(protocol.EventActivitySucceeded, item.id, outcome)); err != nil {
			t.Fatal(err)
		}
		compact := fmt.Sprintf(`{"from":{"journal_kind":"session","journal_id":"session-1","commit_seq":1,"transaction_id":"t"},"through":{"journal_kind":"session","journal_id":"session-1","commit_seq":%d,"transaction_id":"t"},"summary_evidence_id":"e-%s","revision":"r-%s"}`, item.bytes, item.id, item.id)
		got, err := adapter.Event(compactionApplicationEvent(protocol.EventContextCompacted, item.id, compact))
		if err != nil {
			t.Fatal(err)
		}
		if got.Compaction == nil || got.Compaction.Trigger != item.trigger || got.Compaction.SummaryBytes != item.bytes || got.Compaction.Usage.Input.Value != item.usage || got.Compaction.Revision != "r-"+string(item.id) {
			t.Fatalf("facts crossed: %+v", got.Compaction)
		}
	}
}

func TestApplicationLegacyAdapterContextPlanDoesNotGuessPolicy(t *testing.T) {
	adapter := app.NewLegacyAdapter(app.LegacyAdapterOptions{Actor: protocol.ActorRef{ID: "user", Kind: protocol.ActorUser}})
	plan := `{"plan":{"body":{"sources":[],"excluded":[],"estimated_input_tokens":{"state":"known","value":1,"provenance":"e"},"output_reserve":1,"context_window":{"state":"known","value":9,"provenance":"c"},"compaction_revision":"r","tool_exposure_revision":"t"},"digest":{"algorithm":"sha256","value":"x"}}}`
	got, err := adapter.Event(compactionApplicationEvent(protocol.EventContextPlanRecorded, "", plan))
	if err != nil {
		t.Fatal(err)
	}
	if got.Context == nil || got.Context.AutoAvailable || got.Context.AutoReason != "" {
		t.Fatalf("live plan guessed policy: %+v", got.Context)
	}
}

func TestApplicationLegacyAdapterDerivesSkillProvenanceOnlyFromCanonicalPlan(t *testing.T) {
	adapter := app.NewLegacyAdapter(app.LegacyAdapterOptions{})
	activityID, available := startSkillActivityForTest(t, adapter)
	output, err := adapter.Event(compactionApplicationEvent(protocol.EventToolResultAvailable, activityID, string(available)))
	if err != nil || output.Kind != app.EventToolOutput || output.Runtime.Skill == nil || output.Runtime.Progress == nil || output.Runtime.Progress.Text != "bounded skill body" || !output.Runtime.Progress.Truncated {
		t.Fatalf("output=%+v err=%v", output, err)
	}
	completed, err := adapter.Event(compactionApplicationEvent(protocol.EventActivitySucceeded, activityID, `{}`))
	if err != nil || completed.Kind != app.EventToolCompleted || completed.Runtime.Result == nil || completed.Runtime.Result.Status != domain.ToolSucceeded || completed.Runtime.Result.Content != "bounded skill body" || !completed.Runtime.Result.Truncated || completed.Runtime.Result.Duration != time.Second {
		t.Fatalf("completed=%+v err=%v", completed, err)
	}
	if _, ok := adapter.SkillProvenance(activityID); ok {
		t.Fatal("terminal skill activity retained provenance")
	}
}

func TestApplicationLegacyAdapterSkillTransientLifecycleRejectsMismatchesAndDuplicates(t *testing.T) {
	adapter := app.NewLegacyAdapter(app.LegacyAdapterOptions{})
	activityID, available := startSkillActivityForTest(t, adapter)
	var mismatched protocol.ToolResultAvailableV1
	if err := json.Unmarshal(available, &mismatched); err != nil {
		t.Fatal(err)
	}
	mismatched.CallID = "another-call"
	mismatchRaw, err := json.Marshal(mismatched)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := adapter.Event(compactionApplicationEvent(protocol.EventToolResultAvailable, activityID, string(mismatchRaw))); err != nil || got.Kind != "" {
		t.Fatalf("mismatched output=%+v err=%v", got, err)
	}
	if got, err := adapter.Event(compactionApplicationEvent(protocol.EventToolResultAvailable, activityID, string(available))); err != nil || got.Kind != app.EventToolOutput {
		t.Fatalf("first output=%+v err=%v", got, err)
	}
	if got, err := adapter.Event(compactionApplicationEvent(protocol.EventToolResultAvailable, activityID, string(available))); err != nil || got.Kind != "" {
		t.Fatalf("duplicate output=%+v err=%v", got, err)
	}
	terminal, err := adapter.Event(compactionApplicationEvent(protocol.EventActivityFailed, activityID, `{}`))
	if err != nil || terminal.Kind != app.EventToolCompleted || terminal.Runtime.Result == nil || terminal.Runtime.Result.Status != domain.ToolFailed || terminal.Runtime.Result.Content != "bounded skill body" {
		t.Fatalf("terminal=%+v err=%v", terminal, err)
	}
	if got, err := adapter.Event(compactionApplicationEvent(protocol.EventToolResultAvailable, activityID, string(available))); err != nil || got.Kind != "" {
		t.Fatalf("late output=%+v err=%v", got, err)
	}
}

func TestApplicationLegacyAdapterSkillTerminalWithoutTransientCreatesEmptyCard(t *testing.T) {
	adapter := app.NewLegacyAdapter(app.LegacyAdapterOptions{})
	activityID, _ := startSkillActivityForTest(t, adapter)
	terminal, err := adapter.Event(compactionApplicationEvent(protocol.EventActivitySucceeded, activityID, `{}`))
	if err != nil || terminal.Kind != app.EventToolCompleted || terminal.Runtime.Result == nil || terminal.Runtime.Result.CallID != "skill-call" || terminal.Runtime.Result.Status != domain.ToolSucceeded || terminal.Runtime.Result.Content != "" {
		t.Fatalf("terminal=%+v err=%v", terminal, err)
	}
	if _, ok := adapter.SkillProvenance(activityID); ok {
		t.Fatal("terminal without transient retained provenance")
	}
}

func TestApplicationLegacyAdapterInvalidReplacementPlanClearsSkillState(t *testing.T) {
	adapter := app.NewLegacyAdapter(app.LegacyAdapterOptions{})
	activityID, available := startSkillActivityForTest(t, adapter)
	if got, err := adapter.Event(compactionApplicationEvent(protocol.EventActivityPlanned, activityID, `{"kind":"tool"}`)); err != nil || got.Kind != "" {
		t.Fatalf("replacement plan=%+v err=%v", got, err)
	}
	if got, err := adapter.Event(compactionApplicationEvent(protocol.EventToolResultAvailable, activityID, string(available))); err != nil || got.Kind != "" {
		t.Fatalf("stale output=%+v err=%v", got, err)
	}
	if got, err := adapter.Event(compactionApplicationEvent(protocol.EventActivitySucceeded, activityID, `{}`)); err != nil || got.Kind != "" {
		t.Fatalf("stale terminal=%+v err=%v", got, err)
	}
}

func TestApplicationLegacyAdapterRejectsNonCanonicalSkillPlans(t *testing.T) {
	activityID := protocol.ActivityID("skill-activity")
	basePlan, baseStart := canonicalSkillLifecycleForTest(t, activityID)
	for _, test := range []struct {
		name   string
		mutate func(*protocol.ActivityPlannedV1)
	}{
		{"descriptor_digest", func(p *protocol.ActivityPlannedV1) { p.Plan.Body.DescriptorDigest.Value = strings.Repeat("b", 64) }},
		{"source_revision", func(p *protocol.ActivityPlannedV1) { p.Plan.Body.SourceRevision = "forged-revision" }},
		{"body_semantics", func(p *protocol.ActivityPlannedV1) { p.Plan.Body.Purpose = "execute" }},
		{"resource_runtime_relation", func(p *protocol.ActivityPlannedV1) { p.Plan.Body.Resources[0].Attributes[0].Value = "other-runtime" }},
		{"source_workspace_relation", func(p *protocol.ActivityPlannedV1) { p.Plan.Body.Resources[0].Attributes[1].Value = "global" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter := app.NewLegacyAdapter(app.LegacyAdapterOptions{})
			planned := protocol.DeepCopy(basePlan)
			test.mutate(&planned)
			digest, err := canonicaljson.Digest(planned.Plan.Body)
			if err != nil {
				t.Fatal(err)
			}
			planned.Plan.Digest = digest
			start := protocol.DeepCopy(baseStart)
			start.PlanDigest = digest
			plannedRaw, err := json.Marshal(planned)
			if err != nil {
				t.Fatal(err)
			}
			if got, err := adapter.Event(compactionApplicationEvent(protocol.EventActivityPlanned, activityID, string(plannedRaw))); err != nil || got.Kind != "" {
				t.Fatalf("planned=%+v err=%v", got, err)
			}
			startedRaw, err := json.Marshal(start)
			if err != nil {
				t.Fatal(err)
			}
			if got, err := adapter.Event(compactionApplicationEvent(protocol.EventActivityStarted, activityID, string(startedRaw))); err != nil || got.Kind != "" {
				t.Fatalf("started=%+v err=%v", got, err)
			}
		})
	}
}

func startSkillActivityForTest(t *testing.T, adapter *app.LegacyAdapter) (protocol.ActivityID, []byte) {
	t.Helper()
	activityID := protocol.ActivityID("skill-activity")
	planned, start := canonicalSkillLifecycleForTest(t, activityID)
	raw, err := json.Marshal(planned)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.Event(compactionApplicationEvent(protocol.EventActivityPlanned, activityID, string(raw))); err != nil {
		t.Fatal(err)
	}
	startedPayload, err := json.Marshal(start)
	if err != nil {
		t.Fatal(err)
	}
	wrongState := protocol.DeepCopy(start)
	wrongState.DispatchState = "started"
	wrongStatePayload, err := json.Marshal(wrongState)
	if err != nil {
		t.Fatal(err)
	}
	if ignored, err := adapter.Event(compactionApplicationEvent(protocol.EventActivityStarted, activityID, string(wrongStatePayload))); err != nil || ignored.Kind != "" {
		t.Fatalf("wrong dispatch state=%+v err=%v", ignored, err)
	}
	started, err := adapter.Event(compactionApplicationEvent(protocol.EventActivityStarted, activityID, string(startedPayload)))
	if err != nil || started.Kind != app.EventToolStarted || started.Runtime.Skill == nil || started.Runtime.Skill.Name != "go-testing" || started.Runtime.Progress == nil || started.Runtime.Progress.CallID != "skill-call" {
		t.Fatalf("started=%+v err=%v", started, err)
	}
	available, err := json.Marshal(protocol.ToolResultAvailableV1{ActivityID: activityID, CallID: start.CallID, Status: string(domain.ToolSucceeded), Content: "bounded skill body", DurationNanos: int64(time.Second), Truncated: true})
	if err != nil {
		t.Fatal(err)
	}
	return activityID, available
}

func canonicalSkillLifecycleForTest(t *testing.T, activityID protocol.ActivityID) (protocol.ActivityPlannedV1, protocol.ActivityStartedV1) {
	t.Helper()
	digest := protocol.Digest{Algorithm: "sha256", Value: strings.Repeat("a", 64)}
	descriptor := skilltool.BuiltinDescriptor()
	body := protocol.ActionPlanBody{
		CallID: "skill-call", Tool: descriptor.Body.Identity, SourceRevision: descriptor.Body.SourceRevision, DescriptorDigest: descriptor.DescriptorDigest,
		Action: "skill", Purpose: "inspect", ExecutionLocus: "builtin", Effect: "observation", Boundary: "workspace", Reversibility: "not_applicable", VerificationCoverage: "full", RequestedProfile: "restricted", EffectiveProfile: "restricted", RuntimeGenerationID: "runtime-1",
		Resources: []protocol.ResourceTarget{{Kind: "skill", CanonicalID: "go-testing", Digest: "sha256:" + digest.Value, Attributes: []protocol.ResourceAttribute{{Name: "runtime_generation", Value: "runtime-1"}, {Name: "source", Value: "project"}, {Name: "workspace_id", Value: "workspace-1"}}}},
	}
	planDigest, err := canonicaljson.Digest(body)
	if err != nil {
		t.Fatal(err)
	}
	planned := protocol.ActivityPlannedV1{Kind: "tool", Purpose: "tool observation", PurposeActor: protocol.ActorRef{ID: "orchestrator", Kind: protocol.ActorAgent}, Source: "builtin", RequestedProfile: "restricted", EffectiveProfile: "restricted", Plan: &protocol.ActionPlan{Body: body, Digest: planDigest}}
	started := protocol.ActivityStartedV1{DecisionNonce: "nonce", DecisionEventID: "decision", ActivityID: activityID, CallID: body.CallID, PlanDigest: planDigest, RequestDigest: digest, DispatchDigest: digest, RuntimeGenerationID: body.RuntimeGenerationID, DispatchState: "registered"}
	return planned, started
}

func compactionApplicationEvent(kind string, activityID protocol.ActivityID, body string) protocol.ApplicationEvent {
	return protocol.ApplicationEvent{
		ProtocolVersion: protocol.ApplicationProtocolVersion,
		StreamEventID:   string(kind) + ":" + string(activityID),
		Correlation:     protocol.EventCorrelation{JournalKind: protocol.JournalSession, JournalID: "session-1", SessionID: "session-1", ActivityID: activityID},
		Time:            time.Unix(1, 0).UTC(), Kind: string(kind), Classification: "durable", PayloadVersion: 1, Payload: json.RawMessage(body),
	}
}

func TestApplicationLegacyAdapterCompactIdentityBindsCursorAndGeneration(t *testing.T) {
	cursor := protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "session-1", CommitSeq: 9, TransactionID: "tx-9"}
	adapter := app.NewLegacyAdapter(app.LegacyAdapterOptions{
		Actor: protocol.ActorRef{ID: "user-1", Kind: protocol.ActorUser}, RuntimeGenerationID: "runtime-a",
		Cursor: func() *protocol.CommandExpectation {
			return &protocol.CommandExpectation{SelectedSessionID: "session-1", Session: &cursor}
		},
	})
	first, err := adapter.Command(app.Command{Kind: app.CommandCompact})
	if err != nil {
		t.Fatal(err)
	}
	second, err := adapter.Command(app.Command{Kind: app.CommandCompact})
	if err != nil {
		t.Fatal(err)
	}
	if first.CommandID != second.CommandID || first.IdempotencyKey != second.IdempotencyKey || first.RequestDigest != second.RequestDigest {
		t.Fatalf("same compact cursor/generation identity changed: first=%+v second=%+v", first, second)
	}
	cursor.CommitSeq++
	cursor.TransactionID = "tx-10"
	changedHead, err := adapter.Command(app.Command{Kind: app.CommandCompact})
	if err != nil {
		t.Fatal(err)
	}
	if changedHead.CommandID == first.CommandID {
		t.Fatal("changed compact cursor reused command identity")
	}
	otherGeneration := app.NewLegacyAdapter(app.LegacyAdapterOptions{Actor: protocol.ActorRef{ID: "user-1", Kind: protocol.ActorUser}, RuntimeGenerationID: "runtime-b", Cursor: adapterCursor("session-1", cursor)})
	changedGeneration, err := otherGeneration.Command(app.Command{Kind: app.CommandCompact})
	if err != nil {
		t.Fatal(err)
	}
	if changedGeneration.CommandID == changedHead.CommandID {
		t.Fatal("changed runtime generation reused command identity")
	}
}

func TestApplicationLegacyAdapterStartTurnIdentityIsUniqueAfterRestart(t *testing.T) {
	firstAdapter := app.NewLegacyAdapter(app.LegacyAdapterOptions{Actor: protocol.ActorRef{ID: "user-1", Kind: protocol.ActorUser}, RuntimeGenerationID: "same-runtime-after-process-restart"})
	secondAdapter := app.NewLegacyAdapter(app.LegacyAdapterOptions{Actor: protocol.ActorRef{ID: "user-1", Kind: protocol.ActorUser}, RuntimeGenerationID: "same-runtime-after-process-restart"})
	first, err := firstAdapter.Command(app.Command{Kind: app.CommandStartTurn, Prompt: "resume"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := secondAdapter.Command(app.Command{Kind: app.CommandStartTurn, Prompt: "resume"})
	if err != nil {
		t.Fatal(err)
	}
	if first.CommandID == second.CommandID || first.IdempotencyKey == second.IdempotencyKey {
		t.Fatalf("restart reused normal turn identity: first=%+v second=%+v", first, second)
	}
}

func adapterCursor(session protocol.SessionID, cursor protocol.CommittedCursor) func() *protocol.CommandExpectation {
	return func() *protocol.CommandExpectation {
		return &protocol.CommandExpectation{SelectedSessionID: session, Session: &cursor}
	}
}
