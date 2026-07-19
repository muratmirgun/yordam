package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/cli"
	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/permission"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
	"github.com/muratmirgun/yordam/internal/skills"
	"github.com/muratmirgun/yordam/internal/testsupport/agentfixture"
)

func TestNewUsesEmptyRedactorBindingWhenNoneIsSupplied(t *testing.T) {
	application := New(Options{RuntimeSet: RuntimeSet{Redactor: secret.New("candidate-secret")}})
	if got := application.redactors.String("candidate-secret"); got != "candidate-secret" {
		t.Fatalf("implicit binding inherited candidate secrets: %q", got)
	}
}

func TestRuntimeSetReadinessClassifiesModelsAndCredentials(t *testing.T) {
	set := RuntimeSet{
		Models:         []domain.ModelSelection{{Profile: "primary", Model: "a"}, {Profile: "secondary", Model: "b"}},
		CredentialEnvs: map[string]string{"primary": "PRIMARY_KEY", "secondary": "SECONDARY_KEY"},
		Credentials:    map[string]string{"primary": "secret", "secondary": ""},
		configPath:     "/home/user/.config/yordam/config.jsonc",
	}
	if err := set.Ready(domain.ModelSelection{Profile: "primary", Model: "a"}); err != nil {
		t.Fatal(err)
	}
	for selection, want := range map[domain.ModelSelection]string{
		{Profile: "secondary", Model: "b"}: "SECONDARY_KEY",
		{Profile: "missing", Model: "x"}:   "not configured",
	} {
		err := set.Ready(selection)
		var typed *domain.TypedError
		if err == nil || !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), "/home/user/.config/yordam/config.jsonc") || !strings.Contains(err.Error(), "restart") && want == "SECONDARY_KEY" || !asTypedConfiguration(err, &typed) {
			t.Fatalf("selection=%+v error=%v", selection, err)
		}
	}
}

func TestRuntimeModelDescriptorsContextWindowProvenance(t *testing.T) {
	descriptors := runtimeModelDescriptors(config.Config{Profiles: map[string]config.Profile{
		"primary": {
			APIKeyEnv:           "PRIMARY_KEY",
			Models:              []string{"configured", "unknown"},
			ModelContextWindows: map[string]int64{"configured": 128000},
		},
	}}, "generation")
	if len(descriptors) != 2 {
		t.Fatalf("descriptors=%+v", descriptors)
	}
	for _, descriptor := range descriptors {
		if descriptor.MaximumOutput.State != protocol.ValueUnknown {
			t.Fatalf("maximum output=%+v", descriptor.MaximumOutput)
		}
		switch descriptor.ModelID {
		case "configured":
			if descriptor.ContextWindow != (protocol.ValueInt64{State: protocol.ValueKnown, Value: 128000, Provenance: "configured_claim"}) {
				t.Fatalf("configured context window=%+v", descriptor.ContextWindow)
			}
		case "unknown":
			if descriptor.ContextWindow != (protocol.ValueInt64{State: protocol.ValueUnknown}) {
				t.Fatalf("unknown context window=%+v", descriptor.ContextWindow)
			}
		default:
			t.Fatalf("unexpected model=%q", descriptor.ModelID)
		}
	}
}

func TestRuntimeBrokerSnapshotReconstructsDurableCompactionContext(t *testing.T) {
	builder := newRuntimeBuilderForTest(t, nil)
	t.Setenv("PRIMARY_KEY", "primary-test-key")
	if _, err := builder.build(loadRuntimeConfig(t, "https://example.invalid/v1", 120), domain.ModelSelection{}); err != nil {
		t.Fatal(err)
	}
	workspaceRef, err := builder.store.EnsureWorkspaceControl(t.Context(), builder.workspace)
	if err != nil {
		t.Fatal(err)
	}
	workspaceSeed, err := canonicaljson.Marshal(protocol.ControlOperationTerminalV1{ControlOperationID: "seed", Status: "interrupted"})
	if err != nil {
		t.Fatal(err)
	}
	seeded, err := builder.store.AppendBatch(t.Context(), journal.AppendRequest{Journal: workspaceRef, TransactionID: "seed", Events: []protocol.ProposedEvent{{EventID: "seed", Time: time.Unix(1, 0).UTC(), PayloadVersion: 1, Kind: protocol.EventControlOperationInterrupted, Payload: workspaceSeed}}})
	if err != nil || seeded.Status != journal.AppendCommitted {
		t.Fatalf("seed=%+v err=%v", seeded, err)
	}
	workspaceHead, err := builder.store.Head(t.Context(), workspaceRef)
	if err != nil {
		t.Fatal(err)
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(builder.activeSession.get())}
	head, err := builder.store.Head(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	planBody := protocol.ContextPlanBody{Sources: []protocol.ContentSource{}, Excluded: []protocol.ExcludedContentSource{}, EstimatedInputTokens: protocol.ValueInt64{State: protocol.ValueKnown, Value: 900, Provenance: "estimator"}, OutputReserve: 100, ContextWindow: protocol.ValueInt64{State: protocol.ValueKnown, Value: 9000, Provenance: "configured_claim"}, CompactionRevision: "prior-r", ToolExposureRevision: "tools-r1"}
	planDigest, err := canonicaljson.Digest(planBody)
	if err != nil {
		t.Fatal(err)
	}
	planPayload, err := canonicaljson.Marshal(protocol.ContextPlanRecordedV1{Plan: protocol.ContextPlan{Body: planBody, Digest: planDigest}})
	if err != nil {
		t.Fatal(err)
	}
	compactPayload, err := canonicaljson.Marshal(protocol.ContextCompactedV1{From: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: ref.ID, CommitSeq: 1, TransactionID: "ctx"}, Through: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: ref.ID, CommitSeq: 1, TransactionID: "ctx"}, SummaryEvidenceID: "evidence-1", Revision: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	appended, err := builder.store.AppendBatch(t.Context(), journal.AppendRequest{Journal: ref, ExpectedHead: head, TransactionID: "context", Compatibility: &journal.CompatibilityDeclaration{ReaderVersion: protocol.EnvelopeVersion, WriterVersion: protocol.EnvelopeVersion, LegacyHead: head}, Events: []protocol.ProposedEvent{
		{EventID: "context-plan", Time: time.Unix(1, 0).UTC(), PayloadVersion: 1, Kind: protocol.EventContextPlanRecorded, SessionID: protocol.SessionID(ref.ID), RuntimeGenerationID: "test-generation", Payload: planPayload},
		{EventID: "context-compacted", Time: time.Unix(2, 0).UTC(), PayloadVersion: 1, Kind: protocol.EventContextCompacted, SessionID: protocol.SessionID(ref.ID), Payload: compactPayload},
	}})
	if err != nil || appended.Status != journal.AppendCommitted {
		t.Fatalf("append=%+v err=%v", appended, err)
	}
	durable, _, err := (runtimeBrokerSource{Repository: builder.store, Workspace: workspaceRef, Manifest: protocol.RuntimeGenerationManifest{ID: "test-generation", Body: protocol.RuntimeGenerationBody{Limits: protocol.RuntimeLimits{AutoCompact: true}}}}).Project(t.Context(), SnapshotVector{WorkspaceControl: workspaceHead, SelectedSession: &appended.Cursor})
	if err != nil {
		t.Fatal(err)
	}
	var contextState protocol.ContextProjectionV1
	if err := json.Unmarshal(durable.Context.Data, &contextState); err != nil {
		t.Fatal(err)
	}
	if !contextState.AutoAvailable || contextState.AutoReason != "below_threshold" || contextState.EstimatedInputTokens.Value != 900 || contextState.ContextWindow.Value != 9000 || contextState.ReserveTokens.Value != 2048 || contextState.Revision != "r1" || contextState.SummaryEvidenceID != "evidence-1" || contextState.LatestRange == nil || contextState.LatestRange.Through.CommitSeq != 1 {
		t.Fatalf("context=%+v", contextState)
	}
	if strings.Contains(string(durable.Context.Data), "provider-body-sentinel") || strings.Contains(string(durable.Context.Data), "summary-body-sentinel") {
		t.Fatalf("context leaked content: %s", durable.Context.Data)
	}
}

func TestRuntimeBrokerProjectsFrozenMetadataOnlySkillCatalog(t *testing.T) {
	builder := newRuntimeBuilderForTest(t, nil)
	workspaceRef, err := builder.store.EnsureWorkspaceControl(t.Context(), builder.workspace)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := canonicaljson.Marshal(protocol.ControlOperationTerminalV1{ControlOperationID: "seed", Status: "interrupted"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := builder.store.AppendBatch(t.Context(), journal.AppendRequest{Journal: workspaceRef, TransactionID: "seed", Events: []protocol.ProposedEvent{{EventID: "seed", Time: time.Unix(1, 0).UTC(), PayloadVersion: 1, Kind: protocol.EventControlOperationInterrupted, Payload: payload}}}); err != nil {
		t.Fatal(err)
	}
	head, err := builder.store.Head(t.Context(), workspaceRef)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("---\nname: go-testing\ndescription: Test Go.\n---\nmetadata-only-sentinel\n")
	sum := sha256.Sum256(content)
	catalog, err := skills.Build(skills.BuildOptions{Discovery: skills.Discovery{Candidates: []skills.Candidate{{Name: "go-testing", Description: "Test Go.", Content: content, Source: protocol.SkillSourceGlobal, CanonicalPath: "/skills/global/go-testing/SKILL.md", ContentDigest: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: hex.EncodeToString(sum[:])}}}}, Policy: config.ProjectSkillsAsk, Workspace: builder.workspace, Generation: "generation"})
	if err != nil {
		t.Fatal(err)
	}
	durable, _, err := (runtimeBrokerSource{Repository: builder.store, Workspace: workspaceRef, Skills: catalog}).Project(t.Context(), SnapshotVector{WorkspaceControl: head})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(durable.Skills, catalog.Snapshot()) {
		t.Fatalf("skills = %#v, want %#v", durable.Skills, catalog.Snapshot())
	}
	encoded, err := json.Marshal(durable)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "metadata-only-sentinel") || strings.Contains(string(encoded), `"content"`) {
		t.Fatalf("durable projection leaked skill content: %s", encoded)
	}
	copy := catalog.Snapshot()
	copy.Active[0].Description = "changed"
	if durable.Skills.Active[0].Description == "changed" {
		t.Fatal("durable projection aliases catalog snapshot")
	}
	nilCatalog, _, err := (runtimeBrokerSource{Repository: builder.store, Workspace: workspaceRef}).Project(t.Context(), SnapshotVector{WorkspaceControl: head})
	if err != nil || nilCatalog.Skills.Revision != "" {
		t.Fatalf("nil catalog compatibility = %#v / %v", nilCatalog.Skills, err)
	}
}

func TestContextProjectionPolicyUsesImmutableGeneration(t *testing.T) {
	plan := func(g protocol.RuntimeGenerationID, input, window, output int64) protocol.EventRecord {
		return protocol.EventRecord{Envelope: protocol.EventEnvelope{RuntimeGenerationID: g}, Decoded: &protocol.ContextPlanRecordedV1{Plan: protocol.ContextPlan{Body: protocol.ContextPlanBody{EstimatedInputTokens: protocol.ValueInt64{State: protocol.ValueKnown, Value: input, Provenance: "e"}, ContextWindow: protocol.ValueInt64{State: protocol.ValueKnown, Value: window, Provenance: "w"}, OutputReserve: output, CompactionRevision: "r"}}}}
	}
	manifest := func(auto bool, reserve int64) protocol.RuntimeGenerationManifest {
		limits := protocol.RuntimeLimits{AutoCompact: auto}
		if reserve > 0 {
			limits.CompactReserveTokens = protocol.ValueInt64{State: protocol.ValueKnown, Value: reserve, Provenance: "c"}
		}
		return protocol.RuntimeGenerationManifest{Body: protocol.RuntimeGenerationBody{Limits: limits}}
	}
	for _, tc := range []struct {
		name                  string
		m                     protocol.RuntimeGenerationManifest
		input, window, output int64
		reason                string
	}{{"disabled", manifest(false, 0), 1, 9000, 1, "disabled"}, {"below", manifest(true, 0), 1, 9000, 1, "below_threshold"}, {"threshold", manifest(true, 0), 6952, 9000, 0, "threshold_reached"}, {"invalid", manifest(true, 9000), 1, 9000, 1, "invalid_budget"}} {
		t.Run(tc.name, func(t *testing.T) {
			tc.m.ID = "A"
			p := contextProjectionProjector{generations: map[protocol.RuntimeGenerationID]protocol.RuntimeGenerationManifest{"A": tc.m}}
			got, err := p.Apply(p.Zero(protocol.JournalRef{}), plan("A", tc.input, tc.window, tc.output))
			if err != nil || got.AutoReason != tc.reason {
				t.Fatalf("%+v %v", got, err)
			}
		})
	}
	p := contextProjectionProjector{generations: map[protocol.RuntimeGenerationID]protocol.RuntimeGenerationManifest{"A": manifest(false, 0)}, bootstrap: protocol.RuntimeGenerationManifest{ID: "B", Body: manifest(true, 0).Body}}
	got, _ := p.Apply(p.Zero(protocol.JournalRef{}), plan("A", 1, 9000, 1))
	if got.AutoReason != "disabled" {
		t.Fatalf("old generation=%+v", got)
	}
	got, _ = p.Apply(p.Zero(protocol.JournalRef{}), plan("missing", 1, 9000, 1))
	if got.AutoReason != "unknown_generation" || got.ReserveTokens.State != protocol.ValueUnknown {
		t.Fatalf("missing=%+v", got)
	}
	known := manifest(true, 0)
	known.ID = "A"
	projector := contextProjectionProjector{generations: map[protocol.RuntimeGenerationID]protocol.RuntimeGenerationManifest{"A": known}}
	got, _ = projector.Apply(projector.Zero(protocol.JournalRef{}), plan("A", 1, 9000, 1))
	if !got.AutoAvailable || got.ReserveTokens.Value != 2048 || got.ReserveTokens.Provenance != "compaction_policy" {
		t.Fatalf("default=%+v", got)
	}
	explicit := manifest(true, 3333)
	explicit.ID = "A"
	projector.generations["A"] = explicit
	got, _ = projector.Apply(projector.Zero(protocol.JournalRef{}), plan("A", 1, 9000, 1))
	if !got.AutoAvailable || got.ReserveTokens.Value != 3333 || got.ReserveTokens.Provenance != "compaction_policy" {
		t.Fatalf("explicit=%+v", got)
	}
	unknown := plan("A", 1, 9000, 1)
	unknown.Decoded.(*protocol.ContextPlanRecordedV1).Plan.Body.ContextWindow = protocol.ValueInt64{State: protocol.ValueUnknown}
	got, _ = projector.Apply(projector.Zero(protocol.JournalRef{}), unknown)
	if got.AutoAvailable || got.AutoReason != "unknown_context_window" || got.ReserveTokens.State != protocol.ValueUnknown {
		t.Fatalf("unknown=%+v", got)
	}
	fallback := contextProjectionProjector{bootstrap: protocol.RuntimeGenerationManifest{ID: "B", Body: manifest(true, 0).Body}}
	got, _ = fallback.Apply(fallback.Zero(protocol.JournalRef{}), plan("B", 1, 9000, 1))
	if !got.AutoAvailable {
		t.Fatalf("fallback=%+v", got)
	}
	state, _ := projector.Apply(projector.Zero(protocol.JournalRef{}), plan("A", 1, 9000, 1))
	compact := &protocol.ContextCompactedV1{From: protocol.CommittedCursor{JournalID: "s", CommitSeq: 1}, Through: protocol.CommittedCursor{JournalID: "s", CommitSeq: 2}, SummaryEvidenceID: "e", Revision: "r2"}
	state, _ = projector.Apply(state, protocol.EventRecord{Decoded: compact})
	if state.Revision != "r2" || state.SummaryEvidenceID != "e" || state.LatestRange == nil || state.LatestRange.Through.CommitSeq != 2 {
		t.Fatalf("sequential=%+v", state)
	}
}

func TestRuntimeSetReadyReturnsConfigurationErrorBeforeCompatibilityBypass(t *testing.T) {
	configuration := configurationError("/home/user/.config/yordam/config.jsonc", "configuration is invalid; edit the file and run /reload", nil)
	set := RuntimeSet{ConfigurationError: configuration, unchecked: true}

	if got := set.Ready(domain.ModelSelection{}); !errors.Is(got, configuration) {
		t.Fatalf("Ready() error = %v, want configuration error %v", got, configuration)
	}
}

func TestRuntimeBuilderBindsProvidersToolsLimitsAndCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(response, "data: [DONE]\n\n")
	}))
	defer server.Close()

	t.Setenv("PRIMARY_KEY", "primary-secret")
	t.Setenv("SECONDARY_KEY", "secondary-secret")
	t.Setenv("ORDINARY_VALUE", "preserved")
	cfg := loadRuntimeConfig(t, server.URL+"/v1", 1)
	builder := newRuntimeBuilderForTest(t, server.Client())
	set, err := builder.build(cfg, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	wantModels := []domain.ModelSelection{{Profile: "primary", Model: "a"}, {Profile: "primary", Model: "b"}, {Profile: "secondary", Model: "c"}}
	if !slices.Equal(set.Models, wantModels) || set.DefaultSelection != wantModels[0] {
		t.Fatalf("models=%+v default=%+v", set.Models, set.DefaultSelection)
	}
	if set.Credentials["primary"] != "primary-secret" || set.Credentials["secondary"] != "secondary-secret" {
		t.Fatalf("credentials not bound by provider")
	}
	if summary := fmt.Sprintf("%+v", set); strings.Contains(summary, "primary-secret") || strings.Contains(summary, "secondary-secret") {
		t.Fatalf("runtime set summary exposes credentials: %s", summary)
	}
	serialized, err := json.Marshal(set)
	if err != nil {
		t.Fatalf("serialize runtime set summary: %v", err)
	}
	if strings.Contains(string(serialized), "primary-secret") || strings.Contains(string(serialized), "secondary-secret") {
		t.Fatalf("serialized runtime set summary exposes credentials: %s", serialized)
	}
	if _, ok := set.Runtime.(*agent.OrchestratedRunner); !ok || set.Manifest.Body.Limits.MaxToolCalls != 7 || set.Manifest.Body.Limits.ShellTimeoutNanos != int64(time.Second) {
		t.Fatalf("runtime=%T limits=%+v", set.Runtime, set.Manifest.Body.Limits)
	}
	if len(set.ProviderCatalog.List()) != 3 || len(set.Manifest.Body.Tools) != 5 {
		t.Fatalf("provider models=%d tool descriptors=%d", len(set.ProviderCatalog.List()), len(set.Manifest.Body.Tools))
	}
}

func TestRuntimeBuilderBindsCatalogSkillsToConfigAndWorkspaceRoots(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "skill-test-key")
	cfg := loadRuntimeConfig(t, "https://example.invalid/v1", 120)
	builder := newRuntimeBuilderForTest(t, nil)
	configDir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	builder.configPath = filepath.Join(configDir, "config.jsonc")
	globalPath := filepath.Join(configDir, "skills", "go-testing", "SKILL.md")
	projectPath := filepath.Join(builder.workspace.CanonicalPath, ".yordam", "skills", "go-testing", "SKILL.md")
	writeRuntimeSkill(t, globalPath, "Global skill.", "global-generation-body")
	writeRuntimeSkill(t, projectPath, "Project skill.", "project-generation-body")

	for _, test := range []struct {
		policy     config.ProjectSkillPolicy
		wantSource protocol.SkillSource
		wantBody   string
	}{
		{config.ProjectSkillsAllow, protocol.SkillSourceProject, "project-generation-body"},
		{config.ProjectSkillsAsk, protocol.SkillSourceGlobal, "global-generation-body"},
		{config.ProjectSkillsDeny, protocol.SkillSourceGlobal, "global-generation-body"},
	} {
		t.Run(string(test.policy), func(t *testing.T) {
			candidate := cfg
			candidate.Skills.ProjectPolicy = test.policy
			set, err := builder.build(candidate, domain.ModelSelection{})
			if err != nil {
				t.Fatal(err)
			}
			snapshot := set.Skills.Snapshot()
			loaded, ok := set.Skills.Load("go-testing")
			if !ok || loaded.Identity.Source != test.wantSource || !strings.Contains(string(loaded.Content), test.wantBody) || len(snapshot.Active) != 1 || !slices.EqualFunc(snapshot.Active, set.Manifest.Body.Skills, sameRuntimeSkillDescriptor) || set.Manifest.Body.SkillCatalogRevision != snapshot.Revision {
				t.Fatalf("snapshot=%#v loaded=%#v manifest=%#v", snapshot, loaded, set.Manifest.Body)
			}
			for _, descriptor := range snapshot.Discovered {
				if descriptor.Identity.RuntimeGenerationID != set.RuntimeGenerationID {
					t.Fatalf("generation=%q descriptor=%#v", set.RuntimeGenerationID, descriptor)
				}
			}
			aliases := make([]string, 0, len(set.Manifest.Body.Tools))
			for _, descriptor := range set.Manifest.Body.Tools {
				aliases = append(aliases, descriptor.Body.Identity.Name)
			}
			if !slices.Equal(aliases, []string{"read", "search", "skill", "edit", "shell"}) {
				t.Fatalf("tool order=%v", aliases)
			}
			writeRuntimeSkill(t, projectPath, "Project skill.", "changed-after-build")
			frozen, _ := set.Skills.Load("go-testing")
			if !strings.Contains(string(frozen.Content), test.wantBody) {
				t.Fatalf("built catalog mutated by source edit: %q", frozen.Content)
			}
		})
	}
}

func writeRuntimeSkill(t *testing.T, path, description, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("---\nname: go-testing\ndescription: "+description+"\n---\n"+body+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeGenerationManifestIsImmutableAcrossCandidateBuilds(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "generation-a")
	cfg := loadRuntimeConfig(t, "https://example.invalid/v1", 120)
	builder := newRuntimeBuilderForTest(t, nil)
	first, err := builder.build(cfg, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Validate(); err != nil {
		t.Fatalf("first runtime set: %v", err)
	}
	want := protocol.DeepCopy(first.Manifest)

	t.Setenv("PRIMARY_KEY", "generation-b")
	second, err := builder.build(cfg, domain.ModelSelection{Profile: "secondary", Model: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Validate(); err != nil {
		t.Fatalf("second runtime set: %v", err)
	}
	if first.Manifest.ID == second.Manifest.ID {
		t.Fatalf("runtime generations were not independently identified: first=%+v second=%+v", first.Manifest, second.Manifest)
	}
	if got := first.Manifest; !reflect.DeepEqual(got, want) {
		t.Fatalf("generation A changed while preparing B:\n got: %#v\nwant: %#v", got, want)
	}

	second.Manifest.Body.Models[0].DisplayName = "mutated candidate"
	second.Manifest.Body.Tools[0].Body.InputSchema[0] = '['
	if got := first.Manifest; !reflect.DeepEqual(got, want) {
		t.Fatalf("candidate manifest aliases active generation:\n got: %#v\nwant: %#v", got, want)
	}
	if err := second.Validate(); err == nil {
		t.Fatal("tampered candidate manifest passed validation")
	}
}

func TestRuntimeReloadKeepsCapturedGenerationDependenciesImmutable(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "generation-a")
	builder := newRuntimeBuilderForTest(t, nil)
	cfg := loadRuntimeConfig(t, "https://example.invalid/v1", 120)
	first, err := builder.build(cfg, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	oldAdapter := first.LegacyAdapter
	oldCompact, err := oldAdapter.Command(Command{Kind: CommandCompact})
	if err != nil {
		t.Fatal(err)
	}

	// A candidate receives fresh generation-scoped services while the captured
	// generation-A adapter remains an immutable compatibility closure.
	t.Setenv("PRIMARY_KEY", "generation-b")
	cfg.Context.AutoCompact = false
	second, err := builder.build(cfg, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	defer second.retireSecrets()
	if first.RuntimeGenerationID == second.RuntimeGenerationID || first.ProviderService == second.ProviderService || first.ToolService == second.ToolService || first.Orchestrator == second.Orchestrator || first.Broker == second.Broker || first.LegacyAdapter == second.LegacyAdapter || first.AuthorizationService == second.AuthorizationService {
		t.Fatalf("candidate reused generation-bound dependencies: first=%+v second=%+v", first, second)
	}

	workspaceControl, err := builder.store.EnsureWorkspaceControl(t.Context(), builder.workspace)
	if err != nil {
		t.Fatal(err)
	}
	selection := cfg.DefaultSelection()
	application := New(Options{
		RuntimeSet: first, Sessions: builder.store, Workspace: builder.workspace, WorkspaceControl: workspaceControl,
		Session:     domain.Session{ID: builder.activeSession.get(), Workspace: builder.workspace, Selection: selection},
		Replay:      domain.SessionReplay{Session: domain.Session{ID: builder.activeSession.get(), Workspace: builder.workspace, Selection: selection}},
		EventBuffer: 1,
	})
	application.completeReload(t.Context(), operationResult{kind: operationReload, runtimeSet: second})
	if application.runtimeSet.RuntimeGenerationID != second.RuntimeGenerationID || application.runtimeSet.ProviderService != second.ProviderService || application.runtimeSet.AuthorizationService != second.AuthorizationService {
		t.Fatalf("reload did not activate candidate generation: active=%+v candidate=%+v", application.runtimeSet, second)
	}
	oldAfter, err := oldAdapter.Command(Command{Kind: CommandCompact})
	if err != nil {
		t.Fatal(err)
	}
	newCompact, err := second.LegacyAdapter.Command(Command{Kind: CommandCompact})
	if err != nil {
		t.Fatal(err)
	}
	if oldAfter.CommandID != oldCompact.CommandID || newCompact.CommandID == oldCompact.CommandID {
		t.Fatalf("compact identity was not generation-bound: old=%s old_after=%s new=%s", oldCompact.CommandID, oldAfter.CommandID, newCompact.CommandID)
	}
}

func TestRuntimeCompositionUsesOnlyOrchestratedRunner(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "primary-secret")
	builder := newRuntimeBuilderForTest(t, nil)
	set, err := builder.build(loadRuntimeConfig(t, "https://example.invalid/v1", 120), domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := set.Runtime.(*agent.OrchestratedRunner); !ok {
		t.Fatalf("production runtime=%T, want *agent.OrchestratedRunner", set.Runtime)
	}
	if set.Orchestrator == nil || set.ProviderCatalog == nil || set.ProviderService == nil || set.ToolService == nil || set.AuthorizationService == nil || set.Broker == nil || set.ApplicationService == nil || set.LegacyAdapter == nil || set.CompactSession == nil {
		t.Fatalf("foundation composition is incomplete: %+v", set)
	}
}

func TestRuntimeBuilderProjectsCompactionPolicyIntoImmutableManifest(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "primary-secret")
	reserve := int64(8_192)
	cfg := loadRuntimeConfig(t, "https://example.invalid/v1", 120)
	cfg.Context.AutoCompact = false
	cfg.Context.CompactReserveTokens = &reserve

	builder := newRuntimeBuilderForTest(t, nil)
	set, err := builder.build(cfg, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	limits := set.Manifest.Body.Limits
	if limits.AutoCompact || limits.CompactReserveTokens.State != protocol.ValueKnown || limits.CompactReserveTokens.Value != reserve || limits.CompactReserveTokens.Provenance != "configured" {
		t.Fatalf("compaction limits=%+v", limits)
	}
	if err := set.Validate(); err != nil {
		t.Fatalf("valid runtime rejected: %v", err)
	}

	cfg.Context.CompactReserveTokens = nil
	defaultBuilder := newRuntimeBuilderForTest(t, nil)
	defaulted, err := defaultBuilder.build(cfg, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	if got := defaulted.Manifest.Body.Limits.CompactReserveTokens; got != (protocol.ValueInt64{State: protocol.ValueUnknown}) {
		t.Fatalf("default reserve=%+v want unknown", got)
	}
}

func TestProductionCompactProtocolCommandUsesRuntimeCompactionService(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "primary-secret")
	builder := newRuntimeBuilderForTest(t, nil)
	set, err := builder.build(loadRuntimeConfig(t, "https://example.invalid/v1", 120), domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	command, err := set.LegacyAdapter.Command(Command{Kind: CommandCompact})
	if err != nil {
		t.Fatal(err)
	}
	result, err := set.ApplicationService.Execute(t.Context(), command)
	if err != nil || result.Error == nil || result.Error.Code == codeUnsupportedCommand {
		t.Fatalf("compact result=%+v err=%v, want a serializable compaction service failure rather than dispatcher rejection", result, err)
	}
}

func TestProductionCompactProtocolRejectsStaleCursorBeforeProviderEgress(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	t.Setenv("PRIMARY_KEY", "primary-secret")
	builder := newRuntimeBuilderForTest(t, server.Client())
	set, err := builder.build(loadRuntimeConfig(t, server.URL+"/v1", 120), domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	command, err := set.LegacyAdapter.Command(Command{Kind: CommandCompact})
	if err != nil {
		t.Fatal(err)
	}
	stale := *command.Expected.Session
	stale.CommitSeq++
	stale.TransactionID = "stale-cursor"
	command.Expected.Session = &stale
	command.RequestDigest, err = CanonicalRequestDigest(command)
	if err != nil {
		t.Fatal(err)
	}

	result, err := set.ApplicationService.Execute(t.Context(), command)
	if err != nil || result.Error == nil || result.Error.Code != "stale_cursor" || requests.Load() != 0 {
		t.Fatalf("result=%+v requests=%d err=%v", result, requests.Load(), err)
	}
}

func TestProductionCompactProtocolRequiresIdleSessionWithoutProviderEgress(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	t.Setenv("PRIMARY_KEY", "primary-secret")
	builder := newRuntimeBuilderForTest(t, server.Client())
	set, err := builder.build(loadRuntimeConfig(t, server.URL+"/v1", 120), domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(builder.activeSession.get())}
	head, err := builder.store.Head(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := canonicaljson.Marshal(protocol.TurnAcceptedV1{CommandID: "active-command", Goal: "active", OutcomeContractID: "active-contract", ContractVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	appended, err := builder.store.AppendBatch(t.Context(), journal.AppendRequest{
		Journal: ref, ExpectedHead: head, TransactionID: "active-turn",
		Compatibility: &journal.CompatibilityDeclaration{ReaderVersion: protocol.EnvelopeVersion, WriterVersion: protocol.EnvelopeVersion, LegacyHead: head},
		Events:        []protocol.ProposedEvent{{EventID: "active-turn-accepted", Time: time.Now().UTC(), PayloadVersion: 1, Kind: protocol.EventTurnAccepted, SessionID: protocol.SessionID(ref.ID), TurnID: "active-turn", Payload: payload}},
	})
	if err != nil || appended.Status != journal.AppendCommitted {
		t.Fatalf("append=%+v err=%v", appended, err)
	}
	command, err := set.LegacyAdapter.Command(Command{Kind: CommandCompact})
	if err != nil {
		t.Fatal(err)
	}
	result, err := set.ApplicationService.Execute(t.Context(), command)
	if err != nil || result.Error == nil || result.Error.Code != "session_not_idle" || result.Error.Retryable || requests.Load() != 0 {
		t.Fatalf("result=%+v requests=%d err=%v", result, requests.Load(), err)
	}
}

func TestProductionCompactProtocolSerializesCancelledContext(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "primary-secret")
	builder := newRuntimeBuilderForTest(t, nil)
	set, err := builder.build(loadRuntimeConfig(t, "https://example.invalid/v1", 120), domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	command, err := set.LegacyAdapter.Command(Command{Kind: CommandCompact})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := set.ApplicationService.Execute(ctx, command)
	if err != nil || result.Error == nil || result.Error.Code != "cancelled" || result.Error.Retryable || strings.Contains(result.Error.Message, "context canceled") {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestProductionLegacyCompactNeverReportsMissingConfiguration(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "primary-secret")
	builder := newRuntimeBuilderForTest(t, nil)
	set, err := builder.build(loadRuntimeConfig(t, "https://example.invalid/v1", 120), domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	inspection, err := builder.store.InspectSession(t.Context(), protocol.SessionID(builder.activeSession.get()))
	if err != nil {
		t.Fatal(err)
	}
	replay := LegacyReplayFromInspection(inspection)
	application := New(Options{RuntimeSet: set, Sessions: builder.store, Session: replay.Session, Replay: replay, Workspace: builder.workspace})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go application.Run(ctx)
	application.Commands() <- Command{Kind: CommandCompact}
	var event Event
	for range 8 {
		select {
		case event = <-application.Events():
			if event.Kind == EventError || event.Kind == EventTurnCompleted || event.Kind == EventTurnInterrupted {
				goto terminal
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for compact result")
		}
	}
	t.Fatal("compact did not produce a terminal event")
terminal:
	if strings.Contains(event.Message, "compaction is not configured") {
		t.Fatalf("valid production runtime rejected compact as unconfigured: %+v", event)
	}
}

func TestProductionLegacyCommandUsesApplicationProtocolAndRealCursorProjection(t *testing.T) {
	var providerRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/event-stream")
		switch providerRequests.Add(1) {
		case 1:
			arguments := `{"path":"protocol-evidence.txt","create":true,"new_content":"committed evidence\n"}`
			fmt.Fprintf(response, "data: {\"id\":\"request-production-tool\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"edit-production\",\"function\":{\"name\":\"edit\",\"arguments\":%q}}]}}]}\n\n", arguments)
			fmt.Fprintln(response, `data: {"id":"request-production-tool","choices":[{"delta":{},"finish_reason":"tool_calls"}]}`)
			fmt.Fprintln(response)
			fmt.Fprintln(response, "data: [DONE]")
			fmt.Fprintln(response)
			return
		case 2:
			fmt.Fprintln(response, `data: {"id":"request-production","choices":[{"delta":{"content":"protocol reply"},"finish_reason":"stop"}]}`)
		default:
			fmt.Fprintln(response, `data: {"id":"request-production-compact","choices":[{"delta":{"content":"{\"goal\":\"compact\",\"constraints\":[],\"decisions\":[],\"files\":[],\"commands_and_tests\":[],\"unresolved\":[],\"children\":[],\"skills\":[],\"unknown_effects\":[]}"},"finish_reason":"stop"}]}`)
		}
		fmt.Fprintln(response)
		fmt.Fprintln(response, "data: [DONE]")
		fmt.Fprintln(response)
	}))
	defer server.Close()

	root := t.TempDir()
	configPath := filepath.Join(root, "config.jsonc")
	configBody := fmt.Sprintf(`{
  "model": "primary/model-a",
  "provider": {
    "primary": {
      "options": {"baseURL": %q, "apiKeyEnv": "PRIMARY_KEY"},
      "models": {"model-a": {}}
    }
  }
}`, server.URL)
	if err := os.WriteFile(configPath, []byte(configBody), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PRIMARY_KEY", "production-key")
	application, bootstrapSnapshot, err := Bootstrap(t.Context(), BootstrapOptions{
		ConfigPath: configPath, CWD: root, HTTPClient: server.Client(),
		CLI: cli.Options{Mode: domain.ModeAsk, DataDir: filepath.Join(root, "data"), MaxToolCalls: 32, ShellTimeout: 2 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	initial, subscription, err := application.runtimeSet.ApplicationService.SnapshotAndSubscribe(t.Context(), protocol.SnapshotRequest{
		ProtocolVersion: protocol.ApplicationProtocolVersion, SelectedSessionID: protocol.SessionID(bootstrapSnapshot.Session.ID), Consumer: "production-equivalence", QueueCapacity: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	if initial.Durable.Task != nil || len(initial.Durable.Activities) != 0 {
		t.Fatalf("initial projection unexpectedly contains turn state: %+v", initial.Durable)
	}

	application.runtimeSet.Runtime = forbiddenLegacyRuntime{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- application.Run(ctx) }()
	tuiSemantic := []string{}
	application.Commands() <- Command{Kind: CommandChangeMode, Mode: domain.ModeAuto}
	for {
		select {
		case event := <-application.Events():
			if event.Kind == EventError || event.Kind == EventRejected {
				t.Fatalf("production setting command failed: %+v", event)
			}
			if event.Kind == EventState && event.Mode == domain.ModeAuto {
				tuiSemantic = append(tuiSemantic, legacyConsumerSemantic(event)...)
				goto modeChanged
			}
		case <-time.After(20 * time.Second):
			t.Fatal("timed out waiting for application-protocol setting")
		}
	}

modeChanged:
	store := application.sessions.(*jsonl.Store)
	workspaceInspection, err := store.Inspect(t.Context(), application.workspaceControl)
	if err != nil {
		t.Fatal(err)
	}
	sessionInspection, err := store.Inspect(t.Context(), protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(bootstrapSnapshot.Session.ID)})
	if err != nil {
		t.Fatal(err)
	}
	workspaceKinds := inspectionKinds(workspaceInspection.Events)
	sessionKinds := inspectionKinds(sessionInspection.Events)
	if !slices.Contains(workspaceKinds, protocol.EventControlOperationAuthorized) || !slices.Contains(workspaceKinds, protocol.EventAuthorizationDecisionConsumed) || !slices.Contains(workspaceKinds, protocol.EventControlOperationStarted) || !slices.Contains(workspaceKinds, protocol.EventCommandCompleted) {
		t.Fatalf("setting command missed canonical workspace control lifecycle: %v", workspaceKinds)
	}
	if countString(sessionKinds, protocol.EventModeChanged) != 1 || slices.Contains(sessionKinds, protocol.EventControlOperationStarted) {
		t.Fatalf("setting consequence was not isolated to the session journal: %v", sessionKinds)
	}

	application.Commands() <- Command{Kind: CommandStartTurn, DraftID: 41, Prompt: "through application protocol"}
	for {
		select {
		case event := <-application.Events():
			if event.Kind == EventError {
				t.Fatalf("production command failed: %+v", event)
			}
			if event.Kind == EventTurnCompleted {
				tuiSemantic = append(tuiSemantic, legacyConsumerSemantic(event)...)
				goto completed
			}
			tuiSemantic = append(tuiSemantic, legacyConsumerSemantic(event)...)
		case <-time.After(20 * time.Second):
			t.Fatal("timed out waiting for application-protocol turn")
		}
	}

completed:
	current, currentSubscription, err := application.runtimeSet.ApplicationService.SnapshotAndSubscribe(t.Context(), protocol.SnapshotRequest{
		ProtocolVersion: protocol.ApplicationProtocolVersion, SelectedSessionID: protocol.SessionID(bootstrapSnapshot.Session.ID), Consumer: "production-equivalence", QueueCapacity: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = currentSubscription.Close()
	if current.Durable.Task == nil || len(current.Durable.Activities) < 2 || len(current.Durable.Evidence) == 0 || !strings.Contains(string(current.Durable.Task.Data), "through application protocol") {
		t.Fatalf("current durable projection is not journal-derived: %+v", current.Durable)
	}
	if !strings.Contains(string(current.Durable.Permissions.Data), `"decisions":`) || strings.Contains(string(current.Durable.Permissions.Data), `"decisions":{}`) {
		t.Fatalf("current durable projection has no committed authorization state: %s", current.Durable.Permissions.Data)
	}
	if !strings.Contains(string(current.Durable.Workspace.Data), `"consumed":`) || strings.Contains(string(current.Durable.Workspace.Data), `"consumed":{}`) {
		t.Fatalf("current durable projection has no committed control authorization state: %s", current.Durable.Workspace.Data)
	}
	_, compactionSubscription, err := application.runtimeSet.ApplicationService.SnapshotAndSubscribe(t.Context(), protocol.SnapshotRequest{
		ProtocolVersion: protocol.ApplicationProtocolVersion, SelectedSessionID: protocol.SessionID(bootstrapSnapshot.Session.ID), Consumer: "compaction-protocol", QueueCapacity: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer compactionSubscription.Close()
	compactCommand, err := application.runtimeSet.LegacyAdapter.Command(Command{Kind: CommandCompact})
	if err != nil {
		t.Fatal(err)
	}
	providerBeforeCompact := providerRequests.Load()
	firstCompact, err := application.runtimeSet.ApplicationService.Execute(t.Context(), compactCommand)
	if err != nil || firstCompact.Status != "completed" {
		t.Fatalf("first compact=%+v err=%v", firstCompact, err)
	}
	secondCompact, err := application.runtimeSet.ApplicationService.Execute(t.Context(), compactCommand)
	if err != nil || !reflect.DeepEqual(secondCompact, firstCompact) || providerRequests.Load() != providerBeforeCompact+1 {
		t.Fatalf("replayed compact=%+v first=%+v calls=%d before=%d err=%v", secondCompact, firstCompact, providerRequests.Load(), providerBeforeCompact, err)
	}
	streamCtx, streamCancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer streamCancel()
	sawProgress, sawCompletion := false, false
	for !sawProgress || !sawCompletion {
		item, nextErr := compactionSubscription.Next(streamCtx)
		if nextErr != nil {
			t.Fatalf("compaction subscription progress=%t completion=%t err=%v", sawProgress, sawCompletion, nextErr)
		}
		if item.Event == nil {
			continue
		}
		switch item.Event.Kind {
		case protocol.EventActivityStarted:
			sawProgress = true
		case protocol.EventCommandCompleted:
			sawCompletion = true
		}
	}
	oldDurable, _, err := (runtimeBrokerSource{Repository: store, Workspace: application.workspaceControl, Generation: application.runtimeSet.RuntimeGenerationID}).Project(t.Context(), SnapshotVector{
		WorkspaceControl: initial.Cursor.WorkspaceControl, SelectedSession: initial.Cursor.SelectedSession,
	})
	if err != nil {
		t.Fatal(err)
	}
	if oldDurable.Task != nil || len(oldDurable.Activities) != 0 {
		t.Fatalf("projection at old cursor observed later turn: %+v", oldDurable)
	}

	legacyCompleted := false
	legacySemantic := []string{}
	for !legacyCompleted {
		item, err := subscription.Next(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if item.Event == nil {
			continue
		}
		legacy, err := application.runtimeSet.LegacyAdapter.Event(*item.Event)
		if err != nil {
			t.Fatal(err)
		}
		legacySemantic = append(legacySemantic, legacyConsumerSemantic(legacy)...)
		legacyCompleted = legacy.Kind == EventTurnCompleted
	}
	raw, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	var headless protocol.ApplicationSnapshot
	if err := json.Unmarshal(raw, &headless); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(current.Durable, headless.Durable) || !reflect.DeepEqual(tuiSemantic, legacySemantic) {
		t.Fatalf("consumer equivalence tui=%v legacy=%v headless=%+v current=%+v", tuiSemantic, legacySemantic, headless.Durable, current.Durable)
	}
	compactedInspection, err := store.InspectSession(t.Context(), protocol.SessionID(bootstrapSnapshot.Session.ID))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(inspectionKinds(compactedInspection.Journal.Events), protocol.EventContextCompacted) {
		t.Fatalf("legacy compact missed canonical compaction event: %v", inspectionKinds(compactedInspection.Journal.Events))
	}
	application.Commands() <- Command{Kind: CommandShutdown}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestAppRunCancelledProductionCompactPublishesOneInterruptedTerminal(t *testing.T) {
	var blockCompaction atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "text/event-stream")
		if blockCompaction.Load() {
			<-request.Context().Done()
			return
		}
		fmt.Fprintf(response, "data: {\"id\":\"cancelled-compact\",\"choices\":[{\"delta\":{\"content\":%q},\"finish_reason\":\"stop\"}]}\n\n", "ordinary turn reply")
		fmt.Fprintln(response, "data: [DONE]")
		fmt.Fprintln(response)
	}))
	defer server.Close()
	t.Setenv("PRIMARY_KEY", "production-cancel-key")
	root := t.TempDir()
	configPath := filepath.Join(root, "config.jsonc")
	configBody := fmt.Sprintf(`{"model":"primary/model-a","provider":{"primary":{"options":{"baseURL":%q,"apiKeyEnv":"PRIMARY_KEY"},"models":{"model-a":{}}}}}`, server.URL)
	if err := os.WriteFile(configPath, []byte(configBody), 0o600); err != nil {
		t.Fatal(err)
	}
	application, _, err := Bootstrap(t.Context(), BootstrapOptions{
		ConfigPath: configPath, CWD: root, HTTPClient: server.Client(),
		CLI: cli.Options{Mode: domain.ModeAsk, DataDir: filepath.Join(root, "data"), MaxToolCalls: 32, ShellTimeout: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- application.Run(ctx) }()

	awaitTurn := func(prompt string) {
		t.Helper()
		application.Commands() <- Command{Kind: CommandStartTurn, Prompt: prompt}
		accepted := false
		for {
			select {
			case event := <-application.Events():
				switch event.Kind {
				case EventError, EventTurnInterrupted, EventCompactionCompleted, EventCompactionFailed:
					t.Fatalf("turn %q unexpected event=%+v", prompt, event)
				case EventTurnAccepted:
					accepted = true
				case EventTurnCompleted:
					if !accepted {
						t.Fatalf("turn %q completed before acceptance", prompt)
					}
					return
				}
			case <-ctx.Done():
				t.Fatalf("timed out waiting for turn %q: %v", prompt, ctx.Err())
			}
		}
	}

	// Build the durable history required for manual compaction, then make only
	// the compact provider stream wait for cancellation.
	awaitTurn("one")
	awaitTurn("two")
	blockCompaction.Store(true)
	application.Commands() <- Command{Kind: CommandCompact}
	cancelled, terminals := false, 0
	for terminals == 0 {
		select {
		case event := <-application.Events():
			switch event.Kind {
			case EventError:
				t.Fatalf("cancelled compact emitted generic error: %+v", event)
			case EventCompactionStarted:
				if event.Compaction != nil && event.Compaction.Trigger == "manual" && !cancelled {
					cancelled = true
					application.Commands() <- Command{Kind: CommandCancelTurn}
				}
			case EventCompactionCompleted:
				t.Fatalf("cancelled compact completed: %+v", event)
			case EventCompactionFailed:
				terminals++
				if !cancelled || event.Code != "compaction_interrupted" || event.Compaction == nil || event.Compaction.Stage != protocol.CompactionCancelled || event.Compaction.Error == nil || event.Compaction.Error.Code != "compaction_interrupted" {
					t.Fatalf("cancelled compact terminal=%+v cancelled=%t", event, cancelled)
				}
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for cancelled compact terminal: %v", ctx.Err())
		}
	}
	blockCompaction.Store(false)
	// A subsequent command proves the lane is clear and also catches a delayed
	// duplicate compaction terminal before the next turn can complete.
	awaitTurn("after cancelled compact")
	application.Commands() <- Command{Kind: CommandShutdown}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatalf("timed out shutting down: %v", ctx.Err())
	}
}

func TestAppRunCompactProtocolSubscriptionDoesNotLeakTerminalEvents(t *testing.T) {
	var providerCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/event-stream")
		content := "ordinary turn reply"
		if providerCalls.Add(1) > 2 {
			content = `{"goal":"compact","constraints":[],"decisions":[],"files":[],"commands_and_tests":[],"unresolved":[],"children":[],"skills":[],"unknown_effects":[]}`
		}
		fmt.Fprintf(response, "data: {\"id\":\"app-run-%d\",\"choices\":[{\"delta\":{\"content\":%q},\"finish_reason\":\"stop\"}]}\n\n", providerCalls.Load(), content)
		fmt.Fprintln(response, "data: [DONE]")
		fmt.Fprintln(response)
	}))
	defer server.Close()

	root := t.TempDir()
	configPath := filepath.Join(root, "config.jsonc")
	configBody := fmt.Sprintf(`{"model":"primary/model-a","provider":{"primary":{"options":{"baseURL":%q,"apiKeyEnv":"PRIMARY_KEY"},"models":{"model-a":{}}}}}`, server.URL)
	if err := os.WriteFile(configPath, []byte(configBody), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PRIMARY_KEY", "app-run-key")
	application, _, err := Bootstrap(t.Context(), BootstrapOptions{
		ConfigPath: configPath, CWD: root, HTTPClient: server.Client(),
		CLI: cli.Options{Mode: domain.ModeAsk, DataDir: filepath.Join(root, "data"), MaxToolCalls: 32, ShellTimeout: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	// This is deliberately one deadline for the entire sequence. Under -race
	// the provider and journal operations can be individually slow without
	// indicating a protocol failure; the required condition is that every
	// operation reaches its terminal event and the immediately following turn
	// can proceed without receiving a leaked terminal from its predecessor.
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- application.Run(ctx) }()

	awaitTerminal := func(command Command, requireAccepted bool) {
		t.Helper()
		application.Commands() <- command
		accepted, terminals, contextState := false, 0, false
		for terminals == 0 {
			select {
			case event := <-application.Events():
				if event.Kind == EventState && event.Context != nil {
					contextState = true
				}
				if event.Kind == EventTurnAccepted {
					accepted = true
				}
				terminal := event.Kind == EventTurnCompleted || event.Kind == EventTurnInterrupted || event.Kind == EventError
				if command.Kind == CommandCompact {
					terminal = terminal || event.Kind == EventCompactionCompleted || event.Kind == EventCompactionFailed
				}
				if terminal {
					terminals++
					if command.Kind == CommandCompact && event.Kind != EventCompactionCompleted {
						t.Fatalf("command %s terminal=%+v", command.Kind, event)
					}
					if command.Kind != CommandCompact && event.Kind != EventTurnCompleted {
						t.Fatalf("command %s terminal=%+v", command.Kind, event)
					}
				}
			case <-ctx.Done():
				t.Fatalf("timed out waiting for %s terminal: %v", command.Kind, ctx.Err())
			}
		}
		if requireAccepted && !accepted {
			t.Fatalf("%s terminal arrived before its accepted event", command.Kind)
		}
		if !contextState {
			t.Fatalf("%s did not receive durable context state before terminal", command.Kind)
		}
	}

	// Two completed turns create a sufficiently rich durable history for the
	// first compact; the intervening turn makes the second compact meaningful.
	awaitTerminal(Command{Kind: CommandStartTurn, Prompt: "one"}, true)
	awaitTerminal(Command{Kind: CommandStartTurn, Prompt: "two"}, true)
	awaitTerminal(Command{Kind: CommandCompact}, false)
	awaitTerminal(Command{Kind: CommandStartTurn, Prompt: "after first compact"}, true)
	awaitTerminal(Command{Kind: CommandCompact}, false)
	awaitTerminal(Command{Kind: CommandStartTurn, Prompt: "after second compact"}, true)

	application.Commands() <- Command{Kind: CommandShutdown}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("app shutdown: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("timed out shutting down app: %v", ctx.Err())
	}
}

func legacyConsumerSemantic(event Event) []string {
	switch event.Kind {
	case EventState:
		// A full durable snapshot is bootstrap metadata, not an event in the
		// legacy durable stream. Context-plan events deliberately have no policy
		// reason and remain part of the equivalence sequence below.
		if event.Context != nil && event.Context.AutoReason != "" {
			return nil
		}
		return []string{"state"}
	case EventTurnAccepted:
		return []string{"turn.accepted"}
	case EventTextDelta:
		return []string{"text:" + event.Runtime.Text}
	case EventTurnCompleted:
		return []string{"turn.completed"}
	default:
		return nil
	}
}

func inspectionKinds(events []protocol.EventRecord) []string {
	kinds := make([]string, 0, len(events))
	for _, event := range events {
		kinds = append(kinds, event.Envelope.Kind)
	}
	return kinds
}

func inspectionSequenceRefs(events []protocol.EventRecord) []string {
	values := make([]string, 0, len(events))
	for _, event := range events {
		values = append(values, fmt.Sprintf("%d/%s", event.Envelope.Seq, event.Envelope.TransactionID))
	}
	return values
}

func countString(values []string, target string) int {
	count := 0
	for _, value := range values {
		if value == target {
			count++
		}
	}
	return count
}

type forbiddenLegacyRuntime struct{}

func (forbiddenLegacyRuntime) RunTurn(context.Context, agent.RunInput) error {
	return errors.New("legacy Runtime.RunTurn bypass was invoked")
}

func TestRuntimeGenerationsKeepImmutableRedactors(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "generation-a")
	cfg := loadRuntimeConfig(t, "https://example.invalid/v1", 120)
	builder := newRuntimeBuilderForTest(t, nil)
	first, err := builder.build(cfg, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("PRIMARY_KEY", "generation-b")
	second, err := builder.build(cfg, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	if got := first.Redactor.String("generation-a generation-b"); got != "[REDACTED] generation-b" {
		t.Fatalf("first redactor=%q", got)
	}
	if got := second.Redactor.String("generation-a generation-b"); got != "generation-a [REDACTED]" {
		t.Fatalf("second redactor=%q", got)
	}
	firstRunner, ok := first.Runtime.(*agent.OrchestratedRunner)
	if !ok {
		t.Fatalf("first runtime=%T", first.Runtime)
	}
	release, err := firstRunner.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	first.retireSecrets()
	if got := first.Redactor.String("generation-a generation-b"); got != "[REDACTED] generation-b" {
		t.Fatalf("active first generation redactor=%q", got)
	}
	release()
	if _, err := firstRunner.Acquire(); err == nil {
		t.Fatal("retired generation accepted a new turn lease")
	}
}

func TestRuntimeGenerationBootstrapBindingAdvancesStoreWhileOldOutputIsImmutable(t *testing.T) {
	const generationA = "generation-a"
	const generationB = "generation-b"
	t.Setenv("PRIMARY_KEY", generationA)

	configPath := filepath.Join(t.TempDir(), "config.jsonc")
	if err := os.WriteFile(configPath, []byte(`{
  "model": "primary/a",
  "provider": {
    "primary": {
      "options": {"baseURL": "https://example.invalid/v1", "apiKeyEnv": "PRIMARY_KEY"},
      "models": {"a": {}}
    }
  }
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	application, _, err := Bootstrap(t.Context(), BootstrapOptions{
		ConfigPath: configPath,
		CLI: cli.Options{
			Mode:         domain.ModeAsk,
			DataDir:      t.TempDir(),
			MaxToolCalls: 32,
			ShellTimeout: 120 * time.Second,
		},
		CWD: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	firstSet := application.runtimeSet
	firstRunner, ok := firstSet.Runtime.(*agent.OrchestratedRunner)
	if !ok {
		t.Fatalf("first runtime=%T", application.runtimeSet.Runtime)
	}
	release, err := firstRunner.Acquire()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- application.Run(ctx) }()
	t.Setenv("PRIMARY_KEY", generationB)
	application.Commands() <- Command{Kind: CommandReloadConfig}
	select {
	case event := <-application.Events():
		if event.Kind != EventReloadCompleted || !event.Applied || event.Err != nil {
			t.Fatalf("reload event=%+v", event)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for reload")
	}

	if got := firstSet.Redactor.String(generationA + " " + generationB); got != "[REDACTED] "+generationB {
		t.Fatalf("active old-generation redactor=%q", got)
	}
	if got := application.redactors.String(generationA + " " + generationB); got != generationA+" [REDACTED]" {
		t.Fatalf("new production binding=%q", got)
	}
	release()
	if _, err := firstRunner.Acquire(); err == nil {
		t.Fatal("retired runtime started a new producer")
	}

	application.Commands() <- Command{Kind: CommandShutdown}
	if err := <-done; err != nil {
		t.Fatalf("app shutdown: %v", err)
	}
}

func TestRuntimeBuilderRedactsConfiguredAndOverrideCredentials(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "configured-secret")
	t.Setenv("YORDAM_API_KEY", "override-secret")
	cfg := loadRuntimeConfig(t, "https://example.invalid/v1", 120)
	builder := newRuntimeBuilderForTest(t, nil)
	set, err := builder.build(cfg, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	if set.Credentials["primary"] != "override-secret" {
		t.Fatalf("effective credential=%q", set.Credentials["primary"])
	}
	if got := set.Redactor.String("configured-secret override-secret"); got != "[REDACTED] [REDACTED]" {
		t.Fatalf("redacted=%q", got)
	}
	if set.Admission == nil || set.RuntimeGenerationID == "" {
		t.Fatalf("runtime has no generation lease: %+v", set)
	}
	if got := set.Redactor.String("Y29uZmlndXJlZC1zZWNyZXQ="); got != "[REDACTED]" {
		t.Fatalf("encoded redacted=%q", got)
	}
}

func TestRuntimeBuilderRegistersGenerationInSharedProductionRegistry(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "shared-production-secret")
	shared := secret.NewRegistry()
	builder := newRuntimeBuilderForTest(t, nil)
	builder.secrets = shared
	set, err := builder.build(loadRuntimeConfig(t, "https://example.invalid/v1", 120), domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	producer, err := shared.AcquireExisting(set.RuntimeGenerationID)
	if err != nil {
		t.Fatalf("runtime generation not registered in shared registry: %v", err)
	}
	defer func() { _ = producer.Close() }()
	if got := producer.String("c2hhcmVkLXByb2R1Y3Rpb24tc2VjcmV0"); got != "[REDACTED]" {
		t.Fatalf("shared generation redaction=%q", got)
	}
}

func TestRuntimeSetBindsApproverToItsRunner(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "primary-secret")
	builder := newRuntimeBuilderForTest(t, nil)
	set, err := builder.build(loadRuntimeConfig(t, "https://example.invalid/v1", 120), domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	_, ok := set.Runtime.(*agent.OrchestratedRunner)
	if !ok {
		t.Fatalf("runtime=%T", set.Runtime)
	}
	approver := &agentfixture.Approver{}
	set.BindApprover(approver)
}

func TestRuntimeBuilderFallsBackToRootWhenCurrentModelWasRemoved(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "primary-secret")
	cfg := loadRuntimeConfig(t, "https://example.invalid/v1", 120)
	builder := newRuntimeBuilderForTest(t, nil)
	set, err := builder.build(cfg, domain.ModelSelection{Profile: "removed", Model: "gone"})
	if err != nil {
		t.Fatal(err)
	}
	if set.DefaultSelection != (domain.ModelSelection{Profile: "primary", Model: "a"}) {
		t.Fatalf("default selection=%+v", set.DefaultSelection)
	}
}

func asTypedConfiguration(err error, target **domain.TypedError) bool {
	return errors.As(err, target) && (*target).Kind == domain.ErrorConfigurationInvalid
}

func loadRuntimeConfig(t *testing.T, baseURL string, timeout int) config.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.jsonc")
	body := fmt.Sprintf(`{
  "model": "primary/a",
  "provider": {
    "primary": {
      "options": {"baseURL": %q, "apiKeyEnv": "PRIMARY_KEY"},
      "models": {"b": {}, "a": {}}
    },
    "secondary": {
      "options": {"baseURL": %q, "apiKeyEnv": "SECONDARY_KEY"},
      "models": {"c": {}}
    }
  },
  "limits": {"maxToolCalls": 7, "shellTimeoutSeconds": %d}
}`, baseURL, baseURL, timeout)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.LoadOptions{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func newRuntimeBuilderForTest(t *testing.T, client *http.Client) runtimeBuilder {
	t.Helper()
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store := jsonl.New(t.TempDir(), jsonl.Options{Sanitize: func(value any) (json.RawMessage, error) { return json.Marshal(value) }})
	session, err := store.Create(t.Context(), workspace, domain.ModeAsk, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	return runtimeBuilder{
		configPath:    "/home/user/.config/yordam/config.jsonc",
		cli:           cli.Options{MaxToolCalls: 32, ShellTimeout: 120 * time.Second},
		workspace:     workspace,
		store:         store,
		policy:        newPolicyBinding(permission.NewSession(domain.ModeAsk)),
		activeSession: &sessionBinding{id: session.ID},
		runtimeEvents: make(chan agent.RuntimeEvent, 64),
		httpClient:    client,
	}
}
