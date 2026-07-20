package eventcodec_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/eventcodec"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestFoundationRegistryRejectsInvalidNativeCompactionPayloads(t *testing.T) {
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	valid := compactionPayload("session", 1, 2)
	for name, mutate := range map[string]func(*protocol.ContextCompactedV1, *protocol.EventEnvelope){
		"empty evidence":      func(p *protocol.ContextCompactedV1, _ *protocol.EventEnvelope) { p.SummaryEvidenceID = "" },
		"empty revision":      func(p *protocol.ContextCompactedV1, _ *protocol.EventEnvelope) { p.Revision = "" },
		"other session range": func(p *protocol.ContextCompactedV1, _ *protocol.EventEnvelope) { p.Through.JournalID = "other" },
		"zero cursor":         func(p *protocol.ContextCompactedV1, _ *protocol.EventEnvelope) { p.From.CommitSeq = 0 },
		"reversed range":      func(p *protocol.ContextCompactedV1, _ *protocol.EventEnvelope) { p.From.CommitSeq = 3 },
		"through at event":    func(_ *protocol.ContextCompactedV1, e *protocol.EventEnvelope) { e.Seq = 2 },
		"workspace cursor": func(p *protocol.ContextCompactedV1, _ *protocol.EventEnvelope) {
			p.From.JournalKind = protocol.JournalWorkspaceControl
		},
	} {
		t.Run(name, func(t *testing.T) {
			payload := valid
			event := compactionEnvelope(3)
			mutate(&payload, &event)
			if _, err := registry.Decode(envelopeFor(t, event, payload)); err == nil {
				t.Fatal("invalid compaction accepted")
			}
		})
	}

	oversized := compactionPayload("session", 1, 2)
	oversized.Revision = strings.Repeat("x", protocol.MaxStringBytes+1)
	if _, err := registry.Decode(envelopeFor(t, compactionEnvelope(3), oversized)); err == nil {
		t.Fatal("oversized compaction accepted")
	}
	if _, err := registry.Decode(envelopeFor(t, compactionEnvelope(3), valid)); err != nil {
		t.Fatalf("valid legacy-compatible compaction rejected: %v", err)
	}
}

func TestContextCompactionReferenceBindsSessionAndExactRange(t *testing.T) {
	payload := compactionPayload("session", 1, 2)
	reference := protocol.ContextCompactionReference{SessionID: "session", From: payload.From, Through: payload.Through, SummaryEvidenceID: "summary", Revision: "r1"}
	if err := reference.Validate(); err != nil {
		t.Fatal(err)
	}
	reference.Through.JournalID = "other"
	if err := reference.Validate(); err == nil {
		t.Fatal("cross-session compaction range accepted")
	}
}

func TestFoundationRegistryAcceptsProjectSkillTrustOnlyInWorkspaceControl(t *testing.T) {
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	payload := protocol.ProjectSkillTrustChangedV1{
		WorkspaceID: "workspace", CatalogDigest: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("a", 64)}, Decision: "allow",
	}
	control := protocol.EventEnvelope{JournalKind: protocol.JournalWorkspaceControl, JournalID: "workspace", EventID: "skill-trust", Seq: 1, Time: time.Unix(1, 0).UTC(), Kind: protocol.EventProjectSkillTrustChanged, TransactionID: "transaction"}
	if _, err := registry.Decode(envelopeFor(t, control, payload)); err != nil {
		t.Fatalf("valid workspace-control trust event rejected: %v", err)
	}
	wrongWorkspace := control
	wrongWorkspace.JournalID = "other-workspace"
	if _, err := registry.Decode(envelopeFor(t, wrongWorkspace, payload)); err == nil {
		t.Fatal("project skill trust accepted with a mismatched workspace-control journal ID")
	}
	session := control
	session.JournalKind, session.JournalID, session.SessionID = protocol.JournalSession, "session", "session"
	if _, err := registry.Decode(envelopeFor(t, session, payload)); err == nil {
		t.Fatal("project skill trust accepted outside workspace-control journal")
	}
	payload.Decision = "ask"
	record, err := registry.Decode(envelopeFor(t, control, payload))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Validate(record); err == nil {
		t.Fatal("unresolved skill trust policy accepted as durable decision")
	}
}

func TestFoundationRegistryRejectsDuplicateRuntimeSkillNamesAcrossSources(t *testing.T) {
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	global := protocol.SkillDescriptor{Identity: protocol.SkillIdentity{Name: "go-testing", Source: protocol.SkillSourceGlobal, CanonicalPath: "/global/go-testing/SKILL.md", ContentDigest: testDigest('a'), RuntimeGenerationID: "generation"}, Description: "global", State: protocol.SkillStateActive}
	project := protocol.SkillDescriptor{Identity: protocol.SkillIdentity{Name: "go-testing", Source: protocol.SkillSourceProject, CanonicalPath: "/project/go-testing/SKILL.md", WorkspaceID: "workspace", ContentDigest: testDigest('b'), RuntimeGenerationID: "generation"}, Description: "project", State: protocol.SkillStateActive}
	body := protocol.RuntimeGenerationBody{
		ProviderCatalogRevision: "providers", Models: []protocol.ModelDescriptor{}, ToolCatalogRevision: "tools", Tools: []protocol.ToolDescriptor{},
		SkillCatalogRevision: "skills", Skills: []protocol.SkillDescriptor{global, project}, InstructionRevision: "instructions", PolicyGeneration: "policy",
		ExecutionProfiles: []string{"restricted"}, Limits: protocol.RuntimeLimits{MaxToolCalls: 1, ShellTimeoutNanos: 1, ApplicationQueueCapacity: 1, Subagents: validSubagentLimits()},
	}
	digest, err := canonicaljson.Digest(body)
	if err != nil {
		t.Fatal(err)
	}
	manifest := protocol.RuntimeGenerationManifest{ID: "generation", Body: body, Digest: digest}
	record, err := registry.Decode(envelopeFor(t, protocol.EventEnvelope{JournalKind: protocol.JournalWorkspaceControl, JournalID: "workspace", RuntimeGenerationID: "generation", Kind: protocol.EventRuntimeGenerationActivated}, protocol.RuntimeGenerationActivatedV1{Manifest: manifest}))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Validate(record); err == nil {
		t.Fatal("runtime manifest accepted active skills with the same canonical name across sources")
	}
}

func TestFoundationRegistryRejectsRuntimeSkillShadowFromOtherGeneration(t *testing.T) {
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	shadow := protocol.SkillIdentity{Name: "go-testing", Source: protocol.SkillSourceGlobal, CanonicalPath: "/global/go-testing/SKILL.md", ContentDigest: testDigest('a'), RuntimeGenerationID: "other-generation"}
	project := protocol.SkillDescriptor{Identity: protocol.SkillIdentity{Name: "go-testing", Source: protocol.SkillSourceProject, CanonicalPath: "/project/go-testing/SKILL.md", WorkspaceID: "workspace", ContentDigest: testDigest('b'), RuntimeGenerationID: "generation"}, Description: "project", State: protocol.SkillStateActive, Shadows: &shadow}
	body := protocol.RuntimeGenerationBody{
		ProviderCatalogRevision: "providers", Models: []protocol.ModelDescriptor{}, ToolCatalogRevision: "tools", Tools: []protocol.ToolDescriptor{},
		SkillCatalogRevision: "skills", Skills: []protocol.SkillDescriptor{project}, InstructionRevision: "instructions", PolicyGeneration: "policy",
		ExecutionProfiles: []string{"restricted"}, Limits: protocol.RuntimeLimits{MaxToolCalls: 1, ShellTimeoutNanos: 1, ApplicationQueueCapacity: 1, Subagents: validSubagentLimits()},
	}
	digest, err := canonicaljson.Digest(body)
	if err != nil {
		t.Fatal(err)
	}
	manifest := protocol.RuntimeGenerationManifest{ID: "generation", Body: body, Digest: digest}
	record, err := registry.Decode(envelopeFor(t, protocol.EventEnvelope{JournalKind: protocol.JournalWorkspaceControl, JournalID: "workspace", RuntimeGenerationID: "generation", Kind: protocol.EventRuntimeGenerationActivated}, protocol.RuntimeGenerationActivatedV1{Manifest: manifest}))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Validate(record); err == nil {
		t.Fatal("runtime manifest accepted a shadow target from another generation")
	}
}

func TestFoundationRegistryRejectsInvalidSubagentRuntimeLimits(t *testing.T) {
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	manifest := protocol.RuntimeGenerationManifest{
		ID: "generation",
		Body: protocol.RuntimeGenerationBody{
			ProviderCatalogRevision: "providers", Models: []protocol.ModelDescriptor{}, ToolCatalogRevision: "tools", Tools: []protocol.ToolDescriptor{}, InstructionRevision: "instructions", PolicyGeneration: "policy",
			ExecutionProfiles: []string{"restricted"},
			Limits:            protocol.RuntimeLimits{MaxToolCalls: 1, ShellTimeoutNanos: 1, ApplicationQueueCapacity: 1, Subagents: protocol.SubagentLimits{Enabled: true, MaxPerTurn: 1, MaxToolCalls: 1, TimeoutNanos: int64(time.Second)}},
		},
	}
	manifest.Digest, err = canonicaljson.Digest(manifest.Body)
	if err != nil {
		t.Fatal(err)
	}
	valid, err := registry.Decode(envelopeFor(t, protocol.EventEnvelope{JournalKind: protocol.JournalWorkspaceControl, JournalID: "workspace", RuntimeGenerationID: manifest.ID, Kind: protocol.EventRuntimeGenerationActivated}, protocol.RuntimeGenerationActivatedV1{Manifest: manifest}))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Validate(valid); err != nil {
		t.Fatalf("valid runtime manifest rejected: %v", err)
	}

	for name, mutate := range map[string]func(*protocol.SubagentLimits){
		"max per turn":   func(limits *protocol.SubagentLimits) { limits.MaxPerTurn = 0 },
		"max tool calls": func(limits *protocol.SubagentLimits) { limits.MaxToolCalls = 65 },
		"one nanosecond": func(limits *protocol.SubagentLimits) { limits.TimeoutNanos = 1 },
		"above maximum":  func(limits *protocol.SubagentLimits) { limits.TimeoutNanos = int64(1800*time.Second) + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := protocol.DeepCopy(manifest)
			mutate(&candidate.Body.Limits.Subagents)
			candidate.Digest, err = canonicaljson.Digest(candidate.Body)
			if err != nil {
				t.Fatal(err)
			}
			record, err := registry.Decode(envelopeFor(t, protocol.EventEnvelope{JournalKind: protocol.JournalWorkspaceControl, JournalID: "workspace", RuntimeGenerationID: candidate.ID, Kind: protocol.EventRuntimeGenerationActivated}, protocol.RuntimeGenerationActivatedV1{Manifest: candidate}))
			if err != nil {
				t.Fatal(err)
			}
			if err := registry.Validate(record); err == nil {
				t.Fatal("canonically redigested invalid subagent limits accepted")
			}
		})
	}

	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var explicitZero map[string]any
	if err := json.Unmarshal(raw, &explicitZero); err != nil {
		t.Fatal(err)
	}
	limits := explicitZero["body"].(map[string]any)["limits"].(map[string]any)
	limits["subagents"] = map[string]any{"enabled": false, "max_per_turn": float64(0), "max_tool_calls": float64(0), "timeout_nanos": float64(0)}
	raw, err = json.Marshal(explicitZero)
	if err != nil {
		t.Fatal(err)
	}
	var candidate protocol.RuntimeGenerationManifest
	if err := json.Unmarshal(raw, &candidate); err != nil {
		t.Fatal(err)
	}
	candidate.Digest, err = canonicaljson.Digest(candidate.Body)
	if err != nil {
		t.Fatal(err)
	}
	record, err := registry.Decode(envelopeFor(t, protocol.EventEnvelope{JournalKind: protocol.JournalWorkspaceControl, JournalID: "workspace", RuntimeGenerationID: candidate.ID, Kind: protocol.EventRuntimeGenerationActivated}, protocol.RuntimeGenerationActivatedV1{Manifest: candidate}))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Validate(record); err == nil {
		t.Fatal("explicit zero subagent limits accepted")
	}
}

func TestFoundationRegistryPreservesLegacyRuntimeManifestWithoutSubagents(t *testing.T) {
	type legacyRuntimeLimits struct {
		MaxToolCalls             int                 `json:"max_tool_calls"`
		ShellTimeoutNanos        int64               `json:"shell_timeout_nanos"`
		ApplicationQueueCapacity int                 `json:"application_queue_capacity"`
		AutoCompact              bool                `json:"auto_compact"`
		CompactReserveTokens     protocol.ValueInt64 `json:"compact_reserve_tokens"`
	}
	type legacyRuntimeBody struct {
		ProviderCatalogRevision string                     `json:"provider_catalog_revision"`
		Models                  []protocol.ModelDescriptor `json:"models"`
		ToolCatalogRevision     string                     `json:"tool_catalog_revision"`
		Tools                   []protocol.ToolDescriptor  `json:"tools"`
		InstructionRevision     string                     `json:"instruction_revision"`
		PolicyGeneration        string                     `json:"policy_generation"`
		ExecutionProfiles       []string                   `json:"execution_profiles"`
		Limits                  legacyRuntimeLimits        `json:"limits"`
	}
	legacy := legacyRuntimeBody{
		ProviderCatalogRevision: "providers", Models: []protocol.ModelDescriptor{}, ToolCatalogRevision: "tools", Tools: []protocol.ToolDescriptor{},
		InstructionRevision: "instructions", PolicyGeneration: "policy", ExecutionProfiles: []string{"restricted"},
		Limits: legacyRuntimeLimits{MaxToolCalls: 1, ShellTimeoutNanos: 1, ApplicationQueueCapacity: 1, CompactReserveTokens: protocol.ValueInt64{State: protocol.ValueUnknown}},
	}
	legacyJSON, err := canonicaljson.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	legacyDigest, err := canonicaljson.Digest(legacy)
	if err != nil {
		t.Fatal(err)
	}
	var decoded protocol.RuntimeGenerationBody
	if err := json.Unmarshal(legacyJSON, &decoded); err != nil {
		t.Fatal(err)
	}
	replayedJSON, err := canonicaljson.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if string(replayedJSON) != string(legacyJSON) {
		t.Fatalf("legacy replay changed body:\n got %s\nwant %s", replayedJSON, legacyJSON)
	}
	manifest := protocol.RuntimeGenerationManifest{ID: "generation", Body: decoded, Digest: legacyDigest}
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	record, err := registry.Decode(envelopeFor(t, protocol.EventEnvelope{JournalKind: protocol.JournalWorkspaceControl, JournalID: "workspace", RuntimeGenerationID: manifest.ID, Kind: protocol.EventRuntimeGenerationActivated}, protocol.RuntimeGenerationActivatedV1{Manifest: manifest}))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Validate(record); err != nil {
		t.Fatalf("legacy runtime manifest rejected: %v", err)
	}
}

func compactionPayload(session protocol.SessionID, from, through uint64) protocol.ContextCompactedV1 {
	cursor := func(sequence uint64) protocol.CommittedCursor {
		return protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(session), CommitSeq: sequence, TransactionID: "transaction"}
	}
	return protocol.ContextCompactedV1{From: cursor(from), Through: cursor(through), SummaryEvidenceID: "summary", Revision: "r1"}
}

func compactionEnvelope(sequence uint64) protocol.EventEnvelope {
	return protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", EventID: "compact", Seq: sequence, Time: time.Unix(1, 0).UTC(), Kind: protocol.EventContextCompacted, TransactionID: "transaction"}
}
