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

func TestFoundationToolMessageDescriptorRoundTripAndEnvelope(t *testing.T) {
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	payload := protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{{CallID: "call-1", Status: "succeeded", Text: "ok"}}}
	event := protocol.EventEnvelope{
		JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session",
		TurnID: "turn", ActivityID: "activity", Kind: protocol.EventToolMessage,
	}
	record, err := registry.Decode(envelopeFor(t, event, payload))
	if err != nil {
		t.Fatalf("valid tool message rejected: %v", err)
	}
	if _, ok := record.Decoded.(*protocol.ToolMessageV1); !ok {
		t.Fatalf("decoded payload type = %T, want *protocol.ToolMessageV1", record.Decoded)
	}

	event.JournalKind = protocol.JournalWorkspaceControl
	event.JournalID = "workspace"
	event.SessionID = ""
	if _, err := registry.Decode(envelopeFor(t, event, payload)); err == nil {
		t.Fatal("tool message outside a session journal accepted")
	}
}

func TestFoundationSubagentDescriptorsBindSessionsAndTerminalCursor(t *testing.T) {
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	manifest := protocol.SubagentManifestV1{AttemptID: "attempt", ParentSessionID: "parent", ParentCursor: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "parent", CommitSeq: 2, TransactionID: "parent-transaction"}, ChildSessionID: "child", ChildTaskID: "task", ChildTurnID: "turn", RuntimeGenerationID: "generation", SkillCatalogRevision: "skills", MaxToolCalls: 1, Deadline: time.Unix(10, 0).UTC()}
	request := protocol.SubagentRequestedV1{Call: protocol.SubagentCallV1{Task: "task"}, Manifest: manifest}
	parent := protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "parent", SessionID: "parent", RuntimeGenerationID: "generation", TaskID: "parent-task", TurnID: "parent-turn", Kind: protocol.EventSubagentRequested, Seq: 3, Time: time.Unix(3, 0).UTC(), TransactionID: "parent-request"}
	if _, err := registry.Decode(envelopeFor(t, parent, request)); err != nil {
		t.Fatalf("valid parent request rejected: %v", err)
	}
	parent.SessionID = "other"
	parent.JournalID = "other"
	if _, err := registry.Decode(envelopeFor(t, parent, request)); err == nil {
		t.Fatal("request in non-parent session accepted")
	}

	unknown := protocol.UsageValue{State: protocol.UsageUnknown}
	receipt := protocol.SubagentReceiptV1{Status: "succeeded", Summary: "done", Manifest: manifest, TerminalCursor: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child", CommitSeq: 7, TransactionID: "child-terminal"}, ChangedFiles: []string{}, CommandsAndTests: []string{}, Usage: protocol.ModelUsage{Input: unknown, Output: unknown, Cached: unknown, CacheWrite: unknown, Reasoning: unknown}, EvidenceIDs: []protocol.EvidenceID{}, UnknownEffects: []protocol.ActivityID{}}
	child := protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "child", SessionID: "child", RuntimeGenerationID: "generation", TaskID: "task", TurnID: "turn", Kind: protocol.EventSubagentReceipt, Seq: 7, Time: time.Unix(7, 0).UTC(), TransactionID: "child-terminal"}
	if _, err := registry.Decode(envelopeFor(t, child, receipt)); err != nil {
		t.Fatalf("valid child receipt rejected: %v", err)
	}
	child.Seq++
	if _, err := registry.Decode(envelopeFor(t, child, receipt)); err == nil {
		t.Fatal("receipt with a nonterminal predicted cursor accepted")
	}

	digest, err := canonicaljson.Digest(receipt)
	if err != nil {
		t.Fatal(err)
	}
	attachment := protocol.SubagentResultAttachedV1{AttemptID: manifest.AttemptID, ChildSessionID: manifest.ChildSessionID, TerminalCursor: receipt.TerminalCursor, ReceiptDigest: digest, ReceiptEvidenceID: "receipt-evidence"}
	parent = protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "parent", SessionID: "parent", RuntimeGenerationID: "generation", TaskID: "parent-task", TurnID: "parent-turn", Kind: protocol.EventSubagentResultAttached, Seq: 5, Time: time.Unix(5, 0).UTC(), TransactionID: "parent-attachment"}
	if _, err := registry.Decode(envelopeFor(t, parent, attachment)); err != nil {
		t.Fatalf("valid attachment rejected: %v", err)
	}
}

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

	for name, mutate := range map[string]func(map[string]any){
		"limits": func(limits map[string]any) { limits["extra"] = true },
		"subagents": func(limits map[string]any) {
			limits["subagents"].(map[string]any)["extra"] = true
		},
	} {
		t.Run("unknown "+name, func(t *testing.T) {
			raw, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			var payload map[string]any
			if err := json.Unmarshal(raw, &payload); err != nil {
				t.Fatal(err)
			}
			body := payload["body"].(map[string]any)
			mutate(body["limits"].(map[string]any))
			digest, err := canonicaljson.Digest(body)
			if err != nil {
				t.Fatal(err)
			}
			payload["digest"] = map[string]any{"algorithm": digest.Algorithm, "value": digest.Value}
			rawPayload, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			envelope, err := json.Marshal(protocol.EventEnvelope{SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1, JournalKind: protocol.JournalWorkspaceControl, JournalID: "workspace", EventID: "event", Seq: 1, Time: time.Unix(1, 0).UTC(), Kind: protocol.EventRuntimeGenerationActivated, RuntimeGenerationID: manifest.ID, TransactionID: "transaction", Payload: rawPayload})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := registry.Decode(envelope); err == nil || !strings.Contains(err.Error(), "unknown field") {
				t.Fatalf("canonically redigested unknown runtime limit field error=%v", err)
			}
		})
	}
}

func TestFoundationRegistryPreservesLegacyRuntimeManifestWithoutCompactionOrSubagents(t *testing.T) {
	type legacyRuntimeLimits struct {
		MaxToolCalls             int   `json:"max_tool_calls"`
		ShellTimeoutNanos        int64 `json:"shell_timeout_nanos"`
		ApplicationQueueCapacity int   `json:"application_queue_capacity"`
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
		Limits: legacyRuntimeLimits{MaxToolCalls: 1, ShellTimeoutNanos: 1, ApplicationQueueCapacity: 1},
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
