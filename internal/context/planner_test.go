package context_test

import (
	stdcontext "context"
	"encoding/json"
	"strings"
	"testing"

	contextplanner "github.com/muratmirgun/yordam/internal/context"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/tooling"
	subagenttool "github.com/muratmirgun/yordam/internal/tools/subagent"
)

func TestContextPlanRecordsProvenanceBudgetAndDigest(t *testing.T) {
	model := contextModel(64)
	system := source(t, "system", strings.Repeat("s", 16), "configured")
	request := contextplanner.Request{
		Session: "session", TaskID: "task", OutcomeContractID: "contract", OutcomeContractVersion: 1,
		SystemInstructions: []protocol.ContentSource{system}, Model: model, OutputReserve: 56,
	}
	planner := contextplanner.NewPlanner("tools-r1", nil)
	plan, err := planner.Plan(stdcontext.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Body.Sources) != 1 || plan.Body.Sources[0].ID != "system" || len(plan.Body.Excluded) != 0 {
		t.Fatalf("plan=%#v", plan)
	}
	if plan.Body.EstimatedInputTokens.State != protocol.ValueKnown || plan.Body.EstimatedInputTokens.Provenance == "" || plan.Body.ContextWindow != model.ContextWindow || plan.Body.ToolExposureRevision != "tools-r1" {
		t.Fatalf("plan=%#v", plan)
	}
	if plan.Digest.Validate() != nil {
		t.Fatalf("digest=%v", plan.Digest)
	}
	plan.Body.Sources[0].Content[0].Text = "mutated"
	again, err := planner.Plan(stdcontext.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if again.Body.Sources[0].Content[0].Text != strings.Repeat("s", 16) {
		t.Fatal("planner retained a mutable result alias")
	}
}

func TestChildPlannerBindsDerivedExposureAndRejectsCanonicalSubagent(t *testing.T) {
	parentExposure := protocol.ToolExposure{
		CatalogRevision: "parent-tools", Tools: []protocol.ExposedTool{{Alias: "read", Identity: protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "read"}, Description: "Read", InputSchema: []byte(`{"type":"object"}`)}},
		Aliases: []protocol.ToolAliasBinding{{Alias: "read", Identity: protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "read"}, SourceRevision: "builtin-v1", DescriptorDigest: contextDigest("a")}},
	}
	childExposure := protocol.ToolExposure{
		CatalogRevision: "forged-child-revision", Tools: protocol.DeepCopy(parentExposure.Tools),
		Aliases: protocol.DeepCopy(parentExposure.Aliases),
	}
	if _, err := contextplanner.NewChildPlanner(parentExposure, childExposure, nil); err == nil {
		t.Fatal("child planner accepted forged child exposure revision")
	}
	derived, err := tooling.DerivedExposureRevision(parentExposure, childExposure.Tools, childExposure.Aliases)
	if err != nil {
		t.Fatal(err)
	}
	childExposure.CatalogRevision = derived
	planner, err := contextplanner.NewChildPlanner(parentExposure, childExposure, nil)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := planner.Plan(stdcontext.Background(), contextplanner.Request{Session: "child", TaskID: "task", OutcomeContractID: "contract", OutcomeContractVersion: 1, Model: contextModel(1024), OutputReserve: 64})
	if err != nil || plan.Body.ToolExposureRevision != childExposure.CatalogRevision {
		t.Fatalf("plan=%#v err=%v", plan, err)
	}
	blocked := protocol.DeepCopy(childExposure)
	descriptor := subagenttool.BuiltinDescriptor()
	parentExposure.Tools = append(parentExposure.Tools, protocol.ExposedTool{Alias: "subagent", Identity: descriptor.Body.Identity, Description: descriptor.Body.Description, InputSchema: descriptor.Body.InputSchema})
	parentExposure.Aliases = append(parentExposure.Aliases, protocol.ToolAliasBinding{Alias: "subagent", Identity: descriptor.Body.Identity, SourceRevision: descriptor.Body.SourceRevision, DescriptorDigest: descriptor.DescriptorDigest})
	blocked.Tools = append(blocked.Tools, protocol.ExposedTool{Alias: "renamed", Identity: descriptor.Body.Identity, Description: descriptor.Body.Description, InputSchema: descriptor.Body.InputSchema})
	blocked.Aliases = append(blocked.Aliases, protocol.ToolAliasBinding{Alias: "renamed", Identity: descriptor.Body.Identity, SourceRevision: descriptor.Body.SourceRevision, DescriptorDigest: descriptor.DescriptorDigest})
	blocked.CatalogRevision, err = tooling.DerivedExposureRevision(parentExposure, blocked.Tools, blocked.Aliases)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := contextplanner.NewChildPlanner(parentExposure, blocked, nil); err == nil {
		t.Fatal("child planner accepted canonical subagent exposure")
	}
	parent, err := contextplanner.NewPlannerForExposure(blocked, nil)
	if err != nil {
		t.Fatal(err)
	}
	parentPlan, err := parent.Plan(stdcontext.Background(), contextplanner.Request{Session: "parent", TaskID: "task", OutcomeContractID: "contract", OutcomeContractVersion: 1, Model: contextModel(1024), OutputReserve: 64})
	if err != nil || parentPlan.Body.ToolExposureRevision != blocked.CatalogRevision {
		t.Fatalf("parent plan=%#v err=%v", parentPlan, err)
	}
}

func contextDigest(fill string) protocol.Digest {
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat(fill, 64)}
}

func TestContextPlanAdaptsEventTranscriptAndLegacyCompaction(t *testing.T) {
	events := []protocol.EventRecord{
		contextEvent("event-old", 1, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "old"}),
		{
			Envelope: protocol.EventEnvelope{EventID: "event-compact", Seq: 2, Kind: protocol.EventContextCompacted},
			Decoded:  &protocol.ContextCompactedV1{Revision: "legacy-r1"},
			Legacy:   &protocol.LegacySource{SchemaVersion: 1, EventID: "event-compact", Seq: 2, Kind: protocol.EventContextCompacted, Payload: json.RawMessage(`{"from_seq":0,"through_seq":1,"summary":"old summary"}`)},
		},
		contextEvent("event-new", 3, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "new"}),
		contextEvent("event-assistant", 4, protocol.EventAssistantMessage, &protocol.AssistantMessageV1{Blocks: []protocol.ContentBlock{{Kind: protocol.ContentText, Text: "answer"}}}),
	}
	plan, err := contextplanner.NewPlanner("tools-r1", nil).Plan(stdcontext.Background(), contextplanner.Request{
		Session: "session", TaskID: "task", OutcomeContractID: "contract", OutcomeContractVersion: 1,
		Events: events, Model: contextModel(1024), OutputReserve: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Body.Sources) != 3 || plan.Body.Sources[0].Content[0].Text != "old summary" || plan.Body.Sources[1].Content[0].Text != "new" || plan.Body.Sources[2].Content[0].Text != "answer" {
		t.Fatalf("sources=%#v", plan.Body.Sources)
	}
	if len(plan.Body.Excluded) != 1 || plan.Body.Excluded[0].ID != "event-old" || plan.Body.Excluded[0].Reason != contextplanner.ExcludedCompacted || plan.Body.CompactionRevision != "legacy-r1" {
		t.Fatalf("plan=%#v", plan)
	}
}

func TestContextPlanDecodesEventPayloadWhenProjectionIsUnavailable(t *testing.T) {
	record := protocol.EventRecord{Envelope: protocol.EventEnvelope{EventID: "event-raw", Seq: 1, Kind: protocol.EventUserMessage, Payload: json.RawMessage(`{"content":"from raw"}`)}}
	plan, err := contextplanner.NewPlanner("tools-r1", nil).Plan(stdcontext.Background(), contextplanner.Request{
		Session: "session", TaskID: "task", OutcomeContractID: "contract", OutcomeContractVersion: 1,
		Events: []protocol.EventRecord{record}, Model: contextModel(1024), OutputReserve: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Body.Sources) != 1 || plan.Body.Sources[0].Content[0].Text != "from raw" {
		t.Fatalf("sources=%#v", plan.Body.Sources)
	}
}

func TestContextPlanFailsClosedForMalformedNativeCompaction(t *testing.T) {
	events := []protocol.EventRecord{
		contextEvent("event-old", 1, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "old"}),
		contextEvent("event-compact", 2, protocol.EventContextCompacted, &protocol.ContextCompactedV1{Revision: "compact-r2", SummaryEvidenceID: "summary-evidence"}),
		contextEvent("event-new", 3, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "new"}),
	}
	plan, err := contextplanner.NewPlanner("tools-r1", nil).Plan(stdcontext.Background(), contextplanner.Request{
		Session: "session", TaskID: "task", OutcomeContractID: "contract", OutcomeContractVersion: 1,
		Events: events, Model: contextModel(1024), OutputReserve: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Body.CompactionRevision != "none" || len(plan.Body.Sources) != 2 || plan.Body.Sources[0].Content[0].Text != "old" || plan.Body.Sources[1].Content[0].Text != "new" {
		t.Fatalf("plan=%#v", plan)
	}
}

func TestContextPlanIgnoresMalformedLaterLegacyCompaction(t *testing.T) {
	events := []protocol.EventRecord{
		contextEvent("event-old", 1, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "old"}),
		{
			Envelope: protocol.EventEnvelope{EventID: "event-valid-compact", Seq: 2, Kind: protocol.EventContextCompacted},
			Decoded:  &protocol.ContextCompactedV1{Revision: "valid-r1"},
			Legacy:   &protocol.LegacySource{SchemaVersion: 1, EventID: "event-valid-compact", Seq: 2, Kind: protocol.EventContextCompacted, Payload: json.RawMessage(`{"from_seq":0,"through_seq":1,"summary":"valid summary"}`)},
		},
		contextEvent("event-new", 3, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "new"}),
		{
			Envelope: protocol.EventEnvelope{EventID: "event-invalid-compact", Seq: 4, Kind: protocol.EventContextCompacted},
			Decoded:  &protocol.ContextCompactedV1{Revision: "invalid-r2"},
			Legacy:   &protocol.LegacySource{SchemaVersion: 1, EventID: "event-invalid-compact", Seq: 4, Kind: protocol.EventContextCompacted, Payload: json.RawMessage(`{"from_seq":5,"through_seq":3,"summary":"invalid summary"}`)},
		},
		contextEvent("event-latest", 5, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "latest"}),
	}
	plan, err := contextplanner.NewPlanner("tools-r1", nil).Plan(stdcontext.Background(), contextplanner.Request{
		Session: "session", TaskID: "task", OutcomeContractID: "contract", OutcomeContractVersion: 1,
		Events: events, Model: contextModel(1024), OutputReserve: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Body.CompactionRevision != "valid-r1" || len(plan.Body.Sources) != 3 || plan.Body.Sources[0].Content[0].Text != "valid summary" || plan.Body.Sources[1].Content[0].Text != "new" || plan.Body.Sources[2].Content[0].Text != "latest" {
		t.Fatalf("plan=%#v", plan)
	}
}

func contextEvent(id string, sequence uint64, kind string, decoded any) protocol.EventRecord {
	return protocol.EventRecord{Envelope: protocol.EventEnvelope{EventID: protocol.EventID(id), Seq: sequence, Kind: kind}, Decoded: decoded}
}

func TestContextPlanExcludesSourcesBeyondKnownBudget(t *testing.T) {
	request := contextplanner.Request{
		Session: "session", TaskID: "task", OutcomeContractID: "contract", OutcomeContractVersion: 1,
		SystemInstructions: []protocol.ContentSource{source(t, "first", strings.Repeat("a", 16), "configured"), source(t, "second", strings.Repeat("b", 20), "configured")},
		Model:              contextModel(12), OutputReserve: 6,
	}
	plan, err := contextplanner.NewPlanner("tools-r1", nil).Plan(stdcontext.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Body.Sources) != 1 || len(plan.Body.Excluded) != 1 || plan.Body.Excluded[0].ID != "second" || plan.Body.Excluded[0].Reason != contextplanner.ExcludedBudget {
		t.Fatalf("plan=%#v", plan)
	}
}

func contextModel(window int64) protocol.ModelDescriptor {
	return protocol.ModelDescriptor{
		ProviderID: "openai", ModelID: "model", AdapterKind: "openai_compatible", DisplayName: "Model",
		ContextWindow: protocol.ValueInt64{State: protocol.ValueKnown, Value: window, Provenance: "catalog"},
		MaximumOutput: protocol.ValueInt64{State: protocol.ValueKnown, Value: window, Provenance: "catalog"},
		Capabilities:  []protocol.CapabilityFact{}, UsageCategories: []string{}, Pricing: []protocol.PricingFact{},
		CredentialBindingRef: "credential", SourceRevision: "rev", RuntimeGenerationID: "generation",
	}
}

func source(t *testing.T, id, text, provenance string) protocol.ContentSource {
	t.Helper()
	content := []protocol.ContentBlock{{Kind: protocol.ContentText, Text: text}}
	digest := protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("a", 64)}
	// The planner verifies and canonicalizes source digests rather than trusting callers.
	return protocol.ContentSource{ID: id, Kind: "instruction", Scope: "session", Provenance: provenance, Digest: digest, Content: content}
}
