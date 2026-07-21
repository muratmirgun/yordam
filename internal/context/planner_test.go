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

func TestContextPlanDoesNotDuplicateDualEncodedToolIntents(t *testing.T) {
	intent := protocol.ToolUseBlock{CallID: "load-skill", Alias: "skill", Arguments: json.RawMessage(`{"name":"go-development"}`)}
	event := contextEvent("assistant-tool", 2, protocol.EventAssistantMessage, &protocol.AssistantMessageV1{
		Blocks:      []protocol.ContentBlock{{Kind: protocol.ContentToolUse, ToolUse: &intent}},
		ToolIntents: []protocol.ToolUseBlock{intent},
	})
	plan, err := contextplanner.NewPlanner("tools-r1", nil).Plan(stdcontext.Background(), contextplanner.Request{
		Session: "session", TaskID: "task", OutcomeContractID: "contract", OutcomeContractVersion: 1,
		Events: []protocol.EventRecord{event, contextEvent("tool-result", 3, protocol.EventToolMessage, &protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{{CallID: intent.CallID, Status: "succeeded", Text: "ok"}}})}, Model: contextModel(1024), OutputReserve: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Body.Sources) != 2 || len(plan.Body.Sources[0].Content) != 1 || plan.Body.Sources[0].Content[0].Kind != protocol.ContentToolUse || plan.Body.Sources[0].Content[0].ToolUse == nil || plan.Body.Sources[0].Content[0].ToolUse.CallID != intent.CallID {
		t.Fatalf("dual-encoded tool intent was not projected exactly once: %+v", plan.Body.Sources)
	}
}

func TestContextPlanKeepsLegacyToolIntentsOnly(t *testing.T) {
	intent := protocol.ToolUseBlock{CallID: "legacy-call", Alias: "read", Arguments: json.RawMessage(`{"path":"README.md"}`)}
	event := contextEvent("legacy-assistant-tool", 2, protocol.EventAssistantMessage, &protocol.AssistantMessageV1{ToolIntents: []protocol.ToolUseBlock{intent}})
	plan, err := contextplanner.NewPlanner("tools-r1", nil).Plan(stdcontext.Background(), contextplanner.Request{
		Session: "session", TaskID: "task", OutcomeContractID: "contract", OutcomeContractVersion: 1,
		Events: []protocol.EventRecord{event, contextEvent("tool-result", 3, protocol.EventToolMessage, &protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{{CallID: intent.CallID, Status: "succeeded", Text: "ok"}}})}, Model: contextModel(1024), OutputReserve: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Body.Sources) != 2 || len(plan.Body.Sources[0].Content) != 1 || plan.Body.Sources[0].Content[0].ToolUse == nil || plan.Body.Sources[0].Content[0].ToolUse.CallID != intent.CallID || plan.Body.Sources[0].Content[0].ToolUse.Alias != intent.Alias || string(plan.Body.Sources[0].Content[0].ToolUse.Arguments) != string(intent.Arguments) {
		t.Fatalf("legacy tool intent was not preserved exactly once: %+v", plan.Body.Sources)
	}
}

func TestContextPlanRejectsConflictingDualEncodedToolIntent(t *testing.T) {
	modern := protocol.ToolUseBlock{CallID: "same-call", Alias: "read", Arguments: json.RawMessage(`{"path":"README.md"}`)}
	conflict := protocol.ToolUseBlock{CallID: "same-call", Alias: "read", Arguments: json.RawMessage(`{"path":"SECURITY.md"}`)}
	event := contextEvent("conflicting-assistant-tool", 2, protocol.EventAssistantMessage, &protocol.AssistantMessageV1{
		Blocks:      []protocol.ContentBlock{{Kind: protocol.ContentToolUse, ToolUse: &modern}},
		ToolIntents: []protocol.ToolUseBlock{conflict},
	})
	_, err := contextplanner.NewPlanner("tools-r1", nil).Plan(stdcontext.Background(), contextplanner.Request{
		Session: "session", TaskID: "task", OutcomeContractID: "contract", OutcomeContractVersion: 1,
		Events: []protocol.EventRecord{event}, Model: contextModel(1024), OutputReserve: 64,
	})
	if err == nil || !strings.Contains(err.Error(), "conflicting tool intent") {
		t.Fatalf("conflicting dual encoding error=%v", err)
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

func TestContextPlanProjectsCanonicalToolResultsAfterAssistantToolUse(t *testing.T) {
	intent := protocol.ToolUseBlock{CallID: "call-a", Alias: "read", Arguments: json.RawMessage(`{"path":"README.md"}`)}
	result := protocol.ToolResultBlock{CallID: intent.CallID, Status: "succeeded", Text: "contents"}
	events := []protocol.EventRecord{
		contextEvent("assistant-tool", 1, protocol.EventAssistantMessage, &protocol.AssistantMessageV1{Blocks: []protocol.ContentBlock{{Kind: protocol.ContentToolUse, ToolUse: &intent}}}),
		contextEvent("tool-result", 2, protocol.EventToolMessage, &protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{result}}),
	}
	plan, err := contextplanner.NewPlanner("tools-r1", nil).Plan(stdcontext.Background(), contextRequest(events))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Body.Sources) != 2 || plan.Body.Sources[0].Kind != "assistant_message" || plan.Body.Sources[1].Kind != "tool_message" {
		t.Fatalf("sources=%#v", plan.Body.Sources)
	}
	if plan.Body.Sources[0].Content[0].ToolUse == nil || plan.Body.Sources[0].Content[0].ToolUse.CallID != intent.CallID || plan.Body.Sources[1].Content[0].ToolResult == nil || plan.Body.Sources[1].Content[0].ToolResult.CallID != intent.CallID {
		t.Fatalf("sources=%#v", plan.Body.Sources)
	}
}

func TestContextPlanSynthesizesMissingResultForTerminalHistoricalTurn(t *testing.T) {
	intent := protocol.ToolUseBlock{CallID: "historical-call", Alias: "read", Arguments: json.RawMessage(`{"path":"README.md"}`)}
	events := []protocol.EventRecord{
		contextTurnEvent("assistant-event", 1, protocol.EventAssistantMessage, "turn-a", &protocol.AssistantMessageV1{Blocks: []protocol.ContentBlock{{Kind: protocol.ContentToolUse, ToolUse: &intent}}}),
		contextTurnEvent("turn-terminal", 2, protocol.EventTurnCompleted, "turn-a", &protocol.TurnTerminalV1{Status: "completed", Reason: "done"}),
	}
	plan, err := contextplanner.NewPlanner("tools-r1", nil).Plan(stdcontext.Background(), contextRequest(events))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Body.Sources) != 2 {
		t.Fatalf("sources=%#v", plan.Body.Sources)
	}
	tool := plan.Body.Sources[1]
	if tool.ID != "assistant-event:legacy-tool-result:historical-call" || tool.Kind != "tool_message" || tool.Provenance != "legacy_tool_result" || len(tool.Content) != 1 || tool.Content[0].ToolResult == nil || tool.Content[0].ToolResult.CallID != intent.CallID || tool.Content[0].ToolResult.Status != "uncertain" || tool.Content[0].ToolResult.Text != "historical tool result unavailable" {
		t.Fatalf("tool source=%#v", tool)
	}
}

func TestContextPlanHistoricalCompatibilityExchangeFullyCompactedYieldsSummaryOnly(t *testing.T) {
	intent := protocol.ToolUseBlock{CallID: "historical-call", Alias: "read", Arguments: json.RawMessage(`{"path":"README.md"}`)}
	resolver := &fakeSummaryResolver{sources: map[protocol.EvidenceID]protocol.ContentSource{
		"summary": summarySource(t, "summary", "historical exchange summary"),
	}}
	events := []protocol.EventRecord{
		contextTurnEvent("assistant-event", 1, protocol.EventAssistantMessage, "turn-a", &protocol.AssistantMessageV1{Blocks: []protocol.ContentBlock{{Kind: protocol.ContentToolUse, ToolUse: &intent}}}),
		contextTurnEvent("turn-terminal", 2, protocol.EventTurnCompleted, "turn-a", &protocol.TurnTerminalV1{Status: "completed", Reason: "historical"}),
		nativeCompactionEvent("compact", 3, "session", 1, 2, "summary", "native-r1"),
	}

	plan, err := contextplanner.NewPlanner("tools-r1", resolver).Plan(stdcontext.Background(), contextRequest(events))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Body.Sources) != 1 || plan.Body.Sources[0].ID != "summary" || plan.Body.Sources[0].Kind != "compaction_summary" {
		t.Fatalf("sources=%#v", plan.Body.Sources)
	}
	if len(plan.Body.Excluded) != 2 || plan.Body.Excluded[0].ID != "assistant-event" || plan.Body.Excluded[1].ID != "assistant-event:legacy-tool-result:historical-call" {
		t.Fatalf("excluded=%#v", plan.Body.Excluded)
	}
}

func TestContextPlanHistoricalCompatibilityExchangeStaysAdjacentBeforeSummary(t *testing.T) {
	intent := protocol.ToolUseBlock{CallID: "historical-call", Alias: "read", Arguments: json.RawMessage(`{"path":"README.md"}`)}
	resolver := &fakeSummaryResolver{sources: map[protocol.EvidenceID]protocol.ContentSource{
		"summary": summarySource(t, "summary", "older history summary"),
	}}
	events := []protocol.EventRecord{
		contextEvent("old-user", 1, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "old"}),
		contextTurnEvent("assistant-event", 2, protocol.EventAssistantMessage, "turn-a", &protocol.AssistantMessageV1{Blocks: []protocol.ContentBlock{{Kind: protocol.ContentToolUse, ToolUse: &intent}}}),
		nativeCompactionEvent("compact", 3, "session", 1, 1, "summary", "native-r1"),
		contextTurnEvent("turn-terminal", 4, protocol.EventTurnCompleted, "turn-a", &protocol.TurnTerminalV1{Status: "completed", Reason: "historical"}),
	}

	plan, err := contextplanner.NewPlanner("tools-r1", resolver).Plan(stdcontext.Background(), contextRequest(events))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Body.Sources) != 3 || plan.Body.Sources[0].ID != "assistant-event" || plan.Body.Sources[1].ID != "assistant-event:legacy-tool-result:historical-call" || plan.Body.Sources[2].ID != "summary" {
		t.Fatalf("sources=%#v", plan.Body.Sources)
	}
	assistant, result := plan.Body.Sources[0], plan.Body.Sources[1]
	if assistant.Kind != "assistant_message" || result.Kind != "tool_message" || assistant.Content[0].ToolUse == nil || result.Content[0].ToolResult == nil || assistant.Content[0].ToolUse.CallID != result.Content[0].ToolResult.CallID {
		t.Fatalf("provider transcript order is invalid: assistant=%#v result=%#v", assistant, result)
	}
	if len(plan.Body.Excluded) != 1 || plan.Body.Excluded[0].ID != "old-user" {
		t.Fatalf("excluded=%#v", plan.Body.Excluded)
	}
}

func TestContextPlanRejectsHistoricalResultWhenTerminalPrecedesAssistant(t *testing.T) {
	intent := protocol.ToolUseBlock{CallID: "historical-call", Alias: "read", Arguments: json.RawMessage(`{"path":"README.md"}`)}
	events := []protocol.EventRecord{
		contextTurnEvent("turn-terminal", 1, protocol.EventTurnCompleted, "turn-a", &protocol.TurnTerminalV1{Status: "completed", Reason: "done"}),
		contextTurnEvent("assistant-event", 2, protocol.EventAssistantMessage, "turn-a", &protocol.AssistantMessageV1{Blocks: []protocol.ContentBlock{{Kind: protocol.ContentToolUse, ToolUse: &intent}}}),
	}
	_, err := contextplanner.NewPlanner("tools-r1", nil).Plan(stdcontext.Background(), contextRequest(events))
	if err == nil || !strings.Contains(err.Error(), intent.CallID) || !strings.Contains(err.Error(), "unresolved tool call") {
		t.Fatalf("error=%v", err)
	}
}

func TestContextPlanRejectsMissingResultForNonTerminalTurn(t *testing.T) {
	intent := protocol.ToolUseBlock{CallID: "unresolved-call", Alias: "read", Arguments: json.RawMessage(`{"path":"README.md"}`)}
	events := []protocol.EventRecord{
		contextTurnEvent("assistant-event", 1, protocol.EventAssistantMessage, "turn-a", &protocol.AssistantMessageV1{Blocks: []protocol.ContentBlock{{Kind: protocol.ContentToolUse, ToolUse: &intent}}}),
	}
	_, err := contextplanner.NewPlanner("tools-r1", nil).Plan(stdcontext.Background(), contextRequest(events))
	if err == nil || !strings.Contains(err.Error(), intent.CallID) || !strings.Contains(err.Error(), "unresolved tool call") {
		t.Fatalf("error=%v", err)
	}
}

func TestContextPlanRejectsMalformedToolResultExchanges(t *testing.T) {
	intent := protocol.ToolUseBlock{CallID: "call-a", Alias: "read", Arguments: json.RawMessage(`{"path":"README.md"}`)}
	result := protocol.ToolResultBlock{CallID: intent.CallID, Status: "succeeded", Text: "contents"}
	unknown := protocol.ToolResultBlock{CallID: "unknown-call", Status: "succeeded", Text: "contents"}
	cases := []struct {
		name    string
		events  []protocol.EventRecord
		callID  string
		eventID string
	}{
		{name: "duplicate", callID: intent.CallID, eventID: "second-result", events: []protocol.EventRecord{
			contextEvent("assistant-event", 1, protocol.EventAssistantMessage, &protocol.AssistantMessageV1{Blocks: []protocol.ContentBlock{{Kind: protocol.ContentToolUse, ToolUse: &intent}}}),
			contextEvent("first-result", 2, protocol.EventToolMessage, &protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{result}}),
			contextEvent("second-result", 3, protocol.EventToolMessage, &protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{result}}),
		}},
		{name: "same payload duplicate", callID: intent.CallID, eventID: "duplicate-result", events: []protocol.EventRecord{
			contextEvent("assistant-event", 1, protocol.EventAssistantMessage, &protocol.AssistantMessageV1{Blocks: []protocol.ContentBlock{{Kind: protocol.ContentToolUse, ToolUse: &intent}}}),
			contextEvent("duplicate-result", 2, protocol.EventToolMessage, &protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{result, result}}),
		}},
		{name: "unknown", callID: unknown.CallID, eventID: "unknown-result", events: []protocol.EventRecord{
			contextEvent("assistant-event", 1, protocol.EventAssistantMessage, &protocol.AssistantMessageV1{Blocks: []protocol.ContentBlock{{Kind: protocol.ContentToolUse, ToolUse: &intent}}}),
			contextEvent("unknown-result", 2, protocol.EventToolMessage, &protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{unknown}}),
		}},
		{name: "late", callID: result.CallID, eventID: "late-result", events: []protocol.EventRecord{
			contextEvent("assistant-event", 1, protocol.EventAssistantMessage, &protocol.AssistantMessageV1{Blocks: []protocol.ContentBlock{{Kind: protocol.ContentToolUse, ToolUse: &intent}}}),
			contextEvent("result", 2, protocol.EventToolMessage, &protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{result}}),
			contextEvent("user-event", 3, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "next"}),
			contextEvent("late-result", 4, protocol.EventToolMessage, &protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{result}}),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := contextplanner.NewPlanner("tools-r1", nil).Plan(stdcontext.Background(), contextRequest(tc.events))
			if err == nil || !strings.Contains(err.Error(), tc.callID) || !strings.Contains(err.Error(), tc.eventID) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestContextPlanOrdersTwoToolResultsByPayloadOrder(t *testing.T) {
	first := protocol.ToolUseBlock{CallID: "call-a", Alias: "read", Arguments: json.RawMessage(`{"path":"a.go"}`)}
	second := protocol.ToolUseBlock{CallID: "call-b", Alias: "read", Arguments: json.RawMessage(`{"path":"b.go"}`)}
	events := []protocol.EventRecord{
		contextEvent("assistant-event", 1, protocol.EventAssistantMessage, &protocol.AssistantMessageV1{Blocks: []protocol.ContentBlock{{Kind: protocol.ContentToolUse, ToolUse: &first}, {Kind: protocol.ContentToolUse, ToolUse: &second}}}),
		contextEvent("result-event", 2, protocol.EventToolMessage, &protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{{CallID: second.CallID, Status: "succeeded", Text: "b"}, {CallID: first.CallID, Status: "succeeded", Text: "a"}}}),
	}
	plan, err := contextplanner.NewPlanner("tools-r1", nil).Plan(stdcontext.Background(), contextRequest(events))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Body.Sources) != 3 || plan.Body.Sources[1].ID != "result-event:0" || plan.Body.Sources[1].Content[0].ToolResult.CallID != second.CallID || plan.Body.Sources[2].ID != "result-event:1" || plan.Body.Sources[2].Content[0].ToolResult.CallID != first.CallID {
		t.Fatalf("sources=%#v", plan.Body.Sources)
	}
}

func TestContextPlanDecodesToolMessagesFromEveryProjectionForm(t *testing.T) {
	intent := protocol.ToolUseBlock{CallID: "call-a", Alias: "read", Arguments: json.RawMessage(`{"path":"README.md"}`)}
	message := protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{{CallID: intent.CallID, Status: "succeeded", Text: "contents"}}}
	raw, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		event protocol.EventRecord
	}{
		{name: "pointer", event: contextEvent("tool-result", 2, protocol.EventToolMessage, &message)},
		{name: "value", event: contextEvent("tool-result", 2, protocol.EventToolMessage, message)},
		{name: "raw", event: protocol.EventRecord{Envelope: protocol.EventEnvelope{EventID: "tool-result", Seq: 2, Kind: protocol.EventToolMessage, Payload: raw}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assistant := contextEvent("assistant-event", 1, protocol.EventAssistantMessage, &protocol.AssistantMessageV1{Blocks: []protocol.ContentBlock{{Kind: protocol.ContentToolUse, ToolUse: &intent}}})
			plan, err := contextplanner.NewPlanner("tools-r1", nil).Plan(stdcontext.Background(), contextRequest([]protocol.EventRecord{assistant, tc.event}))
			if err != nil || len(plan.Body.Sources) != 2 || plan.Body.Sources[1].Content[0].ToolResult == nil || plan.Body.Sources[1].Content[0].ToolResult.CallID != intent.CallID {
				t.Fatalf("plan=%#v error=%v", plan, err)
			}
		})
	}
}

func contextRequest(events []protocol.EventRecord) contextplanner.Request {
	return contextplanner.Request{Session: "session", TaskID: "task", OutcomeContractID: "contract", OutcomeContractVersion: 1, Events: events, Model: contextModel(1024), OutputReserve: 64}
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

func contextTurnEvent(id string, sequence uint64, kind string, turnID protocol.TurnID, decoded any) protocol.EventRecord {
	event := contextEvent(id, sequence, kind, decoded)
	event.Envelope.TurnID = turnID
	return event
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

func TestContextPlanBudgetExcludesWholeToolExchange(t *testing.T) {
	intent := protocol.ToolUseBlock{CallID: "call-a", Alias: "read", Arguments: json.RawMessage(`{"path":"README.md"}`)}
	events := []protocol.EventRecord{
		contextEvent("assistant-tool", 1, protocol.EventAssistantMessage, &protocol.AssistantMessageV1{Blocks: []protocol.ContentBlock{{Kind: protocol.ContentToolUse, ToolUse: &intent}}}),
		contextEvent("tool-result", 2, protocol.EventToolMessage, &protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{{CallID: intent.CallID, Status: "succeeded", Text: strings.Repeat("result", 128)}}}),
	}
	request := contextRequest(events)
	request.Model = contextModel(64)
	request.OutputReserve = 0

	plan, err := contextplanner.NewPlanner("tools-r1", nil).Plan(stdcontext.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Body.Sources) != 0 {
		t.Fatalf("partial tool exchange included: %#v", plan.Body.Sources)
	}
	if len(plan.Body.Excluded) != 2 || plan.Body.Excluded[0].ID != "assistant-tool" || plan.Body.Excluded[1].ID != "tool-result" {
		t.Fatalf("excluded=%#v", plan.Body.Excluded)
	}
	for _, excluded := range plan.Body.Excluded {
		if excluded.Reason != contextplanner.ExcludedBudget {
			t.Fatalf("excluded=%#v", plan.Body.Excluded)
		}
	}
}

func TestContextPlanBudgetIncludesWholeToolExchangeInSourceOrder(t *testing.T) {
	first := protocol.ToolUseBlock{CallID: "call-a", Alias: "read", Arguments: json.RawMessage(`{"path":"a.go"}`)}
	second := protocol.ToolUseBlock{CallID: "call-b", Alias: "read", Arguments: json.RawMessage(`{"path":"b.go"}`)}
	events := []protocol.EventRecord{
		contextEvent("assistant-tool", 1, protocol.EventAssistantMessage, &protocol.AssistantMessageV1{Blocks: []protocol.ContentBlock{{Kind: protocol.ContentToolUse, ToolUse: &first}, {Kind: protocol.ContentToolUse, ToolUse: &second}}}),
		contextEvent("tool-results", 2, protocol.EventToolMessage, &protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{{CallID: second.CallID, Status: "succeeded", Text: "second"}, {CallID: first.CallID, Status: "succeeded", Text: "first"}}}),
	}
	request := contextRequest(events)
	request.Model = contextModel(1024)
	request.OutputReserve = 0

	plan, err := contextplanner.NewPlanner("tools-r1", nil).Plan(stdcontext.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Body.Excluded) != 0 || len(plan.Body.Sources) != 3 {
		t.Fatalf("plan=%#v", plan)
	}
	if plan.Body.Sources[0].ID != "assistant-tool" || plan.Body.Sources[1].ID != "tool-results:0" || plan.Body.Sources[2].ID != "tool-results:1" {
		t.Fatalf("sources=%#v", plan.Body.Sources)
	}
	if plan.Body.Sources[1].Content[0].ToolResult.CallID != second.CallID || plan.Body.Sources[2].Content[0].ToolResult.CallID != first.CallID {
		t.Fatalf("sources=%#v", plan.Body.Sources)
	}
}

func TestContextPlanRejectsMalformedGroupedToolHistory(t *testing.T) {
	assistant := func(id, callID string) protocol.ContentSource {
		intent := protocol.ToolUseBlock{CallID: callID, Alias: "read", Arguments: json.RawMessage(`{"path":"README.md"}`)}
		return transcriptSource(id, "assistant_message", []protocol.ContentBlock{{Kind: protocol.ContentToolUse, ToolUse: &intent}})
	}
	result := func(id, callID string) protocol.ContentSource {
		value := protocol.ToolResultBlock{CallID: callID, Status: "succeeded", Text: "result"}
		return transcriptSource(id, "tool_message", []protocol.ContentBlock{{Kind: protocol.ContentToolResult, ToolResult: &value}})
	}
	ordinary := transcriptSource("ordinary", "instruction", []protocol.ContentBlock{{Kind: protocol.ContentText, Text: "ordinary"}})
	tests := []struct {
		name    string
		sources []protocol.ContentSource
	}{
		{name: "standalone", sources: []protocol.ContentSource{result("standalone-result", "call-a")}},
		{name: "unknown", sources: []protocol.ContentSource{assistant("assistant", "call-a"), result("unknown-result", "call-b")}},
		{name: "duplicate", sources: []protocol.ContentSource{assistant("assistant", "call-a"), result("first-result", "call-a"), result("duplicate-result", "call-a")}},
		{name: "late", sources: []protocol.ContentSource{assistant("assistant", "call-a"), result("first-result", "call-a"), ordinary, result("late-result", "call-a")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := contextRequest(nil)
			request.SystemInstructions = test.sources
			if _, err := contextplanner.NewPlanner("tools-r1", nil).Plan(stdcontext.Background(), request); err == nil {
				t.Fatalf("accepted malformed grouped tool history: %#v", test.sources)
			}
		})
	}
}

func transcriptSource(id, kind string, blocks []protocol.ContentBlock) protocol.ContentSource {
	return protocol.ContentSource{
		ID: id, Kind: kind, Scope: "session", Provenance: "test", Content: blocks,
		Digest: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("a", 64)},
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
