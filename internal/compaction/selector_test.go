package compaction_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/compaction"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestSelectRetainsSafetyFactsAndBoundsInputDeterministically(t *testing.T) {
	t.Parallel()
	events := []protocol.EventRecord{
		selectionEvent(1, protocol.EventTaskCreated, &protocol.TaskCreatedV1{Goal: "ship compaction", OutcomeContractID: "outcome", ContractVersion: 1}),
		selectionEvent(2, protocol.EventOutcomeContractDeclared, &protocol.OutcomeContractDeclaredV1{OutcomeContractID: "outcome", Version: 1, Goal: "ship compaction", Source: "user", Frozen: true}),
		selectionEvent(3, protocol.EventAuthorizationRequested, &protocol.AuthorizationRequestedV1{Request: protocol.AuthorizationRequest{RequestID: "approval-1"}}),
		selectionEvent(4, protocol.EventAssistantMessage, &protocol.AssistantMessageV1{Blocks: []protocol.ContentBlock{{Kind: protocol.ContentToolResult, ToolResult: &protocol.ToolResultBlock{CallID: "call-1", Status: "failed", Text: strings.Repeat("large tool body ", 4_096), EvidenceIDs: []protocol.EvidenceID{"evidence-1"}}}}}),
		selectionEvent(5, protocol.EventContextCompacted, &protocol.ContextCompactedV1{From: selectionCursor(1), Through: selectionCursor(2), SummaryEvidenceID: "old-summary", Revision: "old-revision"}),
		selectionEvent(6, "child.receipt_recorded", json.RawMessage(`{"child_session_id":"child-1","receipt_id":"receipt-1"}`)),
		selectionEvent(7, "skill.loaded", json.RawMessage(`{"identity":"skill:go"}`)),
		selectionEvent(8, protocol.EventActivityFailed, &protocol.ActivityOutcomeV1{Status: "failed", Reason: "test failed", UnknownEffects: []protocol.SubjectRef{{Kind: "file", ID: "unknown.go"}}}),
		selectionEvent(9, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "continue safely"}),
		selectionEvent(10, protocol.EventAssistantMessage, &protocol.AssistantMessageV1{Blocks: []protocol.ContentBlock{{Kind: protocol.ContentText, Text: "recent reply"}}}),
	}

	selection, err := compaction.Select(events, selectionCursor(10), compaction.TriggerManual, 0)
	if err != nil {
		t.Fatal(err)
	}
	if selection.From != selectionCursor(3) || selection.Through != selectionCursor(6) {
		t.Fatalf("range=%+v through=%+v", selection.From, selection.Through)
	}
	if got, want := selection.SummarizedEventIDs, []protocol.EventID{"event-3", "event-4", "event-5", "event-6"}; !sameIDs(got, want) {
		t.Fatalf("summarized=%v want=%v", got, want)
	}
	if got, want := selection.RetainedEventIDs, []protocol.EventID{"event-1", "event-2", "event-3", "event-7", "event-8", "event-9", "event-10"}; !sameIDs(got, want) {
		t.Fatalf("retained=%v want=%v", got, want)
	}
	if selection.Through.CommitSeq > 10 || selection.SourceDigest.Validate() != nil || len(selection.Sources) == 0 {
		t.Fatalf("selection=%+v", selection)
	}
	joined := sourcesText(selection.Sources)
	if strings.Contains(joined, "large tool body") || !strings.Contains(joined, "evidence-1") || !strings.Contains(joined, "approval-1") || !strings.Contains(joined, "child-1") {
		t.Fatalf("normalized sources lost safety or leaked body: %q", joined)
	}
	if !strings.Contains(joined, "skill:go") || !strings.Contains(joined, "unknown.go") {
		t.Fatalf("normalized sources=%q", joined)
	}

	again, err := compaction.Select(events, selectionCursor(10), compaction.TriggerManual, 0)
	if err != nil || !reflect.DeepEqual(again, selection) {
		t.Fatalf("selection is not deterministic: again=%+v err=%v", again, err)
	}
}

func TestSelectRejectsNoSafeCommittedRange(t *testing.T) {
	t.Parallel()
	events := []protocol.EventRecord{
		selectionEvent(1, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "only recent"}),
		selectionEvent(2, protocol.EventAssistantMessage, &protocol.AssistantMessageV1{Blocks: []protocol.ContentBlock{{Kind: protocol.ContentText, Text: "only recent"}}}),
	}
	_, err := compaction.Select(events, selectionCursor(2), compaction.TriggerAutomatic, 1024)
	if !errors.Is(err, compaction.ErrNothingToCompact) {
		t.Fatalf("err=%v", err)
	}
}

func TestSelectRejectsEventsOutsideCommittedHead(t *testing.T) {
	t.Parallel()
	events := []protocol.EventRecord{
		selectionEvent(1, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "one"}),
		selectionEvent(3, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "gap"}),
	}
	_, err := compaction.Select(events, selectionCursor(2), compaction.TriggerManual, 1024)
	if err == nil {
		t.Fatal("accepted non-contiguous committed events")
	}
}

func TestSelectBoundsNormalizedSourcesWithAnExplicitLimit(t *testing.T) {
	t.Parallel()
	events := make([]protocol.EventRecord, 0, 15)
	for sequence := uint64(1); sequence <= 15; sequence++ {
		events = append(events, selectionEvent(sequence, protocol.EventUserMessage, &protocol.UserMessageV1{Content: strings.Repeat("x", 80)}))
	}
	selection, err := compaction.Select(events, selectionCursor(15), compaction.TriggerManual, 3_000)
	if err != nil {
		t.Fatal(err)
	}
	if selection.From.CommitSeq <= 1 || selection.Through.CommitSeq != 11 {
		t.Fatalf("selection=%+v", selection)
	}
	request, err := compaction.BuildSummaryRequest(selection, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(request.Messages[1].Blocks[0].Text); got > 3_000 {
		t.Fatalf("model-visible request bytes=%d", got)
	}
}

func TestSelectTracksTaskAndOutcomeFactsByIdentity(t *testing.T) {
	t.Parallel()
	events := []protocol.EventRecord{
		selectionTaskEvent(1, "task-a", protocol.EventTaskCreated, &protocol.TaskCreatedV1{Goal: "a", OutcomeContractID: "contract-a", ContractVersion: 1}),
		selectionTaskEvent(2, "task-b", protocol.EventTaskCreated, &protocol.TaskCreatedV1{Goal: "b", OutcomeContractID: "contract-b", ContractVersion: 1}),
		selectionTaskEvent(3, "task-a", protocol.EventTaskStatusChanged, &protocol.TaskStatusChangedV1{From: string(protocol.TaskRunning), To: string(protocol.TaskCompleted)}),
		selectionTaskEvent(4, "task-a", protocol.EventTaskStatusChanged, &protocol.TaskStatusChangedV1{From: string(protocol.TaskCompleted), To: string(protocol.TaskReopened)}),
		selectionTaskEvent(5, "task-a", protocol.EventOutcomeContractDeclared, &protocol.OutcomeContractDeclaredV1{OutcomeContractID: "contract-a", Version: 1, Goal: "a", Source: "user", Frozen: true}),
		selectionTaskEvent(6, "task-b", protocol.EventOutcomeContractDeclared, &protocol.OutcomeContractDeclaredV1{OutcomeContractID: "contract-b", Version: 1, Goal: "b", Source: "user", Frozen: true}),
		selectionTaskEvent(7, "task-a", protocol.EventOutcomeFinalAssessed, &protocol.OutcomeFinalAssessedV1{OutcomeContractID: "contract-a", ContractVersion: 1, Status: "passed"}),
		selectionTaskEvent(8, "task-b", protocol.EventUserMessage, &protocol.UserMessageV1{Content: "recent one"}),
		selectionTaskEvent(9, "task-b", protocol.EventUserMessage, &protocol.UserMessageV1{Content: "recent two"}),
		selectionTaskEvent(10, "task-b", protocol.EventUserMessage, &protocol.UserMessageV1{Content: "recent three"}),
	}
	selection, err := compaction.Select(events, selectionCursor(10), compaction.TriggerManual, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := selection.RetainedEventIDs, []protocol.EventID{"event-2", "event-4", "event-6", "event-7", "event-8", "event-9", "event-10"}; !sameIDs(got, want) {
		t.Fatalf("retained=%v want=%v", got, want)
	}
}

func TestSelectRejectsLimitThatCannotFitRequiredSafetyFacts(t *testing.T) {
	t.Parallel()
	events := []protocol.EventRecord{
		selectionTaskEvent(1, "task", protocol.EventTaskCreated, &protocol.TaskCreatedV1{Goal: strings.Repeat("required-goal ", 40), OutcomeContractID: "contract", ContractVersion: 1}),
		selectionTaskEvent(2, "task", protocol.EventUserMessage, &protocol.UserMessageV1{Content: "older"}),
		selectionTaskEvent(3, "task", protocol.EventUserMessage, &protocol.UserMessageV1{Content: "older"}),
		selectionTaskEvent(4, "task", protocol.EventUserMessage, &protocol.UserMessageV1{Content: "older"}),
		selectionTaskEvent(5, "task", protocol.EventUserMessage, &protocol.UserMessageV1{Content: strings.Repeat("recent ", 40)}),
		selectionTaskEvent(6, "task", protocol.EventUserMessage, &protocol.UserMessageV1{Content: strings.Repeat("recent ", 40)}),
	}
	_, err := compaction.Select(events, selectionCursor(6), compaction.TriggerManual, 128)
	if err == nil || errors.Is(err, compaction.ErrNothingToCompact) {
		t.Fatalf("unsafe selection limit error=%v", err)
	}
}

func selectionEvent(sequence uint64, kind string, decoded any) protocol.EventRecord {
	return selectionTaskEvent(sequence, "", kind, decoded)
}

func selectionTaskEvent(sequence uint64, task protocol.TaskID, kind string, decoded any) protocol.EventRecord {
	payload, _ := json.Marshal(decoded)
	return protocol.EventRecord{Envelope: protocol.EventEnvelope{
		SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1, JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session",
		EventID: protocol.EventID(fmt.Sprintf("event-%d", sequence)), Seq: sequence, Time: time.Unix(int64(sequence), 0).UTC(), Kind: kind, TaskID: task, TransactionID: protocol.TransactionID(fmt.Sprintf("tx-%d", sequence)), Payload: payload,
	}, Decoded: decoded}
}

func selectionCursor(sequence uint64) protocol.CommittedCursor {
	return protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "session", CommitSeq: sequence, TransactionID: protocol.TransactionID(fmt.Sprintf("tx-%d", sequence))}
}

func sameIDs(got, want []protocol.EventID) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func sourcesText(sources []protocol.ContentSource) string {
	var values []string
	for _, source := range sources {
		for _, block := range source.Content {
			values = append(values, block.Text)
		}
	}
	return strings.Join(values, "\n")
}
