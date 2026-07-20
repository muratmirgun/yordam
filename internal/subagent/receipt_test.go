package subagent

import (
	"encoding/json"
	"testing"

	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestProjectReceiptUsesOnlyDurableEffectsAndMarksUnmatchedActivityUncertain(t *testing.T) {
	manifest := protocol.SubagentManifestV1{ChildSessionID: "child"}
	events := []protocol.EventRecord{
		durableReceiptEvent(protocol.EventFileChanged, protocol.FileChangedV1{Subject: protocol.SubjectRef{Kind: "file", ID: "/workspace/z.go"}}),
		durableReceiptEvent(protocol.EventFileChanged, protocol.FileChangedV1{Subject: protocol.SubjectRef{Kind: "file", ID: "/workspace/a.go"}}),
		activityReceiptEvent(protocol.EventExecutionPlanDeclared, "shell", protocol.ExecutionPlanDeclaredV1{Plan: protocol.ActionPlan{Body: protocol.ActionPlanBody{Tool: protocol.ToolIdentity{Name: "shell"}, Resources: []protocol.ResourceTarget{{Kind: "directory", CanonicalID: "/workspace", Attributes: []protocol.ResourceAttribute{{Name: "command", Value: "go test ./..."}}}}}}}),
		activityReceiptEvent(protocol.EventActivityStarted, "shell", protocol.ActivityStartedV1{}),
		activityReceiptEvent(protocol.EventActivitySucceeded, "shell", protocol.ActivityOutcomeV1{Status: "succeeded"}),
		durableReceiptEvent(protocol.EventEvidenceRecorded, protocol.EvidenceRecordedV1{Record: protocol.EvidenceRecord{Body: protocol.EvidenceRecordBody{ID: "evidence-1"}}}),
		{Envelope: protocol.EventEnvelope{Kind: protocol.EventActivityStarted, ActivityID: "ambiguous"}},
	}
	receipt := ProjectReceipt(manifest, protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child"}, "succeeded", "assistant claims file changes", protocol.ModelUsage{}, nil, events)
	if receipt.Status != "uncertain" || len(receipt.UnknownEffects) != 1 || receipt.UnknownEffects[0] != "ambiguous" {
		t.Fatalf("unmatched activity was not uncertain: %+v", receipt)
	}
	if got, want := receipt.ChangedFiles, []string{"/workspace/a.go", "/workspace/z.go"}; !equalStrings(got, want) {
		t.Fatalf("changed files = %v, want %v", got, want)
	}
	if got, want := receipt.CommandsAndTests, []string{"go test ./..."}; !equalStrings(got, want) {
		t.Fatalf("commands = %v, want %v", got, want)
	}
	if len(receipt.EvidenceIDs) != 1 || receipt.EvidenceIDs[0] != "evidence-1" {
		t.Fatalf("evidence = %v", receipt.EvidenceIDs)
	}
}

func TestProjectReceiptDoesNotTreatAssistantProseAsEvidence(t *testing.T) {
	receipt := ProjectReceipt(protocol.SubagentManifestV1{}, protocol.CommittedCursor{}, "succeeded", "changed /workspace/fake.go and ran go test", protocol.ModelUsage{}, nil, nil)
	if len(receipt.ChangedFiles) != 0 || len(receipt.CommandsAndTests) != 0 {
		t.Fatalf("assistant prose became durable evidence: %+v", receipt)
	}
}

func TestProjectReceiptExcludesShellPlansThatNeverStarted(t *testing.T) {
	plan := activityReceiptEvent(protocol.EventExecutionPlanDeclared, "denied", protocol.ExecutionPlanDeclaredV1{Plan: protocol.ActionPlan{Body: protocol.ActionPlanBody{Tool: protocol.ToolIdentity{Name: "shell"}, Resources: []protocol.ResourceTarget{{Kind: "directory", CanonicalID: "/workspace", Attributes: []protocol.ResourceAttribute{{Name: "command", Value: "rm -rf nope"}}}}}}})
	denied := activityReceiptEvent(protocol.EventActivityDenied, "denied", protocol.ActivityOutcomeV1{Status: "denied"})
	receipt := ProjectReceipt(protocol.SubagentManifestV1{}, protocol.CommittedCursor{}, "failed", "", protocol.ModelUsage{}, nil, []protocol.EventRecord{plan, denied})
	if len(receipt.CommandsAndTests) != 0 {
		t.Fatalf("unstarted command was reported: %v", receipt.CommandsAndTests)
	}
}

func durableReceiptEvent(kind string, payload any) protocol.EventRecord {
	raw, _ := json.Marshal(payload)
	return protocol.EventRecord{Envelope: protocol.EventEnvelope{Kind: kind, Payload: raw}}
}

func activityReceiptEvent(kind string, activityID protocol.ActivityID, payload any) protocol.EventRecord {
	event := durableReceiptEvent(kind, payload)
	event.Envelope.ActivityID = activityID
	return event
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
