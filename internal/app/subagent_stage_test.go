package app

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestApplicationSubagentReceiptEventCarriesOnlyStableStageIdentity(t *testing.T) {
	manifest := protocol.SubagentManifestV1{AttemptID: "attempt", ParentSessionID: "parent", ParentCursor: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "parent", CommitSeq: 1, TransactionID: "parent-head"}, ChildSessionID: "child", ChildTaskID: "child-task", ChildTurnID: "child-turn", RuntimeGenerationID: "generation", SkillCatalogRevision: "skills", MaxToolCalls: 4, Deadline: time.Unix(20, 0).UTC()}
	unknown := protocol.UsageValue{State: protocol.UsageUnknown}
	receipt := protocol.SubagentReceiptV1{Status: "uncertain", Summary: "RAW-SECRET-RECEIPT", Manifest: manifest, TerminalCursor: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child", CommitSeq: 8, TransactionID: "terminal"}, ChangedFiles: []string{}, CommandsAndTests: []string{}, Usage: protocol.ModelUsage{Input: unknown, Output: unknown, Cached: unknown, CacheWrite: unknown, Reasoning: unknown}, EvidenceIDs: []protocol.EvidenceID{}, UnknownEffects: []protocol.ActivityID{"effect"}}
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	envelope := protocol.EventEnvelope{SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1, JournalKind: protocol.JournalSession, JournalID: "child", SessionID: "child", EventID: "receipt", Seq: 8, Time: time.Unix(10, 0).UTC(), Kind: protocol.EventSubagentReceipt, TaskID: "child-task", TurnID: "child-turn", TransactionID: "terminal", RuntimeGenerationID: "generation", Payload: raw}
	event, err := applicationEvent(envelope, receipt.TerminalCursor, protocol.ApplicationCursor{WorkspaceControl: protocol.CommittedCursor{JournalKind: protocol.JournalWorkspaceControl, JournalID: "workspace", CommitSeq: 1, TransactionID: "workspace"}, SelectedSession: &receipt.TerminalCursor, Stream: protocol.StreamCursor{Epoch: "epoch"}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(event.Payload), receipt.Summary) || strings.Contains(string(event.Payload), "changed_files") || strings.Contains(string(event.Payload), "unknown_effects") {
		t.Fatalf("application event leaked raw receipt: %s", event.Payload)
	}
	var stage protocol.SubagentStageV1
	if err := json.Unmarshal(event.Payload, &stage); err != nil {
		t.Fatal(err)
	}
	if stage.Stage != protocol.SubagentStageUncertain || stage.AttemptID != manifest.AttemptID || stage.ParentSessionID != manifest.ParentSessionID || stage.ChildSessionID != manifest.ChildSessionID || event.Correlation.ParentSessionID != manifest.ParentSessionID || event.Correlation.DelegationAttemptID != manifest.AttemptID {
		t.Fatalf("stage event=%+v correlation=%+v", stage, event.Correlation)
	}
}

func TestBrokerFansOutChildStageToSelectedParentWithoutAdvancingParentJournalCursor(t *testing.T) {
	source := &stageSnapshotSource{}
	broker, err := NewBroker(BrokerOptions{Source: source, Epoch: "epoch", DefaultQueueCapacity: 8, MaxQueueCapacity: 8})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, subscription, err := broker.SnapshotAndSubscribe(t.Context(), protocol.SnapshotRequest{ProtocolVersion: protocol.ApplicationProtocolVersion, SelectedSessionID: "parent", Consumer: "parent-ui", QueueCapacity: 8})
	if err != nil {
		t.Fatal(err)
	}
	manifest := protocol.SubagentManifestV1{AttemptID: "attempt", ParentSessionID: "parent", ParentCursor: *snapshot.Cursor.SelectedSession, ChildSessionID: "child", ChildTaskID: "task", ChildTurnID: "turn", RuntimeGenerationID: "generation", SkillCatalogRevision: "skills", MaxToolCalls: 1, Deadline: time.Unix(20, 0).UTC()}
	raw, _ := json.Marshal(manifest)
	childCursor := protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child", CommitSeq: 2, TransactionID: "child-manifest"}
	envelope := protocol.EventEnvelope{SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1, JournalKind: protocol.JournalSession, JournalID: "child", SessionID: "child", EventID: "manifest", Seq: 2, Time: time.Unix(10, 0).UTC(), Kind: protocol.EventSubagentManifest, TaskID: "task", TurnID: "turn", TransactionID: "child-manifest", RuntimeGenerationID: "generation", Payload: raw}
	if err := broker.PublishCommitted(t.Context(), protocol.JournalRef{Kind: protocol.JournalSession, ID: "child"}, childCursor, []protocol.EventEnvelope{envelope}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	item, err := subscription.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if item.Event == nil || item.Event.Classification != "transient" || item.Event.JournalCursor != nil || item.Event.Cursor.SelectedSession == nil || *item.Event.Cursor.SelectedSession != *snapshot.Cursor.SelectedSession || item.Event.Correlation.ParentSessionID != "parent" || item.Event.Correlation.DelegationAttemptID != "attempt" {
		t.Fatalf("parent child-stage fanout=%+v", item)
	}
}
