package subagent

import (
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func projectorManifest() protocol.SubagentManifestV1 {
	return protocol.SubagentManifestV1{AttemptID: "attempt", ParentSessionID: "parent", ParentCursor: cursor("parent", 1), ChildSessionID: "child", ChildTaskID: "child-task", ChildTurnID: "child-turn", RuntimeGenerationID: "generation", SkillCatalogRevision: "skills", MaxToolCalls: 1, Deadline: time.Unix(10, 0).UTC()}
}

func cursor(session protocol.SessionID, seq uint64) protocol.CommittedCursor {
	return protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(session), CommitSeq: seq, TransactionID: "transaction"}
}

func subagentEvent(kind string, session protocol.SessionID, seq uint64, decoded any) protocol.EventRecord {
	return protocol.EventRecord{Envelope: protocol.EventEnvelope{SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1, JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(session), SessionID: session, EventID: protocol.EventID(kind + string(rune('0'+seq))), Seq: seq, Time: time.Unix(int64(seq), 0).UTC(), Kind: kind, RuntimeGenerationID: "generation", TransactionID: "transaction"}, Decoded: decoded}
}

func requestEvent(manifest protocol.SubagentManifestV1) protocol.EventRecord {
	event := subagentEvent(protocol.EventSubagentRequested, manifest.ParentSessionID, 2, &protocol.SubagentRequestedV1{Call: protocol.SubagentCallV1{Task: "task"}, Manifest: manifest})
	event.Envelope.TaskID, event.Envelope.TurnID = "parent-task", "parent-turn"
	return event
}

func waitingEvent(manifest protocol.SubagentManifestV1) protocol.EventRecord {
	event := subagentEvent(protocol.EventSubagentWaiting, manifest.ParentSessionID, 3, &protocol.SubagentWaitingV1{AttemptID: manifest.AttemptID, ChildSessionID: manifest.ChildSessionID})
	event.Envelope.TaskID, event.Envelope.TurnID = "parent-task", "parent-turn"
	return event
}

func manifestEvent(manifest protocol.SubagentManifestV1) protocol.EventRecord {
	event := subagentEvent(protocol.EventSubagentManifest, manifest.ChildSessionID, 1, &manifest)
	event.Envelope.TaskID, event.Envelope.TurnID = manifest.ChildTaskID, manifest.ChildTurnID
	return event
}

func receiptEvent(manifest protocol.SubagentManifestV1, status string) protocol.EventRecord {
	unknown := protocol.UsageValue{State: protocol.UsageUnknown}
	receipt := protocol.SubagentReceiptV1{Status: status, Summary: status, Manifest: manifest, TerminalCursor: cursor(manifest.ChildSessionID, 5), ChangedFiles: []string{}, CommandsAndTests: []string{}, Usage: protocol.ModelUsage{Input: unknown, Output: unknown, Cached: unknown, CacheWrite: unknown, Reasoning: unknown}, EvidenceIDs: []protocol.EvidenceID{}, UnknownEffects: []protocol.ActivityID{}}
	event := subagentEvent(protocol.EventSubagentReceipt, manifest.ChildSessionID, 5, &receipt)
	event.Envelope.TaskID, event.Envelope.TurnID, event.Envelope.TransactionID = manifest.ChildTaskID, manifest.ChildTurnID, receipt.TerminalCursor.TransactionID
	return event
}

func TestSubagentProjectorTracksRequestWaitTerminalAndAttachment(t *testing.T) {
	projector := Projector{}
	state := projector.Zero(protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"})
	manifest := projectorManifest()
	var err error
	state, err = projector.Apply(state, requestEvent(manifest))
	if err != nil || state.Attempts[manifest.AttemptID].State != StateIncomplete {
		t.Fatalf("request = %#v, %v", state, err)
	}
	state, err = projector.Apply(state, waitingEvent(manifest))
	if err != nil || state.Attempts[manifest.AttemptID].State != StateWaiting {
		t.Fatalf("waiting = %#v, %v", state, err)
	}
	state, err = projector.Apply(state, manifestEvent(manifest))
	if err != nil || !state.Attempts[manifest.AttemptID].ChildManifested {
		t.Fatalf("manifest = %#v, %v", state, err)
	}
	receipt := receiptEvent(manifest, "succeeded")
	state, err = projector.Apply(state, receipt)
	if err != nil || state.Attempts[manifest.AttemptID].State != StateTerminal || state.Attempts[manifest.AttemptID].TerminalCursor != cursor("child", 5) {
		t.Fatalf("receipt = %#v, %v", state, err)
	}
	digest, err := canonicaljson.Digest(receipt.Decoded.(*protocol.SubagentReceiptV1))
	if err != nil {
		t.Fatal(err)
	}
	attachment := subagentEvent(protocol.EventSubagentResultAttached, "parent", 6, &protocol.SubagentResultAttachedV1{AttemptID: manifest.AttemptID, ChildSessionID: "child", TerminalCursor: cursor("child", 5), ReceiptDigest: digest, ReceiptEvidenceID: "evidence"})
	attachment.Envelope.TaskID, attachment.Envelope.TurnID = "parent-task", "parent-turn"
	state, err = projector.Apply(state, attachment)
	if err != nil || state.Attempts[manifest.AttemptID].State != StateAttached {
		t.Fatalf("attachment = %#v, %v", state, err)
	}
	state, err = projector.Apply(state, attachment)
	if err != nil || state.Attempts[manifest.AttemptID].State != StateAttached {
		t.Fatalf("idempotent attachment = %#v, %v", state, err)
	}
}

func TestSubagentProjectorReportsConflictsAndRejectsInvalidOrdering(t *testing.T) {
	projector := Projector{}
	manifest := projectorManifest()
	state := projector.Zero(protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"})
	if _, err := projector.Apply(state, waitingEvent(manifest)); err == nil {
		t.Fatal("waiting without request accepted")
	}
	if _, err := projector.Apply(state, receiptEvent(manifest, "succeeded")); err == nil {
		t.Fatal("receipt before parent wait accepted")
	}
	var err error
	state, err = projector.Apply(state, requestEvent(manifest))
	if err != nil {
		t.Fatal(err)
	}
	other := manifest
	other.AttemptID, other.ChildSessionID = "other", "other-child"
	if _, err := projector.Apply(state, requestEvent(other)); err == nil {
		t.Fatal("two active children accepted")
	}
	state, err = projector.Apply(state, waitingEvent(manifest))
	if err != nil {
		t.Fatal(err)
	}
	state, err = projector.Apply(state, manifestEvent(manifest))
	if err != nil {
		t.Fatal(err)
	}
	state, err = projector.Apply(state, receiptEvent(manifest, "succeeded"))
	if err != nil {
		t.Fatal(err)
	}
	state, err = projector.Apply(state, receiptEvent(manifest, "failed"))
	if err != nil || state.Attempts[manifest.AttemptID].State != StateConflict {
		t.Fatalf("conflicting receipt = %#v, %v", state, err)
	}
}
