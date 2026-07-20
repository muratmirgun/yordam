package subagent

import (
	"fmt"
	"strings"
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

func attachmentEvent(manifest protocol.SubagentManifestV1, receipt protocol.EventRecord, seq uint64) protocol.EventRecord {
	digest, err := canonicaljson.Digest(receipt.Decoded.(*protocol.SubagentReceiptV1))
	if err != nil {
		panic(err)
	}
	event := subagentEvent(protocol.EventSubagentResultAttached, manifest.ParentSessionID, seq, &protocol.SubagentResultAttachedV1{AttemptID: manifest.AttemptID, ChildSessionID: manifest.ChildSessionID, TerminalCursor: receipt.Decoded.(*protocol.SubagentReceiptV1).TerminalCursor, ReceiptDigest: digest, ReceiptEvidenceID: "evidence"})
	event.Envelope.TaskID, event.Envelope.TurnID = "parent-task", "parent-turn"
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
	attachment := attachmentEvent(manifest, receipt, 6)
	state, err = projector.Apply(state, attachment)
	if err != nil || state.Attempts[manifest.AttemptID].State != StateAttached {
		t.Fatalf("attachment = %#v, %v", state, err)
	}
	state, err = projector.Apply(state, attachment)
	if err != nil || state.Attempts[manifest.AttemptID].State != StateAttached {
		t.Fatalf("idempotent attachment = %#v, %v", state, err)
	}
}

func TestSubagentProjectorConflictIsAbsorbingAfterAValidAttachment(t *testing.T) {
	projector := Projector{}
	manifest := projectorManifest()
	state := projector.Zero(protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"})
	for _, event := range []protocol.EventRecord{requestEvent(manifest), waitingEvent(manifest), manifestEvent(manifest)} {
		var err error
		state, err = projector.Apply(state, event)
		if err != nil {
			t.Fatal(err)
		}
	}
	receipt := receiptEvent(manifest, "succeeded")
	var err error
	state, err = projector.Apply(state, receipt)
	if err != nil {
		t.Fatal(err)
	}
	invalid := attachmentEvent(manifest, receipt, 6)
	invalid.Decoded.(*protocol.SubagentResultAttachedV1).ReceiptDigest.Value = strings.Repeat("a", 64)
	state, err = projector.Apply(state, invalid)
	if err != nil || state.Attempts[manifest.AttemptID].State != StateConflict {
		t.Fatalf("invalid attachment = %#v, %v", state, err)
	}
	state, err = projector.Apply(state, attachmentEvent(manifest, receipt, 7))
	if err != nil || state.Attempts[manifest.AttemptID].State != StateConflict || state.Attempts[manifest.AttemptID].Attachment != nil {
		t.Fatalf("conflict was silently resolved by matching attachment: %#v, %v", state, err)
	}
}

func TestSubagentProjectorAcceptsEveryTerminalStatus(t *testing.T) {
	for _, status := range []string{"succeeded", "failed", "cancelled", "uncertain"} {
		t.Run(status, func(t *testing.T) {
			projector := Projector{}
			manifest := projectorManifest()
			state := projector.Zero(protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"})
			for _, event := range []protocol.EventRecord{requestEvent(manifest), waitingEvent(manifest), manifestEvent(manifest), receiptEvent(manifest, status)} {
				var err error
				state, err = projector.Apply(state, event)
				if err != nil {
					t.Fatal(err)
				}
			}
			attempt := state.Attempts[manifest.AttemptID]
			if attempt.State != StateTerminal || attempt.Receipt == nil || attempt.Receipt.Status != status {
				t.Fatalf("terminal status = %#v", attempt)
			}
		})
	}
}

func TestSubagentProjectorEnforcesAttemptLimitPerParentTurn(t *testing.T) {
	projector := Projector{}
	state := projector.Zero(protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"})
	for index := 0; index < protocol.MaxSubagentAttemptsPerTurn; index++ {
		requestSeq := uint64(2 + index*10)
		manifest := projectorManifest()
		manifest.AttemptID = protocol.DelegationAttemptID(fmt.Sprintf("attempt-%d", index))
		manifest.ChildSessionID = protocol.SessionID(fmt.Sprintf("child-%d", index))
		manifest.ChildTaskID = protocol.TaskID(fmt.Sprintf("child-task-%d", index))
		manifest.ChildTurnID = protocol.TurnID(fmt.Sprintf("child-turn-%d", index))
		manifest.ParentCursor = cursor("parent", requestSeq-1)
		request := requestEvent(manifest)
		request.Envelope.Seq = requestSeq
		waiting := waitingEvent(manifest)
		waiting.Envelope.Seq = requestSeq + 1
		receipt := receiptEvent(manifest, "succeeded")
		attachment := attachmentEvent(manifest, receipt, requestSeq+2)
		for _, event := range []protocol.EventRecord{request, waiting, manifestEvent(manifest), receipt, attachment} {
			var err error
			state, err = projector.Apply(state, event)
			if err != nil {
				t.Fatalf("attempt %d event %s: %v", index, event.Envelope.Kind, err)
			}
		}
	}
	limited := projectorManifest()
	limited.AttemptID, limited.ChildSessionID = "attempt-over-limit", "child-over-limit"
	limited.ChildTaskID, limited.ChildTurnID = "child-task-over-limit", "child-turn-over-limit"
	limited.ParentCursor = cursor("parent", 100)
	request := requestEvent(limited)
	request.Envelope.Seq = 101
	if _, err := projector.Apply(state, request); err == nil {
		t.Fatal("fifth replayed attempt for the parent turn accepted")
	}
}

func TestSubagentProjectorRejectsMalformedAndOutOfOrderParentChildEvents(t *testing.T) {
	projector := Projector{}
	manifest := projectorManifest()
	state := projector.Zero(protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"})
	if _, err := projector.Apply(state, manifestEvent(manifest)); err == nil {
		t.Fatal("child manifest before parent waiting accepted")
	}
	state, _ = projector.Apply(state, requestEvent(manifest))
	wrongRuntime := waitingEvent(manifest)
	wrongRuntime.Envelope.RuntimeGenerationID = "other-generation"
	if _, err := projector.Apply(state, wrongRuntime); err == nil {
		t.Fatal("waiting with an unbound runtime accepted")
	}
	wrongParent := waitingEvent(manifest)
	wrongParent.Envelope.JournalID, wrongParent.Envelope.SessionID = "other-parent", "other-parent"
	if _, err := projector.Apply(state, wrongParent); err == nil {
		t.Fatal("waiting in another parent journal accepted")
	}
	state, _ = projector.Apply(state, waitingEvent(manifest))
	if _, err := projector.Apply(state, receiptEvent(manifest, "succeeded")); err == nil {
		t.Fatal("receipt before child manifest accepted")
	}
	state, _ = projector.Apply(state, manifestEvent(manifest))
	receipt := receiptEvent(manifest, "succeeded")
	attachment := attachmentEvent(manifest, receipt, 6)
	attachment.Envelope.TurnID = "other-turn"
	if _, err := projector.Apply(state, attachment); err == nil {
		t.Fatal("attachment with an unbound parent turn accepted")
	}
	malformed := receiptEvent(manifest, "succeeded")
	malformed.Envelope.Seq = 6
	if _, err := projector.Apply(state, malformed); err == nil {
		t.Fatal("receipt with mismatched terminal cursor accepted")
	}
	wrongDecoded := requestEvent(manifest)
	wrongDecoded.Decoded = &protocol.SubagentWaitingV1{AttemptID: manifest.AttemptID, ChildSessionID: manifest.ChildSessionID}
	if _, err := projector.Apply(state, wrongDecoded); err == nil {
		t.Fatal("event with a descriptor-mismatched decoded payload accepted")
	}
}

func TestSubagentProjectorBindsWaitingAndAttachmentToStoredParent(t *testing.T) {
	projector := Projector{}
	manifest := projectorManifest()
	state := projector.Zero(protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"})
	var err error
	state, err = projector.Apply(state, requestEvent(manifest))
	if err != nil {
		t.Fatal(err)
	}
	wrongWaiting := waitingEvent(manifest)
	wrongWaiting.Envelope.TaskID = "other-parent-task"
	if _, err := projector.Apply(state, wrongWaiting); err == nil {
		t.Fatal("waiting did not bind the stored parent task")
	}
	for _, event := range []protocol.EventRecord{waitingEvent(manifest), manifestEvent(manifest), receiptEvent(manifest, "succeeded")} {
		state, err = projector.Apply(state, event)
		if err != nil {
			t.Fatal(err)
		}
	}
	wrongAttachment := attachmentEvent(manifest, receiptEvent(manifest, "succeeded"), 6)
	wrongAttachment.Envelope.RuntimeGenerationID = "other-generation"
	if _, err := projector.Apply(state, wrongAttachment); err == nil {
		t.Fatal("attachment did not bind the stored parent runtime")
	}
	state, err = projector.Apply(state, attachmentEvent(manifest, receiptEvent(manifest, "succeeded"), 7))
	if err != nil || state.Attempts[manifest.AttemptID].State != StateAttached {
		t.Fatalf("matching parent attachment = %#v, %v", state, err)
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
	validAttachment := attachmentEvent(manifest, receiptEvent(manifest, "succeeded"), 6)
	state, err = projector.Apply(state, validAttachment)
	if err != nil || state.Attempts[manifest.AttemptID].State != StateConflict || state.Attempts[manifest.AttemptID].Attachment != nil {
		t.Fatalf("conflicting receipt was silently resolved by matching attachment: %#v, %v", state, err)
	}
}

func TestSubagentProjectorRequestConflictIsAbsorbing(t *testing.T) {
	projector := Projector{}
	manifest := projectorManifest()
	state := projector.Zero(protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"})
	var err error
	state, err = projector.Apply(state, requestEvent(manifest))
	if err != nil {
		t.Fatal(err)
	}
	conflicting := requestEvent(manifest)
	conflicting.Decoded.(*protocol.SubagentRequestedV1).Call.Context = "changed"
	state, err = projector.Apply(state, conflicting)
	if err != nil || state.Attempts[manifest.AttemptID].State != StateConflict {
		t.Fatalf("conflicting request = %#v, %v", state, err)
	}
	state, err = projector.Apply(state, requestEvent(manifest))
	if err != nil || state.Attempts[manifest.AttemptID].State != StateConflict {
		t.Fatalf("conflicting request was silently resolved: %#v, %v", state, err)
	}
}
