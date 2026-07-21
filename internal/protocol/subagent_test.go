package protocol_test

import (
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func validSubagentManifest() protocol.SubagentManifestV1 {
	return protocol.SubagentManifestV1{
		AttemptID: "attempt", ParentSessionID: "parent", ParentCursor: subagentCursor("parent", 4),
		ChildSessionID: "child", ChildTaskID: "child-task", ChildTurnID: "child-turn",
		RuntimeGenerationID: "generation", SkillCatalogRevision: "skills-r1", MaxToolCalls: 16,
		Deadline: time.Unix(10, 0).UTC(),
	}
}

func subagentCursor(session protocol.SessionID, seq uint64) protocol.CommittedCursor {
	return protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(session), CommitSeq: seq, TransactionID: "transaction"}
}

func validSubagentReceipt() protocol.SubagentReceiptV1 {
	manifest := validSubagentManifest()
	return protocol.SubagentReceiptV1{
		Status: "succeeded", Summary: "completed", Manifest: manifest, TerminalCursor: subagentCursor(manifest.ChildSessionID, 8),
		ChangedFiles: []string{"a.go"}, CommandsAndTests: []string{"go test ./..."}, Usage: subagentUsage(), EvidenceIDs: []protocol.EvidenceID{"evidence"}, UnknownEffects: []protocol.ActivityID{},
	}
}

func subagentUsage() protocol.ModelUsage {
	unknown := protocol.UsageValue{State: protocol.UsageUnknown}
	return protocol.ModelUsage{Input: unknown, Output: unknown, Cached: unknown, CacheWrite: unknown, Reasoning: unknown}
}

func TestSubagentCallAndManifestValidateBoundedDelegation(t *testing.T) {
	call := protocol.SubagentCallV1{Task: "inspect", ExpectedOutput: "report", Context: "context"}
	if err := call.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := validSubagentManifest().Validate(); err != nil {
		t.Fatal(err)
	}

	for name, mutate := range map[string]func(*protocol.SubagentCallV1){
		"empty task": func(v *protocol.SubagentCallV1) { v.Task = "" },
		"task bound": func(v *protocol.SubagentCallV1) { v.Task = strings.Repeat("x", protocol.MaxSubagentTaskBytes+1) },
		"expected output bound": func(v *protocol.SubagentCallV1) {
			v.ExpectedOutput = strings.Repeat("x", protocol.MaxSubagentExpectedOutputBytes+1)
		},
		"context bound": func(v *protocol.SubagentCallV1) { v.Context = strings.Repeat("x", protocol.MaxSubagentContextBytes+1) },
	} {
		t.Run(name, func(t *testing.T) {
			value := call
			mutate(&value)
			if err := value.Validate(); err == nil {
				t.Fatal("invalid subagent call accepted")
			}
		})
	}
	bad := validSubagentManifest()
	bad.ParentCursor.JournalID = "other"
	if err := bad.Validate(); err == nil {
		t.Fatal("cross-parent cursor accepted")
	}
	bad = validSubagentManifest()
	bad.ChildSessionID = bad.ParentSessionID
	if err := bad.Validate(); err == nil {
		t.Fatal("parent may be its own child")
	}
	bad = validSubagentManifest()
	bad.MaxToolCalls = 0
	if err := bad.Validate(); err == nil {
		t.Fatal("zero child tool budget accepted")
	}
	bad = validSubagentManifest()
	bad.Deadline = time.Time{}
	if err := bad.Validate(); err == nil {
		t.Fatal("missing deadline accepted")
	}
}

func TestSubagentReceiptDigestBindsExactTerminalReceipt(t *testing.T) {
	receipt := validSubagentReceipt()
	if err := receipt.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := canonicaljson.Digest(receipt); err != nil {
		t.Fatal(err)
	}

	for name, mutate := range map[string]func(*protocol.SubagentReceiptV1){
		"invalid status": func(v *protocol.SubagentReceiptV1) { v.Status = "running" },
		"summary bound": func(v *protocol.SubagentReceiptV1) {
			v.Summary = strings.Repeat("x", protocol.MaxSubagentReceiptSummaryBytes+1)
		},
		"other terminal journal": func(v *protocol.SubagentReceiptV1) { v.TerminalCursor.JournalID = "other" },
		"unsorted files":         func(v *protocol.SubagentReceiptV1) { v.ChangedFiles = []string{"b.go", "a.go"} },
		"duplicate tests":        func(v *protocol.SubagentReceiptV1) { v.CommandsAndTests = []string{"go test", "go test"} },
		"duplicate evidence":     func(v *protocol.SubagentReceiptV1) { v.EvidenceIDs = []protocol.EvidenceID{"e", "e"} },
		"duplicate effects":      func(v *protocol.SubagentReceiptV1) { v.UnknownEffects = []protocol.ActivityID{"a", "a"} },
	} {
		t.Run(name, func(t *testing.T) {
			value := receipt
			mutate(&value)
			if err := value.Validate(); err == nil {
				t.Fatal("invalid receipt accepted")
			}
		})
	}
}

func TestSubagentApplicationCorrelationRequiresParentAttemptPair(t *testing.T) {
	correlation := protocol.EventCorrelation{JournalKind: protocol.JournalSession, JournalID: "child", SessionID: "child", ParentSessionID: "parent", DelegationAttemptID: "attempt"}
	if err := correlation.Validate(); err != nil {
		t.Fatal(err)
	}
	correlation.ParentSessionID = ""
	if err := correlation.Validate(); err == nil {
		t.Fatal("attempt without parent session accepted")
	}
}
