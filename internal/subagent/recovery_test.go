package subagent

import (
	"testing"

	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestSubagentRecoverTerminalReceiptNeedsAttachmentBeforeParentCanResume(t *testing.T) {
	manifest := projectorManifest()
	receipt := receiptEvent(manifest, "succeeded").Decoded.(*protocol.SubagentReceiptV1)
	digest, err := receiptDigest(*receipt)
	if err != nil {
		t.Fatal(err)
	}
	attempt := Attempt{AttemptID: manifest.AttemptID, Manifest: manifest, State: StateTerminal, Receipt: receipt, ReceiptDigest: digest, TerminalCursor: cursor("child", 5)}
	result, err := Reconcile(ReconcileRequest{ParentSessionID: manifest.ParentSessionID, ParentCursor: manifest.ParentCursor, Runtime: protocol.RuntimeGenerationManifest{ID: manifest.RuntimeGenerationID}}, attempt, ChildRecoveryState{Exists: true, CommitKnown: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Attached || result.ParentResumable || result.ChildStatus != "succeeded" {
		t.Fatalf("result=%+v", result)
	}
}

func TestSubagentRecoverNoEffectChildIsCancelledButUnprovenEffectIsUncertain(t *testing.T) {
	manifest := projectorManifest()
	attempt := Attempt{AttemptID: manifest.AttemptID, Manifest: manifest, State: StateWaiting}
	request := ReconcileRequest{ParentSessionID: manifest.ParentSessionID, ParentCursor: manifest.ParentCursor, Runtime: protocol.RuntimeGenerationManifest{ID: manifest.RuntimeGenerationID}}
	for _, test := range []struct {
		name   string
		state  ChildRecoveryState
		status string
	}{
		{name: "no effect", state: ChildRecoveryState{Exists: true, CommitKnown: true, NoUnmatchedEffect: true}, status: "cancelled"},
		{name: "unproven effect", state: ChildRecoveryState{Exists: true, CommitKnown: true}, status: "uncertain"},
		{name: "commit unknown", state: ChildRecoveryState{Exists: true, CommitKnown: false}, status: "uncertain"},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := Reconcile(request, attempt, test.state)
			if err != nil {
				t.Fatal(err)
			}
			if result.ChildStatus != test.status || result.ParentResumable {
				t.Fatalf("result=%+v", result)
			}
		})
	}
}

func TestSubagentRecoverAttachmentIsTheOnlyResumableState(t *testing.T) {
	manifest := projectorManifest()
	receipt := receiptEvent(manifest, "cancelled").Decoded.(*protocol.SubagentReceiptV1)
	digest, err := receiptDigest(*receipt)
	if err != nil {
		t.Fatal(err)
	}
	attempt := Attempt{AttemptID: manifest.AttemptID, Manifest: manifest, State: StateAttached, Receipt: receipt, ReceiptDigest: digest, TerminalCursor: receipt.TerminalCursor, Attachment: &protocol.SubagentResultAttachedV1{AttemptID: manifest.AttemptID, ChildSessionID: manifest.ChildSessionID, TerminalCursor: receipt.TerminalCursor, ReceiptDigest: digest, ReceiptEvidenceID: "evidence"}}
	result, err := Reconcile(ReconcileRequest{ParentSessionID: manifest.ParentSessionID, ParentCursor: manifest.ParentCursor, Runtime: protocol.RuntimeGenerationManifest{ID: manifest.RuntimeGenerationID}}, attempt, ChildRecoveryState{Exists: true, CommitKnown: true})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Attached || !result.ParentResumable || result.ChildStatus != "cancelled" {
		t.Fatalf("result=%+v", result)
	}
}

func TestSubagentRecoverRestartAndCancellationMatrix(t *testing.T) {
	manifest := projectorManifest()
	request := ReconcileRequest{ParentSessionID: manifest.ParentSessionID, ParentCursor: manifest.ParentCursor, Runtime: protocol.RuntimeGenerationManifest{ID: manifest.RuntimeGenerationID}}
	nonterminal := Attempt{AttemptID: manifest.AttemptID, Manifest: manifest, State: StateWaiting}
	receipt := receiptEvent(manifest, "cancelled").Decoded.(*protocol.SubagentReceiptV1)
	digest, err := receiptDigest(*receipt)
	if err != nil {
		t.Fatal(err)
	}
	attached := Attempt{AttemptID: manifest.AttemptID, Manifest: manifest, State: StateAttached, Receipt: receipt, ReceiptDigest: digest, TerminalCursor: receipt.TerminalCursor, Attachment: &protocol.SubagentResultAttachedV1{AttemptID: manifest.AttemptID, ChildSessionID: manifest.ChildSessionID, TerminalCursor: receipt.TerminalCursor, ReceiptDigest: digest, ReceiptEvidenceID: "evidence"}}
	for _, test := range []struct {
		name      string
		attempt   Attempt
		child     ChildRecoveryState
		status    string
		create    bool
		resumable bool
	}{
		{name: "parent wait before child creation", attempt: nonterminal, child: ChildRecoveryState{CommitKnown: true}, status: "planned", create: true},
		{name: "child created before provider stream", attempt: nonterminal, child: ChildRecoveryState{Exists: true, CommitKnown: true, NoUnmatchedEffect: true}, status: "cancelled"},
		{name: "provider stream cancellation with unproven mutation", attempt: nonterminal, child: ChildRecoveryState{Exists: true, CommitKnown: true}, status: "uncertain"},
		{name: "pending approval cancellation leaves no effect", attempt: nonterminal, child: ChildRecoveryState{Exists: true, CommitKnown: true, NoUnmatchedEffect: true}, status: "cancelled"},
		{name: "shell process cancellation with unproven effect", attempt: nonterminal, child: ChildRecoveryState{Exists: true, CommitKnown: true}, status: "uncertain"},
		{name: "child receipt before parent attachment", attempt: Attempt{AttemptID: manifest.AttemptID, Manifest: manifest, State: StateTerminal, Receipt: receipt, ReceiptDigest: digest, TerminalCursor: receipt.TerminalCursor}, child: ChildRecoveryState{Exists: true, CommitKnown: true}, status: "cancelled"},
		{name: "parent reacquire after exact attachment", attempt: attached, child: ChildRecoveryState{Exists: true, CommitKnown: true}, status: "cancelled", resumable: true},
		{name: "child receipt commit unknown", attempt: nonterminal, child: ChildRecoveryState{Exists: true}, status: "uncertain"},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := Reconcile(request, test.attempt, test.child)
			if err != nil {
				t.Fatal(err)
			}
			if result.ChildStatus != test.status || result.CreateOnce != test.create || result.ParentResumable != test.resumable || result.Attached != test.resumable {
				t.Fatalf("result=%+v", result)
			}
		})
	}
}
