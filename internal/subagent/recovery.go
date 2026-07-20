package subagent

import (
	"fmt"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
)

// ReconcileRequest is the immutable parent binding used when recovery examines
// a sequential child.  It intentionally contains no provider request: recovery
// may repair durable handoff state, but it must never re-run a child effect.
type ReconcileRequest struct {
	ParentSessionID protocol.SessionID
	ParentCursor    protocol.CommittedCursor
	Runtime         protocol.RuntimeGenerationManifest
}

// ReconcileResult describes only what the durable journals prove.  A caller
// may continue a parent provider loop only when ParentResumable is true.
type ReconcileResult struct {
	AttemptID       protocol.DelegationAttemptID
	ChildSessionID  protocol.SessionID
	ChildStatus     string
	Attached        bool
	ParentResumable bool

	// CreateOnce is true solely for a reserved, exact child identity whose
	// absence is proven. It does not authorize a provider retry.
	CreateOnce bool
	Diagnostic string
}

// ChildRecoveryState captures the only facts recovery needs from the child
// journal and effect probe. CommitKnown is deliberately separate from Exists:
// an unavailable or commit-unknown journal is an uncertainty, not absence.
type ChildRecoveryState struct {
	Exists            bool
	CommitKnown       bool
	NoUnmatchedEffect bool
}

// Reconcile classifies a projected parent attempt without mutating either
// journal. Mutation is deliberately left to the orchestrator, which writes a
// child receipt before a parent attachment under its lane/lease discipline.
func Reconcile(request ReconcileRequest, attempt Attempt, child ChildRecoveryState) (ReconcileResult, error) {
	if err := reconcileBinding(request, attempt); err != nil {
		return ReconcileResult{}, err
	}
	result := ReconcileResult{AttemptID: attempt.AttemptID, ChildSessionID: attempt.Manifest.ChildSessionID}
	if attempt.State == StateConflict {
		result.ChildStatus, result.Diagnostic = "uncertain", "conflicting subagent journal state"
		return result, nil
	}
	if !child.Exists {
		if child.CommitKnown && (attempt.State == StateIncomplete || attempt.State == StateWaiting) {
			result.ChildStatus, result.CreateOnce, result.Diagnostic = "planned", true, "exact child identity is absent"
			return result, nil
		}
		result.ChildStatus, result.Diagnostic = "uncertain", "child journal absence is not proven"
		return result, nil
	}
	if !child.CommitKnown {
		result.ChildStatus, result.Diagnostic = "uncertain", "child journal commit is unknown"
		return result, nil
	}
	if attempt.Receipt != nil {
		if err := validateReceipt(attempt); err != nil {
			result.ChildStatus, result.Diagnostic = "uncertain", err.Error()
			return result, nil
		}
		result.ChildStatus = attempt.Receipt.Status
		if attempt.State == StateAttached && attachmentMatches(attempt) {
			result.Attached, result.ParentResumable = true, true
			return result, nil
		}
		if attempt.State == StateAttached {
			result.ChildStatus, result.Diagnostic = "uncertain", "attached receipt identity or digest is not proven"
			return result, nil
		}
		result.Diagnostic = "terminal child receipt requires parent attachment"
		return result, nil
	}
	if child.NoUnmatchedEffect {
		result.ChildStatus, result.Diagnostic = "cancelled", "child interrupted before any unmatched effect"
		return result, nil
	}
	result.ChildStatus, result.Diagnostic = "uncertain", "child has an unproven effect"
	return result, nil
}

func reconcileBinding(request ReconcileRequest, attempt Attempt) error {
	if request.ParentSessionID == "" || request.ParentCursor.Validate() != nil || request.ParentCursor.JournalKind != protocol.JournalSession || request.ParentCursor.JournalID != protocol.JournalID(request.ParentSessionID) {
		return fmt.Errorf("invalid parent recovery binding")
	}
	if attempt.Manifest.Validate() != nil || attempt.AttemptID == "" || attempt.AttemptID != attempt.Manifest.AttemptID || attempt.Manifest.ParentSessionID != request.ParentSessionID || attempt.Manifest.RuntimeGenerationID != request.Runtime.ID {
		return fmt.Errorf("subagent recovery identity mismatch")
	}
	if request.ParentCursor.CommitSeq < attempt.Manifest.ParentCursor.CommitSeq {
		return fmt.Errorf("parent recovery cursor predates child request")
	}
	return nil
}

func validateReceipt(attempt Attempt) error {
	if attempt.Receipt.Validate() != nil || attempt.Receipt.Manifest != attempt.Manifest || attempt.Receipt.TerminalCursor != attempt.TerminalCursor {
		return fmt.Errorf("child receipt identity is not proven")
	}
	digest, err := canonicaljson.Digest(*attempt.Receipt)
	if err != nil || digest != attempt.ReceiptDigest {
		return fmt.Errorf("child receipt digest is not proven")
	}
	return nil
}

func attachmentMatches(attempt Attempt) bool {
	attachment := attempt.Attachment
	return attachment != nil && attachment.AttemptID == attempt.AttemptID && attachment.ChildSessionID == attempt.Manifest.ChildSessionID && attachment.TerminalCursor == attempt.TerminalCursor && attachment.ReceiptDigest == attempt.ReceiptDigest && attachment.ReceiptEvidenceID != ""
}

func receiptDigest(receipt protocol.SubagentReceiptV1) (protocol.Digest, error) {
	return canonicaljson.Digest(receipt)
}
