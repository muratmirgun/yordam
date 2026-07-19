package context

import (
	stdcontext "context"
	"fmt"
	"io"

	"github.com/muratmirgun/yordam/internal/compaction"
	"github.com/muratmirgun/yordam/internal/evidence"
	"github.com/muratmirgun/yordam/internal/protocol"
)

const MaxCompactionSummaryBytes = 128 * 1024

type SummaryResolver interface {
	ResolveCompactionSummary(stdcontext.Context, protocol.SessionID, protocol.EvidenceID) (protocol.ContentSource, error)
}

// VerifiedSummaryResolver is an additive activation-time contract. Native
// compaction events are active only when the evidence metadata, immutable
// content, range, and deterministic revision all agree.
type VerifiedSummaryResolver interface {
	SummaryResolver
	ResolveVerifiedCompactionSummary(stdcontext.Context, protocol.ContextCompactionReference) (protocol.ContentSource, error)
}

type evidenceSummaryResolver struct {
	store evidence.Store
}

func NewEvidenceSummaryResolver(store evidence.Store) SummaryResolver {
	return &evidenceSummaryResolver{store: store}
}

func (r *evidenceSummaryResolver) ResolveCompactionSummary(ctx stdcontext.Context, sessionID protocol.SessionID, id protocol.EvidenceID) (protocol.ContentSource, error) {
	if r == nil || r.store == nil || sessionID == "" || id == "" {
		return protocol.ContentSource{}, fmt.Errorf("compaction summary resolver is incomplete")
	}
	record, err := r.store.Get(ctx, id)
	if err != nil {
		return protocol.ContentSource{}, fmt.Errorf("get compaction summary evidence: %w", err)
	}
	if record.Body.SessionID != sessionID {
		return protocol.ContentSource{}, fmt.Errorf("compaction summary evidence session mismatch")
	}
	opened, err := r.store.Open(ctx, id)
	if err != nil {
		return protocol.ContentSource{}, fmt.Errorf("open compaction summary evidence: %w", err)
	}
	defer opened.Close()
	content, err := io.ReadAll(io.LimitReader(opened, MaxCompactionSummaryBytes+1))
	if err != nil {
		return protocol.ContentSource{}, fmt.Errorf("read compaction summary evidence: %w", err)
	}
	if len(content) == 0 || len(content) > MaxCompactionSummaryBytes {
		return protocol.ContentSource{}, fmt.Errorf("compaction summary evidence exceeds %d bytes", MaxCompactionSummaryBytes)
	}
	return contentSource(string(id), "compaction_summary", "evidence:"+string(id), []protocol.ContentBlock{{Kind: protocol.ContentText, Text: string(content)}})
}

func (r *evidenceSummaryResolver) ResolveVerifiedCompactionSummary(ctx stdcontext.Context, reference protocol.ContextCompactionReference) (protocol.ContentSource, error) {
	if r == nil || r.store == nil || reference.Validate() != nil {
		return protocol.ContentSource{}, fmt.Errorf("verified compaction summary reference is invalid")
	}
	record, err := r.store.Get(ctx, reference.SummaryEvidenceID)
	if err != nil {
		return protocol.ContentSource{}, fmt.Errorf("get verified compaction evidence: %w", err)
	}
	if record.Body.SessionID != reference.SessionID || record.Body.Kind != "context_summary" || record.Body.Subject.Kind != "context_compaction_summary" {
		return protocol.ContentSource{}, fmt.Errorf("verified compaction evidence identity mismatch")
	}
	binding, err := compaction.DecodeEvidenceBinding(record.Body.Subject.ID)
	if err != nil {
		return protocol.ContentSource{}, fmt.Errorf("decode verified compaction binding: %w", err)
	}
	if binding.From != reference.From || binding.Through != reference.Through {
		return protocol.ContentSource{}, fmt.Errorf("verified compaction range mismatch")
	}
	opened, err := r.store.Open(ctx, reference.SummaryEvidenceID)
	if err != nil {
		return protocol.ContentSource{}, fmt.Errorf("open verified compaction evidence: %w", err)
	}
	defer opened.Close()
	content, err := io.ReadAll(io.LimitReader(opened, MaxCompactionSummaryBytes+1))
	if err != nil {
		return protocol.ContentSource{}, fmt.Errorf("read verified compaction evidence: %w", err)
	}
	if len(content) == 0 || len(content) > MaxCompactionSummaryBytes {
		return protocol.ContentSource{}, fmt.Errorf("verified compaction evidence exceeds %d bytes", MaxCompactionSummaryBytes)
	}
	revision, err := compaction.RevisionForBinding(binding, content)
	if err != nil || revision != reference.Revision {
		return protocol.ContentSource{}, fmt.Errorf("verified compaction revision mismatch")
	}
	return contentSource(string(reference.SummaryEvidenceID), "compaction_summary", "evidence:"+string(reference.SummaryEvidenceID), []protocol.ContentBlock{{Kind: protocol.ContentText, Text: string(content)}})
}
