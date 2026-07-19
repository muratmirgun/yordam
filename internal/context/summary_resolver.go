package context

import (
	stdcontext "context"
	"fmt"
	"io"

	"github.com/muratmirgun/yordam/internal/evidence"
	"github.com/muratmirgun/yordam/internal/protocol"
)

const MaxCompactionSummaryBytes = 128 * 1024

type SummaryResolver interface {
	ResolveCompactionSummary(stdcontext.Context, protocol.SessionID, protocol.EvidenceID) (protocol.ContentSource, error)
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
