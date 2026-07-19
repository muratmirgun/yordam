package eventcodec_test

import (
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/eventcodec"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestFoundationRegistryRejectsInvalidNativeCompactionPayloads(t *testing.T) {
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	valid := compactionPayload("session", 1, 2)
	for name, mutate := range map[string]func(*protocol.ContextCompactedV1, *protocol.EventEnvelope){
		"empty evidence":      func(p *protocol.ContextCompactedV1, _ *protocol.EventEnvelope) { p.SummaryEvidenceID = "" },
		"empty revision":      func(p *protocol.ContextCompactedV1, _ *protocol.EventEnvelope) { p.Revision = "" },
		"other session range": func(p *protocol.ContextCompactedV1, _ *protocol.EventEnvelope) { p.Through.JournalID = "other" },
		"zero cursor":         func(p *protocol.ContextCompactedV1, _ *protocol.EventEnvelope) { p.From.CommitSeq = 0 },
		"reversed range":      func(p *protocol.ContextCompactedV1, _ *protocol.EventEnvelope) { p.From.CommitSeq = 3 },
		"through at event":    func(_ *protocol.ContextCompactedV1, e *protocol.EventEnvelope) { e.Seq = 2 },
		"workspace cursor": func(p *protocol.ContextCompactedV1, _ *protocol.EventEnvelope) {
			p.From.JournalKind = protocol.JournalWorkspaceControl
		},
	} {
		t.Run(name, func(t *testing.T) {
			payload := valid
			event := compactionEnvelope(3)
			mutate(&payload, &event)
			if _, err := registry.Decode(envelopeFor(t, event, payload)); err == nil {
				t.Fatal("invalid compaction accepted")
			}
		})
	}

	oversized := compactionPayload("session", 1, 2)
	oversized.Revision = strings.Repeat("x", protocol.MaxStringBytes+1)
	if _, err := registry.Decode(envelopeFor(t, compactionEnvelope(3), oversized)); err == nil {
		t.Fatal("oversized compaction accepted")
	}
	if _, err := registry.Decode(envelopeFor(t, compactionEnvelope(3), valid)); err != nil {
		t.Fatalf("valid legacy-compatible compaction rejected: %v", err)
	}
}

func TestContextCompactionReferenceBindsSessionAndExactRange(t *testing.T) {
	payload := compactionPayload("session", 1, 2)
	reference := protocol.ContextCompactionReference{SessionID: "session", From: payload.From, Through: payload.Through, SummaryEvidenceID: "summary", Revision: "r1"}
	if err := reference.Validate(); err != nil {
		t.Fatal(err)
	}
	reference.Through.JournalID = "other"
	if err := reference.Validate(); err == nil {
		t.Fatal("cross-session compaction range accepted")
	}
}

func compactionPayload(session protocol.SessionID, from, through uint64) protocol.ContextCompactedV1 {
	cursor := func(sequence uint64) protocol.CommittedCursor {
		return protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(session), CommitSeq: sequence, TransactionID: "transaction"}
	}
	return protocol.ContextCompactedV1{From: cursor(from), Through: cursor(through), SummaryEvidenceID: "summary", Revision: "r1"}
}

func compactionEnvelope(sequence uint64) protocol.EventEnvelope {
	return protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", EventID: "compact", Seq: sequence, Time: time.Unix(1, 0).UTC(), Kind: protocol.EventContextCompacted, TransactionID: "transaction"}
}
