package skills

import (
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func fmtHex(value []byte) string {
	const hex = "0123456789abcdef"
	result := make([]byte, len(value)*2)
	for i, b := range value {
		result[i*2], result[i*2+1] = hex[b>>4], hex[b&15]
	}
	return string(result)
}
func validTrustCursor() protocol.CommittedCursor { return trustCursor("workspace", 1) }
func trustCursor(workspace protocol.WorkspaceID, sequence uint64) protocol.CommittedCursor {
	return protocol.CommittedCursor{JournalKind: protocol.JournalWorkspaceControl, JournalID: protocol.JournalID(workspace), CommitSeq: sequence, TransactionID: "transaction"}
}

func trustRecord(sequence uint64, workspace protocol.WorkspaceID, digest protocol.Digest, decision string) protocol.EventRecord {
	payload := &protocol.ProjectSkillTrustChangedV1{WorkspaceID: workspace, CatalogDigest: digest, Decision: decision}
	return protocol.EventRecord{Envelope: protocol.EventEnvelope{SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1, JournalKind: protocol.JournalWorkspaceControl, JournalID: protocol.JournalID(workspace), EventID: protocol.EventID("event" + string(rune('0'+sequence))), Seq: sequence, Time: time.Unix(int64(sequence), 0).UTC(), Kind: protocol.EventProjectSkillTrustChanged, TransactionID: "transaction"}, Decoded: payload}
}

func TestTrustProjectorProjectsExactHistoryAndResolvesNewestMatch(t *testing.T) {
	projector := TrustProjector{}
	zero := projector.Zero(protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace"})
	if zero.WorkspaceID != "workspace" || len(zero.Decisions) != 0 {
		t.Fatalf("zero = %#v", zero)
	}
	digest := catalogDigest([]byte("digest"))
	other := catalogDigest([]byte("other"))
	state, err := projector.Apply(zero, trustRecord(1, "workspace", digest, "allow"))
	if err != nil {
		t.Fatal(err)
	}
	state, err = projector.Apply(state, trustRecord(2, "workspace", other, "deny"))
	if err != nil {
		t.Fatal(err)
	}
	state, err = projector.Apply(state, trustRecord(3, "workspace", digest, "deny"))
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Decisions) != 3 {
		t.Fatalf("decisions = %#v", state.Decisions)
	}
	resolved, ok := state.Resolve("workspace", digest)
	if !ok || resolved.Decision != config.ProjectSkillsDeny || resolved.Cursor.CommitSeq != 3 {
		t.Fatalf("resolved = %#v / %v", resolved, ok)
	}
	resolved.Decision = config.ProjectSkillsAllow
	again, _ := state.Resolve("workspace", digest)
	if again.Decision != config.ProjectSkillsDeny {
		t.Fatal("Resolve returned an alias")
	}
}

func TestTrustProjectorRejectsInvalidBindingAndIgnoresUnrelatedEvents(t *testing.T) {
	projector := TrustProjector{}
	state := projector.Zero(protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace"})
	unrelated := trustRecord(1, "workspace", catalogDigest([]byte("digest")), "allow")
	unrelated.Envelope.Kind = protocol.EventRuntimeGenerationActivated
	unrelated.Decoded = &protocol.RuntimeGenerationActivatedV1{}
	updated, err := projector.Apply(state, unrelated)
	if err != nil || len(updated.Decisions) != 0 {
		t.Fatalf("unrelated = %#v / %v", updated, err)
	}
	invalid := trustRecord(1, "workspace", catalogDigest([]byte("digest")), "ask")
	if _, err := projector.Apply(state, invalid); err == nil {
		t.Fatal("unknown decision accepted")
	}
	wrongJournal := trustRecord(1, "workspace", catalogDigest([]byte("digest")), "allow")
	wrongJournal.Envelope.JournalKind = protocol.JournalSession
	if _, err := projector.Apply(state, wrongJournal); err == nil {
		t.Fatal("wrong journal accepted")
	}
	wrongID := trustRecord(1, "workspace", catalogDigest([]byte("digest")), "allow")
	wrongID.Envelope.JournalID = "other"
	if _, err := projector.Apply(state, wrongID); err == nil {
		t.Fatal("wrong journal id accepted")
	}
	badSequence := trustRecord(0, "workspace", catalogDigest([]byte("digest")), "allow")
	if _, err := projector.Apply(state, badSequence); err == nil {
		t.Fatal("invalid event cursor accepted")
	}
	if _, err := projector.Apply(TrustProjection{}, unrelated); err == nil {
		t.Fatal("unbound projection accepted")
	}
	badHistory := state
	badHistory.Decisions = []TrustState{{WorkspaceID: "workspace", CatalogDigest: catalogDigest([]byte("digest")), Decision: config.ProjectSkillsAsk, Cursor: validTrustCursor()}}
	if _, err := projector.Apply(badHistory, unrelated); err == nil {
		t.Fatal("malformed prior history accepted")
	}
}
