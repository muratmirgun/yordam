package context

import (
	"bytes"
	stdcontext "context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/compaction"
	"github.com/muratmirgun/yordam/internal/evidence"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestEvidenceSummaryResolverRejectsTamperedActivationBinding(t *testing.T) {
	binding := verifiedBinding(t)
	content := verifiedSummary(t)
	revision, err := compaction.RevisionForBinding(binding, content)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := compaction.EncodeEvidenceBinding(binding)
	if err != nil {
		t.Fatal(err)
	}
	record := verifiedRecord(t, encoded)
	store := &verifiedStore{record: record, content: content}
	resolver := NewEvidenceSummaryResolver(store).(VerifiedSummaryResolver)
	reference := protocol.ContextCompactionReference{SessionID: "session", From: binding.From, Through: binding.Through, SummaryEvidenceID: record.Body.ID, Revision: revision}
	if _, err := resolver.ResolveVerifiedCompactionSummary(stdcontext.Background(), reference); err != nil {
		t.Fatal(err)
	}

	for name, mutate := range map[string]func(*protocol.ContextCompactionReference, *verifiedStore){
		"revision":            func(r *protocol.ContextCompactionReference, _ *verifiedStore) { r.Revision = strings.Repeat("0", 64) },
		"from":                func(r *protocol.ContextCompactionReference, _ *verifiedStore) { r.From.CommitSeq++ },
		"through transaction": func(r *protocol.ContextCompactionReference, _ *verifiedStore) { r.Through.TransactionID = "other" },
		"source digest metadata": func(_ *protocol.ContextCompactionReference, s *verifiedStore) {
			b := binding
			b.SourceDigest = protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("f", 64)}
			s.rebind(t, b)
		},
		"contract version": func(_ *protocol.ContextCompactionReference, s *verifiedStore) {
			s.record.Body.Subject.ID = "invalid-contract-binding"
		},
		"content": func(_ *protocol.ContextCompactionReference, s *verifiedStore) {
			s.content = []byte(`{"goal":"tampered","constraints":[],"decisions":[],"files":[],"commands_and_tests":[],"unresolved":[],"children":[],"skills":[],"unknown_effects":[]}`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			local := *store
			local.content = append([]byte(nil), store.content...)
			local.record = protocol.DeepCopy(store.record)
			candidate := reference
			mutate(&candidate, &local)
			if _, err := resolverFor(&local).ResolveVerifiedCompactionSummary(stdcontext.Background(), candidate); err == nil {
				t.Fatal("tampered activation accepted")
			}
		})
	}
}

func TestEvidenceSummaryResolverFailsClosedToEarlierValidCompaction(t *testing.T) {
	// Planner-level tests exercise this via a verified resolver whose later
	// evidence fails verification; this test pins the concrete resolver's
	// failure signal used for that fallback.
	binding := verifiedBinding(t)
	content := verifiedSummary(t)
	encoded, _ := compaction.EncodeEvidenceBinding(binding)
	store := &verifiedStore{record: verifiedRecord(t, encoded), content: []byte(`not-json`)}
	revision, _ := compaction.RevisionForBinding(binding, content)
	ref := protocol.ContextCompactionReference{SessionID: "session", From: binding.From, Through: binding.Through, SummaryEvidenceID: store.record.Body.ID, Revision: revision}
	if _, err := resolverFor(store).ResolveVerifiedCompactionSummary(stdcontext.Background(), ref); err == nil {
		t.Fatal("invalid later summary activated")
	}
}

func verifiedBinding(t *testing.T) compaction.EvidenceBinding {
	t.Helper()
	return compaction.EvidenceBinding{ContractVersion: compaction.SummaryContractVersion, From: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "session", CommitSeq: 1, TransactionID: "from-tx"}, Through: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "session", CommitSeq: 2, TransactionID: "through-tx"}, SourceDigest: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("a", 64)}}
}
func verifiedSummary(t *testing.T) []byte {
	t.Helper()
	raw, err := compaction.ParseSummary([]byte(`{"goal":"ok","constraints":[],"decisions":[],"files":[],"commands_and_tests":[],"unresolved":[],"children":[],"skills":[],"unknown_effects":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func verifiedRecord(t *testing.T, subject string) protocol.EvidenceRecord {
	t.Helper()
	body := protocol.EvidenceRecordBody{ID: "summary", Kind: "context_summary", WorkspaceID: "workspace", SessionID: "session", Availability: protocol.ContentAvailable, Blob: &protocol.BlobRef{Digest: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("b", 64)}}, MediaType: "application/json", Size: 1, ProducingActivityID: "activity", Actor: protocol.ActorRef{ID: "orchestrator", Kind: protocol.ActorSystem}, Subject: protocol.SubjectRef{Kind: "context_compaction_summary", ID: subject}, CreatedAt: time.Unix(1, 0).UTC()}
	digest, _ := canonicaljson.Digest(body)
	return protocol.EvidenceRecord{Body: body, Digest: digest}
}

type verifiedStore struct {
	record  protocol.EvidenceRecord
	content []byte
}

func (s *verifiedStore) Get(stdcontext.Context, protocol.EvidenceID) (protocol.EvidenceRecord, error) {
	return protocol.DeepCopy(s.record), nil
}
func (s *verifiedStore) Open(stdcontext.Context, protocol.EvidenceID) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.content)), nil
}
func (s *verifiedStore) rebind(t *testing.T, binding compaction.EvidenceBinding) {
	t.Helper()
	encoded, err := compaction.EncodeEvidenceBinding(binding)
	if err != nil {
		t.Fatal(err)
	}
	s.record.Body.Subject.ID = encoded
}
func resolverFor(store *verifiedStore) VerifiedSummaryResolver {
	return NewEvidenceSummaryResolver(store).(VerifiedSummaryResolver)
}
func (*verifiedStore) Close() error { return nil }
func (*verifiedStore) Put(stdcontext.Context, protocol.EvidenceCandidate) (protocol.EvidenceRecord, error) {
	return protocol.EvidenceRecord{}, errors.New("unexpected")
}
func (*verifiedStore) Verify(stdcontext.Context, protocol.EvidenceID) error { return nil }
func (*verifiedStore) MigrateLegacyArtifact(stdcontext.Context, protocol.SessionID, string) (protocol.EvidenceRecord, error) {
	return protocol.EvidenceRecord{}, errors.New("unexpected")
}
func (*verifiedStore) VerifyReceipt(stdcontext.Context, protocol.VerificationReceipt) error {
	return nil
}
func (*verifiedStore) Diagnostics() []evidence.Diagnostic { return nil }

var _ evidence.Store = (*verifiedStore)(nil)
