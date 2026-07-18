package evidence_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/muratmirgun/yordam/internal/evidence"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
)

func TestTwoSessionsResolveOneImmutableBlob(t *testing.T) {
	root, store := newEvidenceStore(t, secret.NewAdmissionScanner())
	first := putEvidence(t, store, evidenceCandidate("ses-a", "ev-a", []byte("same")))
	second := putEvidence(t, store, evidenceCandidate("ses-b", "ev-b", []byte("same")))
	if first.Body.Blob == nil || second.Body.Blob == nil || first.Body.Blob.Digest != second.Body.Blob.Digest {
		t.Fatalf("records differ: %+v %+v", first, second)
	}
	if got := countNamedFiles(t, root, first.Body.Blob.Digest.Value); got != 1 {
		t.Fatalf("blob count=%d", got)
	}
}

func TestEvidencePreservesCandidateProvenance(t *testing.T) {
	_, store := newEvidenceStore(t, secret.NewAdmissionScanner())
	candidate := evidenceCandidate("ses-a", "ev-a", []byte("result"))
	record := putEvidence(t, store, candidate)
	if record.Body.WorkspaceID != candidate.WorkspaceID || record.Body.SessionID != candidate.SessionID ||
		record.Body.ProducingActivityID != candidate.ProducingActivityID || record.Body.Actor != candidate.Actor ||
		record.Body.Subject != candidate.Subject || record.Body.Kind != candidate.Kind || record.Body.MediaType != candidate.MediaType {
		t.Fatalf("provenance was not preserved: candidate=%+v record=%+v", candidate, record)
	}
}

func TestEvidenceWithholdsEveryRegisteredVariantAndWritesNoSecret(t *testing.T) {
	raw := []byte("zero-on-disk-secret")
	for name, variant := range encodedVariants(raw) {
		t.Run(name, func(t *testing.T) {
			root, store := newEvidenceStore(t, secret.NewAdmissionScanner(raw))
			record := putEvidence(t, store, evidenceCandidate("ses-a", "ev-a", variant))
			if record.Body.Availability != protocol.ContentWithheldSecret || record.Body.Blob != nil || !record.Body.Redacted {
				t.Fatalf("record=%+v", record)
			}
			assertTreeOmits(t, root, variant)
		})
	}
}

func TestEvidenceProjectsMissingBlobAndCannotVerify(t *testing.T) {
	root, store := newEvidenceStore(t, secret.NewAdmissionScanner())
	record := putEvidence(t, store, evidenceCandidate("ses-a", "ev-a", []byte("missing")))
	if record.Body.Blob == nil {
		t.Fatal("available record has no blob")
	}
	if err := os.Remove(blobPath(root, record)); err != nil {
		t.Fatal(err)
	}
	projected, err := store.Get(context.Background(), record.Body.ID)
	if err != nil {
		t.Fatal(err)
	}
	if projected.Body.Availability != protocol.ContentMissing || projected.Body.Blob != nil {
		t.Fatalf("projection=%+v", projected)
	}
	if err := store.Verify(context.Background(), record.Body.ID); !errors.Is(err, evidence.ErrBlobMissing) {
		t.Fatalf("verify error=%v", err)
	}
}

func TestEvidenceDetectsDigestMismatchAndProjectsCorrupt(t *testing.T) {
	root, store := newEvidenceStore(t, secret.NewAdmissionScanner())
	record := putEvidence(t, store, evidenceCandidate("ses-a", "ev-a", []byte("original")))
	if err := os.WriteFile(blobPath(root, record), []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Open(context.Background(), record.Body.ID); !errors.Is(err, evidence.ErrDigestMismatch) {
		t.Fatalf("open error=%v", err)
	}
	projected, err := store.Get(context.Background(), record.Body.ID)
	if err != nil {
		t.Fatal(err)
	}
	if projected.Body.Availability != protocol.ContentCorrupt || projected.Body.Blob != nil {
		t.Fatalf("projection=%+v", projected)
	}
	if err := store.VerifyReceipt(context.Background(), protocol.VerificationReceipt{Body: protocol.VerificationReceiptBody{EvidenceIDs: []protocol.EvidenceID{record.Body.ID}}}); !errors.Is(err, evidence.ErrEvidenceUnavailable) {
		t.Fatalf("receipt verification error=%v", err)
	}
}

func TestEvidenceTruncatesAtRetainedLimit(t *testing.T) {
	_, store := newEvidenceStore(t, secret.NewAdmissionScanner())
	content := bytes.Repeat([]byte("x"), int(evidence.MaxRetainedBytes)+17)
	record := putEvidence(t, store, evidenceCandidate("ses-a", "ev-a", content))
	if !record.Body.Truncated || record.Body.Size != evidence.MaxRetainedBytes || record.Body.OriginalSize != int64(len(content)) {
		t.Fatalf("record=%+v", record)
	}
	opened, err := store.Open(context.Background(), record.Body.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	raw, err := io.ReadAll(opened)
	if err != nil || len(raw) != int(evidence.MaxRetainedBytes) {
		t.Fatalf("read=(%d, %v)", len(raw), err)
	}
}

func TestEvidenceMetadataPublicationDoesNotReplace(t *testing.T) {
	_, store := newEvidenceStore(t, secret.NewAdmissionScanner())
	first := evidenceCandidate("ses-a", "ev-a", []byte("first"))
	putEvidence(t, store, first)
	second := first
	second.Content = []byte("second")
	if _, err := store.Put(context.Background(), second); !errors.Is(err, evidence.ErrEvidenceExists) {
		t.Fatalf("second put error=%v", err)
	}
	opened, err := store.Open(context.Background(), first.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	if raw, err := io.ReadAll(opened); err != nil || string(raw) != "first" {
		t.Fatalf("content=(%q, %v)", raw, err)
	}
}

func TestEvidenceNoReplaceRacePublishesOneRecord(t *testing.T) {
	_, store := newEvidenceStore(t, secret.NewAdmissionScanner())
	start := make(chan struct{})
	errorsByWriter := make(chan error, 2)
	var wait sync.WaitGroup
	for _, content := range [][]byte{[]byte("first"), []byte("second")} {
		content := content
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := store.Put(context.Background(), evidenceCandidate("ses-a", "ev-race", content))
			errorsByWriter <- err
		}()
	}
	close(start)
	wait.Wait()
	close(errorsByWriter)
	var success, exists int
	for err := range errorsByWriter {
		switch {
		case err == nil:
			success++
		case errors.Is(err, evidence.ErrEvidenceExists):
			exists++
		default:
			t.Fatalf("unexpected put error: %v", err)
		}
	}
	if success != 1 || exists != 1 {
		t.Fatalf("success=%d exists=%d", success, exists)
	}
}

func TestEvidenceRejectsBlobSymlinkSubstitution(t *testing.T) {
	root, store := newEvidenceStore(t, secret.NewAdmissionScanner())
	record := putEvidence(t, store, evidenceCandidate("ses-a", "ev-a", []byte("original")))
	path := blobPath(root, record)
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, path); err != nil {
		t.Fatal(err)
	}
	if err := store.Verify(context.Background(), record.Body.ID); !errors.Is(err, evidence.ErrUnsafePath) {
		t.Fatalf("verify error=%v", err)
	}
}

func newEvidenceStore(t *testing.T, scanner *secret.AdmissionScanner) (string, evidence.Store) {
	t.Helper()
	root := t.TempDir()
	store, err := evidence.New(root, scanner)
	if err != nil {
		t.Fatal(err)
	}
	return root, store
}

func evidenceCandidate(session, id string, content []byte) protocol.EvidenceCandidate {
	return protocol.EvidenceCandidate{
		ID: protocol.EvidenceID(id), Kind: "tool_output", WorkspaceID: "workspace-a", SessionID: protocol.SessionID(session),
		MediaType: "text/plain", ProducingActivityID: "activity-a",
		Actor:   protocol.ActorRef{ID: "tool-a", Kind: protocol.ActorTool, Source: "builtin"},
		Subject: protocol.SubjectRef{Kind: "file", ID: "result.txt"}, Content: append([]byte(nil), content...),
	}
}

func putEvidence(t *testing.T, store evidence.Store, candidate protocol.EvidenceCandidate) protocol.EvidenceRecord {
	t.Helper()
	record, err := store.Put(context.Background(), candidate)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func blobPath(root string, record protocol.EvidenceRecord) string {
	return filepath.Join(root, "workspaces", string(record.Body.WorkspaceID), "evidence", "blobs", "sha256", record.Body.Blob.Digest.Value)
}

func countNamedFiles(t *testing.T, root, name string) int {
	t.Helper()
	count := 0
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() && entry.Name() == name {
			count++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return count
}

func assertTreeOmits(t *testing.T, root string, value []byte) {
	t.Helper()
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || !entry.Type().IsRegular() {
			return err
		}
		raw, err := os.ReadFile(path)
		if err == nil && bytes.Contains(raw, value) {
			t.Fatalf("secret %x found in %s", value, path)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func encodedVariants(raw []byte) map[string][]byte {
	return map[string][]byte{
		"raw":              raw,
		"base64-padded":    []byte(base64.StdEncoding.EncodeToString(raw)),
		"base64-raw":       []byte(base64.RawStdEncoding.EncodeToString(raw)),
		"base64url-padded": []byte(base64.URLEncoding.EncodeToString(raw)),
		"base64url-raw":    []byte(base64.RawURLEncoding.EncodeToString(raw)),
		"hex-lower":        []byte(hex.EncodeToString(raw)),
		"hex-upper":        []byte(stringUpper(hex.EncodeToString(raw))),
	}
}

func stringUpper(value string) string {
	buffer := []byte(value)
	for index, character := range buffer {
		if character >= 'a' && character <= 'f' {
			buffer[index] = character - ('a' - 'A')
		}
	}
	return string(buffer)
}
