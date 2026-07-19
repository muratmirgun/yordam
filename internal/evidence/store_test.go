package evidence_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/muratmirgun/yordam/internal/evidence"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
)

func TestTwoSessionsResolveOneImmutableBlob(t *testing.T) {
	root, store := newEvidenceStore(t)
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
	_, store := newEvidenceStore(t)
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
			root, store := newEvidenceStore(t, raw)
			record := putEvidence(t, store, evidenceCandidate("ses-a", "ev-a", variant))
			if record.Body.Availability != protocol.ContentWithheldSecret || record.Body.Blob != nil || !record.Body.Redacted {
				t.Fatalf("record=%+v", record)
			}
			assertTreeOmits(t, root, variant)
		})
	}
}

func TestEvidenceProjectsMissingBlobAndCannotVerify(t *testing.T) {
	root, store := newEvidenceStore(t)
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
	root, store := newEvidenceStore(t)
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
	_, store := newEvidenceStore(t)
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
	_, store := newEvidenceStore(t)
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
	_, store := newEvidenceStore(t)
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
	root, store := newEvidenceStore(t)
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

func TestEvidenceRejectsSecretMetadataAndInvalidBoundsBeforeCreatingRoot(t *testing.T) {
	for name, mutate := range map[string]func(*protocol.EvidenceCandidate){
		"identity_secret": func(candidate *protocol.EvidenceCandidate) { candidate.ID = "aWRlbnRpdHktc2VjcmV0" },
		"kind_secret":     func(candidate *protocol.EvidenceCandidate) { candidate.Kind = "aWRlbnRpdHktc2VjcmV0" },
		"actor_secret":    func(candidate *protocol.EvidenceCandidate) { candidate.Actor.ID = "aWRlbnRpdHktc2VjcmV0" },
		"subject_secret":  func(candidate *protocol.EvidenceCandidate) { candidate.Subject.ID = "aWRlbnRpdHktc2VjcmV0" },
		"invalid_bounds": func(candidate *protocol.EvidenceCandidate) {
			candidate.MediaType = strings.Repeat("m", protocol.MaxStringBytes+1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "not-created")
			registry := secret.NewRegistry()
			lease, err := registry.Acquire("evidence-metadata", [][]byte{[]byte("identity-secret")})
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Close()
			store, err := evidence.New(root, lease)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			candidate := evidenceCandidate("ses-a", "ev-a", bytes.Repeat([]byte("x"), 4<<20))
			mutate(&candidate)
			if _, err := store.Put(context.Background(), candidate); err == nil {
				t.Fatal("invalid or secret metadata was accepted")
			}
			if _, err := os.Stat(root); !os.IsNotExist(err) {
				t.Fatalf("rejected metadata created storage root: %v", err)
			}
		})
	}
}

func TestEvidenceAdmitsFinalCanonicalRecordBeforeAnyPublication(t *testing.T) {
	content := []byte("final-record-content")
	contentDigest := sha256.Sum256(content)
	for name, generatedOnly := range map[string][]byte{
		"created_at":   []byte(`"created_at"`),
		"availability": []byte(`"availability"`),
		"blob_digest":  []byte(hex.EncodeToString(contentDigest[:])),
	} {
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "not-created")
			registry := secret.NewRegistry()
			lease, err := registry.Acquire("evidence-final", [][]byte{generatedOnly})
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Close()
			store, err := evidence.New(root, lease)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if _, err := store.Put(context.Background(), evidenceCandidate("ses-a", "final-a", content)); !errors.Is(err, secret.ErrSecretDetected) {
				t.Fatalf("final record admission error=%v", err)
			}
			if _, err := os.Stat(root); !os.IsNotExist(err) {
				t.Fatalf("rejected final record created storage: %v", err)
			}
		})
	}
}

func TestEvidenceFailsAfterPinnedRootOrParentReplacement(t *testing.T) {
	for _, target := range []string{"root", "parent"} {
		t.Run(target, func(t *testing.T) {
			parent := t.TempDir()
			root := filepath.Join(parent, "evidence")
			registry := secret.NewRegistry()
			lease, err := registry.Acquire("evidence-root", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Close()
			store, err := evidence.New(root, lease)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			putEvidence(t, store, evidenceCandidate("ses-a", "root-a", []byte("first")))
			if target == "root" {
				if err := os.Rename(root, root+"-moved"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(root, 0o700); err != nil {
					t.Fatal(err)
				}
			} else {
				moved := parent + "-moved"
				if err := os.Rename(parent, moved); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(parent, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(filepath.Join(parent, "evidence"), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := store.Put(context.Background(), evidenceCandidate("ses-a", "root-b", []byte("second"))); !errors.Is(err, evidence.ErrUnsafePath) {
				t.Fatalf("replacement error=%v", err)
			}
		})
	}
}

func TestEvidencePinsExistingRootOrDeepestAncestorAtConstruction(t *testing.T) {
	for _, target := range []string{"existing_root", "deepest_existing_ancestor"} {
		t.Run(target, func(t *testing.T) {
			base := t.TempDir()
			anchor := filepath.Join(base, "anchor")
			if err := os.Mkdir(anchor, 0o700); err != nil {
				t.Fatal(err)
			}
			root := anchor
			if target == "deepest_existing_ancestor" {
				root = filepath.Join(anchor, "missing", "evidence")
			}
			registry := secret.NewRegistry()
			lease, err := registry.Acquire("evidence-construction-anchor", nil)
			if err != nil {
				t.Fatal(err)
			}
			store, err := evidence.New(root, lease)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if target == "deepest_existing_ancestor" {
				if _, err := os.Stat(root); !os.IsNotExist(err) {
					t.Fatalf("constructor created confidential root: %v", err)
				}
			}
			moved := anchor + "-retained"
			if err := os.Rename(anchor, moved); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(anchor, 0o700); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Put(context.Background(), evidenceCandidate("ses-a", "construction-swap", []byte("confidential"))); !errors.Is(err, evidence.ErrUnsafePath) {
				t.Fatalf("construction identity replacement error=%v", err)
			}
			for _, candidateRoot := range []string{root, filepath.Join(moved, "missing", "evidence")} {
				if raw, err := os.ReadDir(candidateRoot); err == nil && len(raw) != 0 {
					t.Fatalf("rejected put populated %q", candidateRoot)
				} else if err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestEvidenceRejectsInRootSymlinkSwapOfPinnedDescendant(t *testing.T) {
	for _, test := range []struct {
		name     string
		relative func(protocol.EvidenceRecord) string
		access   func(context.Context, evidence.Store, protocol.EvidenceRecord) error
	}{
		{
			name: "records",
			relative: func(protocol.EvidenceRecord) string {
				return filepath.Join("evidence", "records")
			},
			access: func(ctx context.Context, store evidence.Store, record protocol.EvidenceRecord) error {
				_, err := store.Get(ctx, record.Body.ID)
				return err
			},
		},
		{
			name: "dynamic_workspace",
			relative: func(record protocol.EvidenceRecord) string {
				return filepath.Join("workspaces", string(record.Body.WorkspaceID))
			},
			access: func(ctx context.Context, store evidence.Store, record protocol.EvidenceRecord) error {
				opened, err := store.Open(ctx, record.Body.ID)
				if opened != nil {
					_ = opened.Close()
				}
				return err
			},
		},
		{
			name: "blob_directory",
			relative: func(record protocol.EvidenceRecord) string {
				return filepath.Join("workspaces", string(record.Body.WorkspaceID), "evidence", "blobs", "sha256")
			},
			access: func(ctx context.Context, store evidence.Store, record protocol.EvidenceRecord) error {
				opened, err := store.Open(ctx, record.Body.ID)
				if opened != nil {
					_ = opened.Close()
				}
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root, store := newEvidenceStore(t)
			record := putEvidence(t, store, evidenceCandidate("ses-a", "descendant-"+test.name, []byte("content")))
			relative := test.relative(record)
			original := filepath.Join(root, relative)
			retained := original + "-retained"
			if err := os.Rename(original, retained); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Base(retained), original); err != nil {
				t.Fatal(err)
			}
			if err := test.access(context.Background(), store, record); !errors.Is(err, evidence.ErrUnsafePath) {
				t.Fatalf("existing store descendant swap error=%v", err)
			}

			registry := secret.NewRegistry()
			lease, err := registry.Acquire("evidence-fresh-descendant", nil)
			if err != nil {
				t.Fatal(err)
			}
			fresh, err := evidence.New(root, lease)
			if err != nil {
				t.Fatal(err)
			}
			defer fresh.Close()
			if err := test.access(context.Background(), fresh, record); !errors.Is(err, evidence.ErrUnsafePath) {
				t.Fatalf("fresh store descendant swap error=%v", err)
			}
		})
	}
}

func TestEvidencePublishNeverUsesReplacementDescendantAtBoundary(t *testing.T) {
	for _, target := range []string{"blob", "record"} {
		for _, lifecycle := range []string{"existing_store", "fresh_store"} {
			t.Run(target+"/"+lifecycle, func(t *testing.T) {
				root := filepath.Join(t.TempDir(), "evidence")
				if lifecycle == "fresh_store" {
					baseline := newEvidenceStoreAtRoot(t, root)
					putEvidence(t, baseline, evidenceCandidate("ses-a", "baseline-"+target, []byte("baseline-content")))
					if err := baseline.Close(); err != nil {
						t.Fatal(err)
					}
				}
				publication := 0
				var replacementTemporary string
				store := newEvidenceStoreAtRoot(t, root, evidence.WithFault(func(point evidence.FaultPoint) error {
					if point != evidence.FaultBeforePublish {
						return nil
					}
					publication++
					wanted := 1
					directory := filepath.Join(root, "workspaces", "workspace-a", "evidence", "blobs", "sha256")
					if target == "record" {
						wanted = 2
						directory = filepath.Join(root, "evidence", "records")
					}
					if publication != wanted {
						return nil
					}
					retained := directory + "-retained"
					if err := os.Rename(directory, retained); err != nil {
						return err
					}
					if err := os.Mkdir(directory, 0o700); err != nil {
						return err
					}
					temporary, err := onlyEvidenceTemporary(retained)
					if err != nil {
						return err
					}
					replacementTemporary = filepath.Join(directory, temporary)
					return os.WriteFile(replacementTemporary, []byte("replacement-path-content"), 0o600)
				}))
				_, putErr := store.Put(context.Background(), evidenceCandidate("ses-a", "boundary-"+target+"-"+lifecycle, []byte("boundary-content-"+lifecycle)))
				if !errors.Is(putErr, evidence.ErrUnsafePath) {
					t.Fatalf("boundary replacement error=%v", putErr)
				}
				if raw, err := os.ReadFile(replacementTemporary); err != nil || string(raw) != "replacement-path-content" {
					t.Fatalf("operation touched replacement temporary: raw=%q err=%v", raw, err)
				}
			})
		}
	}
}

func newEvidenceStoreAtRoot(t *testing.T, root string, options ...evidence.Option) evidence.Store {
	t.Helper()
	registry := secret.NewRegistry()
	lease, err := registry.Acquire(protocol.RuntimeGenerationID("evidence-root-"+filepath.Base(t.TempDir())), nil)
	if err != nil {
		t.Fatal(err)
	}
	store, err := evidence.New(root, lease, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = lease.Close() })
	return store
}

func onlyEvidenceTemporary(directory string) (string, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return "", err
	}
	var matched string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".publish-") && strings.HasSuffix(entry.Name(), ".tmp") {
			if matched != "" {
				return "", errors.New("multiple evidence temporaries")
			}
			matched = entry.Name()
		}
	}
	if matched == "" {
		return "", errors.New("evidence temporary not found")
	}
	return matched, nil
}

func newEvidenceStore(t *testing.T, values ...[]byte) (string, evidence.Store) {
	t.Helper()
	root := t.TempDir()
	registry := secret.NewRegistry()
	lease, err := registry.Acquire("evidence-test", values)
	if err != nil {
		t.Fatal(err)
	}
	store, err := evidence.New(root, lease)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = lease.Close() })
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
