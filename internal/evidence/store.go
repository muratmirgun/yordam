package evidence

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/rootanchor"
	"github.com/muratmirgun/yordam/internal/safefile"
	"github.com/muratmirgun/yordam/internal/secret"
)

const MaxRetainedBytes int64 = 10 << 20

var (
	ErrEvidenceExists      = errors.New("evidence already exists")
	ErrEvidenceNotFound    = errors.New("evidence not found")
	ErrEvidenceUnavailable = errors.New("evidence is unavailable")
	ErrBlobMissing         = errors.New("evidence blob is missing")
	ErrDigestMismatch      = errors.New("evidence blob digest mismatch")
	ErrUnsafePath          = errors.New("unsafe evidence path")
)

type Store interface {
	Close() error
	Put(context.Context, protocol.EvidenceCandidate) (protocol.EvidenceRecord, error)
	Get(context.Context, protocol.EvidenceID) (protocol.EvidenceRecord, error)
	Open(context.Context, protocol.EvidenceID) (io.ReadCloser, error)
	Verify(context.Context, protocol.EvidenceID) error
	MigrateLegacyArtifact(context.Context, protocol.SessionID, string) (protocol.EvidenceRecord, error)
	VerifyReceipt(context.Context, protocol.VerificationReceipt) error
	Diagnostics() []Diagnostic
}

type Diagnostic struct {
	EvidenceID   protocol.EvidenceID
	Availability protocol.ContentAvailability
	Message      string
}

type LegacyResolver interface {
	ResolveLegacyArtifact(context.Context, protocol.SessionID, string) (protocol.WorkspaceID, []byte, error)
}

type Option func(*storeOptions) error

type storeOptions struct {
	legacy LegacyResolver
}

func WithLegacyResolver(resolver LegacyResolver) Option {
	return func(options *storeOptions) error {
		if resolver == nil {
			return fmt.Errorf("legacy resolver is nil")
		}
		options.legacy = resolver
		return nil
	}
}

type fileStore struct {
	root         string
	admission    *secret.Lease
	clock        func() time.Time
	rootMu       sync.Mutex
	rootAnchor   *rootanchor.Anchor
	closed       bool
	legacy       LegacyResolver
	diagnosticMu sync.Mutex
	diagnostics  []Diagnostic
}

func New(root string, admission *secret.Lease, configured ...Option) (Store, error) {
	if admission == nil {
		return nil, fmt.Errorf("generation admission lease is required")
	}
	owned, err := admission.Derive()
	if err != nil {
		return nil, fmt.Errorf("derive evidence admission lease: %w", err)
	}
	options := storeOptions{}
	for _, configure := range configured {
		if configure == nil {
			_ = owned.Close()
			return nil, fmt.Errorf("evidence option is nil")
		}
		if err := configure(&options); err != nil {
			_ = owned.Close()
			return nil, err
		}
	}
	anchor, err := rootanchor.New(root, ErrUnsafePath)
	if err != nil {
		_ = owned.Close()
		return nil, err
	}
	return &fileStore{root: anchor.Target(), admission: owned, clock: time.Now, legacy: options.legacy, rootAnchor: anchor}, nil
}

func (s *fileStore) Close() error {
	if s == nil {
		return nil
	}
	s.rootMu.Lock()
	if s.closed {
		s.rootMu.Unlock()
		return nil
	}
	s.closed = true
	anchor := s.rootAnchor
	s.rootAnchor = nil
	s.rootMu.Unlock()
	var rootErr error
	if anchor != nil {
		rootErr = anchor.Close()
	}
	return errors.Join(rootErr, s.admission.Close())
}

func (s *fileStore) Put(ctx context.Context, candidate protocol.EvidenceCandidate) (protocol.EvidenceRecord, error) {
	return s.put(ctx, candidate, nil)
}

func (s *fileStore) put(ctx context.Context, candidate protocol.EvidenceCandidate, aliases []string) (protocol.EvidenceRecord, error) {
	if err := validateCandidate(candidate); err != nil {
		return protocol.EvidenceRecord{}, err
	}
	metadata := candidate
	metadata.Content = nil
	metadataRaw, err := canonicaljson.Marshal(struct {
		Candidate protocol.EvidenceCandidate
		Aliases   []string
	}{metadata, append([]string(nil), aliases...)})
	if err != nil {
		return protocol.EvidenceRecord{}, err
	}
	if err := s.admission.Admit(metadataRaw); err != nil {
		return protocol.EvidenceRecord{}, fmt.Errorf("admit evidence metadata: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return protocol.EvidenceRecord{}, err
	}
	originalSize := int64(len(candidate.Content))
	limit := candidate.Limit
	if limit <= 0 || limit > MaxRetainedBytes {
		limit = MaxRetainedBytes
	}
	retainedSize := int64(len(candidate.Content))
	if retainedSize > limit {
		retainedSize = limit
	}
	retained := bytes.Clone(candidate.Content[:retainedSize])
	truncated := originalSize > limit
	body := protocol.EvidenceRecordBody{
		ID: candidate.ID, Kind: candidate.Kind, WorkspaceID: candidate.WorkspaceID, SessionID: candidate.SessionID,
		MediaType: candidate.MediaType, Size: int64(len(retained)), ProducingActivityID: candidate.ProducingActivityID,
		Actor: protocol.DeepCopy(candidate.Actor), Subject: protocol.DeepCopy(candidate.Subject), CreatedAt: s.clock().UTC(),
		Truncated: truncated, LegacyArtifactAliases: append([]string(nil), aliases...),
	}
	if truncated {
		body.OriginalSize = originalSize
	}
	contentAdmissionErr := s.admission.Admit(candidate.Content)
	if errors.Is(contentAdmissionErr, secret.ErrSecretDetected) {
		body.Availability = protocol.ContentWithheldSecret
		body.Redacted = true
	} else {
		if contentAdmissionErr != nil {
			return protocol.EvidenceRecord{}, contentAdmissionErr
		}
		digest := digestBytes(retained)
		body.Availability = protocol.ContentAvailable
		body.Blob = &protocol.BlobRef{Digest: digest}
	}
	if err := body.Validate(); err != nil {
		return protocol.EvidenceRecord{}, err
	}
	recordDigest, err := canonicaljson.Digest(body)
	if err != nil {
		return protocol.EvidenceRecord{}, err
	}
	record := protocol.EvidenceRecord{Body: body, Digest: recordDigest}
	if err := record.Validate(); err != nil {
		return protocol.EvidenceRecord{}, err
	}
	finalRecord, err := canonicaljson.Marshal(record)
	if err != nil {
		return protocol.EvidenceRecord{}, err
	}
	if err := s.admission.Admit(finalRecord); err != nil {
		return protocol.EvidenceRecord{}, fmt.Errorf("admit final evidence record: %w", err)
	}
	if body.Availability == protocol.ContentAvailable {
		if err := s.publishBlob(ctx, candidate.WorkspaceID, body.Blob.Digest, retained); err != nil {
			return protocol.EvidenceRecord{}, fmt.Errorf("publish evidence blob: %w", err)
		}
	}
	if err := s.publishRecord(ctx, record); err != nil {
		return protocol.EvidenceRecord{}, fmt.Errorf("publish evidence metadata: %w", err)
	}
	return protocol.DeepCopy(record), nil
}

func (s *fileStore) Get(ctx context.Context, id protocol.EvidenceID) (protocol.EvidenceRecord, error) {
	record, err := s.readRecord(ctx, id)
	if err != nil {
		return protocol.EvidenceRecord{}, err
	}
	if record.Body.Availability != protocol.ContentAvailable {
		return record, nil
	}
	_, err = s.readVerifiedBlob(ctx, record)
	if err == nil {
		return record, nil
	}
	availability := protocol.ContentCorrupt
	if errors.Is(err, ErrBlobMissing) {
		availability = protocol.ContentMissing
	}
	s.recordDiagnostic(id, availability, err.Error())
	return projectUnavailable(record, availability)
}

func (s *fileStore) Open(ctx context.Context, id protocol.EvidenceID) (io.ReadCloser, error) {
	record, err := s.readRecord(ctx, id)
	if err != nil {
		return nil, err
	}
	if record.Body.Availability != protocol.ContentAvailable || record.Body.Blob == nil {
		return nil, ErrEvidenceUnavailable
	}
	raw, err := s.readVerifiedBlob(ctx, record)
	if err != nil {
		availability := protocol.ContentCorrupt
		if errors.Is(err, ErrBlobMissing) {
			availability = protocol.ContentMissing
		}
		s.recordDiagnostic(id, availability, err.Error())
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(raw)), nil
}

func (s *fileStore) Verify(ctx context.Context, id protocol.EvidenceID) error {
	opened, err := s.Open(ctx, id)
	if err != nil {
		return err
	}
	return opened.Close()
}

func (s *fileStore) VerifyReceipt(ctx context.Context, receipt protocol.VerificationReceipt) error {
	for _, id := range receipt.Body.EvidenceIDs {
		if err := s.Verify(ctx, id); err != nil {
			return fmt.Errorf("%w: evidence %q: %v", ErrEvidenceUnavailable, id, err)
		}
	}
	return nil
}

func (s *fileStore) Diagnostics() []Diagnostic {
	s.diagnosticMu.Lock()
	defer s.diagnosticMu.Unlock()
	return append([]Diagnostic(nil), s.diagnostics...)
}

func (s *fileStore) recordDiagnostic(id protocol.EvidenceID, availability protocol.ContentAvailability, message string) {
	s.diagnosticMu.Lock()
	defer s.diagnosticMu.Unlock()
	s.diagnostics = append(s.diagnostics, Diagnostic{EvidenceID: id, Availability: availability, Message: message})
}

func (s *fileStore) publishBlob(ctx context.Context, workspace protocol.WorkspaceID, digest protocol.Digest, content []byte) error {
	directory := filepath.Join("workspaces", string(workspace), "evidence", "blobs", "sha256")
	if err := s.ensureDirectory(ctx, directory, 0o700); err != nil {
		return fmt.Errorf("prepare blob directory: %w", err)
	}
	root, err := s.pinnedRoot(ctx, false)
	if err != nil {
		return err
	}
	path := filepath.Join(directory, digest.Value)
	if raw, err := s.readRegularRoot(ctx, root, path, MaxRetainedBytes); err == nil {
		if digestBytes(raw) != digest {
			return ErrDigestMismatch
		}
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect existing blob: %w", err)
	}
	created, err := publishNoReplace(ctx, root, path, content, 0o600)
	if errors.Is(err, os.ErrExist) {
		raw, readErr := s.readRegularRoot(ctx, root, path, MaxRetainedBytes)
		if readErr != nil {
			return readErr
		}
		if digestBytes(raw) != digest {
			return ErrDigestMismatch
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("publish no-replace blob: %w", err)
	}
	if !created {
		return fmt.Errorf("blob publication made no progress")
	}
	return s.verifyRootIdentity()
}

func (s *fileStore) publishRecord(ctx context.Context, record protocol.EvidenceRecord) error {
	directory := filepath.Join("evidence", "records")
	if err := s.ensureDirectory(ctx, directory, 0o700); err != nil {
		return err
	}
	raw, err := canonicaljson.Marshal(record)
	if err != nil {
		return err
	}
	root, err := s.pinnedRoot(ctx, false)
	if err != nil {
		return err
	}
	name := filepath.Join(directory, string(record.Body.ID)+".json")
	_, err = publishNoReplace(ctx, root, name, raw, 0o600)
	if errors.Is(err, os.ErrExist) {
		info, statErr := root.Lstat(name)
		if statErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return errors.Join(ErrUnsafePath, statErr)
		}
		return errors.Join(ErrEvidenceExists, s.verifyRootIdentity())
	}
	return errors.Join(err, s.verifyRootIdentity())
}

func (s *fileStore) readRecord(ctx context.Context, id protocol.EvidenceID) (protocol.EvidenceRecord, error) {
	if !safeName(string(id)) {
		return protocol.EvidenceRecord{}, ErrUnsafePath
	}
	root, err := s.pinnedRoot(ctx, false)
	if os.IsNotExist(err) {
		return protocol.EvidenceRecord{}, ErrEvidenceNotFound
	}
	if err != nil {
		return protocol.EvidenceRecord{}, err
	}
	path := filepath.Join("evidence", "records", string(id)+".json")
	raw, err := s.readRegularRoot(ctx, root, path, protocol.MaxEventBytes)
	if os.IsNotExist(err) {
		return protocol.EvidenceRecord{}, ErrEvidenceNotFound
	}
	if err != nil {
		return protocol.EvidenceRecord{}, err
	}
	var record protocol.EvidenceRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return protocol.EvidenceRecord{}, fmt.Errorf("decode evidence metadata: %w", err)
	}
	if err := record.Validate(); err != nil {
		return protocol.EvidenceRecord{}, fmt.Errorf("validate evidence metadata: %w", err)
	}
	if err := canonicaljson.ValidateDigest(record.Body, record.Digest); err != nil {
		return protocol.EvidenceRecord{}, fmt.Errorf("validate evidence metadata digest: %w", err)
	}
	if record.Body.ID != id {
		return protocol.EvidenceRecord{}, fmt.Errorf("evidence metadata identity mismatch")
	}
	return protocol.DeepCopy(record), nil
}

func (s *fileStore) readVerifiedBlob(ctx context.Context, record protocol.EvidenceRecord) ([]byte, error) {
	if record.Body.Blob == nil {
		return nil, ErrEvidenceUnavailable
	}
	root, err := s.pinnedRoot(ctx, false)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrBlobMissing
		}
		return nil, err
	}
	path := filepath.Join("workspaces", string(record.Body.WorkspaceID), "evidence", "blobs", "sha256", record.Body.Blob.Digest.Value)
	raw, err := s.readRegularRoot(ctx, root, path, MaxRetainedBytes)
	if os.IsNotExist(err) {
		return nil, ErrBlobMissing
	}
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) != record.Body.Size || digestBytes(raw) != record.Body.Blob.Digest {
		return nil, ErrDigestMismatch
	}
	return raw, nil
}

func (s *fileStore) pinnedRoot(ctx context.Context, create bool) (*os.Root, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.rootMu.Lock()
	defer s.rootMu.Unlock()
	if s.closed {
		return nil, secret.ErrLeaseClosed
	}
	if s.rootAnchor == nil {
		return nil, ErrUnsafePath
	}
	return s.rootAnchor.Open(ctx, create)
}

func (s *fileStore) ensureDirectory(ctx context.Context, relative string, mode os.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	root, err := s.pinnedRoot(ctx, true)
	if err != nil {
		return err
	}
	cursor := ""
	for _, part := range strings.Split(filepath.Clean(relative), string(filepath.Separator)) {
		if !safeName(part) {
			return ErrUnsafePath
		}
		cursor = filepath.Join(cursor, part)
		info, statErr := root.Lstat(cursor)
		created := false
		if os.IsNotExist(statErr) {
			if err := root.Mkdir(cursor, mode); err != nil && !os.IsExist(err) {
				return err
			} else if err == nil {
				created = true
			}
			info, statErr = root.Lstat(cursor)
		}
		if statErr != nil {
			return statErr
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return ErrUnsafePath
		}
		if created {
			parent := filepath.Dir(cursor)
			if parent == "" {
				parent = "."
			}
			if err := syncRootDirectory(root, parent); err != nil {
				return err
			}
		}
		if err := s.pinDirectory(cursor); err != nil {
			return err
		}
	}
	return errors.Join(syncRootDirectory(root, relative), s.verifyRootIdentity())
}

func (s *fileStore) pinDirectory(relative string) error {
	s.rootMu.Lock()
	defer s.rootMu.Unlock()
	if s.closed || s.rootAnchor == nil {
		return secret.ErrLeaseClosed
	}
	return s.rootAnchor.PinDirectory(relative)
}

func (s *fileStore) verifyRootIdentity() error {
	s.rootMu.Lock()
	defer s.rootMu.Unlock()
	if s.closed || s.rootAnchor == nil {
		return secret.ErrLeaseClosed
	}
	return s.rootAnchor.Verify()
}

func (s *fileStore) readRegularRoot(ctx context.Context, root *os.Root, path string, limit int64) ([]byte, error) {
	s.rootMu.Lock()
	if s.closed || s.rootAnchor == nil {
		s.rootMu.Unlock()
		return nil, secret.ErrLeaseClosed
	}
	err := s.rootAnchor.VerifyRelative(path)
	s.rootMu.Unlock()
	if err != nil {
		return nil, err
	}
	raw, readErr := readRegularRoot(ctx, root, path, limit)
	return raw, errors.Join(readErr, s.verifyRootIdentity())
}

func syncRootDirectory(root *os.Root, name string) error {
	directory, err := root.Open(name)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func publishNoReplace(ctx context.Context, root *os.Root, final string, content []byte, mode os.FileMode) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !safeRelative(final) {
		return false, fmt.Errorf("%w: invalid publication name %q", ErrUnsafePath, final)
	}
	temporary, err := temporaryName()
	if err != nil {
		return false, err
	}
	directory := filepath.Dir(final)
	if directory == "." {
		directory = ""
	}
	temporary = filepath.Join(directory, temporary)
	file, err := root.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return false, err
	}
	cleanup := func(operationErr error) (bool, error) {
		closeErr := file.Close()
		removeErr := root.Remove(temporary)
		if os.IsNotExist(removeErr) {
			removeErr = nil
		}
		return false, errors.Join(operationErr, closeErr, removeErr)
	}
	if err := file.Chmod(mode); err != nil {
		return cleanup(err)
	}
	if _, err := file.Write(content); err != nil {
		return cleanup(err)
	}
	if err := errors.Join(ctx.Err(), file.Sync()); err != nil {
		return cleanup(err)
	}
	createdInfo, err := file.Stat()
	if err != nil || !createdInfo.Mode().IsRegular() {
		regular := err == nil && createdInfo.Mode().IsRegular()
		return cleanup(errors.Join(err, fmt.Errorf("%w: temporary regular=%t", ErrUnsafePath, regular)))
	}
	if err := file.Close(); err != nil {
		_ = root.Remove(temporary)
		return false, err
	}
	file = nil
	if err := root.Link(temporary, final); err != nil {
		_ = root.Remove(temporary)
		return false, err
	}
	finalInfo, err := root.Lstat(final)
	if err != nil || !finalInfo.Mode().IsRegular() || !os.SameFile(createdInfo, finalInfo) {
		_ = root.Remove(final)
		_ = root.Remove(temporary)
		return false, errors.Join(err, fmt.Errorf("%w: final regular=%t same=%t", ErrUnsafePath, err == nil && finalInfo.Mode().IsRegular(), err == nil && os.SameFile(createdInfo, finalInfo)))
	}
	if err := root.Remove(temporary); err != nil {
		return false, err
	}
	directoryFile, err := root.Open(firstNonempty(directory, "."))
	if err != nil {
		return false, err
	}
	err = errors.Join(directoryFile.Sync(), directoryFile.Close())
	return true, err
}

func firstNonempty(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func temporaryName() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return ".publish-" + hex.EncodeToString(random[:]) + ".tmp", nil
}

func readRegular(ctx context.Context, path string, limit int64) ([]byte, error) {
	file, err := safefile.OpenRegular(ctx, path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, err
		}
		return nil, errors.Join(ErrUnsafePath, err)
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, fmt.Errorf("file exceeds %d bytes", limit)
	}
	return raw, nil
}

func readRegularRoot(ctx context.Context, root *os.Root, path string, limit int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !safeRelative(path) {
		return nil, ErrUnsafePath
	}
	info, err := root.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrUnsafePath
	}
	file, err := root.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return nil, errors.Join(ErrUnsafePath, err)
	}
	raw, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, fmt.Errorf("file exceeds %d bytes", limit)
	}
	return raw, nil
}

func safeRelative(path string) bool {
	if path == "" || filepath.IsAbs(path) || filepath.Clean(path) != path {
		return false
	}
	for _, part := range strings.Split(path, string(filepath.Separator)) {
		if !safeName(part) {
			return false
		}
	}
	return true
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func projectUnavailable(record protocol.EvidenceRecord, availability protocol.ContentAvailability) (protocol.EvidenceRecord, error) {
	record.Body.Availability = availability
	record.Body.Blob = nil
	digest, err := canonicaljson.Digest(record.Body)
	if err != nil {
		return protocol.EvidenceRecord{}, err
	}
	record.Digest = digest
	return record, record.Validate()
}

func validateCandidate(candidate protocol.EvidenceCandidate) error {
	if !safeName(string(candidate.ID)) || !safeName(string(candidate.WorkspaceID)) || candidate.Kind == "" || candidate.MediaType == "" || candidate.ProducingActivityID == "" {
		return fmt.Errorf("evidence candidate is incomplete")
	}
	if candidate.SessionID != "" && !safeName(string(candidate.SessionID)) {
		return ErrUnsafePath
	}
	if err := candidate.Actor.Validate(); err != nil {
		return err
	}
	if err := candidate.Subject.Validate(); err != nil {
		return err
	}
	metadata := candidate
	metadata.Content = nil
	return protocol.ValidateBounds(metadata)
}

func safeName(value string) bool {
	return value != "" && value != "." && value != ".." && filepath.Base(value) == value && !strings.ContainsAny(value, "/\\\x00")
}

func digestBytes(value []byte) protocol.Digest {
	sum := sha256.Sum256(value)
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: hex.EncodeToString(sum[:])}
}
