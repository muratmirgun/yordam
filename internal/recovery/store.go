package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
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
	"github.com/muratmirgun/yordam/internal/secret"
)

var (
	ErrSecretDetected       = secret.ErrSecretDetected
	ErrDigestMismatch       = errors.New("recovery preimage digest mismatch")
	ErrConfidentialMaterial = errors.New("recovery candidate is confidential and cannot be serialized")
	ErrUnsafePath           = errors.New("unsafe recovery path")
	ErrRecoveryBusy         = errors.New("recovery store is busy")
)

const containerMagic = "YORDAM-RECOVERY-1\n"

type FaultPoint string

const (
	FaultTemporarySynced FaultPoint = "temporary_synced"
	FaultBeforePublish   FaultPoint = "before_publish"
	FaultAfterPublish    FaultPoint = "after_publish"
	FaultDirectorySynced FaultPoint = "directory_synced"
)

type FaultInjector func(FaultPoint) error

type Candidate struct {
	WorkspaceID             string
	ActivityID              protocol.ActivityID
	CheckpointID            protocol.CheckpointID
	Subject                 protocol.SubjectRef
	PlanDigest              protocol.Digest
	Preimage                []byte
	PreimageDigest          protocol.Digest
	ExpectedPostimageDigest protocol.Digest
	Mode                    uint32
}

func (Candidate) MarshalJSON() ([]byte, error) { return nil, ErrConfidentialMaterial }
func (Candidate) String() string               { return "recovery.Candidate{confidential}" }
func (Candidate) GoString() string             { return "recovery.Candidate{confidential}" }

type RecoveryStore interface {
	Put(context.Context, Candidate) (protocol.RecoveryMaterialRecord, error)
	Close() error
}

type Sealer interface {
	Seal(context.Context, []byte) ([]byte, error)
}

type SealerFunc func(context.Context, []byte) ([]byte, error)

func (f SealerFunc) Seal(ctx context.Context, value []byte) ([]byte, error) { return f(ctx, value) }

type Option func(*options) error

type options struct {
	sealer Sealer
	clock  func() time.Time
	fault  FaultInjector
}

func WithSealer(sealer Sealer) Option {
	return func(opts *options) error {
		if sealer == nil {
			return fmt.Errorf("recovery sealer is nil")
		}
		opts.sealer = sealer
		return nil
	}
}

func WithFault(fault FaultInjector) Option {
	return func(opts *options) error {
		if fault == nil {
			return fmt.Errorf("recovery fault injector is nil")
		}
		opts.fault = fault
		return nil
	}
}

type fileStore struct {
	root       string
	admission  *secret.Lease
	sealer     Sealer
	clock      func() time.Time
	fault      FaultInjector
	opMu       sync.Mutex
	mu         sync.Mutex
	rootAnchor *rootanchor.Anchor
	closed     bool
}

func New(root string, admission *secret.Lease, configured ...Option) (RecoveryStore, error) {
	if admission == nil {
		return nil, fmt.Errorf("generation admission lease is required")
	}
	owned, err := admission.Derive()
	if err != nil {
		return nil, fmt.Errorf("derive recovery admission lease: %w", err)
	}
	opts := options{clock: time.Now}
	for _, configure := range configured {
		if configure == nil {
			_ = owned.Close()
			return nil, fmt.Errorf("recovery option is nil")
		}
		if err := configure(&opts); err != nil {
			_ = owned.Close()
			return nil, err
		}
	}
	anchor, err := rootanchor.New(root, ErrUnsafePath)
	if err != nil {
		_ = owned.Close()
		return nil, err
	}
	return &fileStore{root: anchor.Target(), admission: owned, sealer: opts.sealer, clock: opts.clock, fault: opts.fault, rootAnchor: anchor}, nil
}

func (s *fileStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	anchor := s.rootAnchor
	s.rootAnchor = nil
	s.mu.Unlock()
	var rootErr error
	if anchor != nil {
		rootErr = anchor.Close()
	}
	return errors.Join(rootErr, s.admission.Close())
}

func (s *fileStore) Put(ctx context.Context, candidate Candidate) (result protocol.RecoveryMaterialRecord, resultErr error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	metadata, err := validateAndAdmitCandidate(s.admission, candidate)
	if err != nil {
		return protocol.RecoveryMaterialRecord{}, err
	}
	id := recoveryID(metadata)
	if existing, exists, err := s.readExisting(ctx, id, candidate); err != nil {
		return protocol.RecoveryMaterialRecord{}, err
	} else if exists {
		root, rootErr := s.pinnedRoot(ctx, false)
		if rootErr != nil {
			return protocol.RecoveryMaterialRecord{}, rootErr
		}
		coordination, coordinationErr := acquireRecoveryCoordination(ctx, root)
		if coordinationErr != nil {
			return protocol.RecoveryMaterialRecord{}, coordinationErr
		}
		defer func() { resultErr = errors.Join(resultErr, coordination.release()) }()
		if err := s.reconcileTemporaries(ctx); err != nil {
			return protocol.RecoveryMaterialRecord{}, err
		}
		return existing, nil
	}
	material := bytes.Clone(candidate.Preimage)
	sealed := false
	if s.sealer != nil {
		material, err = s.sealer.Seal(ctx, bytes.Clone(material))
		if err != nil {
			return protocol.RecoveryMaterialRecord{}, fmt.Errorf("seal recovery material")
		}
		if material == nil {
			return protocol.RecoveryMaterialRecord{}, fmt.Errorf("seal recovery material: empty result")
		}
		sealed = true
	}
	if err := s.admission.Admit(material); err != nil {
		return protocol.RecoveryMaterialRecord{}, fmt.Errorf("admit recovery material: %w", err)
	}
	record := protocol.RecoveryMaterialRecord{
		ID: id, WorkspaceID: protocol.WorkspaceID(candidate.WorkspaceID), ActivityID: candidate.ActivityID,
		CheckpointID: candidate.CheckpointID, Subject: protocol.DeepCopy(candidate.Subject),
		Body: protocol.RecoveryMaterialBody{PlanDigest: candidate.PlanDigest, PreimageDigest: candidate.PreimageDigest,
			ExpectedPostimageDigest: candidate.ExpectedPostimageDigest, Mode: candidate.Mode, Sealed: sealed, CreatedAt: s.clock().UTC()},
		MaterialDigest: digestBytes(material),
	}
	if err := validateRecoveryRecord(record); err != nil {
		return protocol.RecoveryMaterialRecord{}, err
	}
	container, err := encodeContainer(record, material)
	if err != nil {
		return protocol.RecoveryMaterialRecord{}, err
	}
	if err := s.admission.Admit(container); err != nil {
		return protocol.RecoveryMaterialRecord{}, fmt.Errorf("admit final recovery container: %w", err)
	}
	root, err := s.pinnedRoot(ctx, true)
	if err != nil {
		return protocol.RecoveryMaterialRecord{}, err
	}
	if err := s.ensureMaterials(ctx); err != nil {
		return protocol.RecoveryMaterialRecord{}, err
	}
	coordination, err := acquireRecoveryCoordination(ctx, root)
	if err != nil {
		return protocol.RecoveryMaterialRecord{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, coordination.release()) }()
	if err := s.reconcileTemporaries(ctx); err != nil {
		return protocol.RecoveryMaterialRecord{}, err
	}
	if existing, exists, err := s.readExistingRoot(ctx, id, candidate); err != nil {
		return protocol.RecoveryMaterialRecord{}, err
	} else if exists {
		return existing, nil
	}
	name := string(id) + ".recovery"
	if err := s.publishContainer(ctx, name, container); errors.Is(err, os.ErrExist) {
		existing, exists, readErr := s.readExistingRoot(ctx, id, candidate)
		if readErr != nil || !exists {
			return protocol.RecoveryMaterialRecord{}, errors.Join(err, readErr)
		}
		return existing, nil
	} else if err != nil {
		return protocol.RecoveryMaterialRecord{}, err
	}
	return protocol.DeepCopy(record), nil
}

type candidateMetadata struct {
	WorkspaceID             string
	ActivityID              protocol.ActivityID
	CheckpointID            protocol.CheckpointID
	Subject                 protocol.SubjectRef
	PlanDigest              protocol.Digest
	PreimageDigest          protocol.Digest
	ExpectedPostimageDigest protocol.Digest
	Mode                    uint32
}

func validateAndAdmitCandidate(admission *secret.Lease, candidate Candidate) (candidateMetadata, error) {
	if !safeName(candidate.WorkspaceID) || candidate.ActivityID == "" || candidate.CheckpointID == "" || candidate.Mode > 0o777 {
		return candidateMetadata{}, fmt.Errorf("recovery candidate is incomplete")
	}
	if err := candidate.Subject.Validate(); err != nil {
		return candidateMetadata{}, err
	}
	for _, digest := range []protocol.Digest{candidate.PlanDigest, candidate.PreimageDigest, candidate.ExpectedPostimageDigest} {
		if err := digest.Validate(); err != nil {
			return candidateMetadata{}, err
		}
	}
	if digestBytes(candidate.Preimage) != candidate.PreimageDigest {
		return candidateMetadata{}, ErrDigestMismatch
	}
	metadata := candidateMetadata{candidate.WorkspaceID, candidate.ActivityID, candidate.CheckpointID, protocol.DeepCopy(candidate.Subject),
		candidate.PlanDigest, candidate.PreimageDigest, candidate.ExpectedPostimageDigest, candidate.Mode}
	if err := protocol.ValidateBounds(metadata); err != nil {
		return candidateMetadata{}, err
	}
	raw, err := canonicaljson.Marshal(metadata)
	if err != nil {
		return candidateMetadata{}, err
	}
	if err := admission.Admit(raw); err != nil {
		return candidateMetadata{}, fmt.Errorf("admit recovery metadata: %w", err)
	}
	if err := admission.Admit(candidate.Preimage); err != nil {
		return candidateMetadata{}, err
	}
	return metadata, nil
}

func recoveryID(metadata candidateMetadata) protocol.RecoveryMaterialID {
	raw, _ := canonicaljson.Marshal(metadata)
	sum := sha256.Sum256(append([]byte("recovery-material\x00"), raw...))
	return protocol.RecoveryMaterialID("recovery-" + hex.EncodeToString(sum[:]))
}

func validateRecoveryRecord(record protocol.RecoveryMaterialRecord) error {
	if record.ID == "" || !safeName(string(record.ID)) || !safeName(string(record.WorkspaceID)) || record.ActivityID == "" || record.CheckpointID == "" || record.Body.CreatedAt.IsZero() {
		return fmt.Errorf("recovery record is incomplete")
	}
	if err := record.Subject.Validate(); err != nil {
		return err
	}
	for _, digest := range []protocol.Digest{record.Body.PlanDigest, record.Body.PreimageDigest, record.Body.ExpectedPostimageDigest, record.MaterialDigest} {
		if err := digest.Validate(); err != nil {
			return err
		}
	}
	return protocol.ValidateBounds(record)
}

func encodeContainer(record protocol.RecoveryMaterialRecord, material []byte) ([]byte, error) {
	metadata, err := canonicaljson.Marshal(record)
	if err != nil {
		return nil, err
	}
	container := make([]byte, 0, len(containerMagic)+8+len(metadata)+len(material))
	container = append(container, containerMagic...)
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(metadata)))
	container = append(container, size[:]...)
	container = append(container, metadata...)
	container = append(container, material...)
	return container, nil
}

func decodeContainer(raw []byte) (protocol.RecoveryMaterialRecord, []byte, error) {
	if len(raw) < len(containerMagic)+8 || string(raw[:len(containerMagic)]) != containerMagic {
		return protocol.RecoveryMaterialRecord{}, nil, fmt.Errorf("invalid recovery container")
	}
	metadataSize := binary.BigEndian.Uint64(raw[len(containerMagic) : len(containerMagic)+8])
	start := len(containerMagic) + 8
	if metadataSize > uint64(protocol.MaxEventBytes) || metadataSize > uint64(len(raw)-start) {
		return protocol.RecoveryMaterialRecord{}, nil, fmt.Errorf("invalid recovery metadata size")
	}
	end := start + int(metadataSize)
	var record protocol.RecoveryMaterialRecord
	if err := json.Unmarshal(raw[start:end], &record); err != nil {
		return protocol.RecoveryMaterialRecord{}, nil, err
	}
	if err := validateRecoveryRecord(record); err != nil {
		return protocol.RecoveryMaterialRecord{}, nil, err
	}
	material := bytes.Clone(raw[end:])
	if digestBytes(material) != record.MaterialDigest {
		return protocol.RecoveryMaterialRecord{}, nil, ErrDigestMismatch
	}
	return record, material, nil
}

func (s *fileStore) readExisting(ctx context.Context, id protocol.RecoveryMaterialID, candidate Candidate) (protocol.RecoveryMaterialRecord, bool, error) {
	_, err := s.pinnedRoot(ctx, false)
	if os.IsNotExist(err) {
		return protocol.RecoveryMaterialRecord{}, false, nil
	}
	if err != nil {
		return protocol.RecoveryMaterialRecord{}, false, err
	}
	if err := s.pinDirectory("materials"); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return protocol.RecoveryMaterialRecord{}, false, nil
		}
		return protocol.RecoveryMaterialRecord{}, false, err
	}
	return s.readExistingRoot(ctx, id, candidate)
}

func (s *fileStore) readExistingRoot(ctx context.Context, id protocol.RecoveryMaterialID, candidate Candidate) (protocol.RecoveryMaterialRecord, bool, error) {
	name := string(id) + ".recovery"
	raw, err := s.readRegularDirectory(ctx, "materials", name, 64<<20)
	if errors.Is(err, os.ErrNotExist) {
		return protocol.RecoveryMaterialRecord{}, false, nil
	}
	if err != nil {
		return protocol.RecoveryMaterialRecord{}, false, err
	}
	if err := s.admission.Admit(raw); err != nil {
		return protocol.RecoveryMaterialRecord{}, false, fmt.Errorf("admit existing recovery container: %w", err)
	}
	record, _, err := decodeContainer(raw)
	if err != nil {
		return protocol.RecoveryMaterialRecord{}, false, err
	}
	if record.ID != id || record.WorkspaceID != protocol.WorkspaceID(candidate.WorkspaceID) || record.ActivityID != candidate.ActivityID ||
		record.CheckpointID != candidate.CheckpointID || record.Subject != candidate.Subject || record.Body.PlanDigest != candidate.PlanDigest ||
		record.Body.PreimageDigest != candidate.PreimageDigest || record.Body.ExpectedPostimageDigest != candidate.ExpectedPostimageDigest || record.Body.Mode != candidate.Mode {
		return protocol.RecoveryMaterialRecord{}, false, fmt.Errorf("recovery container provenance mismatch")
	}
	return protocol.DeepCopy(record), true, nil
}

func (s *fileStore) ensureMaterials(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.rootAnchor == nil {
		return secret.ErrLeaseClosed
	}
	if _, err := s.rootAnchor.Open(ctx, true); err != nil {
		return err
	}
	return s.rootAnchor.EnsureDirectory("materials", 0o700)
}

func (s *fileStore) reconcileTemporaries(ctx context.Context) error {
	return s.useDirectory(ctx, "materials", func(root *os.Root) error {
		directory, err := root.Open(".")
		if err != nil {
			return err
		}
		entries, readErr := directory.ReadDir(-1)
		closeErr := directory.Close()
		if readErr != nil || closeErr != nil {
			return errors.Join(readErr, closeErr)
		}
		changed := false
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name(), ".recovery-") || !strings.HasSuffix(entry.Name(), ".tmp") {
				continue
			}
			info, err := root.Lstat(entry.Name())
			if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
				return errors.Join(ErrUnsafePath, err)
			}
			if err := root.Remove(entry.Name()); err != nil {
				return err
			}
			changed = true
		}
		if changed {
			return syncRootDir(root, ".")
		}
		return nil
	})
}

func (s *fileStore) publishContainer(ctx context.Context, final string, content []byte) error {
	return s.useDirectory(ctx, "materials", func(root *os.Root) error {
		return s.publishContainerDirectory(ctx, root, final, content, s.rootAnchor.Verify)
	})
}

func (s *fileStore) publishContainerDirectory(ctx context.Context, root *os.Root, final string, content []byte, verify func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !safeName(final) {
		return ErrUnsafePath
	}
	temporary := fmt.Sprintf(".recovery-%d.tmp", time.Now().UnixNano())
	file, err := root.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	cleanup := func(operationErr error, remove bool) error {
		closeErr := file.Close()
		var removeErr error
		if remove {
			removeErr = root.Remove(temporary)
			if os.IsNotExist(removeErr) {
				removeErr = nil
			}
		}
		return errors.Join(operationErr, closeErr, removeErr)
	}
	if _, err := file.Write(content); err != nil {
		return cleanup(err, true)
	}
	if err := errors.Join(ctx.Err(), file.Sync()); err != nil {
		return cleanup(err, true)
	}
	created, err := file.Stat()
	if err != nil || !created.Mode().IsRegular() {
		return cleanup(errors.Join(ErrUnsafePath, err), true)
	}
	if err := file.Close(); err != nil {
		_ = root.Remove(temporary)
		return err
	}
	file = nil
	if err := s.inject(FaultTemporarySynced); err != nil {
		return err
	}
	if verify != nil {
		if err := verify(); err != nil {
			_ = root.Remove(temporary)
			return err
		}
	}
	if err := s.inject(FaultBeforePublish); err != nil {
		return err
	}
	if verify != nil {
		if err := verify(); err != nil {
			_ = root.Remove(temporary)
			return err
		}
	}
	if err := root.Link(temporary, final); err != nil {
		_ = root.Remove(temporary)
		return err
	}
	finalInfo, err := root.Lstat(final)
	if err != nil || !finalInfo.Mode().IsRegular() || finalInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(created, finalInfo) {
		_ = root.Remove(final)
		_ = root.Remove(temporary)
		return errors.Join(ErrUnsafePath, err)
	}
	if err := s.inject(FaultAfterPublish); err != nil {
		return err
	}
	if verify != nil {
		if err := verify(); err != nil {
			return err
		}
	}
	if err := root.Remove(temporary); err != nil {
		return err
	}
	if err := syncRootDir(root, "."); err != nil {
		return err
	}
	return s.inject(FaultDirectorySynced)
}

func (s *fileStore) inject(point FaultPoint) error {
	if s.fault == nil {
		return nil
	}
	return s.fault(point)
}

func (s *fileStore) pinnedRoot(ctx context.Context, create bool) (*os.Root, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, secret.ErrLeaseClosed
	}
	if s.rootAnchor == nil {
		return nil, ErrUnsafePath
	}
	return s.rootAnchor.Open(ctx, create)
}

func (s *fileStore) pinDirectory(relative string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.rootAnchor == nil {
		return secret.ErrLeaseClosed
	}
	return s.rootAnchor.PinDirectory(relative)
}

func (s *fileStore) verifyRootIdentity() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.rootAnchor == nil {
		return secret.ErrLeaseClosed
	}
	return s.rootAnchor.Verify()
}

func (s *fileStore) useDirectory(ctx context.Context, relative string, operation func(*os.Root) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.rootAnchor == nil {
		return secret.ErrLeaseClosed
	}
	if _, err := s.rootAnchor.Open(ctx, false); err != nil {
		return err
	}
	return s.rootAnchor.UseDirectory(relative, operation)
}

func (s *fileStore) readRegularDirectory(ctx context.Context, directory, name string, limit int64) ([]byte, error) {
	var raw []byte
	err := s.useDirectory(ctx, directory, func(root *os.Root) (readErr error) {
		raw, readErr = readRegularRoot(ctx, root, name, limit)
		return readErr
	})
	return raw, err
}

func readRegularRoot(ctx context.Context, root *os.Root, name string, limit int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !safeName(name) {
		return nil, ErrUnsafePath
	}
	info, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrUnsafePath
	}
	file, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, errors.Join(ErrUnsafePath, err)
	}
	raw, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, fmt.Errorf("recovery container exceeds %d bytes", limit)
	}
	return raw, nil
}

func syncRootDir(root *os.Root, name string) error {
	directory, err := root.Open(name)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func digestBytes(value []byte) protocol.Digest {
	sum := sha256.Sum256(value)
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: hex.EncodeToString(sum[:])}
}

func safeName(value string) bool {
	return value != "" && value != "." && value != ".." && filepath.Base(value) == value && !strings.ContainsAny(value, "/\\\x00")
}
