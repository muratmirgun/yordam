package recovery

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
)

var (
	ErrSecretDetected       = secret.ErrSecretDetected
	ErrDigestMismatch       = errors.New("recovery preimage digest mismatch")
	ErrConfidentialMaterial = errors.New("recovery candidate is confidential and cannot be serialized")
)

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

func (Candidate) MarshalJSON() ([]byte, error) {
	return nil, ErrConfidentialMaterial
}

func (Candidate) String() string {
	return "recovery.Candidate{confidential}"
}

func (Candidate) GoString() string {
	return "recovery.Candidate{confidential}"
}

type RecoveryStore interface {
	Put(context.Context, Candidate) (protocol.RecoveryMaterialRecord, error)
}

type Sealer interface {
	Seal(context.Context, []byte) ([]byte, error)
}

type SealerFunc func(context.Context, []byte) ([]byte, error)

func (f SealerFunc) Seal(ctx context.Context, value []byte) ([]byte, error) {
	return f(ctx, value)
}

type Option func(*options) error

type options struct {
	sealer Sealer
	clock  func() time.Time
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

type fileStore struct {
	root    string
	scanner *secret.AdmissionScanner
	sealer  Sealer
	clock   func() time.Time
}

func New(root string, scanner *secret.AdmissionScanner, configured ...Option) (RecoveryStore, error) {
	if scanner == nil {
		return nil, fmt.Errorf("admission scanner is required")
	}
	opts := options{clock: time.Now}
	for _, configure := range configured {
		if configure == nil {
			return nil, fmt.Errorf("recovery option is nil")
		}
		if err := configure(&opts); err != nil {
			return nil, err
		}
	}
	absolute, err := normalizeRoot(root)
	if err != nil {
		return nil, err
	}
	return &fileStore{root: absolute, scanner: scanner, sealer: opts.sealer, clock: opts.clock}, nil
}

func (s *fileStore) Put(ctx context.Context, candidate Candidate) (protocol.RecoveryMaterialRecord, error) {
	if err := validateCandidate(candidate); err != nil {
		return protocol.RecoveryMaterialRecord{}, err
	}
	if digestBytes(candidate.Preimage) != candidate.PreimageDigest {
		return protocol.RecoveryMaterialRecord{}, ErrDigestMismatch
	}
	if s.scanner.Scan(candidate.Preimage) {
		return protocol.RecoveryMaterialRecord{}, ErrSecretDetected
	}
	if err := ctx.Err(); err != nil {
		return protocol.RecoveryMaterialRecord{}, err
	}
	material := append([]byte(nil), candidate.Preimage...)
	sealed := false
	if s.sealer != nil {
		var err error
		material, err = s.sealer.Seal(ctx, append([]byte(nil), material...))
		if err != nil {
			return protocol.RecoveryMaterialRecord{}, fmt.Errorf("seal recovery material")
		}
		if material == nil {
			return protocol.RecoveryMaterialRecord{}, fmt.Errorf("seal recovery material: empty result")
		}
		sealed = true
	}
	id, err := recoveryID()
	if err != nil {
		return protocol.RecoveryMaterialRecord{}, err
	}
	record := protocol.RecoveryMaterialRecord{
		ID: protocol.RecoveryMaterialID(id), WorkspaceID: protocol.WorkspaceID(candidate.WorkspaceID),
		ActivityID: candidate.ActivityID, CheckpointID: candidate.CheckpointID, Subject: protocol.DeepCopy(candidate.Subject),
		Body: protocol.RecoveryMaterialBody{
			PlanDigest: candidate.PlanDigest, PreimageDigest: candidate.PreimageDigest,
			ExpectedPostimageDigest: candidate.ExpectedPostimageDigest, Mode: candidate.Mode,
			Sealed: sealed, CreatedAt: s.clock().UTC(),
		},
		MaterialDigest: digestBytes(material),
	}
	metadata, err := canonicaljson.Marshal(record)
	if err != nil {
		return protocol.RecoveryMaterialRecord{}, err
	}
	if err := s.ensureLayout(ctx); err != nil {
		return protocol.RecoveryMaterialRecord{}, err
	}
	root, err := os.OpenRoot(filepath.Join(s.root, "materials"))
	if err != nil {
		return protocol.RecoveryMaterialRecord{}, err
	}
	defer root.Close()
	if err := publishPair(ctx, root, id+".bin", material, id+".json", metadata); err != nil {
		return protocol.RecoveryMaterialRecord{}, err
	}
	return protocol.DeepCopy(record), nil
}

func validateCandidate(candidate Candidate) error {
	if !safeName(candidate.WorkspaceID) || candidate.ActivityID == "" || candidate.CheckpointID == "" || candidate.Mode > 0o777 {
		return fmt.Errorf("recovery candidate is incomplete")
	}
	if err := candidate.Subject.Validate(); err != nil {
		return err
	}
	for _, digest := range []protocol.Digest{candidate.PlanDigest, candidate.PreimageDigest, candidate.ExpectedPostimageDigest} {
		if err := digest.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (s *fileStore) ensureLayout(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(s.root, "materials"), 0o700); err != nil {
		return err
	}
	for _, path := range []string{s.root, filepath.Join(s.root, "materials")} {
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe recovery directory")
		}
		if err := os.Chmod(path, 0o700); err != nil {
			return err
		}
	}
	return errors.Join(syncDirectory(s.root), syncDirectory(filepath.Join(s.root, "materials")))
}

func publishPair(ctx context.Context, root *os.Root, materialName string, material []byte, metadataName string, metadata []byte) error {
	materialTemporary, err := writeTemporary(ctx, root, material)
	if err != nil {
		return err
	}
	cleanupMaterial := func() { _ = root.Remove(materialTemporary) }
	metadataTemporary, err := writeTemporary(ctx, root, metadata)
	if err != nil {
		cleanupMaterial()
		return err
	}
	cleanupMetadata := func() { _ = root.Remove(metadataTemporary) }
	if err := root.Link(materialTemporary, materialName); err != nil {
		cleanupMaterial()
		cleanupMetadata()
		return err
	}
	if err := root.Link(metadataTemporary, metadataName); err != nil {
		_ = root.Remove(materialName)
		cleanupMaterial()
		cleanupMetadata()
		return err
	}
	cleanupMaterial()
	cleanupMetadata()
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func writeTemporary(ctx context.Context, root *os.Root, content []byte) (string, error) {
	name, err := temporaryName()
	if err != nil {
		return "", err
	}
	file, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	cleanup := func(operationErr error) (string, error) {
		closeErr := file.Close()
		removeErr := root.Remove(name)
		if os.IsNotExist(removeErr) {
			removeErr = nil
		}
		return "", errors.Join(operationErr, closeErr, removeErr)
	}
	if err := file.Chmod(0o600); err != nil {
		return cleanup(err)
	}
	if _, err := file.Write(content); err != nil {
		return cleanup(err)
	}
	if err := errors.Join(ctx.Err(), file.Sync()); err != nil {
		return cleanup(err)
	}
	if err := file.Close(); err != nil {
		_ = root.Remove(name)
		return "", err
	}
	return name, nil
}

func recoveryID() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return "recovery-" + hex.EncodeToString(random[:]), nil
}

func temporaryName() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return ".recovery-" + hex.EncodeToString(random[:]) + ".tmp", nil
}

func digestBytes(value []byte) protocol.Digest {
	sum := sha256.Sum256(value)
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: hex.EncodeToString(sum[:])}
}

func normalizeRoot(root string) (string, error) {
	absolute, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return "", err
	}
	cursor := absolute
	missing := []string{}
	for {
		if canonical, resolveErr := filepath.EvalSymlinks(cursor); resolveErr == nil {
			for index := len(missing) - 1; index >= 0; index-- {
				canonical = filepath.Join(canonical, missing[index])
			}
			return canonical, nil
		}
		parent := filepath.Dir(cursor)
		if parent == cursor {
			return absolute, nil
		}
		missing = append(missing, filepath.Base(cursor))
		cursor = parent
	}
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func safeName(value string) bool {
	return value != "" && value != "." && value != ".." && filepath.Base(value) == value && !strings.ContainsAny(value, "/\\\x00")
}

var _ json.Marshaler = Candidate{}
