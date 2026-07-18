package jsonl

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/oklog/ulid/v2"
)

const MaxArtifactBytes int64 = 10 << 20

// Put checks ctx before and after each source read. It cannot interrupt an
// arbitrary Reader whose Read method is itself blocked.
func (s *Store) Put(ctx context.Context, sessionID, mediaType string, src io.Reader, limit int64) (domain.Artifact, error) {
	if err := validateSessionID(sessionID); err != nil {
		return domain.Artifact{}, err
	}
	lock := s.journalLock(protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(sessionID)})
	if err := lock.lock(ctx); err != nil {
		return domain.Artifact{}, err
	}
	defer lock.unlock()
	return s.putLocked(ctx, sessionID, mediaType, src, limit)
}

func (s *Store) putLocked(ctx context.Context, sessionID, mediaType string, src io.Reader, limit int64) (domain.Artifact, error) {
	if err := ctx.Err(); err != nil {
		return domain.Artifact{}, err
	}
	if src == nil {
		return domain.Artifact{}, fmt.Errorf("artifact source is nil")
	}
	if limit <= 0 || limit > MaxArtifactBytes {
		limit = MaxArtifactBytes
	}
	if s.artifactAdmission != nil {
		candidate, err := readArtifactCandidate(ctx, src, limit)
		if err != nil {
			return domain.Artifact{}, err
		}
		if s.artifactAdmission.Scanner().Scan(candidate) {
			return domain.Artifact{}, secret.ErrSecretDetected
		}
		src = bytes.NewReader(candidate)
	}
	transaction, session, err := s.openSessionTransaction(ctx, sessionID, os.O_RDONLY, 0)
	if err != nil {
		return domain.Artifact{}, err
	}
	fail := func(operationErr error) (domain.Artifact, error) {
		return domain.Artifact{}, errors.Join(operationErr, transaction.close())
	}
	if err := transaction.openArtifacts(); err != nil {
		return fail(err)
	}
	id, err := s.nextID()
	if err != nil {
		return fail(err)
	}
	written, truncated, writeErr := writeArtifactTransactional(ctx, transaction, id, src, limit)
	closeErr := transaction.close()
	if writeErr != nil || closeErr != nil {
		return domain.Artifact{}, errors.Join(writeErr, closeErr)
	}
	return domain.Artifact{
		ID:        id,
		SessionID: session.ID,
		MediaType: mediaType,
		Path:      filepath.Join(s.root, "workspaces", session.Workspace.ID, "sessions", session.ID, "artifacts", id+".bin"),
		Size:      written,
		Truncated: truncated,
	}, nil
}

func readArtifactCandidate(ctx context.Context, src io.Reader, limit int64) ([]byte, error) {
	reader := io.LimitReader(src, limit+1)
	var content bytes.Buffer
	buffer := make([]byte, 32*1024)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		count, err := reader.Read(buffer)
		if count > 0 {
			_, _ = content.Write(buffer[:count])
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return content.Bytes(), nil
			}
			return nil, err
		}
		if count == 0 {
			return nil, io.ErrNoProgress
		}
	}
}

func writeArtifactTransactional(
	ctx context.Context,
	transaction *sessionTransaction,
	id string,
	src io.Reader,
	limit int64,
) (int64, bool, error) {
	artifactsRoot := transaction.artifactsRoot
	temporary := "." + id + ".tmp"
	final := id + ".bin"
	file, fileInfo, err := openRootedRegularFile(ctx, artifactsRoot, temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, false, err
	}
	written, copyErr := copyArtifact(ctx, file, src, limit)
	truncated := written > limit
	if truncated {
		copyErr = errors.Join(copyErr, ctx.Err(), file.Truncate(limit))
		written = limit
	}
	if copyErr == nil {
		copyErr = file.Sync()
	}
	if copyErr == nil {
		copyErr = verifyRootedRegularFile(artifactsRoot, temporary, fileInfo)
	}
	copyErr = errors.Join(copyErr, file.Close())
	if copyErr != nil {
		return 0, false, errors.Join(copyErr, cleanupRootEntryIfSame(artifactsRoot, fileInfo, temporary))
	}
	if err := transaction.verifyArtifacts(); err != nil {
		return 0, false, errors.Join(err, cleanupRootEntryIfSame(artifactsRoot, fileInfo, temporary))
	}
	if err := verifyRootedRegularFile(artifactsRoot, temporary, fileInfo); err != nil {
		return 0, false, errors.Join(err, cleanupRootEntryIfSame(artifactsRoot, fileInfo, temporary))
	}
	if err := artifactsRoot.Link(temporary, final); err != nil {
		return 0, false, errors.Join(err, cleanupRootEntryIfSame(artifactsRoot, fileInfo, temporary))
	}
	if err := errors.Join(
		transaction.verifyArtifacts(),
		verifyRootedRegularFile(artifactsRoot, final, fileInfo),
	); err != nil {
		return 0, false, errors.Join(err, cleanupRootEntryIfSame(artifactsRoot, fileInfo, final, temporary))
	}
	if err := artifactsRoot.Remove(temporary); err != nil {
		return 0, false, errors.Join(err, cleanupRootEntryIfSame(artifactsRoot, fileInfo, final, temporary))
	}
	if err := syncRootDir(artifactsRoot, "."); err != nil {
		return 0, false, errors.Join(err, cleanupRootEntryIfSame(artifactsRoot, fileInfo, final, temporary))
	}
	if err := errors.Join(
		transaction.verifyArtifacts(),
		verifyRootedRegularFile(artifactsRoot, final, fileInfo),
	); err != nil {
		return 0, false, errors.Join(err, cleanupRootEntryIfSame(artifactsRoot, fileInfo, final))
	}
	return written, truncated, nil
}

func validArtifactTemporaryName(name string) bool {
	if len(name) < 3 || name[0] != '.' || filepath.Ext(name) != ".tmp" {
		return false
	}
	id := name[1 : len(name)-len(".tmp")]
	parsed, err := ulid.ParseStrict(id)
	return err == nil && parsed.String() == id
}

func copyArtifact(ctx context.Context, dst io.Writer, src io.Reader, limit int64) (int64, error) {
	reader := io.LimitReader(src, limit+1)
	buffer := make([]byte, 32*1024)
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		count, readErr := reader.Read(buffer)
		if err := ctx.Err(); err != nil {
			return written, err
		}
		if count > 0 {
			writeCount, writeErr := dst.Write(buffer[:count])
			written += int64(writeCount)
			if writeErr != nil {
				return written, writeErr
			}
			if writeCount != count {
				return written, io.ErrShortWrite
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return written, nil
			}
			return written, readErr
		}
	}
}

func (s *Store) Open(ctx context.Context, artifact domain.Artifact) (io.ReadCloser, error) {
	if err := validateSessionID(artifact.SessionID); err != nil {
		return nil, err
	}
	lock := s.journalLock(protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(artifact.SessionID)})
	if err := lock.lock(ctx); err != nil {
		return nil, err
	}
	defer lock.unlock()
	transaction, session, err := s.openSessionTransaction(ctx, artifact.SessionID, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	fail := func(file *os.File, operationErr error) (io.ReadCloser, error) {
		var fileCloseErr error
		if file != nil {
			fileCloseErr = file.Close()
		}
		return nil, errors.Join(operationErr, fileCloseErr, transaction.close())
	}
	parsed, err := ulid.ParseStrict(artifact.ID)
	if err != nil || parsed.String() != artifact.ID {
		return fail(nil, fmt.Errorf("invalid artifact ID %q", artifact.ID))
	}
	relativeDir := filepath.Join("workspaces", session.Workspace.ID, "sessions", session.ID, "artifacts")
	relativePath := filepath.Join(relativeDir, artifact.ID+".bin")
	if artifact.Path != filepath.Join(s.root, relativePath) {
		return fail(nil, fmt.Errorf("artifact path does not match artifact identity"))
	}
	if err := transaction.openArtifacts(); err != nil {
		return fail(nil, err)
	}
	file, _, openErr := openRootedRegularFile(ctx, transaction.artifactsRoot, artifact.ID+".bin", os.O_RDONLY, 0)
	if openErr == nil {
		openErr = transaction.verifyArtifacts()
	}
	closeErr := transaction.close()
	if openErr != nil || closeErr != nil {
		if file != nil {
			closeErr = errors.Join(closeErr, file.Close())
		}
		return nil, errors.Join(openErr, closeErr)
	}
	return file, nil
}
