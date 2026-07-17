package jsonl

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/muratmirgun/yordam/internal/domain"
)

const (
	maxWorkspaceMetadataBytes int64 = 64 << 10
	maxSessionMetadataBytes   int64 = 64 << 10
	persistentReadChunkSize         = 32 << 10
)

var errWorkspaceNotFound = errors.New("workspace storage not found")

func openRootedDirectory(parent *os.Root, name string) (*os.Root, os.FileInfo, error) {
	directory, err := parent.OpenRoot(name)
	if err != nil {
		return nil, nil, err
	}
	descriptor, err := directory.Open(".")
	if err != nil {
		directory.Close()
		return nil, nil, err
	}
	openedInfo, statErr := descriptor.Stat()
	closeErr := descriptor.Close()
	if statErr != nil || closeErr != nil {
		directory.Close()
		return nil, nil, errors.Join(statErr, closeErr)
	}
	checkedInfo, err := parent.Lstat(name)
	if err != nil {
		directory.Close()
		return nil, nil, err
	}
	if !checkedInfo.IsDir() || checkedInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(openedInfo, checkedInfo) {
		directory.Close()
		return nil, nil, fmt.Errorf("%q is not the opened directory", name)
	}
	return directory, openedInfo, nil
}

func verifyRootedDirectory(parent *os.Root, name string, openedInfo os.FileInfo) error {
	checkedInfo, err := parent.Lstat(name)
	if err != nil {
		return err
	}
	if !checkedInfo.IsDir() || checkedInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(openedInfo, checkedInfo) {
		return fmt.Errorf("%q no longer identifies the opened directory", name)
	}
	return nil
}

func openRootedRegularFile(ctx context.Context, root *os.Root, name string, flags int, mode os.FileMode) (*os.File, os.FileInfo, error) {
	file, err := root.OpenFile(name, flags, mode)
	if err != nil {
		return nil, nil, err
	}
	closeFailedOpen := func(openedInfo os.FileInfo, openErr error) (*os.File, os.FileInfo, error) {
		closeErr := file.Close()
		if flags&os.O_CREATE != 0 && flags&os.O_EXCL != 0 {
			if openedInfo != nil {
				return nil, nil, errors.Join(openErr, closeErr, cleanupRootEntryIfSame(root, openedInfo, name))
			}
			return nil, nil, errors.Join(openErr, closeErr, cleanupRootEntries(root, name))
		}
		return nil, nil, errors.Join(openErr, closeErr)
	}
	openedInfo, statErr := file.Stat()
	if statErr != nil {
		return closeFailedOpen(nil, statErr)
	}
	if err := ctx.Err(); err != nil {
		return closeFailedOpen(openedInfo, err)
	}
	checkedInfo, checkErr := root.Lstat(name)
	if checkErr != nil {
		return closeFailedOpen(openedInfo, checkErr)
	}
	if !openedInfo.Mode().IsRegular() || !checkedInfo.Mode().IsRegular() || checkedInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(openedInfo, checkedInfo) {
		return closeFailedOpen(openedInfo, fmt.Errorf("%q is not the opened regular file", name))
	}
	return file, openedInfo, nil
}

func verifyRootedRegularFile(root *os.Root, name string, openedInfo os.FileInfo) error {
	checkedInfo, err := root.Lstat(name)
	if err != nil {
		return err
	}
	if !checkedInfo.Mode().IsRegular() || checkedInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(openedInfo, checkedInfo) {
		return fmt.Errorf("%q no longer identifies the opened regular file", name)
	}
	return nil
}

func readOpenedFile(ctx context.Context, file *os.File, limit int64) ([]byte, error) {
	if limit < 0 {
		return nil, fmt.Errorf("invalid persistent read limit %d", limit)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	if info, err := file.Stat(); err != nil {
		return nil, err
	} else if info.Size() > limit {
		return nil, fmt.Errorf("persistent file exceeds %d bytes", limit)
	}
	var contents bytes.Buffer
	buffer := make([]byte, persistentReadChunkSize)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		remaining := limit + 1 - int64(contents.Len())
		if remaining <= 0 {
			return nil, fmt.Errorf("persistent file exceeds %d bytes", limit)
		}
		chunk := buffer
		if int64(len(chunk)) > remaining {
			chunk = chunk[:remaining]
		}
		count, readErr := file.Read(chunk)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if count > 0 {
			contents.Write(chunk[:count])
			if int64(contents.Len()) > limit {
				return nil, fmt.Errorf("persistent file exceeds %d bytes", limit)
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return contents.Bytes(), nil
			}
			return nil, readErr
		}
		if count == 0 {
			return nil, io.ErrNoProgress
		}
	}
}

type workspaceLayout struct {
	storeRoot      *os.Root
	workspacesRoot *os.Root
	workspacesInfo os.FileInfo
	workspaceRoot  *os.Root
	workspaceInfo  os.FileInfo
	workspaceID    string
	sessionsRoot   *os.Root
	sessionsInfo   os.FileInfo
}

func (s *Store) openWorkspaceLayout(ctx context.Context, workspace domain.Workspace, create bool) (*workspaceLayout, error) {
	if create {
		if err := ensureDir(s.root); err != nil {
			return nil, err
		}
	}
	storeRoot, err := os.OpenRoot(s.root)
	if err != nil {
		if os.IsNotExist(err) && !create {
			return nil, errWorkspaceNotFound
		}
		return nil, err
	}
	layout := &workspaceLayout{storeRoot: storeRoot, workspaceID: workspace.ID}
	fail := func(operationErr error) (*workspaceLayout, error) {
		return nil, errors.Join(operationErr, layout.close())
	}
	layout.workspacesRoot, layout.workspacesInfo, err = openOrCreateRootedDirectory(ctx, storeRoot, "workspaces", create)
	if err != nil {
		if os.IsNotExist(err) && !create {
			return fail(errWorkspaceNotFound)
		}
		return fail(err)
	}
	layout.workspaceRoot, layout.workspaceInfo, err = openOrCreateRootedDirectory(ctx, layout.workspacesRoot, workspace.ID, create)
	if err != nil {
		if os.IsNotExist(err) && !create {
			return fail(errWorkspaceNotFound)
		}
		return fail(err)
	}
	if err := ensureWorkspaceIdentity(ctx, layout, workspace, create); err != nil {
		return fail(err)
	}
	layout.sessionsRoot, layout.sessionsInfo, err = openOrCreateRootedDirectory(ctx, layout.workspaceRoot, "sessions", create)
	if err != nil {
		if os.IsNotExist(err) && !create {
			return fail(errWorkspaceNotFound)
		}
		return fail(err)
	}
	if err := layout.verify(); err != nil {
		return fail(err)
	}
	return layout, nil
}

func createSessionStaging(ctx context.Context, sessionsRoot *os.Root, sessionID string) (string, *os.Root, os.FileInfo, error) {
	for range 100 {
		if err := ctx.Err(); err != nil {
			return "", nil, nil, err
		}
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", nil, nil, err
		}
		name := ".yordam-create-" + sessionID + "-" + hex.EncodeToString(random[:]) + ".tmp"
		if err := sessionsRoot.Mkdir(name, 0o700); os.IsExist(err) {
			continue
		} else if err != nil {
			return "", nil, nil, err
		}
		root, info, err := openRootedDirectory(sessionsRoot, name)
		if err != nil {
			return "", nil, nil, errors.Join(err, cleanupRootEntries(sessionsRoot, name))
		}
		return name, root, info, nil
	}
	return "", nil, nil, fmt.Errorf("could not allocate session staging directory")
}

func reconcileSessionStaging(ctx context.Context, sessionsRoot *os.Root) error {
	directory, err := sessionsRoot.Open(".")
	if err != nil {
		return err
	}
	entries, readErr := directory.ReadDir(-1)
	readErr = errors.Join(readErr, directory.Close())
	if readErr != nil {
		return readErr
	}
	removed := false
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.IsDir() || !validSessionStagingName(entry.Name()) {
			continue
		}
		info, err := sessionsRoot.Lstat(entry.Name())
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		if err := sessionsRoot.RemoveAll(entry.Name()); err != nil {
			return err
		}
		removed = true
	}
	if removed {
		return syncRootDir(sessionsRoot, ".")
	}
	return nil
}

func validSessionStagingName(name string) bool {
	const prefix = ".yordam-create-"
	const suffix = ".tmp"
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return false
	}
	body := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
	if len(body) != 26+1+32 || body[26] != '-' || validateSessionID(body[:26]) != nil {
		return false
	}
	random := body[27:]
	decoded, err := hex.DecodeString(random)
	return err == nil && len(decoded) == 16 && random == strings.ToLower(random)
}

func openOrCreateRootedDirectory(ctx context.Context, parent *os.Root, name string, create bool) (*os.Root, os.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	root, info, err := openRootedDirectory(parent, name)
	if err == nil || !os.IsNotExist(err) || !create {
		return root, info, err
	}
	if err := parent.Mkdir(name, 0o700); err != nil {
		return nil, nil, err
	}
	if err := syncRootDir(parent, "."); err != nil {
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return openRootedDirectory(parent, name)
}

func ensureWorkspaceIdentity(ctx context.Context, layout *workspaceLayout, workspace domain.Workspace, create bool) error {
	if err := cleanupRootTemporaries(layout.workspaceRoot, validWorkspaceTemporaryName); err != nil {
		return err
	}
	file, info, err := openRootedRegularFile(ctx, layout.workspaceRoot, "workspace.json", os.O_RDONLY, 0)
	if os.IsNotExist(err) && create {
		return writeJSONAtomicInRoot(
			ctx,
			layout.workspaceRoot,
			layout.verifyWorkspace,
			"workspace.json",
			".workspace-",
			workspace,
			maxWorkspaceMetadataBytes,
		)
	}
	if err != nil {
		return err
	}
	raw, readErr := readOpenedFile(ctx, file, maxWorkspaceMetadataBytes)
	if readErr == nil {
		readErr = errors.Join(layout.verifyWorkspace(), verifyRootedRegularFile(layout.workspaceRoot, "workspace.json", info))
	}
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return errors.Join(readErr, closeErr)
	}
	var stored domain.Workspace
	if json.Unmarshal(raw, &stored) != nil || stored.ID != workspace.ID || stored.CanonicalPath != workspace.CanonicalPath {
		return fmt.Errorf("workspace identity mismatch for %q", workspace.ID)
	}
	return nil
}

func writeJSONAtomicInRoot(
	ctx context.Context,
	root *os.Root,
	verifyParent func() error,
	final string,
	prefix string,
	value any,
	limit int64,
) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if int64(len(raw)) > limit {
		return fmt.Errorf("persistent JSON exceeds %d bytes", limit)
	}
	temporary, file, fileInfo, err := createRootedTemporary(ctx, root, prefix, ".tmp")
	if err != nil {
		return err
	}
	cleanup := func(operationErr error) error {
		return errors.Join(operationErr, cleanupRootEntryIfSame(root, fileInfo, temporary))
	}
	writeErr := error(nil)
	if _, writeErr = file.Write(raw); writeErr == nil {
		writeErr = file.Sync()
	}
	if writeErr == nil {
		writeErr = errors.Join(verifyParent(), verifyRootedRegularFile(root, temporary, fileInfo))
	}
	writeErr = errors.Join(writeErr, file.Close())
	if writeErr != nil {
		return cleanup(writeErr)
	}
	if err := ctx.Err(); err != nil {
		return cleanup(err)
	}
	if _, err := root.Lstat(final); err == nil {
		return cleanup(fmt.Errorf("%q already exists", final))
	} else if !os.IsNotExist(err) {
		return cleanup(err)
	}
	if err := errors.Join(verifyParent(), verifyRootedRegularFile(root, temporary, fileInfo)); err != nil {
		return cleanup(err)
	}
	if err := root.Rename(temporary, final); err != nil {
		return cleanup(err)
	}
	return syncRootDir(root, ".")
}

func (l *workspaceLayout) verifyWorkspace() error {
	return errors.Join(
		verifyRootedDirectory(l.storeRoot, "workspaces", l.workspacesInfo),
		verifyRootedDirectory(l.workspacesRoot, l.workspaceID, l.workspaceInfo),
	)
}

func (l *workspaceLayout) verify() error {
	return errors.Join(
		l.verifyWorkspace(),
		verifyRootedDirectory(l.workspaceRoot, "sessions", l.sessionsInfo),
	)
}

func (l *workspaceLayout) close() error {
	var closeErrs []error
	if l.sessionsRoot != nil {
		closeErrs = append(closeErrs, l.sessionsRoot.Close())
	}
	if l.workspaceRoot != nil {
		closeErrs = append(closeErrs, l.workspaceRoot.Close())
	}
	if l.workspacesRoot != nil {
		closeErrs = append(closeErrs, l.workspacesRoot.Close())
	}
	if l.storeRoot != nil {
		closeErrs = append(closeErrs, l.storeRoot.Close())
	}
	return errors.Join(closeErrs...)
}

type sessionTransaction struct {
	storeRoot      *os.Root
	workspacesRoot *os.Root
	workspacesInfo os.FileInfo
	workspaceRoot  *os.Root
	workspaceInfo  os.FileInfo
	workspaceID    string
	sessionsRoot   *os.Root
	sessionsInfo   os.FileInfo
	sessionID      string
	sessionRoot    *os.Root
	directoryInfo  os.FileInfo
	metadata       *os.File
	metadataInfo   os.FileInfo
	events         *os.File
	eventsInfo     os.FileInfo
	artifactsRoot  *os.Root
	artifactsInfo  os.FileInfo
}

func (s *Store) openSessionTransaction(
	ctx context.Context,
	sessionID string,
	eventFlags int,
	eventMode os.FileMode,
) (*sessionTransaction, domain.Session, error) {
	if err := validateSessionID(sessionID); err != nil {
		return nil, domain.Session{}, err
	}
	storeRoot, err := os.OpenRoot(s.root)
	if err != nil {
		return nil, domain.Session{}, err
	}
	workspacesRoot, _, err := openRootedDirectory(storeRoot, "workspaces")
	if err != nil {
		storeRoot.Close()
		if os.IsNotExist(err) {
			return nil, domain.Session{}, fmt.Errorf("session %q not found", sessionID)
		}
		return nil, domain.Session{}, err
	}
	workspaces, err := workspacesRoot.Open(".")
	if err != nil {
		return nil, domain.Session{}, errors.Join(err, workspacesRoot.Close(), storeRoot.Close())
	}
	entries, readErr := workspaces.ReadDir(-1)
	readErr = errors.Join(readErr, workspaces.Close(), workspacesRoot.Close(), storeRoot.Close())
	if readErr != nil {
		return nil, domain.Session{}, readErr
	}
	var transaction *sessionTransaction
	var session domain.Session
	for _, workspace := range entries {
		if err := ctx.Err(); err != nil {
			if transaction != nil {
				err = errors.Join(err, transaction.close())
			}
			return nil, domain.Session{}, err
		}
		if !validWorkspaceID(workspace.Name()) {
			continue
		}
		if !workspace.IsDir() {
			if transaction != nil {
				transaction.close()
			}
			return nil, domain.Session{}, fmt.Errorf("workspace entry %q is not a directory", workspace.Name())
		}
		candidate, candidateSession, openErr := s.openSessionAtWorkspace(ctx, workspace.Name(), sessionID, eventFlags, eventMode)
		if errors.Is(openErr, os.ErrNotExist) {
			continue
		}
		if openErr != nil {
			if transaction != nil {
				openErr = errors.Join(openErr, transaction.close())
			}
			return nil, candidateSession, openErr
		}
		if transaction != nil {
			return nil, domain.Session{}, errors.Join(candidate.close(), transaction.close(), fmt.Errorf("session %q is not unique", sessionID))
		}
		transaction = candidate
		session = candidateSession
	}
	if transaction == nil {
		return nil, domain.Session{}, fmt.Errorf("session %q not found", sessionID)
	}
	return transaction, session, nil
}

func (s *Store) openSessionAtWorkspace(
	ctx context.Context,
	workspaceID string,
	sessionID string,
	eventFlags int,
	eventMode os.FileMode,
) (*sessionTransaction, domain.Session, error) {
	storeRoot, err := os.OpenRoot(s.root)
	if err != nil {
		return nil, domain.Session{}, err
	}
	transaction := &sessionTransaction{storeRoot: storeRoot, workspaceID: workspaceID, sessionID: sessionID}
	fail := func(operationErr error) (*sessionTransaction, domain.Session, error) {
		return nil, domain.Session{}, errors.Join(operationErr, transaction.close())
	}
	transaction.workspacesRoot, transaction.workspacesInfo, err = openRootedDirectory(storeRoot, "workspaces")
	if err != nil {
		return fail(err)
	}
	transaction.workspaceRoot, transaction.workspaceInfo, err = openRootedDirectory(transaction.workspacesRoot, workspaceID)
	if err != nil {
		return fail(err)
	}
	if err := cleanupRootTemporaries(transaction.workspaceRoot, validWorkspaceTemporaryName); err != nil {
		return fail(err)
	}
	transaction.sessionsRoot, transaction.sessionsInfo, err = openRootedDirectory(transaction.workspaceRoot, "sessions")
	if err != nil {
		return fail(err)
	}
	transaction.sessionRoot, transaction.directoryInfo, err = openRootedDirectory(transaction.sessionsRoot, sessionID)
	if err != nil {
		return fail(err)
	}
	if err := cleanupRootTemporaries(transaction.sessionRoot, validMetadataTemporaryName); err != nil {
		return fail(err)
	}
	transaction.metadata, transaction.metadataInfo, err = openRootedRegularFile(ctx, transaction.sessionRoot, "metadata.json", os.O_RDONLY, 0)
	if err != nil {
		return fail(err)
	}
	raw, err := readOpenedFile(ctx, transaction.metadata, maxSessionMetadataBytes)
	if err == nil {
		err = transaction.verifyMetadata()
	}
	var session domain.Session
	if err == nil {
		err = json.Unmarshal(raw, &session)
	}
	if err == nil && session.ID != sessionID {
		err = fmt.Errorf("session metadata ID %q does not match %q", session.ID, sessionID)
	}
	if err == nil && session.Workspace.ID != workspaceID {
		err = fmt.Errorf("session workspace ID %q does not match directory %q", session.Workspace.ID, workspaceID)
	}
	if err == nil {
		err = validateWorkspace(session.Workspace)
	}
	if err == nil {
		err = verifyStoredWorkspace(ctx, transaction, session.Workspace)
	}
	if err != nil {
		return fail(err)
	}
	transaction.events, transaction.eventsInfo, err = openRootedRegularFile(ctx, transaction.sessionRoot, "events.jsonl", eventFlags, eventMode)
	if os.IsNotExist(err) {
		return nil, session, errors.Join(
			&missingEventLogError{sessionID: session.ID},
			transaction.close(),
		)
	}
	if err != nil {
		return fail(err)
	}
	if err := transaction.verifyEvents(); err != nil {
		return fail(err)
	}
	return transaction, session, nil
}

func verifyStoredWorkspace(ctx context.Context, transaction *sessionTransaction, workspace domain.Workspace) error {
	file, info, err := openRootedRegularFile(ctx, transaction.workspaceRoot, "workspace.json", os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	raw, readErr := readOpenedFile(ctx, file, maxWorkspaceMetadataBytes)
	if readErr == nil {
		readErr = errors.Join(transaction.verifyWorkspace(), verifyRootedRegularFile(transaction.workspaceRoot, "workspace.json", info))
	}
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return errors.Join(readErr, closeErr)
	}
	var stored domain.Workspace
	if json.Unmarshal(raw, &stored) != nil || stored.ID != workspace.ID || stored.CanonicalPath != workspace.CanonicalPath {
		return fmt.Errorf("workspace identity mismatch for %q", workspace.ID)
	}
	return nil
}

func (t *sessionTransaction) openArtifacts() error {
	if t.artifactsRoot != nil {
		return t.verifyArtifacts()
	}
	root, info, err := openRootedDirectory(t.sessionRoot, "artifacts")
	if err != nil {
		return err
	}
	t.artifactsRoot, t.artifactsInfo = root, info
	if err := cleanupRootTemporaries(root, validArtifactTemporaryName); err != nil {
		t.artifactsRoot, t.artifactsInfo = nil, nil
		return errors.Join(err, root.Close())
	}
	if err := t.verifyArtifacts(); err != nil {
		t.artifactsRoot, t.artifactsInfo = nil, nil
		return errors.Join(err, root.Close())
	}
	return nil
}

func (t *sessionTransaction) verifySession() error {
	return errors.Join(
		t.verifyWorkspace(),
		verifyRootedDirectory(t.workspaceRoot, "sessions", t.sessionsInfo),
		verifyRootedDirectory(t.sessionsRoot, t.sessionID, t.directoryInfo),
	)
}

func (t *sessionTransaction) verifyWorkspace() error {
	return errors.Join(
		verifyRootedDirectory(t.storeRoot, "workspaces", t.workspacesInfo),
		verifyRootedDirectory(t.workspacesRoot, t.workspaceID, t.workspaceInfo),
	)
}

func (t *sessionTransaction) verifyMetadata() error {
	return errors.Join(
		t.verifySession(),
		verifyRootedRegularFile(t.sessionRoot, "metadata.json", t.metadataInfo),
	)
}

func (t *sessionTransaction) verifyEvents() error {
	return errors.Join(
		t.verifyMetadata(),
		verifyRootedRegularFile(t.sessionRoot, "events.jsonl", t.eventsInfo),
	)
}

func (t *sessionTransaction) verifyArtifacts() error {
	if t.artifactsRoot == nil {
		return fmt.Errorf("artifacts directory is not open")
	}
	return errors.Join(
		t.verifyEvents(),
		verifyRootedDirectory(t.sessionRoot, "artifacts", t.artifactsInfo),
	)
}

func (t *sessionTransaction) closeWithoutStoreRoot() error {
	var closeErrs []error
	if t.artifactsRoot != nil {
		closeErrs = append(closeErrs, t.artifactsRoot.Close())
	}
	if t.events != nil {
		closeErrs = append(closeErrs, t.events.Close())
	}
	if t.metadata != nil {
		closeErrs = append(closeErrs, t.metadata.Close())
	}
	if t.sessionRoot != nil {
		closeErrs = append(closeErrs, t.sessionRoot.Close())
	}
	if t.sessionsRoot != nil {
		closeErrs = append(closeErrs, t.sessionsRoot.Close())
	}
	if t.workspaceRoot != nil {
		closeErrs = append(closeErrs, t.workspaceRoot.Close())
	}
	if t.workspacesRoot != nil {
		closeErrs = append(closeErrs, t.workspacesRoot.Close())
	}
	return errors.Join(closeErrs...)
}

func (t *sessionTransaction) close() error {
	return errors.Join(t.closeWithoutStoreRoot(), t.storeRoot.Close())
}

func writeJSONAtomicRooted(ctx context.Context, transaction *sessionTransaction, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if int64(len(raw)) > maxSessionMetadataBytes {
		return fmt.Errorf("session metadata exceeds %d bytes", maxSessionMetadataBytes)
	}
	if err := transaction.verifyEvents(); err != nil {
		return err
	}
	temporary, file, fileInfo, err := createRootedTemporary(ctx, transaction.sessionRoot, ".metadata-", ".tmp")
	if err != nil {
		return err
	}
	cleanup := func(operationErr error) error {
		return errors.Join(operationErr, cleanupRootEntryIfSame(transaction.sessionRoot, fileInfo, temporary))
	}
	writeErr := error(nil)
	if _, writeErr = file.Write(raw); writeErr == nil {
		writeErr = file.Sync()
	}
	if writeErr == nil {
		writeErr = verifyRootedRegularFile(transaction.sessionRoot, temporary, fileInfo)
	}
	writeErr = errors.Join(writeErr, file.Close())
	if writeErr != nil {
		return cleanup(writeErr)
	}
	if err := ctx.Err(); err != nil {
		return cleanup(err)
	}
	if err := errors.Join(
		transaction.verifyEvents(),
		verifyRootedRegularFile(transaction.sessionRoot, temporary, fileInfo),
	); err != nil {
		return cleanup(err)
	}
	if err := transaction.sessionRoot.Rename(temporary, "metadata.json"); err != nil {
		return cleanup(err)
	}
	if err := syncRootDir(transaction.sessionRoot, "."); err != nil {
		return err
	}
	metadata, metadataInfo, err := openRootedRegularFile(ctx, transaction.sessionRoot, "metadata.json", os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	oldMetadata := transaction.metadata
	transaction.metadata, transaction.metadataInfo = metadata, metadataInfo
	return errors.Join(
		oldMetadata.Close(),
		transaction.verifyEvents(),
	)
}

func createRootedTemporary(
	ctx context.Context,
	root *os.Root,
	prefix string,
	suffix string,
) (string, *os.File, os.FileInfo, error) {
	for range 100 {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", nil, nil, err
		}
		name := prefix + hex.EncodeToString(random[:]) + suffix
		file, fileInfo, err := openRootedRegularFile(ctx, root, name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if os.IsExist(err) {
			continue
		}
		return name, file, fileInfo, err
	}
	return "", nil, nil, fmt.Errorf("could not allocate rooted temporary file")
}

func validWorkspaceTemporaryName(name string) bool {
	return validRandomTemporaryName(name, ".workspace-", ".tmp")
}

func validMetadataTemporaryName(name string) bool {
	return validRandomTemporaryName(name, ".metadata-", ".tmp")
}

func validRandomTemporaryName(name, prefix, suffix string) bool {
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return false
	}
	random := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
	decoded, err := hex.DecodeString(random)
	return err == nil && len(decoded) == 16 && random == strings.ToLower(random)
}

func cleanupRootTemporaries(root *os.Root, valid func(string) bool) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	entries, readErr := directory.ReadDir(-1)
	readErr = errors.Join(readErr, directory.Close())
	if readErr != nil {
		return readErr
	}
	removed := false
	for _, entry := range entries {
		if !valid(entry.Name()) || !entry.Type().IsRegular() {
			continue
		}
		if err := root.Remove(entry.Name()); err != nil && !os.IsNotExist(err) {
			return err
		}
		removed = true
	}
	if removed {
		return syncRootDir(root, ".")
	}
	return nil
}

func syncRootDir(root *os.Root, path string) error {
	directory, err := root.Open(path)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	return errors.Join(syncErr, directory.Close())
}

func cleanupRootEntries(root *os.Root, names ...string) error {
	var cleanupErrs []error
	for _, name := range names {
		if name == "" {
			continue
		}
		if err := root.Remove(name); err != nil && !os.IsNotExist(err) {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("cleanup artifact %q: %w", name, err))
		}
	}
	if err := syncRootDir(root, "."); err != nil {
		cleanupErrs = append(cleanupErrs, fmt.Errorf("sync artifact cleanup: %w", err))
	}
	return errors.Join(cleanupErrs...)
}

func cleanupRootEntryIfSame(root *os.Root, openedInfo os.FileInfo, names ...string) error {
	var cleanupErrs []error
	for _, name := range names {
		checkedInfo, err := root.Lstat(name)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("inspect artifact cleanup %q: %w", name, err))
			continue
		}
		if !os.SameFile(openedInfo, checkedInfo) {
			continue
		}
		if err := root.Remove(name); err != nil && !os.IsNotExist(err) {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("cleanup artifact %q: %w", name, err))
		}
	}
	if err := syncRootDir(root, "."); err != nil {
		cleanupErrs = append(cleanupErrs, fmt.Errorf("sync artifact cleanup: %w", err))
	}
	return errors.Join(cleanupErrs...)
}
