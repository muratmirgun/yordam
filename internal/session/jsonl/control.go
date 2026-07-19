package jsonl

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type controlMetadata struct {
	WorkspaceID protocol.WorkspaceID `json:"workspace_id"`
	LastSeq     uint64               `json:"last_seq"`
	UpdatedAt   time.Time            `json:"updated_at"`
}

func (s *Store) EnsureWorkspaceControl(ctx context.Context, workspace domain.Workspace) (protocol.JournalRef, error) {
	if err := validateWorkspace(workspace); err != nil {
		return protocol.JournalRef{}, err
	}
	layout, err := s.openWorkspaceLayout(ctx, workspace, true)
	if err != nil {
		return protocol.JournalRef{}, err
	}
	if err := layout.close(); err != nil {
		return protocol.JournalRef{}, err
	}
	return protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: protocol.JournalID(workspace.ID)}, nil
}

func (s *Store) ensureControlJournalLayout(ctx context.Context, layout *workspaceLayout) error {
	controlRoot, controlInfo, err := openRootedDirectory(layout.workspaceRoot, "control")
	if err == nil {
		layout.controlRoot, layout.controlInfo = controlRoot, controlInfo
		return s.validateControlJournalFiles(ctx, layout)
	}
	if !os.IsNotExist(err) {
		return err
	}
	if err := reconcileControlStaging(ctx, layout.workspaceRoot); err != nil {
		return err
	}
	stagingName, stagingRoot, stagingInfo, err := createControlStaging(ctx, layout.workspaceRoot)
	if err != nil {
		return err
	}
	published := false
	defer func() {
		_ = stagingRoot.Close()
		if !published {
			_ = cleanupRootEntryIfSame(layout.workspaceRoot, stagingInfo, stagingName)
		}
	}()
	metadata := controlMetadata{WorkspaceID: protocol.WorkspaceID(layout.workspaceID), UpdatedAt: s.clock().UTC()}
	raw, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}
	for _, member := range []struct {
		name string
		raw  []byte
	}{
		{name: "metadata.json", raw: raw},
		{name: "events.jsonl"},
		{name: journalLockName},
	} {
		if err := writeStagedFile(ctx, stagingRoot, member.name, member.raw); err != nil {
			return err
		}
	}
	if err := writeDurableLockSetState(ctx, stagingRoot, protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: protocol.JournalID(layout.workspaceID)}); err != nil {
		return err
	}
	if err := syncRootDir(stagingRoot, "."); err != nil {
		return err
	}
	if err := errors.Join(layout.verifyWorkspace(), verifyRootedDirectory(layout.workspaceRoot, stagingName, stagingInfo)); err != nil {
		return err
	}
	if _, err := layout.workspaceRoot.Lstat("control"); err == nil {
		return fmt.Errorf("control layout appeared while workspace coordination lock was held")
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := layout.workspaceRoot.Rename(stagingName, "control"); err != nil {
		return err
	}
	published = true
	if err := syncRootDir(layout.workspaceRoot, "."); err != nil {
		return err
	}
	layout.controlRoot, layout.controlInfo, err = openRootedDirectory(layout.workspaceRoot, "control")
	if err != nil {
		return err
	}
	return s.validateControlJournalFiles(ctx, layout)
}

func (s *Store) validateControlJournalFiles(ctx context.Context, layout *workspaceLayout) error {
	if layout.controlRoot == nil {
		return fmt.Errorf("workspace control directory is not open")
	}
	metadata, metadataInfo, err := openRootedRegularFile(ctx, layout.controlRoot, "metadata.json", os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	raw, readErr := readOpenedFile(ctx, metadata, maxSessionMetadataBytes)
	var state controlMetadata
	if readErr == nil {
		readErr = json.Unmarshal(raw, &state)
	}
	if readErr == nil && state.WorkspaceID != protocol.WorkspaceID(layout.workspaceID) {
		readErr = fmt.Errorf("control metadata workspace ID mismatch")
	}
	readErr = errors.Join(readErr, verifyRootedRegularFile(layout.controlRoot, "metadata.json", metadataInfo), metadata.Close())
	if readErr != nil {
		return readErr
	}
	for _, name := range []string{"events.jsonl", journalLockName} {
		file, info, err := openRootedRegularFile(ctx, layout.controlRoot, name, os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		memberErr := error(nil)
		if info.Mode().Perm() != 0o600 {
			memberErr = fmt.Errorf("%q mode is %04o, want 0600", name, info.Mode().Perm())
		}
		memberErr = errors.Join(memberErr, verifyRootedRegularFile(layout.controlRoot, name, info), file.Close())
		if memberErr != nil {
			return memberErr
		}
	}
	lockSet, err := openValidatedLockSetAtRoot(
		ctx,
		layout.controlRoot,
		true,
		protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: protocol.JournalID(layout.workspaceID)},
	)
	if err != nil {
		return err
	}
	if err := lockSet.close(); err != nil {
		return err
	}
	return layout.verify()
}

func createControlStaging(ctx context.Context, workspaceRoot *os.Root) (string, *os.Root, os.FileInfo, error) {
	for range 100 {
		if err := ctx.Err(); err != nil {
			return "", nil, nil, err
		}
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", nil, nil, err
		}
		name := ".yordam-control-" + hex.EncodeToString(random[:]) + ".tmp"
		if err := workspaceRoot.Mkdir(name, 0o700); os.IsExist(err) {
			continue
		} else if err != nil {
			return "", nil, nil, err
		}
		root, info, err := openRootedDirectory(workspaceRoot, name)
		if err != nil {
			return "", nil, nil, errors.Join(err, cleanupRootEntries(workspaceRoot, name))
		}
		return name, root, info, nil
	}
	return "", nil, nil, fmt.Errorf("could not allocate control staging directory")
}

func reconcileControlStaging(ctx context.Context, workspaceRoot *os.Root) error {
	directory, err := workspaceRoot.Open(".")
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
		if !entry.IsDir() || !validControlStagingName(entry.Name()) {
			continue
		}
		info, err := workspaceRoot.Lstat(entry.Name())
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		if err := workspaceRoot.RemoveAll(entry.Name()); err != nil {
			return err
		}
		removed = true
	}
	if removed {
		return syncRootDir(workspaceRoot, ".")
	}
	return nil
}

func validControlStagingName(name string) bool {
	const prefix = ".yordam-control-"
	const suffix = ".tmp"
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return false
	}
	random := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
	decoded, err := hex.DecodeString(random)
	return err == nil && len(decoded) == 16 && random == strings.ToLower(random)
}

func ensureDurableRootedFile(ctx context.Context, root *os.Root, name string, contents []byte) error {
	file, info, err := openRootedRegularFile(ctx, root, name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if os.IsExist(err) {
		file, info, err = openRootedRegularFile(ctx, root, name, os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		if info.Mode().Perm() != 0o600 {
			return errors.Join(fmt.Errorf("%q mode is %04o, want 0600", name, info.Mode().Perm()), file.Close())
		}
		return errors.Join(verifyRootedRegularFile(root, name, info), file.Close())
	}
	if err != nil {
		return err
	}
	writeErr := error(nil)
	if len(contents) > 0 {
		_, writeErr = file.Write(contents)
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	if writeErr == nil {
		writeErr = verifyRootedRegularFile(root, name, info)
	}
	return errors.Join(writeErr, file.Close(), syncRootDir(root, "."))
}

func (s *Store) openControlTransaction(ctx context.Context, workspaceID string, eventFlags int) (*sessionTransaction, domain.Session, error) {
	if !validWorkspaceID(workspaceID) {
		return nil, domain.Session{}, fmt.Errorf("invalid workspace control ID %q", workspaceID)
	}
	storeRoot, err := os.OpenRoot(s.root)
	if err != nil {
		return nil, domain.Session{}, err
	}
	transaction := &sessionTransaction{storeRoot: storeRoot, workspaceID: workspaceID, sessionID: workspaceID, control: true}
	fail := func(operationErr error) (*sessionTransaction, domain.Session, error) {
		return nil, domain.Session{}, errors.Join(operationErr, transaction.close())
	}
	transaction.workspacesRoot, transaction.workspacesInfo, err = openRootedDirectory(transaction.storeRoot, "workspaces")
	if err != nil {
		return fail(err)
	}
	transaction.workspaceRoot, transaction.workspaceInfo, err = openRootedDirectory(transaction.workspacesRoot, workspaceID)
	if err != nil {
		return fail(err)
	}
	workspaceFile, workspaceInfo, err := openRootedRegularFile(ctx, transaction.workspaceRoot, "workspace.json", os.O_RDONLY, 0)
	if err != nil {
		return fail(err)
	}
	workspaceRaw, readErr := readOpenedFile(ctx, workspaceFile, maxWorkspaceMetadataBytes)
	readErr = errors.Join(readErr, verifyRootedRegularFile(transaction.workspaceRoot, "workspace.json", workspaceInfo), workspaceFile.Close())
	if readErr != nil {
		return fail(readErr)
	}
	var workspace domain.Workspace
	if err := json.Unmarshal(workspaceRaw, &workspace); err != nil || workspace.ID != workspaceID || validateWorkspace(workspace) != nil {
		return fail(fmt.Errorf("workspace identity mismatch for control %q", workspaceID))
	}
	transaction.sessionRoot, transaction.directoryInfo, err = openRootedDirectory(transaction.workspaceRoot, "control")
	if err != nil {
		return fail(err)
	}
	transaction.metadata, transaction.metadataInfo, err = openRootedRegularFile(ctx, transaction.sessionRoot, "metadata.json", os.O_RDONLY, 0)
	if err != nil {
		return fail(err)
	}
	metadataRaw, err := readOpenedFile(ctx, transaction.metadata, maxSessionMetadataBytes)
	if err == nil {
		err = json.Unmarshal(metadataRaw, &transaction.controlState)
	}
	if err == nil && transaction.controlState.WorkspaceID != protocol.WorkspaceID(workspaceID) {
		err = fmt.Errorf("control metadata workspace ID mismatch")
	}
	if err != nil {
		return fail(err)
	}
	transaction.events, transaction.eventsInfo, err = openRootedRegularFile(ctx, transaction.sessionRoot, "events.jsonl", eventFlags, 0)
	if err != nil {
		return fail(err)
	}
	if err := transaction.verifyEvents(); err != nil {
		return fail(err)
	}
	return transaction, domain.Session{Workspace: workspace, LastSeq: transaction.controlState.LastSeq, UpdatedAt: transaction.controlState.UpdatedAt}, nil
}
