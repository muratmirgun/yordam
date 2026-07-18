package jsonl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
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

func (s *Store) ensureControlJournalFiles(ctx context.Context, layout *workspaceLayout) error {
	if layout.controlRoot == nil {
		return fmt.Errorf("workspace control directory is not open")
	}
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
		if err := ensureDurableRootedFile(ctx, layout.controlRoot, member.name, member.raw); err != nil {
			return err
		}
	}
	return errors.Join(layout.verify(), syncRootDir(layout.controlRoot, "."))
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
