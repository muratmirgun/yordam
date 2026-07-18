package evidence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type legacyAlias struct {
	SessionID  protocol.SessionID  `json:"session_id"`
	ArtifactID string              `json:"artifact_id"`
	EvidenceID protocol.EvidenceID `json:"evidence_id"`
}

func (s *fileStore) MigrateLegacyArtifact(ctx context.Context, sessionID protocol.SessionID, artifactID string) (protocol.EvidenceRecord, error) {
	if !safeName(string(sessionID)) || !safeName(artifactID) {
		return protocol.EvidenceRecord{}, ErrUnsafePath
	}
	if alias, err := s.readLegacyAlias(ctx, sessionID, artifactID); err == nil {
		return s.Get(ctx, alias.EvidenceID)
	} else if !errors.Is(err, os.ErrNotExist) {
		return protocol.EvidenceRecord{}, err
	}
	workspaceID, content, err := s.readLegacyArtifact(ctx, sessionID, artifactID)
	if err != nil {
		return protocol.EvidenceRecord{}, err
	}
	evidenceID := legacyEvidenceID(sessionID, artifactID)
	candidate := protocol.EvidenceCandidate{
		ID: evidenceID, Kind: "legacy_artifact", WorkspaceID: workspaceID, SessionID: sessionID,
		MediaType: "application/octet-stream", ProducingActivityID: "legacy-artifact-migration",
		Actor:   protocol.ActorRef{ID: "legacy-migrator", Kind: protocol.ActorSystem, Source: "jsonl"},
		Subject: protocol.SubjectRef{Kind: "legacy_artifact", ID: artifactID}, Content: content,
	}
	record, err := s.put(ctx, candidate, []string{artifactID})
	if errors.Is(err, ErrEvidenceExists) {
		record, err = s.Get(ctx, evidenceID)
	}
	if err != nil {
		return protocol.EvidenceRecord{}, err
	}
	alias := legacyAlias{SessionID: sessionID, ArtifactID: artifactID, EvidenceID: record.Body.ID}
	if err := s.publishLegacyAlias(ctx, alias); errors.Is(err, os.ErrExist) {
		persisted, readErr := s.readLegacyAlias(ctx, sessionID, artifactID)
		if readErr != nil {
			return protocol.EvidenceRecord{}, readErr
		}
		if persisted.EvidenceID != record.Body.ID {
			return protocol.EvidenceRecord{}, fmt.Errorf("legacy alias publication conflict")
		}
	} else if err != nil {
		return protocol.EvidenceRecord{}, err
	}
	return record, nil
}

func (s *fileStore) readLegacyArtifact(ctx context.Context, sessionID protocol.SessionID, artifactID string) (protocol.WorkspaceID, []byte, error) {
	workspaces := filepath.Join(s.root, "workspaces")
	entries, err := os.ReadDir(workspaces)
	if err != nil {
		return "", nil, err
	}
	var workspaceID protocol.WorkspaceID
	var content []byte
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return "", nil, err
		}
		if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || !safeName(entry.Name()) {
			continue
		}
		path := filepath.Join(workspaces, entry.Name(), "sessions", string(sessionID), "artifacts", artifactID+".bin")
		raw, readErr := readRegular(ctx, path, MaxRetainedBytes+1)
		if os.IsNotExist(readErr) {
			continue
		}
		if readErr != nil {
			return "", nil, readErr
		}
		if workspaceID != "" {
			return "", nil, fmt.Errorf("legacy session identity is ambiguous")
		}
		workspaceID = protocol.WorkspaceID(entry.Name())
		content = raw
	}
	if workspaceID == "" {
		return "", nil, os.ErrNotExist
	}
	return workspaceID, content, nil
}

func (s *fileStore) publishLegacyAlias(ctx context.Context, alias legacyAlias) error {
	directory := filepath.Join("evidence", "aliases", string(alias.SessionID))
	if err := s.ensureDirectory(ctx, directory, 0o700); err != nil {
		return err
	}
	raw, err := canonicaljson.Marshal(alias)
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(filepath.Join(s.root, directory))
	if err != nil {
		return err
	}
	defer root.Close()
	_, err = publishNoReplace(ctx, root, alias.ArtifactID+".json", raw, 0o600)
	return err
}

func (s *fileStore) readLegacyAlias(ctx context.Context, sessionID protocol.SessionID, artifactID string) (legacyAlias, error) {
	path := filepath.Join(s.root, "evidence", "aliases", string(sessionID), artifactID+".json")
	raw, err := readRegular(ctx, path, protocol.MaxByteFieldBytes)
	if err != nil {
		return legacyAlias{}, err
	}
	var alias legacyAlias
	if err := json.Unmarshal(raw, &alias); err != nil {
		return legacyAlias{}, err
	}
	if alias.SessionID != sessionID || alias.ArtifactID != artifactID || !safeName(string(alias.EvidenceID)) {
		return legacyAlias{}, fmt.Errorf("invalid legacy alias metadata")
	}
	return alias, nil
}

func legacyEvidenceID(sessionID protocol.SessionID, artifactID string) protocol.EvidenceID {
	sum := sha256.Sum256([]byte("legacy-artifact\x00" + string(sessionID) + "\x00" + artifactID))
	return protocol.EvidenceID("legacy-" + hex.EncodeToString(sum[:]))
}
