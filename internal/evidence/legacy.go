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

type legacyAliasBody struct {
	SessionID   protocol.SessionID   `json:"session_id"`
	ArtifactID  string               `json:"artifact_id"`
	WorkspaceID protocol.WorkspaceID `json:"workspace_id"`
	EvidenceID  protocol.EvidenceID  `json:"evidence_id"`
}

type legacyAlias struct {
	Body   legacyAliasBody `json:"body"`
	Digest protocol.Digest `json:"digest"`
}

func (s *fileStore) MigrateLegacyArtifact(ctx context.Context, sessionID protocol.SessionID, artifactID string) (protocol.EvidenceRecord, error) {
	if !safeName(string(sessionID)) || !safeName(artifactID) {
		return protocol.EvidenceRecord{}, ErrUnsafePath
	}
	if alias, err := s.readLegacyAlias(ctx, sessionID, artifactID); err == nil {
		record, getErr := s.Get(ctx, alias.Body.EvidenceID)
		if getErr != nil {
			return protocol.EvidenceRecord{}, getErr
		}
		if err := validateLegacyTarget(record, alias.Body); err != nil {
			return protocol.EvidenceRecord{}, err
		}
		return record, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return protocol.EvidenceRecord{}, err
	}
	if s.legacy == nil {
		return protocol.EvidenceRecord{}, fmt.Errorf("legacy migration requires a rooted resolver")
	}
	workspaceID, content, err := s.legacy.ResolveLegacyArtifact(ctx, sessionID, artifactID)
	if err != nil {
		return protocol.EvidenceRecord{}, err
	}
	if !safeName(string(workspaceID)) {
		return protocol.EvidenceRecord{}, ErrUnsafePath
	}
	evidenceID := legacyEvidenceID(sessionID, artifactID)
	body := legacyAliasBody{SessionID: sessionID, ArtifactID: artifactID, WorkspaceID: workspaceID, EvidenceID: evidenceID}
	digest, err := canonicaljson.Digest(body)
	if err != nil {
		return protocol.EvidenceRecord{}, err
	}
	alias := legacyAlias{Body: body, Digest: digest}
	aliasRaw, err := canonicaljson.Marshal(alias)
	if err != nil {
		return protocol.EvidenceRecord{}, err
	}
	if err := s.admission.Admit(aliasRaw); err != nil {
		return protocol.EvidenceRecord{}, fmt.Errorf("admit final legacy alias: %w", err)
	}
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
	body.EvidenceID = record.Body.ID
	if err := validateLegacyTarget(record, body); err != nil {
		return protocol.EvidenceRecord{}, err
	}
	if err := s.publishLegacyAlias(ctx, alias); errors.Is(err, os.ErrExist) {
		persisted, readErr := s.readLegacyAlias(ctx, sessionID, artifactID)
		if readErr != nil {
			return protocol.EvidenceRecord{}, readErr
		}
		if persisted != alias {
			return protocol.EvidenceRecord{}, fmt.Errorf("legacy alias publication conflict")
		}
	} else if err != nil {
		return protocol.EvidenceRecord{}, err
	}
	return record, nil
}

func validateLegacyTarget(record protocol.EvidenceRecord, alias legacyAliasBody) error {
	wantActor := protocol.ActorRef{ID: "legacy-migrator", Kind: protocol.ActorSystem, Source: "jsonl"}
	wantSubject := protocol.SubjectRef{Kind: "legacy_artifact", ID: alias.ArtifactID}
	if alias.EvidenceID != legacyEvidenceID(alias.SessionID, alias.ArtifactID) ||
		record.Body.ID != alias.EvidenceID || record.Body.WorkspaceID != alias.WorkspaceID ||
		record.Body.SessionID != alias.SessionID || record.Body.Kind != "legacy_artifact" ||
		record.Body.MediaType != "application/octet-stream" || record.Body.ProducingActivityID != "legacy-artifact-migration" ||
		record.Body.Actor != wantActor || record.Body.Subject != wantSubject ||
		len(record.Body.LegacyArtifactAliases) != 1 || record.Body.LegacyArtifactAliases[0] != alias.ArtifactID {
		return fmt.Errorf("legacy evidence target provenance mismatch")
	}
	return nil
}

func (s *fileStore) publishLegacyAlias(ctx context.Context, alias legacyAlias) error {
	directory := filepath.Join("evidence", "aliases", string(alias.Body.SessionID))
	if err := s.ensureDirectory(ctx, directory, 0o700); err != nil {
		return err
	}
	raw, err := canonicaljson.Marshal(alias)
	if err != nil {
		return err
	}
	root, err := s.pinnedRoot(ctx, false)
	if err != nil {
		return err
	}
	_, err = publishNoReplace(ctx, root, filepath.Join(directory, alias.Body.ArtifactID+".json"), raw, 0o600)
	return errors.Join(err, s.verifyRootIdentity())
}

func (s *fileStore) readLegacyAlias(ctx context.Context, sessionID protocol.SessionID, artifactID string) (legacyAlias, error) {
	root, err := s.pinnedRoot(ctx, false)
	if err != nil {
		return legacyAlias{}, err
	}
	path := filepath.Join("evidence", "aliases", string(sessionID), artifactID+".json")
	raw, err := s.readRegularRoot(ctx, root, path, protocol.MaxByteFieldBytes)
	if err != nil {
		return legacyAlias{}, err
	}
	var alias legacyAlias
	if err := json.Unmarshal(raw, &alias); err != nil {
		return legacyAlias{}, err
	}
	if err := canonicaljson.ValidateDigest(alias.Body, alias.Digest); err != nil {
		return legacyAlias{}, fmt.Errorf("invalid legacy alias digest: %w", err)
	}
	if alias.Body.SessionID != sessionID || alias.Body.ArtifactID != artifactID ||
		!safeName(string(alias.Body.WorkspaceID)) || !safeName(string(alias.Body.EvidenceID)) ||
		alias.Body.EvidenceID != legacyEvidenceID(sessionID, artifactID) {
		return legacyAlias{}, fmt.Errorf("invalid legacy alias metadata")
	}
	return alias, nil
}

func legacyEvidenceID(sessionID protocol.SessionID, artifactID string) protocol.EvidenceID {
	sum := sha256.Sum256([]byte("legacy-artifact\x00" + string(sessionID) + "\x00" + artifactID))
	return protocol.EvidenceID("legacy-" + hex.EncodeToString(sum[:]))
}
