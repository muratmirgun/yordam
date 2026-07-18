package jsonl

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
)

const (
	recoveryManifestVersion  = 1
	maxRecoveryJournalBytes  = 64 << 20
	recoveryDiagnosticStatus = "recovery.completed"
)

type explicitRecoveryManifest struct {
	Version          uint32                  `json:"version"`
	Request          journal.RecoveryRequest `json:"request"`
	Observation      recoveryObservation     `json:"observation"`
	PrefixDigest     protocol.Digest         `json:"prefix_digest"`
	SourceDigest     protocol.Digest         `json:"source_digest"`
	QuarantineName   string                  `json:"quarantine_name"`
	CandidateName    string                  `json:"candidate_name"`
	MetadataName     string                  `json:"metadata_name"`
	RequestFileName  string                  `json:"request_file_name"`
	RequestTemporary string                  `json:"request_temporary"`
}

func (s *Store) RecoverSession(ctx context.Context, request journal.RecoveryRequest) (journal.RecoveryResult, error) {
	if err := validateRecoveryRequest(request); err != nil {
		return journal.RecoveryResult{}, err
	}
	lock := s.journalLock(request.Journal)
	if err := lock.lock(ctx); err != nil {
		return journal.RecoveryResult{}, err
	}
	defer lock.unlock()
	return s.recoverSessionLocked(ctx, request)
}

func validateRecoveryRequest(request journal.RecoveryRequest) error {
	if request.OperationID == "" || request.TransactionID == "" {
		return fmt.Errorf("recovery operation ID and transaction ID are required")
	}
	if err := request.Journal.Validate(); err != nil {
		return err
	}
	if request.Journal.Kind != protocol.JournalSession {
		return fmt.Errorf("explicit recovery currently requires a session journal")
	}
	if err := request.ExpectedHead.Validate(); err != nil {
		return fmt.Errorf("invalid recovery expected head: %w", err)
	}
	if request.ExpectedHead.JournalKind != request.Journal.Kind || request.ExpectedHead.JournalID != request.Journal.ID {
		return fmt.Errorf("recovery expected-head journal identity mismatch")
	}
	if err := request.ObservedTailDigest.Validate(); err != nil {
		return fmt.Errorf("invalid observed-tail digest: %w", err)
	}
	if err := protocol.ValidateBounds(request); err != nil {
		return fmt.Errorf("recovery request bounds: %w", err)
	}
	return nil
}

func (s *Store) recoverSessionLocked(ctx context.Context, request journal.RecoveryRequest) (result journal.RecoveryResult, resultErr error) {
	transaction, session, err := s.openJournal(ctx, request.Journal, os.O_RDONLY)
	if err != nil {
		return journal.RecoveryResult{}, err
	}
	transactionOpen := true
	defer func() {
		if transactionOpen {
			resultErr = errors.Join(resultErr, transaction.close())
		}
	}()

	operationHash := recoveryOperationHash(request.OperationID)
	manifestName := ".recovery-request-" + operationHash + ".json"
	manifest, exists, err := loadExplicitRecoveryManifest(ctx, transaction, manifestName)
	if err != nil {
		return journal.RecoveryResult{}, err
	}
	if exists && !reflect.DeepEqual(manifest.Request, request) {
		return journal.RecoveryResult{Status: "conflict"}, nil
	}

	scan, err := s.scanJournal(ctx, transaction, request.Journal)
	if err != nil {
		return journal.RecoveryResult{}, err
	}
	if committed, ok := scan.transactions[request.TransactionID]; ok {
		if !exists {
			return journal.RecoveryResult{Status: "conflict", Cursor: committed.cursor}, nil
		}
		diagnostic, ok := committedRecoveryDiagnostic(committed, request)
		if !ok {
			return journal.RecoveryResult{Status: "conflict", Cursor: committed.cursor}, nil
		}
		return journal.RecoveryResult{
			Status: "already_recovered", Cursor: committed.cursor,
			QuarantineDigest: request.ObservedTailDigest, Diagnostic: diagnostic,
		}, nil
	}

	activeRaw, err := readRecoveryJournal(ctx, transaction.events)
	if err != nil {
		return journal.RecoveryResult{}, err
	}
	if !exists {
		if scan.head != request.ExpectedHead || scan.validPrefixSize < 0 || scan.validPrefixSize >= int64(len(activeRaw)) {
			return journal.RecoveryResult{Status: "conflict", Cursor: scan.head}, nil
		}
		tailDigest := digestBytes(activeRaw[scan.validPrefixSize:])
		if tailDigest != request.ObservedTailDigest {
			return journal.RecoveryResult{Status: "conflict", Cursor: scan.head}, nil
		}
		manifest = newExplicitRecoveryManifest(request, manifestName, operationHash, activeRaw, scan.validPrefixSize)
		if err := persistExplicitRecoveryManifest(ctx, transaction, manifest); err != nil {
			return journal.RecoveryResult{}, err
		}
		exists = true
	}
	if manifest.Version != recoveryManifestVersion || manifest.Observation.ObservedTailDigest != request.ObservedTailDigest || manifest.Observation.ValidPrefixBytes < 0 || manifest.Observation.SourceBytes <= manifest.Observation.ValidPrefixBytes {
		return journal.RecoveryResult{}, fmt.Errorf("persisted recovery manifest is invalid")
	}

	prefix, tail, activated, err := recoveryMaterialFromActive(ctx, transaction, manifest, activeRaw, scan)
	if err != nil {
		return journal.RecoveryResult{}, err
	}
	if !activated {
		if err := transaction.openArtifacts(); err != nil {
			return journal.RecoveryResult{}, err
		}
		if err := s.runRecoveryAction(FaultQuarantineWrite, func() error {
			return ensureRootedContents(ctx, transaction.artifactsRoot, manifest.QuarantineName, tail, 0o600)
		}); err != nil {
			return journal.RecoveryResult{}, err
		}
		if err := s.runRecoveryAction(FaultQuarantineSync, func() error {
			return syncRootedFileAndDirectory(ctx, transaction.artifactsRoot, manifest.QuarantineName)
		}); err != nil {
			return journal.RecoveryResult{}, err
		}
		if err := s.runRecoveryAction(FaultCandidateWrite, func() error {
			return ensureRootedContents(ctx, transaction.sessionRoot, manifest.CandidateName, prefix, 0o600)
		}); err != nil {
			return journal.RecoveryResult{}, err
		}
		if err := s.runRecoveryAction(FaultCandidateSync, func() error {
			return syncRootedFile(ctx, transaction.sessionRoot, manifest.CandidateName)
		}); err != nil {
			return journal.RecoveryResult{}, err
		}

		rebuilt := rebuiltRecoveryMetadata(session, scan)
		metadataRaw, err := json.MarshalIndent(rebuilt, "", "  ")
		if err != nil {
			return journal.RecoveryResult{}, err
		}
		if err := s.runRecoveryAction(FaultRecoveryMetadataWrite, func() error {
			return ensureRootedContents(ctx, transaction.sessionRoot, manifest.MetadataName, metadataRaw, 0o600)
		}); err != nil {
			return journal.RecoveryResult{}, err
		}
		if err := s.runRecoveryAction(FaultRecoveryMetadataSync, func() error {
			return syncRootedFile(ctx, transaction.sessionRoot, manifest.MetadataName)
		}); err != nil {
			return journal.RecoveryResult{}, err
		}
		if err := s.runRecoveryAction(FaultCandidateValidate, func() error {
			return validateRecoveryCandidate(ctx, transaction, manifest, prefix, rebuilt)
		}); err != nil {
			return journal.RecoveryResult{}, err
		}
		if err := s.runRecoveryAction(FaultCandidateActivate, func() error {
			return activateRecoveryCandidate(ctx, transaction, manifest)
		}); err != nil {
			return journal.RecoveryResult{}, err
		}
		activated = true
	}
	if !activated {
		return journal.RecoveryResult{}, fmt.Errorf("recovery candidate was not activated")
	}
	if err := s.runRecoveryAction(FaultRecoveryDirectorySync, func() error {
		return syncRootDir(transaction.sessionRoot, ".")
	}); err != nil {
		return journal.RecoveryResult{}, err
	}
	if err := transaction.close(); err != nil {
		transactionOpen = false
		return journal.RecoveryResult{}, err
	}
	transactionOpen = false

	diagnostic := recoveryCompletedDiagnostic(request, manifest)
	appendResult, appendErr := s.commitRecoveryDiagnostic(ctx, request, diagnostic)
	if appendErr != nil {
		return journal.RecoveryResult{}, appendErr
	}
	return journal.RecoveryResult{
		Status: "recovered", Cursor: appendResult.Cursor,
		QuarantineDigest: request.ObservedTailDigest, Diagnostic: diagnostic,
	}, nil
}

func (s *Store) commitRecoveryDiagnostic(ctx context.Context, request journal.RecoveryRequest, diagnostic protocol.Diagnostic) (journal.AppendResult, error) {
	var result journal.AppendResult
	err := s.runRecoveryAction(FaultRecoveryDiagnosticCommit, func() error {
		transaction, session, err := s.openJournal(ctx, request.Journal, os.O_RDWR|os.O_APPEND)
		if err != nil {
			return err
		}
		payload, err := canonicaljson.Marshal(protocol.DiagnosticV1{Diagnostic: diagnostic})
		if err != nil {
			return errors.Join(err, transaction.close())
		}
		eventID, err := s.nextID()
		if err != nil {
			return errors.Join(err, transaction.close())
		}
		eventTime := s.clock().UTC()
		if eventTime.Before(session.UpdatedAt) {
			eventTime = session.UpdatedAt
		}
		appendRequest := journal.AppendRequest{
			Journal: request.Journal, ExpectedHead: request.ExpectedHead, TransactionID: request.TransactionID,
			Events: []protocol.ProposedEvent{{
				EventID: protocol.EventID(eventID), Time: eventTime, PayloadVersion: 1,
				Kind: protocol.EventRecoveryDiagnostic, SessionID: protocol.SessionID(request.Journal.ID), Payload: payload,
			}},
		}
		scan, _, scanErr := s.loadJournalScan(ctx, transaction, request.Journal)
		if scanErr != nil {
			return errors.Join(scanErr, transaction.close())
		}
		if committed, ok := scan.transactions[request.TransactionID]; ok {
			result = journal.AppendResult{Status: journal.AppendCommitted, Cursor: committed.cursor, CurrentHead: committed.cursor}
			return transaction.close()
		}
		if scan.hasLegacy && !scan.hasV2 {
			appendRequest.Compatibility = &journal.CompatibilityDeclaration{
				ReaderVersion: protocol.EnvelopeVersion, WriterVersion: protocol.EnvelopeVersion, LegacyHead: request.ExpectedHead,
			}
		}
		result, err = s.appendBatchLocked(ctx, transaction, session, appendRequest)
		return errors.Join(err, transaction.close())
	})
	if err != nil {
		return result, err
	}
	if result.Status != journal.AppendCommitted {
		return result, fmt.Errorf("recovery diagnostic append status %q", result.Status)
	}
	return result, nil
}

func (s *Store) runRecoveryAction(point FaultPoint, action func() error) error {
	if err := s.injectFault(point); err != nil {
		return err
	}
	if err := action(); err != nil {
		return err
	}
	return s.injectFault(point)
}

func newExplicitRecoveryManifest(request journal.RecoveryRequest, manifestName, operationHash string, source []byte, prefixSize int64) explicitRecoveryManifest {
	return explicitRecoveryManifest{
		Version: recoveryManifestVersion, Request: request,
		Observation: recoveryObservation{
			ObservedTailDigest: request.ObservedTailDigest, ValidPrefixBytes: prefixSize, SourceBytes: int64(len(source)),
		},
		PrefixDigest:     digestBytes(source[:prefixSize]),
		SourceDigest:     digestBytes(source),
		QuarantineName:   "recovery-tail-" + operationHash + "-" + request.ObservedTailDigest.Value + ".bin",
		CandidateName:    ".recovery-candidate-" + operationHash + ".jsonl",
		MetadataName:     ".recovery-metadata-" + operationHash + ".json",
		RequestFileName:  manifestName,
		RequestTemporary: strings.TrimSuffix(manifestName, ".json") + ".tmp",
	}
}

func recoveryOperationHash(operationID protocol.ControlOperationID) string {
	digest := sha256.Sum256([]byte(operationID))
	return hex.EncodeToString(digest[:])
}

func digestBytes(contents []byte) protocol.Digest {
	digest := sha256.Sum256(contents)
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: hex.EncodeToString(digest[:])}
}

func persistExplicitRecoveryManifest(ctx context.Context, transaction *sessionTransaction, manifest explicitRecoveryManifest) error {
	raw, err := canonicaljson.Marshal(manifest)
	if err != nil {
		return err
	}
	if err := ensureRootedContents(ctx, transaction.sessionRoot, manifest.RequestTemporary, raw, 0o600); err != nil {
		return err
	}
	if err := syncRootedFile(ctx, transaction.sessionRoot, manifest.RequestTemporary); err != nil {
		return err
	}
	if _, err := transaction.sessionRoot.Lstat(manifest.RequestFileName); err == nil {
		return fmt.Errorf("recovery request manifest already exists")
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := transaction.sessionRoot.Rename(manifest.RequestTemporary, manifest.RequestFileName); err != nil {
		return err
	}
	return syncRootDir(transaction.sessionRoot, ".")
}

func loadExplicitRecoveryManifest(ctx context.Context, transaction *sessionTransaction, name string) (explicitRecoveryManifest, bool, error) {
	file, _, err := openRootedRegularFile(ctx, transaction.sessionRoot, name, os.O_RDONLY, 0)
	if os.IsNotExist(err) {
		return explicitRecoveryManifest{}, false, nil
	}
	if err != nil {
		return explicitRecoveryManifest{}, false, err
	}
	raw, readErr := readOpenedFile(ctx, file, maxSessionMetadataBytes)
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return explicitRecoveryManifest{}, false, errors.Join(readErr, closeErr)
	}
	var manifest explicitRecoveryManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return explicitRecoveryManifest{}, false, fmt.Errorf("decode recovery request manifest: %w", err)
	}
	if manifest.RequestFileName != name {
		return explicitRecoveryManifest{}, false, fmt.Errorf("recovery request manifest identity mismatch")
	}
	return manifest, true, nil
}

func readRecoveryJournal(ctx context.Context, file *os.File) ([]byte, error) {
	return readOpenedFile(ctx, file, maxRecoveryJournalBytes)
}

func recoveryMaterialFromActive(
	ctx context.Context,
	transaction *sessionTransaction,
	manifest explicitRecoveryManifest,
	active []byte,
	scan journalScan,
) (prefix []byte, tail []byte, activated bool, err error) {
	prefixSize := manifest.Observation.ValidPrefixBytes
	sourceSize := manifest.Observation.SourceBytes
	switch {
	case int64(len(active)) == sourceSize && digestBytes(active) == manifest.SourceDigest:
		prefix = bytes.Clone(active[:prefixSize])
		tail = bytes.Clone(active[prefixSize:])
		if scan.head != manifest.Request.ExpectedHead || digestBytes(tail) != manifest.Request.ObservedTailDigest {
			return nil, nil, false, fmt.Errorf("active recovery source no longer matches persisted observation")
		}
		return prefix, tail, false, nil
	case int64(len(active)) == prefixSize && digestBytes(active) == manifest.PrefixDigest:
		prefix = bytes.Clone(active)
		if scan.head != manifest.Request.ExpectedHead || scan.validPrefixSize != int64(len(active)) {
			return nil, nil, false, fmt.Errorf("activated recovery prefix no longer matches expected head")
		}
		if err := transaction.openArtifacts(); err != nil {
			return nil, nil, false, err
		}
		file, _, err := openRootedRegularFile(ctx, transaction.artifactsRoot, manifest.QuarantineName, os.O_RDONLY, 0)
		if err != nil {
			return nil, nil, false, err
		}
		tail, readErr := readOpenedFile(ctx, file, maxRecoveryJournalBytes)
		closeErr := file.Close()
		if readErr != nil || closeErr != nil {
			return nil, nil, false, errors.Join(readErr, closeErr)
		}
		if digestBytes(tail) != manifest.Request.ObservedTailDigest {
			return nil, nil, false, fmt.Errorf("recovery quarantine digest mismatch")
		}
		return prefix, tail, true, nil
	default:
		return nil, nil, false, fmt.Errorf("active journal conflicts with persisted recovery operation")
	}
}

func ensureRootedContents(ctx context.Context, root *os.Root, name string, contents []byte, mode os.FileMode) error {
	file, info, err := openRootedRegularFile(ctx, root, name, os.O_RDWR, 0)
	if err == nil {
		if err := verifyRootedRegularFile(root, name, info); err != nil {
			return errors.Join(err, file.Close())
		}
		existing, readErr := readOpenedFile(ctx, file, int64(len(contents))+1)
		if readErr == nil && bytes.Equal(existing, contents) {
			return file.Close()
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(err, file.Close())
		}
		if err := file.Truncate(0); err != nil {
			return errors.Join(err, file.Close())
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return errors.Join(err, file.Close())
		}
		writeErr := writeFull(file, contents)
		if writeErr == nil {
			writeErr = verifyRootedRegularFile(root, name, info)
		}
		return errors.Join(writeErr, file.Close())
	}
	if !os.IsNotExist(err) {
		return err
	}
	file, info, err = openRootedRegularFile(ctx, root, name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	writeErr := writeFull(file, contents)
	if writeErr == nil {
		writeErr = verifyRootedRegularFile(root, name, info)
	}
	return errors.Join(writeErr, file.Close())
}

func syncRootedFile(ctx context.Context, root *os.Root, name string) error {
	file, info, err := openRootedRegularFile(ctx, root, name, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	syncErr := file.Sync()
	if syncErr == nil {
		syncErr = verifyRootedRegularFile(root, name, info)
	}
	return errors.Join(syncErr, file.Close())
}

func syncRootedFileAndDirectory(ctx context.Context, root *os.Root, name string) error {
	return errors.Join(syncRootedFile(ctx, root, name), syncRootDir(root, "."))
}

func rebuiltRecoveryMetadata(session domain.Session, scan journalScan) domain.Session {
	rebuilt := session
	rebuilt.LastSeq = scan.head.CommitSeq
	rebuilt.UpdatedAt = rebuilt.CreatedAt
	if len(scan.events) > 0 {
		last := scan.events[len(scan.events)-1].record
		if last.Legacy != nil {
			rebuilt.UpdatedAt = last.Legacy.Time
		} else {
			rebuilt.UpdatedAt = last.Envelope.Time
		}
	}
	return rebuilt
}

func validateRecoveryCandidate(ctx context.Context, transaction *sessionTransaction, manifest explicitRecoveryManifest, prefix []byte, rebuilt domain.Session) error {
	if err := transaction.verifyEvents(); err != nil {
		return err
	}
	candidate, candidateInfo, err := openRootedRegularFile(ctx, transaction.sessionRoot, manifest.CandidateName, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	raw, readErr := readOpenedFile(ctx, candidate, int64(len(prefix)))
	if readErr == nil && !bytes.Equal(raw, prefix) {
		readErr = fmt.Errorf("recovery candidate differs from validated prefix")
	}
	readErr = errors.Join(readErr, verifyRootedRegularFile(transaction.sessionRoot, manifest.CandidateName, candidateInfo), candidate.Close())
	if readErr != nil {
		return readErr
	}
	metadata, metadataInfo, err := openRootedRegularFile(ctx, transaction.sessionRoot, manifest.MetadataName, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	metadataRaw, readErr := readOpenedFile(ctx, metadata, maxSessionMetadataBytes)
	var decoded domain.Session
	if readErr == nil {
		readErr = json.Unmarshal(metadataRaw, &decoded)
	}
	if readErr == nil && !reflect.DeepEqual(decoded, rebuilt) {
		readErr = fmt.Errorf("recovery metadata does not match rebuilt committed head")
	}
	return errors.Join(readErr, verifyRootedRegularFile(transaction.sessionRoot, manifest.MetadataName, metadataInfo), metadata.Close())
}

func activateRecoveryCandidate(ctx context.Context, transaction *sessionTransaction, manifest explicitRecoveryManifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := transaction.verifyEvents(); err != nil {
		return err
	}
	if err := transaction.metadata.Close(); err != nil {
		return err
	}
	transaction.metadata = nil
	if err := transaction.sessionRoot.Rename(manifest.MetadataName, "metadata.json"); err != nil {
		return err
	}
	if err := transaction.events.Close(); err != nil {
		return err
	}
	transaction.events = nil
	if err := transaction.sessionRoot.Rename(manifest.CandidateName, "events.jsonl"); err != nil {
		return err
	}
	return nil
}

func recoveryCompletedDiagnostic(request journal.RecoveryRequest, manifest explicitRecoveryManifest) protocol.Diagnostic {
	details, _ := canonicaljson.Marshal(map[string]any{
		"operation_id": request.OperationID, "transaction_id": request.TransactionID,
		"observed_tail_digest": request.ObservedTailDigest, "quarantine_name": manifest.QuarantineName,
		"valid_prefix_bytes": manifest.Observation.ValidPrefixBytes, "source_bytes": manifest.Observation.SourceBytes,
	})
	return protocol.Diagnostic{
		Code: recoveryDiagnosticStatus, Message: "journal recovery preserved the observed tail and activated the validated prefix",
		Journal: request.Journal, AtSeq: request.ExpectedHead.CommitSeq + 1, Details: details,
	}
}

func committedRecoveryDiagnostic(commit scannedCommit, request journal.RecoveryRequest) (protocol.Diagnostic, bool) {
	for _, record := range commit.events {
		if record.Envelope.Kind != protocol.EventRecoveryDiagnostic {
			continue
		}
		payload, ok := record.Decoded.(*protocol.DiagnosticV1)
		if !ok || payload.Diagnostic.Code != recoveryDiagnosticStatus || payload.Diagnostic.Journal != request.Journal {
			continue
		}
		var details struct {
			OperationID protocol.ControlOperationID `json:"operation_id"`
		}
		if json.Unmarshal(payload.Diagnostic.Details, &details) != nil || details.OperationID != request.OperationID {
			continue
		}
		return protocol.DeepCopy(payload.Diagnostic), true
	}
	return protocol.Diagnostic{}, false
}
