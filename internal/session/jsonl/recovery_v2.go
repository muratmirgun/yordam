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
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
)

const (
	recoveryManifestVersion  = 2
	maxRecoveryJournalBytes  = 64 << 20
	recoveryDiagnosticStatus = "recovery.completed"
	// This covers base64 expansion of two admitted payloads plus a three-line
	// transaction at protocol event caps, with bounded request-string overhead.
	maxRecoveryManifestBytes = int64(8*protocol.MaxEventBytes + 8*protocol.MaxStringBytes)
)

type explicitRecoveryManifest struct {
	Version          uint32                           `json:"version"`
	Request          journal.RecoveryRequest          `json:"request"`
	Observation      recoveryObservation              `json:"observation"`
	PrefixDigest     protocol.Digest                  `json:"prefix_digest"`
	SourceDigest     protocol.Digest                  `json:"source_digest"`
	QuarantineName   string                           `json:"quarantine_name"`
	CandidateName    string                           `json:"candidate_name"`
	MetadataName     string                           `json:"metadata_name"`
	RequestFileName  string                           `json:"request_file_name"`
	RequestTemporary string                           `json:"request_temporary"`
	DiagnosticAppend recoveryDiagnosticAppendManifest `json:"diagnostic_append"`
}

type recoveryDiagnosticAppendManifest struct {
	EventTime            time.Time `json:"event_time"`
	CompatibilityPayload []byte    `json:"compatibility_payload,omitempty"`
	DiagnosticPayload    []byte    `json:"diagnostic_payload"`
	TransactionBytes     []byte    `json:"transaction_bytes"`
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
	guard, err := s.acquireJournalMutation(ctx, request.Journal)
	if errors.Is(err, errJournalLockInitializationRequired) {
		guard, err = s.acquireLegacyRecoveryMutation(ctx, request.Journal)
	}
	if err != nil {
		return journal.RecoveryResult{}, err
	}
	result, recoveryErr := s.recoverSessionLocked(ctx, request, guard)
	return result, errors.Join(recoveryErr, guard.release())
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

func (s *Store) recoverSessionLocked(ctx context.Context, request journal.RecoveryRequest, guard *journalMutationGuard) (result journal.RecoveryResult, resultErr error) {
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
	if !guard.fallback {
		if err := guard.bind(ctx, transaction); err != nil {
			return journal.RecoveryResult{}, err
		}
	}

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
	activeRaw, err := readRecoveryJournal(ctx, transaction.events)
	if err != nil {
		return journal.RecoveryResult{}, err
	}
	if exists {
		if err := validatePersistedRecoveryManifest(ctx, transaction, manifest, request, manifestName, operationHash, activeRaw); err != nil {
			return journal.RecoveryResult{}, err
		}
		if int64(len(activeRaw)) == manifest.Observation.SourceBytes && digestBytes(activeRaw) == manifest.SourceDigest && !recoveryEligible(scan) {
			return journal.RecoveryResult{Status: "conflict", Cursor: scan.head}, nil
		}
	}
	if committed, ok := scan.transactions[request.TransactionID]; ok {
		if !exists {
			return journal.RecoveryResult{Status: "conflict", Cursor: committed.cursor}, nil
		}
		diagnostic, ok := committedRecoveryDiagnostic(committed, request)
		if !ok {
			return journal.RecoveryResult{Status: "conflict", Cursor: committed.cursor}, nil
		}
		uncertain, err := s.syncExactRecoveryMarkerUncertainty(ctx, transaction, request, committed)
		if err != nil {
			return journal.RecoveryResult{}, err
		}
		if err := repairCommittedRecoveryMetadata(ctx, transaction, session, scan); err != nil {
			return journal.RecoveryResult{}, err
		}
		if uncertain {
			s.clearMarkerUncertainty(request.Journal, request.TransactionID)
		}
		return journal.RecoveryResult{
			Status: "already_recovered", Cursor: committed.cursor,
			QuarantineDigest: request.ObservedTailDigest, Diagnostic: diagnostic,
		}, nil
	}
	if exists {
		deterministic, buildErr := s.buildPersistedRecoveryDiagnosticAppend(session, request, manifest, scan)
		if buildErr != nil {
			return journal.RecoveryResult{}, buildErr
		}
		if recoveryDiagnosticAppendIsPartial(scan, manifest, request, activeRaw, deterministic.raw) {
			if err := s.preserveAndResetPartialRecoveryDiagnostic(ctx, transaction, manifest, operationHash, activeRaw); err != nil {
				return journal.RecoveryResult{}, err
			}
			s.clearMarkerUncertainty(request.Journal, request.TransactionID)
			if err := transaction.close(); err != nil {
				transactionOpen = false
				return journal.RecoveryResult{}, err
			}
			transactionOpen = false
			return s.recoverSessionLocked(ctx, request, guard)
		}
	}
	if !exists {
		if !recoveryEligible(scan) {
			return journal.RecoveryResult{Status: "conflict", Cursor: scan.head}, nil
		}
		if scan.head != request.ExpectedHead || scan.validPrefixSize < 0 || scan.validPrefixSize >= int64(len(activeRaw)) {
			return journal.RecoveryResult{Status: "conflict", Cursor: scan.head}, nil
		}
		tailDigest := digestBytes(activeRaw[scan.validPrefixSize:])
		if tailDigest != request.ObservedTailDigest {
			return journal.RecoveryResult{Status: "conflict", Cursor: scan.head}, nil
		}
		manifest = newExplicitRecoveryManifest(request, manifestName, operationHash, activeRaw, scan.validPrefixSize)
		diagnostic := recoveryCompletedDiagnostic(request, manifest)
		validatedHead := rebuiltRecoveryMetadata(session, scan)
		admitted, admissionErr := s.admitRecoveryDiagnosticAppend(validatedHead, request, diagnostic, scan)
		if admissionErr != nil {
			return journal.RecoveryResult{}, admissionErr
		}
		manifest.DiagnosticAppend = admitted
		if err := guard.ensureInitialized(ctx); err != nil {
			return journal.RecoveryResult{}, err
		}
		if err := guard.bind(ctx, transaction); err != nil {
			return journal.RecoveryResult{}, err
		}
		if err := persistExplicitRecoveryManifest(ctx, transaction, manifest); err != nil {
			return journal.RecoveryResult{}, err
		}
		exists = true
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
		var validated recoveryCandidateValidation
		if err := s.runRecoveryAction(FaultCandidateValidate, func() (err error) {
			validated, err = validateRecoveryCandidate(ctx, transaction, manifest, prefix, tail, rebuilt)
			return err
		}); err != nil {
			return journal.RecoveryResult{}, err
		}
		if err := s.runRecoveryAction(FaultCandidateActivate, func() error {
			return s.activateRecoveryCandidate(ctx, transaction, manifest, tail, validated)
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

	diagnostic, diagnosticErr := recoveryDiagnosticFromManifest(manifest, request)
	if diagnosticErr != nil {
		return journal.RecoveryResult{}, diagnosticErr
	}
	appendResult, appendErr := s.commitRecoveryDiagnostic(ctx, request, manifest)
	if appendErr != nil {
		return journal.RecoveryResult{}, appendErr
	}
	return journal.RecoveryResult{
		Status: "recovered", Cursor: appendResult.Cursor,
		QuarantineDigest: request.ObservedTailDigest, Diagnostic: diagnostic,
	}, nil
}

func (s *Store) commitRecoveryDiagnostic(ctx context.Context, request journal.RecoveryRequest, manifest explicitRecoveryManifest) (journal.AppendResult, error) {
	var result journal.AppendResult
	err := s.runRecoveryAction(FaultRecoveryDiagnosticCommit, func() error {
		transaction, session, err := s.openJournal(ctx, request.Journal, os.O_RDWR|os.O_APPEND)
		if err != nil {
			return err
		}
		scan, _, scanErr := s.loadJournalScan(ctx, transaction, request.Journal)
		if scanErr != nil {
			return errors.Join(scanErr, transaction.close())
		}
		if committed, ok := scan.transactions[request.TransactionID]; ok {
			result = journal.AppendResult{Status: journal.AppendCommitted, Cursor: committed.cursor, CurrentHead: committed.cursor}
			return transaction.close()
		}
		deterministic, buildErr := s.buildPersistedRecoveryDiagnosticAppend(session, request, manifest, scan)
		if buildErr != nil {
			return errors.Join(buildErr, transaction.close())
		}
		result, err = s.appendBatchLockedWithIdentity(ctx, transaction, session, deterministic.request, deterministic.identity)
		return errors.Join(err, transaction.close())
	})
	if err != nil {
		return result, err
	}
	if result.Status != journal.AppendCommitted {
		return result, fmt.Errorf("recovery diagnostic append status %q", result.Status)
	}
	s.clearMarkerUncertainty(request.Journal, request.TransactionID)
	return result, nil
}

type deterministicRecoveryDiagnosticAppend struct {
	request  journal.AppendRequest
	identity appendGeneratedIdentity
	raw      []byte
}

func (s *Store) admitRecoveryDiagnosticAppend(
	session domain.Session,
	request journal.RecoveryRequest,
	diagnostic protocol.Diagnostic,
	scan journalScan,
) (recoveryDiagnosticAppendManifest, error) {
	if s.encoder == nil {
		return recoveryDiagnosticAppendManifest{}, fmt.Errorf("journal encoder is required")
	}
	operationPrefix := "recovery:" + recoveryOperationHash(request.OperationID)
	eventTime := session.UpdatedAt.UTC()
	admitted := recoveryDiagnosticAppendManifest{EventTime: eventTime}
	if scan.hasLegacy && !scanHasCommittedV2(scan) {
		payload, err := canonicaljson.Marshal(protocol.MigrationCompatibilityDeclaredV1{
			ReaderVersion: protocol.EnvelopeVersion, WriterVersion: protocol.EnvelopeVersion,
			LegacyHead: request.ExpectedHead, DowngradeStatus: "v0.1_read_only_after_v2",
		})
		if err != nil {
			return recoveryDiagnosticAppendManifest{}, err
		}
		proposed := protocol.ProposedEvent{
			EventID: protocol.EventID(operationPrefix + ":compatibility"), Time: eventTime, PayloadVersion: 1,
			Kind: protocol.EventMigrationCompatibilityDeclared, SessionID: protocol.SessionID(request.Journal.ID), Payload: payload,
		}
		admitted.CompatibilityPayload, err = s.admitRecoveryPayload(proposed)
		if err != nil {
			return recoveryDiagnosticAppendManifest{}, err
		}
	}
	diagnosticPayload, err := canonicaljson.Marshal(protocol.DiagnosticV1{Diagnostic: diagnostic})
	if err != nil {
		return recoveryDiagnosticAppendManifest{}, err
	}
	proposed := protocol.ProposedEvent{
		EventID: protocol.EventID(operationPrefix + ":diagnostic"), Time: eventTime, PayloadVersion: 1,
		Kind: protocol.EventRecoveryDiagnostic, SessionID: protocol.SessionID(request.Journal.ID), Payload: diagnosticPayload,
	}
	admitted.DiagnosticPayload, err = s.admitRecoveryPayload(proposed)
	if err != nil {
		return recoveryDiagnosticAppendManifest{}, err
	}
	deterministic, err := s.buildRecoveryDiagnosticAppend(session, request, admitted, scan)
	if err != nil {
		return recoveryDiagnosticAppendManifest{}, err
	}
	admitted.TransactionBytes = bytes.Clone(deterministic.raw)
	return admitted, nil
}

func (s *Store) admitRecoveryPayload(proposed protocol.ProposedEvent) ([]byte, error) {
	admitted, err := s.admitProposed(protocol.CloneProposedEvent(proposed))
	if err != nil {
		return nil, fmt.Errorf("encode proposed event %q: %w", proposed.EventID, err)
	}
	canonical, err := canonicaljson.Marshal(admitted)
	if err != nil {
		return nil, fmt.Errorf("canonicalize proposed event %q: %w", proposed.EventID, err)
	}
	return bytes.Clone(canonical), nil
}

func (s *Store) buildPersistedRecoveryDiagnosticAppend(
	session domain.Session,
	request journal.RecoveryRequest,
	manifest explicitRecoveryManifest,
	scan journalScan,
) (deterministicRecoveryDiagnosticAppend, error) {
	deterministic, err := s.buildRecoveryDiagnosticAppend(session, request, manifest.DiagnosticAppend, scan)
	if err != nil {
		return deterministicRecoveryDiagnosticAppend{}, err
	}
	if len(manifest.DiagnosticAppend.TransactionBytes) == 0 || !bytes.Equal(deterministic.raw, manifest.DiagnosticAppend.TransactionBytes) {
		return deterministicRecoveryDiagnosticAppend{}, fmt.Errorf("persisted recovery diagnostic transaction bytes do not match admitted payloads")
	}
	deterministic.identity.expectedBytes = bytes.Clone(manifest.DiagnosticAppend.TransactionBytes)
	return deterministic, nil
}

func (s *Store) buildRecoveryDiagnosticAppend(
	session domain.Session,
	request journal.RecoveryRequest,
	admitted recoveryDiagnosticAppendManifest,
	scan journalScan,
) (deterministicRecoveryDiagnosticAppend, error) {
	operationPrefix := "recovery:" + recoveryOperationHash(request.OperationID)
	eventTime := admitted.EventTime.UTC()
	validatedHeadTime := rebuiltRecoveryMetadata(session, scan).UpdatedAt.UTC()
	if admitted.EventTime.IsZero() || !eventTime.Equal(validatedHeadTime) {
		return deterministicRecoveryDiagnosticAppend{}, fmt.Errorf("persisted recovery diagnostic time does not match the validated session head")
	}
	diagnosticPayload, err := canonicalPersistedRecoveryPayload(admitted.DiagnosticPayload)
	if err != nil {
		return deterministicRecoveryDiagnosticAppend{}, fmt.Errorf("persisted recovery diagnostic payload: %w", err)
	}
	if _, err := recoveryDiagnosticFromPayload(diagnosticPayload, request); err != nil {
		return deterministicRecoveryDiagnosticAppend{}, err
	}

	appendRequest := journal.AppendRequest{
		Journal: request.Journal, ExpectedHead: request.ExpectedHead, TransactionID: request.TransactionID,
	}
	identity := appendGeneratedIdentity{
		compatibilityEventID: protocol.EventID(operationPrefix + ":compatibility"),
		compatibilityTime:    eventTime,
		markerEventID:        protocol.EventID(operationPrefix + ":marker"),
		markerTime:           eventTime,
		admittedPayloads:     make(map[protocol.EventID]json.RawMessage, 2),
	}

	seq := request.ExpectedHead.CommitSeq + 1
	envelopes := make([]protocol.EventEnvelope, 0, 2)
	needsCompatibility := scan.hasLegacy && !scanHasCommittedV2(scan)
	if needsCompatibility {
		compatibilityPayload, payloadErr := canonicalPersistedRecoveryPayload(admitted.CompatibilityPayload)
		if payloadErr != nil {
			return deterministicRecoveryDiagnosticAppend{}, fmt.Errorf("persisted recovery compatibility payload: %w", payloadErr)
		}
		var declaration protocol.MigrationCompatibilityDeclaredV1
		if err := json.Unmarshal(compatibilityPayload, &declaration); err != nil || declaration.ReaderVersion != protocol.EnvelopeVersion ||
			declaration.WriterVersion != protocol.EnvelopeVersion || declaration.LegacyHead != request.ExpectedHead ||
			declaration.DowngradeStatus != "v0.1_read_only_after_v2" {
			return deterministicRecoveryDiagnosticAppend{}, fmt.Errorf("persisted recovery compatibility admission is invalid")
		}
		appendRequest.Compatibility = &journal.CompatibilityDeclaration{
			ReaderVersion: protocol.EnvelopeVersion, WriterVersion: protocol.EnvelopeVersion, LegacyHead: request.ExpectedHead,
		}
		identity.admittedPayloads[identity.compatibilityEventID] = protocol.CloneRawMessage(compatibilityPayload)
		envelopes = append(envelopes, protocol.EventEnvelope{
			SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1,
			JournalKind: request.Journal.Kind, JournalID: request.Journal.ID,
			EventID: identity.compatibilityEventID, SessionID: protocol.SessionID(request.Journal.ID),
			Seq: seq, Time: eventTime, Kind: protocol.EventMigrationCompatibilityDeclared,
			TransactionID: request.TransactionID, Payload: compatibilityPayload,
		})
		seq++
	} else if len(admitted.CompatibilityPayload) != 0 {
		return deterministicRecoveryDiagnosticAppend{}, fmt.Errorf("persisted recovery admission has an unexpected compatibility payload")
	}

	diagnosticEvent := protocol.ProposedEvent{
		EventID: protocol.EventID(operationPrefix + ":diagnostic"), Time: eventTime, PayloadVersion: 1,
		Kind: protocol.EventRecoveryDiagnostic, SessionID: protocol.SessionID(request.Journal.ID), Payload: diagnosticPayload,
	}
	identity.admittedPayloads[diagnosticEvent.EventID] = protocol.CloneRawMessage(diagnosticPayload)
	appendRequest.Events = []protocol.ProposedEvent{diagnosticEvent}
	envelopes = append(envelopes, protocol.EventEnvelope{
		SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: diagnosticEvent.PayloadVersion,
		JournalKind: request.Journal.Kind, JournalID: request.Journal.ID,
		EventID: diagnosticEvent.EventID, SessionID: diagnosticEvent.SessionID,
		Seq: seq, Time: diagnosticEvent.Time, Kind: diagnosticEvent.Kind,
		TransactionID: request.TransactionID, Payload: diagnosticPayload,
	})

	digest, err := canonicaljson.TransactionDigest(envelopes)
	if err != nil {
		return deterministicRecoveryDiagnosticAppend{}, err
	}
	markerPayload, err := canonicaljson.Marshal(protocol.TransactionCommittedV1{
		TransactionID: request.TransactionID,
		FirstSeq:      envelopes[0].Seq,
		LastSeq:       envelopes[len(envelopes)-1].Seq,
		EventCount:    uint32(len(envelopes)),
		Digest:        digest,
	})
	if err != nil {
		return deterministicRecoveryDiagnosticAppend{}, err
	}
	marker := protocol.EventEnvelope{
		SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1,
		JournalKind: request.Journal.Kind, JournalID: request.Journal.ID,
		EventID: identity.markerEventID, SessionID: protocol.SessionID(request.Journal.ID),
		Seq: envelopes[len(envelopes)-1].Seq + 1, Time: eventTime, Kind: protocol.EventTransactionCommitted,
		TransactionID: request.TransactionID, Payload: markerPayload,
	}
	raw := make([]byte, 0)
	for _, envelope := range envelopes {
		line, lineErr := encodeLine(envelope)
		if lineErr != nil {
			return deterministicRecoveryDiagnosticAppend{}, lineErr
		}
		record, decodeErr := s.registry.Decode(protocol.CloneRawMessage(line[:len(line)-1]))
		if decodeErr != nil {
			return deterministicRecoveryDiagnosticAppend{}, decodeErr
		}
		if validateErr := s.registry.Validate(record); validateErr != nil {
			return deterministicRecoveryDiagnosticAppend{}, validateErr
		}
		raw = append(raw, line...)
	}
	markerLine, err := encodeLine(marker)
	if err != nil {
		return deterministicRecoveryDiagnosticAppend{}, err
	}
	markerRecord, err := s.registry.Decode(protocol.CloneRawMessage(markerLine[:len(markerLine)-1]))
	if err != nil {
		return deterministicRecoveryDiagnosticAppend{}, err
	}
	if err := s.registry.Validate(markerRecord); err != nil {
		return deterministicRecoveryDiagnosticAppend{}, err
	}
	raw = append(raw, markerLine...)
	return deterministicRecoveryDiagnosticAppend{request: appendRequest, identity: identity, raw: raw}, nil
}

func canonicalPersistedRecoveryPayload(raw []byte) (json.RawMessage, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("payload is empty")
	}
	canonical, err := canonicaljson.Marshal(json.RawMessage(raw))
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(canonical, raw) {
		return nil, fmt.Errorf("payload is not canonical")
	}
	return protocol.CloneRawMessage(canonical), nil
}

func recoveryDiagnosticFromPayload(raw json.RawMessage, request journal.RecoveryRequest) (protocol.Diagnostic, error) {
	var payload protocol.DiagnosticV1
	if err := json.Unmarshal(raw, &payload); err != nil {
		return protocol.Diagnostic{}, fmt.Errorf("decode persisted recovery diagnostic admission: %w", err)
	}
	var details struct {
		OperationID   protocol.ControlOperationID `json:"operation_id"`
		TransactionID protocol.TransactionID      `json:"transaction_id"`
	}
	if payload.Diagnostic.Code != recoveryDiagnosticStatus || payload.Diagnostic.Journal != request.Journal ||
		payload.Diagnostic.AtSeq != request.ExpectedHead.CommitSeq+1 ||
		json.Unmarshal(payload.Diagnostic.Details, &details) != nil || details.OperationID != request.OperationID ||
		details.TransactionID != request.TransactionID {
		return protocol.Diagnostic{}, fmt.Errorf("persisted recovery diagnostic admission does not match the recovery request")
	}
	return protocol.DeepCopy(payload.Diagnostic), nil
}

func recoveryDiagnosticFromManifest(manifest explicitRecoveryManifest, request journal.RecoveryRequest) (protocol.Diagnostic, error) {
	raw, err := canonicalPersistedRecoveryPayload(manifest.DiagnosticAppend.DiagnosticPayload)
	if err != nil {
		return protocol.Diagnostic{}, fmt.Errorf("persisted recovery diagnostic payload: %w", err)
	}
	return recoveryDiagnosticFromPayload(raw, request)
}

func scanHasCommittedV2(scan journalScan) bool {
	for _, commit := range scan.commits {
		if len(commit.envelopes) > 0 {
			return true
		}
	}
	return false
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

func validatePersistedRecoveryManifest(
	ctx context.Context,
	transaction *sessionTransaction,
	manifest explicitRecoveryManifest,
	request journal.RecoveryRequest,
	manifestName string,
	operationHash string,
	active []byte,
) error {
	expectedQuarantine := "recovery-tail-" + operationHash + "-" + request.ObservedTailDigest.Value + ".bin"
	expectedCandidate := ".recovery-candidate-" + operationHash + ".jsonl"
	expectedMetadata := ".recovery-metadata-" + operationHash + ".json"
	expectedTemporary := strings.TrimSuffix(manifestName, ".json") + ".tmp"
	if manifest.Version != recoveryManifestVersion || !reflect.DeepEqual(manifest.Request, request) ||
		manifest.Observation.ObservedTailDigest != request.ObservedTailDigest ||
		manifest.Observation.ValidPrefixBytes < 0 || manifest.Observation.SourceBytes <= manifest.Observation.ValidPrefixBytes ||
		manifest.PrefixDigest.Validate() != nil || manifest.SourceDigest.Validate() != nil ||
		manifest.QuarantineName != expectedQuarantine || manifest.CandidateName != expectedCandidate ||
		manifest.MetadataName != expectedMetadata || manifest.RequestFileName != manifestName ||
		manifest.RequestTemporary != expectedTemporary || manifest.DiagnosticAppend.EventTime.IsZero() ||
		len(manifest.DiagnosticAppend.DiagnosticPayload) == 0 || len(manifest.DiagnosticAppend.TransactionBytes) == 0 ||
		len(manifest.DiagnosticAppend.TransactionBytes) > 3*(protocol.MaxEventBytes+1) {
		return fmt.Errorf("persisted recovery manifest does not match the derived recovery operation")
	}
	for _, name := range []string{manifest.QuarantineName, manifest.CandidateName, manifest.MetadataName, manifest.RequestTemporary} {
		if !safeRecoveryLeaf(name) {
			return fmt.Errorf("persisted recovery manifest contains unsafe leaf %q", name)
		}
	}
	prefixSize := manifest.Observation.ValidPrefixBytes
	if int64(len(active)) < prefixSize || digestBytes(active[:prefixSize]) != manifest.PrefixDigest {
		return fmt.Errorf("persisted recovery prefix identity mismatch")
	}
	if int64(len(active)) == manifest.Observation.SourceBytes && digestBytes(active) == manifest.SourceDigest {
		if digestBytes(active[prefixSize:]) != request.ObservedTailDigest {
			return fmt.Errorf("persisted recovery tail identity mismatch")
		}
		return nil
	}
	if err := transaction.openArtifacts(); err != nil {
		return err
	}
	quarantine, quarantineInfo, err := openRootedRegularFile(ctx, transaction.artifactsRoot, expectedQuarantine, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	tail, readErr := readOpenedFile(ctx, quarantine, maxRecoveryJournalBytes)
	readErr = errors.Join(readErr, verifyRootedRegularFile(transaction.artifactsRoot, expectedQuarantine, quarantineInfo), quarantine.Close())
	if readErr != nil {
		return readErr
	}
	if int64(len(tail)) != manifest.Observation.SourceBytes-prefixSize || digestBytes(tail) != request.ObservedTailDigest {
		return fmt.Errorf("persisted recovery quarantine identity mismatch")
	}
	source := make([]byte, 0, int(prefixSize)+len(tail))
	source = append(source, active[:prefixSize]...)
	source = append(source, tail...)
	if digestBytes(source) != manifest.SourceDigest {
		return fmt.Errorf("persisted recovery source identity mismatch")
	}
	return nil
}

func safeRecoveryLeaf(name string) bool {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\\`) {
		return false
	}
	switch name {
	case "events.jsonl", "metadata.json", "journal.index.json", "artifacts", "workspace.json":
		return false
	default:
		return true
	}
}

func recoveryDiagnosticAppendIsPartial(
	scan journalScan,
	manifest explicitRecoveryManifest,
	request journal.RecoveryRequest,
	active []byte,
	expected []byte,
) bool {
	prefixSize := manifest.Observation.ValidPrefixBytes
	if scan.head != request.ExpectedHead ||
		scan.validPrefixSize != prefixSize || int64(len(active)) <= prefixSize ||
		digestBytes(active[:prefixSize]) != manifest.PrefixDigest {
		return false
	}
	tail := active[prefixSize:]
	return len(tail) < len(expected) && bytes.Equal(tail, expected[:len(tail)])
}

func (s *Store) preserveAndResetPartialRecoveryDiagnostic(
	ctx context.Context,
	transaction *sessionTransaction,
	manifest explicitRecoveryManifest,
	operationHash string,
	active []byte,
) error {
	prefixSize := manifest.Observation.ValidPrefixBytes
	prefix := bytes.Clone(active[:prefixSize])
	partial := bytes.Clone(active[prefixSize:])
	partialDigest := digestBytes(partial)
	partialName := "recovery-diagnostic-tail-" + operationHash + "-" + partialDigest.Value + ".bin"
	if !safeRecoveryLeaf(partialName) {
		return fmt.Errorf("derived recovery diagnostic quarantine name is unsafe")
	}
	if err := transaction.openArtifacts(); err != nil {
		return err
	}
	if err := ensureRootedContents(ctx, transaction.artifactsRoot, partialName, partial, 0o600); err != nil {
		return err
	}
	if err := syncRootedFileAndDirectory(ctx, transaction.artifactsRoot, partialName); err != nil {
		return err
	}
	if err := ensureRootedContents(ctx, transaction.sessionRoot, manifest.CandidateName, prefix, 0o600); err != nil {
		return err
	}
	if err := syncRootedFile(ctx, transaction.sessionRoot, manifest.CandidateName); err != nil {
		return err
	}
	candidate, candidateInfo, err := openRootedRegularFile(ctx, transaction.sessionRoot, manifest.CandidateName, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	candidateRaw, readErr := readOpenedFile(ctx, candidate, int64(len(prefix)))
	if readErr == nil && !bytes.Equal(candidateRaw, prefix) {
		readErr = fmt.Errorf("recovery diagnostic reset candidate differs from validated prefix")
	}
	readErr = errors.Join(readErr, verifyRootedRegularFile(transaction.sessionRoot, manifest.CandidateName, candidateInfo), candidate.Close())
	if readErr != nil {
		return readErr
	}
	if err := transaction.verifyArtifacts(); err != nil {
		return err
	}
	if err := transaction.events.Close(); err != nil {
		return err
	}
	transaction.events = nil
	if err := s.injectFault(FaultRecoveryDiagnosticResetBoundary); err != nil {
		return err
	}
	if err := transaction.verifyMetadata(); err != nil {
		return err
	}
	if err := verifyRootedRegularFile(transaction.sessionRoot, manifest.CandidateName, candidateInfo); err != nil {
		return err
	}
	if err := transaction.sessionRoot.Rename(manifest.CandidateName, "events.jsonl"); err != nil {
		return err
	}
	return syncRootDir(transaction.sessionRoot, ".")
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
	if int64(len(raw)) > maxRecoveryManifestBytes {
		return fmt.Errorf("recovery manifest exceeds %d bytes", maxRecoveryManifestBytes)
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
	raw, readErr := readOpenedFile(ctx, file, maxRecoveryManifestBytes)
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
		if scan.head != manifest.Request.ExpectedHead || digestBytes(prefix) != manifest.PrefixDigest || digestBytes(tail) != manifest.Request.ObservedTailDigest {
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
		if int64(len(tail)) != sourceSize-prefixSize {
			return nil, nil, false, fmt.Errorf("recovery quarantine size mismatch")
		}
		source := append(bytes.Clone(prefix), tail...)
		if digestBytes(source) != manifest.SourceDigest {
			return nil, nil, false, fmt.Errorf("recovery source digest mismatch")
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

func (s *Store) syncExactRecoveryMarkerUncertainty(
	ctx context.Context,
	transaction *sessionTransaction,
	request journal.RecoveryRequest,
	committed scannedCommit,
) (bool, error) {
	value, uncertain := s.state.markerUncertainty.Load(markerUncertaintyKey(request.Journal, request.TransactionID))
	if !uncertain {
		return false, nil
	}
	eventsInfo, ok := value.(os.FileInfo)
	if !ok || eventsInfo == nil || transaction.eventsInfo == nil || !os.SameFile(eventsInfo, transaction.eventsInfo) ||
		committed.cursor.TransactionID != request.TransactionID || committed.cursor.JournalKind != request.Journal.Kind ||
		committed.cursor.JournalID != request.Journal.ID {
		return false, errUnresolvedMarkerDurability
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := transaction.events.Sync(); err != nil {
		return false, err
	}
	if err := errors.Join(ctx.Err(), transaction.verifyEvents()); err != nil {
		return false, err
	}
	return true, nil
}

func repairCommittedRecoveryMetadata(ctx context.Context, transaction *sessionTransaction, session domain.Session, scan journalScan) error {
	want := session
	want.LastSeq = scan.head.CommitSeq
	if len(scan.commits) > 0 {
		last := scan.commits[len(scan.commits)-1]
		if !last.markerTime.IsZero() {
			want.UpdatedAt = last.markerTime
		} else if len(last.events) > 0 {
			record := last.events[len(last.events)-1]
			if record.Legacy != nil {
				want.UpdatedAt = record.Legacy.Time
			} else {
				want.UpdatedAt = record.Envelope.Time
			}
		}
	}
	if reflect.DeepEqual(session, want) {
		return nil
	}
	if err := writeJSONAtomicRooted(ctx, transaction, want); err != nil {
		return err
	}
	_ = writeJournalIndex(ctx, transaction, scan)
	return syncRootDir(transaction.sessionRoot, ".")
}

type recoveryCandidateValidation struct {
	candidate  os.FileInfo
	metadata   os.FileInfo
	quarantine os.FileInfo
	artifacts  os.FileInfo
}

func validateRecoveryCandidate(
	ctx context.Context,
	transaction *sessionTransaction,
	manifest explicitRecoveryManifest,
	prefix []byte,
	tail []byte,
	rebuilt domain.Session,
) (recoveryCandidateValidation, error) {
	var validated recoveryCandidateValidation
	if err := transaction.verifyEvents(); err != nil {
		return validated, err
	}
	if err := transaction.verifyArtifacts(); err != nil {
		return validated, err
	}
	candidate, candidateInfo, err := openRootedRegularFile(ctx, transaction.sessionRoot, manifest.CandidateName, os.O_RDONLY, 0)
	if err != nil {
		return validated, err
	}
	raw, readErr := readOpenedFile(ctx, candidate, int64(len(prefix)))
	if readErr == nil && !bytes.Equal(raw, prefix) {
		readErr = fmt.Errorf("recovery candidate differs from validated prefix")
	}
	readErr = errors.Join(readErr, verifyRootedRegularFile(transaction.sessionRoot, manifest.CandidateName, candidateInfo), candidate.Close())
	if readErr != nil {
		return validated, readErr
	}
	metadata, metadataInfo, err := openRootedRegularFile(ctx, transaction.sessionRoot, manifest.MetadataName, os.O_RDONLY, 0)
	if err != nil {
		return validated, err
	}
	metadataRaw, readErr := readOpenedFile(ctx, metadata, maxSessionMetadataBytes)
	var decoded domain.Session
	if readErr == nil {
		readErr = json.Unmarshal(metadataRaw, &decoded)
	}
	if readErr == nil && !reflect.DeepEqual(decoded, rebuilt) {
		readErr = fmt.Errorf("recovery metadata does not match rebuilt committed head")
	}
	readErr = errors.Join(readErr, verifyRootedRegularFile(transaction.sessionRoot, manifest.MetadataName, metadataInfo), metadata.Close())
	if readErr != nil {
		return validated, readErr
	}
	quarantine, quarantineInfo, err := openRootedRegularFile(ctx, transaction.artifactsRoot, manifest.QuarantineName, os.O_RDONLY, 0)
	if err != nil {
		return validated, err
	}
	quarantineRaw, readErr := readOpenedFile(ctx, quarantine, int64(len(tail)))
	if readErr == nil && !bytes.Equal(quarantineRaw, tail) {
		readErr = fmt.Errorf("recovery quarantine differs from observed tail")
	}
	readErr = errors.Join(readErr, verifyRootedRegularFile(transaction.artifactsRoot, manifest.QuarantineName, quarantineInfo), quarantine.Close())
	if readErr != nil {
		return validated, readErr
	}
	validated = recoveryCandidateValidation{
		candidate: candidateInfo, metadata: metadataInfo, quarantine: quarantineInfo, artifacts: transaction.artifactsInfo,
	}
	return validated, nil
}

func (s *Store) activateRecoveryCandidate(
	ctx context.Context,
	transaction *sessionTransaction,
	manifest explicitRecoveryManifest,
	tail []byte,
	validated recoveryCandidateValidation,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if validated.candidate == nil || validated.metadata == nil || validated.quarantine == nil || validated.artifacts == nil {
		return fmt.Errorf("recovery candidate has no retained validation identity")
	}
	if err := errors.Join(
		transaction.verifyArtifacts(),
		verifyRootedRegularFile(transaction.sessionRoot, manifest.CandidateName, validated.candidate),
		verifyRootedRegularFile(transaction.sessionRoot, manifest.MetadataName, validated.metadata),
		verifyRootedDirectory(transaction.sessionRoot, "artifacts", validated.artifacts),
	); err != nil {
		return err
	}
	quarantine, quarantineInfo, err := openRootedRegularFile(ctx, transaction.artifactsRoot, manifest.QuarantineName, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	quarantineRaw, readErr := readOpenedFile(ctx, quarantine, int64(len(tail)))
	if readErr == nil && (!os.SameFile(quarantineInfo, validated.quarantine) || !bytes.Equal(quarantineRaw, tail)) {
		readErr = fmt.Errorf("recovery quarantine identity changed before activation")
	}
	if readErr = errors.Join(readErr, verifyRootedRegularFile(transaction.artifactsRoot, manifest.QuarantineName, validated.quarantine), quarantine.Close()); readErr != nil {
		return readErr
	}
	if err := transaction.metadata.Close(); err != nil {
		return err
	}
	transaction.metadata = nil
	if err := s.injectFault(FaultRecoveryMetadataRenameBoundary); err != nil {
		return err
	}
	if err := verifyRootedRegularFile(transaction.sessionRoot, manifest.MetadataName, validated.metadata); err != nil {
		return err
	}
	if err := transaction.sessionRoot.Rename(manifest.MetadataName, "metadata.json"); err != nil {
		return err
	}
	if err := transaction.events.Close(); err != nil {
		return err
	}
	transaction.events = nil
	if err := s.injectFault(FaultRecoveryCandidateRenameBoundary); err != nil {
		return err
	}
	if err := verifyRootedRegularFile(transaction.sessionRoot, manifest.CandidateName, validated.candidate); err != nil {
		return err
	}
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
