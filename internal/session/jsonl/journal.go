package jsonl

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"reflect"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/eventcodec"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type sanitizeEncoder struct {
	sanitize func(any) (json.RawMessage, error)
}

func (e sanitizeEncoder) EncodeProposed(event protocol.ProposedEvent) (json.RawMessage, error) {
	if err := protocol.ValidateRawJSON(event.Payload); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(event.Payload))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return e.sanitize(value)
}

type scannedEvent struct {
	record protocol.EventRecord
	cursor protocol.CommittedCursor
}

type scannedCommit struct {
	cursor      protocol.CommittedCursor
	events      []protocol.EventRecord
	envelopes   []protocol.EventEnvelope
	markerTime  time.Time
	firstOffset int64
	endOffset   int64
}

type journalScan struct {
	ref                   protocol.JournalRef
	head                  protocol.CommittedCursor
	events                []scannedEvent
	commits               []scannedCommit
	transactions          map[protocol.TransactionID]scannedCommit
	eventIDs              map[protocol.EventID]struct{}
	eventIDOrder          []protocol.EventID
	physicalSeq           uint64
	writable              bool
	diagnostics           []protocol.Diagnostic
	incompleteTransaction protocol.TransactionID
	incompleteTail        bool
	hasLegacy             bool
	hasV2                 bool
	sourceSize            int64
	sourceDigest          protocol.Digest
	validPrefixSize       int64
}

func (s *Store) openJournal(
	ctx context.Context,
	ref protocol.JournalRef,
	flags int,
) (*sessionTransaction, domain.Session, error) {
	if err := ref.Validate(); err != nil {
		return nil, domain.Session{}, err
	}
	if ref.Kind == protocol.JournalWorkspaceControl {
		return s.openControlTransaction(ctx, string(ref.ID), flags)
	}
	return s.openSessionTransaction(ctx, string(ref.ID), flags, 0)
}

func (s *Store) scanJournal(ctx context.Context, transaction *sessionTransaction, ref protocol.JournalRef) (journalScan, error) {
	return s.scanJournalAfterPrefix(ctx, transaction, ref, journalScan{}, sha256.New())
}

func (s *Store) scanJournalAfterPrefix(
	ctx context.Context,
	transaction *sessionTransaction,
	ref protocol.JournalRef,
	prefix journalScan,
	prefixHash hash.Hash,
) (journalScan, error) {
	scan := cloneJournalScan(prefix)
	if scan.transactions == nil {
		scan = journalScan{
			ref: ref, writable: true,
			transactions: make(map[protocol.TransactionID]scannedCommit),
			eventIDs:     make(map[protocol.EventID]struct{}),
		}
	}
	if err := transaction.verifyEvents(); err != nil {
		return scan, err
	}
	if _, err := transaction.events.Seek(scan.sourceSize, io.SeekStart); err != nil {
		return scan, err
	}
	reader := bufio.NewReaderSize(transaction.events, 64*1024)
	seenEvents := make(map[protocol.EventID]struct{}, len(scan.eventIDs))
	for eventID := range scan.eventIDs {
		seenEvents[eventID] = struct{}{}
	}
	var pendingRecords []protocol.EventRecord
	var pendingEnvelopes []protocol.EventEnvelope
	var pendingID protocol.TransactionID
	var pendingOffset int64
	hasV2 := scan.hasV2
	committedV2 := scan.hasV2
	legacyState := legacyStateAfterScan(scan, protocol.SessionID(ref.ID))
	offset := scan.sourceSize
scanLines:
	for {
		if err := ctx.Err(); err != nil {
			return scan, err
		}
		physical, complete, eof, err := readJournalLine(reader)
		if err != nil {
			if errors.Is(err, errJournalCompleteLineTooLarge) {
				scan.writable = false
				scan.incompleteTail = true
				scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "invalid_known_payload", err.Error(), scan.physicalSeq+1, ""))
				break
			}
			if errors.Is(err, errJournalLineTooLarge) {
				scan.writable = false
				scan.incompleteTail = true
				scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "incomplete_final_fragment", err.Error(), scan.physicalSeq+1, ""))
				break
			}
			return scan, err
		}
		if len(physical) == 0 && eof {
			break
		}
		_, _ = prefixHash.Write(physical)
		scan.sourceSize += int64(len(physical))
		lineOffset := offset
		offset += int64(len(physical))
		if !complete {
			scan.incompleteTail = true
			scan.writable = false
			scan.incompleteTransaction = pendingID
			if scan.incompleteTransaction == "" {
				var identity struct {
					TransactionID protocol.TransactionID `json:"transaction_id"`
				}
				_ = json.Unmarshal(physical, &identity)
				scan.incompleteTransaction = identity.TransactionID
			}
			scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "incomplete_final_fragment", "journal has an incomplete final line", scan.physicalSeq+1, ""))
			break
		}
		line := physical[:len(physical)-1]
		if len(line) == 0 {
			scan.writable = false
			scan.incompleteTail = true
			scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "invalid_known_payload", fmt.Sprintf("empty journal line at sequence %d", scan.physicalSeq+1), scan.physicalSeq+1, ""))
			break
		}
		var identity struct {
			SchemaVersion uint32 `json:"schema_version"`
		}
		if err := json.Unmarshal(line, &identity); err != nil {
			scan.writable = false
			scan.incompleteTail = true
			scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "invalid_known_payload", fmt.Sprintf("decode journal line %d: %v", scan.physicalSeq+1, err), scan.physicalSeq+1, ""))
			break
		}
		switch identity.SchemaVersion {
		case 1:
			scan.hasLegacy = true
			if hasV2 || pendingID != "" {
				return scan, fmt.Errorf("v1 event follows v2 journal data")
			}
			var legacy domain.DurableEvent
			if err := json.Unmarshal(line, &legacy); err != nil {
				scan.writable = false
				scan.incompleteTail = true
				scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "invalid_known_payload", fmt.Sprintf("decode v1 event: %v", err), scan.physicalSeq+1, ""))
				break scanLines
			}
			if err := legacy.Validate(); err != nil {
				scan.writable = false
				scan.incompleteTail = true
				scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "invalid_known_payload", err.Error(), scan.physicalSeq+1, protocol.EventID(legacy.EventID)))
				break scanLines
			}
			if ref.Kind != protocol.JournalSession || protocol.JournalID(legacy.SessionID) != ref.ID {
				scan.writable = false
				scan.incompleteTail = true
				scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "invalid_known_payload", "v1 journal identity mismatch", scan.physicalSeq+1, protocol.EventID(legacy.EventID)))
				break scanLines
			}
			if legacy.Seq != scan.physicalSeq+1 {
				scan.writable = false
				scan.incompleteTail = true
				scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "invalid_sequence", fmt.Sprintf("v1 sequence=%d want %d", legacy.Seq, scan.physicalSeq+1), scan.physicalSeq+1, protocol.EventID(legacy.EventID)))
				break scanLines
			}
			raw := protocol.CloneRawMessage(line)
			source := protocol.LegacySource{
				SchemaVersion: 1, EventID: protocol.EventID(legacy.EventID), SessionID: protocol.SessionID(legacy.SessionID),
				Seq: legacy.Seq, Time: legacy.Time, Kind: string(legacy.Kind),
				Payload: protocol.CloneRawMessage(legacy.Payload), RawEnvelope: protocol.CloneRawMessage(raw),
			}
			mapped, nextLegacyState, migrationDiagnostics := UpcastV1(source, legacyState)
			if diagnostic, invalid := diagnosticWithCode(migrationDiagnostics, "migration.invalid_payload"); invalid {
				scan.writable = false
				scan.incompleteTail = true
				scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "invalid_known_payload", diagnostic.Message, scan.physicalSeq+1, source.EventID))
				break scanLines
			}
			if err := validateMappedLegacyRecord(s.registry, mapped); err != nil {
				scan.writable = false
				scan.incompleteTail = true
				scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "invalid_known_payload", fmt.Sprintf("validate mapped v1 event: %v", err), scan.physicalSeq+1, source.EventID))
				break scanLines
			}
			legacyState = nextLegacyState
			eventID := protocol.EventID(legacy.EventID)
			if _, duplicate := seenEvents[eventID]; duplicate {
				return scan, fmt.Errorf("duplicate event ID %q", eventID)
			}
			seenEvents[eventID] = struct{}{}
			scan.eventIDs[eventID] = struct{}{}
			scan.eventIDOrder = append(scan.eventIDOrder, eventID)
			transactionID := protocol.TransactionID("legacy:" + legacy.EventID)
			if _, duplicate := scan.transactions[transactionID]; duplicate {
				return scan, fmt.Errorf("duplicate transaction ID %q", transactionID)
			}
			record := protocol.EventRecord{
				RawEnvelope: raw,
				Legacy:      &source,
			}
			cursor := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: legacy.Seq, TransactionID: transactionID}
			commit := scannedCommit{cursor: cursor, events: []protocol.EventRecord{record}, firstOffset: lineOffset, endOffset: offset}
			scan.events = append(scan.events, scannedEvent{record: record, cursor: cursor})
			scan.commits = append(scan.commits, commit)
			scan.transactions[transactionID] = commit
			scan.head = cursor
			scan.physicalSeq = legacy.Seq
			scan.validPrefixSize = offset
		case protocol.EnvelopeVersion:
			scan.hasV2 = true
			hasV2 = true
			record, decodeErr := s.registry.Decode(protocol.CloneRawMessage(line))
			unknown := false
			var unknownKind *eventcodec.UnknownKindError
			var unsupported *eventcodec.UnsupportedPayloadVersionError
			if errors.As(decodeErr, &unknownKind) || errors.As(decodeErr, &unsupported) {
				unknown = true
				scan.writable = false
				scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "unsupported_event", decodeErr.Error(), record.Envelope.Seq, record.Envelope.EventID))
			} else if decodeErr != nil {
				scan.writable = false
				scan.incompleteTail = true
				scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "invalid_known_payload", fmt.Sprintf("decode v2 event: %v", decodeErr), scan.physicalSeq+1, record.Envelope.EventID))
				break scanLines
			} else if err := s.registry.Validate(record); err != nil {
				scan.writable = false
				scan.incompleteTail = true
				scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "invalid_known_payload", fmt.Sprintf("validate v2 event: %v", err), scan.physicalSeq+1, record.Envelope.EventID))
				break scanLines
			}
			envelope := record.Envelope
			if envelope.JournalKind != ref.Kind || envelope.JournalID != ref.ID {
				scan.writable = false
				scan.incompleteTail = true
				scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "invalid_known_payload", "v2 journal identity mismatch", scan.physicalSeq+1, envelope.EventID))
				break scanLines
			}
			if envelope.Seq != scan.physicalSeq+1 {
				scan.writable = false
				scan.incompleteTail = true
				scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "invalid_sequence", fmt.Sprintf("v2 sequence=%d want %d", envelope.Seq, scan.physicalSeq+1), scan.physicalSeq+1, envelope.EventID))
				break scanLines
			}
			if _, duplicate := seenEvents[envelope.EventID]; duplicate {
				return scan, fmt.Errorf("duplicate event ID %q", envelope.EventID)
			}
			seenEvents[envelope.EventID] = struct{}{}
			scan.eventIDs[envelope.EventID] = struct{}{}
			scan.eventIDOrder = append(scan.eventIDOrder, envelope.EventID)
			scan.physicalSeq = envelope.Seq
			if envelope.Kind == protocol.EventTransactionCommitted && unknown {
				scan.incompleteTail = true
				scan.incompleteTransaction = envelope.TransactionID
				break scanLines
			}
			if envelope.Kind == protocol.EventTransactionCommitted && !unknown {
				if pendingID == "" || len(pendingEnvelopes) == 0 {
					return scan, fmt.Errorf("transaction marker has no events")
				}
				if envelope.TransactionID != pendingID {
					return scan, fmt.Errorf("transaction marker ID %q want %q", envelope.TransactionID, pendingID)
				}
				marker, ok := record.Decoded.(*protocol.TransactionCommittedV1)
				if !ok {
					return scan, fmt.Errorf("transaction marker payload type %T", record.Decoded)
				}
				if marker.FirstSeq != pendingEnvelopes[0].Seq || marker.LastSeq != pendingEnvelopes[len(pendingEnvelopes)-1].Seq || uint64(marker.EventCount) != uint64(len(pendingEnvelopes)) {
					return scan, fmt.Errorf("transaction marker bounds/count mismatch")
				}
				digest, err := canonicaljson.TransactionDigest(pendingEnvelopes)
				if err != nil {
					return scan, err
				}
				if marker.Digest != digest {
					return scan, fmt.Errorf("transaction marker digest mismatch")
				}
				if err := validateCompatibilityTransition(scan.hasLegacy, committedV2, scan.head, pendingRecords); err != nil {
					scan.writable = false
					scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "invalid_transition", err.Error(), pendingEnvelopes[0].Seq, pendingEnvelopes[0].EventID))
					rollbackPendingEventIDs(&scan, pendingEnvelopes, envelope.EventID)
					pendingRecords, pendingEnvelopes = nil, nil
					pendingID = ""
					break scanLines
				}
				if _, duplicate := scan.transactions[pendingID]; duplicate {
					return scan, fmt.Errorf("duplicate transaction ID %q", pendingID)
				}
				cursor := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: envelope.Seq, TransactionID: pendingID}
				commit := scannedCommit{
					cursor: cursor, events: cloneRecords(pendingRecords), envelopes: cloneEnvelopes(pendingEnvelopes),
					markerTime: envelope.Time, firstOffset: pendingOffset, endOffset: offset,
				}
				for _, pendingRecord := range pendingRecords {
					scan.events = append(scan.events, scannedEvent{record: protocol.CloneEventRecord(pendingRecord), cursor: cursor})
				}
				scan.commits = append(scan.commits, commit)
				scan.transactions[pendingID] = commit
				scan.head = cursor
				scan.validPrefixSize = offset
				committedV2 = true
				pendingRecords, pendingEnvelopes = nil, nil
				pendingID = ""
				continue
			}
			if len(pendingEnvelopes) >= 1000 {
				return scan, fmt.Errorf("physical transaction exceeds 1000 events")
			}
			if pendingID == "" {
				if _, duplicate := scan.transactions[envelope.TransactionID]; duplicate {
					return scan, fmt.Errorf("duplicate transaction ID %q", envelope.TransactionID)
				}
				pendingID = envelope.TransactionID
				pendingOffset = lineOffset
			} else if envelope.TransactionID != pendingID {
				return scan, fmt.Errorf("interleaved transaction %q before marker for %q", envelope.TransactionID, pendingID)
			}
			pendingRecords = append(pendingRecords, protocol.CloneEventRecord(record))
			pendingEnvelopes = append(pendingEnvelopes, protocol.CloneEventEnvelope(envelope))
		default:
			scan.writable = false
			scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "unsupported_envelope_version", fmt.Sprintf("unsupported envelope version %d", identity.SchemaVersion), scan.physicalSeq+1, ""))
			scan.incompleteTail = true
			break
		}
		if identity.SchemaVersion != 1 && identity.SchemaVersion != protocol.EnvelopeVersion {
			break
		}
		if eof {
			break
		}
	}
	if pendingID != "" {
		scan.incompleteTail = true
		scan.writable = false
		scan.incompleteTransaction = pendingID
		scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "incomplete_transaction", "transaction has no durable commit marker", scan.physicalSeq, pendingEnvelopes[len(pendingEnvelopes)-1].EventID))
	}
	scan.sourceDigest = protocol.Digest{Algorithm: protocol.DigestSHA256, Value: hex.EncodeToString(prefixHash.Sum(nil))}
	if info, err := transaction.events.Stat(); err == nil {
		scan.sourceSize = info.Size()
	}
	if err := transaction.verifyEvents(); err != nil {
		return scan, err
	}
	return scan, nil
}

func cloneJournalScan(scan journalScan) journalScan {
	clone := scan
	clone.events = make([]scannedEvent, len(scan.events))
	for index := range scan.events {
		clone.events[index] = scannedEvent{
			record: protocol.CloneEventRecord(scan.events[index].record),
			cursor: scan.events[index].cursor,
		}
	}
	clone.commits = make([]scannedCommit, len(scan.commits))
	if scan.transactions != nil {
		clone.transactions = make(map[protocol.TransactionID]scannedCommit, len(scan.transactions))
	}
	for index := range scan.commits {
		commit := cloneScannedCommit(scan.commits[index])
		clone.commits[index] = commit
		clone.transactions[commit.cursor.TransactionID] = commit
	}
	if scan.eventIDs != nil {
		clone.eventIDs = make(map[protocol.EventID]struct{}, len(scan.eventIDs))
		for eventID := range scan.eventIDs {
			clone.eventIDs[eventID] = struct{}{}
		}
	}
	clone.eventIDOrder = append([]protocol.EventID(nil), scan.eventIDOrder...)
	clone.diagnostics = cloneDiagnostics(scan.diagnostics)
	return clone
}

func legacyStateAfterScan(scan journalScan, sessionID protocol.SessionID) UpcastState {
	state := UpcastState{SessionID: sessionID}
	for _, scanned := range scan.events {
		if scanned.record.Legacy == nil {
			continue
		}
		_, state, _ = UpcastV1(*scanned.record.Legacy, state)
	}
	return state
}

func diagnosticWithCode(diagnostics []protocol.Diagnostic, code string) (protocol.Diagnostic, bool) {
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == code {
			return diagnostic, true
		}
	}
	return protocol.Diagnostic{}, false
}

func validateMappedLegacyRecord(registry *eventcodec.Registry, record protocol.EventRecord) error {
	descriptor, ok := registry.Descriptor(record.Envelope.Kind, record.Envelope.PayloadVersion)
	if !ok {
		return fmt.Errorf("mapped legacy event kind %q has no v2 descriptor", record.Envelope.Kind)
	}
	if record.Decoded == nil || reflect.TypeOf(record.Decoded) != reflect.TypeOf(descriptor.New()) {
		return fmt.Errorf("mapped legacy decoded payload type %T is invalid", record.Decoded)
	}
	if descriptor.ValidateSemantic != nil {
		if err := descriptor.ValidateSemantic(record.Decoded); err != nil {
			return fmt.Errorf("payload semantics %s@%d: %w", descriptor.Kind, descriptor.Version, err)
		}
	}
	return nil
}

func validateCompatibilityTransition(hasLegacy, committedV2 bool, legacyHead protocol.CommittedCursor, records []protocol.EventRecord) error {
	declarationCount := 0
	declarationIndex := -1
	var declaration *protocol.MigrationCompatibilityDeclaredV1
	for index, record := range records {
		if record.Envelope.Kind != protocol.EventMigrationCompatibilityDeclared {
			continue
		}
		declarationCount++
		declarationIndex = index
		declaration, _ = record.Decoded.(*protocol.MigrationCompatibilityDeclaredV1)
	}
	if !hasLegacy || committedV2 {
		if declarationCount != 0 {
			return fmt.Errorf("migration compatibility declaration is only valid as the first event of the first v2 transaction after legacy data")
		}
		return nil
	}
	if declarationCount != 1 || declarationIndex != 0 || declaration == nil {
		return fmt.Errorf("first committed v2 transaction after legacy data requires exactly one compatibility declaration as its first event")
	}
	if declaration.ReaderVersion != protocol.EnvelopeVersion || declaration.WriterVersion != protocol.EnvelopeVersion {
		return fmt.Errorf("migration compatibility reader/writer versions must both be %d", protocol.EnvelopeVersion)
	}
	if declaration.LegacyHead != legacyHead {
		return fmt.Errorf("migration compatibility legacy head does not match the exact validated legacy head")
	}
	if declaration.DowngradeStatus != "v0.1_read_only_after_v2" {
		return fmt.Errorf("migration compatibility downgrade status is invalid")
	}
	return nil
}

func rollbackPendingEventIDs(scan *journalScan, pending []protocol.EventEnvelope, markerID protocol.EventID) {
	for _, envelope := range pending {
		delete(scan.eventIDs, envelope.EventID)
	}
	delete(scan.eventIDs, markerID)
	remove := len(pending) + 1
	if remove <= len(scan.eventIDOrder) {
		scan.eventIDOrder = scan.eventIDOrder[:len(scan.eventIDOrder)-remove]
	}
}

func cloneScannedCommit(commit scannedCommit) scannedCommit {
	commit.events = cloneRecords(commit.events)
	commit.envelopes = cloneEnvelopes(commit.envelopes)
	return commit
}

var errJournalLineTooLarge = errors.New("journal line exceeds maximum event size")
var errJournalCompleteLineTooLarge = errors.New("complete journal line exceeds maximum event size")

func readJournalLine(reader *bufio.Reader) (line []byte, complete, eof bool, err error) {
	var pending bytes.Buffer
	for {
		fragment, readErr := reader.ReadSlice('\n')
		if pending.Len()+len(fragment) > protocol.MaxEventBytes+1 {
			if len(fragment) > 0 && fragment[len(fragment)-1] == '\n' {
				return nil, false, false, fmt.Errorf("%w: %d bytes", errJournalCompleteLineTooLarge, protocol.MaxEventBytes)
			}
			return nil, false, false, fmt.Errorf("%w: %d bytes", errJournalLineTooLarge, protocol.MaxEventBytes)
		}
		pending.Write(fragment)
		if errors.Is(readErr, bufio.ErrBufferFull) {
			continue
		}
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return nil, false, false, readErr
		}
		contents := bytes.Clone(pending.Bytes())
		return contents, len(contents) > 0 && contents[len(contents)-1] == '\n', errors.Is(readErr, io.EOF), nil
	}
}

func journalDiagnostic(ref protocol.JournalRef, code, message string, seq uint64, eventID protocol.EventID) protocol.Diagnostic {
	return protocol.Diagnostic{Code: code, Message: message, Journal: ref, AtSeq: seq, EventID: eventID}
}

func cloneRecords(records []protocol.EventRecord) []protocol.EventRecord {
	out := make([]protocol.EventRecord, len(records))
	for index := range records {
		out[index] = protocol.CloneEventRecord(records[index])
	}
	return out
}

func cloneEnvelopes(events []protocol.EventEnvelope) []protocol.EventEnvelope {
	out := make([]protocol.EventEnvelope, len(events))
	for index := range events {
		out[index] = protocol.CloneEventEnvelope(events[index])
	}
	return out
}

func cloneDiagnostics(diagnostics []protocol.Diagnostic) []protocol.Diagnostic {
	out := make([]protocol.Diagnostic, len(diagnostics))
	for index := range diagnostics {
		out[index] = protocol.DeepCopy(diagnostics[index])
	}
	return out
}

func (s *Store) Inspect(ctx context.Context, ref protocol.JournalRef) (journal.Inspection, error) {
	lock := s.journalLock(ref)
	if err := lock.lock(ctx); err != nil {
		return journal.Inspection{}, err
	}
	defer lock.unlock()
	transaction, _, err := s.openJournal(ctx, ref, os.O_RDONLY)
	if err != nil {
		return journal.Inspection{}, err
	}
	scan, scanErr := s.inspectJournalTransaction(ctx, transaction, ref)
	closeErr := transaction.close()
	if scanErr != nil || closeErr != nil {
		return journal.Inspection{}, errors.Join(scanErr, closeErr)
	}
	if diagnostic, ok := fatalLegacyInspectDiagnostic(scan.diagnostics); ok {
		return journal.Inspection{}, fmt.Errorf("%s: %s", diagnostic.Code, diagnostic.Message)
	}
	events, migrationDiagnostics := upcastScannedEvents(scan, protocol.SessionID(ref.ID))
	return journal.Inspection{
		Journal: ref, Head: scan.head, Events: events, Writable: scan.writable,
		Diagnostics: append(cloneDiagnostics(scan.diagnostics), migrationDiagnostics...), IncompleteTransaction: scan.incompleteTransaction,
	}, nil
}

func fatalLegacyInspectDiagnostic(diagnostics []protocol.Diagnostic) (protocol.Diagnostic, bool) {
	for _, diagnostic := range diagnostics {
		switch diagnostic.Code {
		case "invalid_known_payload", "invalid_sequence", "invalid_transition":
			return diagnostic, true
		}
	}
	return protocol.Diagnostic{}, false
}

func (s *Store) InspectSession(ctx context.Context, sessionID protocol.SessionID) (journal.SessionInspection, error) {
	if err := validateSessionID(string(sessionID)); err != nil {
		return journal.SessionInspection{}, err
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(sessionID)}
	lock := s.journalLock(ref)
	if err := lock.lock(ctx); err != nil {
		return journal.SessionInspection{}, err
	}
	defer lock.unlock()
	transaction, session, err := s.openJournal(ctx, ref, os.O_RDONLY)
	var missingLog *missingEventLogError
	if errors.As(err, &missingLog) && session.LastSeq > 0 {
		return journal.SessionInspection{
			Session: session,
			Journal: journal.Inspection{
				Journal:  ref,
				Writable: false,
				Diagnostics: []protocol.Diagnostic{{
					Code: "missing_event_log", Message: missingLog.Error(), Journal: ref,
				}},
			},
		}, nil
	}
	if err != nil {
		return journal.SessionInspection{}, err
	}
	scan, scanErr := s.inspectJournalTransaction(ctx, transaction, ref)
	closeErr := transaction.close()
	if scanErr != nil || closeErr != nil {
		return journal.SessionInspection{}, errors.Join(scanErr, closeErr)
	}
	events, migrationDiagnostics := upcastScannedEvents(scan, sessionID)
	diagnostics := append(cloneDiagnostics(scan.diagnostics), migrationDiagnostics...)
	if session.LastSeq != scan.head.CommitSeq {
		scan.writable = false
		code := "metadata_lags_journal"
		message := fmt.Sprintf("metadata last sequence %d is behind validated journal head %d", session.LastSeq, scan.head.CommitSeq)
		if session.LastSeq > scan.head.CommitSeq {
			code = "metadata_leads_journal"
			message = fmt.Sprintf("metadata last sequence %d exceeds validated journal head %d", session.LastSeq, scan.head.CommitSeq)
		}
		diagnostics = append(diagnostics, protocol.Diagnostic{Code: code, Message: message, Journal: ref, AtSeq: scan.head.CommitSeq})
	}
	return journal.SessionInspection{
		Session: session,
		Journal: journal.Inspection{
			Journal: ref, Head: scan.head, Events: events, Writable: scan.writable,
			Diagnostics: diagnostics, IncompleteTransaction: scan.incompleteTransaction,
		},
	}, nil
}

func (s *Store) inspectJournalTransaction(ctx context.Context, transaction *sessionTransaction, ref protocol.JournalRef) (journalScan, error) {
	scan, err := s.scanJournal(ctx, transaction, ref)
	if err != nil {
		return scan, err
	}
	s.applyJournalLockHealth(ctx, transaction, ref, &scan)
	if err := s.syncCommittedView(ctx, transaction, ref, scan); err != nil {
		return scan, err
	}
	if recoveryEligible(scan) {
		details, err := observedRecoveryDetails(ctx, transaction.events, scan.validPrefixSize, scan.sourceSize)
		if err != nil {
			return scan, err
		}
		raw, err := json.Marshal(details)
		if err != nil {
			return scan, err
		}
		scan.diagnostics = append(scan.diagnostics, protocol.Diagnostic{
			Code: "recovery.available", Message: "journal has bytes beyond its validated committed prefix",
			Journal: ref, AtSeq: scan.physicalSeq + 1, Details: raw,
		})
	}
	return scan, nil
}

func (s *Store) applyJournalLockHealth(ctx context.Context, transaction *sessionTransaction, ref protocol.JournalRef, scan *journalScan) {
	lockSet, err := s.openValidatedLockSet(ctx, transaction, ref)
	if err == nil {
		err = lockSet.close()
		if err == nil {
			return
		}
	}
	scan.writable = false
	if _, stateErr := transaction.sessionRoot.Lstat(lockSetStateName); os.IsNotExist(stateErr) && !transaction.control {
		_, journalErr := transaction.sessionRoot.Lstat(journalLockName)
		_, turnErr := transaction.sessionRoot.Lstat(turnLockName)
		if os.IsNotExist(journalErr) && os.IsNotExist(turnErr) {
			scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "lock.initialization_required", "pre-lock journal requires explicit create-once initialization before mutation", scan.head.CommitSeq, ""))
			return
		}
	}
	scan.diagnostics = append(scan.diagnostics, journalDiagnostic(ref, "lock.invalid", err.Error(), scan.head.CommitSeq, ""))
}

func recoveryEligible(scan journalScan) bool {
	if scan.validPrefixSize < 0 || scan.validPrefixSize >= scan.sourceSize {
		return false
	}
	eligibleFailure := false
	for _, diagnostic := range scan.diagnostics {
		switch diagnostic.Code {
		case "incomplete_final_fragment", "incomplete_transaction":
			eligibleFailure = true
		case "unsupported_event", "unsupported_envelope_version", "invalid_known_payload", "invalid_sequence", "invalid_transition":
			return false
		}
	}
	return eligibleFailure
}

type recoveryObservation struct {
	ObservedTailDigest protocol.Digest `json:"observed_tail_digest"`
	ValidPrefixBytes   int64           `json:"valid_prefix_bytes"`
	SourceBytes        int64           `json:"source_bytes"`
}

func observedRecoveryDetails(ctx context.Context, file *os.File, prefixSize, sourceSize int64) (recoveryObservation, error) {
	if prefixSize < 0 || sourceSize <= prefixSize {
		return recoveryObservation{}, fmt.Errorf("invalid recovery byte range %d..%d", prefixSize, sourceSize)
	}
	hasher := sha256.New()
	reader := io.NewSectionReader(file, prefixSize, sourceSize-prefixSize)
	buffer := make([]byte, 64*1024)
	for {
		if err := ctx.Err(); err != nil {
			return recoveryObservation{}, err
		}
		count, readErr := reader.Read(buffer)
		if count > 0 {
			_, _ = hasher.Write(buffer[:count])
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return recoveryObservation{}, readErr
		}
		if count == 0 {
			return recoveryObservation{}, io.ErrNoProgress
		}
	}
	return recoveryObservation{
		ObservedTailDigest: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: hex.EncodeToString(hasher.Sum(nil))},
		ValidPrefixBytes:   prefixSize, SourceBytes: sourceSize,
	}, nil
}

func upcastScannedEvents(scan journalScan, sessionID protocol.SessionID) ([]protocol.EventRecord, []protocol.Diagnostic) {
	events := make([]protocol.EventRecord, 0, len(scan.events))
	state := UpcastState{SessionID: sessionID}
	var diagnostics []protocol.Diagnostic
	var lastLegacy protocol.LegacySource
	for _, scanned := range scan.events {
		if scanned.record.Legacy == nil {
			events = append(events, protocol.CloneEventRecord(scanned.record))
			continue
		}
		record, next, emitted := UpcastV1(*scanned.record.Legacy, state)
		state = next
		lastLegacy = *scanned.record.Legacy
		events = append(events, record)
		diagnostics = append(diagnostics, emitted...)
	}
	if len(state.OpenCalls) > 0 {
		ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(sessionID)}
		diagnostics = append(diagnostics, unmatchedCallDiagnostics(ref, lastLegacy, state)...)
	}
	return cloneRecords(events), cloneDiagnostics(diagnostics)
}

func (s *Store) Head(ctx context.Context, ref protocol.JournalRef) (protocol.CommittedCursor, error) {
	inspection, err := s.Inspect(ctx, ref)
	return inspection.Head, err
}

func (s *Store) Recover(ctx context.Context, request journal.RecoveryRequest) (journal.RecoveryResult, error) {
	return s.RecoverSession(ctx, request)
}
