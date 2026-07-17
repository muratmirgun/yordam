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
	"io"
	"os"

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
}

func (s *Store) openJournal(
	ctx context.Context,
	ref protocol.JournalRef,
	flags int,
) (*sessionTransaction, domain.Session, error) {
	if err := ref.Validate(); err != nil {
		return nil, domain.Session{}, err
	}
	if ref.Kind != protocol.JournalSession {
		return nil, domain.Session{}, fmt.Errorf("workspace-control journal creation is a Task 4 boundary")
	}
	return s.openSessionTransaction(ctx, string(ref.ID), flags, 0)
}

func (s *Store) scanJournal(ctx context.Context, transaction *sessionTransaction, ref protocol.JournalRef) (journalScan, error) {
	scan := journalScan{
		ref: ref, writable: true,
		transactions: make(map[protocol.TransactionID]scannedCommit),
		eventIDs:     make(map[protocol.EventID]struct{}),
	}
	if err := transaction.verifyEvents(); err != nil {
		return scan, err
	}
	if _, err := transaction.events.Seek(0, io.SeekStart); err != nil {
		return scan, err
	}
	reader := bufio.NewReaderSize(transaction.events, 64*1024)
	hash := sha256.New()
	seenEvents := make(map[protocol.EventID]struct{})
	var pendingRecords []protocol.EventRecord
	var pendingEnvelopes []protocol.EventEnvelope
	var pendingID protocol.TransactionID
	var pendingOffset int64
	hasV2 := false
	var offset int64
	for {
		if err := ctx.Err(); err != nil {
			return scan, err
		}
		physical, complete, eof, err := readJournalLine(reader)
		if err != nil {
			return scan, err
		}
		if len(physical) == 0 && eof {
			break
		}
		_, _ = hash.Write(physical)
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
			return scan, fmt.Errorf("empty journal line at sequence %d", scan.physicalSeq+1)
		}
		var identity struct {
			SchemaVersion uint32 `json:"schema_version"`
		}
		if err := json.Unmarshal(line, &identity); err != nil {
			return scan, fmt.Errorf("decode journal line %d: %w", scan.physicalSeq+1, err)
		}
		switch identity.SchemaVersion {
		case 1:
			scan.hasLegacy = true
			if hasV2 || pendingID != "" {
				return scan, fmt.Errorf("v1 event follows v2 journal data")
			}
			if err := protocol.ValidateRawJSON(line); err != nil {
				return scan, fmt.Errorf("invalid v1 event JSON: %w", err)
			}
			var legacy domain.DurableEvent
			if err := json.Unmarshal(line, &legacy); err != nil {
				return scan, fmt.Errorf("decode v1 event: %w", err)
			}
			if err := legacy.Validate(); err != nil {
				return scan, err
			}
			if ref.Kind != protocol.JournalSession || protocol.JournalID(legacy.SessionID) != ref.ID {
				return scan, fmt.Errorf("v1 journal identity mismatch")
			}
			if legacy.Seq != scan.physicalSeq+1 {
				return scan, fmt.Errorf("v1 sequence=%d want %d", legacy.Seq, scan.physicalSeq+1)
			}
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
			raw := protocol.CloneRawMessage(line)
			record := protocol.EventRecord{
				RawEnvelope: raw,
				Legacy: &protocol.LegacySource{
					SchemaVersion: 1, EventID: eventID, SessionID: protocol.SessionID(legacy.SessionID),
					Seq: legacy.Seq, Time: legacy.Time, Kind: string(legacy.Kind),
					Payload: protocol.CloneRawMessage(legacy.Payload), RawEnvelope: protocol.CloneRawMessage(raw),
				},
			}
			cursor := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: legacy.Seq, TransactionID: transactionID}
			commit := scannedCommit{cursor: cursor, events: []protocol.EventRecord{record}, firstOffset: lineOffset, endOffset: offset}
			scan.events = append(scan.events, scannedEvent{record: record, cursor: cursor})
			scan.commits = append(scan.commits, commit)
			scan.transactions[transactionID] = commit
			scan.head = cursor
			scan.physicalSeq = legacy.Seq
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
				return scan, fmt.Errorf("decode v2 event: %w", decodeErr)
			} else if err := s.registry.Validate(record); err != nil {
				return scan, fmt.Errorf("validate v2 event: %w", err)
			}
			envelope := record.Envelope
			if envelope.JournalKind != ref.Kind || envelope.JournalID != ref.ID {
				return scan, fmt.Errorf("v2 journal identity mismatch")
			}
			if envelope.Seq != scan.physicalSeq+1 {
				return scan, fmt.Errorf("v2 sequence=%d want %d", envelope.Seq, scan.physicalSeq+1)
			}
			if _, duplicate := seenEvents[envelope.EventID]; duplicate {
				return scan, fmt.Errorf("duplicate event ID %q", envelope.EventID)
			}
			seenEvents[envelope.EventID] = struct{}{}
			scan.eventIDs[envelope.EventID] = struct{}{}
			scan.eventIDOrder = append(scan.eventIDOrder, envelope.EventID)
			scan.physicalSeq = envelope.Seq
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
				if _, duplicate := scan.transactions[pendingID]; duplicate {
					return scan, fmt.Errorf("duplicate transaction ID %q", pendingID)
				}
				cursor := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: envelope.Seq, TransactionID: pendingID}
				commit := scannedCommit{
					cursor: cursor, events: cloneRecords(pendingRecords), envelopes: cloneEnvelopes(pendingEnvelopes),
					firstOffset: pendingOffset, endOffset: offset,
				}
				for _, pendingRecord := range pendingRecords {
					scan.events = append(scan.events, scannedEvent{record: protocol.CloneEventRecord(pendingRecord), cursor: cursor})
				}
				scan.commits = append(scan.commits, commit)
				scan.transactions[pendingID] = commit
				scan.head = cursor
				pendingRecords, pendingEnvelopes = nil, nil
				pendingID = ""
				continue
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
	scan.sourceDigest = protocol.Digest{Algorithm: protocol.DigestSHA256, Value: hex.EncodeToString(hash.Sum(nil))}
	if err := transaction.verifyEvents(); err != nil {
		return scan, err
	}
	return scan, nil
}

func readJournalLine(reader *bufio.Reader) (line []byte, complete, eof bool, err error) {
	var pending bytes.Buffer
	for {
		fragment, readErr := reader.ReadSlice('\n')
		if pending.Len()+len(fragment) > protocol.MaxEventBytes+1 {
			return nil, false, false, fmt.Errorf("journal line exceeds %d bytes", protocol.MaxEventBytes)
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
	transaction, _, err := s.openJournal(ctx, ref, os.O_RDONLY)
	if err != nil {
		return journal.Inspection{}, err
	}
	scan, scanErr := s.scanJournal(ctx, transaction, ref)
	closeErr := transaction.close()
	if scanErr != nil || closeErr != nil {
		return journal.Inspection{}, errors.Join(scanErr, closeErr)
	}
	events := make([]protocol.EventRecord, len(scan.events))
	for index := range scan.events {
		events[index] = protocol.CloneEventRecord(scan.events[index].record)
	}
	return journal.Inspection{
		Journal: ref, Head: scan.head, Events: events, Writable: scan.writable,
		Diagnostics: cloneDiagnostics(scan.diagnostics), IncompleteTransaction: scan.incompleteTransaction,
	}, nil
}

func (s *Store) Head(ctx context.Context, ref protocol.JournalRef) (protocol.CommittedCursor, error) {
	inspection, err := s.Inspect(ctx, ref)
	return inspection.Head, err
}

func (s *Store) Recover(context.Context, journal.RecoveryRequest) (journal.RecoveryResult, error) {
	return journal.RecoveryResult{}, fmt.Errorf("explicit journal recovery is a Task 3 boundary")
}
