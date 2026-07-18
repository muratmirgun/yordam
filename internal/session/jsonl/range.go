package jsonl

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"golang.org/x/sys/unix"
)

const (
	journalIndexVersion  uint32 = 1
	maxJournalIndexBytes int64  = 4 << 20
	journalIndexName            = "journal.index.json"
)

type journalIndexTransaction struct {
	TransactionID protocol.TransactionID   `json:"transaction_id"`
	Cursor        protocol.CommittedCursor `json:"cursor"`
	FirstOffset   int64                    `json:"first_offset"`
	EndOffset     int64                    `json:"end_offset"`
}

type journalIndex struct {
	Version      uint32                    `json:"version"`
	SourceSize   int64                     `json:"source_size"`
	SourceDigest protocol.Digest           `json:"source_digest"`
	EventIDs     []protocol.EventID        `json:"event_ids"`
	Transactions []journalIndexTransaction `json:"transactions"`
}

type verifiedJournalScan struct {
	eventsInfo os.FileInfo
	scan       journalScan
}

func (s *Store) rememberVerifiedJournalScan(transaction *sessionTransaction, ref protocol.JournalRef, scan journalScan) {
	if s.journalHasMarkerUncertainty(ref) || !scan.writable || scan.incompleteTail || scan.sourceSize <= 0 || scan.sourceDigest.Validate() != nil {
		return
	}
	s.verifiedScan.Store(journalLockKey(ref), verifiedJournalScan{
		eventsInfo: transaction.eventsInfo,
		scan:       cloneJournalScan(scan),
	})
}

func (s *Store) loadVerifiedJournalScan(
	ctx context.Context,
	transaction *sessionTransaction,
	ref protocol.JournalRef,
) (journalScan, bool) {
	key := journalLockKey(ref)
	loaded, ok := s.verifiedScan.Load(key)
	if !ok {
		return journalScan{}, false
	}
	prefix, ok := loaded.(verifiedJournalScan)
	if !ok || prefix.eventsInfo == nil || transaction.eventsInfo == nil || !os.SameFile(prefix.eventsInfo, transaction.eventsInfo) || prefix.scan.incompleteTail {
		s.verifiedScan.Delete(key)
		return journalScan{}, false
	}
	info, err := transaction.events.Stat()
	if err != nil || info.Size() < prefix.scan.sourceSize {
		s.verifiedScan.Delete(key)
		return journalScan{}, false
	}
	hash := sha256.New()
	count, err := io.Copy(hash, io.NewSectionReader(transaction.events, 0, prefix.scan.sourceSize))
	if err != nil || count != prefix.scan.sourceSize {
		return journalScan{}, false
	}
	gotDigest := protocol.Digest{Algorithm: protocol.DigestSHA256, Value: fmt.Sprintf("%x", hash.Sum(nil))}
	if gotDigest != prefix.scan.sourceDigest || transaction.verifyEvents() != nil {
		s.verifiedScan.Delete(key)
		return journalScan{}, false
	}
	scan, err := s.scanJournalAfterPrefix(ctx, transaction, ref, prefix.scan, hash)
	if err != nil {
		s.verifiedScan.Delete(key)
		return journalScan{}, false
	}
	s.rememberVerifiedJournalScan(transaction, ref, scan)
	return scan, true
}

func (s *Store) loadJournalScan(
	ctx context.Context,
	transaction *sessionTransaction,
	ref protocol.JournalRef,
) (journalScan, bool, error) {
	if scan, ok := s.loadVerifiedJournalScan(ctx, transaction, ref); ok {
		return scan, true, nil
	}
	if scan, ok := s.loadVerifiedJournalIndex(ctx, transaction, ref); ok {
		s.rememberVerifiedJournalScan(transaction, ref, scan)
		return scan, false, nil
	}
	scan, err := s.scanJournal(ctx, transaction, ref)
	if err != nil {
		return journalScan{}, false, err
	}
	s.rememberVerifiedJournalScan(transaction, ref, scan)
	return scan, true, nil
}

func buildJournalIndex(scan journalScan) journalIndex {
	index := journalIndex{
		Version: journalIndexVersion, SourceSize: scan.sourceSize,
		SourceDigest: scan.sourceDigest,
		EventIDs:     append([]protocol.EventID(nil), scan.eventIDOrder...),
		Transactions: make([]journalIndexTransaction, 0, len(scan.commits)),
	}
	for _, commit := range scan.commits {
		index.Transactions = append(index.Transactions, journalIndexTransaction{
			TransactionID: commit.cursor.TransactionID, Cursor: commit.cursor,
			FirstOffset: commit.firstOffset, EndOffset: commit.endOffset,
		})
	}
	return index
}

func writeJournalIndex(ctx context.Context, transaction *sessionTransaction, scan journalScan) error {
	if !scan.writable || scan.incompleteTail || (scan.hasLegacy && !scan.hasV2) {
		return fmt.Errorf("cannot index an incomplete or read-only journal prefix")
	}
	index := buildJournalIndex(scan)
	return writeReplaceJSONAtomicRooted(ctx, transaction, journalIndexName, ".journal-index-", index, maxJournalIndexBytes)
}

func (s *Store) loadVerifiedJournalIndex(
	ctx context.Context,
	transaction *sessionTransaction,
	ref protocol.JournalRef,
) (journalScan, bool) {
	var seed journalScan
	indexFile, indexInfo, err := openRootedRegularFile(ctx, transaction.sessionRoot, journalIndexName, os.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return seed, false
	}
	raw, readErr := readOpenedFile(ctx, indexFile, maxJournalIndexBytes)
	if readErr == nil {
		readErr = errors.Join(transaction.verifyEvents(), verifyRootedRegularFile(transaction.sessionRoot, journalIndexName, indexInfo))
	}
	closeErr := indexFile.Close()
	if readErr != nil || closeErr != nil {
		return seed, false
	}
	var index journalIndex
	if !decodeJournalIndex(raw, &index) {
		return seed, false
	}
	if !validJournalIndexShape(index, ref) {
		return seed, false
	}
	eventsInfo, err := transaction.events.Stat()
	if err != nil || eventsInfo.Size() != index.SourceSize {
		return seed, false
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, io.NewSectionReader(transaction.events, 0, index.SourceSize)); err != nil {
		return seed, false
	}
	gotDigest := protocol.Digest{Algorithm: protocol.DigestSHA256, Value: fmt.Sprintf("%x", hash.Sum(nil))}
	if gotDigest != index.SourceDigest {
		return seed, false
	}
	var eventOffset int
	var expectedSeq uint64 = 1
	hasLegacy := false
	hasV2 := false
	verifiedCommits := make([]scannedCommit, 0, len(index.Transactions))
	for _, entry := range index.Transactions {
		commit, physicalIDs, legacy, ok := s.verifyIndexedCommit(ctx, transaction, ref, entry, expectedSeq)
		if !ok || eventOffset+len(physicalIDs) > len(index.EventIDs) || !reflect.DeepEqual(physicalIDs, index.EventIDs[eventOffset:eventOffset+len(physicalIDs)]) {
			return seed, false
		}
		if legacy {
			if hasV2 {
				return seed, false
			}
			hasLegacy = true
		} else {
			hasV2 = true
		}
		verifiedCommits = append(verifiedCommits, commit)
		eventOffset += len(physicalIDs)
		expectedSeq = entry.Cursor.CommitSeq + 1
	}
	if eventOffset != len(index.EventIDs) {
		return seed, false
	}
	if transaction.verifyEvents() != nil {
		return seed, false
	}
	last := index.Transactions[len(index.Transactions)-1]
	seed = journalScan{
		ref: ref, head: last.Cursor, physicalSeq: last.Cursor.CommitSeq,
		writable: true, hasLegacy: hasLegacy, hasV2: hasV2,
		transactions: make(map[protocol.TransactionID]scannedCommit, len(index.Transactions)),
		eventIDs:     make(map[protocol.EventID]struct{}, len(index.EventIDs)),
		eventIDOrder: append([]protocol.EventID(nil), index.EventIDs...),
		sourceSize:   index.SourceSize, sourceDigest: index.SourceDigest,
	}
	for _, commit := range verifiedCommits {
		seed.transactions[commit.cursor.TransactionID] = commit
		seed.commits = append(seed.commits, commit)
		for _, record := range commit.events {
			seed.events = append(seed.events, scannedEvent{record: protocol.CloneEventRecord(record), cursor: commit.cursor})
		}
	}
	for _, eventID := range index.EventIDs {
		seed.eventIDs[eventID] = struct{}{}
	}
	return seed, true
}

func decodeJournalIndex(raw []byte, destination *journalIndex) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(destination) == nil && decoder.Decode(&struct{}{}) == io.EOF
}

func validJournalIndexShape(index journalIndex, ref protocol.JournalRef) bool {
	if index.Version != journalIndexVersion || index.SourceSize <= 0 || index.SourceDigest.Validate() != nil || len(index.Transactions) == 0 {
		return false
	}
	last := index.Transactions[len(index.Transactions)-1]
	if last.EndOffset != index.SourceSize || uint64(len(index.EventIDs)) != last.Cursor.CommitSeq {
		return false
	}
	seenEvents := make(map[protocol.EventID]struct{}, len(index.EventIDs))
	for _, eventID := range index.EventIDs {
		if eventID == "" {
			return false
		}
		if _, duplicate := seenEvents[eventID]; duplicate {
			return false
		}
		seenEvents[eventID] = struct{}{}
	}
	var previousOffset int64
	var previousSeq uint64
	seenTransactions := make(map[protocol.TransactionID]struct{}, len(index.Transactions))
	for _, entry := range index.Transactions {
		if entry.TransactionID == "" || entry.Cursor.Validate() != nil || entry.Cursor.JournalKind != ref.Kind || entry.Cursor.JournalID != ref.ID || entry.Cursor.TransactionID != entry.TransactionID {
			return false
		}
		if entry.FirstOffset != previousOffset || entry.EndOffset <= entry.FirstOffset || entry.Cursor.CommitSeq <= previousSeq {
			return false
		}
		if _, duplicate := seenTransactions[entry.TransactionID]; duplicate {
			return false
		}
		seenTransactions[entry.TransactionID] = struct{}{}
		previousOffset, previousSeq = entry.EndOffset, entry.Cursor.CommitSeq
	}
	return true
}

func (s *Store) verifyIndexedCommit(
	ctx context.Context,
	transaction *sessionTransaction,
	ref protocol.JournalRef,
	entry journalIndexTransaction,
	expectedSeq uint64,
) (scannedCommit, []protocol.EventID, bool, bool) {
	failed := func() (scannedCommit, []protocol.EventID, bool, bool) { return scannedCommit{}, nil, false, false }
	length := entry.EndOffset - entry.FirstOffset
	if length <= 0 {
		return failed()
	}
	reader := bufio.NewReaderSize(io.NewSectionReader(transaction.events, entry.FirstOffset, length), 64*1024)
	var envelopes []protocol.EventEnvelope
	var records []protocol.EventRecord
	var eventIDs []protocol.EventID
	var consumed int64
	for {
		if ctx.Err() != nil {
			return failed()
		}
		physical, complete, _, err := readJournalLine(reader)
		if err != nil || !complete || len(physical) == 0 {
			return failed()
		}
		consumed += int64(len(physical))
		lastLine := consumed == length
		if consumed > length {
			return failed()
		}
		line := physical[:len(physical)-1]
		var identity struct {
			SchemaVersion uint32 `json:"schema_version"`
		}
		if json.Unmarshal(line, &identity) != nil {
			return failed()
		}
		if identity.SchemaVersion == 1 {
			if len(envelopes) != 0 || !lastLine {
				return failed()
			}
			var legacy domain.DurableEvent
			if ref.Kind != protocol.JournalSession || json.Unmarshal(line, &legacy) != nil || legacy.Validate() != nil || legacy.Seq != expectedSeq || protocol.JournalID(legacy.SessionID) != ref.ID {
				return failed()
			}
			valid := entry.Cursor == (protocol.CommittedCursor{
				JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: legacy.Seq,
				TransactionID: protocol.TransactionID("legacy:" + legacy.EventID),
			})
			raw := protocol.CloneRawMessage(line)
			record := protocol.EventRecord{
				RawEnvelope: raw,
				Legacy: &protocol.LegacySource{
					SchemaVersion: 1, EventID: protocol.EventID(legacy.EventID), SessionID: protocol.SessionID(legacy.SessionID),
					Seq: legacy.Seq, Time: legacy.Time, Kind: string(legacy.Kind), Payload: protocol.CloneRawMessage(legacy.Payload),
					RawEnvelope: protocol.CloneRawMessage(raw),
				},
			}
			commit := scannedCommit{cursor: entry.Cursor, events: []protocol.EventRecord{record}, firstOffset: entry.FirstOffset, endOffset: entry.EndOffset}
			return commit, []protocol.EventID{protocol.EventID(legacy.EventID)}, true, valid
		}
		record, err := s.registry.Decode(protocol.CloneRawMessage(line))
		if err != nil {
			return failed()
		}
		if s.registry.Validate(record) != nil {
			return failed()
		}
		envelope := record.Envelope
		eventIDs = append(eventIDs, envelope.EventID)
		if envelope.JournalKind != ref.Kind || envelope.JournalID != ref.ID || envelope.TransactionID != entry.TransactionID || envelope.Seq != expectedSeq+uint64(len(eventIDs))-1 {
			return failed()
		}
		if envelope.Kind != protocol.EventTransactionCommitted {
			if len(envelopes) >= 1000 {
				return failed()
			}
			envelopes = append(envelopes, envelope)
			records = append(records, protocol.CloneEventRecord(record))
			if lastLine {
				return failed()
			}
			continue
		}
		if !lastLine || len(envelopes) == 0 || envelope.Seq != entry.Cursor.CommitSeq {
			return failed()
		}
		marker, ok := record.Decoded.(*protocol.TransactionCommittedV1)
		if !ok || marker.FirstSeq != envelopes[0].Seq || marker.LastSeq != envelopes[len(envelopes)-1].Seq || uint64(marker.EventCount) != uint64(len(envelopes)) {
			return failed()
		}
		digest, err := canonicaljson.TransactionDigest(envelopes)
		if err != nil || marker.Digest != digest {
			return failed()
		}
		commit := scannedCommit{
			cursor: entry.Cursor, events: records, envelopes: cloneEnvelopes(envelopes),
			firstOffset: entry.FirstOffset, endOffset: entry.EndOffset,
		}
		return commit, eventIDs, false, true
	}
}

func ensureJournalIndex(ctx context.Context, transaction *sessionTransaction, scan journalScan) error {
	want := buildJournalIndex(scan)
	file, info, err := openRootedRegularFile(ctx, transaction.sessionRoot, journalIndexName, os.O_RDONLY|unix.O_NONBLOCK, 0)
	if err == nil {
		raw, readErr := readOpenedFile(ctx, file, maxJournalIndexBytes)
		if readErr == nil {
			readErr = errors.Join(transaction.verifyEvents(), verifyRootedRegularFile(transaction.sessionRoot, journalIndexName, info))
		}
		closeErr := file.Close()
		var got journalIndex
		decoded := false
		if readErr == nil {
			decoded = decodeJournalIndex(raw, &got)
		}
		if readErr == nil && closeErr == nil && decoded && reflect.DeepEqual(got, want) {
			return nil
		}
		if readErr != nil && closeErr != nil {
			return errors.Join(readErr, closeErr)
		}
	} else if !os.IsNotExist(err) {
		// A symlink, FIFO, or substituted leaf is never replaced through the
		// read path. The authoritative scan remains usable without the cache.
		return err
	}
	return writeJournalIndex(ctx, transaction, scan)
}

func (s *Store) ReadRange(ctx context.Context, request journal.ReadRangeRequest) (journal.EventPage, error) {
	if err := request.Journal.Validate(); err != nil {
		return journal.EventPage{}, err
	}
	if request.Limit < 1 || request.Limit > 1000 {
		return journal.EventPage{}, fmt.Errorf("range limit must be between 1 and 1000")
	}
	if err := validateRangeCursor(request.Journal, request.After); err != nil {
		return journal.EventPage{}, err
	}
	lock := s.journalLock(request.Journal)
	if err := lock.lock(ctx); err != nil {
		return journal.EventPage{}, err
	}
	defer lock.unlock()
	transaction, _, err := s.openJournal(ctx, request.Journal, os.O_RDONLY)
	if err != nil {
		return journal.EventPage{}, err
	}
	if s.journalHasMarkerUncertainty(request.Journal) {
		return journal.EventPage{}, errors.Join(fmt.Errorf("journal has unresolved marker durability uncertainty"), transaction.close())
	}
	scan, rebuildIndex, scanErr := s.loadJournalScan(ctx, transaction, request.Journal)
	if scanErr == nil && rebuildIndex {
		_ = ensureJournalIndex(ctx, transaction, scan)
	}
	closeErr := transaction.close()
	if scanErr != nil || closeErr != nil {
		return journal.EventPage{}, errors.Join(scanErr, closeErr)
	}
	start := 0
	if !zeroCursor(request.After) {
		found := false
		for index, commit := range scan.commits {
			if commit.cursor == request.After {
				start, found = index+1, true
				break
			}
		}
		if !found {
			return journal.EventPage{}, fmt.Errorf("range cursor is not a committed journal cursor")
		}
	}
	page := journal.EventPage{Cursor: request.After, Head: scan.head}
	for index := start; index < len(scan.commits); index++ {
		commit := scan.commits[index]
		if len(page.Events) > 0 && len(page.Events)+len(commit.events) > request.Limit {
			page.More = true
			break
		}
		if len(page.Events) == 0 && len(commit.events) > request.Limit {
			return journal.EventPage{}, fmt.Errorf("committed transaction contains %d events, exceeding page limit %d", len(commit.events), request.Limit)
		}
		page.Events = append(page.Events, cloneRecords(commit.events)...)
		page.Cursor = commit.cursor
		page.More = index+1 < len(scan.commits)
		if len(page.Events) == request.Limit {
			break
		}
	}
	return cloneEventPage(page), nil
}

func validateRangeCursor(ref protocol.JournalRef, cursor protocol.CommittedCursor) error {
	if zeroCursor(cursor) {
		return nil
	}
	if err := cursor.Validate(); err != nil {
		return err
	}
	if cursor.JournalKind != ref.Kind || cursor.JournalID != ref.ID {
		return fmt.Errorf("range cursor journal identity mismatch")
	}
	return nil
}

func zeroCursor(cursor protocol.CommittedCursor) bool {
	return cursor == (protocol.CommittedCursor{})
}

func cloneEventPage(page journal.EventPage) journal.EventPage {
	page.Events = cloneRecords(page.Events)
	return page
}

func (s *Store) LookupTransaction(
	ctx context.Context,
	ref protocol.JournalRef,
	transactionID protocol.TransactionID,
) (journal.TransactionLookup, error) {
	if err := ref.Validate(); err != nil {
		return journal.TransactionLookup{}, err
	}
	if transactionID == "" {
		return journal.TransactionLookup{}, fmt.Errorf("transaction ID is required")
	}
	lock := s.journalLock(ref)
	if err := lock.lock(ctx); err != nil {
		return journal.TransactionLookup{}, err
	}
	defer lock.unlock()
	transaction, _, err := s.openJournal(ctx, ref, os.O_RDONLY)
	if err != nil {
		return journal.TransactionLookup{}, err
	}
	scan, rebuildIndex, scanErr := s.loadJournalScan(ctx, transaction, ref)
	if scanErr != nil {
		return journal.TransactionLookup{}, errors.Join(scanErr, transaction.close())
	}
	if commit, ok := scan.transactions[transactionID]; ok {
		if uncertain, exists := s.state.markerUncertainty.Load(markerUncertaintyKey(ref, transactionID)); exists {
			eventsInfo, valid := uncertain.(os.FileInfo)
			if !valid || eventsInfo == nil || transaction.eventsInfo == nil || !os.SameFile(eventsInfo, transaction.eventsInfo) {
				return journal.TransactionLookup{State: journal.TransactionUnknown}, transaction.close()
			}
			if transaction.events.Sync() != nil || transaction.verifyEvents() != nil {
				return journal.TransactionLookup{State: journal.TransactionUnknown}, transaction.close()
			}
			s.clearMarkerUncertainty(ref, transactionID)
		}
		s.rememberVerifiedJournalScan(transaction, ref, scan)
		if rebuildIndex && !s.journalHasMarkerUncertainty(ref) {
			_ = ensureJournalIndex(ctx, transaction, scan)
		}
		return journal.TransactionLookup{State: journal.TransactionCommitted, Cursor: commit.cursor}, transaction.close()
	}
	if scan.incompleteTail {
		return journal.TransactionLookup{State: journal.TransactionUnknown}, transaction.close()
	}
	if _, uncertain := s.state.markerUncertainty.Load(markerUncertaintyKey(ref, transactionID)); uncertain {
		return journal.TransactionLookup{State: journal.TransactionUnknown}, transaction.close()
	}
	s.rememberVerifiedJournalScan(transaction, ref, scan)
	if rebuildIndex && !s.journalHasMarkerUncertainty(ref) {
		_ = ensureJournalIndex(ctx, transaction, scan)
	}
	return journal.TransactionLookup{State: journal.TransactionNotCommitted}, transaction.close()
}

func (s *Store) ReadCommittedTransaction(
	ctx context.Context,
	ref protocol.JournalRef,
	transactionID protocol.TransactionID,
) (journal.CommittedTransaction, error) {
	if err := ref.Validate(); err != nil {
		return journal.CommittedTransaction{}, err
	}
	if transactionID == "" {
		return journal.CommittedTransaction{}, fmt.Errorf("transaction ID is required")
	}
	lock := s.journalLock(ref)
	if err := lock.lock(ctx); err != nil {
		return journal.CommittedTransaction{}, err
	}
	defer lock.unlock()
	if _, uncertain := s.state.markerUncertainty.Load(markerUncertaintyKey(ref, transactionID)); uncertain {
		return journal.CommittedTransaction{}, fmt.Errorf("transaction %q has unresolved marker durability uncertainty", transactionID)
	}
	// This opens and scans independently on every call. No AppendResult or
	// in-memory envelope slice participates in the verification decision.
	transaction, _, err := s.openJournal(ctx, ref, os.O_RDONLY)
	if err != nil {
		return journal.CommittedTransaction{}, err
	}
	scan, scanErr := s.scanJournal(ctx, transaction, ref)
	closeErr := transaction.close()
	if scanErr != nil || closeErr != nil {
		return journal.CommittedTransaction{}, errors.Join(scanErr, closeErr)
	}
	commit, ok := scan.transactions[transactionID]
	if !ok {
		if scan.incompleteTail {
			return journal.CommittedTransaction{}, fmt.Errorf("transaction %q is not independently provable because the journal tail is incomplete", transactionID)
		}
		return journal.CommittedTransaction{}, fmt.Errorf("transaction %q is not committed", transactionID)
	}
	if len(commit.envelopes) == 0 {
		return journal.CommittedTransaction{}, fmt.Errorf("legacy transaction %q has no v2 envelopes", transactionID)
	}
	return journal.CommittedTransaction{
		Journal: ref, TransactionID: transactionID, Cursor: commit.cursor,
		Events: cloneEnvelopes(commit.envelopes),
	}, nil
}
