package jsonl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
)

const (
	lineageFileName = "lineage.json"
	maxLineageDepth = 64
)

func validateLineage(lineage journal.SessionLineage) error {
	if err := validateSessionID(string(lineage.ParentSessionID)); err != nil {
		return err
	}
	if err := lineage.ParentCursor.Validate(); err != nil {
		return fmt.Errorf("invalid parent cursor: %w", err)
	}
	if lineage.ParentCursor.JournalKind != protocol.JournalSession || lineage.ParentCursor.JournalID != protocol.JournalID(lineage.ParentSessionID) {
		return fmt.Errorf("parent cursor identity mismatch")
	}
	if err := lineage.CheckpointDigest.Validate(); err != nil {
		return fmt.Errorf("invalid checkpoint digest: %w", err)
	}
	return nil
}

func (s *Store) validateLineageAnchor(ctx context.Context, workspace domain.Workspace, lineage journal.SessionLineage) ([]protocol.TurnID, error) {
	if err := validateLineage(lineage); err != nil {
		return nil, err
	}
	transaction, parent, err := s.openSessionTransaction(ctx, string(lineage.ParentSessionID), os.O_RDONLY, 0)
	if err != nil {
		return nil, errors.Join(journal.ErrLineageParentMissing, err)
	}
	if parent.Workspace.ID != workspace.ID || parent.Workspace.CanonicalPath != workspace.CanonicalPath {
		return nil, errors.Join(fmt.Errorf("lineage parent belongs to another workspace"), transaction.close())
	}
	scan, scanErr := s.scanJournal(ctx, transaction, protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(lineage.ParentSessionID)})
	closeErr := transaction.close()
	if scanErr != nil || closeErr != nil {
		return nil, errors.Join(scanErr, closeErr)
	}
	if !scan.writable || !scanHasCursor(scan, lineage.ParentCursor) {
		return nil, fmt.Errorf("lineage parent cursor is not an anchored committed prefix")
	}
	chain, err := s.readLineageChain(ctx, lineage.ParentSessionID)
	if err != nil {
		return nil, err
	}
	if len(chain) >= maxLineageDepth+1 {
		return nil, journal.ErrLineageDepthExceeded
	}
	return nonterminalTurnsAtCursor(scan, lineage.ParentSessionID, lineage.ParentCursor), nil
}

func nonterminalTurnsAtCursor(scan journalScan, sessionID protocol.SessionID, cursor protocol.CommittedCursor) []protocol.TurnID {
	records, _ := upcastScannedEvents(scan, sessionID)
	terminal := make(map[protocol.TurnID]bool)
	order := make([]protocol.TurnID, 0)
	for index, scanned := range scan.events {
		if scanned.cursor.CommitSeq > cursor.CommitSeq {
			break
		}
		record := records[index]
		turnID := record.Envelope.TurnID
		switch record.Envelope.Kind {
		case protocol.EventTurnAccepted:
			if _, exists := terminal[turnID]; !exists {
				order = append(order, turnID)
			}
			terminal[turnID] = false
		case protocol.EventTurnStateChanged:
			if payload, ok := record.Decoded.(*protocol.StateChangedV1); ok {
				switch protocol.TurnState(payload.To) {
				case protocol.TurnCompleted, protocol.TurnFailed, protocol.TurnInterrupted:
					terminal[turnID] = true
				}
			}
		case protocol.EventTurnCompleted, protocol.EventTurnFailed, protocol.EventTurnInterrupted:
			terminal[turnID] = true
		}
	}
	active := make([]protocol.TurnID, 0)
	for _, turnID := range order {
		if turnID != "" && !terminal[turnID] {
			active = append(active, turnID)
		}
	}
	return active
}

func scanHasCursor(scan journalScan, cursor protocol.CommittedCursor) bool {
	for _, commit := range scan.commits {
		if commit.cursor == cursor {
			return true
		}
	}
	return false
}

func (s *Store) SessionLineage(ctx context.Context, sessionID protocol.SessionID) (*journal.SessionLineage, error) {
	transaction, _, err := s.openSessionTransaction(ctx, string(sessionID), os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	lineage, readErr := readLineageFile(ctx, transaction)
	return lineage, errors.Join(readErr, transaction.close())
}

func readLineageFile(ctx context.Context, transaction *sessionTransaction) (*journal.SessionLineage, error) {
	file, info, err := openRootedRegularFile(ctx, transaction.sessionRoot, lineageFileName, os.O_RDONLY, 0)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	raw, readErr := readOpenedFile(ctx, file, maxSessionMetadataBytes)
	if readErr == nil {
		readErr = errors.Join(transaction.verifyEvents(), verifyRootedRegularFile(transaction.sessionRoot, lineageFileName, info))
	}
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return nil, errors.Join(readErr, closeErr)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var lineage journal.SessionLineage
	if err := decoder.Decode(&lineage); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("lineage has trailing JSON")
	}
	if err := validateLineage(lineage); err != nil {
		return nil, err
	}
	return &lineage, nil
}

type lineageNode struct {
	sessionID protocol.SessionID
	scan      journalScan
	lineage   *journal.SessionLineage
}

func (s *Store) readLineageChain(ctx context.Context, sessionID protocol.SessionID) ([]lineageNode, error) {
	seen := make(map[protocol.SessionID]struct{})
	chain := make([]lineageNode, 0, 4)
	current := sessionID
	var workspaceID string
	for {
		if _, duplicate := seen[current]; duplicate {
			return nil, journal.ErrLineageCycle
		}
		seen[current] = struct{}{}
		transaction, session, err := s.openSessionTransaction(ctx, string(current), os.O_RDONLY, 0)
		if err != nil {
			if len(chain) > 0 {
				return nil, errors.Join(journal.ErrLineageParentMissing, err)
			}
			return nil, err
		}
		if workspaceID == "" {
			workspaceID = session.Workspace.ID
		} else if session.Workspace.ID != workspaceID {
			return nil, errors.Join(fmt.Errorf("lineage crosses workspace identity"), transaction.close())
		}
		ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(current)}
		scan, scanErr := s.scanJournal(ctx, transaction, ref)
		lineage, lineageErr := readLineageFile(ctx, transaction)
		closeErr := transaction.close()
		if scanErr != nil || lineageErr != nil || closeErr != nil {
			return nil, errors.Join(scanErr, lineageErr, closeErr)
		}
		chain = append(chain, lineageNode{sessionID: current, scan: scan, lineage: lineage})
		if lineage == nil {
			return chain, nil
		}
		if len(chain) >= maxLineageDepth+1 {
			return nil, journal.ErrLineageDepthExceeded
		}
		current = lineage.ParentSessionID
	}
}

type composedCommit struct {
	cursor journal.LineageCursor
	events []protocol.EventRecord
}

func (s *Store) ReadComposedRange(ctx context.Context, request journal.ComposedReadRequest) (journal.ComposedEventPage, error) {
	if err := validateSessionID(string(request.SessionID)); err != nil {
		return journal.ComposedEventPage{}, err
	}
	if request.Limit < 1 || request.Limit > 1000 {
		return journal.ComposedEventPage{}, fmt.Errorf("composed range limit must be between 1 and 1000")
	}
	if request.After != (journal.LineageCursor{}) && request.After.ViewSessionID != request.SessionID {
		return journal.ComposedEventPage{}, fmt.Errorf("composed cursor view identity mismatch")
	}
	chain, err := s.readLineageChain(ctx, request.SessionID)
	if err != nil {
		return journal.ComposedEventPage{}, err
	}
	commits := make([]composedCommit, 0)
	for index := len(chain) - 1; index >= 0; index-- {
		node := chain[index]
		var cutoff protocol.CommittedCursor
		if index > 0 {
			cutoff = chain[index-1].lineage.ParentCursor
			if cutoff.JournalID != protocol.JournalID(node.sessionID) || !scanHasCursor(node.scan, cutoff) {
				return journal.ComposedEventPage{}, fmt.Errorf("lineage parent cursor is not an anchored committed prefix")
			}
		}
		upcast, _ := upcastScannedEvents(node.scan, node.sessionID)
		eventOffset := 0
		for _, commit := range node.scan.commits {
			count := len(commit.events)
			cursor := journal.LineageCursor{ViewSessionID: request.SessionID, OriginSessionID: node.sessionID, OriginCursor: commit.cursor}
			commits = append(commits, composedCommit{cursor: cursor, events: cloneRecords(upcast[eventOffset : eventOffset+count])})
			eventOffset += count
			if cutoff != (protocol.CommittedCursor{}) && commit.cursor == cutoff {
				break
			}
		}
	}
	page := journal.ComposedEventPage{}
	if len(commits) > 0 {
		page.Head = commits[len(commits)-1].cursor
	}
	start := 0
	if request.After != (journal.LineageCursor{}) {
		found := false
		for index, commit := range commits {
			if commit.cursor == request.After {
				start, found = index+1, true
				break
			}
		}
		if !found {
			return journal.ComposedEventPage{}, fmt.Errorf("composed cursor is not a committed lineage cursor")
		}
		page.Cursor = request.After
	}
	for index := start; index < len(commits); index++ {
		commit := commits[index]
		if len(page.Events) > 0 && len(page.Events)+len(commit.events) > request.Limit {
			page.More = true
			break
		}
		if len(page.Events) == 0 && len(commit.events) > request.Limit {
			return journal.ComposedEventPage{}, fmt.Errorf("composed transaction contains %d events, exceeding page limit %d", len(commit.events), request.Limit)
		}
		for _, record := range commit.events {
			page.Events = append(page.Events, journal.LineageEvent{Cursor: commit.cursor, Record: protocol.CloneEventRecord(record)})
		}
		page.Cursor = commit.cursor
		page.More = index+1 < len(commits)
		if len(page.Events) == request.Limit {
			break
		}
	}
	return page, nil
}
