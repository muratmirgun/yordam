package jsonl

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/safefile"
)

type missingEventLogError struct {
	sessionID string
}

func (e *missingEventLogError) Error() string {
	return fmt.Sprintf("storage corruption: session %q event log is missing", e.sessionID)
}

func (s *Store) Load(ctx context.Context, sessionID string) (domain.SessionReplay, error) {
	if err := s.state.lockContext(ctx); err != nil {
		return domain.SessionReplay{}, err
	}
	defer s.state.unlock()
	if err := validateSessionID(sessionID); err != nil {
		return domain.SessionReplay{}, err
	}
	inspection, err := s.InspectSession(ctx, protocol.SessionID(sessionID))
	if err != nil {
		return domain.SessionReplay{}, err
	}
	events, hasV2, err := legacyReplayEvents(inspection.Journal.Events)
	if err != nil {
		return domain.SessionReplay{}, err
	}
	note := legacyInspectionNote(inspection.Journal.Diagnostics)
	fileRecoveryNote, err := plannedFileRecoveryNote(ctx, events)
	if err != nil {
		return domain.SessionReplay{}, err
	}
	note = joinNote(note, fileRecoveryNote)
	if hasV2 {
		note = joinNote(note, "v0.2 journal is read-only through legacy Load")
	}
	session := inspection.Session
	if len(events) > 0 && session.LastSeq < events[len(events)-1].Seq {
		session.LastSeq = events[len(events)-1].Seq
		session.UpdatedAt = events[len(events)-1].Time
	}
	return domain.SessionReplay{
		Session: session, Events: events, RecoveryNote: note,
		ReadOnly: !inspection.Journal.Writable || hasV2,
	}, nil
}

func legacyReplayEvents(records []protocol.EventRecord) ([]domain.DurableEvent, bool, error) {
	events := make([]domain.DurableEvent, 0, len(records))
	hasV2 := false
	for _, record := range records {
		if record.Legacy == nil {
			hasV2 = true
			continue
		}
		var event domain.DurableEvent
		if err := json.Unmarshal(record.Legacy.RawEnvelope, &event); err != nil {
			return nil, false, fmt.Errorf("decode inspected legacy event %q: %w", record.Envelope.EventID, err)
		}
		events = append(events, event)
	}
	return events, hasV2, nil
}

func legacyInspectionNote(diagnostics []protocol.Diagnostic) string {
	note := ""
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == "" {
			continue
		}
		switch diagnostic.Code {
		case "migration.lossy", "migration.legacy_evidence", "migration.orphan_start", "migration.duplicate_start", "migration.duplicate_terminal", "recovery.available":
			continue
		}
		entry := diagnostic.Code
		switch diagnostic.Code {
		case "invalid_known_payload", "invalid_sequence", "invalid_transition":
			entry = fmt.Sprintf("corruption at sequence %d", diagnostic.AtSeq)
		case "metadata_leads_journal":
			entry = "storage corruption"
		}
		if diagnostic.Message != "" {
			entry += ": " + diagnostic.Message
		}
		note = joinNote(note, entry)
	}
	return note
}

func plannedFileRecoveryNote(ctx context.Context, events []domain.DurableEvent) (string, error) {
	pending := make(map[string]domain.FileChangePlan)
	for _, event := range events {
		switch event.Kind {
		case domain.EventFileChangePlanned:
			var plan domain.FileChangePlan
			if json.Unmarshal(event.Payload, &plan) == nil && plan.CallID != "" && plan.Path != "" {
				pending[plan.CallID] = plan
			}
		case domain.EventFileChanged:
			var change domain.FileChange
			if json.Unmarshal(event.Payload, &change) == nil && change.CallID != "" {
				delete(pending, change.CallID)
			}
		}
	}
	if len(pending) == 0 {
		return "", nil
	}

	callIDs := make([]string, 0, len(pending))
	for callID := range pending {
		callIDs = append(callIDs, callID)
	}
	sort.Strings(callIDs)
	note := ""
	for _, callID := range callIDs {
		matches, err := fileMatchesSHA256(ctx, pending[callID].Path, pending[callID].PlannedSHA256)
		if err != nil {
			return "", err
		}
		status := "planned change not confirmed"
		if matches {
			status = "planned after-state present"
		}
		note = joinNote(note, fmt.Sprintf("%s for call %s", status, callID))
	}
	return note, nil
}

func fileMatchesSHA256(ctx context.Context, path, want string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	file, err := safefile.OpenRegular(ctx, path)
	if err != nil {
		return false, nil
	}
	defer file.Close()

	hasher := sha256.New()
	buffer := make([]byte, 64*1024)
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		count, readErr := file.Read(buffer)
		if count > 0 {
			_, _ = hasher.Write(buffer[:count])
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return false, nil
		}
	}
	return hex.EncodeToString(hasher.Sum(nil)) == strings.ToLower(want), nil
}

func joinNote(left, right string) string {
	if left == "" {
		return right
	}
	return left + "; " + right
}
