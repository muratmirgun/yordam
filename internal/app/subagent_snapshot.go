package app

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
)

type childInspection func(protocol.SessionID) (journal.Inspection, error)

type subagentCardAttempt struct {
	request protocol.SubagentRequestedV1
	at      time.Time
	state   protocol.SubagentStage
	ordinal int
}

type journalRangeReader interface {
	ReadRange(context.Context, journal.ReadRangeRequest) (journal.EventPage, error)
}

// readInspectionAt reconstructs only the exact committed prefix represented by
// target. It intentionally never calls Inspect, whose latest-head view can race
// with a previously captured multi-journal snapshot vector.
func readInspectionAt(ctx context.Context, reader journalRangeReader, ref protocol.JournalRef, target protocol.CommittedCursor) (journal.Inspection, error) {
	if reader == nil || ref.Validate() != nil || target.Validate() != nil || target.JournalKind != ref.Kind || target.JournalID != ref.ID {
		return journal.Inspection{}, fmt.Errorf("exact inspection target is invalid")
	}
	result := journal.Inspection{Journal: ref, Head: target, Events: []protocol.EventRecord{}, Writable: false}
	after := protocol.CommittedCursor{}
	for {
		page, err := reader.ReadRange(ctx, journal.ReadRangeRequest{Journal: ref, After: after, Limit: 1000})
		if err != nil {
			return journal.Inspection{}, err
		}
		if len(page.Events) == 0 {
			return journal.Inspection{}, fmt.Errorf("exact inspection target was not found")
		}
		for _, record := range page.Events {
			if record.Legacy != nil {
				transactionID := protocol.TransactionID("legacy:" + record.Legacy.EventID)
				if record.Legacy.Seq > target.CommitSeq {
					return journal.Inspection{}, fmt.Errorf("exact inspection target is not a transaction boundary")
				}
				result.Events = append(result.Events, protocol.CloneEventRecord(record))
				if record.Legacy.Seq == target.CommitSeq {
					if transactionID != target.TransactionID {
						return journal.Inspection{}, fmt.Errorf("exact inspection target transaction mismatch")
					}
					return result, nil
				}
				continue
			}
			if record.Envelope.JournalKind != ref.Kind || record.Envelope.JournalID != ref.ID || record.Envelope.Seq == 0 {
				return journal.Inspection{}, fmt.Errorf("exact inspection record identity mismatch")
			}
			if record.Envelope.Seq >= target.CommitSeq {
				return journal.Inspection{}, fmt.Errorf("exact inspection target is not a transaction boundary")
			}
			result.Events = append(result.Events, protocol.CloneEventRecord(record))
			if record.Envelope.Seq+1 == target.CommitSeq {
				if record.Envelope.TransactionID != target.TransactionID {
					return journal.Inspection{}, fmt.Errorf("exact inspection target transaction mismatch")
				}
				return result, nil
			}
		}
		if !page.More || page.Cursor == after || page.Cursor.CommitSeq >= target.CommitSeq {
			return journal.Inspection{}, fmt.Errorf("exact inspection target was not found")
		}
		after = page.Cursor
	}
}

func projectSubagentCards(parent journal.Inspection, inspectChild childInspection, now time.Time, redactor secret.Redacting) ([]protocol.SubagentCardV1, error) {
	selected, err := recentSubagentAttempts(parent)
	if err != nil {
		return nil, err
	}
	cards := make([]protocol.SubagentCardV1, 0, len(selected))
	for _, attempt := range selected {
		attemptID := attempt.request.Manifest.AttemptID
		manifest := attempt.request.Manifest
		card := protocol.SubagentCardV1{AttemptID: attemptID, ParentSessionID: manifest.ParentSessionID, ChildSessionID: manifest.ChildSessionID, Task: publicSubagentText(redactor, attempt.request.Call.Task, protocol.MaxSubagentTaskBytes), State: attempt.state, Attempt: attempt.ordinal, StartedAt: attempt.at, Deadline: manifest.Deadline, MaxToolCalls: manifest.MaxToolCalls, ChangedFiles: []string{}, CommandsAndTests: []string{}}
		end := now
		child, childErr := inspectChild(manifest.ChildSessionID)
		if childErr != nil {
			if errors.Is(childErr, journal.ErrSessionNotFound) {
				card.ElapsedNanos = max(0, end.Sub(card.StartedAt).Nanoseconds())
				if validateErr := card.Validate(); validateErr != nil {
					return nil, validateErr
				}
				cards = append(cards, card)
				continue
			}
			return nil, childErr
		}
		for _, record := range child.Events {
			switch value := record.Decoded.(type) {
			case *protocol.SubagentManifestV1:
				if value != nil && *value == manifest && childRecordMatchesManifest(record, manifest) {
					card.State = protocol.SubagentStageRunning
				}
			case *protocol.ActivityPlannedV1:
				if value != nil && value.Kind == "tool" && childRecordMatchesManifest(record, manifest) {
					card.ToolCalls++
				}
			case *protocol.SubagentReceiptV1:
				if value == nil || value.Manifest != manifest || value.Validate() != nil || !childRecordMatchesManifest(record, manifest) {
					continue
				}
				card.State = protocol.SubagentStage(value.Status)
				card.ReceiptSummary = publicSubagentText(redactor, value.Summary, protocol.MaxSubagentPublicSummaryBytes)
				card.ChangedFiles = publicSubagentStrings(redactor, value.ChangedFiles)
				card.CommandsAndTests = publicSubagentStrings(redactor, value.CommandsAndTests)
				end = record.Envelope.Time
				if value.Status == string(protocol.SubagentStageUncertain) || len(value.UnknownEffects) != 0 {
					card.Warning = "effect may have occurred; automatic retry is disabled"
				}
			}
		}
		card.ToolCalls = min(card.ToolCalls, card.MaxToolCalls)
		card.ElapsedNanos = max(0, end.Sub(card.StartedAt).Nanoseconds())
		if err := card.Validate(); err != nil {
			return nil, err
		}
		cards = append(cards, card)
	}
	return cards, nil
}

func recentSubagentAttempts(parent journal.Inspection) ([]subagentCardAttempt, error) {
	order := make([]protocol.DelegationAttemptID, 0)
	attempts := make(map[protocol.DelegationAttemptID]subagentCardAttempt)
	attemptsByTurn := make(map[protocol.TurnID]int)
	for _, record := range parent.Events {
		switch value := record.Decoded.(type) {
		case *protocol.SubagentRequestedV1:
			if value == nil || value.Validate() != nil || value.Manifest.ParentSessionID != protocol.SessionID(parent.Journal.ID) ||
				record.Envelope.JournalKind != protocol.JournalSession || record.Envelope.JournalID != parent.Journal.ID || record.Envelope.SessionID != value.Manifest.ParentSessionID ||
				record.Envelope.TaskID == "" || record.Envelope.TurnID == "" || record.Envelope.RuntimeGenerationID != value.Manifest.RuntimeGenerationID {
				continue
			}
			if existing, exists := attempts[value.Manifest.AttemptID]; exists {
				if existing.request != *value {
					return nil, fmt.Errorf("conflicting duplicate subagent attempt %q", value.Manifest.AttemptID)
				}
				continue
			}
			attemptsByTurn[record.Envelope.TurnID]++
			if attemptsByTurn[record.Envelope.TurnID] > protocol.MaxSubagentAttemptsPerTurn {
				return nil, fmt.Errorf("subagent attempts exceed per-turn limit")
			}
			order = append(order, value.Manifest.AttemptID)
			attempts[value.Manifest.AttemptID] = subagentCardAttempt{request: protocol.DeepCopy(*value), at: record.Envelope.Time, state: protocol.SubagentStageRequested, ordinal: attemptsByTurn[record.Envelope.TurnID]}
		case *protocol.SubagentWaitingV1:
			if value == nil {
				continue
			}
			attempt, ok := attempts[value.AttemptID]
			if ok && attempt.request.Manifest.ChildSessionID == value.ChildSessionID {
				attempt.state = protocol.SubagentStageWaiting
				attempts[value.AttemptID] = attempt
			}
		}
	}
	if len(order) > protocol.MaxCollectionMembers {
		order = order[len(order)-protocol.MaxCollectionMembers:]
	}
	selected := make([]subagentCardAttempt, 0, len(order))
	for _, attemptID := range order {
		selected = append(selected, protocol.DeepCopy(attempts[attemptID]))
	}
	return selected, nil
}

func childRecordMatchesManifest(record protocol.EventRecord, manifest protocol.SubagentManifestV1) bool {
	return record.Envelope.JournalKind == protocol.JournalSession &&
		record.Envelope.JournalID == protocol.JournalID(manifest.ChildSessionID) &&
		record.Envelope.SessionID == manifest.ChildSessionID &&
		record.Envelope.TaskID == manifest.ChildTaskID &&
		record.Envelope.TurnID == manifest.ChildTurnID &&
		record.Envelope.RuntimeGenerationID == manifest.RuntimeGenerationID
}

func publicSubagentText(redactor secret.Redacting, value string, limit int) string {
	if redactor != nil {
		value = redactor.String(value)
	}
	if len(value) <= limit {
		return value
	}
	value = value[:limit]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return strings.TrimSpace(value)
}

func publicSubagentStrings(redactor secret.Redacting, values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = publicSubagentText(redactor, value, protocol.MaxSubagentPublicSummaryBytes)
		if value != "" {
			result = append(result, value)
		}
	}
	sort.Strings(result)
	compacted := result[:0]
	for _, value := range result {
		if len(compacted) == 0 || compacted[len(compacted)-1] != value {
			compacted = append(compacted, value)
		}
	}
	return compacted
}
