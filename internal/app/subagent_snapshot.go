package app

import (
	"errors"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
)

type childInspection func(protocol.SessionID) (journal.Inspection, error)

func projectSubagentCards(parent journal.Inspection, inspectChild childInspection, now time.Time, redactor secret.Redacting) ([]protocol.SubagentCardV1, error) {
	type pending struct {
		request protocol.SubagentRequestedV1
		at      time.Time
		state   protocol.SubagentStage
	}
	order := make([]protocol.DelegationAttemptID, 0)
	attempts := make(map[protocol.DelegationAttemptID]pending)
	for _, record := range parent.Events {
		switch value := record.Decoded.(type) {
		case *protocol.SubagentRequestedV1:
			if value == nil || value.Validate() != nil || value.Manifest.ParentSessionID != protocol.SessionID(parent.Journal.ID) {
				continue
			}
			if _, exists := attempts[value.Manifest.AttemptID]; !exists {
				order = append(order, value.Manifest.AttemptID)
			}
			attempts[value.Manifest.AttemptID] = pending{request: protocol.DeepCopy(*value), at: record.Envelope.Time, state: protocol.SubagentStageRequested}
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
	cards := make([]protocol.SubagentCardV1, 0, len(order))
	for index, attemptID := range order {
		attempt := attempts[attemptID]
		manifest := attempt.request.Manifest
		card := protocol.SubagentCardV1{AttemptID: attemptID, ParentSessionID: manifest.ParentSessionID, ChildSessionID: manifest.ChildSessionID, Task: publicSubagentText(redactor, attempt.request.Call.Task, protocol.MaxSubagentTaskBytes), State: attempt.state, Attempt: index + 1, StartedAt: attempt.at, Deadline: manifest.Deadline, MaxToolCalls: manifest.MaxToolCalls, ChangedFiles: []string{}, CommandsAndTests: []string{}}
		end := now
		child, err := inspectChild(manifest.ChildSessionID)
		if err != nil {
			if errors.Is(err, journal.ErrSessionNotFound) {
				card.ElapsedNanos = max(0, end.Sub(card.StartedAt).Nanoseconds())
				if validateErr := card.Validate(); validateErr != nil {
					return nil, validateErr
				}
				cards = append(cards, card)
				continue
			}
			return nil, err
		}
		for _, record := range child.Events {
			switch value := record.Decoded.(type) {
			case *protocol.SubagentManifestV1:
				if value != nil && *value == manifest {
					card.State = protocol.SubagentStageRunning
				}
			case *protocol.ActivityPlannedV1:
				if value != nil && value.Kind == "tool" {
					card.ToolCalls++
				}
			case *protocol.SubagentReceiptV1:
				if value == nil || value.Manifest.AttemptID != attemptID || value.Validate() != nil {
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
