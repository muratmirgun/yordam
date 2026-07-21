package compaction

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
)

const (
	DefaultManualInputBytes = 256 << 10
	recentSuffixEvents      = 4
	maxNormalizedEventBytes = 16 << 10
)

var ErrNothingToCompact = errors.New("nothing safe to compact")

type Selection struct {
	From               protocol.CommittedCursor
	Through            protocol.CommittedCursor
	Trigger            Trigger
	RetainedEventIDs   []protocol.EventID
	SummarizedEventIDs []protocol.EventID
	Sources            []protocol.ContentSource
	SourceDigest       protocol.Digest
	InputLimitBytes    int
}

// Select chooses an older, contiguous committed range. It intentionally has no
// repository dependency: journal order and envelope identities are its only inputs.
func Select(events []protocol.EventRecord, head protocol.CommittedCursor, trigger Trigger, manualInputBytes int) (Selection, error) {
	if head.Validate() != nil || head.JournalKind != protocol.JournalSession {
		return Selection{}, fmt.Errorf("invalid compaction head")
	}
	if trigger != TriggerManual && trigger != TriggerAutomatic {
		return Selection{}, fmt.Errorf("invalid compaction trigger %q", trigger)
	}
	if manualInputBytes <= 0 {
		manualInputBytes = DefaultManualInputBytes
	}
	committed, err := committedEvents(events, head)
	if err != nil {
		return Selection{}, err
	}
	if len(committed) <= recentSuffixEvents {
		return Selection{}, ErrNothingToCompact
	}
	start := 0
	if priorThrough, ok := latestCompactionThrough(committed, head); ok {
		start = priorThrough + 1
	}
	cutoff := len(committed) - recentSuffixEvents - 1
	cutoff = exchangeSafeCutoff(committed, cutoff)
	if start > cutoff {
		return Selection{}, ErrNothingToCompact
	}
	sources := make([]protocol.ContentSource, 0, len(committed))
	for _, record := range committed {
		source, err := normalizedSource(record)
		if err != nil {
			return Selection{}, err
		}
		sources = append(sources, source)
	}
	selection, err := boundedSelection(committed, sources, start, cutoff, trigger, manualInputBytes)
	if err != nil {
		return Selection{}, err
	}
	selection.RetainedEventIDs = retainedIDs(committed, cutoff)
	return cloneSelection(selection), nil
}

type toolExchangeRange struct {
	assistant int
	result    int
}

func exchangeSafeCutoff(events []protocol.EventRecord, cutoff int) int {
	ranges := toolExchangeRanges(events)
	for {
		adjusted := cutoff
		for _, exchange := range ranges {
			if exchange.assistant <= adjusted && adjusted < exchange.result {
				adjusted = exchange.assistant - 1
			}
		}
		if adjusted == cutoff {
			return cutoff
		}
		cutoff = adjusted
	}
}

func toolExchangeRanges(events []protocol.EventRecord) []toolExchangeRange {
	type pendingExchange struct {
		assistant int
		remaining map[string]struct{}
	}
	var pending *pendingExchange
	ranges := make([]toolExchangeRange, 0)
	for index, record := range events {
		if message, ok := assistantMessage(record); ok {
			callIDs := assistantToolCallIDs(message)
			if len(callIDs) > 0 {
				remaining := make(map[string]struct{}, len(callIDs))
				for _, callID := range callIDs {
					remaining[callID] = struct{}{}
				}
				pending = &pendingExchange{assistant: index, remaining: remaining}
			}
			continue
		}
		if record.Envelope.Kind != protocol.EventToolMessage || pending == nil {
			continue
		}
		message, ok := toolMessage(record)
		if !ok {
			continue
		}
		for _, result := range message.Results {
			delete(pending.remaining, result.CallID)
		}
		if len(pending.remaining) == 0 {
			ranges = append(ranges, toolExchangeRange{assistant: pending.assistant, result: index})
			pending = nil
		}
	}
	return ranges
}

func assistantToolCallIDs(message *protocol.AssistantMessageV1) []string {
	if message == nil {
		return nil
	}
	seen := make(map[string]struct{})
	callIDs := make([]string, 0, len(message.Blocks)+len(message.ToolIntents))
	for _, block := range message.Blocks {
		if block.Kind != protocol.ContentToolUse || block.ToolUse == nil || block.ToolUse.CallID == "" {
			continue
		}
		if _, duplicate := seen[block.ToolUse.CallID]; duplicate {
			continue
		}
		seen[block.ToolUse.CallID] = struct{}{}
		callIDs = append(callIDs, block.ToolUse.CallID)
	}
	for _, intent := range message.ToolIntents {
		if intent.CallID == "" {
			continue
		}
		if _, duplicate := seen[intent.CallID]; duplicate {
			continue
		}
		seen[intent.CallID] = struct{}{}
		callIDs = append(callIDs, intent.CallID)
	}
	return callIDs
}

func boundedSelection(events []protocol.EventRecord, sources []protocol.ContentSource, start, cutoff int, trigger Trigger, limit int) (Selection, error) {
	included := make(map[int]struct{}, len(events))
	for index := cutoff + 1; index < len(events); index++ {
		included[index] = struct{}{}
	}
	positions := make(map[protocol.EventID]int, len(events))
	for index, record := range events {
		positions[record.Envelope.EventID] = index
	}
	for _, record := range activeSafetyFacts(events) {
		included[positions[record.Envelope.EventID]] = struct{}{}
	}
	selectedStart := cutoff + 1
	for index := cutoff; index >= start; index-- {
		_, already := included[index]
		if !already {
			included[index] = struct{}{}
		}
		candidate, err := selectionFromIndexes(events, sources, included, index, cutoff, trigger, limit)
		if err != nil {
			return Selection{}, err
		}
		input, err := summaryRequestInput(candidate, nil)
		if err != nil {
			return Selection{}, err
		}
		if len(input) > limit {
			if !already {
				delete(included, index)
			}
			if already || selectedStart > cutoff {
				return Selection{}, fmt.Errorf("required compaction safety facts exceed %d bytes", limit)
			}
			break
		}
		selectedStart = index
	}
	if selectedStart > cutoff {
		return Selection{}, ErrNothingToCompact
	}
	return selectionFromIndexes(events, sources, included, selectedStart, cutoff, trigger, limit)
}

func selectionFromIndexes(events []protocol.EventRecord, sources []protocol.ContentSource, included map[int]struct{}, start, cutoff int, trigger Trigger, limit int) (Selection, error) {
	boundedSources := make([]protocol.ContentSource, 0, len(included))
	for index, source := range sources {
		if _, ok := included[index]; ok {
			boundedSources = append(boundedSources, source)
		}
	}
	digest, err := SourceDigest(boundedSources)
	if err != nil {
		return Selection{}, err
	}
	return Selection{
		From: cursorFor(events[start]), Through: cursorFor(events[cutoff]), Trigger: trigger,
		Sources: boundedSources, SourceDigest: digest, InputLimitBytes: limit,
		SummarizedEventIDs: eventIDs(events[start : cutoff+1]),
	}, nil
}

func SourceDigest(sources []protocol.ContentSource) (protocol.Digest, error) {
	return canonicaljson.Digest(sources)
}

func committedEvents(events []protocol.EventRecord, head protocol.CommittedCursor) ([]protocol.EventRecord, error) {
	if len(events) == 0 {
		return nil, ErrNothingToCompact
	}
	committed := make([]protocol.EventRecord, 0, len(events))
	legacyPrefix := true
	for index, record := range events {
		legacy := record.Legacy != nil
		if record.Legacy != nil && record.Envelope.Seq == 0 && record.Envelope.EventID == "" {
			record.Envelope = protocol.EventEnvelope{
				SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1, JournalKind: head.JournalKind, JournalID: head.JournalID,
				EventID: record.Legacy.EventID, SessionID: record.Legacy.SessionID, Seq: record.Legacy.Seq, Time: record.Legacy.Time,
				Kind: record.Legacy.Kind, TransactionID: protocol.TransactionID("legacy:" + string(record.Legacy.EventID)), Payload: protocol.CloneRawMessage(record.Legacy.Payload),
			}
		}
		envelope := record.Envelope
		if envelope.Kind == protocol.EventTransactionCommitted {
			return nil, fmt.Errorf("transaction marker must not appear in context projection")
		}
		if envelope.JournalKind != head.JournalKind || envelope.JournalID != head.JournalID || envelope.SessionID != protocol.SessionID(head.JournalID) || envelope.EventID == "" || envelope.TransactionID == "" {
			return nil, fmt.Errorf("events are not a valid committed session journal projection: event identity at %d", index)
		}
		if envelope.Seq > head.CommitSeq {
			return nil, fmt.Errorf("events are not a valid committed session journal projection: event %d is beyond head %d", envelope.Seq, head.CommitSeq)
		}
		if index == 0 && envelope.Seq != 1 {
			return nil, fmt.Errorf("events are not a valid committed session journal projection")
		}
		if legacy && envelope.TransactionID != protocol.TransactionID("legacy:"+string(envelope.EventID)) {
			return nil, fmt.Errorf("legacy event has an invalid derived transaction identity")
		}
		if !legacy {
			legacyPrefix = false
		} else if !legacyPrefix {
			return nil, fmt.Errorf("legacy records may only form a contiguous prefix")
		}
		committed = append(committed, protocol.CloneEventRecord(record))
	}
	last := committed[len(committed)-1].Envelope
	markerBacked := head.CommitSeq == last.Seq+1
	if last.TransactionID != head.TransactionID || (!markerBacked && head.CommitSeq != last.Seq) {
		return nil, fmt.Errorf("committed head is not present in event sequence")
	}
	for index := 1; index < len(committed); index++ {
		previous, current := committed[index-1].Envelope, committed[index].Envelope
		if current.Seq <= previous.Seq {
			return nil, fmt.Errorf("events are not a valid committed session journal projection")
		}
		if !markerBacked {
			if current.Seq != previous.Seq+1 {
				return nil, fmt.Errorf("legacy projection is not contiguous")
			}
			continue
		}
		previousLegacy := committed[index-1].Legacy != nil && committed[index-1].Envelope.TransactionID == protocol.TransactionID("legacy:"+string(committed[index-1].Envelope.EventID))
		currentLegacy := committed[index].Legacy != nil && committed[index].Envelope.TransactionID == protocol.TransactionID("legacy:"+string(committed[index].Envelope.EventID))
		if previousLegacy && currentLegacy {
			if current.Seq != previous.Seq+1 {
				return nil, fmt.Errorf("legacy projection is not contiguous")
			}
			continue
		}
		if currentLegacy {
			return nil, fmt.Errorf("legacy records may only form a contiguous prefix")
		}
		if previousLegacy {
			if current.Seq != previous.Seq+1 {
				return nil, fmt.Errorf("legacy to v2 transition is not contiguous")
			}
			continue
		}
		if current.TransactionID == previous.TransactionID && current.Seq != previous.Seq+1 {
			return nil, fmt.Errorf("v2 transaction has a sequence gap")
		}
		if current.TransactionID != previous.TransactionID && current.Seq != previous.Seq+2 {
			return nil, fmt.Errorf("v2 transactions must be separated by one marker")
		}
	}
	return committed, nil
}

func latestCompactionThrough(events []protocol.EventRecord, head protocol.CommittedCursor) (int, bool) {
	latest := -1
	found := false
	for _, record := range events {
		if record.Envelope.Kind != protocol.EventContextCompacted {
			continue
		}
		value, ok := decodedCompaction(record)
		if !ok || value.From.Validate() != nil || value.Through.Validate() != nil || value.From.JournalKind != head.JournalKind || value.From.JournalID != head.JournalID || value.Through.JournalKind != head.JournalKind || value.Through.JournalID != head.JournalID || value.From.CommitSeq > value.Through.CommitSeq || value.Through.CommitSeq >= record.Envelope.Seq {
			continue
		}
		index, present := eventIndexAtCursor(events, value.Through)
		if !present {
			continue
		}
		if !found || value.Through.CommitSeq > events[latest].Envelope.Seq {
			latest, found = index, true
		}
	}
	return latest, found
}

func eventIndexAtCursor(events []protocol.EventRecord, cursor protocol.CommittedCursor) (int, bool) {
	for index, record := range events {
		if record.Envelope.Seq == cursor.CommitSeq && record.Envelope.TransactionID == cursor.TransactionID {
			return index, true
		}
	}
	return 0, false
}

func decodedCompaction(record protocol.EventRecord) (*protocol.ContextCompactedV1, bool) {
	switch value := record.Decoded.(type) {
	case *protocol.ContextCompactedV1:
		return value, value != nil
	case protocol.ContextCompactedV1:
		return &value, true
	default:
		return nil, false
	}
}

func retainedIDs(events []protocol.EventRecord, cutoff int) []protocol.EventID {
	keep := make(map[protocol.EventID]struct{}, len(events))
	for index := cutoff + 1; index < len(events); index++ {
		keep[events[index].Envelope.EventID] = struct{}{}
	}
	for _, record := range activeSafetyFacts(events) {
		keep[record.Envelope.EventID] = struct{}{}
	}
	ids := make([]protocol.EventID, 0, len(keep))
	for _, record := range events {
		if _, ok := keep[record.Envelope.EventID]; ok {
			ids = append(ids, record.Envelope.EventID)
		}
	}
	return ids
}

func activeSafetyFacts(events []protocol.EventRecord) []protocol.EventRecord {
	tasks := map[string]protocol.EventRecord{}
	outcomes := map[string]protocol.EventRecord{}
	requests := map[string]protocol.EventRecord{}
	resolved := map[string]struct{}{}
	for _, record := range events {
		switch record.Envelope.Kind {
		case protocol.EventTaskCreated:
			tasks[taskIdentity(record)] = record
		case protocol.EventTaskStatusChanged:
			if state, ok := taskStatus(record); ok {
				key := taskIdentity(record)
				if terminalTaskState(state.To) {
					delete(tasks, key)
				} else {
					tasks[key] = record
				}
			}
		case protocol.EventOutcomeContractDeclared, protocol.EventOutcomeContractAmended:
			if key, ok := outcomeIdentity(record); ok {
				outcomes[key] = record
			}
		case protocol.EventOutcomeFinalAssessed:
			if key, ok := outcomeIdentity(record); ok {
				delete(outcomes, key)
			}
		case protocol.EventAuthorizationRequested:
			if value, ok := authorizationRequest(record); ok && value != "" {
				requests[value] = record
			}
		case protocol.EventAuthorizationDecided:
			if value, ok := authorizationDecisionRequest(record); ok && value != "" {
				resolved[value] = struct{}{}
			}
		}
	}
	result := make([]protocol.EventRecord, 0, len(tasks)+len(outcomes)+len(requests))
	for _, record := range tasks {
		result = append(result, record)
	}
	for _, record := range outcomes {
		result = append(result, record)
	}
	for id, record := range requests {
		if _, ok := resolved[id]; !ok {
			result = append(result, record)
		}
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].Envelope.Seq < result[j].Envelope.Seq })
	return result
}

func taskIdentity(record protocol.EventRecord) string {
	if record.Envelope.TaskID != "" {
		return string(record.Envelope.TaskID)
	}
	return "event:" + string(record.Envelope.EventID)
}

func taskStatus(record protocol.EventRecord) (*protocol.TaskStatusChangedV1, bool) {
	switch value := record.Decoded.(type) {
	case *protocol.TaskStatusChangedV1:
		return value, value != nil
	case protocol.TaskStatusChangedV1:
		return &value, true
	default:
		return nil, false
	}
}

func terminalTaskState(state string) bool {
	switch protocol.TaskState(state) {
	case protocol.TaskVerified,
		protocol.TaskCompletedWithWaivers,
		protocol.TaskPartial,
		protocol.TaskCompleted,
		protocol.TaskFailed,
		protocol.TaskUnknown,
		protocol.TaskCancelled:
		return true
	default:
		return false
	}
}

func outcomeIdentity(record protocol.EventRecord) (string, bool) {
	switch value := record.Decoded.(type) {
	case *protocol.OutcomeContractDeclaredV1:
		return string(value.OutcomeContractID), value != nil && value.OutcomeContractID != ""
	case protocol.OutcomeContractDeclaredV1:
		return string(value.OutcomeContractID), value.OutcomeContractID != ""
	case *protocol.OutcomeContractAmendedV1:
		return string(value.OutcomeContractID), value != nil && value.OutcomeContractID != ""
	case protocol.OutcomeContractAmendedV1:
		return string(value.OutcomeContractID), value.OutcomeContractID != ""
	case *protocol.OutcomeFinalAssessedV1:
		return string(value.OutcomeContractID), value != nil && value.OutcomeContractID != ""
	case protocol.OutcomeFinalAssessedV1:
		return string(value.OutcomeContractID), value.OutcomeContractID != ""
	default:
		return "", false
	}
}

func authorizationRequest(record protocol.EventRecord) (string, bool) {
	switch value := record.Decoded.(type) {
	case *protocol.AuthorizationRequestedV1:
		return value.Request.RequestID, value != nil
	case protocol.AuthorizationRequestedV1:
		return value.Request.RequestID, true
	default:
		return "", false
	}
}

func authorizationDecisionRequest(record protocol.EventRecord) (string, bool) {
	switch value := record.Decoded.(type) {
	case *protocol.AuthorizationDecidedV1:
		return value.Decision.Request.RequestID, value != nil
	case protocol.AuthorizationDecidedV1:
		return value.Decision.Request.RequestID, true
	default:
		return "", false
	}
}

func normalizedSource(record protocol.EventRecord) (protocol.ContentSource, error) {
	text, err := normalizedEventText(record)
	if err != nil {
		return protocol.ContentSource{}, err
	}
	content := []protocol.ContentBlock{{Kind: protocol.ContentText, Text: text}}
	digest, err := canonicaljson.Digest(content)
	if err != nil {
		return protocol.ContentSource{}, err
	}
	return protocol.ContentSource{ID: string(record.Envelope.EventID), Kind: "journal_event", Scope: "session", Provenance: "journal:" + string(record.Envelope.JournalID), Digest: digest, Content: content}, nil
}

func normalizedEventText(record protocol.EventRecord) (string, error) {
	prefix := "kind=" + record.Envelope.Kind + " event_id=" + string(record.Envelope.EventID) + "\n"
	if message, ok := assistantMessage(record); ok {
		parts := make([]string, 0, len(message.Blocks))
		for _, block := range message.Blocks {
			if block.Kind == protocol.ContentToolResult && block.ToolResult != nil {
				parts = append(parts, fmt.Sprintf("tool_result call_id=%s status=%s evidence_ids=%s", block.ToolResult.CallID, block.ToolResult.Status, strings.Join(evidenceStrings(block.ToolResult.EvidenceIDs), ",")))
				continue
			}
			if block.Text != "" {
				parts = append(parts, block.Text)
			}
		}
		return boundEventText(prefix + strings.Join(parts, "\n")), nil
	}
	if message, ok := userMessage(record); ok {
		return boundEventText(prefix + message), nil
	}
	if record.Envelope.Kind == protocol.EventToolMessage {
		message, ok := toolMessage(record)
		if !ok {
			return "", fmt.Errorf("tool message event %q is invalid", record.Envelope.EventID)
		}
		if err := message.Validate(); err != nil {
			return "", fmt.Errorf("tool message event %q: %w", record.Envelope.EventID, err)
		}
		parts := make([]string, 0, len(message.Results))
		for _, result := range message.Results {
			parts = append(parts, fmt.Sprintf("tool_result call_id=%s status=%s evidence_ids=%s", result.CallID, result.Status, strings.Join(evidenceStrings(result.EvidenceIDs), ",")))
		}
		return boundEventText(prefix + strings.Join(parts, "\n")), nil
	}
	payload := record.Envelope.Payload
	if len(payload) == 0 {
		var err error
		payload, err = canonicaljson.Marshal(record.Decoded)
		if err != nil {
			return "", err
		}
	} else {
		var err error
		payload, err = canonicaljson.Marshal(payload)
		if err != nil {
			return "", err
		}
	}
	return boundEventText(prefix + string(payload)), nil
}

func assistantMessage(record protocol.EventRecord) (*protocol.AssistantMessageV1, bool) {
	switch value := record.Decoded.(type) {
	case *protocol.AssistantMessageV1:
		return value, value != nil
	case protocol.AssistantMessageV1:
		return &value, true
	default:
		return nil, false
	}
}

func toolMessage(record protocol.EventRecord) (*protocol.ToolMessageV1, bool) {
	switch value := record.Decoded.(type) {
	case *protocol.ToolMessageV1:
		return value, value != nil
	case protocol.ToolMessageV1:
		return &value, true
	default:
		var decoded protocol.ToolMessageV1
		if len(record.Envelope.Payload) == 0 || json.Unmarshal(record.Envelope.Payload, &decoded) != nil {
			return nil, false
		}
		return &decoded, true
	}
}

func userMessage(record protocol.EventRecord) (string, bool) {
	switch value := record.Decoded.(type) {
	case *protocol.UserMessageV1:
		return value.Content, value != nil
	case protocol.UserMessageV1:
		return value.Content, true
	default:
		return "", false
	}
}

func evidenceStrings(ids []protocol.EvidenceID) []string {
	result := make([]string, len(ids))
	for index, id := range ids {
		result[index] = string(id)
	}
	return result
}

func boundEventText(value string) string {
	if len(value) <= maxNormalizedEventBytes {
		return value
	}
	digest, _ := canonicaljson.Digest(value)
	return value[:maxNormalizedEventBytes] + "\npayload_truncated_bytes=" + fmt.Sprint(len(value)) + " digest=" + digest.Value
}

func cursorFor(record protocol.EventRecord) protocol.CommittedCursor {
	return protocol.CommittedCursor{JournalKind: record.Envelope.JournalKind, JournalID: record.Envelope.JournalID, CommitSeq: record.Envelope.Seq, TransactionID: record.Envelope.TransactionID}
}

func eventIDs(events []protocol.EventRecord) []protocol.EventID {
	ids := make([]protocol.EventID, len(events))
	for index := range events {
		ids[index] = events[index].Envelope.EventID
	}
	return ids
}

func cloneSelection(selection Selection) Selection { return protocol.DeepCopy(selection) }
