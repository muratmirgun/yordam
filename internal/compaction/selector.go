package compaction

import (
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
		start = int(priorThrough)
	}
	cutoff := len(committed) - recentSuffixEvents - 1
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
	selectedStart, included, err := boundedSelectionSources(committed, sources, start, cutoff, manualInputBytes)
	if err != nil {
		return Selection{}, err
	}
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
	selection := Selection{
		From:               cursorFor(committed[selectedStart]),
		Through:            cursorFor(committed[cutoff]),
		Trigger:            trigger,
		Sources:            boundedSources,
		SourceDigest:       digest,
		SummarizedEventIDs: eventIDs(committed[selectedStart : cutoff+1]),
	}
	selection.RetainedEventIDs = retainedIDs(committed, cutoff)
	return cloneSelection(selection), nil
}

func boundedSelectionSources(events []protocol.EventRecord, sources []protocol.ContentSource, start, cutoff, limit int) (int, map[int]struct{}, error) {
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
	used := 0
	for index := range included {
		used += sourceSize(sources[index])
	}
	if used > limit {
		return 0, nil, fmt.Errorf("required compaction safety facts exceed %d bytes", limit)
	}
	selectedStart := cutoff + 1
	for index := cutoff; index >= start; index-- {
		if _, already := included[index]; already {
			selectedStart = index
			continue
		}
		candidate := sourceSize(sources[index])
		if used+candidate > limit {
			break
		}
		included[index] = struct{}{}
		used += candidate
		selectedStart = index
	}
	if selectedStart > cutoff {
		return 0, nil, ErrNothingToCompact
	}
	return selectedStart, included, nil
}

func SourceDigest(sources []protocol.ContentSource) (protocol.Digest, error) {
	return canonicaljson.Digest(sources)
}

func committedEvents(events []protocol.EventRecord, head protocol.CommittedCursor) ([]protocol.EventRecord, error) {
	if len(events) == 0 {
		return nil, ErrNothingToCompact
	}
	committed := make([]protocol.EventRecord, 0, len(events))
	expected := uint64(1)
	for _, record := range events {
		envelope := record.Envelope
		if envelope.JournalKind != head.JournalKind || envelope.JournalID != head.JournalID || envelope.SessionID != protocol.SessionID(head.JournalID) || envelope.EventID == "" || envelope.TransactionID == "" || envelope.Seq != expected {
			return nil, fmt.Errorf("events are not a contiguous committed session journal")
		}
		if envelope.Seq > head.CommitSeq {
			break
		}
		committed = append(committed, protocol.CloneEventRecord(record))
		expected++
	}
	if len(committed) == 0 || uint64(len(committed)) != head.CommitSeq || committed[len(committed)-1].Envelope.TransactionID != head.TransactionID {
		return nil, fmt.Errorf("committed head is not present in event sequence")
	}
	return committed, nil
}

func latestCompactionThrough(events []protocol.EventRecord, head protocol.CommittedCursor) (uint64, bool) {
	var latest uint64
	found := false
	for _, record := range events {
		if record.Envelope.Kind != protocol.EventContextCompacted {
			continue
		}
		value, ok := decodedCompaction(record)
		if !ok || value.From.Validate() != nil || value.Through.Validate() != nil || value.From.JournalKind != head.JournalKind || value.From.JournalID != head.JournalID || value.Through.JournalKind != head.JournalKind || value.Through.JournalID != head.JournalID || value.From.CommitSeq > value.Through.CommitSeq || value.Through.CommitSeq >= record.Envelope.Seq {
			continue
		}
		if !found || value.Through.CommitSeq > latest {
			latest, found = value.Through.CommitSeq, true
		}
	}
	return latest, found
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
	var tasks, outcomes []protocol.EventRecord
	requests := map[string]protocol.EventRecord{}
	resolved := map[string]struct{}{}
	for _, record := range events {
		switch record.Envelope.Kind {
		case protocol.EventTaskCreated:
			tasks = append(tasks, record)
		case protocol.EventTaskStatusChanged:
			if state, ok := record.Decoded.(*protocol.StateChangedV1); ok && (state.To == string(protocol.TaskCompleted) || state.To == string(protocol.TaskFailed) || state.To == string(protocol.TaskCancelled)) {
				tasks = nil
			}
		case protocol.EventOutcomeContractDeclared, protocol.EventOutcomeContractAmended:
			outcomes = append(outcomes, record)
		case protocol.EventOutcomeFinalAssessed:
			outcomes = nil
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
	result := append([]protocol.EventRecord{}, tasks...)
	result = append(result, outcomes...)
	for id, record := range requests {
		if _, ok := resolved[id]; !ok {
			result = append(result, record)
		}
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].Envelope.Seq < result[j].Envelope.Seq })
	return result
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

func sourcesSize(sources []protocol.ContentSource) int {
	total := 0
	for _, source := range sources {
		for _, block := range source.Content {
			total += len(block.Text)
		}
	}
	return total
}

func sourceSize(source protocol.ContentSource) int {
	total := 0
	for _, block := range source.Content {
		total += len(block.Text)
	}
	return total
}

func cloneSelection(selection Selection) Selection { return protocol.DeepCopy(selection) }
