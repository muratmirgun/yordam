package context

import (
	"bytes"
	stdcontext "context"
	"encoding/json"
	"fmt"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/tooling"
	subagenttool "github.com/muratmirgun/yordam/internal/tools/subagent"
)

const (
	ExcludedBudget    = "context_budget_exceeded"
	ExcludedCompacted = "compacted_by_summary"

	defaultCompactionRevision = "none"
	estimatorProvenance       = "byte_estimate_v1"
)

type Planner interface {
	Plan(stdcontext.Context, Request) (protocol.ContextPlan, error)
}

type Request struct {
	Session                protocol.SessionID
	TaskID                 protocol.TaskID
	OutcomeContractID      protocol.OutcomeContractID
	OutcomeContractVersion uint32
	Events                 []protocol.EventRecord
	SystemInstructions     []protocol.ContentSource
	Model                  protocol.ModelDescriptor
	OutputReserve          int64
}

type boundedPlanner struct {
	toolExposureRevision string
	summaries            SummaryResolver
}

func NewPlanner(toolExposureRevision string, summaries SummaryResolver) Planner {
	return &boundedPlanner{toolExposureRevision: toolExposureRevision, summaries: summaries}
}

// NewPlannerForExposure binds a context plan to a validated immutable tool
// exposure rather than a caller-provided revision string.
func NewPlannerForExposure(exposure protocol.ToolExposure, summaries SummaryResolver) (Planner, error) {
	if err := exposure.Validate(); err != nil {
		return nil, fmt.Errorf("tool exposure: %w", err)
	}
	return NewPlanner(exposure.CatalogRevision, summaries), nil
}

// NewChildPlanner refuses the exact canonical orchestration descriptor. This
// uses the trusted identity/digest binding, not an alias string, so a child can
// never be handed the real subagent schema through a renamed alias.
func NewChildPlanner(parent, child protocol.ToolExposure, summaries SummaryResolver) (Planner, error) {
	if err := tooling.ValidateDerivedExposure(parent, child); err != nil {
		return nil, fmt.Errorf("child tool exposure: %w", err)
	}
	expected := subagenttool.BuiltinDescriptor()
	for _, binding := range child.Aliases {
		if binding.Identity == expected.Body.Identity && binding.SourceRevision == expected.Body.SourceRevision && binding.DescriptorDigest == expected.DescriptorDigest {
			return nil, fmt.Errorf("child tool exposure includes orchestrated subagent")
		}
	}
	return NewPlanner(child.CatalogRevision, summaries), nil
}

func (p *boundedPlanner) Plan(ctx stdcontext.Context, request Request) (protocol.ContextPlan, error) {
	if err := ctx.Err(); err != nil {
		return protocol.ContextPlan{}, err
	}
	if request.Session == "" || request.TaskID == "" || request.OutcomeContractID == "" || request.OutcomeContractVersion == 0 {
		return protocol.ContextPlan{}, fmt.Errorf("context planning identity is incomplete")
	}
	if p.toolExposureRevision == "" {
		return protocol.ContextPlan{}, fmt.Errorf("tool exposure revision is required")
	}
	if err := request.Model.Validate(); err != nil {
		return protocol.ContextPlan{}, fmt.Errorf("context model: %w", err)
	}
	if request.OutputReserve < 0 {
		return protocol.ContextPlan{}, fmt.Errorf("output reserve must not be negative")
	}
	if request.Model.MaximumOutput.State == protocol.ValueKnown && request.OutputReserve > request.Model.MaximumOutput.Value {
		return protocol.ContextPlan{}, fmt.Errorf("output reserve exceeds model maximum output")
	}

	sources := protocol.DeepCopy(request.SystemInstructions)
	eventSources, compacted, compactionRevision, err := adaptEvents(ctx, request.Session, p.summaries, request.Events)
	if err != nil {
		return protocol.ContextPlan{}, err
	}
	sources = append(sources, eventSources...)
	for index := range sources {
		if err := sources[index].Validate(); err != nil {
			return protocol.ContextPlan{}, fmt.Errorf("content source %q: %w", sources[index].ID, err)
		}
		digest, err := canonicaljson.Digest(sources[index].Content)
		if err != nil {
			return protocol.ContextPlan{}, err
		}
		sources[index].Digest = digest
	}

	available, bounded := int64(0), false
	if request.Model.ContextWindow.State == protocol.ValueKnown {
		available = request.Model.ContextWindow.Value - request.OutputReserve
		if available < 0 {
			return protocol.ContextPlan{}, fmt.Errorf("output reserve exceeds context window")
		}
		bounded = true
	}
	included := make([]protocol.ContentSource, 0, len(sources))
	excluded := protocol.DeepCopy(compacted)
	used := int64(0)
	seen := make(map[string]struct{}, len(sources)+len(excluded))
	for _, source := range excluded {
		seen[source.ID] = struct{}{}
	}
	for _, source := range sources {
		if _, duplicate := seen[source.ID]; duplicate {
			return protocol.ContextPlan{}, fmt.Errorf("duplicate content source %q", source.ID)
		}
		seen[source.ID] = struct{}{}
	}
	groups, err := groupContextSources(sources)
	if err != nil {
		return protocol.ContextPlan{}, err
	}
	for _, group := range groups {
		if bounded && used+group.tokens > available {
			for _, source := range group.sources {
				excluded = append(excluded, protocol.ExcludedContentSource{ID: source.ID, Reason: ExcludedBudget, Digest: source.Digest})
			}
			continue
		}
		included = append(included, protocol.DeepCopy(group.sources)...)
		used += group.tokens
	}
	body := protocol.ContextPlanBody{
		Sources: included, Excluded: excluded,
		EstimatedInputTokens: protocol.ValueInt64{State: protocol.ValueKnown, Value: used, Provenance: estimatorProvenance},
		OutputReserve:        request.OutputReserve, ContextWindow: protocol.DeepCopy(request.Model.ContextWindow),
		CompactionRevision: compactionRevision, ToolExposureRevision: p.toolExposureRevision,
	}
	digest, err := canonicaljson.Digest(body)
	if err != nil {
		return protocol.ContextPlan{}, err
	}
	return protocol.ContextPlan{Body: protocol.DeepCopy(body), Digest: digest}, nil
}

type sourceGroup struct {
	sources []protocol.ContentSource
	tokens  int64
}

func groupContextSources(sources []protocol.ContentSource) ([]sourceGroup, error) {
	groups := make([]sourceGroup, 0, len(sources))
	for index := 0; index < len(sources); index++ {
		source := sources[index]
		if source.Kind == "tool_message" {
			return nil, fmt.Errorf("tool source %q has no pending assistant tool call", source.ID)
		}
		group := sourceGroup{sources: []protocol.ContentSource{source}, tokens: estimateTokens(source.Content)}
		callIDs := toolCallIDs(source.Content)
		if source.Kind != "assistant_message" || len(callIDs) == 0 {
			groups = append(groups, group)
			continue
		}

		remaining := make(map[string]struct{}, len(callIDs))
		for _, callID := range callIDs {
			if _, duplicate := remaining[callID]; duplicate {
				return nil, fmt.Errorf("assistant source %q has duplicate tool call %q", source.ID, callID)
			}
			remaining[callID] = struct{}{}
		}
		for len(remaining) > 0 {
			index++
			if index >= len(sources) {
				return nil, fmt.Errorf("assistant source %q has unresolved tool results", source.ID)
			}
			resultSource := sources[index]
			if resultSource.Kind != "tool_message" {
				return nil, fmt.Errorf("assistant source %q is not followed by all tool results", source.ID)
			}
			for _, block := range resultSource.Content {
				if block.Kind != protocol.ContentToolResult || block.ToolResult == nil {
					return nil, fmt.Errorf("tool source %q has invalid result content", resultSource.ID)
				}
				callID := block.ToolResult.CallID
				if _, expected := remaining[callID]; !expected {
					return nil, fmt.Errorf("tool source %q has unexpected result %q", resultSource.ID, callID)
				}
				delete(remaining, callID)
			}
			group.sources = append(group.sources, resultSource)
			group.tokens += estimateTokens(resultSource.Content)
		}
		groups = append(groups, group)
	}
	return groups, nil
}

type legacyCompactionPayload struct {
	FromSeq    uint64 `json:"from_seq"`
	ThroughSeq uint64 `json:"through_seq"`
	Summary    string `json:"summary"`
}

func adaptEvents(ctx stdcontext.Context, session protocol.SessionID, summaries SummaryResolver, events []protocol.EventRecord) ([]protocol.ContentSource, []protocol.ExcludedContentSource, string, error) {
	type compaction struct {
		index         int
		from          uint64
		through       uint64
		fromCursor    protocol.CommittedCursor
		throughCursor protocol.CommittedCursor
		revision      string
		summary       string
		evidence      protocol.EvidenceID
		legacy        bool
	}
	candidates := make([]compaction, 0)
	for index, event := range events {
		if event.Envelope.Kind != protocol.EventContextCompacted {
			continue
		}
		if event.Legacy == nil {
			decoded, ok := contextCompaction(event)
			if !ok || !validNativeCompaction(event, session, decoded) {
				continue
			}
			candidates = append(candidates, compaction{index: index, from: decoded.From.CommitSeq, through: decoded.Through.CommitSeq, fromCursor: decoded.From, throughCursor: decoded.Through, revision: decoded.Revision, evidence: decoded.SummaryEvidenceID})
			continue
		}
		var payload legacyCompactionPayload
		if json.Unmarshal(event.Legacy.Payload, &payload) != nil || payload.Summary == "" || payload.FromSeq > payload.ThroughSeq || payload.ThroughSeq >= event.Envelope.Seq {
			continue
		}
		candidateRevision := fmt.Sprintf("legacy-v%d", event.Legacy.SchemaVersion)
		if decoded, ok := contextCompaction(event); ok && decoded.Revision != "" {
			candidateRevision = decoded.Revision
		}
		candidates = append(candidates, compaction{index: index, from: payload.FromSeq, through: payload.ThroughSeq, revision: candidateRevision, summary: payload.Summary, legacy: true})
	}
	for index := len(candidates) - 1; index >= 0; index-- {
		selected := candidates[index]
		if selected.legacy {
			summarySource, err := contentSource(string(events[selected.index].Envelope.EventID)+":summary", "compaction_summary", "legacy_context_compaction", []protocol.ContentBlock{{Kind: protocol.ContentText, Text: selected.summary}})
			if err != nil {
				return nil, nil, "", err
			}
			return adaptEventSuffix(events, selected.index, selected.from, selected.through, selected.revision, summarySource)
		}
		verified, ok := summaries.(VerifiedSummaryResolver)
		if !ok {
			continue
		}
		resolved, err := verified.ResolveVerifiedCompactionSummary(ctx, protocol.ContextCompactionReference{SessionID: session, From: selected.fromCursor, Through: selected.throughCursor, SummaryEvidenceID: selected.evidence, Revision: selected.revision})
		if err != nil || !validSummarySource(resolved, selected.evidence) {
			continue
		}
		return adaptEventSuffix(events, selected.index, selected.from, selected.through, selected.revision, resolved)
	}
	return adaptEventSuffix(events, -1, 0, 0, defaultCompactionRevision, protocol.ContentSource{})
}

func adaptEventSuffix(events []protocol.EventRecord, compactionIndex int, from, through uint64, compactionRevision string, summary protocol.ContentSource) ([]protocol.ContentSource, []protocol.ExcludedContentSource, string, error) {
	sources := make([]protocol.ContentSource, 0, len(events)+1)
	excluded := make([]protocol.ExcludedContentSource, 0)
	appendSource := func(source protocol.ContentSource, event protocol.EventRecord) {
		if compactionIndex >= 0 && event.Envelope.Seq >= from && event.Envelope.Seq <= through {
			excluded = append(excluded, protocol.ExcludedContentSource{ID: source.ID, Reason: ExcludedCompacted, Digest: source.Digest})
			return
		}
		sources = append(sources, source)
	}
	terminals := terminalTurns(events)
	var pending *pendingExchange
	flushPending := func() error {
		if pending == nil {
			return nil
		}
		if len(pending.remaining) == 0 {
			pending = nil
			return nil
		}
		terminalSeq, terminal := terminals[pending.assistant.turnID]
		if !terminal || terminalSeq <= pending.assistant.seq {
			for _, callID := range pending.ordered {
				if _, unresolved := pending.remaining[callID]; unresolved {
					return fmt.Errorf("assistant event %q has unresolved tool call %q", pending.assistant.eventID, callID)
				}
			}
		}
		for _, callID := range pending.ordered {
			if _, unresolved := pending.remaining[callID]; !unresolved {
				continue
			}
			result := protocol.ToolResultBlock{CallID: callID, Status: "uncertain", Text: "historical tool result unavailable"}
			source, err := resultSource(string(pending.assistant.eventID)+":legacy-tool-result:"+callID, "legacy_tool_result", result)
			if err != nil {
				return err
			}
			appendSource(source, protocol.EventRecord{Envelope: protocol.EventEnvelope{Seq: 0}})
		}
		pending = nil
		return nil
	}
	for index, event := range events {
		if index == compactionIndex {
			sources = append(sources, protocol.DeepCopy(summary))
			continue
		}
		switch event.Envelope.Kind {
		case protocol.EventUserMessage:
			if err := flushPending(); err != nil {
				return nil, nil, "", err
			}
			content, ok := userMessageContent(event)
			if !ok || content == "" {
				continue
			}
			source, err := contentSource(string(event.Envelope.EventID), "user_message", "event_v2", []protocol.ContentBlock{{Kind: protocol.ContentText, Text: content}})
			if err != nil {
				return nil, nil, "", err
			}
			appendSource(source, event)
		case protocol.EventAssistantMessage:
			if err := flushPending(); err != nil {
				return nil, nil, "", err
			}
			assistant, ok := assistantMessage(event)
			if !ok {
				continue
			}
			blocks, err := canonicalAssistantBlocks(assistant)
			if err != nil {
				return nil, nil, "", fmt.Errorf("assistant event %q: %w", event.Envelope.EventID, err)
			}
			if len(blocks) == 0 {
				continue
			}
			source, err := contentSource(string(event.Envelope.EventID), "assistant_message", "event_v2", blocks)
			if err != nil {
				return nil, nil, "", err
			}
			appendSource(source, event)
			ordered := toolCallIDs(blocks)
			if len(ordered) != 0 {
				remaining := make(map[string]struct{}, len(ordered))
				for _, callID := range ordered {
					remaining[callID] = struct{}{}
				}
				pending = &pendingExchange{assistant: transcriptEntry{source: source, eventID: event.Envelope.EventID, turnID: event.Envelope.TurnID, seq: event.Envelope.Seq}, ordered: ordered, remaining: remaining}
			}
		case protocol.EventToolMessage:
			message, ok := toolMessage(event)
			if !ok {
				return nil, nil, "", fmt.Errorf("tool message event %q is invalid", event.Envelope.EventID)
			}
			if err := message.Validate(); err != nil {
				return nil, nil, "", fmt.Errorf("tool message event %q: %w", event.Envelope.EventID, err)
			}
			for resultIndex, result := range message.Results {
				if pending == nil {
					return nil, nil, "", fmt.Errorf("tool result %q from source event %q is late", result.CallID, event.Envelope.EventID)
				}
				if _, expected := pending.remaining[result.CallID]; !expected {
					if containsToolCall(pending.ordered, result.CallID) {
						return nil, nil, "", fmt.Errorf("tool result %q from source event %q is duplicate", result.CallID, event.Envelope.EventID)
					}
					return nil, nil, "", fmt.Errorf("tool result %q from source event %q is unknown", result.CallID, event.Envelope.EventID)
				}
				id := string(event.Envelope.EventID)
				if len(message.Results) > 1 {
					id = fmt.Sprintf("%s:%d", id, resultIndex)
				}
				source, err := resultSource(id, "event_v2", result)
				if err != nil {
					return nil, nil, "", err
				}
				appendSource(source, event)
				delete(pending.remaining, result.CallID)
			}
		}
	}
	if err := flushPending(); err != nil {
		return nil, nil, "", err
	}
	return sources, excluded, compactionRevision, nil
}

type transcriptEntry struct {
	source  protocol.ContentSource
	eventID protocol.EventID
	turnID  protocol.TurnID
	seq     uint64
}

type pendingExchange struct {
	assistant transcriptEntry
	ordered   []string
	remaining map[string]struct{}
}

func terminalTurns(events []protocol.EventRecord) map[protocol.TurnID]uint64 {
	terminal := make(map[protocol.TurnID]uint64)
	for _, event := range events {
		switch event.Envelope.Kind {
		case protocol.EventTurnCompleted, protocol.EventTurnFailed, protocol.EventTurnInterrupted:
			if event.Envelope.TurnID != "" && event.Envelope.Seq > terminal[event.Envelope.TurnID] {
				terminal[event.Envelope.TurnID] = event.Envelope.Seq
			}
		}
	}
	return terminal
}

func toolCallIDs(blocks []protocol.ContentBlock) []string {
	ordered := make([]string, 0)
	for _, block := range blocks {
		if block.Kind == protocol.ContentToolUse && block.ToolUse != nil {
			ordered = append(ordered, block.ToolUse.CallID)
		}
	}
	return ordered
}

func containsToolCall(callIDs []string, callID string) bool {
	for _, candidate := range callIDs {
		if candidate == callID {
			return true
		}
	}
	return false
}

func canonicalAssistantBlocks(assistant protocol.AssistantMessageV1) ([]protocol.ContentBlock, error) {
	type occurrence struct {
		intent protocol.ToolUseBlock
		modern bool
		legacy bool
	}
	blocks := protocol.DeepCopy(assistant.Blocks)
	seen := make(map[string]occurrence)
	for _, block := range blocks {
		if block.Kind != protocol.ContentToolUse {
			continue
		}
		if block.ToolUse == nil || block.ToolUse.CallID == "" {
			return nil, fmt.Errorf("invalid modern tool intent")
		}
		if _, duplicate := seen[block.ToolUse.CallID]; duplicate {
			return nil, fmt.Errorf("duplicate modern tool intent %q", block.ToolUse.CallID)
		}
		seen[block.ToolUse.CallID] = occurrence{intent: protocol.DeepCopy(*block.ToolUse), modern: true}
	}
	for _, intent := range assistant.ToolIntents {
		if intent.CallID == "" {
			return nil, fmt.Errorf("invalid legacy tool intent")
		}
		if existing, found := seen[intent.CallID]; found {
			if existing.legacy {
				return nil, fmt.Errorf("duplicate legacy tool intent %q", intent.CallID)
			}
			if existing.intent.Alias != intent.Alias || !bytes.Equal(existing.intent.Arguments, intent.Arguments) {
				return nil, fmt.Errorf("conflicting tool intent %q", intent.CallID)
			}
			existing.legacy = true
			seen[intent.CallID] = existing
			continue
		}
		intentCopy := protocol.DeepCopy(intent)
		blocks = append(blocks, protocol.ContentBlock{Kind: protocol.ContentToolUse, ToolUse: &intentCopy})
		seen[intent.CallID] = occurrence{intent: intentCopy, legacy: true}
	}
	return blocks, nil
}

func validNativeCompaction(event protocol.EventRecord, session protocol.SessionID, payload protocol.ContextCompactedV1) bool {
	if payload.Validate() != nil || event.Envelope.JournalKind != protocol.JournalSession || event.Envelope.SessionID != session || event.Envelope.JournalID != protocol.JournalID(session) || payload.From.JournalID != event.Envelope.JournalID || payload.Through.JournalID != event.Envelope.JournalID || payload.Through.CommitSeq >= event.Envelope.Seq {
		return false
	}
	return true
}

func validSummarySource(source protocol.ContentSource, evidenceID protocol.EvidenceID) bool {
	if source.ID != string(evidenceID) || source.Kind != "compaction_summary" || source.Scope != "session" || source.Provenance != "evidence:"+string(evidenceID) || len(source.Content) != 1 || source.Content[0].Kind != protocol.ContentText || source.Validate() != nil {
		return false
	}
	return len(source.Content[0].Text) <= MaxCompactionSummaryBytes
}

func contextCompaction(event protocol.EventRecord) (protocol.ContextCompactedV1, bool) {
	switch payload := event.Decoded.(type) {
	case *protocol.ContextCompactedV1:
		return protocol.DeepCopy(*payload), true
	case protocol.ContextCompactedV1:
		return protocol.DeepCopy(payload), true
	default:
		var decoded protocol.ContextCompactedV1
		if json.Unmarshal(event.Envelope.Payload, &decoded) != nil {
			return protocol.ContextCompactedV1{}, false
		}
		return decoded, true
	}
}

func userMessageContent(event protocol.EventRecord) (string, bool) {
	switch payload := event.Decoded.(type) {
	case *protocol.UserMessageV1:
		return payload.Content, true
	case protocol.UserMessageV1:
		return payload.Content, true
	default:
		var decoded protocol.UserMessageV1
		if json.Unmarshal(event.Envelope.Payload, &decoded) != nil {
			return "", false
		}
		return decoded.Content, true
	}
}

func assistantMessage(event protocol.EventRecord) (protocol.AssistantMessageV1, bool) {
	switch payload := event.Decoded.(type) {
	case *protocol.AssistantMessageV1:
		return protocol.DeepCopy(*payload), true
	case protocol.AssistantMessageV1:
		return protocol.DeepCopy(payload), true
	default:
		var decoded protocol.AssistantMessageV1
		if json.Unmarshal(event.Envelope.Payload, &decoded) != nil {
			return protocol.AssistantMessageV1{}, false
		}
		return decoded, true
	}
}

func toolMessage(event protocol.EventRecord) (protocol.ToolMessageV1, bool) {
	switch payload := event.Decoded.(type) {
	case *protocol.ToolMessageV1:
		return protocol.DeepCopy(*payload), true
	case protocol.ToolMessageV1:
		return protocol.DeepCopy(payload), true
	default:
		var decoded protocol.ToolMessageV1
		if json.Unmarshal(event.Envelope.Payload, &decoded) != nil {
			return protocol.ToolMessageV1{}, false
		}
		return decoded, true
	}
}

func resultSource(id, provenance string, result protocol.ToolResultBlock) (protocol.ContentSource, error) {
	resultCopy := protocol.DeepCopy(result)
	return contentSource(id, "tool_message", provenance, []protocol.ContentBlock{{Kind: protocol.ContentToolResult, ToolResult: &resultCopy}})
}

func contentSource(id, kind, provenance string, blocks []protocol.ContentBlock) (protocol.ContentSource, error) {
	digest, err := canonicaljson.Digest(blocks)
	if err != nil {
		return protocol.ContentSource{}, err
	}
	source := protocol.ContentSource{ID: id, Kind: kind, Scope: "session", Provenance: provenance, Digest: digest, Content: protocol.DeepCopy(blocks)}
	if err := source.Validate(); err != nil {
		return protocol.ContentSource{}, err
	}
	return source, nil
}

func estimateTokens(blocks []protocol.ContentBlock) int64 {
	var bytes int64
	for _, block := range blocks {
		switch block.Kind {
		case protocol.ContentText:
			bytes += int64(len(block.Text))
		case protocol.ContentJSON:
			bytes += int64(len(block.JSON))
		case protocol.ContentReasoningSummary:
			bytes += int64(len(block.ReasoningSummary))
		default:
			raw, err := canonicaljson.Marshal(block)
			if err == nil {
				bytes += int64(len(raw))
			}
		}
	}
	if bytes == 0 {
		return 0
	}
	return (bytes + 3) / 4
}
