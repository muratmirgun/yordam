package context

import (
	stdcontext "context"
	"encoding/json"
	"fmt"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
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
func NewChildPlanner(exposure protocol.ToolExposure, summaries SummaryResolver) (Planner, error) {
	if err := exposure.Validate(); err != nil {
		return nil, fmt.Errorf("child tool exposure: %w", err)
	}
	expected := subagenttool.BuiltinDescriptor()
	for _, binding := range exposure.Aliases {
		if binding.Identity == expected.Body.Identity && binding.SourceRevision == expected.Body.SourceRevision && binding.DescriptorDigest == expected.DescriptorDigest {
			return nil, fmt.Errorf("child tool exposure includes orchestrated subagent")
		}
	}
	return NewPlanner(exposure.CatalogRevision, summaries), nil
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
		tokens := estimateTokens(source.Content)
		if bounded && used+tokens > available {
			excluded = append(excluded, protocol.ExcludedContentSource{ID: source.ID, Reason: ExcludedBudget, Digest: source.Digest})
			continue
		}
		included = append(included, protocol.DeepCopy(source))
		used += tokens
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
	for index, event := range events {
		if index == compactionIndex {
			sources = append(sources, protocol.DeepCopy(summary))
			continue
		}
		var blocks []protocol.ContentBlock
		kind := ""
		switch event.Envelope.Kind {
		case protocol.EventUserMessage:
			content, ok := userMessageContent(event)
			if !ok || content == "" {
				continue
			}
			kind, blocks = "user_message", []protocol.ContentBlock{{Kind: protocol.ContentText, Text: content}}
		case protocol.EventAssistantMessage:
			assistant, ok := assistantMessage(event)
			if !ok {
				continue
			}
			kind, blocks = "assistant_message", protocol.DeepCopy(assistant.Blocks)
			for _, intent := range assistant.ToolIntents {
				intentCopy := protocol.DeepCopy(intent)
				blocks = append(blocks, protocol.ContentBlock{Kind: protocol.ContentToolUse, ToolUse: &intentCopy})
			}
			if len(blocks) == 0 {
				continue
			}
		default:
			continue
		}
		source, err := contentSource(string(event.Envelope.EventID), kind, "event_v2", blocks)
		if err != nil {
			return nil, nil, "", err
		}
		if compactionIndex >= 0 && event.Envelope.Seq >= from && event.Envelope.Seq <= through {
			excluded = append(excluded, protocol.ExcludedContentSource{ID: source.ID, Reason: ExcludedCompacted, Digest: source.Digest})
			continue
		}
		sources = append(sources, source)
	}
	return sources, excluded, compactionRevision, nil
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
