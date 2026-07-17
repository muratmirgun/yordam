package protocol

import (
	"encoding/json"
	"fmt"
)

type ValueInt64 struct {
	State      ValueState `json:"state"`
	Value      int64      `json:"value,omitempty"`
	Provenance string     `json:"provenance,omitempty"`
}

func (v ValueInt64) Validate() error {
	if !v.State.Valid() || (v.State != ValueKnown && v.Value != 0) {
		return fmt.Errorf("invalid int64 value state")
	}
	if v.State == ValueKnown && v.Value < 0 {
		return fmt.Errorf("known int64 value must not be negative")
	}
	return nil
}

type PricingFact struct {
	Category          string `json:"category"`
	PerMillionDecimal string `json:"per_million_decimal"`
	Currency          string `json:"currency"`
	Provenance        string `json:"provenance"`
}

type ToolUseBlock struct {
	CallID    string          `json:"call_id"`
	Alias     string          `json:"alias"`
	Arguments json.RawMessage `json:"arguments"`
}

func (b ToolUseBlock) Validate() error {
	if b.CallID == "" || b.Alias == "" {
		return fmt.Errorf("tool-use call ID and alias are required")
	}
	return ValidateRawJSON(b.Arguments)
}

type ToolResultBlock struct {
	CallID      string          `json:"call_id"`
	Status      string          `json:"status"`
	Text        string          `json:"text,omitempty"`
	JSON        json.RawMessage `json:"json,omitempty"`
	EvidenceIDs []EvidenceID    `json:"evidence_ids,omitempty"`
}

func (b ToolResultBlock) Validate() error {
	if b.CallID == "" || b.Status == "" {
		return fmt.Errorf("tool-result call ID and status are required")
	}
	if len(b.Text) > MaxStringBytes {
		return fmt.Errorf("tool-result text exceeds limit")
	}
	if b.JSON != nil {
		if err := ValidateRawJSON(b.JSON); err != nil {
			return err
		}
	}
	return validateSortedUniqueEvidenceIDs(b.EvidenceIDs)
}

type ContentReference struct {
	URI       string `json:"uri"`
	Name      string `json:"name,omitempty"`
	MediaType string `json:"media_type"`
	Digest    Digest `json:"digest"`
}

type RefusalBlock struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type ExcludedContentSource struct {
	ID     string `json:"id"`
	Reason string `json:"reason"`
	Digest Digest `json:"digest,omitempty"`
}

type UsageState string

const (
	UsageProviderReported UsageState = "provider_reported"
	UsageEstimated        UsageState = "estimated"
	UsageUnknown          UsageState = "unknown"
)

type UsageValue struct {
	State      UsageState `json:"state"`
	Value      int64      `json:"value,omitempty"`
	Provenance string     `json:"provenance,omitempty"`
}

func (v UsageValue) Validate() error {
	if v.State != UsageProviderReported && v.State != UsageEstimated && v.State != UsageUnknown {
		return fmt.Errorf("invalid usage state %q", v.State)
	}
	if v.Value < 0 || (v.State == UsageUnknown && v.Value != 0) {
		return fmt.Errorf("invalid usage value")
	}
	return nil
}

type ModelUsage struct {
	Input      UsageValue `json:"input"`
	Output     UsageValue `json:"output"`
	Cached     UsageValue `json:"cached"`
	CacheWrite UsageValue `json:"cache_write"`
	Reasoning  UsageValue `json:"reasoning"`
}

func (u ModelUsage) Validate() error {
	for _, value := range []UsageValue{u.Input, u.Output, u.Cached, u.CacheWrite, u.Reasoning} {
		if err := value.Validate(); err != nil {
			return err
		}
	}
	return nil
}

type CostValue struct {
	State      ValueState `json:"state"`
	Decimal    string     `json:"decimal,omitempty"`
	Currency   string     `json:"currency,omitempty"`
	Provenance string     `json:"provenance,omitempty"`
}

func (v CostValue) Validate() error {
	if !v.State.Valid() {
		return fmt.Errorf("invalid cost state")
	}
	if v.State == ValueKnown {
		if v.Decimal == "" || v.Currency == "" {
			return fmt.Errorf("known cost requires decimal and currency")
		}
	} else if v.Decimal != "" || v.Currency != "" {
		return fmt.Errorf("unknown cost must not contain a value")
	}
	return nil
}

type ContentSource struct {
	ID         string         `json:"id"`
	Kind       string         `json:"kind"`
	Scope      string         `json:"scope"`
	Provenance string         `json:"provenance"`
	Digest     Digest         `json:"digest"`
	Content    []ContentBlock `json:"content"`
}

type ContentBlock struct {
	Kind             string            `json:"kind"`
	Text             string            `json:"text,omitempty"`
	ToolUse          *ToolUseBlock     `json:"tool_use,omitempty"`
	ToolResult       *ToolResultBlock  `json:"tool_result,omitempty"`
	Reference        *ContentReference `json:"reference,omitempty"`
	JSON             json.RawMessage   `json:"json,omitempty"`
	ReasoningSummary string            `json:"reasoning_summary,omitempty"`
	Refusal          *RefusalBlock     `json:"refusal,omitempty"`
}

const (
	ContentText             = "text"
	ContentToolUse          = "tool_use"
	ContentToolResult       = "tool_result"
	ContentReferenceKind    = "reference"
	ContentJSON             = "json"
	ContentReasoningSummary = "reasoning_summary"
	ContentRefusal          = "refusal"

	CapabilityTextInput        = "text_input"
	CapabilityTextOutput       = "text_output"
	CapabilityStreaming        = "streaming"
	CapabilityToolUse          = "tool_use"
	CapabilityStructuredOutput = "structured_output"
	CapabilityImageInput       = "image_input"
	CapabilityFileInput        = "file_input"
	CapabilityReasoningSummary = "reasoning_summary"
	CapabilityUsageReporting   = "usage_reporting"

	CapabilitySupported   = "supported"
	CapabilityUnsupported = "unsupported"
	CapabilityUnknown     = "unknown"

	CapabilityRequired  = "required"
	CapabilityPreferred = "preferred"
	CapabilityUnused    = "unused"
)

func (b ContentBlock) Validate() error {
	if err := ValidateBounds(b); err != nil {
		return err
	}
	present := 0
	if b.Text != "" {
		present++
	}
	if b.ToolUse != nil {
		present++
	}
	if b.ToolResult != nil {
		present++
	}
	if b.Reference != nil {
		present++
	}
	if b.JSON != nil {
		present++
	}
	if b.ReasoningSummary != "" {
		present++
	}
	if b.Refusal != nil {
		present++
	}
	if present != 1 {
		return fmt.Errorf("content block requires exactly one value")
	}
	switch b.Kind {
	case ContentText:
		if b.Text == "" {
			return fmt.Errorf("text block requires text")
		}
	case ContentToolUse:
		if b.ToolUse == nil {
			return fmt.Errorf("tool-use block requires tool_use")
		}
		return b.ToolUse.Validate()
	case ContentToolResult:
		if b.ToolResult == nil {
			return fmt.Errorf("tool-result block requires tool_result")
		}
		return b.ToolResult.Validate()
	case ContentReferenceKind:
		if b.Reference == nil || b.Reference.URI == "" || b.Reference.MediaType == "" {
			return fmt.Errorf("reference block is incomplete")
		}
		return b.Reference.Digest.Validate()
	case ContentJSON:
		if b.JSON == nil {
			return fmt.Errorf("JSON block requires json")
		}
		return ValidateRawJSON(b.JSON)
	case ContentReasoningSummary:
		if b.ReasoningSummary == "" {
			return fmt.Errorf("reasoning summary is required")
		}
	case ContentRefusal:
		if b.Refusal == nil || b.Refusal.Code == "" || b.Refusal.Message == "" {
			return fmt.Errorf("refusal block is incomplete")
		}
	default:
		return fmt.Errorf("invalid content block kind %q", b.Kind)
	}
	return nil
}

type CapabilityFact struct {
	Capability          string              `json:"capability"`
	State               string              `json:"state"`
	Provenance          string              `json:"provenance"`
	RuntimeGenerationID RuntimeGenerationID `json:"runtime_generation_id"`
}

func (f CapabilityFact) Validate() error {
	if f.Capability == "" || f.Provenance == "" || f.RuntimeGenerationID == "" {
		return fmt.Errorf("capability fact is incomplete")
	}
	if f.State != CapabilitySupported && f.State != CapabilityUnsupported && f.State != CapabilityUnknown {
		return fmt.Errorf("invalid capability state %q", f.State)
	}
	return nil
}

type CapabilityRequirement struct {
	Capability string `json:"capability"`
	Level      string `json:"level"`
}

func (r CapabilityRequirement) Validate() error {
	if r.Capability == "" {
		return fmt.Errorf("capability is required")
	}
	if r.Level != CapabilityRequired && r.Level != CapabilityPreferred && r.Level != CapabilityUnused {
		return fmt.Errorf("invalid capability requirement level %q", r.Level)
	}
	return nil
}

type ModelDescriptor struct {
	ProviderID           ProviderID          `json:"provider_id"`
	ModelID              ModelID             `json:"model_id"`
	AdapterKind          string              `json:"adapter_kind"`
	DisplayName          string              `json:"display_name"`
	ContextWindow        ValueInt64          `json:"context_window"`
	MaximumOutput        ValueInt64          `json:"maximum_output"`
	Capabilities         []CapabilityFact    `json:"capabilities"`
	UsageCategories      []string            `json:"usage_categories"`
	Pricing              []PricingFact       `json:"pricing"`
	CredentialBindingRef string              `json:"credential_binding_ref"`
	SourceRevision       string              `json:"source_revision"`
	RuntimeGenerationID  RuntimeGenerationID `json:"runtime_generation_id"`
}

type NegotiatedProviderPlanBody struct {
	Descriptor           ModelDescriptor         `json:"descriptor"`
	Requirements         []CapabilityRequirement `json:"requirements"`
	ToolExposureRevision string                  `json:"tool_exposure_revision"`
	Warnings             []string                `json:"warnings"`
}

type NegotiatedProviderPlan struct {
	Body   NegotiatedProviderPlanBody `json:"body"`
	Digest Digest                     `json:"digest"`
}

type ContextPlanBody struct {
	Sources              []ContentSource         `json:"sources"`
	Excluded             []ExcludedContentSource `json:"excluded"`
	EstimatedInputTokens ValueInt64              `json:"estimated_input_tokens"`
	OutputReserve        int64                   `json:"output_reserve"`
	ContextWindow        ValueInt64              `json:"context_window"`
	CompactionRevision   string                  `json:"compaction_revision"`
	ToolExposureRevision string                  `json:"tool_exposure_revision"`
}

type ContextPlan struct {
	Body   ContextPlanBody `json:"body"`
	Digest Digest          `json:"digest"`
}

type ModelRequest struct {
	RequestID    string                  `json:"request_id"`
	ProviderID   ProviderID              `json:"provider_id"`
	ModelID      ModelID                 `json:"model_id"`
	Messages     []ModelMessage          `json:"messages"`
	Tools        ToolExposure            `json:"tools"`
	Requirements []CapabilityRequirement `json:"requirements"`
	Plan         NegotiatedProviderPlan  `json:"plan"`
}

type ModelMessage struct {
	Role   string         `json:"role"`
	Blocks []ContentBlock `json:"blocks"`
}

type ContentDelta struct {
	BlockID      string `json:"block_id"`
	Kind         string `json:"kind"`
	Text         string `json:"text,omitempty"`
	JSONFragment string `json:"json_fragment,omitempty"`
}

type ModelTerminal struct {
	Reason          string `json:"reason"`
	NativeReason    string `json:"native_reason,omitempty"`
	ServerRequestID string `json:"server_request_id,omitempty"`
}

type ProviderError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type ModelEvent struct {
	Kind       string         `json:"kind"`
	Sequence   uint64         `json:"sequence"`
	Delta      *ContentDelta  `json:"delta,omitempty"`
	Block      *ContentBlock  `json:"block,omitempty"`
	ToolIntent *ToolUseBlock  `json:"tool_intent,omitempty"`
	Usage      *ModelUsage    `json:"usage,omitempty"`
	Terminal   *ModelTerminal `json:"terminal,omitempty"`
	Error      *ProviderError `json:"error,omitempty"`
}

const (
	ModelEventContentDelta = "content_delta"
	ModelEventContentBlock = "content_block"
	ModelEventToolIntent   = "tool_intent"
	ModelEventUsageUpdate  = "usage_update"
	ModelEventTerminal     = "terminal"
	ModelEventError        = "error"
)

func (e ModelEvent) Validate() error {
	if err := ValidateBounds(e); err != nil {
		return err
	}
	if e.Sequence == 0 {
		return fmt.Errorf("model event sequence is required")
	}
	present := 0
	for _, exists := range []bool{e.Delta != nil, e.Block != nil, e.ToolIntent != nil, e.Usage != nil, e.Terminal != nil, e.Error != nil} {
		if exists {
			present++
		}
	}
	if present != 1 {
		return fmt.Errorf("model event requires exactly one payload")
	}
	switch e.Kind {
	case ModelEventContentDelta:
		if e.Delta == nil || e.Delta.BlockID == "" || !oneContentDeltaValue(*e.Delta) {
			return fmt.Errorf("invalid content delta")
		}
	case ModelEventContentBlock:
		if e.Block == nil {
			return fmt.Errorf("content-block event requires block")
		}
		return e.Block.Validate()
	case ModelEventToolIntent:
		if e.ToolIntent == nil {
			return fmt.Errorf("tool-intent event requires intent")
		}
		return e.ToolIntent.Validate()
	case ModelEventUsageUpdate:
		if e.Usage == nil {
			return fmt.Errorf("usage event requires usage")
		}
		return e.Usage.Validate()
	case ModelEventTerminal:
		if e.Terminal == nil || e.Terminal.Reason == "" {
			return fmt.Errorf("terminal event requires reason")
		}
	case ModelEventError:
		if e.Error == nil || e.Error.Code == "" || e.Error.Message == "" {
			return fmt.Errorf("error event is incomplete")
		}
	default:
		return fmt.Errorf("invalid model event kind %q", e.Kind)
	}
	return nil
}

func oneContentDeltaValue(delta ContentDelta) bool {
	if delta.Kind == ContentText {
		return delta.Text != "" && delta.JSONFragment == ""
	}
	if delta.Kind == ContentJSON {
		return delta.Text == "" && delta.JSONFragment != ""
	}
	return false
}
