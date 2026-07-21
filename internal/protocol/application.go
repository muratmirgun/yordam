package protocol

import (
	"encoding/json"
	"fmt"
	"time"
)

type StreamCursor struct {
	Epoch string `json:"epoch"`
	Seq   uint64 `json:"seq"`
}

type ApplicationCursor struct {
	WorkspaceControl CommittedCursor   `json:"workspace_control"`
	SelectedSession  *CommittedCursor  `json:"selected_session,omitempty"`
	RelatedSessions  []CommittedCursor `json:"related_sessions,omitempty"`
	Stream           StreamCursor      `json:"stream"`
}

type CommandExpectation struct {
	SelectedSessionID SessionID        `json:"selected_session_id,omitempty"`
	WorkspaceControl  *CommittedCursor `json:"workspace_control,omitempty"`
	Session           *CommittedCursor `json:"session,omitempty"`
}

const EventToolResultAvailable = "runtime.tool_result_available"

type ToolResultAvailableV1 struct {
	ActivityID    ActivityID `json:"activity_id"`
	CallID        string     `json:"call_id"`
	Status        string     `json:"status"`
	Content       string     `json:"content"`
	DurationNanos int64      `json:"duration_nanos"`
	Truncated     bool       `json:"truncated"`
}

func (p ToolResultAvailableV1) Validate() error {
	if err := ValidateBounds(p); err != nil {
		return fmt.Errorf("transient tool result bounds: %w", err)
	}
	if p.ActivityID == "" || p.CallID == "" || p.DurationNanos < 0 || len(p.Content) > MaxStringBytes {
		return fmt.Errorf("invalid transient tool result")
	}
	switch p.Status {
	case "succeeded", "failed", "denied", "cancelled":
		return nil
	default:
		return fmt.Errorf("invalid transient tool result status")
	}
}

type Command struct {
	ProtocolVersion uint32              `json:"protocol_version"`
	CommandID       CommandID           `json:"command_id"`
	Actor           ActorRef            `json:"actor"`
	IdempotencyKey  string              `json:"idempotency_key"`
	RequestDigest   Digest              `json:"request_digest"`
	Expected        *CommandExpectation `json:"expected,omitempty"`
	Kind            string              `json:"kind"`
	PayloadVersion  uint32              `json:"payload_version"`
	Payload         json.RawMessage     `json:"payload"`
}

func (c Command) Validate() error {
	if err := ValidateBounds(c); err != nil {
		return fmt.Errorf("command bounds: %w", err)
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}
	if len(raw) > MaxCommandBytes {
		return fmt.Errorf("command exceeds %d bytes", MaxCommandBytes)
	}
	if c.ProtocolVersion != ApplicationProtocolVersion || c.CommandID == "" || c.IdempotencyKey == "" || c.Kind == "" || c.PayloadVersion == 0 {
		return fmt.Errorf("invalid application command")
	}
	if err := c.Actor.Validate(); err != nil {
		return err
	}
	if err := c.RequestDigest.Validate(); err != nil {
		return err
	}
	if len(c.Payload) > MaxCommandBytes {
		return fmt.Errorf("command payload exceeds %d bytes", MaxCommandBytes)
	}
	return ValidateRawJSON(c.Payload)
}

func CloneCommand(command Command) Command {
	return DeepCopy(command)
}

type PublicError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type CommandResult struct {
	ProtocolVersion uint32            `json:"protocol_version"`
	CommandID       CommandID         `json:"command_id"`
	Status          string            `json:"status"`
	RequestDigest   Digest            `json:"request_digest"`
	Cursor          ApplicationCursor `json:"cursor"`
	PayloadVersion  uint32            `json:"payload_version"`
	Payload         json.RawMessage   `json:"payload"`
	Error           *PublicError      `json:"error,omitempty"`
}

type EventCorrelation struct {
	JournalKind         JournalKind         `json:"journal_kind"`
	JournalID           JournalID           `json:"journal_id"`
	SessionID           SessionID           `json:"session_id,omitempty"`
	ControlOperationID  ControlOperationID  `json:"control_operation_id,omitempty"`
	TaskID              TaskID              `json:"task_id,omitempty"`
	TurnID              TurnID              `json:"turn_id,omitempty"`
	ActivityID          ActivityID          `json:"activity_id,omitempty"`
	ParentSessionID     SessionID           `json:"parent_session_id,omitempty"`
	DelegationAttemptID DelegationAttemptID `json:"delegation_attempt_id,omitempty"`
}

func (c EventCorrelation) Validate() error {
	if !c.JournalKind.Valid() || c.JournalID == "" {
		return fmt.Errorf("invalid event correlation journal")
	}
	switch c.JournalKind {
	case JournalSession:
		if c.SessionID == "" || JournalID(c.SessionID) != c.JournalID || c.ControlOperationID != "" {
			return fmt.Errorf("invalid session correlation")
		}
	case JournalWorkspaceControl:
		if c.SessionID != "" {
			return fmt.Errorf("invalid workspace-control correlation")
		}
	}
	if (c.ParentSessionID == "") != (c.DelegationAttemptID == "") || (c.ParentSessionID != "" && (c.JournalKind != JournalSession || c.ParentSessionID == c.SessionID)) {
		return fmt.Errorf("invalid subagent application correlation")
	}
	return nil
}

type ApplicationEvent struct {
	ProtocolVersion uint32            `json:"protocol_version"`
	StreamEventID   string            `json:"stream_event_id"`
	Cursor          ApplicationCursor `json:"cursor"`
	Correlation     EventCorrelation  `json:"correlation"`
	Time            time.Time         `json:"time"`
	Kind            string            `json:"kind"`
	Classification  string            `json:"classification"`
	JournalCursor   *CommittedCursor  `json:"journal_cursor,omitempty"`
	PayloadVersion  uint32            `json:"payload_version"`
	Payload         json.RawMessage   `json:"payload"`
	Error           *PublicError      `json:"error,omitempty"`
}

func (e ApplicationEvent) Validate() error {
	if err := ValidateBounds(e); err != nil {
		return fmt.Errorf("application event bounds: %w", err)
	}
	if e.ProtocolVersion != ApplicationProtocolVersion || e.StreamEventID == "" || e.Time.IsZero() || e.Kind == "" || e.Classification == "" || e.PayloadVersion == 0 {
		return fmt.Errorf("invalid application event")
	}
	if err := e.Correlation.Validate(); err != nil {
		return err
	}
	if isControlOperationEventKind(e.Kind) && (e.Correlation.JournalKind != JournalWorkspaceControl || e.Correlation.ControlOperationID == "") {
		return fmt.Errorf("control-operation application event requires workspace-control journal and control-operation ID")
	}
	return ValidateRawJSON(e.Payload)
}

func isControlOperationEventKind(kind string) bool {
	switch kind {
	case EventControlOperationPlanned, EventControlOperationAuthorized, EventControlOperationStarted,
		EventControlOperationCompleted, EventControlOperationFailed, EventControlOperationInterrupted:
		return true
	default:
		return false
	}
}

func CloneApplicationEvent(event ApplicationEvent) ApplicationEvent {
	return DeepCopy(event)
}

type SnapshotRequest struct {
	ProtocolVersion   uint32    `json:"protocol_version"`
	SelectedSessionID SessionID `json:"selected_session_id,omitempty"`
	Consumer          string    `json:"consumer"`
	QueueCapacity     int       `json:"queue_capacity"`
}

type SubscriptionRequest struct {
	ProtocolVersion   uint32            `json:"protocol_version"`
	SelectedSessionID SessionID         `json:"selected_session_id,omitempty"`
	After             ApplicationCursor `json:"after"`
	Consumer          string            `json:"consumer"`
	QueueCapacity     int               `json:"queue_capacity"`
}

type ProjectionView struct {
	ID     string          `json:"id"`
	Kind   string          `json:"kind"`
	Status string          `json:"status"`
	State  ValueState      `json:"state"`
	Data   json.RawMessage `json:"data"`
}

type CompactionStage string

const (
	CompactionPreparing   CompactionStage = "preparing"
	CompactionSummarizing CompactionStage = "summarizing"
	CompactionPersisting  CompactionStage = "persisting"
	CompactionCompleted   CompactionStage = "completed"
	CompactionCancelled   CompactionStage = "cancelled"
	CompactionUncertain   CompactionStage = "uncertain"
	CompactionFailed      CompactionStage = "failed"
)

type CompactionRange struct {
	From    CommittedCursor `json:"from"`
	Through CommittedCursor `json:"through"`
}

// ContextProjectionV1 is reconstructible context metadata. It intentionally
// excludes summary/provider bodies and internal errors.
type ContextProjectionV1 struct {
	AutoAvailable        bool             `json:"auto_available"`
	AutoReason           string           `json:"auto_reason"`
	EstimatedInputTokens ValueInt64       `json:"estimated_input_tokens"`
	ContextWindow        ValueInt64       `json:"context_window"`
	OutputReserve        int64            `json:"output_reserve"`
	ReserveTokens        ValueInt64       `json:"reserve_tokens"`
	Revision             string           `json:"revision,omitempty"`
	SummaryEvidenceID    EvidenceID       `json:"summary_evidence_id,omitempty"`
	LatestRange          *CompactionRange `json:"latest_range,omitempty"`
}

// CompactionEventV1 is the public compaction lifecycle payload. It contains
// only stable facts and a redacted PublicError, never provider/summary text.
type CompactionEventV1 struct {
	Trigger           string           `json:"trigger"`
	Stage             CompactionStage  `json:"stage"`
	Range             *CompactionRange `json:"range,omitempty"`
	SummaryBytes      int64            `json:"summary_bytes"`
	Usage             ModelUsage       `json:"usage"`
	Revision          string           `json:"revision,omitempty"`
	SummaryEvidenceID EvidenceID       `json:"summary_evidence_id,omitempty"`
	Error             *PublicError     `json:"error,omitempty"`
}

type DurableProjection struct {
	Workspace           ProjectionView       `json:"workspace"`
	SelectedSession     *ProjectionView      `json:"selected_session,omitempty"`
	Task                *ProjectionView      `json:"task,omitempty"`
	Outcome             *ProjectionView      `json:"outcome,omitempty"`
	Lineage             *ProjectionView      `json:"lineage,omitempty"`
	Activities          []ProjectionView     `json:"activities"`
	Provider            ProjectionView       `json:"provider"`
	MCP                 []ProjectionView     `json:"mcp"`
	Instructions        []ProjectionView     `json:"instructions"`
	Skills              SkillCatalogSnapshot `json:"skills"`
	Permissions         ProjectionView       `json:"permissions"`
	Context             ProjectionView       `json:"context"`
	Usage               ModelUsage           `json:"usage"`
	Cost                CostValue            `json:"cost"`
	Checkpoints         []ProjectionView     `json:"checkpoints"`
	Evidence            []ProjectionView     `json:"evidence"`
	Receipts            []ProjectionView     `json:"receipts"`
	Subagents           []ProjectionView     `json:"subagents"`
	RecoveryDiagnostics []Diagnostic         `json:"recovery_diagnostics"`
}

type RuntimeProjection struct {
	RuntimeGenerationID RuntimeGenerationID `json:"runtime_generation_id"`
	ActiveOperationID   string              `json:"active_operation_id,omitempty"`
	ActiveStreams       []ProjectionView    `json:"active_streams"`
	Connections         []ProjectionView    `json:"connections"`
	RevocationEpoch     uint64              `json:"revocation_epoch"`
}

type ApplicationSnapshot struct {
	ProtocolVersion uint32            `json:"protocol_version"`
	Cursor          ApplicationCursor `json:"cursor"`
	Durable         DurableProjection `json:"durable"`
	Runtime         RuntimeProjection `json:"runtime"`
}

func CloneApplicationSnapshot(snapshot ApplicationSnapshot) ApplicationSnapshot {
	return DeepCopy(snapshot)
}

type SubscriptionTerminal struct {
	Code             string            `json:"code"`
	Message          string            `json:"message"`
	ResumeAfter      ApplicationCursor `json:"resume_after"`
	RequiresSnapshot bool              `json:"requires_snapshot"`
}

type SubscriptionItem struct {
	Event    *ApplicationEvent     `json:"event,omitempty"`
	Terminal *SubscriptionTerminal `json:"terminal,omitempty"`
}

func (i SubscriptionItem) Validate() error {
	if (i.Event == nil) == (i.Terminal == nil) {
		return fmt.Errorf("subscription item requires exactly one event or terminal")
	}
	if i.Event != nil {
		return i.Event.Validate()
	}
	if i.Terminal.Code == "" || i.Terminal.Message == "" {
		return fmt.Errorf("subscription terminal is incomplete")
	}
	return nil
}

type StartTurnCommandV1 struct {
	Prompt string `json:"prompt"`
}

type StartTaskCommandV1 struct {
	Goal     string        `json:"goal"`
	Criteria []CriterionV1 `json:"criteria,omitempty"`
}

type StartControlOperationCommandV1 struct {
	ControlOperationID ControlOperationID `json:"control_operation_id"`
	Kind               string             `json:"kind"`
	Purpose            string             `json:"purpose"`
	Payload            json.RawMessage    `json:"payload"`
}

type CancelCommandV1 struct {
	TurnID             TurnID             `json:"turn_id,omitempty"`
	ControlOperationID ControlOperationID `json:"control_operation_id,omitempty"`
}

func (c CancelCommandV1) Validate() error {
	if (c.TurnID == "") == (c.ControlOperationID == "") {
		return fmt.Errorf("cancel command requires exactly one target")
	}
	return nil
}

type ResolvePermissionCommandV1 struct {
	Response ApprovalResponse `json:"response"`
}

type ChangeModeCommandV1 struct {
	Mode string `json:"mode"`
}

type ChangeModelCommandV1 struct {
	ProviderID ProviderID `json:"provider_id"`
	ModelID    ModelID    `json:"model_id"`
}

type TrustedShellCommandV1 struct {
	RequestID string `json:"request_id"`
	Enabled   bool   `json:"enabled"`
}

// SkillTrustCommandV1 records an explicit decision for the exact project
// catalog the caller inspected. It is deliberately independent of permission
// grants: trusting skill text only controls catalog activation.
type SkillTrustCommandV1 struct {
	WorkspaceID   WorkspaceID `json:"workspace_id"`
	CatalogDigest Digest      `json:"catalog_digest"`
	Decision      string      `json:"decision"`
}

func (c SkillTrustCommandV1) Validate() error {
	if c.WorkspaceID == "" || c.CatalogDigest.Validate() != nil || (c.Decision != "allow" && c.Decision != "deny") {
		return fmt.Errorf("invalid skill trust command")
	}
	return nil
}

type OpenSessionCommandV1 struct {
	SessionID SessionID `json:"session_id"`
}

type EmptyCommandV1 struct{}
