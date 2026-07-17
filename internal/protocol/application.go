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
	WorkspaceControl CommittedCursor  `json:"workspace_control"`
	SelectedSession  *CommittedCursor `json:"selected_session,omitempty"`
	Stream           StreamCursor     `json:"stream"`
}

type CommandExpectation struct {
	SelectedSessionID SessionID        `json:"selected_session_id,omitempty"`
	WorkspaceControl  *CommittedCursor `json:"workspace_control,omitempty"`
	Session           *CommittedCursor `json:"session,omitempty"`
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
	JournalKind        JournalKind        `json:"journal_kind"`
	JournalID          JournalID          `json:"journal_id"`
	SessionID          SessionID          `json:"session_id,omitempty"`
	ControlOperationID ControlOperationID `json:"control_operation_id,omitempty"`
	TaskID             TaskID             `json:"task_id,omitempty"`
	TurnID             TurnID             `json:"turn_id,omitempty"`
	ActivityID         ActivityID         `json:"activity_id,omitempty"`
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
		if c.SessionID != "" || c.TaskID != "" || c.TurnID != "" || c.ActivityID != "" {
			return fmt.Errorf("invalid workspace-control correlation")
		}
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
	return ValidateRawJSON(e.Payload)
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

type DurableProjection struct {
	Workspace           ProjectionView   `json:"workspace"`
	SelectedSession     *ProjectionView  `json:"selected_session,omitempty"`
	Task                *ProjectionView  `json:"task,omitempty"`
	Outcome             *ProjectionView  `json:"outcome,omitempty"`
	Lineage             *ProjectionView  `json:"lineage,omitempty"`
	Activities          []ProjectionView `json:"activities"`
	Provider            ProjectionView   `json:"provider"`
	MCP                 []ProjectionView `json:"mcp"`
	Instructions        []ProjectionView `json:"instructions"`
	Permissions         ProjectionView   `json:"permissions"`
	Context             ProjectionView   `json:"context"`
	Usage               ModelUsage       `json:"usage"`
	Cost                CostValue        `json:"cost"`
	Checkpoints         []ProjectionView `json:"checkpoints"`
	Evidence            []ProjectionView `json:"evidence"`
	Receipts            []ProjectionView `json:"receipts"`
	RecoveryDiagnostics []Diagnostic     `json:"recovery_diagnostics"`
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

type OpenSessionCommandV1 struct {
	SessionID SessionID `json:"session_id"`
}

type EmptyCommandV1 struct{}
