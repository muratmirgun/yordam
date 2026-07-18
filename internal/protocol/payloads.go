package protocol

import (
	"encoding/json"
	"fmt"
)

const (
	EventSessionCreated                 = "session.created"
	EventSessionForked                  = "session.forked"
	EventSessionTitleChanged            = "session.title_changed"
	EventSessionLifecycleChanged        = "session.lifecycle_changed"
	EventModeChanged                    = "mode.changed"
	EventModelChanged                   = "model.changed"
	EventTrustedExecutionAcknowledged   = "trusted_execution.acknowledged"
	EventUserMessage                    = "user.message"
	EventAssistantMessage               = "assistant.message"
	EventContextCompacted               = "context.compacted"
	EventFileChangePlanned              = "file.change_planned"
	EventFileChanged                    = "file.changed"
	EventTaskCreated                    = "task.created"
	EventTaskStatusChanged              = "task.status_changed"
	EventOutcomeContractDeclared        = "outcome.contract_declared"
	EventOutcomeContractAmended         = "outcome.contract_amended"
	EventOutcomeCriterionAssessed       = "outcome.criterion_assessed"
	EventOutcomeFinalAssessed           = "outcome.final_assessed"
	EventTurnAccepted                   = "turn.accepted"
	EventTurnStateChanged               = "turn.state_changed"
	EventTurnCompleted                  = "turn.completed"
	EventTurnFailed                     = "turn.failed"
	EventTurnInterrupted                = "turn.interrupted"
	EventActivityPlanned                = "activity.planned"
	EventActivityAuthorized             = "activity.authorized"
	EventActivityStarted                = "activity.started"
	EventActivityProgress               = "activity.progress"
	EventActivitySucceeded              = "activity.succeeded"
	EventActivityFailed                 = "activity.failed"
	EventActivityDenied                 = "activity.denied"
	EventActivityCancelled              = "activity.cancelled"
	EventActivityInterruptedNoEffect    = "activity.interrupted_no_effect"
	EventActivityUncertain              = "activity.uncertain"
	EventProviderCapabilityDecided      = "provider.capability_decided"
	EventProviderAttemptTerminal        = "provider.attempt_terminal"
	EventExecutionPlanDeclared          = "execution.plan_declared"
	EventAuthorizationRequested         = "authorization.requested"
	EventAuthorizationDecided           = "authorization.decided"
	EventAuthorizationDecisionConsumed  = "authorization.decision_consumed"
	EventAuthorizationGrantRevoked      = "authorization.grant_revoked"
	EventEvidenceRecorded               = "evidence.recorded"
	EventEvidenceLinked                 = "evidence.linked"
	EventCheckpointPlanned              = "checkpoint.planned"
	EventCheckpointReady                = "checkpoint.ready"
	EventCheckpointFailed               = "checkpoint.failed"
	EventVerificationReceiptRecorded    = "verification.receipt_recorded"
	EventContextPlanRecorded            = "context.plan_recorded"
	EventContextUsageRecorded           = "context.usage_recorded"
	EventRuntimeGenerationActivated     = "runtime_generation.activated"
	EventControlOperationPlanned        = "control_operation.planned"
	EventControlOperationAuthorized     = "control_operation.authorized"
	EventControlOperationStarted        = "control_operation.started"
	EventControlOperationCompleted      = "control_operation.completed"
	EventControlOperationFailed         = "control_operation.failed"
	EventControlOperationInterrupted    = "control_operation.interrupted"
	EventCommandAccepted                = "command.accepted"
	EventCommandCompleted               = "command.completed"
	EventMigrationCompatibilityDeclared = "migration.compatibility_declared"
	EventMigrationDiagnostic            = "migration.diagnostic"
	EventRecoveryDiagnostic             = "recovery.diagnostic"
	EventTransactionCommitted           = "transaction.committed"
)

type SessionCreatedV1 struct {
	WorkspaceID   WorkspaceID `json:"workspace_id"`
	CanonicalPath string      `json:"canonical_path"`
	Title         string      `json:"title"`
	Mode          string      `json:"mode"`
	ProviderID    ProviderID  `json:"provider_id"`
	ModelID       ModelID     `json:"model_id"`
}

type SessionForkedV1 struct {
	ParentSessionID  SessionID       `json:"parent_session_id"`
	ParentCursor     CommittedCursor `json:"parent_cursor"`
	CheckpointDigest Digest          `json:"checkpoint_digest"`
	TrustReset       bool            `json:"trust_reset"`
	Salvage          bool            `json:"salvage"`
}

type SessionTitleChangedV1 struct {
	Title string `json:"title"`
}

type StateChangedV1 struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Reason string `json:"reason,omitempty"`
}

func (s StateChangedV1) ValidateTask() error {
	from, to := TaskState(s.From), TaskState(s.To)
	if !validTaskState(from) || !validTaskState(to) || from == to {
		return fmt.Errorf("invalid task transition %q -> %q", s.From, s.To)
	}
	valid := (from == TaskPending && (to == TaskRunning || to == TaskCancelled)) ||
		(from == TaskDraft && to == TaskContractDrafting) ||
		(from == TaskContractDrafting && to == TaskContractProposed) ||
		(from == TaskContractProposed && to == TaskContractFrozen) ||
		(from == TaskContractFrozen && to == TaskRunning) ||
		(from == TaskRunning && (to == TaskCompleted || to == TaskFailed || to == TaskCancelled || to == TaskVerifying)) ||
		(from == TaskVerifying && (to == TaskVerified || to == TaskCompletedWithWaivers || to == TaskPartial || to == TaskFailed || to == TaskUnknown || to == TaskCancelled)) ||
		((from == TaskPartial || from == TaskFailed || from == TaskUnknown || from == TaskCompletedWithWaivers) && to == TaskReopened) ||
		(from == TaskReopened && to == TaskRunning)
	if !valid {
		return fmt.Errorf("invalid task transition %q -> %q", s.From, s.To)
	}
	return nil
}

func (s StateChangedV1) ValidateTurn() error {
	from, to := TurnState(s.From), TurnState(s.To)
	if !validTurnState(from) || !validTurnState(to) || from == to || terminalTurnState(from) {
		return fmt.Errorf("invalid turn transition %q -> %q", s.From, s.To)
	}
	valid := (from == TurnAccepted && (to == TurnRunning || to == TurnContractDrafting)) ||
		(from == TurnContractDrafting && to == TurnFreezingContract) ||
		(from == TurnFreezingContract && to == TurnPlanningContext) ||
		(from == TurnPlanningContext && to == TurnWaitingProvider) ||
		(from == TurnWaitingProvider && to == TurnReceivingProvider) ||
		(from == TurnReceivingProvider && to == TurnPlanningAction) ||
		(from == TurnPlanningAction && to == TurnCheckpointing) ||
		(from == TurnCheckpointing && to == TurnAwaitingPermission) ||
		(from == TurnAwaitingPermission && to == TurnExecuting) ||
		(from == TurnExecuting && to == TurnRecordingEvidence) ||
		(from == TurnRecordingEvidence && to == TurnReturningResult) ||
		(from == TurnReturningResult && to == TurnVerifying) ||
		(from == TurnVerifying && (to == TurnCompleted || to == TurnFailed || to == TurnInterrupted)) ||
		(from == TurnRunning && (to == TurnCompleted || to == TurnFailed || to == TurnInterrupted))
	if !valid {
		return fmt.Errorf("invalid turn transition %q -> %q", s.From, s.To)
	}
	return nil
}

func (s StateChangedV1) ValidateActivity() error {
	from, to := ActivityState(s.From), ActivityState(s.To)
	if !validActivityState(from) || !validActivityState(to) || from == to || terminalActivityState(from) {
		return fmt.Errorf("invalid activity transition %q -> %q", s.From, s.To)
	}
	valid := (from == ActivityPlanned && (to == ActivityAuthorized || to == ActivityDenied || to == ActivityCancelled)) ||
		(from == ActivityAuthorized && (to == ActivityStarted || to == ActivityCancelled || to == ActivityInterruptedNoEffect)) ||
		(from == ActivityStarted && terminalActivityState(to))
	if !valid {
		return fmt.Errorf("invalid activity transition %q -> %q", s.From, s.To)
	}
	return nil
}

type ModeChangedV1 struct {
	Mode string `json:"mode"`
}

type ModelChangedV1 struct {
	ProviderID ProviderID `json:"provider_id"`
	ModelID    ModelID    `json:"model_id"`
}

type TrustedExecutionAcknowledgedV1 struct {
	Enabled bool   `json:"enabled"`
	Profile string `json:"profile"`
}

type UserMessageV1 struct {
	Content string `json:"content"`
}

type AssistantMessageV1 struct {
	Blocks      []ContentBlock `json:"blocks"`
	ToolIntents []ToolUseBlock `json:"tool_intents,omitempty"`
}

type ContextCompactedV1 struct {
	From              CommittedCursor `json:"from"`
	Through           CommittedCursor `json:"through"`
	SummaryEvidenceID EvidenceID      `json:"summary_evidence_id"`
	Revision          string          `json:"revision"`
}

type FileChangePlannedV1 struct {
	Plan           ActionPlan `json:"plan"`
	DiffEvidenceID EvidenceID `json:"diff_evidence_id,omitempty"`
}

type FileChangedV1 struct {
	CallID      string       `json:"call_id"`
	Subject     SubjectRef   `json:"subject"`
	Before      Digest       `json:"before"`
	After       Digest       `json:"after"`
	EvidenceIDs []EvidenceID `json:"evidence_ids"`
}

type TaskCreatedV1 struct {
	Goal              string            `json:"goal"`
	OutcomeContractID OutcomeContractID `json:"outcome_contract_id"`
	ContractVersion   uint32            `json:"contract_version"`
}

type TaskStatusChangedV1 struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Reason string `json:"reason,omitempty"`
}

type CriterionV1 struct {
	ID                    string `json:"id"`
	Required              bool   `json:"required"`
	Description           string `json:"description"`
	VerificationMethod    string `json:"verification_method"`
	ExpectedEvidenceKind  string `json:"expected_evidence_kind"`
	HumanJudgmentRequired bool   `json:"human_judgment_required"`
}

type OutcomeContractDeclaredV1 struct {
	OutcomeContractID OutcomeContractID `json:"outcome_contract_id"`
	Version           uint32            `json:"version"`
	Goal              string            `json:"goal"`
	Source            string            `json:"source"`
	Frozen            bool              `json:"frozen"`
	Criteria          []CriterionV1     `json:"criteria"`
}

type OutcomeContractAmendedV1 struct {
	OutcomeContractID OutcomeContractID `json:"outcome_contract_id"`
	FromVersion       uint32            `json:"from_version"`
	ToVersion         uint32            `json:"to_version"`
	Reason            string            `json:"reason"`
	Actor             ActorRef          `json:"actor"`
	Criteria          []CriterionV1     `json:"criteria"`
	Frozen            bool              `json:"frozen"`
}

type CriterionAssessedV1 struct {
	OutcomeContractID OutcomeContractID `json:"outcome_contract_id"`
	ContractVersion   uint32            `json:"contract_version"`
	CriterionID       string            `json:"criterion_id"`
	Status            string            `json:"status"`
	EvidenceIDs       []EvidenceID      `json:"evidence_ids"`
	ReceiptIDs        []ReceiptID       `json:"receipt_ids"`
	Reason            string            `json:"reason"`
}

type OutcomeFinalAssessedV1 struct {
	OutcomeContractID OutcomeContractID `json:"outcome_contract_id"`
	ContractVersion   uint32            `json:"contract_version"`
	Status            string            `json:"status"`
	CriterionIDs      []string          `json:"criterion_ids"`
	ReceiptIDs        []ReceiptID       `json:"receipt_ids"`
	UnknownEffects    []SubjectRef      `json:"unknown_effects"`
}

type TurnAcceptedV1 struct {
	CommandID         CommandID         `json:"command_id"`
	Goal              string            `json:"goal"`
	OutcomeContractID OutcomeContractID `json:"outcome_contract_id"`
	ContractVersion   uint32            `json:"contract_version"`
}

type TurnStateChangedV1 = StateChangedV1

type TurnTerminalV1 struct {
	Status         string       `json:"status"`
	Reason         string       `json:"reason"`
	ErrorCode      string       `json:"error_code,omitempty"`
	UnknownEffects []SubjectRef `json:"unknown_effects,omitempty"`
}

type ActivityPlannedV1 struct {
	Kind             string       `json:"kind"`
	Purpose          string       `json:"purpose"`
	PurposeActor     ActorRef     `json:"purpose_actor"`
	Source           string       `json:"source"`
	Plan             *ActionPlan  `json:"plan,omitempty"`
	InputEvidenceIDs []EvidenceID `json:"input_evidence_ids,omitempty"`
	RequestedProfile string       `json:"requested_profile"`
	EffectiveProfile string       `json:"effective_profile"`
}

type ActivityAuthorizedV1 struct {
	DecisionNonce   DecisionNonce `json:"decision_nonce"`
	DecisionEventID EventID       `json:"decision_event_id"`
	PlanDigest      Digest        `json:"plan_digest"`
	RequestDigest   Digest        `json:"request_digest"`
	DispatchDigest  Digest        `json:"dispatch_digest"`
}

type ActivityStartedV1 struct {
	DecisionNonce       DecisionNonce       `json:"decision_nonce"`
	DecisionEventID     EventID             `json:"decision_event_id"`
	ActivityID          ActivityID          `json:"activity_id"`
	CallID              string              `json:"call_id"`
	PlanDigest          Digest              `json:"plan_digest"`
	RequestDigest       Digest              `json:"request_digest"`
	DispatchDigest      Digest              `json:"dispatch_digest"`
	RuntimeGenerationID RuntimeGenerationID `json:"runtime_generation_id"`
	DispatchState       string              `json:"dispatch_state"`
}

type ActivityProgressV1 struct {
	Message   string `json:"message"`
	Completed int64  `json:"completed,omitempty"`
	Total     int64  `json:"total,omitempty"`
	Truncated bool   `json:"truncated"`
}

type ActivityOutcomeV1 struct {
	Status            string       `json:"status"`
	Reason            string       `json:"reason,omitempty"`
	ErrorCode         string       `json:"error_code,omitempty"`
	OutputEvidenceIDs []EvidenceID `json:"output_evidence_ids,omitempty"`
	UnknownEffects    []SubjectRef `json:"unknown_effects,omitempty"`
}

type ProviderCapabilityDecidedV1 struct {
	Plan    NegotiatedProviderPlan `json:"plan"`
	Status  string                 `json:"status"`
	Missing []string               `json:"missing,omitempty"`
}

type ProviderAttemptTerminalV1 struct {
	Status          string     `json:"status"`
	TerminalReason  string     `json:"terminal_reason"`
	NativeReason    string     `json:"native_reason,omitempty"`
	ServerRequestID string     `json:"server_request_id,omitempty"`
	Usage           ModelUsage `json:"usage"`
	ErrorCode       string     `json:"error_code,omitempty"`
}

type ExecutionPlanDeclaredV1 struct {
	Plan ActionPlan `json:"plan"`
}

type AuthorizationRequestedV1 struct {
	Request AuthorizationRequest `json:"request"`
}

type AuthorizationDecidedV1 struct {
	Decision AuthorizationDecision `json:"decision"`
}

type AuthorizationDecisionConsumedV1 struct {
	DecisionNonce       DecisionNonce       `json:"decision_nonce"`
	DecisionEventID     EventID             `json:"decision_event_id"`
	DecisionDigest      Digest              `json:"decision_digest"`
	RequestID           string              `json:"request_id"`
	ActivityID          ActivityID          `json:"activity_id,omitempty"`
	ControlOperationID  ControlOperationID  `json:"control_operation_id,omitempty"`
	CallID              string              `json:"call_id"`
	PlanDigest          Digest              `json:"plan_digest"`
	RequestDigest       Digest              `json:"request_digest"`
	DispatchDigest      Digest              `json:"dispatch_digest"`
	RuntimeGenerationID RuntimeGenerationID `json:"runtime_generation_id"`
}

func (p AuthorizationDecisionConsumedV1) Validate() error {
	if (p.ActivityID == "") == (p.ControlOperationID == "") {
		return fmt.Errorf("authorization consumption requires exactly one activity or control operation")
	}
	if p.DecisionNonce == "" || p.DecisionEventID == "" || p.RequestID == "" || p.CallID == "" || p.RuntimeGenerationID == "" {
		return fmt.Errorf("authorization consumption is incomplete")
	}
	for _, digest := range []Digest{p.DecisionDigest, p.PlanDigest, p.RequestDigest, p.DispatchDigest} {
		if err := digest.Validate(); err != nil {
			return err
		}
	}
	return nil
}

type AuthorizationGrantRevokedV1 struct {
	GrantID         string `json:"grant_id"`
	Reason          string `json:"reason"`
	RevocationEpoch uint64 `json:"revocation_epoch"`
}

type EvidenceRecordedV1 struct {
	Record EvidenceRecord `json:"record"`
}

type EvidenceLinkedV1 struct {
	EvidenceID EvidenceID `json:"evidence_id"`
	Subject    SubjectRef `json:"subject"`
	Relation   string     `json:"relation"`
}

type CheckpointPlannedV1 struct {
	Body       CheckpointBody `json:"body"`
	PlanDigest Digest         `json:"plan_digest"`
}

type CheckpointReadyV1 struct {
	Body                CheckpointBody       `json:"body"`
	Digest              Digest               `json:"digest"`
	RecoveryMaterialIDs []RecoveryMaterialID `json:"recovery_material_ids"`
}

type CheckpointFailedV1 struct {
	PlanDigest Digest `json:"plan_digest"`
	ErrorCode  string `json:"error_code"`
	Reason     string `json:"reason"`
}

type VerificationReceiptRecordedV1 struct {
	Receipt VerificationReceipt `json:"receipt"`
}

type ContextPlanRecordedV1 struct {
	Plan ContextPlan `json:"plan"`
}

type ContextUsageRecordedV1 struct {
	Usage ModelUsage `json:"usage"`
	Cost  CostValue  `json:"cost"`
}

type RuntimeGenerationActivatedV1 struct {
	Manifest RuntimeGenerationManifest `json:"manifest"`
	Previous RuntimeGenerationID       `json:"previous,omitempty"`
}

type ControlOperationPlannedV1 struct {
	ControlOperationID ControlOperationID `json:"control_operation_id"`
	Kind               string             `json:"kind"`
	Purpose            string             `json:"purpose"`
	Plan               ActionPlan         `json:"plan"`
}

type ControlOperationAuthorizedV1 struct {
	ControlOperationID ControlOperationID `json:"control_operation_id"`
	DecisionEventID    EventID            `json:"decision_event_id"`
	DecisionNonce      DecisionNonce      `json:"decision_nonce"`
	PlanDigest         Digest             `json:"plan_digest"`
	RequestDigest      Digest             `json:"request_digest"`
	DispatchDigest     Digest             `json:"dispatch_digest"`
}

type ControlOperationStartedV1 struct {
	ControlOperationID  ControlOperationID  `json:"control_operation_id"`
	DecisionEventID     EventID             `json:"decision_event_id"`
	DecisionNonce       DecisionNonce       `json:"decision_nonce"`
	PlanDigest          Digest              `json:"plan_digest"`
	RequestDigest       Digest              `json:"request_digest"`
	DispatchDigest      Digest              `json:"dispatch_digest"`
	RuntimeGenerationID RuntimeGenerationID `json:"runtime_generation_id"`
}

type ControlOperationTerminalV1 struct {
	ControlOperationID ControlOperationID `json:"control_operation_id"`
	Status             string             `json:"status"`
	Reason             string             `json:"reason,omitempty"`
	ErrorCode          string             `json:"error_code,omitempty"`
}

type CommandAcceptedV1 struct {
	CommandID      CommandID `json:"command_id"`
	RequestDigest  Digest    `json:"request_digest"`
	IdempotencyKey string    `json:"idempotency_key"`
}

type CommandCompletedV1 struct {
	CommandID     CommandID       `json:"command_id"`
	RequestDigest Digest          `json:"request_digest"`
	Status        string          `json:"status"`
	Result        json.RawMessage `json:"result"`
	Error         *PublicError    `json:"error,omitempty"`
}

type MigrationCompatibilityDeclaredV1 struct {
	ReaderVersion   uint32          `json:"reader_version"`
	WriterVersion   uint32          `json:"writer_version"`
	LegacyHead      CommittedCursor `json:"legacy_head"`
	DowngradeStatus string          `json:"downgrade_status"`
}

type DiagnosticV1 struct {
	Diagnostic Diagnostic `json:"diagnostic"`
}
