package orchestrator

import (
	"context"
	"encoding/json"

	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/compaction"
	contextplanner "github.com/muratmirgun/yordam/internal/context"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/provider"
	"github.com/muratmirgun/yordam/internal/tooling"
	"github.com/muratmirgun/yordam/internal/verification"
)

type CommandMetadata struct {
	CommandID      protocol.CommandID
	IdempotencyKey string
	RequestDigest  protocol.Digest
	Actor          protocol.ActorRef
}

type StartTurnRequest struct {
	Command      CommandMetadata
	WorkspaceID  protocol.WorkspaceID
	SessionID    protocol.SessionID
	ExpectedHead protocol.CommittedCursor
	Prompt       string
	ProviderID   protocol.ProviderID
	ModelID      protocol.ModelID
	Runtime      protocol.RuntimeGenerationManifest

	child *childTurnConfig
}

type CompactRequest struct {
	Command      CommandMetadata
	WorkspaceID  protocol.WorkspaceID
	SessionID    protocol.SessionID
	ExpectedHead protocol.CommittedCursor
	ProviderID   protocol.ProviderID
	ModelID      protocol.ModelID
	Runtime      protocol.RuntimeGenerationManifest
	Trigger      compaction.Trigger
}

type CompactResult struct {
	Cursor          protocol.CommittedCursor
	SummaryEvidence protocol.EvidenceRecord
	From            protocol.CommittedCursor
	Through         protocol.CommittedCursor
	Revision        string
	Usage           protocol.ModelUsage
}

type RunResult struct {
	TaskID        protocol.TaskID
	TurnID        protocol.TurnID
	Cursor        protocol.CommittedCursor
	Status        string
	CommandResult protocol.CommandResult
	Assistant     protocol.AssistantMessageV1
}

type SessionChangeRequest struct {
	Command             CommandMetadata
	OperationID         protocol.ControlOperationID
	Journal             protocol.JournalRef
	SessionID           protocol.SessionID
	ExpectedHead        protocol.CommittedCursor
	TransactionID       protocol.TransactionID
	RuntimeGenerationID protocol.RuntimeGenerationID
	Event               protocol.ProposedEvent
	Consequential       bool
}

type ControlRequest struct {
	Command                   CommandMetadata
	OperationID               protocol.ControlOperationID
	Kind                      OperationKind
	Journal                   protocol.JournalRef
	ExpectedHead              protocol.CommittedCursor
	TransactionID             protocol.TransactionID
	ConsequentialJournal      protocol.JournalRef
	ConsequentialExpectedHead protocol.CommittedCursor
	Runtime                   protocol.RuntimeGenerationManifest
	Plan                      protocol.ActionPlan
	Event                     protocol.ProposedEvent
}

type RecoveryControlRequest struct {
	Control ControlRequest
	Storage journal.RecoveryRequest
}

type ControlResult struct {
	OperationID   protocol.ControlOperationID
	Cursor        protocol.CommittedCursor
	Status        string
	Error         *protocol.PublicError
	CommandResult protocol.CommandResult
}

type RecoveryControlResult struct {
	Recovery      journal.RecoveryResult
	CommandResult protocol.CommandResult
}

type PureCommandCompletion struct {
	Journal        protocol.JournalRef
	ExpectedHead   protocol.CommittedCursor
	PayloadVersion uint32
	Payload        json.RawMessage
	Error          *protocol.PublicError
}

type ApplicationEventPublisher interface {
	PublishCommitted(context.Context, protocol.JournalRef, protocol.CommittedCursor, []protocol.EventEnvelope) error
}

type TransientApplicationEventPublisher interface {
	PublishTransient(protocol.ApplicationEvent) error
}

type StreamingSanitizer interface {
	Write(string) (string, error)
	Close() (string, error)
}

// AdmissionService pins sanitization to the request's runtime generation.
// It is applied before any model-visible or durable output leaves the
// orchestrator, including provider streams split across chunk boundaries.
type AdmissionService interface {
	SanitizeText(context.Context, protocol.RuntimeGenerationID, string) (string, error)
	SanitizeJSON(context.Context, protocol.RuntimeGenerationID, json.RawMessage) (json.RawMessage, error)
	OpenTextStream(context.Context, protocol.RuntimeGenerationID) (StreamingSanitizer, error)
}

type InstructionService interface {
	SystemInstructions(context.Context, protocol.RuntimeGenerationID, string, protocol.SessionID) ([]protocol.ContentSource, error)
}

type ContextPlanner interface {
	Plan(context.Context, contextplanner.Request) (protocol.ContextPlan, error)
}

type ProviderCatalog interface {
	Resolve(protocol.ProviderID, protocol.ModelID) (protocol.ModelDescriptor, bool)
	Negotiate(protocol.ProviderID, protocol.ModelID, []protocol.CapabilityRequirement, string) (protocol.NegotiatedProviderPlan, error)
}

type ProviderService interface {
	Prepare(context.Context, protocol.ActivityID, string, protocol.ModelRequest, protocol.Digest) (provider.ProviderHandle, error)
	Stream(context.Context, provider.ProviderHandle, authorization.CommittedToken) (<-chan protocol.ModelEvent, error)
}

// ProvenZeroByteProviderError is the only provider failure eligible for an
// automatic retry. Both predicates must be true; every other start/stream
// failure is treated as ambiguous and terminalized.
type ProvenZeroByteProviderError interface {
	error
	Retryable() bool
	ZeroBytesSent() bool
}

type ToolService interface {
	Plan(context.Context, tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error)
	PlanPreviewInspection(context.Context, tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error)
	PreparePreview(context.Context, tooling.ActionHandle, authorization.CommittedToken) (tooling.PreviewResult, protocol.ActionPlan, []protocol.EvidenceCandidate, error)
	PlanMutation(context.Context, tooling.PreviewResult, tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error)
	Revalidate(context.Context, tooling.ActionHandle) (protocol.ActionPlan, bool, error)
	Execute(context.Context, tooling.ActionHandle, authorization.CommittedToken) (protocol.ExecutionResult, error)
}

type AuthorizationService interface {
	Decide(context.Context, protocol.AuthorizationRequest) (protocol.AuthorizationDecision, error)
	ResolveInteractive(context.Context, protocol.AuthorizationRequest, protocol.AuthorizationDecision, protocol.ApprovalResponse) (protocol.AuthorizationDecision, error)
	Issue(context.Context, authorization.CommitReference) (authorization.CommittedToken, error)
	Dispatch(context.Context, authorization.CommittedToken, authorization.DispatchBinding, func(context.Context) error) error
}

type InteractiveApprover interface {
	Approve(context.Context, protocol.AuthorizationDecision) (protocol.ApprovalResponse, error)
}

type EvidenceRecorder interface {
	Put(context.Context, protocol.EvidenceCandidate) (protocol.EvidenceRecord, error)
}

type RecoveryRecorder interface {
	PrepareAndPut(context.Context, tooling.PreviewResult, protocol.ActivityID, protocol.CheckpointBody, protocol.ActionPlan) (protocol.RecoveryMaterialRecord, error)
}

type VerificationService interface {
	Assess(context.Context, verification.Request) (verification.Result, error)
}

type RecoveryProjection struct {
	ActiveTurnID          protocol.TurnID
	TaskID                protocol.TaskID
	OriginalCommandID     protocol.CommandID
	OriginalRequestDigest protocol.Digest
	StartedActivities     []protocol.ActivityID
	// ProviderVisibleToolCalls contains only unresolved provider-visible tool
	// activities. Provider activities and mutation previews are excluded.
	ProviderVisibleToolCalls map[protocol.ActivityID]string
	UnmatchedNoEffect        map[protocol.ActivityID]bool
	ChildManifest            *protocol.SubagentManifestV1
	// SubagentRecoveryDiagnostic is populated only when reconciliation cannot
	// prove a waiting parent/child boundary.  It is recorded with the parent
	// terminal transaction rather than silently degrading to a generic recovery
	// interruption.
	SubagentRecoveryDiagnostic *SubagentRecoveryDiagnostic
}

// SubagentRecoveryDiagnostic is the public, durable shape for a failed-closed
// sequential-child reconciliation.  The receipt digest/cursor are optional:
// an ambiguous child may not have reached a terminal receipt.
type SubagentRecoveryDiagnostic struct {
	AttemptID      protocol.DelegationAttemptID `json:"attempt_id"`
	ChildSessionID protocol.SessionID           `json:"child_session_id"`
	ChildStatus    string                       `json:"child_status"`
	TerminalCursor protocol.CommittedCursor     `json:"terminal_cursor,omitempty"`
	ReceiptDigest  protocol.Digest              `json:"receipt_digest,omitempty"`
	Reason         string                       `json:"reason"`
}

type ProjectionService interface {
	InspectRecovery(context.Context, protocol.JournalRef, protocol.CommittedCursor) (RecoveryProjection, error)
}

type EffectStartProbe interface {
	ProvesNoEffect(context.Context, protocol.ActivityID) (bool, error)
}

// ChildSessionStore reserves the identity which is bound into the durable
// parent request before any child session can be published.
type ChildSessionStore interface {
	ReserveSessionID() (protocol.SessionID, error)
	CreateWithIdentity(context.Context, protocol.SessionID, domain.Workspace, domain.PermissionMode, domain.ModelSelection, *journal.SessionLineage) (domain.Session, error)
	InspectSession(context.Context, protocol.SessionID) (journal.Inspection, error)
}

// ParentSessionInspector supplies the immutable session identity inherited by
// a child. It is deliberately separate from ChildSessionStore because the
// latter exposes only journal inspection to the parent handoff path.
type ParentSessionInspector interface {
	InspectSession(context.Context, protocol.SessionID) (journal.SessionInspection, error)
}

// ChildCoordinator owns the child-side session creation and turn execution.
// It receives the exact frozen parent request and the pre-reserved identity;
// implementations must never mint or substitute a child identity.
type ChildCoordinator interface {
	RunChild(context.Context, ChildRunRequest) (protocol.SubagentReceiptV1, error)
}

// ChildPolicyRegistry creates an isolated policy entry before the child turn
// can issue any authorization request. Implementations must not clone grants.
type ChildPolicyRegistry interface {
	RegisterChild(protocol.SessionID, domain.PermissionMode) error
}

type ChildPromptRegistry interface {
	RegisterChildLineage(protocol.SessionID, protocol.SessionID, protocol.DelegationAttemptID) error
}

type ChildRunRequest struct {
	Manifest protocol.SubagentManifestV1
	Call     protocol.SubagentCallV1
	Parent   StartTurnRequest
}

type Dependencies struct {
	Lane           OperationLane
	Repository     journal.Repository
	TurnLeases     journal.TurnLeaseManager
	Context        ContextPlanner
	Providers      ProviderCatalog
	Provider       ProviderService
	Tools          ToolService
	Authorization  AuthorizationService
	Approver       InteractiveApprover
	Evidence       EvidenceRecorder
	Recovery       RecoveryRecorder
	Verification   VerificationService
	Projection     ProjectionService
	EffectProbe    EffectStartProbe
	Publisher      ApplicationEventPublisher
	BarrierProbe   BarrierProbe
	Admission      AdmissionService
	Instructions   InstructionService
	ChildSessions  ChildSessionStore
	ParentSessions ParentSessionInspector
	Children       ChildCoordinator
}
