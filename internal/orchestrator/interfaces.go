package orchestrator

import (
	"context"
	"encoding/json"

	"github.com/muratmirgun/yordam/internal/authorization"
	contextplanner "github.com/muratmirgun/yordam/internal/context"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/provider"
	"github.com/muratmirgun/yordam/internal/recovery"
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
	SessionID    protocol.SessionID
	ExpectedHead protocol.CommittedCursor
	Prompt       string
	Runtime      protocol.RuntimeGenerationManifest
}

type RunResult struct {
	TaskID        protocol.TaskID
	TurnID        protocol.TurnID
	Cursor        protocol.CommittedCursor
	Status        string
	CommandResult protocol.CommandResult
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
	Command       CommandMetadata
	OperationID   protocol.ControlOperationID
	Kind          OperationKind
	Journal       protocol.JournalRef
	ExpectedHead  protocol.CommittedCursor
	TransactionID protocol.TransactionID
	Runtime       protocol.RuntimeGenerationManifest
	Plan          protocol.ActionPlan
	Event         protocol.ProposedEvent
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

type ToolService interface {
	Plan(context.Context, tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error)
	PlanPreviewInspection(context.Context, tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error)
	PreparePreview(context.Context, tooling.ActionHandle, authorization.CommittedToken) (tooling.PreviewResult, protocol.ActionPlan, []protocol.EvidenceCandidate, error)
	PlanMutation(context.Context, tooling.PreviewResult, tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error)
	Revalidate(context.Context, tooling.ActionHandle) (protocol.ActionPlan, bool, error)
	Execute(context.Context, tooling.ActionHandle, authorization.CommittedToken) (protocol.ExecutionResult, error)
	RecoveryCandidate(context.Context, tooling.PreviewResult, protocol.CheckpointBody, protocol.ActionPlan) (recovery.Candidate, error)
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
	Put(context.Context, recovery.Candidate) (protocol.RecoveryMaterialRecord, error)
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
	UnmatchedNoEffect     map[protocol.ActivityID]bool
}

type ProjectionService interface {
	InspectRecovery(context.Context, protocol.JournalRef, protocol.CommittedCursor) (RecoveryProjection, error)
}

type EffectStartProbe interface {
	ProvesNoEffect(context.Context, protocol.ActivityID) (bool, error)
}

type Dependencies struct {
	Lane          OperationLane
	Repository    journal.Repository
	TurnLeases    journal.TurnLeaseManager
	Context       ContextPlanner
	Providers     ProviderCatalog
	Provider      ProviderService
	Tools         ToolService
	Authorization AuthorizationService
	Approver      InteractiveApprover
	Evidence      EvidenceRecorder
	Recovery      RecoveryRecorder
	Verification  VerificationService
	Projection    ProjectionService
	EffectProbe   EffectStartProbe
	Publisher     ApplicationEventPublisher
	BarrierProbe  BarrierProbe
}
