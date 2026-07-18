package orchestrator

import (
	"context"

	"github.com/muratmirgun/yordam/internal/protocol"
)

type Barrier string

const (
	BarrierCommandAccepted                Barrier = "command_accepted"
	BarrierGoalDraftCommitted             Barrier = "goal_draft_committed"
	BarrierContractFrozen                 Barrier = "contract_frozen"
	BarrierProviderAuthorizationCommitted Barrier = "provider_authorization_committed"
	BarrierActionPlanCommitted            Barrier = "action_plan_committed"
	BarrierCheckpointReady                Barrier = "checkpoint_ready"
	BarrierResourcesRevalidated           Barrier = "resources_revalidated"
	BarrierAuthorizationCommitted         Barrier = "authorization_committed"
	BarrierActivityStartedCommitted       Barrier = "activity_started_committed"
	BarrierEffectDispatch                 Barrier = "effect_dispatch"
	BarrierActionTerminalCommitted        Barrier = "action_terminal_committed"
	BarrierProviderContinuation           Barrier = "provider_continuation"
	BarrierVerificationCommitted          Barrier = "verification_committed"
	BarrierTurnTerminalCommitted          Barrier = "turn_terminal_committed"
	BarrierRecoveryTurnTerminalCommitted  Barrier = "recovery_turn_terminal_committed"
	BarrierCommandCompleted               Barrier = "command_completed"
)

type BarrierState struct {
	Journal            protocol.JournalRef
	Cursor             protocol.CommittedCursor
	CommandID          protocol.CommandID
	ControlOperationID protocol.ControlOperationID
	TaskID             protocol.TaskID
	TurnID             protocol.TurnID
	ActivityID         protocol.ActivityID
	PlanDigest         protocol.Digest
}

type BarrierProbe interface {
	Before(context.Context, Barrier, BarrierState) error
	After(context.Context, Barrier, BarrierState) error
}

type noopBarrierProbe struct{}

func NoopBarrierProbe() BarrierProbe { return noopBarrierProbe{} }

func (noopBarrierProbe) Before(context.Context, Barrier, BarrierState) error { return nil }
func (noopBarrierProbe) After(context.Context, Barrier, BarrierState) error  { return nil }
