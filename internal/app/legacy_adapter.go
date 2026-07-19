package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
)

const (
	ApplicationEventState               = "legacy.state"
	ApplicationEventTextDelta           = "legacy.text_delta"
	ApplicationEventPermissionRequested = "legacy.permission_requested"
	ApplicationEventToolStarted         = "legacy.tool_started"
	ApplicationEventToolOutput          = "legacy.tool_output"
	ApplicationEventToolCompleted       = "legacy.tool_completed"
	ApplicationEventTurnAccepted        = "legacy.turn_accepted"
	ApplicationEventTurnCompleted       = "legacy.turn_completed"
	ApplicationEventTurnInterrupted     = "legacy.turn_interrupted"
	ApplicationEventReloadCompleted     = "legacy.reload_completed"
	ApplicationEventNotice              = "legacy.notice"
	ApplicationEventError               = "legacy.error"
	ApplicationEventRejected            = "legacy.rejected"
)

type LegacyAdapterOptions struct {
	Actor               protocol.ActorRef
	SelectedSessionID   protocol.SessionID
	Cursor              func() *protocol.CommandExpectation
	RuntimeGenerationID protocol.RuntimeGenerationID
	Now                 func() time.Time
}

type LegacyAdapter struct {
	actor               protocol.ActorRef
	selectedSessionID   protocol.SessionID
	cursor              func() *protocol.CommandExpectation
	runtimeGenerationID protocol.RuntimeGenerationID
	now                 func() time.Time
	sequence            atomic.Uint64
	pendingMu           sync.Mutex
	pending             map[protocol.CommandID]Command
	turns               map[protocol.TurnID]Command
	compactions         map[protocol.ActivityID]string
	manualCompactQueued bool
}

func NewLegacyAdapter(options LegacyAdapterOptions) *LegacyAdapter {
	if options.Actor.ID == "" {
		options.Actor = protocol.ActorRef{ID: "legacy-user", Kind: protocol.ActorUser}
	}
	if options.Now == nil {
		options.Now = func() time.Time { return time.Now().UTC() }
	}
	return &LegacyAdapter{
		actor: options.Actor, selectedSessionID: options.SelectedSessionID, cursor: options.Cursor, runtimeGenerationID: options.RuntimeGenerationID, now: options.Now,
		pending: make(map[protocol.CommandID]Command), turns: make(map[protocol.TurnID]Command), compactions: make(map[protocol.ActivityID]string),
	}
}

func (a *LegacyAdapter) Command(command Command) (protocol.Command, error) {
	if a == nil || a.actor.Validate() != nil {
		return protocol.Command{}, fmt.Errorf("legacy adapter actor is invalid")
	}
	kind, payload, err := a.commandPayload(command)
	if err != nil {
		return protocol.Command{}, err
	}
	raw, err := canonicaljson.Marshal(payload)
	if err != nil {
		return protocol.Command{}, err
	}
	sequence := a.sequence.Add(1)
	commandID := protocol.CommandID(fmt.Sprintf("legacy-command-%d", sequence))
	idempotencyKey := fmt.Sprintf("legacy-%d", sequence)
	var expectation *protocol.CommandExpectation
	if a.cursor != nil {
		expectation = protocol.DeepCopy(a.cursor())
	} else if a.selectedSessionID != "" {
		expectation = &protocol.CommandExpectation{SelectedSessionID: a.selectedSessionID}
	}
	if command.Kind == CommandCompact {
		if expectation == nil || expectation.SelectedSessionID == "" || expectation.Session == nil {
			return protocol.Command{}, fmt.Errorf("compact command requires a selected session cursor")
		}
		identity, digestErr := canonicaljson.Digest(struct {
			SessionID  protocol.SessionID           `json:"session_id"`
			Cursor     protocol.CommittedCursor     `json:"cursor"`
			Trigger    string                       `json:"trigger"`
			Generation protocol.RuntimeGenerationID `json:"generation"`
		}{expectation.SelectedSessionID, *expectation.Session, "manual", a.runtimeGenerationID})
		if digestErr != nil {
			return protocol.Command{}, digestErr
		}
		commandID = protocol.CommandID("legacy-compact-" + identity.Value)
		idempotencyKey = string(commandID)
	}
	applicationCommand := protocol.Command{
		ProtocolVersion: protocol.ApplicationProtocolVersion,
		CommandID:       commandID,
		Actor:           a.actor,
		IdempotencyKey:  idempotencyKey,
		Kind:            kind,
		PayloadVersion:  1,
		Payload:         raw,
	}
	applicationCommand.Expected = expectation
	applicationCommand.RequestDigest, err = CanonicalRequestDigest(applicationCommand)
	if err != nil {
		return protocol.Command{}, err
	}
	if command.Kind == CommandStartTurn {
		a.pendingMu.Lock()
		a.pending[applicationCommand.CommandID] = command
		a.pendingMu.Unlock()
	}
	if command.Kind == CommandCompact {
		a.pendingMu.Lock()
		a.manualCompactQueued = true
		a.pendingMu.Unlock()
	}
	return applicationCommand, nil
}

func (a *LegacyAdapter) commandPayload(command Command) (string, any, error) {
	switch command.Kind {
	case CommandStartTurn:
		return string(CommandStartTurn), protocol.StartTurnCommandV1{Prompt: command.Prompt}, nil
	case CommandCancelTurn:
		return CommandKindCancel, protocol.CancelCommandV1{TurnID: "active-turn"}, nil
	case CommandResolvePermission:
		scopeDigest, err := canonicaljson.Digest(command.Decision.Scope)
		if err != nil {
			return "", nil, err
		}
		return string(CommandResolvePermission), protocol.ResolvePermissionCommandV1{Response: protocol.ApprovalResponse{
			RequestID: command.CallID, Action: string(command.Decision.Action), Lifetime: string(command.Decision.Lifetime),
			ScopeDigest: scopeDigest, Actor: a.actor, Reason: command.Decision.Reason,
		}}, nil
	case CommandChangeMode:
		return string(CommandChangeMode), protocol.ChangeModeCommandV1{Mode: string(command.Mode)}, nil
	case CommandChangeModel:
		return string(CommandChangeModel), protocol.ChangeModelCommandV1{ProviderID: protocol.ProviderID(command.Selection.Profile), ModelID: protocol.ModelID(command.Selection.Model)}, nil
	case CommandAcknowledgeAutoShell:
		return string(CommandAcknowledgeAutoShell), protocol.TrustedShellCommandV1{RequestID: "legacy-auto-shell", Enabled: true}, nil
	case CommandCompact:
		return string(CommandCompact), protocol.EmptyCommandV1{}, nil
	case CommandReloadConfig:
		return string(CommandReloadConfig), protocol.EmptyCommandV1{}, nil
	case CommandNewSession:
		return string(CommandNewSession), protocol.EmptyCommandV1{}, nil
	case CommandOpenSession:
		return string(CommandOpenSession), protocol.OpenSessionCommandV1{SessionID: protocol.SessionID(command.SessionID)}, nil
	case CommandShutdown:
		return string(CommandShutdown), protocol.EmptyCommandV1{}, nil
	default:
		return "", nil, fmt.Errorf("unsupported legacy command %q", command.Kind)
	}
}

type legacyApplicationPayload struct {
	Message   string                `json:"message,omitempty"`
	DraftID   uint64                `json:"draft_id,omitempty"`
	Draft     string                `json:"draft,omitempty"`
	State     string                `json:"state,omitempty"`
	Mode      domain.PermissionMode `json:"mode,omitempty"`
	Selection domain.ModelSelection `json:"selection,omitempty"`
	Applied   bool                  `json:"applied,omitempty"`
}

func (a *LegacyAdapter) Event(event protocol.ApplicationEvent) (Event, error) {
	if event.ProtocolVersion != protocol.ApplicationProtocolVersion {
		return Event{}, requestError(codeInvalidProtocolVersion, "unsupported application protocol version", nil)
	}
	if err := event.Validate(); err != nil {
		return Event{}, requestError(codeInvalidPayload, "invalid application event", err)
	}
	var payload legacyApplicationPayload
	if err := strictUnmarshal(event.Payload, &payload); err != nil {
		// Durable Foundation payloads are decoded below by kind; they do not use
		// the private transient legacy shape.
		if event.Classification != "durable" {
			return Event{}, requestError(codeInvalidPayload, "invalid legacy event payload", err)
		}
	}
	legacy := Event{DraftID: payload.DraftID, Draft: payload.Draft, Message: payload.Message, Mode: payload.Mode, Selection: payload.Selection, Applied: payload.Applied}
	switch event.Kind {
	case ApplicationEventState:
		legacy.Kind = EventState
		legacy.Runtime.State = payload.State
	case ApplicationEventTextDelta:
		legacy.Kind = EventTextDelta
		legacy.Runtime.Text = payload.Message
	case ApplicationEventPermissionRequested:
		legacy.Kind = EventPermissionRequested
	case ApplicationEventToolStarted:
		legacy.Kind = EventToolStarted
	case ApplicationEventToolOutput:
		legacy.Kind = EventToolOutput
	case ApplicationEventToolCompleted:
		legacy.Kind = EventToolCompleted
	case ApplicationEventTurnAccepted:
		legacy.Kind = EventTurnAccepted
	case protocol.EventTurnAccepted:
		var accepted protocol.TurnAcceptedV1
		if err := json.Unmarshal(event.Payload, &accepted); err != nil {
			return Event{}, err
		}
		legacy.Kind = EventTurnAccepted
		a.pendingMu.Lock()
		command := a.pending[accepted.CommandID]
		delete(a.pending, accepted.CommandID)
		if event.Correlation.TurnID != "" {
			a.turns[event.Correlation.TurnID] = command
		}
		a.pendingMu.Unlock()
		legacy.DraftID, legacy.Draft = command.DraftID, command.Prompt
	case ApplicationEventTurnCompleted, protocol.EventTurnCompleted:
		legacy.Kind = EventTurnCompleted
	case ApplicationEventTurnInterrupted, protocol.EventTurnInterrupted:
		legacy.Kind = EventTurnInterrupted
	case ApplicationEventReloadCompleted, protocol.EventRuntimeGenerationActivated:
		legacy.Kind = EventReloadCompleted
	case ApplicationEventNotice:
		legacy.Kind = EventNotice
	case ApplicationEventRejected:
		legacy.Kind = EventRejected
	case ApplicationEventError, protocol.EventTurnFailed:
		legacy.Kind = EventError
	case protocol.EventAssistantMessage:
		var message protocol.AssistantMessageV1
		if err := json.Unmarshal(event.Payload, &message); err != nil {
			return Event{}, err
		}
		legacy.Kind = EventTextDelta
		for _, block := range message.Blocks {
			if block.Kind == protocol.ContentText {
				legacy.Runtime.Text += block.Text
			}
		}
	case protocol.EventModeChanged:
		var changed protocol.ModeChangedV1
		if err := json.Unmarshal(event.Payload, &changed); err != nil {
			return Event{}, err
		}
		legacy.Kind, legacy.Mode = EventState, domain.PermissionMode(changed.Mode)
	case protocol.EventModelChanged:
		var changed protocol.ModelChangedV1
		if err := json.Unmarshal(event.Payload, &changed); err != nil {
			return Event{}, err
		}
		legacy.Kind = EventState
		legacy.Selection = domain.ModelSelection{Profile: string(changed.ProviderID), Model: string(changed.ModelID)}
	case protocol.EventContextPlanRecorded:
		var recorded protocol.ContextPlanRecordedV1
		if err := json.Unmarshal(event.Payload, &recorded); err != nil {
			return Event{}, err
		}
		state := protocol.ContextProjectionV1{AutoAvailable: recorded.Plan.Body.ContextWindow.State == protocol.ValueKnown, EstimatedInputTokens: recorded.Plan.Body.EstimatedInputTokens, ContextWindow: recorded.Plan.Body.ContextWindow, OutputReserve: recorded.Plan.Body.OutputReserve, Revision: recorded.Plan.Body.CompactionRevision}
		if state.AutoAvailable {
			state.AutoReason = "below_threshold"
		} else {
			state.AutoReason = "unknown_context_window"
		}
		legacy.Kind, legacy.Context = EventState, &state
	case protocol.EventActivityPlanned:
		var planned protocol.ActivityPlannedV1
		if err := json.Unmarshal(event.Payload, &planned); err != nil {
			return Event{}, err
		}
		if planned.Kind != "provider" || planned.Purpose != "summarize stable context" || event.Correlation.ActivityID == "" {
			break
		}
		trigger := "automatic"
		a.pendingMu.Lock()
		if a.manualCompactQueued {
			trigger, a.manualCompactQueued = "manual", false
		}
		a.compactions[event.Correlation.ActivityID] = trigger
		a.pendingMu.Unlock()
		legacy.Kind, legacy.Message = EventCompactionStarted, trigger+":preparing"
		legacy.Compaction = &protocol.CompactionEventV1{Trigger: trigger, Stage: protocol.CompactionPreparing, Usage: unknownCompactionUsage()}
	case protocol.EventActivityStarted:
		if trigger, ok := a.compactionTrigger(event.Correlation.ActivityID); ok {
			legacy.Kind, legacy.Message = EventCompactionProgress, trigger+":summarizing"
			legacy.Compaction = &protocol.CompactionEventV1{Trigger: trigger, Stage: protocol.CompactionSummarizing, Usage: unknownCompactionUsage()}
		}
	case protocol.EventActivitySucceeded:
		if trigger, ok := a.compactionTrigger(event.Correlation.ActivityID); ok {
			legacy.Kind, legacy.Message = EventCompactionProgress, trigger+":persisting"
			legacy.Compaction = &protocol.CompactionEventV1{Trigger: trigger, Stage: protocol.CompactionPersisting, Usage: unknownCompactionUsage()}
		}
	case protocol.EventActivityFailed, protocol.EventActivityDenied, protocol.EventActivityCancelled, protocol.EventActivityInterruptedNoEffect, protocol.EventActivityUncertain:
		if trigger, ok := a.compactionTrigger(event.Correlation.ActivityID); ok {
			stage, code, message := protocol.CompactionFailed, "compaction_failed", "context compaction failed"
			if event.Kind == protocol.EventActivityCancelled || event.Kind == protocol.EventActivityInterruptedNoEffect {
				stage, code, message = protocol.CompactionCancelled, "cancelled", "compaction was cancelled"
			}
			if event.Kind == protocol.EventActivityUncertain {
				stage, code, message = protocol.CompactionUncertain, "commit_uncertain", "compaction outcome is uncertain"
			}
			legacy.Kind, legacy.Message = EventCompactionFailed, message
			legacy.Compaction = &protocol.CompactionEventV1{Trigger: trigger, Stage: stage, Usage: unknownCompactionUsage(), Error: &protocol.PublicError{Code: code, Message: message}}
			a.finishCompaction(event.Correlation.ActivityID)
		}
	case protocol.EventContextCompacted:
		var compacted protocol.ContextCompactedV1
		if err := json.Unmarshal(event.Payload, &compacted); err != nil {
			return Event{}, err
		}
		trigger, ok := a.compactionTrigger(event.Correlation.ActivityID)
		if !ok {
			break
		}
		rangeValue := protocol.CompactionRange{From: compacted.From, Through: compacted.Through}
		legacy.Kind, legacy.Message = EventCompactionCompleted, fmt.Sprintf("%s:%d–%d", rangeValue.From.JournalID, rangeValue.From.CommitSeq, rangeValue.Through.CommitSeq)
		legacy.Compaction = &protocol.CompactionEventV1{Trigger: trigger, Stage: protocol.CompactionCompleted, Range: &rangeValue, Usage: unknownCompactionUsage(), Revision: compacted.Revision, SummaryEvidenceID: compacted.SummaryEvidenceID}
		legacy.Context = &protocol.ContextProjectionV1{Revision: compacted.Revision, SummaryEvidenceID: compacted.SummaryEvidenceID, LatestRange: &rangeValue}
		a.finishCompaction(event.Correlation.ActivityID)
	default:
		legacy.Kind = ""
	}
	if event.Correlation.TurnID != "" && (legacy.Kind == EventTurnCompleted || legacy.Kind == EventTurnInterrupted || legacy.Kind == EventError) {
		a.pendingMu.Lock()
		command := a.turns[event.Correlation.TurnID]
		delete(a.turns, event.Correlation.TurnID)
		a.pendingMu.Unlock()
		legacy.DraftID, legacy.Draft = command.DraftID, command.Prompt
	}
	if event.Error != nil {
		legacy.Code = event.Error.Code
		legacy.Message = event.Error.Message
		legacy.Err = nil
	}
	return legacy, nil
}

func unknownCompactionUsage() protocol.ModelUsage {
	unknown := protocol.UsageValue{State: protocol.UsageUnknown}
	return protocol.ModelUsage{Input: unknown, Output: unknown, Cached: unknown, CacheWrite: unknown, Reasoning: unknown}
}
func (a *LegacyAdapter) compactionTrigger(id protocol.ActivityID) (string, bool) {
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	trigger, ok := a.compactions[id]
	return trigger, ok
}
func (a *LegacyAdapter) finishCompaction(id protocol.ActivityID) {
	a.pendingMu.Lock()
	delete(a.compactions, id)
	a.pendingMu.Unlock()
}

func strictUnmarshal(raw json.RawMessage, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	return ensureJSONEOF(decoder)
}
