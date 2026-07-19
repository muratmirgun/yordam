package app

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
	skilltool "github.com/muratmirgun/yordam/internal/tools/skill"
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
	pendingMu           sync.Mutex
	pending             map[protocol.CommandID]Command
	turns               map[protocol.TurnID]Command
	compactions         map[protocol.ActivityID]string
	compactionFacts     map[protocol.ActivityID]compactionFacts
	skillProvenance     map[protocol.ActivityID]domain.SkillProvenance
	skillCalls          map[protocol.ActivityID]string
	skillPlanCalls      map[protocol.ActivityID]string
	skillPlanDigests    map[protocol.ActivityID]protocol.Digest
	skillGenerations    map[protocol.ActivityID]protocol.RuntimeGenerationID
	skillResults        map[protocol.ActivityID]bool
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
		pending: make(map[protocol.CommandID]Command), turns: make(map[protocol.TurnID]Command), compactions: make(map[protocol.ActivityID]string), compactionFacts: make(map[protocol.ActivityID]compactionFacts), skillProvenance: make(map[protocol.ActivityID]domain.SkillProvenance), skillCalls: make(map[protocol.ActivityID]string), skillPlanCalls: make(map[protocol.ActivityID]string), skillPlanDigests: make(map[protocol.ActivityID]protocol.Digest), skillGenerations: make(map[protocol.ActivityID]protocol.RuntimeGenerationID), skillResults: make(map[protocol.ActivityID]bool),
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
	var expectation *protocol.CommandExpectation
	if a.cursor != nil {
		expectation = protocol.DeepCopy(a.cursor())
	} else if a.selectedSessionID != "" {
		expectation = &protocol.CommandExpectation{SelectedSessionID: a.selectedSessionID}
	}
	// Trust controls are workspace-scoped. Deliberately drop a convenience
	// selected-session cursor supplied by the normal runtime callback so the
	// exact workspace decision cannot accidentally become session-bound.
	if command.Kind == CommandTrustSkillCatalog && expectation != nil {
		expectation = &protocol.CommandExpectation{WorkspaceControl: protocol.DeepCopy(expectation.WorkspaceControl)}
	}
	var commandID protocol.CommandID
	var idempotencyKey string
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
	} else if command.Kind == CommandTrustSkillCatalog {
		if expectation == nil || expectation.WorkspaceControl == nil {
			return protocol.Command{}, fmt.Errorf("skill trust command requires a workspace-control cursor")
		}
		identity, digestErr := canonicaljson.Digest(struct {
			Workspace  protocol.CommittedCursor     `json:"workspace"`
			Trust      protocol.SkillTrustCommandV1 `json:"trust"`
			Generation protocol.RuntimeGenerationID `json:"generation"`
		}{*expectation.WorkspaceControl, command.SkillTrust, a.runtimeGenerationID})
		if digestErr != nil {
			return protocol.Command{}, digestErr
		}
		commandID = protocol.CommandID("legacy-skill-trust-" + identity.Value)
		idempotencyKey = string(commandID)
	} else {
		random := make([]byte, 16)
		if _, err := rand.Read(random); err != nil {
			return protocol.Command{}, err
		}
		commandID = protocol.CommandID(fmt.Sprintf("legacy-command-%x", random))
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
	case CommandTrustSkillCatalog:
		return string(CommandTrustSkillCatalog), command.SkillTrust, nil
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
	case "runtime.tool_result_available":
		var available protocol.ToolResultAvailableV1
		if err := strictUnmarshal(event.Payload, &available); err != nil {
			return Event{}, err
		}
		if provenance, result, ok := a.skillResult(event.Correlation.ActivityID, available); ok {
			legacy.Kind, legacy.Runtime.Skill, legacy.Runtime.Result = EventToolCompleted, &provenance, &result
		}
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
		// Policy conclusions belong to the immutable generation-aware snapshot
		// projection. A live plan event carries facts only, never a guess based
		// on the currently selected generation.
		state := protocol.ContextProjectionV1{EstimatedInputTokens: recorded.Plan.Body.EstimatedInputTokens, ContextWindow: recorded.Plan.Body.ContextWindow, OutputReserve: recorded.Plan.Body.OutputReserve, Revision: recorded.Plan.Body.CompactionRevision}
		legacy.Kind, legacy.Context = EventState, &state
	case protocol.EventActivityPlanned:
		a.clearSkillActivity(event.Correlation.ActivityID)
		var planned protocol.ActivityPlannedV1
		if err := json.Unmarshal(event.Payload, &planned); err != nil {
			return Event{}, err
		}
		if provenance, ok := plannedSkillProvenance(planned); ok && event.Correlation.ActivityID != "" {
			a.pendingMu.Lock()
			a.skillProvenance[event.Correlation.ActivityID] = provenance
			a.skillPlanCalls[event.Correlation.ActivityID] = planned.Plan.Body.CallID
			a.skillPlanDigests[event.Correlation.ActivityID] = planned.Plan.Digest
			a.skillGenerations[event.Correlation.ActivityID] = planned.Plan.Body.RuntimeGenerationID
			a.pendingMu.Unlock()
		}
		if planned.CompactionTrigger == "" || event.Correlation.ActivityID == "" {
			break
		}
		trigger := planned.CompactionTrigger
		a.pendingMu.Lock()
		a.compactions[event.Correlation.ActivityID] = trigger
		a.pendingMu.Unlock()
		legacy.Kind, legacy.Message = EventCompactionStarted, trigger+":preparing"
		legacy.Compaction = &protocol.CompactionEventV1{Trigger: trigger, Stage: protocol.CompactionPreparing, Usage: unknownCompactionUsage()}
	case protocol.EventActivityStarted:
		if provenance, callID, ok := a.startSkillActivity(event.Correlation.ActivityID, event.Payload); ok {
			legacy.Kind = EventToolStarted
			legacy.Runtime.Skill = &provenance
			legacy.Runtime.Progress = &domain.ToolProgress{CallID: callID}
			break
		}
		if trigger, ok := a.compactionTrigger(event.Correlation.ActivityID); ok {
			legacy.Kind, legacy.Message = EventCompactionProgress, trigger+":summarizing"
			legacy.Compaction = &protocol.CompactionEventV1{Trigger: trigger, Stage: protocol.CompactionSummarizing, Usage: unknownCompactionUsage()}
		}
	case protocol.EventActivitySucceeded:
		if provenance, callID, published, ok := a.finishSkillActivity(event.Correlation.ActivityID); ok && !published {
			legacy.Kind = EventToolCompleted
			legacy.Runtime.Skill = &provenance
			legacy.Runtime.Result = &domain.ToolResult{CallID: callID, Status: domain.ToolSucceeded}
			break
		}
		if trigger, ok := a.compactionTrigger(event.Correlation.ActivityID); ok {
			var outcome protocol.ActivityOutcomeV1
			if err := json.Unmarshal(event.Payload, &outcome); err != nil {
				return Event{}, err
			}
			a.pendingMu.Lock()
			a.compactionFacts[event.Correlation.ActivityID] = compactionFacts{usage: outcome.Usage, outputBytes: outcome.OutputBytes}
			a.pendingMu.Unlock()
			legacy.Kind, legacy.Message = EventCompactionProgress, trigger+":persisting"
			legacy.Compaction = &protocol.CompactionEventV1{Trigger: trigger, Stage: protocol.CompactionPersisting, Usage: unknownCompactionUsage()}
		}
	case protocol.EventActivityFailed, protocol.EventActivityDenied, protocol.EventActivityCancelled, protocol.EventActivityInterruptedNoEffect, protocol.EventActivityUncertain:
		if provenance, callID, published, ok := a.finishSkillActivity(event.Correlation.ActivityID); ok && !published {
			legacy.Kind = EventToolCompleted
			legacy.Runtime.Skill = &provenance
			legacy.Runtime.Result = &domain.ToolResult{CallID: callID, Status: skillTerminalStatus(event.Kind)}
			break
		}
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
		facts := a.takeCompactionFacts(event.Correlation.ActivityID)
		usage := unknownCompactionUsage()
		if facts.usage != nil {
			usage = *facts.usage
		}
		legacy.Compaction = &protocol.CompactionEventV1{Trigger: trigger, Stage: protocol.CompactionCompleted, Range: &rangeValue, Usage: usage, SummaryBytes: facts.outputBytes, Revision: compacted.Revision, SummaryEvidenceID: compacted.SummaryEvidenceID}
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

// SkillProvenance returns metadata only when it was derived from an exact
// durable activity plan. Tool result JSON is never used as provenance.
func (a *LegacyAdapter) SkillProvenance(activityID protocol.ActivityID) (*domain.SkillProvenance, bool) {
	if a == nil || activityID == "" {
		return nil, false
	}
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	provenance, ok := a.skillProvenance[activityID]
	if !ok || provenance.Validate() != nil {
		return nil, false
	}
	copy := protocol.DeepCopy(provenance)
	return &copy, true
}

func (a *LegacyAdapter) startSkillActivity(activityID protocol.ActivityID, raw json.RawMessage) (domain.SkillProvenance, string, bool) {
	if activityID == "" {
		return domain.SkillProvenance{}, "", false
	}
	var started protocol.ActivityStartedV1
	if json.Unmarshal(raw, &started) != nil || started.CallID == "" || started.ActivityID != activityID {
		return domain.SkillProvenance{}, "", false
	}
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	provenance, ok := a.skillProvenance[activityID]
	plannedCallID := a.skillPlanCalls[activityID]
	plannedDigest := a.skillPlanDigests[activityID]
	plannedGeneration := a.skillGenerations[activityID]
	if !ok || provenance.Validate() != nil || plannedCallID == "" || plannedCallID != started.CallID || plannedDigest != started.PlanDigest || plannedGeneration == "" || plannedGeneration != started.RuntimeGenerationID {
		return domain.SkillProvenance{}, "", false
	}
	a.skillCalls[activityID] = started.CallID
	return protocol.DeepCopy(provenance), started.CallID, true
}

func (a *LegacyAdapter) finishSkillActivity(activityID protocol.ActivityID) (domain.SkillProvenance, string, bool, bool) {
	if activityID == "" {
		return domain.SkillProvenance{}, "", false, false
	}
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	provenance, hasProvenance := a.skillProvenance[activityID]
	callID, hasCall := a.skillCalls[activityID]
	published := a.skillResults[activityID]
	delete(a.skillProvenance, activityID)
	delete(a.skillCalls, activityID)
	delete(a.skillPlanCalls, activityID)
	delete(a.skillPlanDigests, activityID)
	delete(a.skillGenerations, activityID)
	delete(a.skillResults, activityID)
	if !hasProvenance || !hasCall || provenance.Validate() != nil || callID == "" {
		return domain.SkillProvenance{}, "", false, false
	}
	return protocol.DeepCopy(provenance), callID, published, true
}

func (a *LegacyAdapter) clearSkillActivity(activityID protocol.ActivityID) {
	if a == nil || activityID == "" {
		return
	}
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	delete(a.skillProvenance, activityID)
	delete(a.skillCalls, activityID)
	delete(a.skillPlanCalls, activityID)
	delete(a.skillPlanDigests, activityID)
	delete(a.skillGenerations, activityID)
	delete(a.skillResults, activityID)
}

func (a *LegacyAdapter) skillResult(activityID protocol.ActivityID, available protocol.ToolResultAvailableV1) (domain.SkillProvenance, domain.ToolResult, bool) {
	if activityID == "" || available.ActivityID != activityID || available.CallID == "" || available.Status == "" || available.DurationNanos < 0 {
		return domain.SkillProvenance{}, domain.ToolResult{}, false
	}
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	provenance, ok := a.skillProvenance[activityID]
	if !ok || provenance.Validate() != nil || a.skillCalls[activityID] != available.CallID || a.skillResults[activityID] {
		return domain.SkillProvenance{}, domain.ToolResult{}, false
	}
	status := domain.ToolStatus(available.Status)
	if status != domain.ToolSucceeded && status != domain.ToolFailed && status != domain.ToolDenied && status != domain.ToolCancelled {
		return domain.SkillProvenance{}, domain.ToolResult{}, false
	}
	a.skillResults[activityID] = true
	return protocol.DeepCopy(provenance), domain.ToolResult{CallID: available.CallID, Status: status, Content: available.Content, Duration: time.Duration(available.DurationNanos), Truncated: available.Truncated}, true
}

func skillTerminalStatus(kind string) domain.ToolStatus {
	switch kind {
	case protocol.EventActivityDenied:
		return domain.ToolDenied
	case protocol.EventActivityCancelled:
		return domain.ToolCancelled
	default:
		return domain.ToolFailed
	}
}

func plannedSkillProvenance(planned protocol.ActivityPlannedV1) (domain.SkillProvenance, bool) {
	plan := planned.Plan
	if planned.Kind != "tool" || planned.Purpose != "tool observation" || planned.PurposeActor != (protocol.ActorRef{ID: "orchestrator", Kind: protocol.ActorAgent}) || planned.Source != "builtin" || len(planned.InputEvidenceIDs) != 0 || planned.RequestedProfile != "restricted" || planned.EffectiveProfile != "restricted" {
		return domain.SkillProvenance{}, false
	}
	canonical := skilltool.BuiltinDescriptor()
	if plan == nil || plan.Body.Validate() != nil || canonicaljson.ValidateDigest(plan.Body, plan.Digest) != nil ||
		plan.Body.Tool != canonical.Body.Identity || plan.Body.SourceRevision != canonical.Body.SourceRevision || plan.Body.DescriptorDigest != canonical.DescriptorDigest ||
		plan.Body.Action != "skill" || plan.Body.Purpose != "inspect" || plan.Body.ExecutionLocus != "builtin" || plan.Body.Effect != "observation" || plan.Body.Boundary != "workspace" || plan.Body.Reversibility != "not_applicable" || plan.Body.VerificationCoverage != "full" || plan.Body.RequestedProfile != "restricted" || plan.Body.EffectiveProfile != "restricted" || len(plan.Body.Resources) != 1 {
		return domain.SkillProvenance{}, false
	}
	resource := plan.Body.Resources[0]
	if resource.Kind != "skill" || resource.CanonicalID == "" || resource.Digest == "" {
		return domain.SkillProvenance{}, false
	}
	algorithm, value, found := strings.Cut(resource.Digest, ":")
	if !found {
		return domain.SkillProvenance{}, false
	}
	attributes := make(map[string]string, len(resource.Attributes))
	for _, attribute := range resource.Attributes {
		if _, duplicate := attributes[attribute.Name]; duplicate {
			return domain.SkillProvenance{}, false
		}
		attributes[attribute.Name] = attribute.Value
	}
	if len(attributes) != 3 || attributes["runtime_generation"] != string(plan.Body.RuntimeGenerationID) || attributes["source"] == "" {
		return domain.SkillProvenance{}, false
	}
	if _, ok := attributes["workspace_id"]; !ok {
		return domain.SkillProvenance{}, false
	}
	source := protocol.SkillSource(attributes["source"])
	if (source == protocol.SkillSourceGlobal && attributes["workspace_id"] != "") || (source == protocol.SkillSourceProject && attributes["workspace_id"] == "") {
		return domain.SkillProvenance{}, false
	}
	provenance := domain.SkillProvenance{Name: resource.CanonicalID, Source: source, Digest: protocol.Digest{Algorithm: algorithm, Value: value}}
	return provenance, provenance.Validate() == nil
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
	delete(a.compactionFacts, id)
	a.pendingMu.Unlock()
}

type compactionFacts struct {
	usage       *protocol.ModelUsage
	outputBytes int64
}

func (a *LegacyAdapter) takeCompactionFacts(id protocol.ActivityID) compactionFacts {
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	return a.compactionFacts[id]
}

func strictUnmarshal(raw json.RawMessage, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	return ensureJSONEOF(decoder)
}
