package task

import (
	"encoding/json"
	"fmt"

	"github.com/muratmirgun/yordam/internal/projection"
	"github.com/muratmirgun/yordam/internal/protocol"
)

const projectionVersion uint32 = 1

type State string

const (
	StateDraft                State = "draft"
	StatePending              State = "pending"
	StateContractDrafting     State = "contract_drafting"
	StateContractProposed     State = "contract_proposed"
	StateContractFrozen       State = "contract_frozen"
	StateRunning              State = "running"
	StateVerifying            State = "verifying"
	StateVerified             State = "verified"
	StateCompletedWithWaivers State = "completed_with_waivers"
	StatePartial              State = "partial"
	StateFailed               State = "failed"
	StateUnknown              State = "unknown"
	StateCancelled            State = "cancelled"
	StateReopened             State = "reopened"
	StateCompleted            State = "completed"
)

type TurnState string

const (
	TurnStateAccepted           TurnState = "accepted"
	TurnStateContractDrafting   TurnState = "contract_drafting"
	TurnStateFreezingContract   TurnState = "freezing_contract"
	TurnStatePlanningContext    TurnState = "planning_context"
	TurnStateWaitingProvider    TurnState = "waiting_provider"
	TurnStateReceivingProvider  TurnState = "receiving_provider"
	TurnStatePlanningAction     TurnState = "planning_action"
	TurnStateCheckpointing      TurnState = "checkpointing"
	TurnStateAwaitingPermission TurnState = "awaiting_permission"
	TurnStateExecuting          TurnState = "executing"
	TurnStateRecordingEvidence  TurnState = "recording_evidence"
	TurnStateReturningResult    TurnState = "returning_result"
	TurnStateVerifying          TurnState = "verifying"
	TurnStateRunning            TurnState = "running"
	TurnStateCompleted          TurnState = "completed"
	TurnStateFailed             TurnState = "failed"
	TurnStateInterrupted        TurnState = "interrupted"
)

type CriterionStatus string

const (
	CriterionPending      CriterionStatus = "pending"
	CriterionVerified     CriterionStatus = "verified"
	CriterionFailed       CriterionStatus = "failed"
	CriterionUnknown      CriterionStatus = "unknown"
	CriterionWaivedByUser CriterionStatus = "waived_by_user"
)

type Criterion struct {
	Definition  protocol.CriterionV1  `json:"definition"`
	Status      CriterionStatus       `json:"status"`
	EvidenceIDs []protocol.EvidenceID `json:"evidence_ids,omitempty"`
	ReceiptIDs  []protocol.ReceiptID  `json:"receipt_ids,omitempty"`
	Reason      string                `json:"reason,omitempty"`
}

type Outcome struct {
	ContractVersion uint32                `json:"contract_version"`
	State           protocol.ValueState   `json:"state"`
	Status          string                `json:"status,omitempty"`
	CriterionIDs    []string              `json:"criterion_ids,omitempty"`
	ReceiptIDs      []protocol.ReceiptID  `json:"receipt_ids,omitempty"`
	UnknownEffects  []protocol.SubjectRef `json:"unknown_effects,omitempty"`
}

type ContractVersion struct {
	Version         uint32               `json:"version"`
	Goal            string               `json:"goal"`
	Source          string               `json:"source"`
	Frozen          bool                 `json:"frozen"`
	Criteria        map[string]Criterion `json:"criteria"`
	AmendmentReason string               `json:"amendment_reason,omitempty"`
	AmendmentActor  *protocol.ActorRef   `json:"amendment_actor,omitempty"`
}

type Assessment struct {
	ContractVersion uint32                `json:"contract_version"`
	CriterionID     string                `json:"criterion_id"`
	Status          CriterionStatus       `json:"status"`
	EvidenceIDs     []protocol.EvidenceID `json:"evidence_ids,omitempty"`
	ReceiptIDs      []protocol.ReceiptID  `json:"receipt_ids,omitempty"`
	Reason          string                `json:"reason"`
}

type Record struct {
	TaskID            protocol.TaskID            `json:"task_id"`
	Goal              string                     `json:"goal"`
	State             State                      `json:"state"`
	OutcomeContractID protocol.OutcomeContractID `json:"outcome_contract_id"`
	ContractVersion   uint32                     `json:"contract_version"`
	ContractSource    string                     `json:"contract_source,omitempty"`
	ContractFrozen    bool                       `json:"contract_frozen"`
	Criteria          map[string]Criterion       `json:"criteria"`
	Contracts         map[uint32]ContractVersion `json:"contracts"`
	Assessments       []Assessment               `json:"assessments,omitempty"`
	VerificationState protocol.ValueState        `json:"verification_state"`
	Outcome           Outcome                    `json:"outcome"`
	OutcomeHistory    []Outcome                  `json:"outcome_history,omitempty"`
	Legacy            bool                       `json:"legacy,omitempty"`
}

type TurnRecord struct {
	TurnID            protocol.TurnID            `json:"turn_id"`
	TaskID            protocol.TaskID            `json:"task_id,omitempty"`
	CommandID         protocol.CommandID         `json:"command_id"`
	Goal              string                     `json:"goal"`
	OutcomeContractID protocol.OutcomeContractID `json:"outcome_contract_id"`
	ContractVersion   uint32                     `json:"contract_version"`
	State             TurnState                  `json:"state"`
	Reason            string                     `json:"reason,omitempty"`
	ErrorCode         string                     `json:"error_code,omitempty"`
	UnknownEffects    []protocol.SubjectRef      `json:"unknown_effects,omitempty"`
}

type Projection struct {
	Journal              protocol.JournalRef                                  `json:"journal"`
	Tasks                map[protocol.TaskID]Record                           `json:"tasks"`
	Turns                map[protocol.TurnID]TurnRecord                       `json:"turns"`
	EvidenceAvailability map[protocol.EvidenceID]protocol.ContentAvailability `json:"evidence_availability"`
	Diagnostics          []protocol.Diagnostic                                `json:"diagnostics,omitempty"`
}

type Projector struct{}

func (Projector) Version() uint32 { return projectionVersion }

func (Projector) Zero(ref protocol.JournalRef) Projection {
	return Projection{
		Journal: ref, Tasks: make(map[protocol.TaskID]Record), Turns: make(map[protocol.TurnID]TurnRecord),
		EvidenceAvailability: make(map[protocol.EvidenceID]protocol.ContentAvailability),
	}
}

func (Projector) Apply(current Projection, event protocol.EventRecord) (Projection, error) {
	if err := projection.ValidateFoundationEvent(event); err != nil {
		return current, err
	}
	next := protocol.DeepCopy(current)
	ensureMaps(&next)
	switch event.Envelope.Kind {
	case protocol.EventTaskCreated:
		payload, ok := event.Decoded.(*protocol.TaskCreatedV1)
		if !ok || event.Envelope.TaskID == "" {
			return current, fmt.Errorf("invalid task creation")
		}
		if _, exists := next.Tasks[event.Envelope.TaskID]; exists {
			return current, fmt.Errorf("task %q already exists", event.Envelope.TaskID)
		}
		initialState := StateDraft
		if event.Legacy != nil {
			initialState = StatePending
		}
		next.Tasks[event.Envelope.TaskID] = Record{
			TaskID: event.Envelope.TaskID, Goal: payload.Goal, State: initialState,
			OutcomeContractID: payload.OutcomeContractID, ContractVersion: payload.ContractVersion,
			Criteria: make(map[string]Criterion), Contracts: make(map[uint32]ContractVersion), VerificationState: protocol.ValueUnknown,
			Outcome: Outcome{State: protocol.ValueUnknown}, Legacy: event.Legacy != nil,
		}
	case protocol.EventTaskStatusChanged:
		payload, ok := event.Decoded.(*protocol.TaskStatusChangedV1)
		if !ok {
			return current, fmt.Errorf("invalid task transition")
		}
		record, exists := next.Tasks[event.Envelope.TaskID]
		if !exists {
			return current, fmt.Errorf("task %q is unknown", event.Envelope.TaskID)
		}
		from, to := State(payload.From), State(payload.To)
		if record.State != from || !allowedTaskTransition(record.Legacy, from, to) {
			return current, fmt.Errorf("invalid task transition %q -> %q", from, to)
		}
		record.State = to
		next.Tasks[record.TaskID] = record
	case protocol.EventOutcomeContractDeclared:
		payload, ok := event.Decoded.(*protocol.OutcomeContractDeclaredV1)
		if !ok {
			return current, fmt.Errorf("invalid outcome contract")
		}
		record, exists := next.Tasks[event.Envelope.TaskID]
		if !exists || payload.OutcomeContractID != record.OutcomeContractID || payload.Version != record.ContractVersion {
			return current, fmt.Errorf("outcome contract binding mismatch")
		}
		if _, exists := record.Contracts[payload.Version]; exists {
			return current, fmt.Errorf("outcome contract version %d is already declared", payload.Version)
		}
		criteria, err := newCriteria(payload.Criteria)
		if err != nil {
			return current, err
		}
		record.Goal, record.ContractSource, record.ContractFrozen, record.Criteria = payload.Goal, payload.Source, payload.Frozen, criteria
		record.Contracts[payload.Version] = ContractVersion{
			Version: payload.Version, Goal: payload.Goal, Source: payload.Source, Frozen: payload.Frozen, Criteria: protocol.DeepCopy(criteria),
		}
		next.Tasks[record.TaskID] = record
	case protocol.EventOutcomeContractAmended:
		payload, ok := event.Decoded.(*protocol.OutcomeContractAmendedV1)
		if !ok {
			return current, fmt.Errorf("invalid outcome contract amendment")
		}
		record, exists := next.Tasks[event.Envelope.TaskID]
		if !exists || payload.OutcomeContractID != record.OutcomeContractID || payload.FromVersion != record.ContractVersion || payload.ToVersion != payload.FromVersion+1 {
			return current, fmt.Errorf("outcome contract amendment binding mismatch")
		}
		criteria, err := newCriteria(payload.Criteria)
		if err != nil {
			return current, err
		}
		record.ContractVersion, record.ContractFrozen, record.Criteria = payload.ToVersion, payload.Frozen, criteria
		if record.Contracts == nil {
			record.Contracts = make(map[uint32]ContractVersion)
		}
		actor := protocol.DeepCopy(payload.Actor)
		record.Contracts[payload.ToVersion] = ContractVersion{
			Version: payload.ToVersion, Goal: record.Goal, Source: "amendment", Frozen: payload.Frozen,
			Criteria: protocol.DeepCopy(criteria), AmendmentReason: payload.Reason, AmendmentActor: &actor,
		}
		record.VerificationState, record.Outcome = protocol.ValueUnknown, Outcome{State: protocol.ValueUnknown}
		next.Tasks[record.TaskID] = record
	case protocol.EventOutcomeCriterionAssessed:
		payload, ok := event.Decoded.(*protocol.CriterionAssessedV1)
		if !ok {
			return current, fmt.Errorf("invalid criterion assessment")
		}
		record, exists := next.Tasks[event.Envelope.TaskID]
		if !exists || payload.OutcomeContractID != record.OutcomeContractID || payload.ContractVersion != record.ContractVersion {
			return current, fmt.Errorf("criterion assessment contract binding mismatch")
		}
		criterion, exists := record.Criteria[payload.CriterionID]
		if !exists {
			return current, fmt.Errorf("criterion %q is unknown", payload.CriterionID)
		}
		criterion.Status = CriterionStatus(payload.Status)
		if !validCriterionStatus(criterion.Status) {
			return current, fmt.Errorf("invalid criterion status %q", payload.Status)
		}
		criterion.EvidenceIDs, criterion.ReceiptIDs, criterion.Reason = append([]protocol.EvidenceID(nil), payload.EvidenceIDs...), append([]protocol.ReceiptID(nil), payload.ReceiptIDs...), payload.Reason
		if criterion.Status == CriterionVerified && !evidenceAvailable(next.EvidenceAvailability, criterion.EvidenceIDs) {
			criterion.Status = CriterionUnknown
			next.Diagnostics = append(next.Diagnostics, missingEvidenceDiagnostic(next.Journal, event, payload.CriterionID))
		}
		record.Criteria[payload.CriterionID] = criterion
		contract := record.Contracts[payload.ContractVersion]
		if contract.Criteria != nil {
			contract.Criteria[payload.CriterionID] = protocol.DeepCopy(criterion)
			record.Contracts[payload.ContractVersion] = contract
		}
		record.Assessments = append(record.Assessments, Assessment{
			ContractVersion: payload.ContractVersion, CriterionID: payload.CriterionID, Status: criterion.Status,
			EvidenceIDs: append([]protocol.EvidenceID(nil), criterion.EvidenceIDs...), ReceiptIDs: append([]protocol.ReceiptID(nil), criterion.ReceiptIDs...), Reason: criterion.Reason,
		})
		record.VerificationState = aggregateVerification(record.Criteria)
		next.Tasks[record.TaskID] = record
	case protocol.EventOutcomeFinalAssessed:
		payload, ok := event.Decoded.(*protocol.OutcomeFinalAssessedV1)
		if !ok {
			return current, fmt.Errorf("invalid final outcome")
		}
		record, exists := next.Tasks[event.Envelope.TaskID]
		if !exists || payload.OutcomeContractID != record.OutcomeContractID || payload.ContractVersion != record.ContractVersion {
			return current, fmt.Errorf("final outcome contract binding mismatch")
		}
		if payload.Status == "verified" {
			for id, criterion := range record.Criteria {
				if criterion.Definition.Required && criterion.Status != CriterionVerified {
					return current, fmt.Errorf("required criterion %q is %q", id, criterion.Status)
				}
			}
		}
		record.Outcome = Outcome{
			ContractVersion: payload.ContractVersion, State: protocol.ValueKnown, Status: payload.Status, CriterionIDs: append([]string(nil), payload.CriterionIDs...),
			ReceiptIDs: append([]protocol.ReceiptID(nil), payload.ReceiptIDs...), UnknownEffects: protocol.DeepCopy(payload.UnknownEffects),
		}
		record.OutcomeHistory = append(record.OutcomeHistory, protocol.DeepCopy(record.Outcome))
		next.Tasks[record.TaskID] = record
	case protocol.EventTurnAccepted:
		payload, ok := event.Decoded.(*protocol.TurnAcceptedV1)
		if !ok || event.Envelope.TurnID == "" {
			return current, fmt.Errorf("invalid accepted turn")
		}
		if _, exists := next.Turns[event.Envelope.TurnID]; exists {
			return current, fmt.Errorf("turn %q already exists", event.Envelope.TurnID)
		}
		for _, existing := range next.Turns {
			if !terminalTurnState(existing.State) {
				return current, fmt.Errorf("turn %q is still active", existing.TurnID)
			}
		}
		next.Turns[event.Envelope.TurnID] = TurnRecord{
			TurnID: event.Envelope.TurnID, TaskID: event.Envelope.TaskID, CommandID: payload.CommandID, Goal: payload.Goal,
			OutcomeContractID: payload.OutcomeContractID, ContractVersion: payload.ContractVersion, State: TurnStateAccepted,
		}
	case protocol.EventTurnStateChanged:
		payload, ok := event.Decoded.(*protocol.TurnStateChangedV1)
		if !ok {
			return current, fmt.Errorf("invalid turn transition")
		}
		record, exists := next.Turns[event.Envelope.TurnID]
		from, to := TurnState(payload.From), TurnState(payload.To)
		if !exists || record.State != from || !allowedTurnTransition(from, to) {
			return current, fmt.Errorf("invalid turn transition %q -> %q", from, to)
		}
		record.State = to
		next.Turns[record.TurnID] = record
	case protocol.EventTurnCompleted, protocol.EventTurnFailed, protocol.EventTurnInterrupted:
		payload, ok := event.Decoded.(*protocol.TurnTerminalV1)
		if !ok {
			return current, fmt.Errorf("invalid terminal turn")
		}
		record, exists := next.Turns[event.Envelope.TurnID]
		to := turnTerminalState(event.Envelope.Kind)
		if !exists || (record.State != TurnStateVerifying && record.State != TurnStateRunning) || payload.Status != string(to) {
			return current, fmt.Errorf("invalid turn transition %q -> %q", record.State, to)
		}
		record.State, record.Reason, record.ErrorCode = to, payload.Reason, payload.ErrorCode
		record.UnknownEffects = protocol.DeepCopy(payload.UnknownEffects)
		next.Turns[record.TurnID] = record
	case protocol.EventEvidenceRecorded:
		payload, ok := event.Decoded.(*protocol.EvidenceRecordedV1)
		if !ok {
			return current, fmt.Errorf("invalid evidence record")
		}
		next.EvidenceAvailability[payload.Record.Body.ID] = payload.Record.Body.Availability
		if payload.Record.Body.Availability != protocol.ContentAvailable {
			invalidateEvidence(&next, payload.Record.Body.ID, protocol.Diagnostic{Code: "evidence." + string(payload.Record.Body.Availability), Message: "evidence content is unavailable", Journal: next.Journal, EventID: event.Envelope.EventID})
		}
	case protocol.EventRecoveryDiagnostic:
		payload, ok := event.Decoded.(*protocol.DiagnosticV1)
		if !ok {
			return current, fmt.Errorf("invalid recovery diagnostic")
		}
		if payload.Diagnostic.Code == "evidence.missing" || payload.Diagnostic.Code == "evidence.corrupt" {
			var details struct {
				EvidenceID protocol.EvidenceID `json:"evidence_id"`
			}
			if json.Unmarshal(payload.Diagnostic.Details, &details) == nil && details.EvidenceID != "" {
				next.EvidenceAvailability[details.EvidenceID] = protocol.ContentMissing
				invalidateEvidence(&next, details.EvidenceID, payload.Diagnostic)
			}
		}
	default:
		return next, nil
	}
	return next, nil
}

func ensureMaps(state *Projection) {
	if state.Tasks == nil {
		state.Tasks = make(map[protocol.TaskID]Record)
	}
	if state.Turns == nil {
		state.Turns = make(map[protocol.TurnID]TurnRecord)
	}
	if state.EvidenceAvailability == nil {
		state.EvidenceAvailability = make(map[protocol.EvidenceID]protocol.ContentAvailability)
	}
}

func allowedTaskTransition(legacy bool, from, to State) bool {
	if legacy {
		return (from == StatePending && (to == StateRunning || to == StateCancelled)) ||
			(from == StateRunning && (to == StateCompleted || to == StateFailed || to == StateCancelled))
	}
	switch from {
	case StateDraft:
		return to == StateContractDrafting
	case StateContractDrafting:
		return to == StateContractProposed
	case StateContractProposed:
		return to == StateContractFrozen
	case StateContractFrozen:
		return to == StateRunning
	case StateRunning:
		return to == StateVerifying
	case StateVerifying:
		return to == StateVerified || to == StateCompletedWithWaivers || to == StatePartial || to == StateFailed || to == StateUnknown || to == StateCancelled
	case StatePartial, StateFailed, StateUnknown, StateCompletedWithWaivers:
		return to == StateReopened
	case StateReopened:
		return to == StateRunning
	default:
		return false
	}
}

func terminalTurnState(state TurnState) bool {
	return state == TurnStateCompleted || state == TurnStateFailed || state == TurnStateInterrupted
}

func allowedTurnTransition(from, to TurnState) bool {
	if from == TurnStateAccepted && to == TurnStateRunning {
		return true
	}
	if from == TurnStateVerifying && (to == TurnStateCompleted || to == TurnStateFailed || to == TurnStateInterrupted) {
		return true
	}
	path := map[TurnState]TurnState{
		TurnStateAccepted: TurnStateContractDrafting, TurnStateContractDrafting: TurnStateFreezingContract,
		TurnStateFreezingContract: TurnStatePlanningContext, TurnStatePlanningContext: TurnStateWaitingProvider,
		TurnStateWaitingProvider: TurnStateReceivingProvider, TurnStateReceivingProvider: TurnStatePlanningAction,
		TurnStatePlanningAction: TurnStateCheckpointing, TurnStateCheckpointing: TurnStateAwaitingPermission,
		TurnStateAwaitingPermission: TurnStateExecuting, TurnStateExecuting: TurnStateRecordingEvidence,
		TurnStateRecordingEvidence: TurnStateReturningResult, TurnStateReturningResult: TurnStateVerifying,
	}
	return path[from] == to
}

func turnTerminalState(kind string) TurnState {
	return map[string]TurnState{protocol.EventTurnCompleted: TurnStateCompleted, protocol.EventTurnFailed: TurnStateFailed, protocol.EventTurnInterrupted: TurnStateInterrupted}[kind]
}

func newCriteria(definitions []protocol.CriterionV1) (map[string]Criterion, error) {
	criteria := make(map[string]Criterion, len(definitions))
	for _, definition := range definitions {
		if definition.ID == "" {
			return nil, fmt.Errorf("criterion ID is required")
		}
		if _, exists := criteria[definition.ID]; exists {
			return nil, fmt.Errorf("duplicate criterion %q", definition.ID)
		}
		criteria[definition.ID] = Criterion{Definition: definition, Status: CriterionPending}
	}
	return criteria, nil
}

func validCriterionStatus(status CriterionStatus) bool {
	return status == CriterionPending || status == CriterionVerified || status == CriterionFailed || status == CriterionUnknown || status == CriterionWaivedByUser
}

func evidenceAvailable(availability map[protocol.EvidenceID]protocol.ContentAvailability, evidence []protocol.EvidenceID) bool {
	if len(evidence) == 0 {
		return false
	}
	for _, id := range evidence {
		if availability[id] != protocol.ContentAvailable {
			return false
		}
	}
	return true
}

func aggregateVerification(criteria map[string]Criterion) protocol.ValueState {
	if len(criteria) == 0 {
		return protocol.ValueUnknown
	}
	for _, criterion := range criteria {
		if criterion.Definition.Required && criterion.Status != CriterionVerified {
			return protocol.ValueUnknown
		}
	}
	return protocol.ValueKnown
}

func invalidateEvidence(state *Projection, evidenceID protocol.EvidenceID, diagnostic protocol.Diagnostic) {
	changed := false
	for taskID, record := range state.Tasks {
		for criterionID, criterion := range record.Criteria {
			if criterion.Status == CriterionVerified && containsEvidence(criterion.EvidenceIDs, evidenceID) {
				criterion.Status = CriterionUnknown
				criterion.Reason = "dependent evidence is unavailable"
				record.Criteria[criterionID] = criterion
				contract := record.Contracts[record.ContractVersion]
				if contract.Criteria != nil {
					contract.Criteria[criterionID] = protocol.DeepCopy(criterion)
					record.Contracts[record.ContractVersion] = contract
				}
				wasVerified := record.State == StateVerified || (record.Outcome.State == protocol.ValueKnown && record.Outcome.Status == "verified")
				record.VerificationState = protocol.ValueUnknown
				record.Outcome = Outcome{State: protocol.ValueUnknown}
				if wasVerified {
					record.State = StateUnknown
				}
				changed = true
			}
		}
		state.Tasks[taskID] = record
	}
	if changed {
		state.Diagnostics = append(state.Diagnostics, diagnostic)
	}
}

func containsEvidence(values []protocol.EvidenceID, target protocol.EvidenceID) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func missingEvidenceDiagnostic(ref protocol.JournalRef, event protocol.EventRecord, criterionID string) protocol.Diagnostic {
	details, _ := json.Marshal(struct {
		CriterionID string `json:"criterion_id"`
	}{CriterionID: criterionID})
	return protocol.Diagnostic{Code: "evidence.unavailable", Message: "criterion evidence is unavailable", Journal: ref, EventID: event.Envelope.EventID, Details: details}
}
