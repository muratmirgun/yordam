package activity

import (
	"fmt"
	"time"

	"github.com/muratmirgun/yordam/internal/projection"
	"github.com/muratmirgun/yordam/internal/protocol"
)

const projectionVersion uint32 = 1

type State string

const (
	StatePlanned             State = "planned"
	StateAuthorized          State = "authorized"
	StateStarted             State = "started"
	StateSucceeded           State = "succeeded"
	StateFailed              State = "failed"
	StateDenied              State = "denied"
	StateCancelled           State = "cancelled"
	StateInterruptedNoEffect State = "interrupted_no_effect"
	StateUncertain           State = "uncertain"
)

type DecisionRef struct {
	DecisionNonce   protocol.DecisionNonce `json:"decision_nonce"`
	DecisionEventID protocol.EventID       `json:"decision_event_id"`
	PlanDigest      protocol.Digest        `json:"plan_digest"`
	RequestDigest   protocol.Digest        `json:"request_digest"`
	DispatchDigest  protocol.Digest        `json:"dispatch_digest"`
}

type ChildSummary struct {
	ActivityID protocol.ActivityID `json:"activity_id"`
	Kind       string              `json:"kind"`
	State      State               `json:"state"`
}

type Record struct {
	ActivityID        protocol.ActivityID   `json:"activity_id"`
	TaskID            protocol.TaskID       `json:"task_id,omitempty"`
	TurnID            protocol.TurnID       `json:"turn_id,omitempty"`
	ParentActivityID  protocol.ActivityID   `json:"parent_activity_id,omitempty"`
	Actor             *protocol.ActorRef    `json:"actor,omitempty"`
	Kind              string                `json:"kind"`
	Purpose           string                `json:"purpose"`
	PurposeActor      protocol.ActorRef     `json:"purpose_actor"`
	Source            string                `json:"source"`
	State             State                 `json:"state"`
	InputEvidenceIDs  []protocol.EvidenceID `json:"input_evidence_ids,omitempty"`
	OutputEvidenceIDs []protocol.EvidenceID `json:"output_evidence_ids,omitempty"`
	Decision          *DecisionRef          `json:"decision,omitempty"`
	RequestedProfile  string                `json:"requested_profile"`
	EffectiveProfile  string                `json:"effective_profile"`
	PlannedAt         time.Time             `json:"planned_at"`
	AuthorizedAt      *time.Time            `json:"authorized_at,omitempty"`
	StartedAt         *time.Time            `json:"started_at,omitempty"`
	TerminalAt        *time.Time            `json:"terminal_at,omitempty"`
	ErrorCode         string                `json:"error_code,omitempty"`
	Reason            string                `json:"reason,omitempty"`
	UnknownEffects    []protocol.SubjectRef `json:"unknown_effects,omitempty"`
	Children          []ChildSummary        `json:"children,omitempty"`
}

type Projection struct {
	Journal    protocol.JournalRef            `json:"journal"`
	Activities map[protocol.ActivityID]Record `json:"activities"`
	Order      []protocol.ActivityID          `json:"order"`
}

type Projector struct{}

func (Projector) Version() uint32 { return projectionVersion }

func (Projector) Zero(ref protocol.JournalRef) Projection {
	return Projection{Journal: ref, Activities: make(map[protocol.ActivityID]Record)}
}

func (Projector) Snapshot(state Projection) Projection { return protocol.DeepCopy(state) }

func (Projector) Apply(current Projection, event protocol.EventRecord) (Projection, error) {
	if err := projection.ValidateFoundationEvent(event); err != nil {
		return current, err
	}
	next := protocol.DeepCopy(current)
	if next.Activities == nil {
		next.Activities = make(map[protocol.ActivityID]Record)
	}
	switch event.Envelope.Kind {
	case protocol.EventActivityPlanned:
		payload, ok := event.Decoded.(*protocol.ActivityPlannedV1)
		if !ok || event.Envelope.ActivityID == "" {
			return current, fmt.Errorf("invalid planned activity")
		}
		if _, exists := next.Activities[event.Envelope.ActivityID]; exists {
			return current, fmt.Errorf("activity %q is already planned", event.Envelope.ActivityID)
		}
		if parent := event.Envelope.ParentActivityID; parent != "" {
			if _, exists := next.Activities[parent]; !exists {
				return current, fmt.Errorf("parent activity %q is unknown", parent)
			}
		}
		record := Record{
			ActivityID: event.Envelope.ActivityID, TaskID: event.Envelope.TaskID, TurnID: event.Envelope.TurnID,
			ParentActivityID: event.Envelope.ParentActivityID, Actor: protocol.DeepCopy(event.Envelope.Actor),
			Kind: payload.Kind, Purpose: payload.Purpose, PurposeActor: payload.PurposeActor, Source: payload.Source,
			State: StatePlanned, InputEvidenceIDs: append([]protocol.EvidenceID(nil), payload.InputEvidenceIDs...),
			RequestedProfile: payload.RequestedProfile, EffectiveProfile: payload.EffectiveProfile, PlannedAt: event.Envelope.Time,
		}
		next.Activities[record.ActivityID] = record
		next.Order = append(next.Order, record.ActivityID)
		updateParent(next.Activities, record)
		return next, nil
	case protocol.EventActivityAuthorized:
		payload, ok := event.Decoded.(*protocol.ActivityAuthorizedV1)
		if !ok {
			return current, fmt.Errorf("invalid authorized activity")
		}
		return transition(next, current, event, StateAuthorized, func(record *Record) {
			record.AuthorizedAt = timePointer(event.Envelope.Time)
			record.Decision = &DecisionRef{
				DecisionNonce: payload.DecisionNonce, DecisionEventID: payload.DecisionEventID,
				PlanDigest: payload.PlanDigest, RequestDigest: payload.RequestDigest, DispatchDigest: payload.DispatchDigest,
			}
		})
	case protocol.EventActivityStarted:
		payload, ok := event.Decoded.(*protocol.ActivityStartedV1)
		if !ok || payload.ActivityID != event.Envelope.ActivityID {
			return current, fmt.Errorf("invalid started activity")
		}
		record, exists := next.Activities[event.Envelope.ActivityID]
		if !exists {
			return current, fmt.Errorf("activity %q is unknown", event.Envelope.ActivityID)
		}
		if record.State != StateAuthorized {
			return current, fmt.Errorf("invalid activity transition %q -> %q", record.State, StateStarted)
		}
		if record.Decision == nil || record.Decision.DecisionNonce != payload.DecisionNonce ||
			record.Decision.DecisionEventID != payload.DecisionEventID || record.Decision.PlanDigest != payload.PlanDigest ||
			record.Decision.RequestDigest != payload.RequestDigest || record.Decision.DispatchDigest != payload.DispatchDigest {
			return current, fmt.Errorf("activity start authorization binding mismatch")
		}
		return transition(next, current, event, StateStarted, func(record *Record) { record.StartedAt = timePointer(event.Envelope.Time) })
	case protocol.EventActivitySucceeded, protocol.EventActivityFailed, protocol.EventActivityDenied,
		protocol.EventActivityCancelled, protocol.EventActivityInterruptedNoEffect, protocol.EventActivityUncertain:
		payload, ok := event.Decoded.(*protocol.ActivityOutcomeV1)
		if !ok {
			return current, fmt.Errorf("invalid activity outcome")
		}
		to := stateForKind(event.Envelope.Kind)
		if payload.Status != string(to) {
			return current, fmt.Errorf("activity outcome status mismatch")
		}
		return transition(next, current, event, to, func(record *Record) {
			record.TerminalAt = timePointer(event.Envelope.Time)
			record.Reason, record.ErrorCode = payload.Reason, payload.ErrorCode
			record.OutputEvidenceIDs = append([]protocol.EvidenceID(nil), payload.OutputEvidenceIDs...)
			record.UnknownEffects = protocol.DeepCopy(payload.UnknownEffects)
		})
	case protocol.EventEvidenceLinked:
		payload, ok := event.Decoded.(*protocol.EvidenceLinkedV1)
		if !ok || event.Envelope.ActivityID == "" {
			return current, fmt.Errorf("invalid activity evidence link")
		}
		record, exists := next.Activities[event.Envelope.ActivityID]
		if !exists {
			return current, fmt.Errorf("activity %q is unknown", event.Envelope.ActivityID)
		}
		switch payload.Relation {
		case "input":
			record.InputEvidenceIDs = appendUniqueEvidence(record.InputEvidenceIDs, payload.EvidenceID)
		case "output":
			record.OutputEvidenceIDs = appendUniqueEvidence(record.OutputEvidenceIDs, payload.EvidenceID)
		default:
			return current, fmt.Errorf("invalid activity evidence relation %q", payload.Relation)
		}
		next.Activities[record.ActivityID] = record
		return next, nil
	default:
		return next, nil
	}
}

func transition(next, original Projection, event protocol.EventRecord, to State, mutate func(*Record)) (Projection, error) {
	record, exists := next.Activities[event.Envelope.ActivityID]
	if !exists {
		return original, fmt.Errorf("activity %q is unknown", event.Envelope.ActivityID)
	}
	if !allowed(record.State, to) {
		return original, fmt.Errorf("invalid activity transition %q -> %q", record.State, to)
	}
	record.State = to
	mutate(&record)
	next.Activities[record.ActivityID] = record
	updateParent(next.Activities, record)
	return next, nil
}

func allowed(from, to State) bool {
	switch from {
	case StatePlanned:
		return to == StateDenied || to == StateCancelled || to == StateAuthorized
	case StateAuthorized:
		return to == StateCancelled || to == StateStarted
	case StateStarted:
		return to == StateSucceeded || to == StateFailed || to == StateInterruptedNoEffect || to == StateUncertain
	default:
		return false
	}
}

func stateForKind(kind string) State {
	return map[string]State{
		protocol.EventActivitySucceeded: StateSucceeded, protocol.EventActivityFailed: StateFailed,
		protocol.EventActivityDenied: StateDenied, protocol.EventActivityCancelled: StateCancelled,
		protocol.EventActivityInterruptedNoEffect: StateInterruptedNoEffect, protocol.EventActivityUncertain: StateUncertain,
	}[kind]
}

func updateParent(activities map[protocol.ActivityID]Record, child Record) {
	if child.ParentActivityID == "" {
		return
	}
	parent := activities[child.ParentActivityID]
	summary := ChildSummary{ActivityID: child.ActivityID, Kind: child.Kind, State: child.State}
	for index := range parent.Children {
		if parent.Children[index].ActivityID == child.ActivityID {
			parent.Children[index] = summary
			activities[parent.ActivityID] = parent
			return
		}
	}
	parent.Children = append(parent.Children, summary)
	activities[parent.ActivityID] = parent
}

func appendUniqueEvidence(values []protocol.EvidenceID, value protocol.EvidenceID) []protocol.EvidenceID {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func timePointer(value time.Time) *time.Time { copy := value; return &copy }
