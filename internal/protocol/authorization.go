package protocol

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

const (
	AuthorizationLifetimeOnce    = "once"
	AuthorizationLifetimeSession = "session"
)

type AuthorizationConstraint struct {
	Name     string          `json:"name"`
	Operator string          `json:"operator"`
	Value    json.RawMessage `json:"value"`
}

type PolicyProvenance struct {
	Source     string `json:"source"`
	Revision   string `json:"revision"`
	Generation string `json:"generation"`
	HardDeny   bool   `json:"hard_deny"`
}

type AuthorizationRequest struct {
	RequestID            string              `json:"request_id"`
	Principal            ActorRef            `json:"principal"`
	Actor                ActorRef            `json:"actor"`
	SessionID            SessionID           `json:"session_id,omitempty"`
	ControlOperationID   ControlOperationID  `json:"control_operation_id,omitempty"`
	TaskID               TaskID              `json:"task_id,omitempty"`
	TurnID               TurnID              `json:"turn_id,omitempty"`
	ActivityID           ActivityID          `json:"activity_id"`
	ParentActivityID     ActivityID          `json:"parent_activity_id,omitempty"`
	CallID               string              `json:"call_id"`
	QueueID              string              `json:"queue_id"`
	Source               ToolIdentity        `json:"source"`
	SourceRevision       string              `json:"source_revision"`
	DescriptorDigest     Digest              `json:"descriptor_digest"`
	Action               string              `json:"action"`
	Resources            []ResourceTarget    `json:"resources"`
	ExecutionLocus       string              `json:"execution_locus"`
	RequestedProfile     string              `json:"requested_profile"`
	EffectiveProfile     string              `json:"effective_profile"`
	Effect               string              `json:"effect"`
	Boundary             string              `json:"boundary"`
	Reversibility        string              `json:"reversibility"`
	VerificationCoverage string              `json:"verification_coverage"`
	RuntimeGenerationID  RuntimeGenerationID `json:"runtime_generation_id"`
	PolicyGeneration     string              `json:"policy_generation"`
	PolicyProvenance     []PolicyProvenance  `json:"policy_provenance"`
	PlanDigest           Digest              `json:"plan_digest"`
	RequestDigest        Digest              `json:"request_digest"`
	DispatchDigest       Digest              `json:"dispatch_digest"`
}

func (r AuthorizationRequest) Validate() error {
	if err := ValidateBounds(r); err != nil {
		return fmt.Errorf("authorization request bounds: %w", err)
	}
	if (r.SessionID == "") == (r.ControlOperationID == "") {
		return fmt.Errorf("authorization request requires exactly one session or control operation")
	}
	if r.RequestID == "" || r.CallID == "" || r.QueueID == "" || r.SourceRevision == "" || r.Action == "" || r.ExecutionLocus == "" || r.RequestedProfile == "" || r.EffectiveProfile == "" || r.Effect == "" || r.Boundary == "" || r.Reversibility == "" || r.VerificationCoverage == "" || r.RuntimeGenerationID == "" || r.PolicyGeneration == "" {
		return fmt.Errorf("authorization request is incomplete")
	}
	if r.SessionID != "" && r.ActivityID == "" {
		return fmt.Errorf("session authorization requires activity ID")
	}
	if err := r.Principal.Validate(); err != nil {
		return err
	}
	if err := r.Actor.Validate(); err != nil {
		return err
	}
	if err := r.Source.Validate(); err != nil {
		return err
	}
	for _, digest := range []Digest{r.DescriptorDigest, r.PlanDigest, r.RequestDigest, r.DispatchDigest} {
		if err := digest.Validate(); err != nil {
			return err
		}
	}
	if len(r.PolicyProvenance) == 0 {
		return fmt.Errorf("authorization request requires policy provenance")
	}
	for _, provenance := range r.PolicyProvenance {
		if provenance.Source == "" || provenance.Revision == "" || provenance.Generation == "" {
			return fmt.Errorf("policy provenance is incomplete")
		}
	}
	return validateSortedUniqueResources(r.Resources)
}

type CanonicalAuthorizationScope struct {
	Capability  string                    `json:"capability"`
	Source      ToolIdentity              `json:"source"`
	Resources   []ResourceTarget          `json:"resources"`
	Constraints []AuthorizationConstraint `json:"constraints"`
}

type AuthorizationDecision struct {
	Request          AuthorizationRequest        `json:"request"`
	Action           string                      `json:"action"`
	Scope            CanonicalAuthorizationScope `json:"scope"`
	Constraints      []AuthorizationConstraint   `json:"constraints"`
	Lifetime         string                      `json:"lifetime"`
	PolicySource     string                      `json:"policy_source"`
	PolicyGeneration string                      `json:"policy_generation"`
	Reason           string                      `json:"reason"`
	ResolvedActor    *ActorRef                   `json:"resolved_actor,omitempty"`
	DecidedAt        time.Time                   `json:"decided_at"`
	ExpiresAt        *time.Time                  `json:"expires_at,omitempty"`
	PlanDigest       Digest                      `json:"plan_digest"`
	DecisionNonce    DecisionNonce               `json:"decision_nonce"`
}

func (d AuthorizationDecision) Validate() error {
	if err := ValidateBounds(d); err != nil {
		return fmt.Errorf("authorization decision bounds: %w", err)
	}
	if err := d.Request.Validate(); err != nil {
		return err
	}
	if d.Action != "allow" && d.Action != "deny" && d.Action != "ask" {
		return fmt.Errorf("invalid authorization action %q", d.Action)
	}
	if d.Lifetime != AuthorizationLifetimeOnce && d.Lifetime != AuthorizationLifetimeSession {
		return fmt.Errorf("invalid authorization lifetime %q", d.Lifetime)
	}
	if d.PolicySource == "" || d.PolicyGeneration == "" || d.Reason == "" || d.DecidedAt.IsZero() || d.DecisionNonce == "" {
		return fmt.Errorf("authorization decision is incomplete")
	}
	if d.ExpiresAt != nil && !d.ExpiresAt.After(d.DecidedAt) {
		return fmt.Errorf("authorization decision expiry is not after decision")
	}
	if d.ResolvedActor != nil {
		if err := d.ResolvedActor.Validate(); err != nil {
			return err
		}
	}
	if err := d.PlanDigest.Validate(); err != nil {
		return err
	}
	if d.PolicyGeneration != d.Request.PolicyGeneration {
		return fmt.Errorf("authorization policy generation binding mismatch")
	}
	if d.PlanDigest != d.Request.PlanDigest {
		return fmt.Errorf("authorization plan digest binding mismatch")
	}
	if d.Scope.Capability == "" || d.Scope.Capability != d.Request.Action {
		return fmt.Errorf("authorization scope capability binding mismatch")
	}
	if err := d.Scope.Source.Validate(); err != nil {
		return err
	}
	if d.Scope.Source != d.Request.Source {
		return fmt.Errorf("authorization scope source binding mismatch")
	}
	if err := validateSortedUniqueResources(d.Scope.Resources); err != nil {
		return err
	}
	if !reflect.DeepEqual(d.Scope.Resources, d.Request.Resources) {
		return fmt.Errorf("authorization scope resource binding mismatch")
	}
	if err := validateAuthorizationConstraints(d.Scope.Constraints); err != nil {
		return err
	}
	if err := validateAuthorizationConstraints(d.Constraints); err != nil {
		return err
	}
	if !reflect.DeepEqual(d.Scope.Constraints, d.Constraints) {
		return fmt.Errorf("authorization constraint binding mismatch")
	}
	return nil
}

type ApprovalResponse struct {
	RequestID   string   `json:"request_id"`
	Action      string   `json:"action"`
	Lifetime    string   `json:"lifetime"`
	ScopeDigest Digest   `json:"scope_digest"`
	Actor       ActorRef `json:"actor"`
	Reason      string   `json:"reason"`
}

func validateAuthorizationConstraints(constraints []AuthorizationConstraint) error {
	previous := ""
	for index, constraint := range constraints {
		if constraint.Name == "" || constraint.Operator == "" {
			return fmt.Errorf("authorization constraint is incomplete")
		}
		if err := ValidateRawJSON(constraint.Value); err != nil {
			return fmt.Errorf("authorization constraint %q: %w", constraint.Name, err)
		}
		key := constraint.Name + "\x00" + constraint.Operator
		if index > 0 && key <= previous {
			return fmt.Errorf("authorization constraints must be sorted and unique")
		}
		previous = key
	}
	return nil
}
