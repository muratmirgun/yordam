package protocol

import (
	"fmt"
	"time"
)

type SubjectRef struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

func (s SubjectRef) Validate() error {
	if s.Kind == "" || s.ID == "" {
		return fmt.Errorf("subject reference is incomplete")
	}
	return nil
}

type ContentAvailability string

const (
	ContentAvailable      ContentAvailability = "available"
	ContentWithheldSecret ContentAvailability = "withheld_secret"
	ContentMissing        ContentAvailability = "missing"
	ContentCorrupt        ContentAvailability = "corrupt"
)

type BlobRef struct {
	Digest Digest `json:"digest"`
}

type EvidenceRecordBody struct {
	ID                    EvidenceID          `json:"evidence_id"`
	Kind                  string              `json:"kind"`
	WorkspaceID           WorkspaceID         `json:"workspace_id"`
	SessionID             SessionID           `json:"session_id,omitempty"`
	Availability          ContentAvailability `json:"availability"`
	Blob                  *BlobRef            `json:"blob,omitempty"`
	MediaType             string              `json:"media_type"`
	Size                  int64               `json:"size"`
	ProducingActivityID   ActivityID          `json:"producing_activity_id"`
	Actor                 ActorRef            `json:"actor"`
	Subject               SubjectRef          `json:"subject"`
	CreatedAt             time.Time           `json:"created_at"`
	Redacted              bool                `json:"redacted"`
	Truncated             bool                `json:"truncated"`
	OriginalSize          int64               `json:"original_size,omitempty"`
	LegacyArtifactAliases []string            `json:"legacy_artifact_aliases,omitempty"`
}

func (b EvidenceRecordBody) Validate() error {
	if err := ValidateBounds(b); err != nil {
		return err
	}
	if b.ID == "" || b.Kind == "" || b.WorkspaceID == "" || b.MediaType == "" || b.Size < 0 || b.ProducingActivityID == "" || b.CreatedAt.IsZero() {
		return fmt.Errorf("evidence record body is incomplete")
	}
	if err := b.Actor.Validate(); err != nil {
		return err
	}
	if err := b.Subject.Validate(); err != nil {
		return err
	}
	switch b.Availability {
	case ContentAvailable:
		if b.Blob == nil || b.Redacted {
			return fmt.Errorf("available evidence requires a blob and must not be redacted")
		}
		if err := b.Blob.Digest.Validate(); err != nil {
			return err
		}
	case ContentWithheldSecret:
		if b.Blob != nil || !b.Redacted {
			return fmt.Errorf("withheld evidence must be redacted and have no blob")
		}
	case ContentMissing, ContentCorrupt:
		if b.Blob != nil {
			return fmt.Errorf("unavailable evidence must not have a blob")
		}
	default:
		return fmt.Errorf("invalid content availability %q", b.Availability)
	}
	if b.Truncated {
		if b.OriginalSize <= b.Size {
			return fmt.Errorf("truncated evidence requires larger original size")
		}
	} else if b.OriginalSize != 0 {
		return fmt.Errorf("untruncated evidence must not have original size")
	}
	return validateSortedUniqueStrings(b.LegacyArtifactAliases, "legacy artifact aliases")
}

type EvidenceRecord struct {
	Body   EvidenceRecordBody `json:"body"`
	Digest Digest             `json:"digest"`
}

func (r EvidenceRecord) Validate() error {
	if err := r.Body.Validate(); err != nil {
		return err
	}
	return r.Digest.Validate()
}

type EvidenceCandidate struct {
	ID                  EvidenceID
	Kind                string
	WorkspaceID         WorkspaceID
	SessionID           SessionID
	MediaType           string
	ProducingActivityID ActivityID
	Actor               ActorRef
	Subject             SubjectRef
	Content             []byte
	Limit               int64
}

type CheckpointCoverage struct {
	Subject            SubjectRef         `json:"subject"`
	Class              string             `json:"class"`
	EvidenceIDs        []EvidenceID       `json:"evidence_ids"`
	RecoveryMaterialID RecoveryMaterialID `json:"recovery_material_id,omitempty"`
}

type CheckpointBody struct {
	ID                  CheckpointID         `json:"checkpoint_id"`
	SessionID           SessionID            `json:"session_id"`
	TaskID              TaskID               `json:"task_id"`
	TurnID              TurnID               `json:"turn_id"`
	EventHead           CommittedCursor      `json:"event_head"`
	ContextPlanDigest   Digest               `json:"context_plan_digest"`
	OutcomeContractID   OutcomeContractID    `json:"outcome_contract_id"`
	ContractVersion     uint32               `json:"contract_version"`
	RuntimeGenerationID RuntimeGenerationID  `json:"runtime_generation_id"`
	PlanDigest          Digest               `json:"plan_digest"`
	Coverage            []CheckpointCoverage `json:"coverage"`
	CreatedAt           time.Time            `json:"created_at"`
}

type VerificationReceiptBody struct {
	ID                     ReceiptID         `json:"receipt_id"`
	TaskID                 TaskID            `json:"task_id"`
	OutcomeContractID      OutcomeContractID `json:"outcome_contract_id"`
	ContractVersion        uint32            `json:"contract_version"`
	CriterionID            string            `json:"criterion_id"`
	ActivityID             ActivityID        `json:"activity_id"`
	Subject                SubjectRef        `json:"subject"`
	VerifierID             string            `json:"verifier_id"`
	VerifierVersion        string            `json:"verifier_version"`
	RequestedProfile       string            `json:"requested_profile"`
	EffectiveProfile       string            `json:"effective_profile"`
	StartedAt              time.Time         `json:"started_at"`
	TerminalAt             time.Time         `json:"terminal_at"`
	Status                 string            `json:"status"`
	Coverage               string            `json:"coverage"`
	EvidenceIDs            []EvidenceID      `json:"evidence_ids"`
	UnsupportedConclusions []string          `json:"unsupported_conclusions"`
}

type VerificationReceipt struct {
	Body   VerificationReceiptBody `json:"body"`
	Digest Digest                  `json:"digest"`
}

type RecoveryMaterialBody struct {
	PlanDigest              Digest    `json:"plan_digest"`
	PreimageDigest          Digest    `json:"preimage_digest"`
	ExpectedPostimageDigest Digest    `json:"expected_postimage_digest"`
	Mode                    uint32    `json:"mode"`
	Sealed                  bool      `json:"sealed"`
	CreatedAt               time.Time `json:"created_at"`
}

type RecoveryMaterialRecord struct {
	ID             RecoveryMaterialID   `json:"id"`
	WorkspaceID    WorkspaceID          `json:"workspace_id"`
	ActivityID     ActivityID           `json:"activity_id"`
	CheckpointID   CheckpointID         `json:"checkpoint_id"`
	Subject        SubjectRef           `json:"subject"`
	Body           RecoveryMaterialBody `json:"body"`
	MaterialDigest Digest               `json:"material_digest"`
}

func validateSortedUniqueEvidenceIDs(values []EvidenceID) error {
	previous := EvidenceID("")
	for index, value := range values {
		if value == "" || (index > 0 && value <= previous) {
			return fmt.Errorf("evidence IDs must be nonempty, sorted, and unique")
		}
		previous = value
	}
	return nil
}

func validateSortedUniqueStrings(values []string, label string) error {
	previous := ""
	for index, value := range values {
		if value == "" || (index > 0 && value <= previous) {
			return fmt.Errorf("%s must be nonempty, sorted, and unique", label)
		}
		previous = value
	}
	return nil
}
