package protocol

import (
	"fmt"
	"strings"
	"time"
)

const (
	MaxSubagentTaskBytes           = 32 << 10
	MaxSubagentExpectedOutputBytes = 16 << 10
	MaxSubagentContextBytes        = 64 << 10
	MaxSubagentReceiptSummaryBytes = 128 << 10
	MaxSubagentAttemptsPerTurn     = 4
)

func (id DelegationAttemptID) Validate() error {
	if id == "" {
		return fmt.Errorf("delegation attempt ID is required")
	}
	return nil
}

type SubagentCallV1 struct {
	Task           string `json:"task"`
	ExpectedOutput string `json:"expected_output,omitempty"`
	Context        string `json:"context,omitempty"`
}

func (v SubagentCallV1) Validate() error {
	if err := ValidateBounds(v); err != nil {
		return err
	}
	if strings.TrimSpace(v.Task) == "" || len(v.Task) > MaxSubagentTaskBytes || len(v.ExpectedOutput) > MaxSubagentExpectedOutputBytes || len(v.Context) > MaxSubagentContextBytes || len(v.Task)+len(v.ExpectedOutput)+len(v.Context) > MaxCommandBytes {
		return fmt.Errorf("invalid subagent call")
	}
	return nil
}

type SubagentManifestV1 struct {
	AttemptID            DelegationAttemptID `json:"attempt_id"`
	ParentSessionID      SessionID           `json:"parent_session_id"`
	ParentCursor         CommittedCursor     `json:"parent_cursor"`
	ChildSessionID       SessionID           `json:"child_session_id"`
	ChildTaskID          TaskID              `json:"child_task_id"`
	ChildTurnID          TurnID              `json:"child_turn_id"`
	RuntimeGenerationID  RuntimeGenerationID `json:"runtime_generation_id"`
	SkillCatalogRevision string              `json:"skill_catalog_revision"`
	MaxToolCalls         int                 `json:"max_tool_calls"`
	Deadline             time.Time           `json:"deadline"`
}

func (v SubagentManifestV1) Validate() error {
	if err := ValidateBounds(v); err != nil {
		return err
	}
	if v.AttemptID.Validate() != nil || v.ParentSessionID == "" || v.ChildSessionID == "" || v.ChildSessionID == v.ParentSessionID || v.ChildTaskID == "" || v.ChildTurnID == "" || v.RuntimeGenerationID == "" || v.SkillCatalogRevision == "" || v.MaxToolCalls < 1 || v.MaxToolCalls > 64 || v.Deadline.IsZero() || v.ParentCursor.Validate() != nil || v.ParentCursor.JournalKind != JournalSession || v.ParentCursor.JournalID != JournalID(v.ParentSessionID) {
		return fmt.Errorf("invalid subagent manifest")
	}
	return nil
}

type SubagentRequestedV1 struct {
	Call     SubagentCallV1     `json:"call"`
	Manifest SubagentManifestV1 `json:"manifest"`
}

func (v SubagentRequestedV1) Validate() error {
	if err := v.Call.Validate(); err != nil {
		return err
	}
	return v.Manifest.Validate()
}

type SubagentWaitingV1 struct {
	AttemptID      DelegationAttemptID `json:"attempt_id"`
	ChildSessionID SessionID           `json:"child_session_id"`
}

func (v SubagentWaitingV1) Validate() error {
	if err := ValidateBounds(v); err != nil {
		return err
	}
	if v.AttemptID.Validate() != nil || v.ChildSessionID == "" {
		return fmt.Errorf("invalid subagent waiting state")
	}
	return nil
}

type SubagentReceiptV1 struct {
	Status           string             `json:"status"`
	Summary          string             `json:"summary"`
	Manifest         SubagentManifestV1 `json:"manifest"`
	TerminalCursor   CommittedCursor    `json:"terminal_cursor"`
	ChangedFiles     []string           `json:"changed_files"`
	CommandsAndTests []string           `json:"commands_and_tests"`
	Usage            ModelUsage         `json:"usage"`
	EvidenceIDs      []EvidenceID       `json:"evidence_ids"`
	UnknownEffects   []ActivityID       `json:"unknown_effects"`
	Error            *PublicError       `json:"error,omitempty"`
}

func (v SubagentReceiptV1) Validate() error {
	if err := ValidateBounds(v); err != nil {
		return err
	}
	if !subagentReceiptStatus(v.Status) || len(v.Summary) > MaxSubagentReceiptSummaryBytes || v.Manifest.Validate() != nil || v.TerminalCursor.Validate() != nil || v.TerminalCursor.JournalKind != JournalSession || v.TerminalCursor.JournalID != JournalID(v.Manifest.ChildSessionID) || v.Usage.Validate() != nil {
		return fmt.Errorf("invalid subagent receipt")
	}
	if v.Error != nil && (strings.TrimSpace(v.Error.Code) == "" || strings.TrimSpace(v.Error.Message) == "") {
		return fmt.Errorf("invalid subagent receipt error")
	}
	if err := validateSortedStrings(v.ChangedFiles, "changed files"); err != nil {
		return err
	}
	if err := validateSortedStrings(v.CommandsAndTests, "commands and tests"); err != nil {
		return err
	}
	if err := validateSortedEvidenceIDs(v.EvidenceIDs); err != nil {
		return err
	}
	return validateSortedActivityIDs(v.UnknownEffects)
}

type SubagentResultAttachedV1 struct {
	AttemptID         DelegationAttemptID `json:"attempt_id"`
	ChildSessionID    SessionID           `json:"child_session_id"`
	TerminalCursor    CommittedCursor     `json:"terminal_cursor"`
	ReceiptDigest     Digest              `json:"receipt_digest"`
	ReceiptEvidenceID EvidenceID          `json:"receipt_evidence_id"`
}

func (v SubagentResultAttachedV1) Validate() error {
	if err := ValidateBounds(v); err != nil {
		return err
	}
	if v.AttemptID.Validate() != nil || v.ChildSessionID == "" || v.ReceiptEvidenceID == "" || v.TerminalCursor.Validate() != nil || v.TerminalCursor.JournalKind != JournalSession || v.TerminalCursor.JournalID != JournalID(v.ChildSessionID) || v.ReceiptDigest.Validate() != nil {
		return fmt.Errorf("invalid subagent result attachment")
	}
	return nil
}

func subagentReceiptStatus(status string) bool {
	switch status {
	case "succeeded", "failed", "cancelled", "uncertain":
		return true
	default:
		return false
	}
}

func validateSortedStrings(values []string, label string) error {
	previous := ""
	for index, value := range values {
		if value == "" || (index > 0 && value <= previous) {
			return fmt.Errorf("%s must be nonempty, sorted, and unique", label)
		}
		previous = value
	}
	return nil
}

func validateSortedEvidenceIDs(values []EvidenceID) error {
	previous := EvidenceID("")
	for index, value := range values {
		if value == "" || (index > 0 && value <= previous) {
			return fmt.Errorf("evidence IDs must be nonempty, sorted, and unique")
		}
		previous = value
	}
	return nil
}

func validateSortedActivityIDs(values []ActivityID) error {
	previous := ActivityID("")
	for index, value := range values {
		if value == "" || (index > 0 && value <= previous) {
			return fmt.Errorf("unknown effects must be nonempty, sorted, and unique")
		}
		previous = value
	}
	return nil
}
