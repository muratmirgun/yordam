package eventcodec

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func FoundationDescriptors() []Descriptor {
	type foundationEntry struct {
		kind       string
		newPayload func() any
		auth       bool
		redaction  string
		domains    []string
	}
	entries := []foundationEntry{
		{protocol.EventSessionCreated, func() any { return new(protocol.SessionCreatedV1) }, false, "public", []string{"session", "workspace"}},
		{protocol.EventSessionForked, func() any { return new(protocol.SessionForkedV1) }, false, "public", []string{"session", "lineage"}},
		{protocol.EventSessionTitleChanged, func() any { return new(protocol.SessionTitleChangedV1) }, false, "public", []string{"session"}},
		{protocol.EventSessionLifecycleChanged, func() any { return new(protocol.StateChangedV1) }, false, "public", []string{"session"}},
		{protocol.EventModeChanged, func() any { return new(protocol.ModeChangedV1) }, true, "public", []string{"session", "permissions"}},
		{protocol.EventModelChanged, func() any { return new(protocol.ModelChangedV1) }, false, "public", []string{"session", "provider"}},
		{protocol.EventTrustedExecutionAcknowledged, func() any { return new(protocol.TrustedExecutionAcknowledgedV1) }, true, "public", []string{"session", "permissions"}},
		{protocol.EventUserMessage, func() any { return new(protocol.UserMessageV1) }, false, "sensitive", []string{"session", "context"}},
		{protocol.EventAssistantMessage, func() any { return new(protocol.AssistantMessageV1) }, false, "sensitive", []string{"session", "context"}},
		{protocol.EventContextCompacted, func() any { return new(protocol.ContextCompactedV1) }, false, "sensitive", []string{"context", "evidence"}},
		{protocol.EventFileChangePlanned, func() any { return new(protocol.FileChangePlannedV1) }, true, "sensitive", []string{"activity", "evidence"}},
		{protocol.EventFileChanged, func() any { return new(protocol.FileChangedV1) }, true, "sensitive", []string{"activity", "evidence"}},
		{protocol.EventTaskCreated, func() any { return new(protocol.TaskCreatedV1) }, false, "public", []string{"task"}},
		{protocol.EventTaskStatusChanged, func() any { return new(protocol.TaskStatusChangedV1) }, false, "public", []string{"task"}},
		{protocol.EventOutcomeContractDeclared, func() any { return new(protocol.OutcomeContractDeclaredV1) }, false, "public", []string{"task", "outcome"}},
		{protocol.EventOutcomeContractAmended, func() any { return new(protocol.OutcomeContractAmendedV1) }, false, "public", []string{"task", "outcome"}},
		{protocol.EventOutcomeCriterionAssessed, func() any { return new(protocol.CriterionAssessedV1) }, false, "public", []string{"outcome", "evidence"}},
		{protocol.EventOutcomeFinalAssessed, func() any { return new(protocol.OutcomeFinalAssessedV1) }, false, "public", []string{"outcome"}},
		{protocol.EventTurnAccepted, func() any { return new(protocol.TurnAcceptedV1) }, false, "public", []string{"task", "turn"}},
		{protocol.EventTurnStateChanged, func() any { return new(protocol.TurnStateChangedV1) }, false, "public", []string{"turn"}},
		{protocol.EventTurnCompleted, func() any { return new(protocol.TurnTerminalV1) }, false, "public", []string{"turn"}},
		{protocol.EventTurnFailed, func() any { return new(protocol.TurnTerminalV1) }, false, "public", []string{"turn"}},
		{protocol.EventTurnInterrupted, func() any { return new(protocol.TurnTerminalV1) }, false, "public", []string{"turn"}},
		{protocol.EventActivityPlanned, func() any { return new(protocol.ActivityPlannedV1) }, true, "sensitive", []string{"activity"}},
		{protocol.EventActivityAuthorized, func() any { return new(protocol.ActivityAuthorizedV1) }, true, "sensitive", []string{"activity", "permissions"}},
		{protocol.EventActivityStarted, func() any { return new(protocol.ActivityStartedV1) }, true, "sensitive", []string{"activity"}},
		{protocol.EventActivityProgress, func() any { return new(protocol.ActivityProgressV1) }, false, "sensitive", []string{"activity"}},
		{protocol.EventActivitySucceeded, func() any { return new(protocol.ActivityOutcomeV1) }, true, "sensitive", []string{"activity", "evidence"}},
		{protocol.EventActivityFailed, func() any { return new(protocol.ActivityOutcomeV1) }, true, "sensitive", []string{"activity", "evidence"}},
		{protocol.EventActivityDenied, func() any { return new(protocol.ActivityOutcomeV1) }, true, "sensitive", []string{"activity", "permissions"}},
		{protocol.EventActivityCancelled, func() any { return new(protocol.ActivityOutcomeV1) }, true, "sensitive", []string{"activity"}},
		{protocol.EventActivityInterruptedNoEffect, func() any { return new(protocol.ActivityOutcomeV1) }, true, "sensitive", []string{"activity"}},
		{protocol.EventActivityUncertain, func() any { return new(protocol.ActivityOutcomeV1) }, true, "sensitive", []string{"activity", "recovery"}},
		{protocol.EventProviderCapabilityDecided, func() any { return new(protocol.ProviderCapabilityDecidedV1) }, false, "public", []string{"provider"}},
		{protocol.EventProviderAttemptTerminal, func() any { return new(protocol.ProviderAttemptTerminalV1) }, false, "sensitive", []string{"provider", "usage"}},
		{protocol.EventExecutionPlanDeclared, func() any { return new(protocol.ExecutionPlanDeclaredV1) }, true, "sensitive", []string{"activity"}},
		{protocol.EventAuthorizationRequested, func() any { return new(protocol.AuthorizationRequestedV1) }, true, "sensitive", []string{"permissions"}},
		{protocol.EventAuthorizationDecided, func() any { return new(protocol.AuthorizationDecidedV1) }, true, "sensitive", []string{"permissions"}},
		{protocol.EventAuthorizationDecisionConsumed, func() any { return new(protocol.AuthorizationDecisionConsumedV1) }, true, "sensitive", []string{"permissions", "activity"}},
		{protocol.EventAuthorizationGrantRevoked, func() any { return new(protocol.AuthorizationGrantRevokedV1) }, true, "sensitive", []string{"permissions"}},
		{protocol.EventEvidenceRecorded, func() any { return new(protocol.EvidenceRecordedV1) }, false, "sensitive", []string{"evidence"}},
		{protocol.EventEvidenceLinked, func() any { return new(protocol.EvidenceLinkedV1) }, false, "sensitive", []string{"evidence"}},
		{protocol.EventCheckpointPlanned, func() any { return new(protocol.CheckpointPlannedV1) }, true, "sensitive", []string{"checkpoints"}},
		{protocol.EventCheckpointReady, func() any { return new(protocol.CheckpointReadyV1) }, true, "sensitive", []string{"checkpoints", "recovery"}},
		{protocol.EventCheckpointFailed, func() any { return new(protocol.CheckpointFailedV1) }, true, "sensitive", []string{"checkpoints"}},
		{protocol.EventVerificationReceiptRecorded, func() any { return new(protocol.VerificationReceiptRecordedV1) }, false, "sensitive", []string{"receipts", "outcome"}},
		{protocol.EventContextPlanRecorded, func() any { return new(protocol.ContextPlanRecordedV1) }, false, "sensitive", []string{"context"}},
		{protocol.EventContextUsageRecorded, func() any { return new(protocol.ContextUsageRecordedV1) }, false, "public", []string{"usage", "context"}},
		{protocol.EventRuntimeGenerationActivated, func() any { return new(protocol.RuntimeGenerationActivatedV1) }, true, "sensitive", []string{"runtime"}},
		{protocol.EventProjectSkillTrustChanged, func() any { return new(protocol.ProjectSkillTrustChangedV1) }, true, "sensitive", []string{"skills", "permissions"}},
		{protocol.EventControlOperationPlanned, func() any { return new(protocol.ControlOperationPlannedV1) }, true, "sensitive", []string{"control"}},
		{protocol.EventControlOperationAuthorized, func() any { return new(protocol.ControlOperationAuthorizedV1) }, true, "sensitive", []string{"control", "permissions"}},
		{protocol.EventControlOperationStarted, func() any { return new(protocol.ControlOperationStartedV1) }, true, "sensitive", []string{"control"}},
		{protocol.EventControlOperationCompleted, func() any { return new(protocol.ControlOperationTerminalV1) }, true, "sensitive", []string{"control"}},
		{protocol.EventControlOperationFailed, func() any { return new(protocol.ControlOperationTerminalV1) }, true, "sensitive", []string{"control"}},
		{protocol.EventControlOperationInterrupted, func() any { return new(protocol.ControlOperationTerminalV1) }, true, "sensitive", []string{"control"}},
		{protocol.EventCommandAccepted, func() any { return new(protocol.CommandAcceptedV1) }, false, "public", []string{"commands"}},
		{protocol.EventCommandCompleted, func() any { return new(protocol.CommandCompletedV1) }, false, "sensitive", []string{"commands"}},
		{protocol.EventMigrationCompatibilityDeclared, func() any { return new(protocol.MigrationCompatibilityDeclaredV1) }, false, "diagnostic", []string{"migration"}},
		{protocol.EventMigrationDiagnostic, func() any { return new(protocol.DiagnosticV1) }, false, "diagnostic", []string{"diagnostics"}},
		{protocol.EventRecoveryDiagnostic, func() any { return new(protocol.DiagnosticV1) }, false, "diagnostic", []string{"diagnostics", "recovery"}},
		{protocol.EventTransactionCommitted, func() any { return new(protocol.TransactionCommittedV1) }, false, "public", []string{"journal"}},
		{protocol.EventSubagentRequested, func() any { return new(protocol.SubagentRequestedV1) }, true, "sensitive", []string{"subagent", "turn"}},
		{protocol.EventSubagentWaiting, func() any { return new(protocol.SubagentWaitingV1) }, false, "public", []string{"subagent", "turn"}},
		{protocol.EventSubagentResultAttached, func() any { return new(protocol.SubagentResultAttachedV1) }, true, "sensitive", []string{"subagent", "turn", "evidence"}},
		{protocol.EventSubagentManifest, func() any { return new(protocol.SubagentManifestV1) }, false, "public", []string{"subagent", "session"}},
		{protocol.EventSubagentReceipt, func() any { return new(protocol.SubagentReceiptV1) }, false, "sensitive", []string{"subagent", "usage", "evidence"}},
	}

	descriptors := make([]Descriptor, 0, len(entries))
	for _, entry := range entries {
		kind := entry.kind
		descriptors = append(descriptors, Descriptor{
			Kind:               entry.kind,
			Version:            1,
			New:                entry.newPayload,
			ValidateStructural: validateFoundationStructural,
			ValidateSemantic: func(payload any) error {
				return validateFoundationSemantic(kind, payload)
			},
			ValidateEnvelope: func(envelope protocol.EventEnvelope, payload any) error {
				switch kind {
				case protocol.EventContextCompacted:
					return validateContextCompactionEnvelope(envelope, payload)
				case protocol.EventProjectSkillTrustChanged:
					trust, ok := payload.(*protocol.ProjectSkillTrustChangedV1)
					if !ok || envelope.JournalKind != protocol.JournalWorkspaceControl || envelope.JournalID != protocol.JournalID(trust.WorkspaceID) {
						return fmt.Errorf("project skill trust requires workspace-control journal")
					}
				case protocol.EventSubagentRequested:
					return validateSubagentRequestedEnvelope(envelope, payload)
				case protocol.EventSubagentWaiting:
					waiting, ok := payload.(*protocol.SubagentWaitingV1)
					if !ok {
						return fmt.Errorf("invalid subagent waiting payload")
					}
					return waiting.Validate()
				case protocol.EventSubagentResultAttached:
					attachment, ok := payload.(*protocol.SubagentResultAttachedV1)
					if !ok {
						return fmt.Errorf("invalid subagent attachment payload")
					}
					return attachment.Validate()
				case protocol.EventSubagentManifest:
					manifest, ok := payload.(*protocol.SubagentManifestV1)
					if !ok {
						return fmt.Errorf("invalid subagent manifest payload")
					}
					return validateSubagentChildEnvelope(envelope, *manifest)
				case protocol.EventSubagentReceipt:
					receipt, ok := payload.(*protocol.SubagentReceiptV1)
					if !ok {
						return fmt.Errorf("invalid subagent receipt payload")
					}
					if err := receipt.Validate(); err != nil {
						return err
					}
					if err := validateSubagentChildEnvelope(envelope, receipt.Manifest); err != nil {
						return err
					}
					if receipt.TerminalCursor.CommitSeq != envelope.Seq || receipt.TerminalCursor.TransactionID != envelope.TransactionID {
						return fmt.Errorf("subagent receipt terminal cursor does not match envelope")
					}
				}
				return nil
			},
			AuthorizationCritical: entry.auth,
			RedactionClass:        entry.redaction,
			ProjectionDomains:     append([]string(nil), entry.domains...),
		})
	}
	return descriptors
}

func validateFoundationStructural(payload any) error {
	if payload == nil || reflect.TypeOf(payload).Kind() != reflect.Pointer || reflect.ValueOf(payload).IsNil() {
		return fmt.Errorf("foundation payload must be a nonnil pointer")
	}
	_, err := canonicaljson.Marshal(payload)
	return err
}

func validateFoundationSemantic(kind string, payload any) error {
	switch value := payload.(type) {
	case *protocol.SessionCreatedV1:
		selectionIncomplete := (value.ProviderID == "") != (value.ModelID == "")
		if value.WorkspaceID == "" || value.CanonicalPath == "" || value.Title == "" || selectionIncomplete || !validMode(value.Mode) {
			return fmt.Errorf("session creation is incomplete")
		}
	case *protocol.SessionForkedV1:
		if value.ParentSessionID == "" || value.ParentCursor.Validate() != nil || value.CheckpointDigest.Validate() != nil {
			return fmt.Errorf("session fork is incomplete")
		}
	case *protocol.SessionTitleChangedV1:
		if value.Title == "" {
			return fmt.Errorf("session title is required")
		}
	case *protocol.StateChangedV1:
		if kind == protocol.EventTurnStateChanged {
			return value.ValidateTurn()
		}
		if value.From == "" || value.To == "" || value.From == value.To {
			return fmt.Errorf("state transition is invalid")
		}
	case *protocol.ModeChangedV1:
		if !validMode(value.Mode) {
			return fmt.Errorf("invalid mode %q", value.Mode)
		}
	case *protocol.ModelChangedV1:
		if value.ProviderID == "" || value.ModelID == "" {
			return fmt.Errorf("model selection is incomplete")
		}
	case *protocol.TrustedExecutionAcknowledgedV1:
		if value.Profile == "" {
			return fmt.Errorf("trusted execution profile is required")
		}
	case *protocol.UserMessageV1:
		if strings.TrimSpace(value.Content) == "" {
			return fmt.Errorf("user message is empty")
		}
	case *protocol.AssistantMessageV1:
		if len(value.Blocks) == 0 {
			return fmt.Errorf("assistant message requires content")
		}
		for _, block := range value.Blocks {
			if err := block.Validate(); err != nil {
				return err
			}
		}
		for _, intent := range value.ToolIntents {
			if err := intent.Validate(); err != nil {
				return err
			}
		}
	case *protocol.ContextCompactedV1:
		return value.Validate()
	case *protocol.ProjectSkillTrustChangedV1:
		return value.Validate()
	case *protocol.FileChangePlannedV1:
		return validateActionPlan(value.Plan)
	case *protocol.FileChangedV1:
		if value.CallID == "" || value.Subject.Validate() != nil || value.Before.Validate() != nil || value.After.Validate() != nil {
			return fmt.Errorf("file change is incomplete")
		}
		return sortedEvidenceIDs(value.EvidenceIDs)
	case *protocol.TaskCreatedV1:
		if strings.TrimSpace(value.Goal) == "" || value.OutcomeContractID == "" || value.ContractVersion == 0 {
			return fmt.Errorf("task creation is incomplete")
		}
	case *protocol.TaskStatusChangedV1:
		return (protocol.StateChangedV1{From: value.From, To: value.To, Reason: value.Reason}).ValidateTask()
	case *protocol.OutcomeContractDeclaredV1:
		if value.OutcomeContractID == "" || value.Version == 0 || strings.TrimSpace(value.Goal) == "" || value.Source == "" {
			return fmt.Errorf("outcome contract is incomplete")
		}
		return validateCriteria(value.Criteria, value.Frozen)
	case *protocol.OutcomeContractAmendedV1:
		if value.OutcomeContractID == "" || value.FromVersion == 0 || value.ToVersion != value.FromVersion+1 || strings.TrimSpace(value.Reason) == "" || value.Actor.Validate() != nil {
			return fmt.Errorf("outcome contract amendment is invalid")
		}
		return validateCriteria(value.Criteria, value.Frozen)
	case *protocol.CriterionAssessedV1:
		if value.OutcomeContractID == "" || value.ContractVersion == 0 || value.CriterionID == "" || !oneOf(value.Status, "verified", "failed", "unknown") || strings.TrimSpace(value.Reason) == "" {
			return fmt.Errorf("criterion assessment is invalid")
		}
		if err := sortedEvidenceIDs(value.EvidenceIDs); err != nil {
			return err
		}
		return sortedReceiptIDs(value.ReceiptIDs)
	case *protocol.OutcomeFinalAssessedV1:
		if value.OutcomeContractID == "" || value.ContractVersion == 0 || !oneOf(value.Status, "verified", "failed", "unknown") {
			return fmt.Errorf("final outcome assessment is invalid")
		}
		if err := sortedStrings(value.CriterionIDs, "criterion IDs"); err != nil {
			return err
		}
		return sortedReceiptIDs(value.ReceiptIDs)
	case *protocol.TurnAcceptedV1:
		if value.CommandID == "" || strings.TrimSpace(value.Goal) == "" || value.OutcomeContractID == "" || value.ContractVersion == 0 {
			return fmt.Errorf("accepted turn is incomplete")
		}
	case *protocol.TurnTerminalV1:
		if !oneOf(value.Status, "completed", "failed", "interrupted") || strings.TrimSpace(value.Reason) == "" {
			return fmt.Errorf("turn terminal payload is invalid")
		}
		want := map[string]string{
			protocol.EventTurnCompleted:   "completed",
			protocol.EventTurnFailed:      "failed",
			protocol.EventTurnInterrupted: "interrupted",
		}[kind]
		if want != "" && value.Status != want {
			return fmt.Errorf("turn terminal status %q does not match event %q", value.Status, kind)
		}
	case *protocol.ActivityPlannedV1:
		if value.Kind == "" || strings.TrimSpace(value.Purpose) == "" || value.PurposeActor.Validate() != nil || value.Source == "" || value.RequestedProfile == "" || value.EffectiveProfile == "" {
			return fmt.Errorf("planned activity lacks durable purpose")
		}
		if err := sortedEvidenceIDs(value.InputEvidenceIDs); err != nil {
			return err
		}
		if value.CompactionTrigger != "" && value.CompactionTrigger != "manual" && value.CompactionTrigger != "automatic" {
			return fmt.Errorf("invalid compaction trigger")
		}
		if value.Plan != nil {
			return validateActionPlan(*value.Plan)
		}
	case *protocol.ActivityAuthorizedV1:
		return validateAuthorizationBindings(value.DecisionNonce, value.DecisionEventID, value.PlanDigest, value.RequestDigest, value.DispatchDigest)
	case *protocol.ActivityStartedV1:
		if value.ActivityID == "" || value.CallID == "" || value.RuntimeGenerationID == "" || value.DispatchState == "" {
			return fmt.Errorf("started activity is incomplete")
		}
		return validateAuthorizationBindings(value.DecisionNonce, value.DecisionEventID, value.PlanDigest, value.RequestDigest, value.DispatchDigest)
	case *protocol.ActivityProgressV1:
		if strings.TrimSpace(value.Message) == "" || value.Completed < 0 || value.Total < 0 || (value.Total > 0 && value.Completed > value.Total) {
			return fmt.Errorf("activity progress is invalid")
		}
	case *protocol.ActivityOutcomeV1:
		if !oneOf(value.Status, "succeeded", "failed", "denied", "cancelled", "interrupted_no_effect", "uncertain") {
			return fmt.Errorf("invalid activity outcome status %q", value.Status)
		}
		want := map[string]string{
			protocol.EventActivitySucceeded:           "succeeded",
			protocol.EventActivityFailed:              "failed",
			protocol.EventActivityDenied:              "denied",
			protocol.EventActivityCancelled:           "cancelled",
			protocol.EventActivityInterruptedNoEffect: "interrupted_no_effect",
			protocol.EventActivityUncertain:           "uncertain",
		}[kind]
		if want != "" && value.Status != want {
			return fmt.Errorf("activity outcome status %q does not match event %q", value.Status, kind)
		}
		if value.OutputBytes < 0 {
			return fmt.Errorf("activity output bytes is invalid")
		}
		if value.Usage != nil {
			if err := value.Usage.Validate(); err != nil {
				return err
			}
		}
		return sortedEvidenceIDs(value.OutputEvidenceIDs)
	case *protocol.ProviderCapabilityDecidedV1:
		if !oneOf(value.Status, "accepted", "degraded", "rejected") {
			return fmt.Errorf("invalid provider capability decision")
		}
		if err := validateNegotiatedPlan(value.Plan); err != nil {
			return err
		}
		return sortedStrings(value.Missing, "missing capabilities")
	case *protocol.ProviderAttemptTerminalV1:
		if value.Status == "" || value.TerminalReason == "" {
			return fmt.Errorf("provider attempt terminal is incomplete")
		}
		return value.Usage.Validate()
	case *protocol.ExecutionPlanDeclaredV1:
		return validateActionPlan(value.Plan)
	case *protocol.AuthorizationRequestedV1:
		return value.Request.Validate()
	case *protocol.AuthorizationDecidedV1:
		return value.Decision.Validate()
	case *protocol.AuthorizationDecisionConsumedV1:
		return value.Validate()
	case *protocol.AuthorizationGrantRevokedV1:
		if value.GrantID == "" || strings.TrimSpace(value.Reason) == "" || value.RevocationEpoch == 0 {
			return fmt.Errorf("grant revocation is incomplete")
		}
	case *protocol.EvidenceRecordedV1:
		return validateEvidenceRecord(value.Record)
	case *protocol.EvidenceLinkedV1:
		if value.EvidenceID == "" || value.Subject.Validate() != nil || value.Relation == "" {
			return fmt.Errorf("evidence link is incomplete")
		}
	case *protocol.CheckpointPlannedV1:
		if err := validateCheckpointBody(value.Body); err != nil {
			return err
		}
		if value.PlanDigest != value.Body.PlanDigest {
			return fmt.Errorf("checkpoint plan digest binding mismatch")
		}
		return value.PlanDigest.Validate()
	case *protocol.CheckpointReadyV1:
		if err := validateCheckpointBody(value.Body); err != nil {
			return err
		}
		if err := requireDigest(value.Body, value.Digest); err != nil {
			return err
		}
		return sortedRecoveryIDs(value.RecoveryMaterialIDs)
	case *protocol.CheckpointFailedV1:
		if value.PlanDigest.Validate() != nil || value.ErrorCode == "" || strings.TrimSpace(value.Reason) == "" {
			return fmt.Errorf("checkpoint failure is incomplete")
		}
	case *protocol.VerificationReceiptRecordedV1:
		return validateReceipt(value.Receipt)
	case *protocol.ContextPlanRecordedV1:
		return validateContextPlan(value.Plan)
	case *protocol.ContextUsageRecordedV1:
		if err := value.Usage.Validate(); err != nil {
			return err
		}
		return value.Cost.Validate()
	case *protocol.RuntimeGenerationActivatedV1:
		return validateManifest(value.Manifest)
	case *protocol.ControlOperationPlannedV1:
		if value.ControlOperationID == "" || value.Kind == "" || strings.TrimSpace(value.Purpose) == "" {
			return fmt.Errorf("control operation plan is incomplete")
		}
		return validateActionPlan(value.Plan)
	case *protocol.ControlOperationAuthorizedV1:
		if value.ControlOperationID == "" {
			return fmt.Errorf("control operation ID is required")
		}
		return validateAuthorizationBindings(value.DecisionNonce, value.DecisionEventID, value.PlanDigest, value.RequestDigest, value.DispatchDigest)
	case *protocol.ControlOperationStartedV1:
		if value.ControlOperationID == "" || value.RuntimeGenerationID == "" {
			return fmt.Errorf("started control operation is incomplete")
		}
		return validateAuthorizationBindings(value.DecisionNonce, value.DecisionEventID, value.PlanDigest, value.RequestDigest, value.DispatchDigest)
	case *protocol.ControlOperationTerminalV1:
		if value.ControlOperationID == "" || !oneOf(value.Status, "completed", "failed", "interrupted") {
			return fmt.Errorf("control operation terminal is invalid")
		}
		want := map[string]string{
			protocol.EventControlOperationCompleted:   "completed",
			protocol.EventControlOperationFailed:      "failed",
			protocol.EventControlOperationInterrupted: "interrupted",
		}[kind]
		if want != "" && value.Status != want {
			return fmt.Errorf("control terminal status %q does not match event %q", value.Status, kind)
		}
	case *protocol.CommandAcceptedV1:
		if value.CommandID == "" || value.IdempotencyKey == "" {
			return fmt.Errorf("accepted command is incomplete")
		}
		return value.RequestDigest.Validate()
	case *protocol.CommandCompletedV1:
		if value.CommandID == "" || value.Status == "" || value.RequestDigest.Validate() != nil || protocol.ValidateRawJSON(value.Result) != nil {
			return fmt.Errorf("completed command is invalid")
		}
	case *protocol.MigrationCompatibilityDeclaredV1:
		if value.ReaderVersion == 0 || value.WriterVersion == 0 || value.LegacyHead.Validate() != nil || value.DowngradeStatus == "" {
			return fmt.Errorf("migration compatibility declaration is invalid")
		}
	case *protocol.DiagnosticV1:
		if value.Diagnostic.Code == "" || value.Diagnostic.Message == "" || value.Diagnostic.Journal.Validate() != nil {
			return fmt.Errorf("diagnostic is incomplete")
		}
		if value.Diagnostic.Details != nil {
			return protocol.ValidateRawJSON(value.Diagnostic.Details)
		}
	case *protocol.TransactionCommittedV1:
		if value.TransactionID == "" || value.FirstSeq == 0 || value.LastSeq < value.FirstSeq || uint64(value.EventCount) != value.LastSeq-value.FirstSeq+1 || value.Digest.Validate() != nil {
			return fmt.Errorf("transaction marker is invalid")
		}
	case *protocol.SubagentRequestedV1:
		return value.Validate()
	case *protocol.SubagentWaitingV1:
		return value.Validate()
	case *protocol.SubagentManifestV1:
		return value.Validate()
	case *protocol.SubagentReceiptV1:
		return value.Validate()
	case *protocol.SubagentResultAttachedV1:
		return value.Validate()
	}
	return nil
}

func validateContextCompactionEnvelope(envelope protocol.EventEnvelope, payload any) error {
	value, ok := payload.(*protocol.ContextCompactedV1)
	if !ok || value == nil {
		return fmt.Errorf("context compaction payload type %T", payload)
	}
	if err := value.Validate(); err != nil {
		return err
	}
	reference := protocol.ContextCompactionReference{SessionID: envelope.SessionID, From: value.From, Through: value.Through, SummaryEvidenceID: value.SummaryEvidenceID, Revision: value.Revision}
	if reference.Validate() != nil || envelope.JournalKind != protocol.JournalSession || envelope.JournalID != protocol.JournalID(envelope.SessionID) || value.Through.CommitSeq >= envelope.Seq {
		return fmt.Errorf("context compaction is not anchored before its event")
	}
	return nil
}

func validateSubagentRequestedEnvelope(envelope protocol.EventEnvelope, payload any) error {
	request, ok := payload.(*protocol.SubagentRequestedV1)
	if !ok || request == nil || request.Validate() != nil || envelope.JournalKind != protocol.JournalSession || envelope.SessionID != request.Manifest.ParentSessionID || envelope.JournalID != protocol.JournalID(request.Manifest.ParentSessionID) || envelope.RuntimeGenerationID != request.Manifest.RuntimeGenerationID || envelope.TaskID == "" || envelope.TurnID == "" || request.Manifest.ParentCursor.CommitSeq >= envelope.Seq {
		return fmt.Errorf("subagent request does not match parent session")
	}
	return nil
}

func validateSubagentChildEnvelope(envelope protocol.EventEnvelope, manifest protocol.SubagentManifestV1) error {
	if manifest.Validate() != nil || envelope.JournalKind != protocol.JournalSession || envelope.SessionID != manifest.ChildSessionID || envelope.JournalID != protocol.JournalID(manifest.ChildSessionID) || envelope.RuntimeGenerationID != manifest.RuntimeGenerationID || envelope.TaskID != manifest.ChildTaskID || envelope.TurnID != manifest.ChildTurnID {
		return fmt.Errorf("subagent child event does not match manifest")
	}
	return nil
}

func validMode(mode string) bool {
	return mode == "safe" || mode == "ask" || mode == "auto"
}

func oneOf(value string, choices ...string) bool {
	for _, choice := range choices {
		if value == choice {
			return true
		}
	}
	return false
}

func validateCriteria(criteria []protocol.CriterionV1, requireRequired bool) error {
	seen := make(map[string]struct{}, len(criteria))
	required := 0
	for _, criterion := range criteria {
		if criterion.ID == "" || strings.TrimSpace(criterion.Description) == "" || criterion.VerificationMethod == "" || criterion.ExpectedEvidenceKind == "" {
			return fmt.Errorf("criterion is incomplete")
		}
		if _, duplicate := seen[criterion.ID]; duplicate {
			return fmt.Errorf("duplicate criterion %q", criterion.ID)
		}
		seen[criterion.ID] = struct{}{}
		if criterion.Required {
			required++
		}
	}
	if requireRequired && required == 0 {
		return fmt.Errorf("frozen contract requires at least one required criterion")
	}
	return nil
}

func validateAuthorizationBindings(nonce protocol.DecisionNonce, eventID protocol.EventID, digests ...protocol.Digest) error {
	if nonce == "" || eventID == "" {
		return fmt.Errorf("authorization binding is incomplete")
	}
	for _, digest := range digests {
		if err := digest.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func validateActionPlan(plan protocol.ActionPlan) error {
	if err := plan.Body.Validate(); err != nil {
		return err
	}
	return requireDigest(plan.Body, plan.Digest)
}

func validateNegotiatedPlan(plan protocol.NegotiatedProviderPlan) error {
	if err := protocol.ValidateBounds(plan.Body); err != nil {
		return err
	}
	if err := plan.Body.Descriptor.Validate(); err != nil {
		return err
	}
	if plan.Body.ToolExposureRevision == "" {
		return fmt.Errorf("tool exposure revision is required")
	}
	previousRequirement := ""
	for _, requirement := range plan.Body.Requirements {
		if err := requirement.Validate(); err != nil {
			return err
		}
		if previousRequirement != "" && requirement.Capability <= previousRequirement {
			return fmt.Errorf("capability requirements must be sorted and unique")
		}
		previousRequirement = requirement.Capability
	}
	if err := sortedStrings(plan.Body.Warnings, "provider plan warnings"); err != nil {
		return err
	}
	return requireDigest(plan.Body, plan.Digest)
}

func validateContextPlan(plan protocol.ContextPlan) error {
	if err := protocol.ValidateBounds(plan.Body); err != nil {
		return err
	}
	if plan.Body.OutputReserve < 0 || plan.Body.CompactionRevision == "" || plan.Body.ToolExposureRevision == "" {
		return fmt.Errorf("context plan body is invalid")
	}
	if err := plan.Body.EstimatedInputTokens.Validate(); err != nil {
		return err
	}
	if err := plan.Body.ContextWindow.Validate(); err != nil {
		return err
	}
	seenSources := make(map[string]struct{}, len(plan.Body.Sources)+len(plan.Body.Excluded))
	for _, source := range plan.Body.Sources {
		if err := source.Validate(); err != nil {
			return fmt.Errorf("content source %q: %w", source.ID, err)
		}
		if _, duplicate := seenSources[source.ID]; duplicate {
			return fmt.Errorf("duplicate content source %q", source.ID)
		}
		seenSources[source.ID] = struct{}{}
		if err := requireDigest(source.Content, source.Digest); err != nil {
			return fmt.Errorf("content source %q digest: %w", source.ID, err)
		}
	}
	for _, source := range plan.Body.Excluded {
		if err := source.Validate(); err != nil {
			return err
		}
		if _, duplicate := seenSources[source.ID]; duplicate {
			return fmt.Errorf("duplicate or conflicting excluded content source %q", source.ID)
		}
		seenSources[source.ID] = struct{}{}
	}
	return requireDigest(plan.Body, plan.Digest)
}

func validateEvidenceRecord(record protocol.EvidenceRecord) error {
	if err := record.Body.Validate(); err != nil {
		return err
	}
	return requireDigest(record.Body, record.Digest)
}

func validateCheckpointBody(body protocol.CheckpointBody) error {
	if body.ID == "" || body.SessionID == "" || body.TaskID == "" || body.TurnID == "" || body.OutcomeContractID == "" || body.ContractVersion == 0 || body.RuntimeGenerationID == "" || body.CreatedAt.IsZero() || body.EventHead.Validate() != nil || body.ContextPlanDigest.Validate() != nil || body.PlanDigest.Validate() != nil {
		return fmt.Errorf("checkpoint body is incomplete")
	}
	for _, coverage := range body.Coverage {
		if coverage.Subject.Validate() != nil || coverage.Class == "" {
			return fmt.Errorf("checkpoint coverage is incomplete")
		}
		if err := sortedEvidenceIDs(coverage.EvidenceIDs); err != nil {
			return err
		}
	}
	return nil
}

func validateReceipt(receipt protocol.VerificationReceipt) error {
	body := receipt.Body
	if body.ID == "" || body.TaskID == "" || body.OutcomeContractID == "" || body.ContractVersion == 0 || body.CriterionID == "" || body.ActivityID == "" || body.Subject.Validate() != nil || body.VerifierID == "" || body.VerifierVersion == "" || body.RequestedProfile == "" || body.EffectiveProfile == "" || body.StartedAt.IsZero() || body.TerminalAt.Before(body.StartedAt) || body.Status == "" || body.Coverage == "" {
		return fmt.Errorf("verification receipt is incomplete")
	}
	if err := sortedEvidenceIDs(body.EvidenceIDs); err != nil {
		return err
	}
	if err := sortedStrings(body.UnsupportedConclusions, "unsupported conclusions"); err != nil {
		return err
	}
	return requireDigest(receipt.Body, receipt.Digest)
}

func validateManifest(manifest protocol.RuntimeGenerationManifest) error {
	if err := protocol.ValidateBounds(manifest.Body); err != nil {
		return err
	}
	if manifest.ID == "" || manifest.Body.ProviderCatalogRevision == "" || manifest.Body.ToolCatalogRevision == "" || manifest.Body.InstructionRevision == "" || manifest.Body.PolicyGeneration == "" {
		return fmt.Errorf("runtime generation manifest is incomplete")
	}
	if err := manifest.Body.Limits.ValidatePersisted(); err != nil {
		return fmt.Errorf("runtime generation manifest limits: %w", err)
	}
	if len(manifest.Body.Skills) > 0 && manifest.Body.SkillCatalogRevision == "" {
		return fmt.Errorf("runtime skill catalog revision is required")
	}
	if len(manifest.Body.Skills) > protocol.MaxActiveSkills {
		return fmt.Errorf("runtime skill catalog exceeds %d active skills", protocol.MaxActiveSkills)
	}
	previousSkill := ""
	seenSkills := make(map[string]struct{}, len(manifest.Body.Skills))
	for _, descriptor := range manifest.Body.Skills {
		if err := descriptor.Validate(); err != nil {
			return fmt.Errorf("skill descriptor: %w", err)
		}
		if descriptor.State != protocol.SkillStateActive || descriptor.Identity.RuntimeGenerationID != manifest.ID {
			return fmt.Errorf("runtime skill descriptor is not active for this generation")
		}
		if _, duplicate := seenSkills[descriptor.Identity.Name]; duplicate {
			return fmt.Errorf("duplicate runtime skill descriptor")
		}
		sortKey := string(descriptor.Identity.Source) + "\x00" + descriptor.Identity.Name + "\x00" + descriptor.Identity.ContentDigest.Algorithm + "\x00" + descriptor.Identity.ContentDigest.Value
		if previousSkill != "" && sortKey <= previousSkill {
			return fmt.Errorf("runtime skill descriptors must be sorted")
		}
		previousSkill = sortKey
		seenSkills[descriptor.Identity.Name] = struct{}{}
	}
	seenModels := make(map[string]struct{}, len(manifest.Body.Models))
	for _, descriptor := range manifest.Body.Models {
		if err := descriptor.Validate(); err != nil {
			return fmt.Errorf("model descriptor: %w", err)
		}
		if descriptor.RuntimeGenerationID != manifest.ID {
			return fmt.Errorf("model descriptor runtime generation mismatch")
		}
		key := string(descriptor.ProviderID) + "\x00" + string(descriptor.ModelID)
		if _, duplicate := seenModels[key]; duplicate {
			return fmt.Errorf("duplicate model descriptor")
		}
		seenModels[key] = struct{}{}
	}
	seenTools := make(map[protocol.ToolIdentity]struct{}, len(manifest.Body.Tools))
	for _, descriptor := range manifest.Body.Tools {
		if err := descriptor.Body.Validate(); err != nil {
			return fmt.Errorf("tool descriptor: %w", err)
		}
		if err := requireDigest(descriptor.Body, descriptor.DescriptorDigest); err != nil {
			return err
		}
		if _, duplicate := seenTools[descriptor.Body.Identity]; duplicate {
			return fmt.Errorf("duplicate tool descriptor")
		}
		seenTools[descriptor.Body.Identity] = struct{}{}
	}
	seenProfiles := make(map[string]struct{}, len(manifest.Body.ExecutionProfiles))
	for _, profile := range manifest.Body.ExecutionProfiles {
		if profile == "" {
			return fmt.Errorf("execution profile is empty")
		}
		if _, duplicate := seenProfiles[profile]; duplicate {
			return fmt.Errorf("duplicate execution profile %q", profile)
		}
		seenProfiles[profile] = struct{}{}
	}
	return requireDigest(manifest.Body, manifest.Digest)
}

func requireDigest(body any, got protocol.Digest) error {
	return canonicaljson.ValidateDigest(body, got)
}

func ValidateAuthorizationConsumption(consumed protocol.AuthorizationDecisionConsumedV1, decisionEnvelope protocol.EventEnvelope, decision protocol.AuthorizationDecidedV1, started any) error {
	if decisionEnvelope.Kind != protocol.EventAuthorizationDecided || decisionEnvelope.PayloadVersion != 1 {
		return fmt.Errorf("committed decision must be authorization.decided@1")
	}
	committed := new(protocol.AuthorizationDecidedV1)
	if err := decodeStrict(decisionEnvelope.Payload, committed); err != nil {
		return fmt.Errorf("decode committed authorization decision: %w", err)
	}
	if err := requireFields(decisionEnvelope.Payload, reflect.TypeOf(*committed)); err != nil {
		return fmt.Errorf("committed authorization decision structure: %w", err)
	}
	registry, err := New(FoundationDescriptors())
	if err != nil {
		return fmt.Errorf("construct foundation registry: %w", err)
	}
	if err := registry.Validate(protocol.EventRecord{Envelope: decisionEnvelope, Decoded: committed}); err != nil {
		return fmt.Errorf("validate committed authorization decision: %w", err)
	}
	if !reflect.DeepEqual(*committed, decision) {
		return fmt.Errorf("supplied authorization decision does not match committed payload")
	}
	if committed.Decision.Action != "allow" {
		return fmt.Errorf("only an allow decision can be consumed")
	}
	if err := consumed.Validate(); err != nil {
		return err
	}
	request := committed.Decision.Request
	decisionDigest, err := canonicaljson.Digest(committed.Decision)
	if err != nil {
		return err
	}
	if consumed.DecisionEventID != decisionEnvelope.EventID || consumed.DecisionNonce != committed.Decision.DecisionNonce || consumed.DecisionDigest != decisionDigest || consumed.RequestID != request.RequestID || consumed.CallID != request.CallID || consumed.PlanDigest != request.PlanDigest || consumed.RequestDigest != request.RequestDigest || consumed.DispatchDigest != request.DispatchDigest || consumed.RuntimeGenerationID != request.RuntimeGenerationID {
		return fmt.Errorf("authorization consumption does not match committed decision bindings")
	}
	if consumed.ControlOperationID != "" {
		if request.ControlOperationID == "" || consumed.ControlOperationID != request.ControlOperationID {
			return fmt.Errorf("authorization consumption control-operation target does not match decision request")
		}
	} else if request.ControlOperationID != "" || consumed.ActivityID != request.ActivityID {
		return fmt.Errorf("authorization consumption activity target does not match decision request")
	}
	switch value := started.(type) {
	case protocol.ActivityStartedV1:
		return validateActivityStartBindings(consumed, value)
	case *protocol.ActivityStartedV1:
		if value == nil {
			return fmt.Errorf("activity start is nil")
		}
		return validateActivityStartBindings(consumed, *value)
	case protocol.ControlOperationStartedV1:
		return validateControlStartBindings(consumed, value)
	case *protocol.ControlOperationStartedV1:
		if value == nil {
			return fmt.Errorf("control operation start is nil")
		}
		return validateControlStartBindings(consumed, *value)
	default:
		return fmt.Errorf("unsupported start payload %T", started)
	}
}

func validateActivityStartBindings(consumed protocol.AuthorizationDecisionConsumedV1, started protocol.ActivityStartedV1) error {
	if consumed.ControlOperationID != "" || consumed.ActivityID != started.ActivityID || consumed.DecisionNonce != started.DecisionNonce || consumed.DecisionEventID != started.DecisionEventID || consumed.CallID != started.CallID || consumed.PlanDigest != started.PlanDigest || consumed.RequestDigest != started.RequestDigest || consumed.DispatchDigest != started.DispatchDigest || consumed.RuntimeGenerationID != started.RuntimeGenerationID {
		return fmt.Errorf("authorization consumption does not match activity start bindings")
	}
	return nil
}

func validateControlStartBindings(consumed protocol.AuthorizationDecisionConsumedV1, started protocol.ControlOperationStartedV1) error {
	if consumed.ActivityID != "" || consumed.ControlOperationID != started.ControlOperationID || consumed.DecisionNonce != started.DecisionNonce || consumed.DecisionEventID != started.DecisionEventID || consumed.PlanDigest != started.PlanDigest || consumed.RequestDigest != started.RequestDigest || consumed.DispatchDigest != started.DispatchDigest || consumed.RuntimeGenerationID != started.RuntimeGenerationID {
		return fmt.Errorf("authorization consumption does not match control operation start bindings")
	}
	return nil
}

func sortedEvidenceIDs(values []protocol.EvidenceID) error {
	previous := protocol.EvidenceID("")
	for index, value := range values {
		if value == "" || (index > 0 && value <= previous) {
			return fmt.Errorf("evidence IDs must be nonempty, sorted, and unique")
		}
		previous = value
	}
	return nil
}

func sortedReceiptIDs(values []protocol.ReceiptID) error {
	previous := protocol.ReceiptID("")
	for index, value := range values {
		if value == "" || (index > 0 && value <= previous) {
			return fmt.Errorf("receipt IDs must be nonempty, sorted, and unique")
		}
		previous = value
	}
	return nil
}

func sortedRecoveryIDs(values []protocol.RecoveryMaterialID) error {
	previous := protocol.RecoveryMaterialID("")
	for index, value := range values {
		if value == "" || (index > 0 && value <= previous) {
			return fmt.Errorf("recovery material IDs must be nonempty, sorted, and unique")
		}
		previous = value
	}
	return nil
}

func sortedStrings(values []string, label string) error {
	previous := ""
	for index, value := range values {
		if value == "" || (index > 0 && value <= previous) {
			return fmt.Errorf("%s must be nonempty, sorted, and unique", label)
		}
		previous = value
	}
	return nil
}

func validateJournalFamily(envelope protocol.EventEnvelope, payload any) error {
	if controlOnlyKind(envelope.Kind) {
		if envelope.JournalKind != protocol.JournalWorkspaceControl {
			return fmt.Errorf("event %q requires workspace-control journal", envelope.Kind)
		}
		return nil
	}
	if sessionOnlyKind(envelope.Kind) {
		if envelope.JournalKind != protocol.JournalSession {
			return fmt.Errorf("event %q requires session journal", envelope.Kind)
		}
		return nil
	}
	switch value := payload.(type) {
	case *protocol.AuthorizationRequestedV1:
		return validateAuthorizationJournal(envelope, value.Request.SessionID, value.Request.ControlOperationID)
	case *protocol.AuthorizationDecidedV1:
		return validateAuthorizationJournal(envelope, value.Decision.Request.SessionID, value.Decision.Request.ControlOperationID)
	case *protocol.AuthorizationDecisionConsumedV1:
		if value.ActivityID != "" {
			return validateAuthorizationJournal(envelope, envelope.SessionID, "")
		}
		return validateAuthorizationJournal(envelope, "", value.ControlOperationID)
	}
	return nil
}

func validateAuthorizationJournal(envelope protocol.EventEnvelope, sessionID protocol.SessionID, controlID protocol.ControlOperationID) error {
	if (sessionID == "") == (controlID == "") {
		return fmt.Errorf("authorization payload requires exactly one correlation")
	}
	if sessionID != "" {
		if envelope.JournalKind != protocol.JournalSession || envelope.SessionID != sessionID {
			return fmt.Errorf("session authorization journal mismatch")
		}
		return nil
	}
	if envelope.JournalKind != protocol.JournalWorkspaceControl {
		return fmt.Errorf("control authorization requires workspace-control journal")
	}
	return nil
}

func controlOnlyKind(kind string) bool {
	switch kind {
	case protocol.EventRuntimeGenerationActivated,
		protocol.EventProjectSkillTrustChanged,
		protocol.EventControlOperationPlanned,
		protocol.EventControlOperationAuthorized,
		protocol.EventControlOperationStarted,
		protocol.EventControlOperationCompleted,
		protocol.EventControlOperationFailed,
		protocol.EventControlOperationInterrupted:
		return true
	default:
		return false
	}
}

func sessionOnlyKind(kind string) bool {
	switch kind {
	case protocol.EventSessionCreated, protocol.EventSessionForked, protocol.EventSessionTitleChanged, protocol.EventSessionLifecycleChanged,
		protocol.EventModeChanged, protocol.EventModelChanged, protocol.EventTrustedExecutionAcknowledged,
		protocol.EventUserMessage, protocol.EventAssistantMessage, protocol.EventContextCompacted,
		protocol.EventFileChangePlanned, protocol.EventFileChanged,
		protocol.EventTaskCreated, protocol.EventTaskStatusChanged,
		protocol.EventOutcomeContractDeclared, protocol.EventOutcomeContractAmended, protocol.EventOutcomeCriterionAssessed, protocol.EventOutcomeFinalAssessed,
		protocol.EventTurnAccepted, protocol.EventTurnStateChanged, protocol.EventTurnCompleted, protocol.EventTurnFailed, protocol.EventTurnInterrupted,
		protocol.EventActivityPlanned, protocol.EventActivityAuthorized, protocol.EventActivityStarted, protocol.EventActivityProgress,
		protocol.EventActivitySucceeded, protocol.EventActivityFailed, protocol.EventActivityDenied, protocol.EventActivityCancelled,
		protocol.EventActivityInterruptedNoEffect, protocol.EventActivityUncertain,
		protocol.EventProviderCapabilityDecided, protocol.EventProviderAttemptTerminal, protocol.EventExecutionPlanDeclared,
		protocol.EventAuthorizationGrantRevoked,
		protocol.EventEvidenceRecorded, protocol.EventEvidenceLinked,
		protocol.EventCheckpointPlanned, protocol.EventCheckpointReady, protocol.EventCheckpointFailed,
		protocol.EventVerificationReceiptRecorded, protocol.EventContextPlanRecorded, protocol.EventContextUsageRecorded:
		return true
	case protocol.EventSubagentRequested, protocol.EventSubagentWaiting, protocol.EventSubagentResultAttached,
		protocol.EventSubagentManifest, protocol.EventSubagentReceipt:
		return true
	default:
		return false
	}
}

func validateEnvelopeIdentity(envelope protocol.EventEnvelope, payload any) error {
	switch value := payload.(type) {
	case *protocol.FileChangePlannedV1:
		return requireRepeatedIdentity("file.change_planned runtime generation ID", string(envelope.RuntimeGenerationID), string(value.Plan.Body.RuntimeGenerationID))
	case *protocol.ActivityPlannedV1:
		if value.Plan != nil {
			return requireRepeatedIdentity("activity.planned runtime generation ID", string(envelope.RuntimeGenerationID), string(value.Plan.Body.RuntimeGenerationID))
		}
	case *protocol.ExecutionPlanDeclaredV1:
		return requireRepeatedIdentity("execution.plan_declared runtime generation ID", string(envelope.RuntimeGenerationID), string(value.Plan.Body.RuntimeGenerationID))
	case *protocol.ActivityStartedV1:
		if err := requireRepeatedIdentity("activity.started activity ID", string(envelope.ActivityID), string(value.ActivityID)); err != nil {
			return err
		}
		return requireRepeatedIdentity("activity.started runtime generation ID", string(envelope.RuntimeGenerationID), string(value.RuntimeGenerationID))
	case *protocol.AuthorizationRequestedV1:
		return validateAuthorizationEnvelopeIdentities(envelope, value.Request)
	case *protocol.AuthorizationDecidedV1:
		return validateAuthorizationEnvelopeIdentities(envelope, value.Decision.Request)
	case *protocol.AuthorizationDecisionConsumedV1:
		if value.ActivityID != "" {
			if err := requireRepeatedIdentity("authorization.decision_consumed activity ID", string(envelope.ActivityID), string(value.ActivityID)); err != nil {
				return err
			}
		}
		return requireRepeatedIdentity("authorization.decision_consumed runtime generation ID", string(envelope.RuntimeGenerationID), string(value.RuntimeGenerationID))
	case *protocol.EvidenceRecordedV1:
		if err := requireRepeatedIdentity("evidence.recorded session ID", string(envelope.SessionID), string(value.Record.Body.SessionID)); err != nil {
			return err
		}
		return requireRepeatedIdentity("evidence.recorded activity ID", string(envelope.ActivityID), string(value.Record.Body.ProducingActivityID))
	case *protocol.CheckpointPlannedV1:
		return validateCheckpointEnvelopeIdentities(envelope, value.Body)
	case *protocol.CheckpointReadyV1:
		return validateCheckpointEnvelopeIdentities(envelope, value.Body)
	case *protocol.VerificationReceiptRecordedV1:
		if err := requireRepeatedIdentity("verification receipt task ID", string(envelope.TaskID), string(value.Receipt.Body.TaskID)); err != nil {
			return err
		}
		return requireRepeatedIdentity("verification receipt activity ID", string(envelope.ActivityID), string(value.Receipt.Body.ActivityID))
	case *protocol.ProviderCapabilityDecidedV1:
		return requireRepeatedIdentity("provider plan runtime generation ID", string(envelope.RuntimeGenerationID), string(value.Plan.Body.Descriptor.RuntimeGenerationID))
	case *protocol.RuntimeGenerationActivatedV1:
		return requireRepeatedIdentity("runtime_generation.activated runtime generation ID", string(envelope.RuntimeGenerationID), string(value.Manifest.ID))
	case *protocol.ControlOperationPlannedV1:
		return requireRepeatedIdentity("control_operation.planned runtime generation ID", string(envelope.RuntimeGenerationID), string(value.Plan.Body.RuntimeGenerationID))
	case *protocol.ControlOperationStartedV1:
		return requireRepeatedIdentity("control_operation.started runtime generation ID", string(envelope.RuntimeGenerationID), string(value.RuntimeGenerationID))
	case *protocol.DiagnosticV1:
		if value.Diagnostic.Journal.Kind != envelope.JournalKind || value.Diagnostic.Journal.ID != envelope.JournalID {
			return fmt.Errorf("diagnostic journal does not match envelope")
		}
	case *protocol.TransactionCommittedV1:
		if value.TransactionID != envelope.TransactionID {
			return fmt.Errorf("transaction ID does not match envelope")
		}
	}
	return nil
}

func validateCheckpointEnvelopeIdentities(envelope protocol.EventEnvelope, body protocol.CheckpointBody) error {
	checks := []struct {
		label    string
		envelope string
		payload  string
	}{
		{"session ID", string(envelope.SessionID), string(body.SessionID)},
		{"task ID", string(envelope.TaskID), string(body.TaskID)},
		{"turn ID", string(envelope.TurnID), string(body.TurnID)},
		{"runtime generation ID", string(envelope.RuntimeGenerationID), string(body.RuntimeGenerationID)},
	}
	for _, check := range checks {
		if err := requireRepeatedIdentity("checkpoint "+check.label, check.envelope, check.payload); err != nil {
			return err
		}
	}
	return nil
}

func validateAuthorizationEnvelopeIdentities(envelope protocol.EventEnvelope, request protocol.AuthorizationRequest) error {
	checks := []struct {
		label    string
		envelope string
		payload  string
	}{
		{"session ID", string(envelope.SessionID), string(request.SessionID)},
		{"task ID", string(envelope.TaskID), string(request.TaskID)},
		{"turn ID", string(envelope.TurnID), string(request.TurnID)},
		{"activity ID", string(envelope.ActivityID), string(request.ActivityID)},
		{"parent activity ID", string(envelope.ParentActivityID), string(request.ParentActivityID)},
		{"runtime generation ID", string(envelope.RuntimeGenerationID), string(request.RuntimeGenerationID)},
	}
	for _, check := range checks {
		if err := requireRepeatedIdentity("authorization request "+check.label, check.envelope, check.payload); err != nil {
			return err
		}
	}
	return nil
}

func requireRepeatedIdentity(label, envelope, payload string) error {
	if envelope != payload {
		return fmt.Errorf("%s does not match envelope", label)
	}
	return nil
}
