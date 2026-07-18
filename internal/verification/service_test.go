package verification_test

import (
	"context"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/verification"
)

func TestLegacyCompatibilityAssessmentNeverTreatsAssistantTextAsVerified(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	service := verification.NewService(func() time.Time { return now })
	result, err := service.Assess(context.Background(), verification.Request{
		TaskID: "task-a", OutcomeContractID: "contract-a", ContractVersion: 2,
		CriterionID: "legacy_turn_terminal", ActivityID: "verification-a", TurnID: "turn-a",
		TerminalStatus: "completed",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.CriterionStatus != "unknown" || result.FinalStatus != "unknown" {
		t.Fatalf("criterion=%q final=%q", result.CriterionStatus, result.FinalStatus)
	}
	if result.Receipt.Body.Status == "verified" {
		t.Fatal("assistant completion produced a verified receipt")
	}
	if err := canonicaljson.ValidateDigest(result.Receipt.Body, result.Receipt.Digest); err != nil {
		t.Fatalf("receipt digest: %v", err)
	}
}

func TestLegacyCompatibilityAssessmentMapsNonSuccessToFailed(t *testing.T) {
	service := verification.NewService(time.Now)
	result, err := service.Assess(context.Background(), verification.Request{
		TaskID: "task-a", OutcomeContractID: "contract-a", ContractVersion: 2,
		CriterionID: "legacy_turn_terminal", ActivityID: "verification-a", TurnID: protocol.TurnID("turn-a"),
		TerminalStatus: "failed",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.CriterionStatus != "failed" || result.FinalStatus != "failed" {
		t.Fatalf("criterion=%q final=%q", result.CriterionStatus, result.FinalStatus)
	}
}
