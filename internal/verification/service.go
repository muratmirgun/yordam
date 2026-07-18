package verification

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
)

const (
	legacyVerifierID      = "yordam.v1_compatibility"
	legacyVerifierVersion = "1"
)

type Request struct {
	TaskID            protocol.TaskID
	OutcomeContractID protocol.OutcomeContractID
	ContractVersion   uint32
	CriterionID       string
	ActivityID        protocol.ActivityID
	TurnID            protocol.TurnID
	TerminalStatus    string
	EvidenceIDs       []protocol.EvidenceID
}

type Result struct {
	Receipt         protocol.VerificationReceipt
	CriterionStatus string
	FinalStatus     string
	Reason          string
}

type Service struct{ clock func() time.Time }

func NewService(clock func() time.Time) *Service {
	if clock == nil {
		clock = time.Now
	}
	return &Service{clock: clock}
}

func (s *Service) Assess(ctx context.Context, request Request) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if request.TaskID == "" || request.OutcomeContractID == "" || request.ContractVersion == 0 || request.CriterionID != "legacy_turn_terminal" || request.ActivityID == "" || request.TurnID == "" || request.TerminalStatus == "" {
		return Result{}, fmt.Errorf("legacy compatibility verification request is incomplete")
	}
	evidenceIDs := append(make([]protocol.EvidenceID, 0, len(request.EvidenceIDs)), request.EvidenceIDs...)
	sort.Slice(evidenceIDs, func(i, j int) bool { return evidenceIDs[i] < evidenceIDs[j] })
	for index, evidenceID := range evidenceIDs {
		if evidenceID == "" || index > 0 && evidenceID == evidenceIDs[index-1] {
			return Result{}, fmt.Errorf("evidence IDs must be nonempty and unique")
		}
	}

	criterionStatus, receiptStatus, reason := "unknown", "unsupported", "legacy compatibility outcome requires non-assistant verification evidence"
	if request.TerminalStatus == "failed" || request.TerminalStatus == "denied" {
		criterionStatus, receiptStatus, reason = "failed", "failed", "legacy turn terminated unsuccessfully"
	}
	now := s.clock().UTC()
	body := protocol.VerificationReceiptBody{
		ID:                receiptID(request),
		TaskID:            request.TaskID,
		OutcomeContractID: request.OutcomeContractID,
		ContractVersion:   request.ContractVersion,
		CriterionID:       request.CriterionID,
		ActivityID:        request.ActivityID,
		Subject:           protocol.SubjectRef{Kind: "turn", ID: string(request.TurnID)},
		VerifierID:        legacyVerifierID,
		VerifierVersion:   legacyVerifierVersion,
		RequestedProfile:  "v1_compatibility",
		EffectiveProfile:  "v1_compatibility",
		StartedAt:         now,
		TerminalAt:        now,
		Status:            receiptStatus,
		Coverage:          "legacy_terminal_only",
		EvidenceIDs:       evidenceIDs,
		UnsupportedConclusions: []string{
			"assistant_text_verifies_outcome",
		},
	}
	digest, err := canonicaljson.Digest(body)
	if err != nil {
		return Result{}, err
	}
	return Result{
		Receipt:         protocol.VerificationReceipt{Body: body, Digest: digest},
		CriterionStatus: criterionStatus,
		FinalStatus:     criterionStatus,
		Reason:          reason,
	}, nil
}

func receiptID(request Request) protocol.ReceiptID {
	sum := sha256.Sum256([]byte(string(request.TaskID) + "\x00" + string(request.TurnID) + "\x00" + request.CriterionID))
	return protocol.ReceiptID("receipt-" + hex.EncodeToString(sum[:16]))
}
