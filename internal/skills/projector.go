package skills

import (
	"fmt"

	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/projection"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type TrustProjection struct {
	WorkspaceID protocol.WorkspaceID `json:"workspace_id"`
	Decisions   []TrustState         `json:"decisions"`
}

func (p TrustProjection) Resolve(workspace protocol.WorkspaceID, digest protocol.Digest) (*TrustState, bool) {
	for index := len(p.Decisions) - 1; index >= 0; index-- {
		decision := p.Decisions[index]
		if decision.WorkspaceID == workspace && decision.CatalogDigest == digest {
			copy := protocol.DeepCopy(decision)
			return &copy, true
		}
	}
	return nil, false
}

type TrustProjector struct{}

func (TrustProjector) Version() uint32 { return 1 }
func (TrustProjector) Zero(ref protocol.JournalRef) TrustProjection {
	if ref.Kind != protocol.JournalWorkspaceControl {
		return TrustProjection{}
	}
	return TrustProjection{WorkspaceID: protocol.WorkspaceID(ref.ID), Decisions: []TrustState{}}
}
func (TrustProjector) Apply(current TrustProjection, event protocol.EventRecord) (TrustProjection, error) {
	if err := projection.ValidateFoundationEvent(event); err != nil {
		return current, err
	}
	if err := validateTrustProjection(current); err != nil {
		return current, err
	}
	if event.Envelope.Kind != protocol.EventProjectSkillTrustChanged {
		return protocol.DeepCopy(current), nil
	}
	payload, ok := event.Decoded.(*protocol.ProjectSkillTrustChangedV1)
	if !ok || payload.Validate() != nil || current.WorkspaceID == "" || event.Envelope.JournalKind != protocol.JournalWorkspaceControl || event.Envelope.JournalID != protocol.JournalID(current.WorkspaceID) || payload.WorkspaceID != current.WorkspaceID || event.Envelope.JournalID != protocol.JournalID(payload.WorkspaceID) || event.Envelope.Seq == 0 || event.Envelope.TransactionID == "" {
		return current, fmt.Errorf("invalid project skill trust event")
	}
	next := protocol.DeepCopy(current)
	next.Decisions = append(next.Decisions, TrustState{WorkspaceID: payload.WorkspaceID, CatalogDigest: payload.CatalogDigest, Decision: config.ProjectSkillPolicy(payload.Decision), Cursor: protocol.CommittedCursor{JournalKind: event.Envelope.JournalKind, JournalID: event.Envelope.JournalID, CommitSeq: event.Envelope.Seq, TransactionID: event.Envelope.TransactionID}})
	return next, nil
}

func validateTrustProjection(value TrustProjection) error {
	if value.WorkspaceID == "" {
		return fmt.Errorf("invalid project skill trust projection")
	}
	for _, decision := range value.Decisions {
		if decision.WorkspaceID != value.WorkspaceID || validateTrustState(decision) != nil {
			return fmt.Errorf("invalid project skill trust projection")
		}
	}
	return nil
}
