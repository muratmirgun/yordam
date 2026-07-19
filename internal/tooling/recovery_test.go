package tooling

import (
	"context"
	"sync"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/recovery"
)

func TestPrepareAndPutRecoveryKeepsPreimageInsideToolingBoundary(t *testing.T) {
	planDigest := protocol.Digest{Algorithm: protocol.DigestSHA256, Value: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	preimageDigest := protocol.Digest{Algorithm: protocol.DigestSHA256, Value: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	postimageDigest := protocol.Digest{Algorithm: protocol.DigestSHA256, Value: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}
	prepared := recoveryPrepared{candidate: recovery.Candidate{Preimage: []byte("before"), PreimageDigest: preimageDigest, ExpectedPostimageDigest: postimageDigest, Mode: 0o600}}
	service := &Service{actions: map[string]*plannedAction{
		"preview-a": {prepared: prepared, previewReady: true, operationMu: &sync.Mutex{}},
	}}
	sink := &capturingRecoverySink{}
	checkpoint := protocol.CheckpointBody{ID: "checkpoint-a", SessionID: "session-a", Coverage: []protocol.CheckpointCoverage{{Subject: protocol.SubjectRef{Kind: "file", ID: "workspace/a.txt"}}}}
	record, err := service.PrepareAndPutRecovery(context.Background(), PreviewResult{handleID: "preview-a"}, "activity-a", checkpoint, protocol.ActionPlan{Digest: planDigest}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if record.Body.PlanDigest != planDigest || string(sink.candidate.Preimage) != "before" || sink.candidate.WorkspaceID != "session-a" || sink.candidate.Subject.ID != "workspace/a.txt" {
		t.Fatalf("recovery binding=%+v record=%+v", sink.candidate, record)
	}
}

type recoveryPrepared struct{ candidate recovery.Candidate }

func (p recoveryPrepared) Preview() domain.PreparedToolRequest       { return domain.PreparedToolRequest{} }
func (p recoveryPrepared) Execute(context.Context) domain.ToolResult { return domain.ToolResult{} }
func (p recoveryPrepared) RecoveryMaterial(context.Context) (recovery.Candidate, bool, error) {
	return p.candidate, true, nil
}

type capturingRecoverySink struct{ candidate recovery.Candidate }

func (s *capturingRecoverySink) Put(_ context.Context, candidate recovery.Candidate) (protocol.RecoveryMaterialRecord, error) {
	s.candidate = candidate
	return protocol.RecoveryMaterialRecord{Body: protocol.RecoveryMaterialBody{PlanDigest: candidate.PlanDigest}}, nil
}
