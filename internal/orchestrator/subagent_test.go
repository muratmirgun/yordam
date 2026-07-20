package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/provider"
	"github.com/muratmirgun/yordam/internal/tooling"
	subagenttool "github.com/muratmirgun/yordam/internal/tools/subagent"
	"github.com/muratmirgun/yordam/internal/verification"
)

func TestDecodeSubagentCallRejectsMalformedAndUnknownFields(t *testing.T) {
	if _, err := decodeSubagentCall(json.RawMessage(`{"task":"inspect","unexpected":true}`)); err == nil {
		t.Fatal("unknown subagent field was accepted")
	}
	if _, err := decodeSubagentCall(json.RawMessage(`{"task":"   "}`)); err == nil {
		t.Fatal("blank subagent task was accepted")
	}
	call, err := decodeSubagentCall(json.RawMessage(`{"task":"inspect","expected_output":"report"}`))
	if err != nil || call.Task != "inspect" || call.ExpectedOutput != "report" {
		t.Fatalf("call=%+v err=%v", call, err)
	}
}

func testSubagentDeadline() time.Time { return time.Unix(100, 0).UTC() }

func TestRunTurnSubagentInterceptsOnlyTheCanonicalDescriptor(t *testing.T) {
	canonical := subagenttool.BuiltinDescriptor()
	if !canonicalSubagentDescriptor(canonical) {
		t.Fatal("canonical descriptor was not intercepted")
	}
	forged := protocol.DeepCopy(canonical)
	forged.Body.Description = "lookalike"
	if canonicalSubagentDescriptor(forged) {
		t.Fatal("lookalike descriptor was intercepted")
	}
}

func TestRunTurnSubagentDerivesChildExposureWithoutSubagent(t *testing.T) {
	runtime := validRuntimeManifest(t, "observation")
	runtime.Body.Tools = append(runtime.Body.Tools, subagenttool.BuiltinDescriptor())
	refreshRuntimeDigest(t, &runtime)
	exposure, err := derivedChildExposure(runtime)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range exposure.Tools {
		if tool.Alias == "subagent" {
			t.Fatalf("child exposure leaked subagent: %#v", exposure)
		}
	}
}

func TestRunTurnSubagentHappyPathBindsTerminalReceipt(t *testing.T) {
	manifest := protocol.SubagentManifestV1{
		AttemptID: "attempt", ParentSessionID: "parent", ParentCursor: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "parent", CommitSeq: 1, TransactionID: "parent-tx"},
		ChildSessionID: "child", ChildTaskID: "task", ChildTurnID: "turn", RuntimeGenerationID: "generation", SkillCatalogRevision: "skills", MaxToolCalls: 1,
		Deadline: testSubagentDeadline(),
	}
	cursor := protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child", CommitSeq: 7, TransactionID: "terminal"}
	receipt := childReceipt(manifest, "succeeded", "done", cursor, unknownUsage(), nil, nil)
	if err := receipt.Validate(); err != nil || receipt.TerminalCursor != cursor || receipt.Manifest != manifest {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
}

func TestRunTurnSubagentLimitsAndFailuresRejectInvalidCalls(t *testing.T) {
	for _, raw := range []string{`{}`, `{"task":" "}`, `{"task":"x","extra":true}`} {
		if _, err := decodeSubagentCall(json.RawMessage(raw)); err == nil {
			t.Fatalf("invalid call accepted: %s", raw)
		}
	}
}

func TestRunTurnSubagentHappyPathCommitsParentHandoffAndAttachment(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	request.Runtime.Body.SkillCatalogRevision = "skills-a"
	request.Runtime.Body.Limits.Subagents = protocol.SubagentLimits{Enabled: true, MaxPerTurn: 4, MaxToolCalls: 1, TimeoutNanos: int64(time.Second)}
	request.Runtime.Body.Tools = append(request.Runtime.Body.Tools, subagenttool.BuiltinDescriptor())
	refreshRuntimeDigest(t, &request.Runtime)
	log := &recordLog{}
	repo := &recordingRepository{head: request.ExpectedHead, log: log}
	child := protocol.SubagentManifestV1{AttemptID: "attempt", ParentSessionID: request.SessionID, ParentCursor: request.ExpectedHead, ChildSessionID: "child", ChildTaskID: "child-task", ChildTurnID: "child-turn", RuntimeGenerationID: request.Runtime.ID, SkillCatalogRevision: "skills-a", MaxToolCalls: 1, Deadline: testSubagentDeadline()}
	receipt := childReceipt(child, "succeeded", "child done", protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child", CommitSeq: 2, TransactionID: "child-terminal"}, unknownUsage(), nil, nil)
	children := &subagentTestChildren{receipt: receipt, workspace: domain.Workspace{ID: "workspace", CanonicalPath: "/workspace"}}
	service, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: &loggingLane{delegate: NewOperationLane(), log: log}, Repository: repo,
		TurnLeases: &recordingTurnLeaseManager{log: log}, Context: fakeContextPlanner{log: log}, Providers: fakeProviderCatalog{log: log}, Provider: &subagentTestProvider{log: log},
		Tools: subagentPlanService{}, Authorization: &allowingAuthorization{log: log}, Evidence: recordingEvidence{log: log}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
		ChildSessions: children, ParentSessions: subagentParentInspector{children}, Children: subagentTestCoordinator{children: children, log: log},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	batches := repo.batchKinds()
	if !containsBatchKind(batches, protocol.EventSubagentRequested) || !containsBatchKind(batches, protocol.EventSubagentWaiting) || !containsBatchKind(batches, protocol.EventSubagentResultAttached) {
		t.Fatalf("handoff batches=%v", batches)
	}
	if !containsBatchKind(batches, protocol.EventEvidenceRecorded) {
		t.Fatalf("receipt evidence missing: %v", batches)
	}
}

func TestSequentialChildCoordinatorRunsOrdinaryChildTurnAndReadsReceipt(t *testing.T) {
	parent := validStartTurnRequest()
	parent.Runtime = validRuntimeManifest(t, "observation")
	parent.Runtime.Body.SkillCatalogRevision = "skills-a"
	parent.Runtime.Body.Limits.Subagents = protocol.SubagentLimits{Enabled: true, MaxPerTurn: 4, MaxToolCalls: 1, TimeoutNanos: int64(time.Second)}
	parent.Runtime.Body.Tools = append(parent.Runtime.Body.Tools, subagenttool.BuiltinDescriptor())
	refreshRuntimeDigest(t, &parent.Runtime)
	manifest := protocol.SubagentManifestV1{AttemptID: "production-attempt", ParentSessionID: parent.SessionID, ParentCursor: parent.ExpectedHead, ChildSessionID: "child", ChildTaskID: protocol.TaskID(stableID("task", "child-command")), ChildTurnID: protocol.TurnID(stableID("turn", "child-command")), RuntimeGenerationID: parent.Runtime.ID, SkillCatalogRevision: "skills-a", MaxToolCalls: 1, Deadline: time.Now().Add(time.Second)}
	store := &productionCoordinatorStore{workspace: domain.Workspace{ID: "workspace", CanonicalPath: "/workspace"}}
	childRepo := &recordingRepository{head: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child", CommitSeq: 1, TransactionID: "create"}}
	childLog := &recordLog{}
	childService, err := NewService(Dependencies{Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: &loggingLane{delegate: NewOperationLane(), log: childLog}, Repository: childRepo, TurnLeases: &recordingTurnLeaseManager{log: childLog}, Context: fakeContextPlanner{log: childLog}, Providers: fakeProviderCatalog{log: childLog}, Provider: fakeProviderService{log: childLog}, Tools: noToolService{}, Authorization: &allowingAuthorization{log: childLog}, Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now)})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewSequentialChildCoordinator(store, productionCoordinatorParent{store}, func(ctx context.Context, request StartTurnRequest) (RunResult, error) {
		result, runErr := childService.RunTurn(ctx, request)
		store.capture(childRepo)
		return result, runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := coordinator.RunChild(context.Background(), ChildRunRequest{Manifest: manifest, Call: protocol.SubagentCallV1{Task: "inspect", ExpectedOutput: "report", Context: "scope"}, Parent: parent})
	if err != nil {
		t.Fatalf("coordinator error=%v child inspection=%+v child batches=%v", err, store.inspection, childRepo.batchKinds())
	}
	if store.created != manifest.ChildSessionID || !sameSubagentManifest(receipt.Manifest, manifest) || receipt.Status != "succeeded" {
		t.Fatalf("created=%q receipt=%+v", store.created, receipt)
	}
	if !containsBatchKind(childRepo.batchKinds(), protocol.EventSubagentManifest) || !containsBatchKind(childRepo.batchKinds(), protocol.EventSubagentReceipt) {
		t.Fatalf("child did not own manifest/receipt: %v", childRepo.batchKinds())
	}
}

func TestRunTurnSubagentDisabledRejectsBeforeChildDispatch(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	request.Runtime.Body.SkillCatalogRevision = "skills-a"
	request.Runtime.Body.Limits.Subagents = protocol.SubagentLimits{Enabled: false, MaxPerTurn: 4, MaxToolCalls: 1, TimeoutNanos: int64(time.Second)}
	request.Runtime.Body.Tools = append(request.Runtime.Body.Tools, subagenttool.BuiltinDescriptor())
	refreshRuntimeDigest(t, &request.Runtime)
	log := &recordLog{}
	repo := &recordingRepository{head: request.ExpectedHead, log: log}
	service, err := NewService(Dependencies{Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: &loggingLane{delegate: NewOperationLane(), log: log}, Repository: repo, TurnLeases: &recordingTurnLeaseManager{log: log}, Context: fakeContextPlanner{log: log}, Providers: fakeProviderCatalog{log: log}, Provider: &subagentTestProvider{log: log}, Tools: noToolService{}, Authorization: &allowingAuthorization{log: log}, Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if containsBatchKind(repo.batchKinds(), protocol.EventSubagentRequested) {
		t.Fatalf("disabled subagent was requested: %v", repo.batchKinds())
	}
}

type productionCoordinatorStore struct {
	workspace  domain.Workspace
	created    protocol.SessionID
	inspection journal.Inspection
}

func (s *productionCoordinatorStore) ReserveSessionID() (protocol.SessionID, error) {
	return "child", nil
}
func (s *productionCoordinatorStore) CreateWithIdentity(_ context.Context, id protocol.SessionID, workspace domain.Workspace, _ domain.PermissionMode, _ domain.ModelSelection, lineage *journal.SessionLineage) (domain.Session, error) {
	if lineage == nil || lineage.Kind != journal.LineageSubagent {
		return domain.Session{}, fmt.Errorf("missing subagent lineage")
	}
	s.created = id
	s.workspace = workspace
	s.inspection.Head = protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(id), CommitSeq: 1, TransactionID: "create"}
	return domain.Session{ID: string(id), Workspace: workspace}, nil
}
func (s *productionCoordinatorStore) InspectSession(context.Context, protocol.SessionID) (journal.Inspection, error) {
	return s.inspection, nil
}
func (s *productionCoordinatorStore) capture(repo *recordingRepository) {
	for _, request := range repo.appendRequests() {
		for _, event := range request.Events {
			if event.Kind == protocol.EventSubagentReceipt {
				var receipt protocol.SubagentReceiptV1
				_ = json.Unmarshal(event.Payload, &receipt)
				s.inspection = journal.Inspection{Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(receipt.Manifest.ChildSessionID)}, Head: receipt.TerminalCursor, Writable: true, Events: []protocol.EventRecord{{Envelope: protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(receipt.Manifest.ChildSessionID), SessionID: receipt.Manifest.ChildSessionID, Kind: protocol.EventSubagentReceipt, Seq: receipt.TerminalCursor.CommitSeq, TransactionID: receipt.TerminalCursor.TransactionID, Payload: event.Payload}}}}
			}
		}
	}
}

type productionCoordinatorParent struct{ store *productionCoordinatorStore }

func (s productionCoordinatorParent) InspectSession(context.Context, protocol.SessionID) (journal.SessionInspection, error) {
	return journal.SessionInspection{Session: domain.Session{Workspace: s.store.workspace, Mode: domain.ModeAsk, Selection: domain.ModelSelection{Profile: "provider-a", Model: "model-a"}}}, nil
}

type subagentTestChildren struct {
	receipt   protocol.SubagentReceiptV1
	workspace domain.Workspace
}

func (s *subagentTestChildren) ReserveSessionID() (protocol.SessionID, error) {
	return s.receipt.Manifest.ChildSessionID, nil
}
func (s *subagentTestChildren) CreateWithIdentity(context.Context, protocol.SessionID, domain.Workspace, domain.PermissionMode, domain.ModelSelection, *journal.SessionLineage) (domain.Session, error) {
	return domain.Session{}, nil
}
func (s *subagentTestChildren) InspectSession(context.Context, protocol.SessionID) (journal.Inspection, error) {
	raw, _ := canonicaljson.Marshal(s.receipt)
	return journal.Inspection{Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(s.receipt.Manifest.ChildSessionID)}, Head: s.receipt.TerminalCursor, Writable: true, Events: []protocol.EventRecord{{Envelope: protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(s.receipt.Manifest.ChildSessionID), SessionID: s.receipt.Manifest.ChildSessionID, Kind: protocol.EventSubagentReceipt, Seq: s.receipt.TerminalCursor.CommitSeq, TransactionID: s.receipt.TerminalCursor.TransactionID, Payload: raw}}}}, nil
}
func (s *subagentTestChildren) InspectSessionParent(context.Context, protocol.SessionID) (journal.SessionInspection, error) {
	return journal.SessionInspection{Session: domain.Session{Workspace: s.workspace}}, nil
}

// ParentSessionInspector has the same method name with a different result, so
// this adapter keeps the test harness explicit.
type subagentParentInspector struct{ *subagentTestChildren }

func (s subagentParentInspector) InspectSession(ctx context.Context, id protocol.SessionID) (journal.SessionInspection, error) {
	return s.subagentTestChildren.InspectSessionParent(ctx, id)
}

type subagentTestCoordinator struct {
	children *subagentTestChildren
	log      *recordLog
}

func (s subagentTestCoordinator) RunChild(_ context.Context, request ChildRunRequest) (protocol.SubagentReceiptV1, error) {
	s.log.add("child.terminal")
	s.children.receipt.Manifest = request.Manifest
	s.children.receipt.TerminalCursor.JournalID = protocol.JournalID(request.Manifest.ChildSessionID)
	return s.children.receipt, nil
}

type subagentPlanService struct{ noToolService }

func (subagentPlanService) PlanPreviewInspection(_ context.Context, request tooling.PlanRequest) (tooling.ActionHandle, protocol.ActionPlan, error) {
	return tooling.ActionHandle{}, testActionPlan(request, "observation", "not_applicable"), nil
}

type subagentTestProvider struct {
	log   *recordLog
	calls int
}

func (p *subagentTestProvider) Prepare(context.Context, protocol.ActivityID, string, protocol.ModelRequest, protocol.Digest) (provider.ProviderHandle, error) {
	p.log.add("provider.prepare")
	return provider.ProviderHandle{}, nil
}
func (p *subagentTestProvider) Stream(context.Context, provider.ProviderHandle, authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	p.calls++
	out := make(chan protocol.ModelEvent, 2)
	if p.calls == 1 {
		out <- protocol.ModelEvent{Kind: protocol.ModelEventToolIntent, Sequence: 1, ToolIntent: &protocol.ToolUseBlock{CallID: "delegate", Alias: "subagent", Arguments: json.RawMessage(`{"task":"child","expected_output":"report"}`)}}
		out <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "tool"}}
	} else {
		out <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: "done"}}
		out <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	}
	close(out)
	return out, nil
}
func containsBatchKind(batches [][]string, kind string) bool {
	for _, batch := range batches {
		for _, got := range batch {
			if got == kind {
				return true
			}
		}
	}
	return false
}
