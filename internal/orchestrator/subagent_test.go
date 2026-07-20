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
	request.Runtime.Body.Limits.MaxToolCalls = 5
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

func TestRunTurnSubagentAttemptsOneThroughFourThenRejectsFifth(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	request.Runtime.Body.Limits.MaxToolCalls = 5
	request.Runtime.Body.SkillCatalogRevision = "skills-a"
	request.Runtime.Body.Limits.Subagents = protocol.SubagentLimits{Enabled: true, MaxPerTurn: 4, MaxToolCalls: 1, TimeoutNanos: int64(time.Second)}
	request.Runtime.Body.Tools = append(request.Runtime.Body.Tools, subagenttool.BuiltinDescriptor())
	refreshRuntimeDigest(t, &request.Runtime)
	log := &recordLog{}
	repo := &recordingRepository{head: request.ExpectedHead, log: log}
	child := protocol.SubagentManifestV1{AttemptID: "seed", ParentSessionID: request.SessionID, ParentCursor: request.ExpectedHead, ChildSessionID: "child", ChildTaskID: "child-task", ChildTurnID: "child-turn", RuntimeGenerationID: request.Runtime.ID, SkillCatalogRevision: "skills-a", MaxToolCalls: 1, Deadline: testSubagentDeadline()}
	children := &subagentTestChildren{receipt: childReceipt(child, "succeeded", "done", protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child", CommitSeq: 2, TransactionID: "child-terminal"}, unknownUsage(), nil, nil), workspace: domain.Workspace{ID: "workspace", CanonicalPath: "/workspace"}}
	service, err := NewService(Dependencies{Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repo, TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: log}, Providers: fakeProviderCatalog{log: log}, Provider: &manySubagentProvider{}, Tools: subagentPlanService{}, Authorization: &allowingAuthorization{log: log}, Evidence: recordingEvidence{log: log}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now), ChildSessions: children, ParentSessions: subagentParentInspector{children}, Children: subagentTestCoordinator{children: children, log: log}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if got := countBatchKind(repo.batchKinds(), protocol.EventSubagentRequested); got != 4 {
		t.Fatalf("requests=%d batches=%v", got, repo.batchKinds())
	}
	if children.reservations != 4 {
		t.Fatalf("reserved child IDs=%d", children.reservations)
	}
	if len(children.attempts) != 4 || children.attempts[0].AttemptID == children.attempts[1].AttemptID || children.attempts[0].ChildSessionID == children.attempts[1].ChildSessionID {
		t.Fatalf("attempt identities=%+v", children.attempts)
	}
}

func TestRunTurnSubagentChildDepthRejectsHiddenDelegation(t *testing.T) {
	runtime := validRuntimeManifest(t, "observation")
	runtime.Body.SkillCatalogRevision = "skills-a"
	runtime.Body.Limits.Subagents = protocol.SubagentLimits{Enabled: true, MaxPerTurn: 4, MaxToolCalls: 1, TimeoutNanos: int64(time.Second)}
	runtime.Body.Tools = append(runtime.Body.Tools, subagenttool.BuiltinDescriptor())
	refreshRuntimeDigest(t, &runtime)
	manifest := protocol.SubagentManifestV1{AttemptID: "depth", ParentSessionID: "parent", ParentCursor: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "parent", CommitSeq: 1, TransactionID: "p"}, ChildSessionID: "child", ChildTaskID: protocol.TaskID(stableID("task", "depth")), ChildTurnID: protocol.TurnID(stableID("turn", "depth")), RuntimeGenerationID: runtime.ID, SkillCatalogRevision: "skills-a", MaxToolCalls: 1, Deadline: time.Now().Add(time.Second)}
	repo := &recordingRepository{head: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child", CommitSeq: 1, TransactionID: "create"}}
	service, err := NewService(Dependencies{Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repo, TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: &recordLog{}}, Providers: fakeProviderCatalog{log: &recordLog{}}, Provider: &subagentTestProvider{log: &recordLog{}}, Tools: noToolService{}, Authorization: &allowingAuthorization{log: &recordLog{}}, Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now)})
	if err != nil {
		t.Fatal(err)
	}
	request := validStartTurnRequest()
	request.Command.CommandID = "depth-command"
	request.Command.IdempotencyKey = "depth-command"
	request.Command.RequestDigest, _ = canonicaljson.Digest("depth")
	request.SessionID = "child"
	request.ExpectedHead = repo.head
	request.Runtime = runtime
	request.child = &childTurnConfig{manifest: manifest, exposure: mustChildExposure(t, runtime)}
	if _, err := service.RunTurn(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if containsBatchKind(repo.batchKinds(), protocol.EventSubagentRequested) {
		t.Fatalf("nested child delegation escaped depth limit: %v", repo.batchKinds())
	}
}

func TestRunTurnSubagentChildToolCallLimitTerminalizesReceipt(t *testing.T) {
	runtime := validRuntimeManifest(t, "observation")
	runtime.Body.SkillCatalogRevision = "skills-a"
	runtime.Body.Limits.Subagents = protocol.SubagentLimits{Enabled: true, MaxPerTurn: 4, MaxToolCalls: 1, TimeoutNanos: int64(time.Second)}
	runtime.Body.Tools = append(runtime.Body.Tools, subagenttool.BuiltinDescriptor())
	refreshRuntimeDigest(t, &runtime)
	manifest := protocol.SubagentManifestV1{AttemptID: "cap", ParentSessionID: "parent", ParentCursor: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "parent", CommitSeq: 1, TransactionID: "p"}, ChildSessionID: "child", ChildTaskID: protocol.TaskID(stableID("task", "cap")), ChildTurnID: protocol.TurnID(stableID("turn", "cap")), RuntimeGenerationID: runtime.ID, SkillCatalogRevision: "skills-a", MaxToolCalls: 1, Deadline: time.Now().Add(time.Second)}
	repo := &recordingRepository{head: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child", CommitSeq: 1, TransactionID: "create"}}
	service, err := NewService(Dependencies{Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repo, TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: &recordLog{}}, Providers: fakeProviderCatalog{log: &recordLog{}}, Provider: &twoToolThenFinalProvider{log: &recordLog{}}, Tools: noToolService{}, Authorization: &allowingAuthorization{log: &recordLog{}}, Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now)})
	if err != nil {
		t.Fatal(err)
	}
	request := validStartTurnRequest()
	request.Command.CommandID = "cap-command"
	request.Command.IdempotencyKey = "cap-command"
	request.Command.RequestDigest, _ = canonicaljson.Digest("cap")
	request.SessionID = "child"
	request.ExpectedHead = repo.head
	request.Runtime = runtime
	request.child = &childTurnConfig{manifest: manifest, exposure: mustChildExposure(t, runtime)}
	if _, err := service.RunTurn(context.Background(), request); err == nil {
		t.Fatal("child accepted two tool calls against max one")
	}
	receipt, ok := receiptFromRepo(repo)
	if !ok || receipt.Status != "failed" {
		t.Fatalf("receipt=%+v ok=%v", receipt, ok)
	}
}

func TestRunTurnSubagentHardDenyPreventsChildReservation(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	request.Runtime.Body.SkillCatalogRevision = "skills-a"
	request.Runtime.Body.Limits.Subagents = protocol.SubagentLimits{Enabled: true, MaxPerTurn: 4, MaxToolCalls: 1, TimeoutNanos: int64(time.Second)}
	request.Runtime.Body.Tools = append(request.Runtime.Body.Tools, subagenttool.BuiltinDescriptor())
	refreshRuntimeDigest(t, &request.Runtime)
	log := &recordLog{}
	repo := &recordingRepository{head: request.ExpectedHead, log: log}
	child := protocol.SubagentManifestV1{AttemptID: "seed", ParentSessionID: request.SessionID, ParentCursor: request.ExpectedHead, ChildSessionID: "child", ChildTaskID: "child-task", ChildTurnID: "child-turn", RuntimeGenerationID: request.Runtime.ID, SkillCatalogRevision: "skills-a", MaxToolCalls: 1, Deadline: testSubagentDeadline()}
	children := &subagentTestChildren{receipt: childReceipt(child, "succeeded", "done", protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child", CommitSeq: 2, TransactionID: "t"}, unknownUsage(), nil, nil), workspace: domain.Workspace{ID: "workspace", CanonicalPath: "/workspace"}}
	service, err := NewService(Dependencies{Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repo, TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: log}, Providers: fakeProviderCatalog{log: log}, Provider: &subagentTestProvider{log: log}, Tools: subagentPlanService{}, Authorization: denyingSubagentAuthorization{}, Evidence: recordingEvidence{log: log}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now), ChildSessions: children, ParentSessions: subagentParentInspector{children}, Children: subagentTestCoordinator{children: children, log: log}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err == nil {
		t.Fatal("hard deny unexpectedly succeeded")
	}
	if children.reservations != 0 {
		t.Fatalf("deny reserved %d children", children.reservations)
	}
}

type denyingSubagentAuthorization struct{}

func (denyingSubagentAuthorization) Decide(_ context.Context, request protocol.AuthorizationRequest) (protocol.AuthorizationDecision, error) {
	return protocol.AuthorizationDecision{Request: request, Action: "deny", Scope: protocol.CanonicalAuthorizationScope{Capability: request.Action, Source: request.Source, Resources: request.Resources, Constraints: []protocol.AuthorizationConstraint{}}, Constraints: []protocol.AuthorizationConstraint{}, Lifetime: protocol.AuthorizationLifetimeOnce, PolicySource: "test", PolicyGeneration: request.PolicyGeneration, Reason: "hard deny", DecidedAt: time.Now().UTC(), PlanDigest: request.PlanDigest, DecisionNonce: "deny"}, nil
}
func (denyingSubagentAuthorization) ResolveInteractive(context.Context, protocol.AuthorizationRequest, protocol.AuthorizationDecision, protocol.ApprovalResponse) (protocol.AuthorizationDecision, error) {
	return protocol.AuthorizationDecision{}, fmt.Errorf("unexpected")
}
func (denyingSubagentAuthorization) Issue(context.Context, authorization.CommitReference) (authorization.CommittedToken, error) {
	return authorization.CommittedToken{}, fmt.Errorf("unexpected")
}
func (denyingSubagentAuthorization) Dispatch(context.Context, authorization.CommittedToken, authorization.DispatchBinding, func(context.Context) error) error {
	return fmt.Errorf("unexpected")
}

func receiptFromRepo(repo *recordingRepository) (protocol.SubagentReceiptV1, bool) {
	for _, request := range repo.appendRequests() {
		for _, event := range request.Events {
			if event.Kind == protocol.EventSubagentReceipt {
				var receipt protocol.SubagentReceiptV1
				if json.Unmarshal(event.Payload, &receipt) == nil {
					return receipt, true
				}
			}
		}
	}
	return protocol.SubagentReceiptV1{}, false
}

func mustChildExposure(t *testing.T, runtime protocol.RuntimeGenerationManifest) protocol.ToolExposure {
	t.Helper()
	exposure, err := derivedChildExposure(runtime)
	if err != nil {
		t.Fatal(err)
	}
	return exposure
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
	receipt      protocol.SubagentReceiptV1
	workspace    domain.Workspace
	reservations int
	attempts     []protocol.SubagentManifestV1
}

func (s *subagentTestChildren) ReserveSessionID() (protocol.SessionID, error) {
	s.reservations++
	return protocol.SessionID(fmt.Sprintf("child-%d", s.reservations)), nil
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
	s.children.attempts = append(s.children.attempts, request.Manifest)
	s.children.receipt.Manifest = request.Manifest
	s.children.receipt.TerminalCursor.JournalID = protocol.JournalID(request.Manifest.ChildSessionID)
	return s.children.receipt, nil
}

type manySubagentProvider struct{ calls int }

func (p *manySubagentProvider) Prepare(context.Context, protocol.ActivityID, string, protocol.ModelRequest, protocol.Digest) (provider.ProviderHandle, error) {
	return provider.ProviderHandle{}, nil
}
func (p *manySubagentProvider) Stream(context.Context, provider.ProviderHandle, authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	p.calls++
	out := make(chan protocol.ModelEvent, 6)
	if p.calls == 1 {
		for i := 1; i <= 5; i++ {
			out <- protocol.ModelEvent{Kind: protocol.ModelEventToolIntent, Sequence: uint64(i), ToolIntent: &protocol.ToolUseBlock{CallID: fmt.Sprintf("delegate-%d", i), Alias: "subagent", Arguments: json.RawMessage(`{"task":"child"}`)}}
		}
		out <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 6, Terminal: &protocol.ModelTerminal{Reason: "tool"}}
	} else {
		out <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: "done"}}
		out <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	}
	close(out)
	return out, nil
}
func countBatchKind(batches [][]string, kind string) int {
	count := 0
	for _, batch := range batches {
		for _, got := range batch {
			if got == kind {
				count++
			}
		}
	}
	return count
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
