package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/activity"
	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/eventcodec"
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
	var planned, evidence, terminal protocol.ActivityID
	for _, appendRequest := range repo.appendRequests() {
		for _, event := range appendRequest.Events {
			switch event.Kind {
			case protocol.EventActivityPlanned:
				if strings.Contains(string(event.Payload), "sequential child orchestration") {
					planned = event.ActivityID
				}
			case protocol.EventEvidenceRecorded:
				evidence = event.ActivityID
			case protocol.EventActivitySucceeded:
				if planned != "" && event.ActivityID == planned {
					terminal = event.ActivityID
				}
			}
		}
	}
	if planned == "" || planned != evidence || planned != terminal {
		t.Fatalf("subagent activity planned=%q evidence=%q terminal=%q", planned, evidence, terminal)
	}
	projector := activity.Projector{}
	projection := projector.Zero(protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)})
	for _, record := range subagentActivityRecords(t, repo.appendRequests(), request.SessionID, planned) {
		var err error
		projection, err = projector.Apply(projection, record)
		if err != nil {
			t.Fatalf("replay parent handoff %s: %v", record.Envelope.Kind, err)
		}
	}
	replayed, ok := projection.Activities[planned]
	if !ok || replayed.State != activity.StateSucceeded || len(replayed.OutputEvidenceIDs) != 1 {
		t.Fatalf("replayed handoff activity=%+v", replayed)
	}
}

// subagentActivityRecords turns the exact persisted parent candidates into the
// stateful records consumed by the activity projector.  Filtering is deliberate:
// the projector ignores all other foundation events, while this regression
// verifies that the handoff's own lifecycle and evidence linkage replay.
func subagentActivityRecords(t *testing.T, requests []journal.AppendRequest, sessionID protocol.SessionID, activityID protocol.ActivityID) []protocol.EventRecord {
	t.Helper()
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	var records []protocol.EventRecord
	var sequence uint64 = 1
	for _, request := range requests {
		for _, event := range request.Events {
			if event.ActivityID != activityID || (event.Kind != protocol.EventActivityPlanned && event.Kind != protocol.EventActivityAuthorized && event.Kind != protocol.EventActivityStarted && event.Kind != protocol.EventActivitySucceeded && event.Kind != protocol.EventEvidenceLinked) {
				sequence++
				continue
			}
			envelope := protocol.EventEnvelope{
				SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: event.PayloadVersion,
				JournalKind: request.Journal.Kind, JournalID: request.Journal.ID, SessionID: sessionID,
				EventID: event.EventID, Seq: sequence, Time: event.Time, Kind: event.Kind,
				TaskID: event.TaskID, TurnID: event.TurnID, ActivityID: event.ActivityID,
				ParentActivityID: event.ParentActivityID, CausationEventID: event.CausationEventID,
				Actor: event.Actor, RuntimeGenerationID: event.RuntimeGenerationID, TransactionID: request.TransactionID,
				Payload: event.Payload,
			}
			raw, marshalErr := canonicaljson.Marshal(envelope)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			record, decodeErr := registry.Decode(raw)
			if decodeErr != nil {
				t.Fatalf("decode persisted %s: %v", event.Kind, decodeErr)
			}
			records = append(records, record)
			sequence++
		}
	}
	return records
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
	childCatalog := &capturingProviderCatalog{log: childLog}
	childProvider := &capturingFinalProvider{log: childLog}
	childService, err := NewService(Dependencies{Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: &loggingLane{delegate: NewOperationLane(), log: childLog}, Repository: childRepo, TurnLeases: &recordingTurnLeaseManager{log: childLog}, Context: fakeContextPlanner{log: childLog}, Providers: childCatalog, Provider: childProvider, Tools: noToolService{}, Authorization: &allowingAuthorization{log: childLog}, Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now)})
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
	if childCatalog.revision != childProvider.request.Tools.CatalogRevision || childCatalog.revision == parent.Runtime.Body.ToolCatalogRevision {
		t.Fatalf("child catalog negotiation=%q exposure=%q parent=%q", childCatalog.revision, childProvider.request.Tools.CatalogRevision, parent.Runtime.Body.ToolCatalogRevision)
	}
}

func TestRunTurnSubagentUsesRealSequentialCoordinatorAndContinuesParent(t *testing.T) {
	parent := validStartTurnRequest()
	parent.Runtime = validRuntimeManifest(t, "observation")
	parent.Runtime.Body.SkillCatalogRevision = "skills-a"
	parent.Runtime.Body.Limits.MaxToolCalls = 5
	parent.Runtime.Body.Limits.Subagents = protocol.SubagentLimits{Enabled: true, MaxPerTurn: 4, MaxToolCalls: 1, TimeoutNanos: int64(time.Second)}
	parent.Runtime.Body.Tools = append(parent.Runtime.Body.Tools, subagenttool.BuiltinDescriptor())
	refreshRuntimeDigest(t, &parent.Runtime)

	log := &recordLog{}
	parentRepo := &recordingRepository{head: parent.ExpectedHead, log: log}
	parentRepo.trace = func(request journal.AppendRequest) {
		if appendHasKinds(request, protocol.EventSubagentRequested, protocol.EventSubagentWaiting) {
			log.add("parent.request_wait.commit")
		}
		if appendHasKinds(request, protocol.EventEvidenceRecorded, protocol.EventSubagentResultAttached) {
			log.add("parent.evidence_attachment.commit")
		}
		if appendHasKinds(request, protocol.EventTurnCompleted, protocol.EventCommandCompleted) {
			log.add("parent.terminal.commit")
		}
	}
	childRepo := &recordingRepository{head: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child", CommitSeq: 1, TransactionID: "create"}, log: log}
	childRepo.trace = func(request journal.AppendRequest) {
		if appendHasKinds(request, protocol.EventSubagentManifest) {
			log.add("child.manifest.commit")
		}
		if appendHasKinds(request, protocol.EventSubagentReceipt) {
			log.add("child.receipt.commit")
		}
	}
	store := &productionCoordinatorStore{workspace: domain.Workspace{ID: "workspace", CanonicalPath: "/workspace"}, log: log}
	childCatalog := &capturingProviderCatalog{log: log}
	childProvider := &capturingFinalProvider{log: log, name: "child"}
	childService, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: &loggingLane{delegate: NewOperationLane(), log: log, name: "child"}, Repository: childRepo,
		TurnLeases: &recordingTurnLeaseManager{log: log}, Context: fakeContextPlanner{log: log}, Providers: childCatalog, Provider: childProvider,
		Tools: noToolService{}, Authorization: &allowingAuthorization{log: log}, Evidence: noEvidenceRecorder{}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
	})
	if err != nil {
		t.Fatal(err)
	}
	parentProvider := &subagentTestProvider{log: log}
	parentProvider.onPrepare = func(index int, _ protocol.ModelRequest) {
		if index == 2 {
			log.add("parent.provider.second.prepare")
		}
	}
	parentProvider.onStream = func(index int) {
		if index == 2 {
			log.add("parent.provider.second.stream")
		}
	}
	evidence := &capturingEvidence{log: log}
	parentService, err := NewService(Dependencies{
		Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: &loggingLane{delegate: NewOperationLane(), log: log, name: "parent"}, Repository: parentRepo,
		TurnLeases: &recordingTurnLeaseManager{log: log}, Context: fakeContextPlanner{log: log}, Providers: fakeProviderCatalog{log: log}, Provider: parentProvider,
		Tools: subagentPlanService{}, Authorization: &allowingAuthorization{log: log}, Evidence: evidence, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now),
		ChildSessions: store, ParentSessions: productionCoordinatorParent{store},
	})
	if err != nil {
		t.Fatal(err)
	}
	var childStart StartTurnRequest
	coordinator, err := NewSequentialChildCoordinator(store, productionCoordinatorParent{store}, func(ctx context.Context, request StartTurnRequest) (RunResult, error) {
		childStart = protocol.DeepCopy(request)
		result, runErr := childService.RunTurn(ctx, request)
		store.capture(childRepo)
		return result, runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	parentService.SetChildCoordinator(coordinator)

	if _, err := parentService.RunTurn(context.Background(), parent); err != nil {
		t.Fatalf("parent turn: %v", err)
	}
	if parentProvider.calls != 2 {
		t.Fatalf("parent provider calls=%d want continuation after child", parentProvider.calls)
	}
	if !containsBatchKind(childRepo.batchKinds(), protocol.EventSubagentManifest) || !containsBatchKind(childRepo.batchKinds(), protocol.EventSubagentReceipt) {
		t.Fatalf("child journal missing lifecycle events: %v", childRepo.batchKinds())
	}
	if containsBatchKind(parentRepo.batchKinds(), protocol.EventSubagentManifest) || containsBatchKind(parentRepo.batchKinds(), protocol.EventSubagentReceipt) {
		t.Fatalf("child lifecycle leaked to parent journal: %v", parentRepo.batchKinds())
	}
	if !containsBatchKind(parentRepo.batchKinds(), protocol.EventSubagentRequested) || !containsBatchKind(parentRepo.batchKinds(), protocol.EventSubagentWaiting) || !containsBatchKind(parentRepo.batchKinds(), protocol.EventSubagentResultAttached) || !containsBatchKind(parentRepo.batchKinds(), protocol.EventEvidenceRecorded) {
		t.Fatalf("parent handoff incomplete: %v", parentRepo.batchKinds())
	}
	if childCatalog.revision != childProvider.request.Tools.CatalogRevision || childCatalog.revision == parent.Runtime.Body.ToolCatalogRevision {
		t.Fatalf("child provider exposure revision negotiated=%q request=%q parent=%q", childCatalog.revision, childProvider.request.Tools.CatalogRevision, parent.Runtime.Body.ToolCatalogRevision)
	}
	if childProvider.request.ProviderID != parent.ProviderID || childProvider.request.ModelID != parent.ModelID {
		t.Fatalf("child provider/model=%q/%q want frozen %q/%q", childProvider.request.ProviderID, childProvider.request.ModelID, parent.ProviderID, parent.ModelID)
	}
	if childStart.Runtime.ID != parent.Runtime.ID || childStart.Runtime.Digest != parent.Runtime.Digest || childStart.Runtime.Body.SkillCatalogRevision != parent.Runtime.Body.SkillCatalogRevision || childStart.child == nil {
		t.Fatalf("child runtime was not frozen from parent: %+v", childStart)
	}
	if childStart.child.exposure.CatalogRevision != childProvider.request.Tools.CatalogRevision || len(childStart.child.exposure.Tools) != len(parent.Runtime.Body.Tools)-1 {
		t.Fatalf("child exposure=%+v request=%+v", childStart.child.exposure, childProvider.request.Tools)
	}
	childReceipt, ok := receiptFromRepo(childRepo)
	if !ok {
		t.Fatal("child receipt was not committed")
	}
	if childReceipt.Manifest.RuntimeGenerationID != parent.Runtime.ID || childReceipt.Manifest.SkillCatalogRevision != parent.Runtime.Body.SkillCatalogRevision || childReceipt.Manifest.ParentSessionID != parent.SessionID {
		t.Fatalf("child manifest did not preserve frozen parent identity: %+v", childReceipt.Manifest)
	}
	attachment, ok := subagentAttachment(parentRepo)
	if !ok {
		t.Fatal("parent attachment was not committed")
	}
	receiptDigest, err := canonicaljson.Digest(childReceipt)
	if err != nil {
		t.Fatal(err)
	}
	if attachment.TerminalCursor != childReceipt.TerminalCursor || attachment.ReceiptDigest != receiptDigest || attachment.ReceiptEvidenceID == "" {
		t.Fatalf("attachment=%+v receipt=%+v", attachment, childReceipt)
	}
	canonicalReceipt, err := canonicaljson.Marshal(childReceipt)
	if err != nil {
		t.Fatal(err)
	}
	if attachment.ReceiptEvidenceID != evidence.record.Body.ID || evidence.candidate.ID != evidence.record.Body.ID || string(evidence.candidate.Content) != string(canonicalReceipt) || evidence.candidate.WorkspaceID != protocol.WorkspaceID(store.workspace.ID) || evidence.candidate.SessionID != parent.SessionID || evidence.candidate.Subject != (protocol.SubjectRef{Kind: "subagent_attempt", ID: string(childReceipt.Manifest.AttemptID)}) {
		t.Fatalf("receipt evidence candidate=%+v record=%+v attachment=%+v", evidence.candidate, evidence.record, attachment)
	}
	plannedActivity := subagentPlannedActivityID(t, parentRepo)
	if evidence.candidate.ProducingActivityID != plannedActivity {
		t.Fatalf("evidence activity=%q planned subagent activity=%q", evidence.candidate.ProducingActivityID, plannedActivity)
	}
	if len(parentProvider.requests) != 2 {
		t.Fatalf("parent provider requests=%d", len(parentProvider.requests))
	}
	continuation, ok := findToolResult(parentProvider.requests[1], "delegate")
	if !ok || continuation.Status != childReceipt.Status || string(continuation.JSON) != string(canonicalReceipt) || len(continuation.EvidenceIDs) != 1 || continuation.EvidenceIDs[0] != attachment.ReceiptEvidenceID {
		t.Fatalf("continuation result=%+v receipt=%+v attachment=%+v", continuation, childReceipt, attachment)
	}
	var continuedReceipt protocol.SubagentReceiptV1
	if err := json.Unmarshal(continuation.JSON, &continuedReceipt); err != nil || !sameSubagentManifest(continuedReceipt.Manifest, childReceipt.Manifest) || continuedReceipt.TerminalCursor != attachment.TerminalCursor {
		t.Fatalf("continuation receipt=%+v err=%v", continuedReceipt, err)
	}
	continuedDigest, err := canonicaljson.Digest(continuedReceipt)
	if err != nil || continuedDigest != attachment.ReceiptDigest {
		t.Fatalf("continuation receipt digest=%+v attachment=%+v err=%v", continuedDigest, attachment, err)
	}
	entries := log.snapshot()
	assertStrictTrace(t, entries, "parent.request_wait.commit", "parent.lane.release", "child.session.create", "child.manifest.commit", "child.provider.prepare", "child.receipt.commit", "child.lane.release", "parent.lane.acquire(turn)", "parent.evidence_attachment.commit", "parent.provider.second.prepare", "parent.provider.second.stream", "parent.terminal.commit")
}

func appendHasKinds(request journal.AppendRequest, kinds ...string) bool {
	for _, kind := range kinds {
		found := false
		for _, event := range request.Events {
			if event.Kind == kind {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func subagentPlannedActivityID(t *testing.T, repo *recordingRepository) protocol.ActivityID {
	t.Helper()
	for _, request := range repo.appendRequests() {
		for _, event := range request.Events {
			if event.Kind == protocol.EventActivityPlanned && strings.Contains(string(event.Payload), "sequential child orchestration") {
				return event.ActivityID
			}
		}
	}
	t.Fatal("subagent activity planned event was not committed")
	return ""
}

func findToolResult(request protocol.ModelRequest, callID string) (protocol.ToolResultBlock, bool) {
	for _, message := range request.Messages {
		for _, block := range message.Blocks {
			if block.Kind == protocol.ContentToolResult && block.ToolResult != nil && block.ToolResult.CallID == callID {
				return protocol.DeepCopy(*block.ToolResult), true
			}
		}
	}
	return protocol.ToolResultBlock{}, false
}

func assertStrictTrace(t *testing.T, entries []string, expected ...string) {
	t.Helper()
	previous := -1
	for _, value := range expected {
		index := -1
		for cursor := previous + 1; cursor < len(entries); cursor++ {
			if entries[cursor] == value {
				index = cursor
				break
			}
		}
		if index == -1 {
			t.Fatalf("trace missing or out of order %q after %d: %v", value, previous, entries)
		}
		previous = index
	}
}

func subagentAttachment(repo *recordingRepository) (protocol.SubagentResultAttachedV1, bool) {
	for _, request := range repo.appendRequests() {
		for _, event := range request.Events {
			if event.Kind != protocol.EventSubagentResultAttached {
				continue
			}
			var attachment protocol.SubagentResultAttachedV1
			if json.Unmarshal(event.Payload, &attachment) == nil {
				return attachment, true
			}
		}
	}
	return protocol.SubagentResultAttachedV1{}, false
}

func indexOf(values []string, want string) int {
	for index, value := range values {
		if value == want {
			return index
		}
	}
	return -1
}

type capturingProviderCatalog struct {
	log      *recordLog
	revision string
}

func (c *capturingProviderCatalog) Resolve(protocol.ProviderID, protocol.ModelID) (protocol.ModelDescriptor, bool) {
	return protocol.ModelDescriptor{}, false
}
func (c *capturingProviderCatalog) Negotiate(providerID protocol.ProviderID, modelID protocol.ModelID, requirements []protocol.CapabilityRequirement, revision string) (protocol.NegotiatedProviderPlan, error) {
	c.revision = revision
	return fakeProviderCatalog{log: c.log}.Negotiate(providerID, modelID, requirements, revision)
}

type capturingFinalProvider struct {
	log     *recordLog
	name    string
	request protocol.ModelRequest
}

func (p *capturingFinalProvider) Prepare(_ context.Context, _ protocol.ActivityID, _ string, request protocol.ModelRequest, _ protocol.Digest) (provider.ProviderHandle, error) {
	p.request = protocol.DeepCopy(request)
	if p.name != "" {
		p.log.add(p.name + ".provider.prepare")
	} else {
		p.log.add("provider.prepare")
	}
	return provider.ProviderHandle{}, nil
}
func (p *capturingFinalProvider) Stream(context.Context, provider.ProviderHandle, authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	if p.name != "" {
		p.log.add(p.name + ".provider.stream")
	}
	stream := make(chan protocol.ModelEvent, 2)
	stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: "done"}}
	stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	close(stream)
	return stream, nil
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

func TestRunTurnSubagentParentHeadMismatchWritesNoEvidence(t *testing.T) {
	request := validStartTurnRequest()
	request.Runtime = validRuntimeManifest(t, "observation")
	request.Runtime.Body.SkillCatalogRevision = "skills-a"
	request.Runtime.Body.Limits.Subagents = protocol.SubagentLimits{Enabled: true, MaxPerTurn: 4, MaxToolCalls: 1, TimeoutNanos: int64(time.Second)}
	request.Runtime.Body.Tools = append(request.Runtime.Body.Tools, subagenttool.BuiltinDescriptor())
	refreshRuntimeDigest(t, &request.Runtime)
	log := &recordLog{}
	repo := &recordingRepository{head: request.ExpectedHead, log: log}
	seed := protocol.SubagentManifestV1{AttemptID: "seed", ParentSessionID: request.SessionID, ParentCursor: request.ExpectedHead, ChildSessionID: "child", ChildTaskID: "task", ChildTurnID: "turn", RuntimeGenerationID: request.Runtime.ID, SkillCatalogRevision: "skills-a", MaxToolCalls: 1, Deadline: testSubagentDeadline()}
	children := &subagentTestChildren{receipt: childReceipt(seed, "succeeded", "done", protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child", CommitSeq: 2, TransactionID: "t"}, unknownUsage(), nil, nil), workspace: domain.Workspace{ID: "workspace", CanonicalPath: "/workspace"}}
	coordinator := headChangingCoordinator{children: children, repo: repo, log: log}
	service, err := NewService(Dependencies{Admission: passthroughAdmission{}, Instructions: emptyInstructionService{}, Lane: NewOperationLane(), Repository: repo, TurnLeases: &recordingTurnLeaseManager{}, Context: fakeContextPlanner{log: log}, Providers: fakeProviderCatalog{log: log}, Provider: &subagentTestProvider{log: log}, Tools: subagentPlanService{}, Authorization: &allowingAuthorization{log: log}, Evidence: recordingEvidence{log: log}, Recovery: noRecoveryRecorder{}, Verification: verification.NewService(time.Now), ChildSessions: children, ParentSessions: subagentParentInspector{children}, Children: coordinator})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunTurn(context.Background(), request); err == nil {
		t.Fatal("head mismatch unexpectedly succeeded")
	}
	if countPrefix(log.snapshot(), "evidence.put") != 0 {
		t.Fatalf("evidence persisted after head mismatch: %v", log.snapshot())
	}
}

func TestSequentialChildCoordinatorCancellationReadsCancelledReceipt(t *testing.T) {
	parent := validStartTurnRequest()
	parent.Runtime = validRuntimeManifest(t, "observation")
	parent.Runtime.Body.SkillCatalogRevision = "skills-a"
	parent.Runtime.Body.Limits.Subagents = protocol.SubagentLimits{Enabled: true, MaxPerTurn: 4, MaxToolCalls: 1, TimeoutNanos: int64(time.Second)}
	parent.Runtime.Body.Tools = append(parent.Runtime.Body.Tools, subagenttool.BuiltinDescriptor())
	refreshRuntimeDigest(t, &parent.Runtime)
	manifest := protocol.SubagentManifestV1{AttemptID: "cancel", ParentSessionID: parent.SessionID, ParentCursor: parent.ExpectedHead, ChildSessionID: "child", ChildTaskID: protocol.TaskID(stableID("task", "cancel")), ChildTurnID: protocol.TurnID(stableID("turn", "cancel")), RuntimeGenerationID: parent.Runtime.ID, SkillCatalogRevision: "skills-a", MaxToolCalls: 1, Deadline: time.Now().Add(time.Second)}
	store := &productionCoordinatorStore{workspace: domain.Workspace{ID: "workspace", CanonicalPath: "/workspace"}}
	coordinator, err := NewSequentialChildCoordinator(store, productionCoordinatorParent{store}, func(_ context.Context, request StartTurnRequest) (RunResult, error) {
		receipt := childReceipt(request.child.manifest, "cancelled", "cancelled", protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(request.SessionID), CommitSeq: 2, TransactionID: "cancel"}, unknownUsage(), &protocol.PublicError{Code: "cancelled", Message: "cancelled"}, nil)
		raw, _ := canonicaljson.Marshal(receipt)
		store.inspection = journal.Inspection{Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)}, Head: receipt.TerminalCursor, Events: []protocol.EventRecord{{Envelope: protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(request.SessionID), SessionID: request.SessionID, Kind: protocol.EventSubagentReceipt, Seq: 2, TransactionID: "cancel", Payload: raw}}}}
		return RunResult{}, context.Canceled
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	receipt, err := coordinator.RunChild(ctx, ChildRunRequest{Manifest: manifest, Call: protocol.SubagentCallV1{Task: "child"}, Parent: parent})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Status != "cancelled" {
		t.Fatalf("receipt=%+v", receipt)
	}
}

type headChangingCoordinator struct {
	children *subagentTestChildren
	repo     *recordingRepository
	log      *recordLog
}

func (s headChangingCoordinator) RunChild(ctx context.Context, request ChildRunRequest) (protocol.SubagentReceiptV1, error) {
	receipt, err := subagentTestCoordinator{children: s.children, log: s.log}.RunChild(ctx, request)
	s.repo.mu.Lock()
	s.repo.head.CommitSeq++
	s.repo.mu.Unlock()
	return receipt, err
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
	log        *recordLog
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
	if s.log != nil {
		s.log.add("child.session.create")
	}
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
	log       *recordLog
	calls     int
	requests  []protocol.ModelRequest
	onPrepare func(int, protocol.ModelRequest)
	onStream  func(int)
}

func (p *subagentTestProvider) Prepare(_ context.Context, _ protocol.ActivityID, _ string, request protocol.ModelRequest, _ protocol.Digest) (provider.ProviderHandle, error) {
	p.requests = append(p.requests, protocol.DeepCopy(request))
	p.log.add("provider.prepare")
	if p.onPrepare != nil {
		p.onPrepare(len(p.requests), protocol.DeepCopy(request))
	}
	return provider.ProviderHandle{}, nil
}
func (p *subagentTestProvider) Stream(context.Context, provider.ProviderHandle, authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	p.calls++
	if p.onStream != nil {
		p.onStream(p.calls)
	}
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

type capturingEvidence struct {
	log       *recordLog
	candidate protocol.EvidenceCandidate
	record    protocol.EvidenceRecord
}

func (e *capturingEvidence) Put(ctx context.Context, candidate protocol.EvidenceCandidate) (protocol.EvidenceRecord, error) {
	e.candidate = protocol.DeepCopy(candidate)
	record, err := (recordingEvidence{log: e.log}).Put(ctx, candidate)
	if err != nil {
		return protocol.EvidenceRecord{}, err
	}
	e.record = protocol.DeepCopy(record)
	return record, nil
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
