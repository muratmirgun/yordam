package task_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/protocol"
	taskprojection "github.com/muratmirgun/yordam/internal/task"
)

func TestLifecycleTaskAndReopenTransitions(t *testing.T) {
	terminals := []taskprojection.State{
		taskprojection.StateVerified, taskprojection.StateCompletedWithWaivers, taskprojection.StatePartial,
		taskprojection.StateFailed, taskprojection.StateUnknown, taskprojection.StateCancelled,
	}
	for _, terminal := range terminals {
		t.Run(string(terminal), func(t *testing.T) {
			projector := taskprojection.Projector{}
			state := applyTask(t, projector, projector.Zero(sessionRef()), taskCreated())
			for _, transition := range [][2]string{
				{"draft", "contract_drafting"}, {"contract_drafting", "contract_proposed"},
				{"contract_proposed", "contract_frozen"}, {"contract_frozen", "running"}, {"running", "verifying"},
			} {
				state = applyTask(t, projector, state, taskChanged(transition[0], transition[1]))
			}
			state = applyTask(t, projector, state, taskChanged("verifying", string(terminal)))
			if state.Tasks["task"].State != terminal {
				t.Fatalf("state=%q", state.Tasks["task"].State)
			}
			if _, err := projector.Apply(state, taskChanged(string(terminal), "running")); err == nil {
				t.Fatal("terminal task skipped explicit reopen")
			}
			if terminal == taskprojection.StatePartial || terminal == taskprojection.StateFailed || terminal == taskprojection.StateUnknown || terminal == taskprojection.StateCompletedWithWaivers {
				state = applyTask(t, projector, state, taskChanged(string(terminal), "reopened"))
				state = applyTask(t, projector, state, taskChanged("reopened", "running"))
				if state.Tasks["task"].State != taskprojection.StateRunning {
					t.Fatalf("reopened state=%q", state.Tasks["task"].State)
				}
			}
		})
	}
}

func TestLifecycleTurnAllowsFullFoundationPathAndRejectsTerminalReuse(t *testing.T) {
	projector := taskprojection.Projector{}
	state := applyTask(t, projector, projector.Zero(sessionRef()), turnAccepted())
	path := []string{
		"contract_drafting", "freezing_contract", "planning_context", "waiting_provider", "receiving_provider",
		"planning_action", "checkpointing", "awaiting_permission", "executing", "recording_evidence",
		"returning_result", "verifying",
	}
	from := "accepted"
	for _, to := range path {
		state = applyTask(t, projector, state, turnChanged(from, to))
		from = to
	}
	state = applyTask(t, projector, state, turnTerminal(protocol.EventTurnCompleted, "completed"))
	if state.Turns["turn"].State != taskprojection.TurnStateCompleted {
		t.Fatalf("turn state=%q", state.Turns["turn"].State)
	}
	if _, err := projector.Apply(state, turnTerminal(protocol.EventTurnFailed, "failed")); err == nil {
		t.Fatal("terminal turn allowed another terminal")
	}
}

func TestLifecycleTurnAcceptsRegistryValidatedStateTerminalAndLegacyLane(t *testing.T) {
	projector := taskprojection.Projector{}
	legacy := applyTask(t, projector, projector.Zero(sessionRef()), turnAccepted())
	legacy = applyTask(t, projector, legacy, turnChanged("accepted", "running"))
	legacy = applyTask(t, projector, legacy, turnTerminal(protocol.EventTurnCompleted, "completed"))
	if legacy.Turns["turn"].State != taskprojection.TurnStateCompleted {
		t.Fatalf("legacy turn=%+v", legacy.Turns["turn"])
	}

	detailed := applyTask(t, projector, projector.Zero(sessionRef()), turnAccepted())
	from := "accepted"
	for _, to := range []string{
		"contract_drafting", "freezing_contract", "planning_context", "waiting_provider", "receiving_provider",
		"planning_action", "checkpointing", "awaiting_permission", "executing", "recording_evidence", "returning_result", "verifying", "completed",
	} {
		detailed = applyTask(t, projector, detailed, turnChanged(from, to))
		from = to
	}
	if detailed.Turns["turn"].State != taskprojection.TurnStateCompleted {
		t.Fatalf("detailed turn=%+v", detailed.Turns["turn"])
	}
}

func TestOutcomeVerifiedRejectsUnknownRequiredCriterion(t *testing.T) {
	projector := taskprojection.Projector{}
	state := applyTask(t, projector, projector.Zero(sessionRef()), taskCreated())
	state = applyTask(t, projector, state, contractDeclared())
	state = applyTask(t, projector, state, criterionAssessed("required", "unknown", nil))
	if _, err := projector.Apply(state, finalAssessed("verified")); err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("verified outcome error=%v", err)
	}
}

func TestOutcomeMissingEvidenceInvalidatesDependentVerificationAndAddsDiagnostic(t *testing.T) {
	projector := taskprojection.Projector{}
	state := applyTask(t, projector, projector.Zero(sessionRef()), taskCreated())
	state = applyTask(t, projector, state, contractDeclared())
	state = applyTask(t, projector, state, evidenceRecorded(protocol.ContentAvailable))
	state = applyTask(t, projector, state, criterionAssessed("required", "verified", []protocol.EvidenceID{"evidence"}))
	if state.Tasks["task"].Criteria["required"].Status != taskprojection.CriterionVerified {
		t.Fatalf("criterion=%+v", state.Tasks["task"].Criteria["required"])
	}
	state = applyTask(t, projector, state, evidenceDiagnostic("evidence.missing"))
	criterion := state.Tasks["task"].Criteria["required"]
	if criterion.Status != taskprojection.CriterionUnknown || state.Tasks["task"].Contracts[1].Criteria["required"].Status != taskprojection.CriterionUnknown || state.Tasks["task"].VerificationState != protocol.ValueUnknown || len(state.Diagnostics) != 1 {
		t.Fatalf("criterion=%+v verification=%q diagnostics=%+v", criterion, state.Tasks["task"].VerificationState, state.Diagnostics)
	}
	if _, err := projector.Apply(state, finalAssessed("verified")); err == nil {
		t.Fatal("stale verified outcome survived missing evidence")
	}
}

func TestProjectionLegacyLeavesAbsentFactsExplicitlyUnknown(t *testing.T) {
	projector := taskprojection.Projector{}
	record := taskCreated()
	record.Legacy = &protocol.LegacySource{SchemaVersion: 1, EventID: "legacy", SessionID: "session", Seq: 1}
	state := applyTask(t, projector, projector.Zero(sessionRef()), record)
	projected := state.Tasks["task"]
	if projected.VerificationState != protocol.ValueUnknown || projected.Outcome.State != protocol.ValueUnknown {
		t.Fatalf("legacy projection invented facts: %+v", projected)
	}
}

func TestProjectionLegacyTaskLifecycleRebuilds(t *testing.T) {
	for _, terminal := range []string{"completed", "failed", "cancelled"} {
		t.Run(terminal, func(t *testing.T) {
			projector := taskprojection.Projector{}
			created := taskCreated()
			created.Legacy = &protocol.LegacySource{SchemaVersion: 1, EventID: "legacy-created", SessionID: "session", Seq: 1}
			state := applyTask(t, projector, projector.Zero(sessionRef()), created)
			state = applyTask(t, projector, state, taskChanged("pending", "running"))
			state = applyTask(t, projector, state, taskChanged("running", terminal))
			if string(state.Tasks["task"].State) != terminal {
				t.Fatalf("legacy task state=%q", state.Tasks["task"].State)
			}
		})
	}

	projector := taskprojection.Projector{}
	detailed := applyTask(t, projector, projector.Zero(sessionRef()), taskCreated())
	if _, err := projector.Apply(detailed, taskChanged("pending", "running")); err == nil {
		t.Fatal("detailed task entered the legacy pending lane")
	}
	for _, transition := range [][2]string{
		{"draft", "contract_drafting"}, {"contract_drafting", "contract_proposed"},
		{"contract_proposed", "contract_frozen"}, {"contract_frozen", "running"},
	} {
		detailed = applyTask(t, projector, detailed, taskChanged(transition[0], transition[1]))
	}
	if _, err := projector.Apply(detailed, taskChanged("running", "completed")); err == nil {
		t.Fatal("detailed task used a legacy terminal shortcut")
	}
}

func TestOutcomeEvidenceInvalidationDowngradesVerifiedTaskLifecycle(t *testing.T) {
	projector := taskprojection.Projector{}
	state := applyTask(t, projector, projector.Zero(sessionRef()), taskCreated())
	state = applyTask(t, projector, state, contractDeclared())
	state = applyTask(t, projector, state, evidenceRecorded(protocol.ContentAvailable))
	state = applyTask(t, projector, state, criterionAssessed("required", "verified", []protocol.EvidenceID{"evidence"}))
	state = applyTask(t, projector, state, finalAssessed("verified"))
	for _, transition := range [][2]string{
		{"draft", "contract_drafting"}, {"contract_drafting", "contract_proposed"}, {"contract_proposed", "contract_frozen"},
		{"contract_frozen", "running"}, {"running", "verifying"}, {"verifying", "verified"},
	} {
		state = applyTask(t, projector, state, taskChanged(transition[0], transition[1]))
	}

	state = applyTask(t, projector, state, evidenceDiagnostic("evidence.missing"))
	record := state.Tasks["task"]
	if record.State != taskprojection.StateUnknown || record.VerificationState != protocol.ValueUnknown || record.Outcome.State != protocol.ValueUnknown {
		t.Fatalf("invalidated verified task=%+v", record)
	}
}

func TestLifecycleTurnRejectsSecondActiveTurn(t *testing.T) {
	projector := taskprojection.Projector{}
	state := applyTask(t, projector, projector.Zero(sessionRef()), turnAcceptedWithID("turn-one"))
	if _, err := projector.Apply(state, turnAcceptedWithID("turn-two")); err == nil {
		t.Fatal("second active turn was accepted")
	}
	state = applyTask(t, projector, state, turnChangedWithID("turn-one", "accepted", "running"))
	state = applyTask(t, projector, state, turnTerminalWithID("turn-one", protocol.EventTurnCompleted, "completed"))
	state = applyTask(t, projector, state, turnAcceptedWithID("turn-two"))
	if state.Turns["turn-two"].State != taskprojection.TurnStateAccepted {
		t.Fatalf("new turn after terminal=%+v", state.Turns)
	}
}

func TestOutcomeContractDeclarationIsImmutable(t *testing.T) {
	projector := taskprojection.Projector{}
	state := applyTask(t, projector, projector.Zero(sessionRef()), taskCreated())
	state = applyTask(t, projector, state, contractDeclared())
	if _, err := projector.Apply(state, contractDeclared()); err == nil {
		t.Fatal("identical contract redeclaration was accepted")
	}
	changed := contractDeclared()
	changed.Decoded.(*protocol.OutcomeContractDeclaredV1).Goal = "changed history"
	if _, err := projector.Apply(state, changed); err == nil {
		t.Fatal("changed contract redeclaration was accepted")
	}
}

func TestProjectionTaskRejectsUnknownStatefulEvent(t *testing.T) {
	projector := taskprojection.Projector{}
	unknown := taskRecord("future.outcome_policy", &struct{}{})
	if _, err := projector.Apply(projector.Zero(sessionRef()), unknown); err == nil {
		t.Fatal("unknown stateful event was skipped")
	}
}

func TestOutcomeProjectionRetainsContractVersionsAssessmentsAndTerminalHistory(t *testing.T) {
	projector := taskprojection.Projector{}
	state := applyTask(t, projector, projector.Zero(sessionRef()), taskCreated())
	state = applyTask(t, projector, state, contractDeclared())
	state = applyTask(t, projector, state, criterionAssessed("required", "unknown", nil))
	state = applyTask(t, projector, state, taskRecord(protocol.EventOutcomeContractAmended, &protocol.OutcomeContractAmendedV1{
		OutcomeContractID: "contract", FromVersion: 1, ToVersion: 2, Reason: "make verification explicit",
		Actor: protocol.ActorRef{ID: "user", Kind: protocol.ActorUser}, Frozen: true,
		Criteria: []protocol.CriterionV1{{ID: "required-v2", Required: true, Description: "race tests pass", VerificationMethod: "go test -race", ExpectedEvidenceKind: "test_result"}},
	}))
	state = applyTask(t, projector, state, taskRecord(protocol.EventOutcomeCriterionAssessed, &protocol.CriterionAssessedV1{
		OutcomeContractID: "contract", ContractVersion: 2, CriterionID: "required-v2", Status: "unknown", Reason: "not run",
	}))
	state = applyTask(t, projector, state, taskRecord(protocol.EventOutcomeFinalAssessed, &protocol.OutcomeFinalAssessedV1{
		OutcomeContractID: "contract", ContractVersion: 2, Status: "unknown", CriterionIDs: []string{"required-v2"},
	}))
	record := state.Tasks["task"]
	if len(record.Contracts) != 2 || record.Contracts[1].Criteria["required"].Definition.ID != "required" || record.Contracts[2].AmendmentReason != "make verification explicit" {
		t.Fatalf("contracts=%+v", record.Contracts)
	}
	if len(record.Assessments) != 2 || record.Assessments[0].ContractVersion != 1 || record.Assessments[1].ContractVersion != 2 {
		t.Fatalf("assessments=%+v", record.Assessments)
	}
	if len(record.OutcomeHistory) != 1 || record.OutcomeHistory[0].Status != "unknown" {
		t.Fatalf("outcome history=%+v", record.OutcomeHistory)
	}
}

func applyTask(t *testing.T, projector taskprojection.Projector, state taskprojection.Projection, record protocol.EventRecord) taskprojection.Projection {
	t.Helper()
	next, err := projector.Apply(state, record)
	if err != nil {
		t.Fatal(err)
	}
	return next
}

func taskCreated() protocol.EventRecord {
	return taskRecord(protocol.EventTaskCreated, &protocol.TaskCreatedV1{Goal: "ship v0.2", OutcomeContractID: "contract", ContractVersion: 1})
}

func taskChanged(from, to string) protocol.EventRecord {
	return taskRecord(protocol.EventTaskStatusChanged, &protocol.TaskStatusChangedV1{From: from, To: to})
}

func contractDeclared() protocol.EventRecord {
	return taskRecord(protocol.EventOutcomeContractDeclared, &protocol.OutcomeContractDeclaredV1{
		OutcomeContractID: "contract", Version: 1, Goal: "ship v0.2", Source: "user", Frozen: true,
		Criteria: []protocol.CriterionV1{{ID: "required", Required: true, Description: "tests pass", VerificationMethod: "go test", ExpectedEvidenceKind: "test_result"}},
	})
}

func criterionAssessed(id, status string, evidence []protocol.EvidenceID) protocol.EventRecord {
	return taskRecord(protocol.EventOutcomeCriterionAssessed, &protocol.CriterionAssessedV1{
		OutcomeContractID: "contract", ContractVersion: 1, CriterionID: id, Status: status,
		EvidenceIDs: evidence, Reason: "assessment",
	})
}

func finalAssessed(status string) protocol.EventRecord {
	return taskRecord(protocol.EventOutcomeFinalAssessed, &protocol.OutcomeFinalAssessedV1{
		OutcomeContractID: "contract", ContractVersion: 1, Status: status, CriterionIDs: []string{"required"},
	})
}

func turnAccepted() protocol.EventRecord {
	return taskRecord(protocol.EventTurnAccepted, &protocol.TurnAcceptedV1{CommandID: "command", Goal: "ship v0.2", OutcomeContractID: "contract", ContractVersion: 1})
}

func turnAcceptedWithID(id protocol.TurnID) protocol.EventRecord {
	record := turnAccepted()
	record.Envelope.TurnID = id
	return record
}

func turnChanged(from, to string) protocol.EventRecord {
	return taskRecord(protocol.EventTurnStateChanged, &protocol.TurnStateChangedV1{From: from, To: to})
}

func turnChangedWithID(id protocol.TurnID, from, to string) protocol.EventRecord {
	record := turnChanged(from, to)
	record.Envelope.TurnID = id
	return record
}

func turnTerminal(kind, status string) protocol.EventRecord {
	return taskRecord(kind, &protocol.TurnTerminalV1{Status: status, Reason: "done"})
}

func turnTerminalWithID(id protocol.TurnID, kind, status string) protocol.EventRecord {
	record := turnTerminal(kind, status)
	record.Envelope.TurnID = id
	return record
}

func evidenceRecorded(availability protocol.ContentAvailability) protocol.EventRecord {
	digest := testDigest("a")
	body := protocol.EvidenceRecordBody{
		ID: "evidence", Kind: "test_result", WorkspaceID: "workspace", SessionID: "session", Availability: availability,
		MediaType: "text/plain", ProducingActivityID: "activity", Actor: protocol.ActorRef{ID: "system", Kind: protocol.ActorSystem},
		Subject: protocol.SubjectRef{Kind: "criterion", ID: "required"}, CreatedAt: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC),
	}
	if availability == protocol.ContentAvailable {
		body.Blob = &protocol.BlobRef{Digest: digest}
	}
	return taskRecord(protocol.EventEvidenceRecorded, &protocol.EvidenceRecordedV1{Record: protocol.EvidenceRecord{Body: body, Digest: digest}})
}

func evidenceDiagnostic(code string) protocol.EventRecord {
	details := json.RawMessage(`{"evidence_id":"evidence"}`)
	return taskRecord(protocol.EventRecoveryDiagnostic, &protocol.DiagnosticV1{Diagnostic: protocol.Diagnostic{
		Code: code, Message: "evidence content is unavailable", Journal: sessionRef(), EventID: "evidence-event", Details: details,
	}})
}

func taskRecord(kind string, decoded any) protocol.EventRecord {
	raw, _ := json.Marshal(decoded)
	return protocol.EventRecord{Envelope: protocol.EventEnvelope{
		JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", EventID: protocol.EventID("event-" + kind),
		Time: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC), Kind: kind, PayloadVersion: 1, TaskID: "task", TurnID: "turn", Payload: raw,
	}, Decoded: decoded}
}

func sessionRef() protocol.JournalRef {
	return protocol.JournalRef{Kind: protocol.JournalSession, ID: "session"}
}

func testDigest(fill string) protocol.Digest {
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat(fill, 64)}
}
