package activity_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/activity"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestLifecycleActivityAllowsOnlyDocumentedTransitions(t *testing.T) {
	terminal := []struct {
		kind  string
		state activity.State
	}{
		{protocol.EventActivitySucceeded, activity.StateSucceeded},
		{protocol.EventActivityFailed, activity.StateFailed},
		{protocol.EventActivityInterruptedNoEffect, activity.StateInterruptedNoEffect},
		{protocol.EventActivityUncertain, activity.StateUncertain},
	}
	for _, item := range terminal {
		t.Run(string(item.state), func(t *testing.T) {
			projector := activity.Projector{}
			state := projector.Zero(sessionRef())
			state = applyActivity(t, projector, state, planned("parent", ""))
			state = applyActivity(t, projector, state, authorized("parent"))
			state = applyActivity(t, projector, state, started("parent"))
			state = applyActivity(t, projector, state, outcome("parent", item.kind))
			if got := state.Activities["parent"].State; got != item.state {
				t.Fatalf("state=%q want=%q", got, item.state)
			}
			if _, err := projector.Apply(state, outcome("parent", item.kind)); err == nil {
				t.Fatal("duplicate terminal transition accepted")
			}
		})
	}

	for _, item := range []struct {
		kind  string
		state activity.State
	}{
		{protocol.EventActivityDenied, activity.StateDenied},
		{protocol.EventActivityCancelled, activity.StateCancelled},
	} {
		t.Run("planned_"+string(item.state), func(t *testing.T) {
			projector := activity.Projector{}
			state := applyActivity(t, projector, projector.Zero(sessionRef()), planned("parent", ""))
			state = applyActivity(t, projector, state, outcome("parent", item.kind))
			if state.Activities["parent"].State != item.state {
				t.Fatalf("state=%q", state.Activities["parent"].State)
			}
			if _, err := projector.Apply(state, authorized("parent")); err == nil {
				t.Fatal("terminal state allowed an outgoing transition")
			}
		})
	}

	projector := activity.Projector{}
	state := applyActivity(t, projector, projector.Zero(sessionRef()), planned("parent", ""))
	if _, err := projector.Apply(state, started("parent")); err == nil || !strings.Contains(err.Error(), "planned") {
		t.Fatalf("planned -> started error=%v", err)
	}
	state = applyActivity(t, projector, state, authorized("parent"))
	state = applyActivity(t, projector, state, outcome("parent", protocol.EventActivityCancelled))
	if state.Activities["parent"].State != activity.StateCancelled {
		t.Fatalf("authorized cancellation state=%q", state.Activities["parent"].State)
	}
}

func TestProjectionActivityRetainsPurposeEvidenceDecisionProfilesAndChildren(t *testing.T) {
	projector := activity.Projector{}
	state := projector.Zero(sessionRef())
	state = applyActivity(t, projector, state, planned("parent", ""))
	state = applyActivity(t, projector, state, planned("child", "parent"))
	state = applyActivity(t, projector, state, authorized("child"))
	state = applyActivity(t, projector, state, started("child"))
	state = applyActivity(t, projector, state, outcome("child", protocol.EventActivityUncertain))

	child := state.Activities["child"]
	if child.Purpose != "inspect workspace" || child.Source != "builtin/yordam/read" || child.RequestedProfile != "restricted local" || child.EffectiveProfile != "restricted local" {
		t.Fatalf("planned fields=%+v", child)
	}
	if child.Decision == nil || child.Decision.DecisionEventID != "decision-event" || child.StartedAt == nil || child.TerminalAt == nil || child.ErrorCode != "uncertain_effect" || len(child.UnknownEffects) != 1 {
		t.Fatalf("lifecycle fields=%+v", child)
	}
	if len(state.Activities["parent"].Children) != 1 || state.Activities["parent"].Children[0].ActivityID != "child" || state.Activities["parent"].Children[0].State != activity.StateUncertain {
		t.Fatalf("parent children=%+v", state.Activities["parent"].Children)
	}

	copy := projector.Snapshot(state)
	copy.Activities["child"].InputEvidenceIDs[0] = "mutated"
	if state.Activities["child"].InputEvidenceIDs[0] != "evidence-input" {
		t.Fatal("activity snapshot aliases projector state")
	}
}

func TestProjectionActivityRejectsUnknownOrUnsupportedStatefulEvent(t *testing.T) {
	projector := activity.Projector{}
	state := projector.Zero(sessionRef())
	unknown := activityRecord("activity", "", "future.activity_policy", &struct{}{})
	if _, err := projector.Apply(state, unknown); err == nil {
		t.Fatal("unknown stateful event was skipped")
	}
	unsupported := planned("activity", "")
	unsupported.Envelope.PayloadVersion = 99
	if _, err := projector.Apply(state, unsupported); err == nil {
		t.Fatal("unsupported payload version was projected")
	}
}

func TestLifecycleActivityStartedMustMatchDurableAuthorization(t *testing.T) {
	projector := activity.Projector{}
	state := applyActivity(t, projector, projector.Zero(sessionRef()), planned("activity", ""))
	state = applyActivity(t, projector, state, authorized("activity"))
	changed := started("activity")
	changed.Decoded.(*protocol.ActivityStartedV1).DispatchDigest = testDigest("f")
	if _, err := projector.Apply(state, changed); err == nil {
		t.Fatal("activity start with changed authorization binding was accepted")
	}
}

func TestProjectionActivityRejectsUnknownEvidenceRelation(t *testing.T) {
	projector := activity.Projector{}
	state := applyActivity(t, projector, projector.Zero(sessionRef()), planned("activity", ""))
	record := activityRecord("activity", "", protocol.EventEvidenceLinked, &protocol.EvidenceLinkedV1{
		EvidenceID: "evidence", Subject: protocol.SubjectRef{Kind: "activity", ID: "activity"}, Relation: "derived",
	})
	if _, err := projector.Apply(state, record); err == nil {
		t.Fatal("unknown evidence relation was classified as output")
	}
}

func applyActivity(t *testing.T, projector activity.Projector, state activity.Projection, record protocol.EventRecord) activity.Projection {
	t.Helper()
	next, err := projector.Apply(state, record)
	if err != nil {
		t.Fatal(err)
	}
	return next
}

func planned(id protocol.ActivityID, parent protocol.ActivityID) protocol.EventRecord {
	payload := &protocol.ActivityPlannedV1{
		Kind: "tool_execution", Purpose: "inspect workspace",
		PurposeActor: protocol.ActorRef{ID: "agent", Kind: protocol.ActorAgent},
		Source:       "builtin/yordam/read", InputEvidenceIDs: []protocol.EvidenceID{"evidence-input"},
		RequestedProfile: "restricted local", EffectiveProfile: "restricted local",
	}
	return activityRecord(id, parent, protocol.EventActivityPlanned, payload)
}

func authorized(id protocol.ActivityID) protocol.EventRecord {
	digest := testDigest("1")
	return activityRecord(id, "", protocol.EventActivityAuthorized, &protocol.ActivityAuthorizedV1{
		DecisionNonce: "nonce", DecisionEventID: "decision-event", PlanDigest: digest,
		RequestDigest: testDigest("2"), DispatchDigest: testDigest("3"),
	})
}

func started(id protocol.ActivityID) protocol.EventRecord {
	return activityRecord(id, "", protocol.EventActivityStarted, &protocol.ActivityStartedV1{
		DecisionNonce: "nonce", DecisionEventID: "decision-event", ActivityID: id, CallID: "call",
		PlanDigest: testDigest("1"), RequestDigest: testDigest("2"), DispatchDigest: testDigest("3"),
		RuntimeGenerationID: "generation", DispatchState: "committed",
	})
}

func outcome(id protocol.ActivityID, kind string) protocol.EventRecord {
	status := map[string]string{
		protocol.EventActivitySucceeded: "succeeded", protocol.EventActivityFailed: "failed",
		protocol.EventActivityDenied: "denied", protocol.EventActivityCancelled: "cancelled",
		protocol.EventActivityInterruptedNoEffect: "interrupted_no_effect", protocol.EventActivityUncertain: "uncertain",
	}[kind]
	return activityRecord(id, "", kind, &protocol.ActivityOutcomeV1{
		Status: status, Reason: "terminal", ErrorCode: "uncertain_effect",
		OutputEvidenceIDs: []protocol.EvidenceID{"evidence-output"},
		UnknownEffects:    []protocol.SubjectRef{{Kind: "file", ID: "README.md"}},
	})
}

func activityRecord(id, parent protocol.ActivityID, kind string, decoded any) protocol.EventRecord {
	raw, _ := json.Marshal(decoded)
	return protocol.EventRecord{Envelope: protocol.EventEnvelope{
		JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session",
		EventID: protocol.EventID("event-" + kind + "-" + string(id)), Time: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC),
		Kind: kind, PayloadVersion: 1, TaskID: "task", TurnID: "turn", ActivityID: id, ParentActivityID: parent, Payload: raw,
	}, Decoded: decoded}
}

func sessionRef() protocol.JournalRef {
	return protocol.JournalRef{Kind: protocol.JournalSession, ID: "session"}
}

func testDigest(fill string) protocol.Digest {
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat(fill, 64)}
}
