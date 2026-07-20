package app

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
)

func TestLiveSubagentStageIsEnrichedFromFreshAuthoritativeSnapshot(t *testing.T) {
	card := protocol.SubagentCardV1{AttemptID: "attempt", ParentSessionID: "parent", ChildSessionID: "child", Task: "task", State: protocol.SubagentStageRunning, Attempt: 1, StartedAt: time.Unix(1, 0).UTC(), Deadline: time.Unix(2, 0).UTC(), MaxToolCalls: 1}
	raw, _ := json.Marshal(card)
	source := &stageSnapshotSource{durable: protocol.DurableProjection{Subagents: []protocol.ProjectionView{{ID: "attempt", Kind: "subagent", Status: "running", State: protocol.ValueKnown, Data: raw}}}}
	broker, err := NewBroker(BrokerOptions{Source: source, Epoch: "epoch", DefaultQueueCapacity: 1, MaxQueueCapacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	application := &App{runtimeSet: RuntimeSet{ApplicationService: &ProtocolService{broker: broker}}, session: domain.Session{ID: "parent"}}
	stage := protocol.SubagentStageV1{AttemptID: "attempt", ParentSessionID: "parent", ChildSessionID: "child", Stage: protocol.SubagentStageWaiting}
	event := application.enrichSubagentStage(t.Context(), Event{Kind: EventSubagentStage, Subagent: &stage})
	if event.Durable == nil || len(event.Durable.Subagents) != 1 || event.Durable.Subagents[0].Status != "running" {
		t.Fatalf("enriched event=%+v", event)
	}
}

type stageSnapshotSource struct{ durable protocol.DurableProjection }

func (s *stageSnapshotSource) WorkspaceControl() protocol.JournalRef {
	return protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace"}
}
func (s *stageSnapshotSource) Session(id protocol.SessionID) protocol.JournalRef {
	return protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(id)}
}
func (s *stageSnapshotSource) Head(_ context.Context, ref protocol.JournalRef) (protocol.CommittedCursor, error) {
	return protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 1, TransactionID: "head"}, nil
}
func (s *stageSnapshotSource) ReadRange(context.Context, journal.ReadRangeRequest) (journal.EventPage, error) {
	return journal.EventPage{}, nil
}
func (s *stageSnapshotSource) Project(context.Context, SnapshotVector) (protocol.DurableProjection, protocol.RuntimeProjection, error) {
	return protocol.DeepCopy(s.durable), protocol.RuntimeProjection{}, nil
}

func TestProjectSubagentCardsUsesDurableParentAndChildFactsWithRedaction(t *testing.T) {
	manifest := protocol.SubagentManifestV1{AttemptID: "attempt", ParentSessionID: "parent", ParentCursor: subagentTestCursor("parent", 1), ChildSessionID: "child", ChildTaskID: "child-task", ChildTurnID: "child-turn", RuntimeGenerationID: "generation", SkillCatalogRevision: "skills", MaxToolCalls: 4, Deadline: time.Unix(20, 0).UTC()}
	request := protocol.SubagentRequestedV1{Call: protocol.SubagentCallV1{Task: "inspect SECRET recovery"}, Manifest: manifest}
	receipt := protocol.SubagentReceiptV1{Status: "uncertain", Summary: strings.Repeat("summary SECRET ", 400), Manifest: manifest, TerminalCursor: subagentTestCursor("child", 5), ChangedFiles: []string{"SECRET.go"}, CommandsAndTests: []string{"go test SECRET"}, Usage: unknownSubagentUsage(), EvidenceIDs: []protocol.EvidenceID{}, UnknownEffects: []protocol.ActivityID{"effect"}}
	parent := journal.Inspection{Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"}, Events: []protocol.EventRecord{
		subagentSnapshotRecord(protocol.EventSubagentRequested, "parent", 2, time.Unix(10, 0).UTC(), &request),
		subagentSnapshotRecord(protocol.EventSubagentWaiting, "parent", 3, time.Unix(11, 0).UTC(), &protocol.SubagentWaitingV1{AttemptID: "attempt", ChildSessionID: "child"}),
	}}
	child := journal.Inspection{Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: "child"}, Events: []protocol.EventRecord{
		subagentSnapshotRecord(protocol.EventSubagentManifest, "child", 1, time.Unix(12, 0).UTC(), &manifest),
		subagentSnapshotRecord(protocol.EventActivityPlanned, "child", 2, time.Unix(13, 0).UTC(), &protocol.ActivityPlannedV1{Kind: "provider"}),
		subagentSnapshotRecord(protocol.EventActivityStarted, "child", 3, time.Unix(14, 0).UTC(), &protocol.ActivityStartedV1{}),
		subagentSnapshotRecord(protocol.EventActivityPlanned, "child", 4, time.Unix(15, 0).UTC(), &protocol.ActivityPlannedV1{Kind: "tool"}),
		subagentSnapshotRecord(protocol.EventActivityStarted, "child", 5, time.Unix(16, 0).UTC(), &protocol.ActivityStartedV1{}),
		subagentSnapshotRecord(protocol.EventSubagentReceipt, "child", 5, time.Unix(18, 0).UTC(), &receipt),
	}}
	cards, err := projectSubagentCards(parent, func(id protocol.SessionID) (journal.Inspection, error) {
		if id != "child" {
			return journal.Inspection{}, errors.New("wrong child")
		}
		return child, nil
	}, time.Unix(19, 0).UTC(), secret.New("SECRET"))
	if err != nil {
		t.Fatal(err)
	}
	if len(cards) != 1 {
		t.Fatalf("cards=%+v", cards)
	}
	card := cards[0]
	if card.State != protocol.SubagentStageUncertain || card.ToolCalls != 1 || card.MaxToolCalls != 4 || card.ElapsedNanos != int64(8*time.Second) || !strings.Contains(card.Task, "[REDACTED]") || strings.Contains(card.Task+card.ReceiptSummary+strings.Join(card.ChangedFiles, "")+strings.Join(card.CommandsAndTests, ""), "SECRET") || len(card.ReceiptSummary) > protocol.MaxSubagentPublicSummaryBytes || card.Warning == "" {
		t.Fatalf("card=%+v", card)
	}
}

func TestProjectSubagentCardsIgnoresNilDecodedLifecycleRecords(t *testing.T) {
	parent := journal.Inspection{Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"}, Events: []protocol.EventRecord{{Decoded: (*protocol.SubagentRequestedV1)(nil)}, {Decoded: (*protocol.SubagentWaitingV1)(nil)}}}
	cards, err := projectSubagentCards(parent, func(protocol.SessionID) (journal.Inspection, error) {
		return journal.Inspection{}, journal.ErrSessionNotFound
	}, time.Now().UTC(), secret.New())
	if err != nil || len(cards) != 0 {
		t.Fatalf("cards=%+v err=%v", cards, err)
	}
}

func subagentSnapshotRecord(kind string, session protocol.SessionID, seq uint64, at time.Time, decoded any) protocol.EventRecord {
	return protocol.EventRecord{Envelope: protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(session), SessionID: session, Kind: kind, Seq: seq, Time: at, RuntimeGenerationID: "generation", TaskID: "task", TurnID: "turn", TransactionID: "transaction"}, Decoded: decoded}
}

func subagentTestCursor(session protocol.SessionID, seq uint64) protocol.CommittedCursor {
	return protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(session), CommitSeq: seq, TransactionID: "transaction"}
}

func unknownSubagentUsage() protocol.ModelUsage {
	u := protocol.UsageValue{State: protocol.UsageUnknown}
	return protocol.ModelUsage{Input: u, Output: u, Cached: u, CacheWrite: u, Reasoning: u}
}
