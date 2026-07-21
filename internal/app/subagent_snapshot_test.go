package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
		subagentChildSnapshotRecord(protocol.EventSubagentManifest, manifest, 1, time.Unix(12, 0).UTC(), &manifest),
		subagentChildSnapshotRecord(protocol.EventActivityPlanned, manifest, 2, time.Unix(13, 0).UTC(), &protocol.ActivityPlannedV1{Kind: "provider"}),
		subagentChildSnapshotRecord(protocol.EventActivityStarted, manifest, 3, time.Unix(14, 0).UTC(), &protocol.ActivityStartedV1{}),
		subagentChildSnapshotRecord(protocol.EventActivityPlanned, manifest, 4, time.Unix(15, 0).UTC(), &protocol.ActivityPlannedV1{Kind: "tool"}),
		subagentChildSnapshotRecord(protocol.EventActivityStarted, manifest, 5, time.Unix(16, 0).UTC(), &protocol.ActivityStartedV1{}),
		subagentChildSnapshotRecord(protocol.EventSubagentReceipt, manifest, 5, time.Unix(18, 0).UTC(), &receipt),
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

func TestProjectSubagentCardsIgnoresCrossJournalParentRequest(t *testing.T) {
	manifest := protocol.SubagentManifestV1{AttemptID: "attempt", ParentSessionID: "parent", ParentCursor: subagentTestCursor("parent", 1), ChildSessionID: "child", ChildTaskID: "child-task", ChildTurnID: "child-turn", RuntimeGenerationID: "generation", SkillCatalogRevision: "skills", MaxToolCalls: 1, Deadline: time.Unix(20, 0).UTC()}
	record := subagentSnapshotRecord(protocol.EventSubagentRequested, "parent", 2, time.Unix(10, 0).UTC(), &protocol.SubagentRequestedV1{Call: protocol.SubagentCallV1{Task: "task"}, Manifest: manifest})
	record.Envelope.SessionID, record.Envelope.JournalID = "other", "other"
	parent := journal.Inspection{Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"}, Events: []protocol.EventRecord{record}}
	cards, err := projectSubagentCards(parent, func(protocol.SessionID) (journal.Inspection, error) {
		return journal.Inspection{}, journal.ErrSessionNotFound
	}, time.Unix(11, 0).UTC(), secret.New())
	if err != nil || len(cards) != 0 {
		t.Fatalf("cross-journal request cards=%+v err=%v", cards, err)
	}
}

func TestProjectSubagentCardsNumbersAttemptsPerParentTurn(t *testing.T) {
	parent := journal.Inspection{Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"}}
	seq := uint64(2)
	for turnIndex := 0; turnIndex < 2; turnIndex++ {
		for attemptIndex := 0; attemptIndex < 3; attemptIndex++ {
			id := protocol.DelegationAttemptID(fmt.Sprintf("turn-%d-attempt-%d", turnIndex, attemptIndex))
			manifest := protocol.SubagentManifestV1{
				AttemptID: id, ParentSessionID: "parent", ParentCursor: subagentTestCursor("parent", seq-1),
				ChildSessionID: protocol.SessionID("child-" + string(id)), ChildTaskID: protocol.TaskID("task-" + string(id)),
				ChildTurnID: protocol.TurnID("child-turn-" + string(id)), RuntimeGenerationID: "generation",
				SkillCatalogRevision: "skills", MaxToolCalls: 1, Deadline: time.Unix(100, 0).UTC(),
			}
			record := subagentSnapshotRecord(protocol.EventSubagentRequested, "parent", seq, time.Unix(int64(seq), 0).UTC(), &protocol.SubagentRequestedV1{Call: protocol.SubagentCallV1{Task: "task"}, Manifest: manifest})
			record.Envelope.TurnID = protocol.TurnID(fmt.Sprintf("parent-turn-%d", turnIndex))
			parent.Events = append(parent.Events, record)
			seq++
		}
	}
	cards, err := projectSubagentCards(parent, func(protocol.SessionID) (journal.Inspection, error) {
		return journal.Inspection{}, journal.ErrSessionNotFound
	}, time.Unix(50, 0).UTC(), secret.New())
	if err != nil {
		t.Fatal(err)
	}
	if len(cards) != 6 {
		t.Fatalf("cards=%d want 6", len(cards))
	}
	for index, card := range cards {
		want := index%3 + 1
		if card.Attempt != want {
			t.Fatalf("card %d attempt=%d want %d", index, card.Attempt, want)
		}
	}
}

func TestProjectSubagentCardsPreservesOrdinalForExactDuplicateRequest(t *testing.T) {
	parent := journal.Inspection{Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"}}
	for index := 0; index < 2; index++ {
		manifest := protocol.SubagentManifestV1{AttemptID: protocol.DelegationAttemptID(fmt.Sprintf("attempt-%d", index)), ParentSessionID: "parent", ParentCursor: subagentTestCursor("parent", uint64(index+1)), ChildSessionID: protocol.SessionID(fmt.Sprintf("child-%d", index)), ChildTaskID: protocol.TaskID(fmt.Sprintf("child-task-%d", index)), ChildTurnID: protocol.TurnID(fmt.Sprintf("child-turn-%d", index)), RuntimeGenerationID: "generation", SkillCatalogRevision: "skills", MaxToolCalls: 1, Deadline: time.Unix(100, 0).UTC()}
		record := subagentSnapshotRecord(protocol.EventSubagentRequested, "parent", uint64(index+2), time.Unix(int64(index+2), 0).UTC(), &protocol.SubagentRequestedV1{Call: protocol.SubagentCallV1{Task: "task"}, Manifest: manifest})
		parent.Events = append(parent.Events, record)
	}
	duplicate := protocol.CloneEventRecord(parent.Events[0])
	duplicate.Envelope.Seq = 4
	duplicate.Envelope.TransactionID = "duplicate"
	parent.Events = append(parent.Events, duplicate)
	cards, err := projectSubagentCards(parent, func(protocol.SessionID) (journal.Inspection, error) {
		return journal.Inspection{}, journal.ErrSessionNotFound
	}, time.Unix(50, 0).UTC(), secret.New())
	if err != nil || len(cards) != 2 || cards[0].Attempt != 1 || cards[1].Attempt != 2 {
		t.Fatalf("cards=%+v err=%v", cards, err)
	}
}

func TestProjectSubagentCardsFailsClosedAbovePerTurnLimit(t *testing.T) {
	parent := journal.Inspection{Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"}}
	for index := 0; index <= protocol.MaxSubagentAttemptsPerTurn; index++ {
		manifest := protocol.SubagentManifestV1{AttemptID: protocol.DelegationAttemptID(fmt.Sprintf("attempt-%d", index)), ParentSessionID: "parent", ParentCursor: subagentTestCursor("parent", uint64(index+1)), ChildSessionID: protocol.SessionID(fmt.Sprintf("child-%d", index)), ChildTaskID: protocol.TaskID(fmt.Sprintf("child-task-%d", index)), ChildTurnID: protocol.TurnID(fmt.Sprintf("child-turn-%d", index)), RuntimeGenerationID: "generation", SkillCatalogRevision: "skills", MaxToolCalls: 1, Deadline: time.Unix(100, 0).UTC()}
		parent.Events = append(parent.Events, subagentSnapshotRecord(protocol.EventSubagentRequested, "parent", uint64(index+2), time.Unix(int64(index+2), 0).UTC(), &protocol.SubagentRequestedV1{Call: protocol.SubagentCallV1{Task: "task"}, Manifest: manifest}))
	}
	if _, err := projectSubagentCards(parent, func(protocol.SessionID) (journal.Inspection, error) {
		return journal.Inspection{}, journal.ErrSessionNotFound
	}, time.Unix(50, 0).UTC(), secret.New()); err == nil {
		t.Fatal("fifth attempt in one parent turn was silently hidden")
	}
}

func TestProjectSubagentCardsUsesDeterministicRecentLifetimeWindow(t *testing.T) {
	parent := journal.Inspection{Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"}}
	for index := 0; index <= protocol.MaxCollectionMembers; index++ {
		attemptID := protocol.DelegationAttemptID(fmt.Sprintf("attempt-%05d", index))
		manifest := protocol.SubagentManifestV1{AttemptID: attemptID, ParentSessionID: "parent", ParentCursor: subagentTestCursor("parent", uint64(index+1)), ChildSessionID: protocol.SessionID("child-" + string(attemptID)), ChildTaskID: protocol.TaskID("child-task-" + string(attemptID)), ChildTurnID: protocol.TurnID("child-turn-" + string(attemptID)), RuntimeGenerationID: "generation", SkillCatalogRevision: "skills", MaxToolCalls: 1, Deadline: time.Unix(10000, 0).UTC()}
		record := subagentSnapshotRecord(protocol.EventSubagentRequested, "parent", uint64(index+2), time.Unix(int64(index+2), 0).UTC(), &protocol.SubagentRequestedV1{Call: protocol.SubagentCallV1{Task: "task"}, Manifest: manifest})
		record.Envelope.TurnID = protocol.TurnID(fmt.Sprintf("parent-turn-%05d", index))
		parent.Events = append(parent.Events, record)
	}
	cards, err := projectSubagentCards(parent, func(protocol.SessionID) (journal.Inspection, error) {
		return journal.Inspection{}, journal.ErrSessionNotFound
	}, time.Unix(9000, 0).UTC(), secret.New())
	if err != nil {
		t.Fatal(err)
	}
	if len(cards) != protocol.MaxCollectionMembers || cards[0].AttemptID != "attempt-00001" || cards[len(cards)-1].AttemptID != "attempt-04096" {
		t.Fatalf("window len=%d first=%q last=%q", len(cards), cards[0].AttemptID, cards[len(cards)-1].AttemptID)
	}
	for _, card := range cards {
		if card.Attempt != 1 || card.Validate() != nil {
			t.Fatalf("invalid window card: %+v", card)
		}
	}
	parentRef := parent.Journal
	reader := &adversarialPrefixReader{records: map[protocol.JournalRef][]protocol.EventRecord{parentRef: parent.Events}}
	repository := &recentWindowRepository{adversarialPrefixReader: reader}
	parentCursor := protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: parentRef.ID, CommitSeq: uint64(protocol.MaxCollectionMembers + 3), TransactionID: "transaction"}
	heads, err := (runtimeBrokerSource{Repository: repository}).RelatedSessionHeads(t.Context(), parentCursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(heads) != len(cards) || heads[0].JournalID != protocol.JournalID(cards[0].ChildSessionID) || heads[len(heads)-1].JournalID != protocol.JournalID(cards[len(cards)-1].ChildSessionID) {
		t.Fatalf("related/card windows diverged: heads=%d cards=%d first=%q/%q last=%q/%q", len(heads), len(cards), heads[0].JournalID, cards[0].ChildSessionID, heads[len(heads)-1].JournalID, cards[len(cards)-1].ChildSessionID)
	}
}

type recentWindowRepository struct {
	journal.Repository
	*adversarialPrefixReader
}

func (r *recentWindowRepository) ReadRange(ctx context.Context, request journal.ReadRangeRequest) (journal.EventPage, error) {
	return r.adversarialPrefixReader.ReadRange(ctx, request)
}

func (r *recentWindowRepository) Head(_ context.Context, ref protocol.JournalRef) (protocol.CommittedCursor, error) {
	return protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 1, TransactionID: "head"}, nil
}

func TestProjectSubagentCardsRejectsReceiptForDifferentManifest(t *testing.T) {
	manifest := protocol.SubagentManifestV1{AttemptID: "attempt", ParentSessionID: "parent", ParentCursor: subagentTestCursor("parent", 1), ChildSessionID: "child", ChildTaskID: "child-task", ChildTurnID: "child-turn", RuntimeGenerationID: "generation", SkillCatalogRevision: "skills", MaxToolCalls: 4, Deadline: time.Unix(20, 0).UTC()}
	request := protocol.SubagentRequestedV1{Call: protocol.SubagentCallV1{Task: "inspect"}, Manifest: manifest}
	mismatched := manifest
	mismatched.ChildTaskID = "other-child-task"
	receipt := protocol.SubagentReceiptV1{Status: "succeeded", Summary: "must not surface", Manifest: mismatched, TerminalCursor: subagentTestCursor("child", 4), ChangedFiles: []string{"SECRET.go"}, CommandsAndTests: []string{"go test SECRET"}, Usage: unknownSubagentUsage(), EvidenceIDs: []protocol.EvidenceID{}, UnknownEffects: []protocol.ActivityID{}}
	parent := journal.Inspection{Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"}, Events: []protocol.EventRecord{subagentSnapshotRecord(protocol.EventSubagentRequested, "parent", 2, time.Unix(10, 0).UTC(), &request)}}
	child := journal.Inspection{Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: "child"}, Events: []protocol.EventRecord{
		subagentChildSnapshotRecord(protocol.EventSubagentManifest, manifest, 1, time.Unix(11, 0).UTC(), &manifest),
		subagentChildSnapshotRecord(protocol.EventSubagentReceipt, manifest, 3, time.Unix(12, 0).UTC(), &receipt),
	}}
	cards, err := projectSubagentCards(parent, func(protocol.SessionID) (journal.Inspection, error) { return child, nil }, time.Unix(13, 0).UTC(), secret.New("SECRET"))
	if err != nil {
		t.Fatal(err)
	}
	if len(cards) != 1 || cards[0].State != protocol.SubagentStageRunning || cards[0].ReceiptSummary != "" || len(cards[0].ChangedFiles) != 0 || len(cards[0].CommandsAndTests) != 0 {
		t.Fatalf("mismatched receipt populated card: %+v", cards)
	}
}

func TestReadInspectionAtExcludesParentAndChildCommitsAfterCapturedVector(t *testing.T) {
	manifest := protocol.SubagentManifestV1{AttemptID: "attempt-1", ParentSessionID: "parent", ParentCursor: subagentTestCursor("parent", 1), ChildSessionID: "child-1", ChildTaskID: "child-task-1", ChildTurnID: "child-turn-1", RuntimeGenerationID: "generation", SkillCatalogRevision: "skills", MaxToolCalls: 4, Deadline: time.Unix(20, 0).UTC()}
	request := protocol.SubagentRequestedV1{Call: protocol.SubagentCallV1{Task: "first"}, Manifest: manifest}
	parentRef := protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"}
	childRef := protocol.JournalRef{Kind: protocol.JournalSession, ID: "child-1"}
	parentRecord := subagentSnapshotRecord(protocol.EventSubagentRequested, "parent", 2, time.Unix(10, 0).UTC(), &request)
	parentRecord.Envelope.TransactionID = "parent-request-1"
	childRecord := subagentChildSnapshotRecord(protocol.EventSubagentManifest, manifest, 1, time.Unix(11, 0).UTC(), &manifest)
	childRecord.Envelope.TransactionID = "child-manifest-1"
	reader := &adversarialPrefixReader{records: map[protocol.JournalRef][]protocol.EventRecord{parentRef: {parentRecord}, childRef: {childRecord}}}
	parentVector := protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "parent", CommitSeq: 3, TransactionID: "parent-request-1"}
	childVector := protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child-1", CommitSeq: 2, TransactionID: "child-manifest-1"}

	secondManifest := manifest
	secondManifest.AttemptID, secondManifest.ChildSessionID, secondManifest.ChildTaskID, secondManifest.ChildTurnID = "attempt-2", "child-2", "child-task-2", "child-turn-2"
	secondManifest.ParentCursor = parentVector
	second := subagentSnapshotRecord(protocol.EventSubagentRequested, "parent", 3, time.Unix(12, 0).UTC(), &protocol.SubagentRequestedV1{Call: protocol.SubagentCallV1{Task: "second"}, Manifest: secondManifest})
	second.Envelope.TransactionID = "parent-request-2"
	receiptCursor := protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "child-1", CommitSeq: 3, TransactionID: "child-receipt-1"}
	receipt := protocol.SubagentReceiptV1{Status: "succeeded", Summary: "late receipt", Manifest: manifest, TerminalCursor: receiptCursor, ChangedFiles: []string{}, CommandsAndTests: []string{}, Usage: unknownSubagentUsage(), EvidenceIDs: []protocol.EvidenceID{}, UnknownEffects: []protocol.ActivityID{}}
	receiptRecord := subagentChildSnapshotRecord(protocol.EventSubagentReceipt, manifest, 2, time.Unix(13, 0).UTC(), &receipt)
	receiptRecord.Envelope.TransactionID = "child-receipt-1"
	reader.records[parentRef] = append(reader.records[parentRef], second)
	reader.records[childRef] = append(reader.records[childRef], receiptRecord)

	parentAtVector, err := readInspectionAt(t.Context(), reader, parentRef, parentVector)
	if err != nil {
		t.Fatal(err)
	}
	childAtVector, err := readInspectionAt(t.Context(), reader, childRef, childVector)
	if err != nil {
		t.Fatal(err)
	}
	cards, err := projectSubagentCards(parentAtVector, func(protocol.SessionID) (journal.Inspection, error) { return childAtVector, nil }, time.Unix(14, 0).UTC(), secret.New())
	if err != nil || len(cards) != 1 || cards[0].State != protocol.SubagentStageRunning || cards[0].ReceiptSummary != "" {
		t.Fatalf("captured cards=%+v err=%v", cards, err)
	}

	latestParent := protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "parent", CommitSeq: 4, TransactionID: "parent-request-2"}
	parentAfter, err := readInspectionAt(t.Context(), reader, parentRef, latestParent)
	if err != nil {
		t.Fatal(err)
	}
	childAfter, err := readInspectionAt(t.Context(), reader, childRef, receiptCursor)
	if err != nil {
		t.Fatal(err)
	}
	cards, err = projectSubagentCards(parentAfter, func(id protocol.SessionID) (journal.Inspection, error) {
		if id == "child-1" {
			return childAfter, nil
		}
		return journal.Inspection{}, journal.ErrSessionNotFound
	}, time.Unix(14, 0).UTC(), secret.New())
	if err != nil || len(cards) != 2 || cards[0].State != protocol.SubagentStageSucceeded || cards[0].ReceiptSummary != "late receipt" {
		t.Fatalf("subsequent cards=%+v err=%v", cards, err)
	}
}

type adversarialPrefixReader struct {
	records map[protocol.JournalRef][]protocol.EventRecord
}

func (r *adversarialPrefixReader) ReadRange(_ context.Context, request journal.ReadRangeRequest) (journal.EventPage, error) {
	records := r.records[request.Journal]
	result := make([]protocol.EventRecord, 0, len(records))
	for _, record := range records {
		if record.Envelope.Seq >= request.After.CommitSeq {
			result = append(result, protocol.CloneEventRecord(record))
		}
	}
	if len(records) == 0 {
		return journal.EventPage{}, nil
	}
	last := records[len(records)-1].Envelope
	head := protocol.CommittedCursor{JournalKind: request.Journal.Kind, JournalID: request.Journal.ID, CommitSeq: last.Seq + 1, TransactionID: last.TransactionID}
	return journal.EventPage{Events: result, Cursor: head, Head: head}, nil
}

func subagentSnapshotRecord(kind string, session protocol.SessionID, seq uint64, at time.Time, decoded any) protocol.EventRecord {
	return protocol.EventRecord{Envelope: protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(session), SessionID: session, Kind: kind, Seq: seq, Time: at, RuntimeGenerationID: "generation", TaskID: "task", TurnID: "turn", TransactionID: "transaction"}, Decoded: decoded}
}

func subagentChildSnapshotRecord(kind string, manifest protocol.SubagentManifestV1, seq uint64, at time.Time, decoded any) protocol.EventRecord {
	record := subagentSnapshotRecord(kind, manifest.ChildSessionID, seq, at, decoded)
	record.Envelope.TaskID = manifest.ChildTaskID
	record.Envelope.TurnID = manifest.ChildTurnID
	record.Envelope.RuntimeGenerationID = manifest.RuntimeGenerationID
	return record
}

func subagentTestCursor(session protocol.SessionID, seq uint64) protocol.CommittedCursor {
	return protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(session), CommitSeq: seq, TransactionID: "transaction"}
}

func unknownSubagentUsage() protocol.ModelUsage {
	u := protocol.UsageValue{State: protocol.UsageUnknown}
	return protocol.ModelUsage{Input: u, Output: u, Cached: u, CacheWrite: u, Reasoning: u}
}
