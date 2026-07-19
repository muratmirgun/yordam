package context_test

import (
	stdcontext "context"
	"fmt"
	"strings"
	"testing"

	contextplanner "github.com/muratmirgun/yordam/internal/context"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type fakeSummaryResolver struct {
	sources map[protocol.EvidenceID]protocol.ContentSource
	errors  map[protocol.EvidenceID]error
	calls   int
}

func (r *fakeSummaryResolver) ResolveCompactionSummary(_ stdcontext.Context, _ protocol.SessionID, id protocol.EvidenceID) (protocol.ContentSource, error) {
	r.calls++
	if err := r.errors[id]; err != nil {
		return protocol.ContentSource{}, err
	}
	return r.sources[id], nil
}

func (r *fakeSummaryResolver) ResolveVerifiedCompactionSummary(ctx stdcontext.Context, reference protocol.ContextCompactionReference) (protocol.ContentSource, error) {
	return r.ResolveCompactionSummary(ctx, reference.SessionID, reference.SummaryEvidenceID)
}

func TestContextPlanReconstructsLatestValidNativeSummary(t *testing.T) {
	resolver := &fakeSummaryResolver{sources: map[protocol.EvidenceID]protocol.ContentSource{
		"summary": summarySource(t, "summary", "native summary"),
	}}
	events := []protocol.EventRecord{
		contextEvent("old-user", 1, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "old user"}),
		contextEvent("old-assistant", 2, protocol.EventAssistantMessage, &protocol.AssistantMessageV1{Blocks: []protocol.ContentBlock{{Kind: protocol.ContentText, Text: "old answer"}}}),
		nativeCompactionEvent("compact", 3, "session", 1, 2, "summary", "native-r1"),
		contextEvent("new-user", 4, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "new user"}),
	}

	plan, err := contextplanner.NewPlanner("tools-r1", resolver).Plan(stdcontext.Background(), contextplanner.Request{
		Session: "session", TaskID: "task", OutcomeContractID: "contract", OutcomeContractVersion: 1,
		Events: events, Model: contextModel(1024), OutputReserve: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolver.calls != 1 || len(plan.Body.Sources) != 2 || plan.Body.Sources[0].Kind != "compaction_summary" || plan.Body.Sources[0].Provenance != "evidence:summary" || plan.Body.Sources[1].ID != "new-user" || plan.Body.CompactionRevision != "native-r1" {
		t.Fatalf("resolver calls=%d plan=%#v", resolver.calls, plan)
	}
	if len(plan.Body.Excluded) != 2 || plan.Body.Excluded[0].ID != "old-user" || plan.Body.Excluded[1].ID != "old-assistant" {
		t.Fatalf("excluded=%#v", plan.Body.Excluded)
	}
}

func TestContextPlanFailsClosedForUnresolvableNativeSummary(t *testing.T) {
	for name, resolver := range map[string]*fakeSummaryResolver{
		"missing":       {errors: map[protocol.EvidenceID]error{"summary": fmt.Errorf("missing")}},
		"corrupt":       {errors: map[protocol.EvidenceID]error{"summary": fmt.Errorf("corrupt")}},
		"oversized":     {errors: map[protocol.EvidenceID]error{"summary": fmt.Errorf("oversized")}},
		"wrong-session": {errors: map[protocol.EvidenceID]error{"summary": fmt.Errorf("wrong session")}},
	} {
		t.Run(name, func(t *testing.T) {
			events := []protocol.EventRecord{
				contextEvent("old", 1, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "old"}),
				nativeCompactionEvent("compact", 2, "session", 1, 1, "summary", "native-r1"),
			}
			plan, err := contextplanner.NewPlanner("tools-r1", resolver).Plan(stdcontext.Background(), contextplanner.Request{
				Session: "session", TaskID: "task", OutcomeContractID: "contract", OutcomeContractVersion: 1,
				Events: events, Model: contextModel(1024), OutputReserve: 64,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Body.Sources) != 1 || plan.Body.Sources[0].ID != "old" || len(plan.Body.Excluded) != 0 || plan.Body.CompactionRevision != "none" {
				t.Fatalf("plan=%#v", plan)
			}
		})
	}
}

func TestContextPlanKeepsEarlierNativeSummaryWhenLaterEventIsInvalid(t *testing.T) {
	resolver := &fakeSummaryResolver{sources: map[protocol.EvidenceID]protocol.ContentSource{
		"valid": summarySource(t, "valid", "valid summary"),
	}}
	events := []protocol.EventRecord{
		contextEvent("old", 1, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "old"}),
		nativeCompactionEvent("valid-compact", 2, "session", 1, 1, "valid", "native-r1"),
		nativeCompactionEvent("invalid-compact", 3, "other", 1, 2, "invalid", "native-r2"),
		contextEvent("new", 4, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "new"}),
	}
	plan, err := contextplanner.NewPlanner("tools-r1", resolver).Plan(stdcontext.Background(), contextplanner.Request{
		Session: "session", TaskID: "task", OutcomeContractID: "contract", OutcomeContractVersion: 1,
		Events: events, Model: contextModel(1024), OutputReserve: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolver.calls != 1 || plan.Body.CompactionRevision != "native-r1" || len(plan.Body.Sources) != 2 || plan.Body.Sources[0].Provenance != "evidence:valid" {
		t.Fatalf("resolver calls=%d plan=%#v", resolver.calls, plan)
	}
}

func TestContextPlanKeepsEarlierResolvableSummaryWhenLaterEvidenceIsMissing(t *testing.T) {
	resolver := &fakeSummaryResolver{
		sources: map[protocol.EvidenceID]protocol.ContentSource{
			"earlier": summarySource(t, "earlier", "earlier summary"),
		},
		errors: map[protocol.EvidenceID]error{"later": fmt.Errorf("missing")},
	}
	events := []protocol.EventRecord{
		contextEvent("old", 1, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "old"}),
		nativeCompactionEvent("earlier-compact", 2, "session", 1, 1, "earlier", "native-r1"),
		contextEvent("middle", 3, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "middle"}),
		nativeCompactionEvent("later-compact", 4, "session", 1, 3, "later", "native-r2"),
		contextEvent("new", 5, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "new"}),
	}

	plan, err := contextplanner.NewPlanner("tools-r1", resolver).Plan(stdcontext.Background(), contextplanner.Request{
		Session: "session", TaskID: "task", OutcomeContractID: "contract", OutcomeContractVersion: 1,
		Events: events, Model: contextModel(1024), OutputReserve: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolver.calls != 2 || plan.Body.CompactionRevision != "native-r1" || len(plan.Body.Sources) != 3 || plan.Body.Sources[0].Provenance != "evidence:earlier" || plan.Body.Sources[1].ID != "middle" || plan.Body.Sources[2].ID != "new" {
		t.Fatalf("resolver calls=%d plan=%#v", resolver.calls, plan)
	}
}

func nativeCompactionEvent(id protocol.EventID, seq uint64, session protocol.SessionID, from, through uint64, evidence protocol.EvidenceID, revision string) protocol.EventRecord {
	cursor := func(commitSeq uint64) protocol.CommittedCursor {
		return protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(session), CommitSeq: commitSeq, TransactionID: "transaction"}
	}
	return protocol.EventRecord{Envelope: protocol.EventEnvelope{EventID: id, Seq: seq, Kind: protocol.EventContextCompacted, JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(session), SessionID: session}, Decoded: &protocol.ContextCompactedV1{From: cursor(from), Through: cursor(through), SummaryEvidenceID: evidence, Revision: revision}}
}

func summarySource(t *testing.T, id, content string) protocol.ContentSource {
	t.Helper()
	source := source(t, id, content, "evidence:"+id)
	source.Kind = "compaction_summary"
	return source
}

func TestSummaryResolverRejectsOversizedSource(t *testing.T) {
	resolver := &fakeSummaryResolver{sources: map[protocol.EvidenceID]protocol.ContentSource{
		"summary": summarySource(t, "summary", strings.Repeat("s", 128*1024+1)),
	}}
	events := []protocol.EventRecord{
		contextEvent("old", 1, protocol.EventUserMessage, &protocol.UserMessageV1{Content: "old"}),
		nativeCompactionEvent("compact", 2, "session", 1, 1, "summary", "native-r1"),
	}
	plan, err := contextplanner.NewPlanner("tools-r1", resolver).Plan(stdcontext.Background(), contextplanner.Request{Session: "session", TaskID: "task", OutcomeContractID: "contract", OutcomeContractVersion: 1, Events: events, Model: contextModel(100000), OutputReserve: 64})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Body.Sources) != 1 || plan.Body.CompactionRevision != "none" {
		t.Fatalf("plan=%#v", plan)
	}
}
