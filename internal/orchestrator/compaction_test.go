package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/compaction"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/provider"
)

func TestRunCompactionLifecycleOrdersAuthorizationEvidenceAndNativeEvent(t *testing.T) {
	log := &recordLog{}
	repository := newCompactionRepository(t, log)
	request := validCompactRequest(t, repository.head)
	provider := &compactionProvider{log: log}
	evidence := &compactionEvidence{log: log}
	service := newCompactionService(t, repository, log, provider, evidence)

	result, err := service.RunCompaction(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.From.JournalID != protocol.JournalID(request.SessionID) || result.Through.CommitSeq >= result.Cursor.CommitSeq || result.SummaryEvidence.Body.ID == "" || result.Revision == "" {
		t.Fatalf("result=%+v", result)
	}
	if result.Usage.Output.State != protocol.UsageUnknown && result.Usage.Output.State != protocol.UsageProviderReported {
		t.Fatalf("usage=%+v", result.Usage)
	}
	got := log.snapshot()
	want := []string{
		"lane.acquire(compaction)", "append(command.accepted)", "provider.negotiate", "provider.prepare",
		"append(activity.planned,authorization.requested)", "authorization.decide",
		"append(authorization.decided,activity.authorized)", "append(authorization.decision_consumed,activity.started)",
		"authorization.issue", "provider.stream", "evidence.put", "append(activity.succeeded,context.compacted,command.completed)", "lane.release",
	}
	if !containsContiguous(got, want) {
		t.Fatalf("ordering:\n got=%v\nwant=%v", got, want)
	}
	requests := repository.appendRequests()
	last := requests[len(requests)-1]
	if len(last.Events) != 3 || last.Events[0].Kind != protocol.EventActivitySucceeded || last.Events[1].Kind != protocol.EventContextCompacted || last.Events[1].RuntimeGenerationID != request.Runtime.ID || last.Events[1].Actor == nil || last.Events[1].Actor.ID != "orchestrator" || last.Events[1].Actor.Kind != protocol.ActorSystem {
		t.Fatalf("final events=%#v", last.Events)
	}
	var payload protocol.ContextCompactedV1
	if err := json.Unmarshal(last.Events[1].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.From != result.From || payload.Through != result.Through || payload.SummaryEvidenceID != result.SummaryEvidence.Body.ID || payload.Revision != result.Revision {
		t.Fatalf("payload=%+v result=%+v", payload, result)
	}
	if evidence.candidate.Kind != "context_summary" || evidence.candidate.Limit != compaction.MaxSummaryBytes || evidence.candidate.SessionID != request.SessionID || evidence.candidate.WorkspaceID != request.WorkspaceID || evidence.candidate.Actor != (protocol.ActorRef{ID: "orchestrator", Kind: protocol.ActorSystem}) {
		t.Fatalf("evidence=%+v", evidence.candidate)
	}
	var binding struct {
		From         protocol.CommittedCursor `json:"from"`
		Through      protocol.CommittedCursor `json:"through"`
		SourceDigest protocol.Digest          `json:"source_digest"`
		Sources      []protocol.ContentSource `json:"normalized_sources"`
	}
	if err := json.Unmarshal([]byte(provider.request.Messages[1].Blocks[0].Text), &binding); err != nil {
		t.Fatal(err)
	}
	admitted, err := compaction.ParseSummary(evidence.candidate.Content)
	if err != nil {
		t.Fatal(err)
	}
	verifiedRevision, err := compaction.Revision(compaction.Selection{From: binding.From, Through: binding.Through, SourceDigest: binding.SourceDigest, Sources: binding.Sources, SummarizedEventIDs: []protocol.EventID{"verified"}}, admitted)
	if err != nil || verifiedRevision != payload.Revision {
		t.Fatalf("revision=%q verified=%q error=%v", payload.Revision, verifiedRevision, err)
	}
	validateAppendRequests(t, requests)
}

func TestRunCompactionAcceptedReplayIsTypedUncertainAndDoesNotResend(t *testing.T) {
	repository := newCompactionRepository(t, &recordLog{})
	request := validCompactRequest(t, repository.head)
	provider := &compactionProvider{}
	service := newCompactionService(t, repository, &recordLog{}, provider, &compactionEvidence{})
	if _, err := repository.AppendBatch(context.Background(), journal.AppendRequest{Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(request.SessionID)}, ExpectedHead: request.ExpectedHead, TransactionID: "accepted-only", Events: []protocol.ProposedEvent{compactionCommandAccepted(request)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunCompaction(context.Background(), request); !errors.Is(err, ErrCommitUncertain) {
		t.Fatalf("accepted replay error=%v", err)
	}
	if provider.streams.Load() != 0 {
		t.Fatalf("accepted replay resent provider request: %d", provider.streams.Load())
	}
}

func TestRunCompactionFailedReplayReturnsTerminalErrorWithoutResend(t *testing.T) {
	repository := newCompactionRepository(t, &recordLog{})
	request := validCompactRequest(t, repository.head)
	provider := &compactionProvider{refuse: true}
	service := newCompactionService(t, repository, &recordLog{}, provider, &compactionEvidence{})
	if _, err := service.RunCompaction(context.Background(), request); err == nil {
		t.Fatal("provider refusal was ignored")
	}
	if _, err := service.RunCompaction(context.Background(), request); err == nil || errors.Is(err, ErrCommitUncertain) {
		t.Fatalf("terminal replay error=%v", err)
	}
	if provider.streams.Load() != 1 {
		t.Fatalf("terminal replay resent provider request: %d", provider.streams.Load())
	}
}

func TestRunCompactionUsesRealProviderAndAuthorizationDispatchExactlyOnce(t *testing.T) {
	repository := newCompactionRepository(t, &recordLog{})
	request := validCompactRequest(t, repository.head)
	catalog := provider.NewCatalog("providers-a", []protocol.ModelDescriptor{validModelDescriptor(request.Runtime.ID)})
	gate := authorization.NewService(repository)
	adapter := &compactionRecordingAdapter{}
	providerService, err := provider.NewService(catalog, []provider.Adapter{adapter}, gate)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(Dependencies{Admission: passthroughAdmission{}, Lane: NewOperationLane(), Repository: repository, Providers: catalog, Provider: providerService, Authorization: &realCompactionAuthorization{gate: gate}, Evidence: &compactionEvidence{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunCompaction(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunCompaction(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if adapter.starts.Load() != 1 {
		t.Fatalf("adapter starts=%d", adapter.starts.Load())
	}
}

func TestRunCompactionIsIdleOnlyAndIdempotent(t *testing.T) {
	repository := newCompactionRepository(t, &recordLog{})
	repository.activeTurn, repository.activeTerminal = "turn-active", false
	request := validCompactRequest(t, repository.head)
	provider := &compactionProvider{}
	service := newCompactionService(t, repository, &recordLog{}, provider, &compactionEvidence{})
	if _, err := service.RunCompaction(context.Background(), request); err == nil {
		t.Fatal("active turn admitted compaction")
	}
	if got := provider.streams.Load(); got != 0 {
		t.Fatalf("provider streams=%d", got)
	}

	repository.activeTurn, repository.activeTerminal = "", true
	first, err := service.RunCompaction(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.RunCompaction(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Cursor != second.Cursor || provider.streams.Load() != 1 {
		t.Fatalf("first=%+v second=%+v streams=%d", first, second, provider.streams.Load())
	}
	changed := request
	changed.Command.RequestDigest = repeatedDigest("9")
	if _, err := service.RunCompaction(context.Background(), changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed request error=%v", err)
	}
}

func TestRunCompactionRequiresDurableIdleInspector(t *testing.T) {
	request := validCompactRequest(t, protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "session-a", CommitSeq: 6, TransactionID: "tx-6"})
	provider := &compactionProvider{}
	service, err := NewService(Dependencies{Admission: passthroughAdmission{}, Lane: NewOperationLane(), Repository: &recordingRepository{head: request.ExpectedHead}, Providers: fakeProviderCatalog{log: &recordLog{}}, Provider: provider, Authorization: &compactionAuthorization{}, Evidence: &compactionEvidence{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunCompaction(context.Background(), request); err == nil || !strings.Contains(err.Error(), "idle inspector") {
		t.Fatalf("compaction without inspector error=%v", err)
	}
	if provider.streams.Load() != 0 {
		t.Fatal("compaction without inspector reached provider")
	}
}

func TestRunCompactionFaultsDoNotLeaveTheLaneHeldOrRepeatProviderEgress(t *testing.T) {
	for _, test := range []struct {
		name          string
		configure     func(*compactionRepository, *compactionProvider, *compactionEvidence)
		wantUncertain bool
	}{
		{name: "provider refusal", configure: func(_ *compactionRepository, p *compactionProvider, _ *compactionEvidence) { p.refuse = true }},
		{name: "malformed summary", configure: func(_ *compactionRepository, p *compactionProvider, _ *compactionEvidence) {
			p.summary = []byte(`{"goal":false}`)
		}},
		{name: "evidence failure", configure: func(_ *compactionRepository, _ *compactionProvider, e *compactionEvidence) {
			e.err = errors.New("evidence failure")
		}},
		{name: "append conflict", configure: func(r *compactionRepository, _ *compactionProvider, _ *compactionEvidence) {
			r.failAppendAt, r.appendStatus = 5, journal.AppendConflict
		}},
		{name: "known non commit", configure: func(r *compactionRepository, _ *compactionProvider, _ *compactionEvidence) {
			r.failAppendAt, r.appendStatus, r.lookup = 5, journal.AppendCommitUnknown, journal.TransactionNotCommitted
		}},
		{name: "commit unknown", wantUncertain: true, configure: func(r *compactionRepository, _ *compactionProvider, _ *compactionEvidence) {
			r.failAppendAt, r.appendStatus, r.lookup = 5, journal.AppendCommitUnknown, journal.TransactionUnknown
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			log := &recordLog{}
			repository := newCompactionRepository(t, log)
			request := validCompactRequest(t, repository.head)
			provider := &compactionProvider{log: log}
			evidence := &compactionEvidence{log: log}
			test.configure(repository, provider, evidence)
			service := newCompactionService(t, repository, log, provider, evidence)
			if _, err := service.RunCompaction(context.Background(), request); err == nil || test.wantUncertain != errors.Is(err, ErrCommitUncertain) {
				t.Fatalf("error=%v uncertain=%v", err, test.wantUncertain)
			}
			lease, err := service.lane.Acquire(context.Background(), OperationClaim{Kind: OperationCompaction, ControlOperationID: "probe"})
			if err != nil {
				t.Fatalf("compaction lane remains held: %v", err)
			}
			lease.Release()
			kinds := flattenAppendKinds(repository.appendRequests())
			if !test.wantUncertain && !containsString(kinds, protocol.EventCommandCompleted) {
				t.Fatalf("proven failure was not terminalized: %v", kinds)
			}
			if test.wantUncertain && containsString(kinds, protocol.EventCommandCompleted) {
				t.Fatalf("uncertain final append speculatively terminalized: %v", kinds)
			}
			if evidence.err == nil && (test.name == "append conflict" || test.name == "known non commit" || test.name == "commit unknown") && evidence.candidate.ID == "" {
				t.Fatal("durable orphan evidence was not retained for inspectability")
			}
			if test.wantUncertain {
				_, _ = service.RunCompaction(context.Background(), request)
				if provider.streams.Load() != 1 {
					t.Fatalf("uncertain replay repeated provider egress: %d", provider.streams.Load())
				}
			}
		})
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestRunCompactionCancellationBeforeAndDuringStream(t *testing.T) {
	t.Run("before dispatch", func(t *testing.T) {
		repository := newCompactionRepository(t, &recordLog{})
		provider := &compactionProvider{}
		service := newCompactionService(t, repository, &recordLog{}, provider, &compactionEvidence{})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := service.RunCompaction(ctx, validCompactRequest(t, repository.head)); !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v", err)
		}
		if provider.streams.Load() != 0 {
			t.Fatal("cancelled request reached provider")
		}
	})
	t.Run("during stream", func(t *testing.T) {
		repository := newCompactionRepository(t, &recordLog{})
		provider := &compactionProvider{block: make(chan struct{})}
		service := newCompactionService(t, repository, &recordLog{}, provider, &compactionEvidence{})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { _, err := service.RunCompaction(ctx, validCompactRequest(t, repository.head)); done <- err }()
		for provider.streams.Load() == 0 {
			time.Sleep(time.Millisecond)
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v", err)
		}
	})
}

func TestCollectCompactionSummaryRejectsContentAfterImmutableBlock(t *testing.T) {
	t.Parallel()
	service := &Service{deps: Dependencies{Admission: passthroughAdmission{}}}
	for name, events := range map[string][]protocol.ModelEvent{
		"delta after block": {
			{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: string(validCompactionSummary)}},
			{Kind: protocol.ModelEventContentDelta, Sequence: 2, Delta: &protocol.ContentDelta{BlockID: "summary", Kind: protocol.ContentText, Text: "late"}},
			{Kind: protocol.ModelEventTerminal, Sequence: 3, Terminal: &protocol.ModelTerminal{Reason: "stop"}},
		},
		"second block": {
			{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: string(validCompactionSummary)}},
			{Kind: protocol.ModelEventContentBlock, Sequence: 2, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: string(validCompactionSummary)}},
			{Kind: protocol.ModelEventTerminal, Sequence: 3, Terminal: &protocol.ModelTerminal{Reason: "stop"}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			stream := make(chan protocol.ModelEvent, len(events))
			for _, event := range events {
				stream <- event
			}
			close(stream)
			if _, _, err := service.collectCompactionSummary(t.Context(), "runtime-a", stream); err == nil {
				t.Fatal("accepted content after immutable summary block")
			}
		})
	}
}

func validCompactRequest(t *testing.T, head protocol.CommittedCursor) CompactRequest {
	t.Helper()
	return CompactRequest{Command: validStartTurnRequest().Command, WorkspaceID: "workspace-a", SessionID: "session-a", ExpectedHead: head, ProviderID: "provider-a", ModelID: "model-a", Runtime: validRuntimeManifest(t, "observation"), Trigger: compaction.TriggerManual}
}

func newCompactionService(t *testing.T, repository *compactionRepository, log *recordLog, provider ProviderService, evidence EvidenceRecorder) *Service {
	t.Helper()
	service, err := NewService(Dependencies{Admission: passthroughAdmission{}, Lane: &loggingLane{delegate: NewOperationLane(), log: log}, Repository: repository, Providers: fakeProviderCatalog{log: log}, Provider: provider, Authorization: &compactionAuthorization{log: log}, Evidence: evidence})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

type compactionRepository struct {
	mu             sync.Mutex
	head           protocol.CommittedCursor
	events         []protocol.EventRecord
	requests       []journal.AppendRequest
	log            *recordLog
	activeTurn     protocol.TurnID
	activeTerminal bool
	failAppendAt   int
	appendStatus   journal.AppendStatus
	lookup         journal.TransactionState
	transactions   map[protocol.TransactionID]journal.CommittedTransaction
	appendMarkers  bool
}

func newCompactionRepository(t *testing.T, log *recordLog) *compactionRepository {
	t.Helper()
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session-a"}
	r := &compactionRepository{log: log, lookup: journal.TransactionNotCommitted, transactions: make(map[protocol.TransactionID]journal.CommittedTransaction)}
	for seq := uint64(1); seq <= 6; seq++ {
		payload, err := canonicaljson.Marshal(protocol.UserMessageV1{Content: fmt.Sprintf("message-%d", seq)})
		if err != nil {
			t.Fatal(err)
		}
		r.events = append(r.events, protocol.EventRecord{Envelope: protocol.EventEnvelope{SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1, JournalKind: ref.Kind, JournalID: ref.ID, SessionID: "session-a", EventID: protocol.EventID(fmt.Sprintf("event-%d", seq)), Seq: seq, Time: time.Unix(int64(seq), 0).UTC(), Kind: protocol.EventUserMessage, TransactionID: protocol.TransactionID(fmt.Sprintf("tx-%d", seq)), Payload: payload}, Decoded: &protocol.UserMessageV1{Content: fmt.Sprintf("message-%d", seq)}})
	}
	r.head = protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 6, TransactionID: "tx-6"}
	return r
}

func newAutomaticCompactionRepository(t *testing.T, log *recordLog) *compactionRepository {
	repository := newCompactionRepository(t, log)
	repository.appendMarkers = true
	for index := range repository.events {
		repository.events[index].Envelope.TransactionID = "tx-initial"
	}
	repository.head = protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "session-a", CommitSeq: 7, TransactionID: "tx-initial"}
	return repository
}

func (r *compactionRepository) Inspect(context.Context, protocol.JournalRef) (journal.Inspection, error) {
	return journal.Inspection{Head: r.head, Writable: true}, nil
}
func (r *compactionRepository) Head(context.Context, protocol.JournalRef) (protocol.CommittedCursor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.head, nil
}
func (r *compactionRepository) ReadRange(_ context.Context, request journal.ReadRangeRequest) (journal.EventPage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	events := make([]protocol.EventRecord, 0, len(r.events))
	for _, event := range r.events {
		if event.Envelope.Kind != protocol.EventTransactionCommitted {
			events = append(events, protocol.DeepCopy(event))
		}
	}
	return journal.EventPage{Events: events, Head: r.head, Cursor: r.head}, nil
}
func (r *compactionRepository) ActiveTurn(context.Context, protocol.SessionID, protocol.CommittedCursor) (protocol.TurnID, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.activeTurn, r.activeTerminal, nil
}
func (r *compactionRepository) AppendBatch(_ context.Context, request journal.AppendRequest) (journal.AppendResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if request.ExpectedHead != r.head {
		return journal.AppendResult{Status: journal.AppendConflict, CurrentHead: r.head}, nil
	}
	if r.failAppendAt > 0 && len(r.requests)+1 == r.failAppendAt {
		r.failAppendAt = 0
		return journal.AppendResult{Status: r.appendStatus, CurrentHead: r.head}, errors.New("append fault")
	}
	r.requests = append(r.requests, protocol.DeepCopy(request))
	kinds := make([]string, len(request.Events))
	envelopes := make([]protocol.EventEnvelope, 0, len(request.Events))
	for index, event := range request.Events {
		seq := r.head.CommitSeq + uint64(index) + 1
		kinds[index] = event.Kind
		envelope := protocol.EventEnvelope{SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: event.PayloadVersion, JournalKind: request.Journal.Kind, JournalID: request.Journal.ID, EventID: event.EventID, SessionID: event.SessionID, Seq: seq, Time: event.Time, Kind: event.Kind, TaskID: event.TaskID, TurnID: event.TurnID, ActivityID: event.ActivityID, Actor: protocol.DeepCopy(event.Actor), RuntimeGenerationID: event.RuntimeGenerationID, TransactionID: request.TransactionID, Payload: protocol.DeepCopy(event.Payload)}
		r.events = append(r.events, protocol.EventRecord{Envelope: envelope})
		envelopes = append(envelopes, envelope)
	}
	if r.appendMarkers {
		markerSeq := r.head.CommitSeq + uint64(len(request.Events)) + 1
		marker := protocol.EventEnvelope{
			SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1, JournalKind: request.Journal.Kind, JournalID: request.Journal.ID,
			EventID: protocol.EventID(fmt.Sprintf("marker-%d", markerSeq)), SessionID: protocol.SessionID(request.Journal.ID), Seq: markerSeq,
			Time: time.Now().UTC(), Kind: protocol.EventTransactionCommitted, RuntimeGenerationID: request.Events[0].RuntimeGenerationID,
			TransactionID: request.TransactionID, Payload: json.RawMessage(`{"transaction_id":"test"}`),
		}
		r.events = append(r.events, protocol.EventRecord{Envelope: marker})
	}
	r.head = protocol.CommittedCursor{JournalKind: request.Journal.Kind, JournalID: request.Journal.ID, CommitSeq: r.head.CommitSeq + uint64(len(request.Events)) + 1, TransactionID: request.TransactionID}
	r.transactions[request.TransactionID] = journal.CommittedTransaction{Journal: request.Journal, TransactionID: request.TransactionID, Cursor: r.head, Events: protocol.DeepCopy(envelopes)}
	if r.log != nil {
		r.log.add("append(" + strings.Join(kinds, ",") + ")")
	}
	return journal.AppendResult{Status: journal.AppendCommitted, Cursor: r.head}, nil
}
func (r *compactionRepository) LookupTransaction(context.Context, protocol.JournalRef, protocol.TransactionID) (journal.TransactionLookup, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return journal.TransactionLookup{State: r.lookup}, nil
}
func (r *compactionRepository) ReadCommittedTransaction(_ context.Context, _ protocol.JournalRef, id protocol.TransactionID) (journal.CommittedTransaction, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	transaction, ok := r.transactions[id]
	if !ok {
		return journal.CommittedTransaction{}, errors.New("not committed")
	}
	return protocol.DeepCopy(transaction), nil
}
func (r *compactionRepository) Recover(context.Context, journal.RecoveryRequest) (journal.RecoveryResult, error) {
	return journal.RecoveryResult{}, nil
}
func (r *compactionRepository) appendRequests() []journal.AppendRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return protocol.DeepCopy(r.requests)
}

type compactionProvider struct {
	log     *recordLog
	summary []byte
	refuse  bool
	block   chan struct{}
	streams atomic.Int64
	request protocol.ModelRequest
}

type compactionRecordingAdapter struct{ starts atomic.Int64 }

func (*compactionRecordingAdapter) Kind() string { return "test" }
func (a *compactionRecordingAdapter) Normalize(_ context.Context, request protocol.ModelRequest) (provider.PreparedRequest, error) {
	return provider.NewPreparedRequest(a.Kind(), request)
}
func (a *compactionRecordingAdapter) StartPrepared(_ context.Context, prepared provider.PreparedRequest) (<-chan protocol.ModelEvent, error) {
	var request protocol.ModelRequest
	if err := prepared.Decode(a.Kind(), &request); err != nil {
		return nil, err
	}
	if request.ProviderID != "provider-a" || request.ModelID != "model-a" {
		return nil, fmt.Errorf("provider request identity drift")
	}
	a.starts.Add(1)
	stream := make(chan protocol.ModelEvent, 2)
	stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: string(validCompactionSummary)}}
	stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "stop"}}
	close(stream)
	return stream, nil
}

func (p *compactionProvider) Prepare(_ context.Context, _ protocol.ActivityID, _ string, request protocol.ModelRequest, _ protocol.Digest) (provider.ProviderHandle, error) {
	if p.log != nil {
		p.log.add("provider.prepare")
	}
	p.request = protocol.DeepCopy(request)
	return provider.ProviderHandle{}, nil
}
func (p *compactionProvider) Stream(ctx context.Context, _ provider.ProviderHandle, _ authorization.CommittedToken) (<-chan protocol.ModelEvent, error) {
	if p.log != nil {
		p.log.add("provider.stream")
	}
	p.streams.Add(1)
	stream := make(chan protocol.ModelEvent, 2)
	go func() {
		defer close(stream)
		if p.block != nil {
			select {
			case <-p.block:
			case <-ctx.Done():
				return
			}
		}
		if p.refuse {
			stream <- protocol.ModelEvent{Kind: protocol.ModelEventError, Sequence: 1, Error: &protocol.ProviderError{Code: "refused", Message: "refused"}}
			return
		}
		summary := p.summary
		if summary == nil {
			summary = validCompactionSummary
		}
		select {
		case stream <- protocol.ModelEvent{Kind: protocol.ModelEventContentBlock, Sequence: 1, Block: &protocol.ContentBlock{Kind: protocol.ContentText, Text: string(summary)}}:
		case <-ctx.Done():
			return
		}
		select {
		case stream <- protocol.ModelEvent{Kind: protocol.ModelEventTerminal, Sequence: 2, Terminal: &protocol.ModelTerminal{Reason: "stop"}}:
		case <-ctx.Done():
		}
	}()
	return stream, nil
}

var validCompactionSummary = []byte(`{"goal":"compact","constraints":[],"decisions":[],"files":[],"commands_and_tests":[],"unresolved":[],"children":[],"skills":[],"unknown_effects":[]}`)

type compactionEvidence struct {
	log       *recordLog
	err       error
	candidate protocol.EvidenceCandidate
}

func (e *compactionEvidence) Put(_ context.Context, candidate protocol.EvidenceCandidate) (protocol.EvidenceRecord, error) {
	if e.log != nil {
		e.log.add("evidence.put")
	}
	e.candidate = protocol.DeepCopy(candidate)
	if e.err != nil {
		return protocol.EvidenceRecord{}, e.err
	}
	body := protocol.EvidenceRecordBody{ID: candidate.ID, Kind: candidate.Kind, WorkspaceID: candidate.WorkspaceID, SessionID: candidate.SessionID, Availability: protocol.ContentAvailable, Blob: &protocol.BlobRef{Digest: repeatedDigest("e")}, MediaType: candidate.MediaType, Size: int64(len(candidate.Content)), ProducingActivityID: candidate.ProducingActivityID, Actor: candidate.Actor, Subject: candidate.Subject, CreatedAt: time.Now().UTC()}
	digest, _ := canonicaljson.Digest(body)
	return protocol.EvidenceRecord{Body: body, Digest: digest}, nil
}

type compactionAuthorization struct {
	log   *recordLog
	nonce atomic.Int64
}

type realCompactionAuthorization struct {
	gate  *authorization.Service
	nonce atomic.Int64
}

func (a *realCompactionAuthorization) Decide(_ context.Context, request protocol.AuthorizationRequest) (protocol.AuthorizationDecision, error) {
	return protocol.AuthorizationDecision{Request: request, Action: "allow", Scope: protocol.CanonicalAuthorizationScope{Capability: request.Action, Source: request.Source, Resources: request.Resources, Constraints: []protocol.AuthorizationConstraint{}}, Constraints: []protocol.AuthorizationConstraint{}, Lifetime: protocol.AuthorizationLifetimeOnce, PolicySource: "test", PolicyGeneration: request.PolicyGeneration, Reason: "allowed", DecidedAt: time.Now().UTC(), PlanDigest: request.PlanDigest, DecisionNonce: protocol.DecisionNonce(fmt.Sprintf("real-nonce-%d", a.nonce.Add(1)))}, nil
}
func (a *realCompactionAuthorization) ResolveInteractive(_ context.Context, request protocol.AuthorizationRequest, decision protocol.AuthorizationDecision, response protocol.ApprovalResponse) (protocol.AuthorizationDecision, error) {
	return a.gate.ResolveInteractive(request, decision, response)
}
func (a *realCompactionAuthorization) Issue(ctx context.Context, reference authorization.CommitReference) (authorization.CommittedToken, error) {
	return a.gate.Issue(ctx, reference)
}
func (a *realCompactionAuthorization) Dispatch(ctx context.Context, token authorization.CommittedToken, binding authorization.DispatchBinding, callback func(context.Context) error) error {
	return a.gate.Dispatch(ctx, token, binding, callback)
}

func (a *compactionAuthorization) Decide(_ context.Context, request protocol.AuthorizationRequest) (protocol.AuthorizationDecision, error) {
	if a.log != nil {
		a.log.add("authorization.decide")
	}
	return protocol.AuthorizationDecision{Request: request, Action: "allow", Scope: protocol.CanonicalAuthorizationScope{Capability: request.Action, Source: request.Source, Resources: request.Resources, Constraints: []protocol.AuthorizationConstraint{}}, Constraints: []protocol.AuthorizationConstraint{}, Lifetime: protocol.AuthorizationLifetimeOnce, PolicySource: "test", PolicyGeneration: request.PolicyGeneration, Reason: "allowed", DecidedAt: time.Now().UTC(), PlanDigest: request.PlanDigest, DecisionNonce: protocol.DecisionNonce(fmt.Sprintf("compact-nonce-%d", a.nonce.Add(1)))}, nil
}
func (*compactionAuthorization) ResolveInteractive(context.Context, protocol.AuthorizationRequest, protocol.AuthorizationDecision, protocol.ApprovalResponse) (protocol.AuthorizationDecision, error) {
	return protocol.AuthorizationDecision{}, errors.New("unexpected interactive authorization")
}
func (a *compactionAuthorization) Issue(context.Context, authorization.CommitReference) (authorization.CommittedToken, error) {
	if a.log != nil {
		a.log.add("authorization.issue")
	}
	return authorization.CommittedToken{}, nil
}
func (a *compactionAuthorization) Dispatch(ctx context.Context, _ authorization.CommittedToken, _ authorization.DispatchBinding, callback func(context.Context) error) error {
	if a.log != nil {
		a.log.add("authorization.dispatch")
	}
	return callback(ctx)
}
