package authorization_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestDecisionBindingRejectsEveryRequestFieldMutation(t *testing.T) {
	request := boundRequest(t)
	decision := allowOnce(request)
	mutations := map[string]func(*protocol.AuthorizationRequest){
		"request ID":            func(r *protocol.AuthorizationRequest) { r.RequestID = "request-other" },
		"principal":             func(r *protocol.AuthorizationRequest) { r.Principal.ID = "principal-other" },
		"actor":                 func(r *protocol.AuthorizationRequest) { r.Actor.ID = "actor-other" },
		"session":               func(r *protocol.AuthorizationRequest) { r.SessionID = "session-other" },
		"control operation":     func(r *protocol.AuthorizationRequest) { r.ControlOperationID = "control-1" },
		"task":                  func(r *protocol.AuthorizationRequest) { r.TaskID = "task-other" },
		"turn":                  func(r *protocol.AuthorizationRequest) { r.TurnID = "turn-other" },
		"activity":              func(r *protocol.AuthorizationRequest) { r.ActivityID = "activity-other" },
		"parent activity":       func(r *protocol.AuthorizationRequest) { r.ParentActivityID = "activity-parent-other" },
		"call":                  func(r *protocol.AuthorizationRequest) { r.CallID = "call-other" },
		"queue":                 func(r *protocol.AuthorizationRequest) { r.QueueID = "queue-other" },
		"source name":           func(r *protocol.AuthorizationRequest) { r.Source.Name = "other" },
		"source authority":      func(r *protocol.AuthorizationRequest) { r.Source.Authority = "other" },
		"source kind":           func(r *protocol.AuthorizationRequest) { r.Source.Source = "extension" },
		"source revision":       func(r *protocol.AuthorizationRequest) { r.SourceRevision = "source-r2" },
		"descriptor digest":     func(r *protocol.AuthorizationRequest) { r.DescriptorDigest = authDigest('d') },
		"action":                func(r *protocol.AuthorizationRequest) { r.Action = "other" },
		"resource identity":     func(r *protocol.AuthorizationRequest) { r.Resources[0].CanonicalID = "/workspace/b.txt" },
		"resource parent":       func(r *protocol.AuthorizationRequest) { r.Resources[0].ParentID = "/other" },
		"resource digest":       func(r *protocol.AuthorizationRequest) { r.Resources[0].Digest = strings.Repeat("9", 64) },
		"resource attributes":   func(r *protocol.AuthorizationRequest) { r.Resources[0].Attributes[0].Value = "write" },
		"execution locus":       func(r *protocol.AuthorizationRequest) { r.ExecutionLocus = "remote" },
		"requested profile":     func(r *protocol.AuthorizationRequest) { r.RequestedProfile = "restricted" },
		"effective profile":     func(r *protocol.AuthorizationRequest) { r.EffectiveProfile = "restricted" },
		"effect":                func(r *protocol.AuthorizationRequest) { r.Effect = "mutation" },
		"boundary":              func(r *protocol.AuthorizationRequest) { r.Boundary = "filesystem_external" },
		"reversibility":         func(r *protocol.AuthorizationRequest) { r.Reversibility = "irreversible" },
		"verification coverage": func(r *protocol.AuthorizationRequest) { r.VerificationCoverage = "none" },
		"runtime generation":    func(r *protocol.AuthorizationRequest) { r.RuntimeGenerationID = "generation-2" },
		"policy generation":     func(r *protocol.AuthorizationRequest) { r.PolicyGeneration = "policy-2" },
		"policy source":         func(r *protocol.AuthorizationRequest) { r.PolicyProvenance[0].Source = "project" },
		"policy revision":       func(r *protocol.AuthorizationRequest) { r.PolicyProvenance[0].Revision = "revision-2" },
		"policy provenance gen": func(r *protocol.AuthorizationRequest) { r.PolicyProvenance[0].Generation = "policy-2" },
		"policy hard deny":      func(r *protocol.AuthorizationRequest) { r.PolicyProvenance[0].HardDeny = true },
		"plan digest":           func(r *protocol.AuthorizationRequest) { r.PlanDigest = authDigest('p') },
		"request digest":        func(r *protocol.AuthorizationRequest) { r.RequestDigest = authDigest('q') },
		"dispatch digest":       func(r *protocol.AuthorizationRequest) { r.DispatchDigest = authDigest('x') },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := protocol.DeepCopy(request)
			mutate(&changed)
			if err := authorization.ValidateBinding(changed, decision); !errors.Is(err, authorization.ErrStaleDecision) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	if err := authorization.ValidateBinding(request, decision); err != nil {
		t.Fatalf("unchanged binding rejected: %v", err)
	}
}

func TestAuthorizationInteractiveResponseCannotAuthorBindings(t *testing.T) {
	request := boundRequest(t)
	ask := allowOnce(request)
	ask.Action = "ask"
	scopeDigest, err := canonicaljson.Digest(ask.Scope)
	if err != nil {
		t.Fatal(err)
	}
	service := authorization.NewService(nil)
	response := protocol.ApprovalResponse{
		RequestID: request.RequestID, Action: "allow", Lifetime: protocol.AuthorizationLifetimeSession,
		ScopeDigest: scopeDigest, Actor: protocol.ActorRef{ID: "user-1", Kind: protocol.ActorUser}, Reason: "approved",
	}
	resolved, err := service.ResolveInteractive(request, ask, response)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Action != "allow" || resolved.Lifetime != protocol.AuthorizationLifetimeSession || resolved.Request.RequestDigest != request.RequestDigest || resolved.Scope.Source != request.Source || len(resolved.Scope.Resources) != 1 {
		t.Fatalf("resolved decision lost server bindings: %#v", resolved)
	}
	response.ScopeDigest = authDigest('z')
	if _, err := service.ResolveInteractive(request, ask, response); !errors.Is(err, authorization.ErrStaleDecision) {
		t.Fatalf("scope substitution err=%v", err)
	}
	ask.Action = "deny"
	response.ScopeDigest = scopeDigest
	if _, err := service.ResolveInteractive(request, ask, response); !errors.Is(err, authorization.ErrTerminalDecision) {
		t.Fatalf("terminal policy err=%v", err)
	}
}

func TestAuthorizationInteractivePreservesPendingExpiry(t *testing.T) {
	request := boundRequest(t)
	pending := allowOnce(request)
	pending.Action = "ask"
	expires := time.Now().UTC().Add(250 * time.Millisecond)
	pending.ExpiresAt = &expires
	scopeDigest, err := canonicaljson.Digest(pending.Scope)
	if err != nil {
		t.Fatal(err)
	}
	response := protocol.ApprovalResponse{
		RequestID: request.RequestID, Action: "allow", Lifetime: protocol.AuthorizationLifetimeOnce,
		ScopeDigest: scopeDigest, Actor: protocol.ActorRef{ID: "user-1", Kind: protocol.ActorUser}, Reason: "approved",
	}
	resolved, err := authorization.NewService(nil).ResolveInteractive(request, pending, response)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.ExpiresAt == nil || !resolved.ExpiresAt.Equal(expires) {
		t.Fatalf("pending expiry was broadened: pending=%v resolved=%v", pending.ExpiresAt, resolved.ExpiresAt)
	}
	time.Sleep(time.Until(expires) + 20*time.Millisecond)
	if err := authorization.ValidateBinding(request, resolved); !errors.Is(err, authorization.ErrExpiredDecision) {
		t.Fatalf("resolved near-expiry decision remained valid: %v", err)
	}
}

func TestAuthorizationInteractiveRejectsAlreadyExpiredPendingDecision(t *testing.T) {
	request := boundRequest(t)
	pending := allowOnce(request)
	pending.Action = "ask"
	expires := time.Now().UTC().Add(-time.Millisecond)
	pending.DecidedAt = expires.Add(-time.Second)
	pending.ExpiresAt = &expires
	scopeDigest, err := canonicaljson.Digest(pending.Scope)
	if err != nil {
		t.Fatal(err)
	}
	response := protocol.ApprovalResponse{RequestID: request.RequestID, Action: "allow", Lifetime: protocol.AuthorizationLifetimeOnce, ScopeDigest: scopeDigest, Actor: protocol.ActorRef{ID: "user-1", Kind: protocol.ActorUser}, Reason: "late"}
	if _, err := authorization.NewService(nil).ResolveInteractive(request, pending, response); !errors.Is(err, authorization.ErrExpiredDecision) {
		t.Fatalf("expired pending decision err=%v", err)
	}
}

func TestAuthorizationIssuerReadsNamedCommittedTransactions(t *testing.T) {
	fixture := committedFixture(t)
	service := authorization.NewService(fixture.reader)
	token, err := service.Issue(context.Background(), fixture.reference)
	if err != nil {
		t.Fatal(err)
	}
	if fixture.reader.reads.Load() != 2 {
		t.Fatalf("committed transaction reads=%d want 2", fixture.reader.reads.Load())
	}
	request := fixture.decision.Request
	if !token.ValidFor(request.ActivityID, request.CallID, request.PlanDigest, request.RequestDigest, request.DispatchDigest, request.RuntimeGenerationID, fixture.decision.DecisionNonce) {
		t.Fatal("issued token does not retain exact committed binding")
	}
	if token.ValidFor(request.ActivityID, request.CallID, request.PlanDigest, request.RequestDigest, authDigest('x'), request.RuntimeGenerationID, fixture.decision.DecisionNonce) {
		t.Fatal("token accepted changed dispatch digest")
	}
}

func TestAuthorizationIssuerRejectsExpiredAndRepeatedDecision(t *testing.T) {
	expired := committedFixture(t)
	expires := time.Now().Add(-time.Minute)
	expired.decision.DecidedAt = expires.Add(-time.Minute)
	expired.decision.ExpiresAt = &expires
	expired.replaceDecision(t)
	if _, err := authorization.NewService(expired.reader).Issue(context.Background(), expired.reference); !errors.Is(err, authorization.ErrExpiredDecision) {
		t.Fatalf("expired err=%v", err)
	}

	fixture := committedFixture(t)
	service := authorization.NewService(fixture.reader)
	if _, err := service.Issue(context.Background(), fixture.reference); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Issue(context.Background(), fixture.reference); !errors.Is(err, authorization.ErrAlreadyUsedDecision) {
		t.Fatalf("reissue err=%v", err)
	}
}

func TestAuthorizationIssuerRejectsUncommittedAmbiguousAndMismatchedRecords(t *testing.T) {
	tests := map[string]func(*committedAuthorizationFixture){
		"deny": func(f *committedAuthorizationFixture) {
			f.decision.Action = "deny"
			f.replaceDecision(t)
		},
		"ask": func(f *committedAuthorizationFixture) {
			f.decision.Action = "ask"
			f.replaceDecision(t)
		},
		"wrong decision transaction": func(f *committedAuthorizationFixture) {
			f.reference.DecisionTransactionID = "missing"
		},
		"wrong start transaction": func(f *committedAuthorizationFixture) {
			f.reference.StartTransactionID = "missing"
		},
		"substituted journal": func(f *committedAuthorizationFixture) {
			f.reader.ignoreJournal = true
			transaction := f.reader.transactions[f.reference.DecisionTransactionID]
			transaction.Journal = protocol.JournalRef{Kind: protocol.JournalSession, ID: "other-session"}
			f.reader.transactions[f.reference.DecisionTransactionID] = transaction
		},
		"ambiguous decisions": func(f *committedAuthorizationFixture) {
			duplicate := f.reader.transactions[f.reference.DecisionTransactionID].Events[0]
			duplicate.EventID = "decision-event-2"
			tx := f.reader.transactions[f.reference.DecisionTransactionID]
			tx.Events = append(tx.Events, duplicate)
			f.reader.transactions[f.reference.DecisionTransactionID] = tx
		},
		"consumed request": func(f *committedAuthorizationFixture) {
			f.consumed.RequestID = "other"
			f.replaceStarted(t)
		},
		"consumed nonce": func(f *committedAuthorizationFixture) {
			f.consumed.DecisionNonce = "other"
			f.replaceStarted(t)
		},
		"consumed decision digest": func(f *committedAuthorizationFixture) {
			f.consumed.DecisionDigest = authDigest('x')
			f.replaceStarted(t)
		},
		"consumed activity": func(f *committedAuthorizationFixture) {
			f.consumed.ActivityID = "other"
			f.replaceStarted(t)
		},
		"consumed call": func(f *committedAuthorizationFixture) {
			f.consumed.CallID = "other"
			f.replaceStarted(t)
		},
		"consumed plan": func(f *committedAuthorizationFixture) {
			f.consumed.PlanDigest = authDigest('x')
			f.replaceStarted(t)
		},
		"consumed request digest": func(f *committedAuthorizationFixture) {
			f.consumed.RequestDigest = authDigest('x')
			f.replaceStarted(t)
		},
		"consumed dispatch": func(f *committedAuthorizationFixture) {
			f.consumed.DispatchDigest = authDigest('x')
			f.replaceStarted(t)
		},
		"consumed generation": func(f *committedAuthorizationFixture) {
			f.consumed.RuntimeGenerationID = "other"
			f.replaceStarted(t)
		},
		"started decision event": func(f *committedAuthorizationFixture) {
			f.started.DecisionEventID = "other"
			f.replaceStarted(t)
		},
		"started nonce": func(f *committedAuthorizationFixture) {
			f.started.DecisionNonce = "other"
			f.replaceStarted(t)
		},
		"started activity": func(f *committedAuthorizationFixture) {
			f.started.ActivityID = "other"
			f.replaceStarted(t)
		},
		"started call": func(f *committedAuthorizationFixture) {
			f.started.CallID = "other"
			f.replaceStarted(t)
		},
		"started plan": func(f *committedAuthorizationFixture) {
			f.started.PlanDigest = authDigest('x')
			f.replaceStarted(t)
		},
		"started request": func(f *committedAuthorizationFixture) {
			f.started.RequestDigest = authDigest('x')
			f.replaceStarted(t)
		},
		"started dispatch": func(f *committedAuthorizationFixture) {
			f.started.DispatchDigest = authDigest('x')
			f.replaceStarted(t)
		},
		"started generation": func(f *committedAuthorizationFixture) {
			f.started.RuntimeGenerationID = "other"
			f.replaceStarted(t)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := committedFixture(t)
			mutate(&fixture)
			if _, err := authorization.NewService(fixture.reader).Issue(context.Background(), fixture.reference); err == nil {
				t.Fatal("invalid committed records issued a token")
			}
		})
	}
}

func TestAuthorizationIssuerRequiresOrderedDistinctCommittedCursors(t *testing.T) {
	tests := map[string]func(*committedAuthorizationFixture){
		"zero decision cursor": func(f *committedAuthorizationFixture) {
			tx := f.reader.transactions[f.reference.DecisionTransactionID]
			tx.Cursor = protocol.CommittedCursor{}
			f.reader.transactions[f.reference.DecisionTransactionID] = tx
		},
		"zero start cursor": func(f *committedAuthorizationFixture) {
			tx := f.reader.transactions[f.reference.StartTransactionID]
			tx.Cursor = protocol.CommittedCursor{}
			f.reader.transactions[f.reference.StartTransactionID] = tx
		},
		"decision cursor journal": func(f *committedAuthorizationFixture) {
			tx := f.reader.transactions[f.reference.DecisionTransactionID]
			tx.Cursor.JournalID = "other-session"
			f.reader.transactions[f.reference.DecisionTransactionID] = tx
		},
		"start cursor transaction": func(f *committedAuthorizationFixture) {
			tx := f.reader.transactions[f.reference.StartTransactionID]
			tx.Cursor.TransactionID = "other-start"
			f.reader.transactions[f.reference.StartTransactionID] = tx
		},
		"equal commit sequence": func(f *committedAuthorizationFixture) {
			tx := f.reader.transactions[f.reference.StartTransactionID]
			tx.Cursor.CommitSeq = f.reader.transactions[f.reference.DecisionTransactionID].Cursor.CommitSeq
			f.reader.transactions[f.reference.StartTransactionID] = tx
		},
		"reversed commit sequence": func(f *committedAuthorizationFixture) {
			tx := f.reader.transactions[f.reference.StartTransactionID]
			tx.Cursor.CommitSeq = f.reader.transactions[f.reference.DecisionTransactionID].Cursor.CommitSeq - 1
			f.reader.transactions[f.reference.StartTransactionID] = tx
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := committedFixture(t)
			mutate(&fixture)
			if _, err := authorization.NewService(fixture.reader).Issue(context.Background(), fixture.reference); err == nil {
				t.Fatal("invalid committed cursor issued a token")
			}
		})
	}

	t.Run("same transaction", func(t *testing.T) {
		fixture := committedFixture(t)
		decisionTx := fixture.reader.transactions[fixture.reference.DecisionTransactionID]
		startTx := fixture.reader.transactions[fixture.reference.StartTransactionID]
		for index := range startTx.Events {
			startTx.Events[index].TransactionID = decisionTx.TransactionID
		}
		decisionTx.Events = append(decisionTx.Events, startTx.Events...)
		fixture.reader.transactions[decisionTx.TransactionID] = decisionTx
		delete(fixture.reader.transactions, fixture.reference.StartTransactionID)
		fixture.reference.StartTransactionID = fixture.reference.DecisionTransactionID
		if _, err := authorization.NewService(fixture.reader).Issue(context.Background(), fixture.reference); err == nil {
			t.Fatal("same transaction was accepted for decision and start")
		}
	})
}

func TestDispatchGateRejectsZeroRepeatedRevokedAndConcurrentUseBeforeCallback(t *testing.T) {
	fixture := committedFixture(t)
	service := authorization.NewService(fixture.reader)
	binding := fixture.binding("tool", "handle-1")
	var callbacks atomic.Int64
	if err := service.Dispatch(context.Background(), authorization.CommittedToken{}, binding, func(context.Context) error {
		callbacks.Add(1)
		return nil
	}); !errors.Is(err, authorization.ErrInvalidCommittedToken) {
		t.Fatalf("zero token err=%v", err)
	}

	token := fixture.issue(t, service)
	if err := service.Dispatch(context.Background(), token, binding, func(context.Context) error {
		callbacks.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.Dispatch(context.Background(), token, binding, func(context.Context) error {
		callbacks.Add(1)
		return nil
	}); !errors.Is(err, authorization.ErrAlreadyDispatched) {
		t.Fatalf("repeat err=%v", err)
	}
	if callbacks.Load() != 1 {
		t.Fatalf("callbacks=%d want 1", callbacks.Load())
	}

	revoked := committedFixture(t)
	revokedService := authorization.NewService(revoked.reader)
	revokedToken := revoked.issue(t, revokedService)
	revokedService.Revoke(revoked.decision.DecisionNonce)
	if err := revokedService.Dispatch(context.Background(), revokedToken, revoked.binding("tool", "handle-2"), func(context.Context) error {
		callbacks.Add(1)
		return nil
	}); !errors.Is(err, authorization.ErrRevokedDecision) {
		t.Fatalf("revoked err=%v", err)
	}

	concurrent := committedFixture(t)
	concurrentService := authorization.NewService(concurrent.reader)
	concurrentToken := concurrent.issue(t, concurrentService)
	start := make(chan struct{})
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			errs <- concurrentService.Dispatch(context.Background(), concurrentToken, concurrent.binding("tool", "handle-3"), func(context.Context) error {
				callbacks.Add(1)
				return nil
			})
		}()
	}
	close(start)
	var success, already int
	for range 2 {
		switch err := <-errs; {
		case err == nil:
			success++
		case errors.Is(err, authorization.ErrAlreadyDispatched):
			already++
		default:
			t.Fatalf("concurrent err=%v", err)
		}
	}
	if success != 1 || already != 1 {
		t.Fatalf("success=%d already=%d", success, already)
	}
}

func TestDispatchGateRejectsProviderKindForToolDecision(t *testing.T) {
	fixture := committedFixture(t)
	service := authorization.NewService(fixture.reader)
	token := fixture.issue(t, service)
	var callbacks atomic.Int64
	if err := service.Dispatch(context.Background(), token, fixture.binding("provider", "wrong-kind"), func(context.Context) error {
		callbacks.Add(1)
		return nil
	}); !errors.Is(err, authorization.ErrInvalidCommittedToken) {
		t.Fatalf("kind mismatch err=%v", err)
	}
	if callbacks.Load() != 0 {
		t.Fatalf("kind mismatch callbacks=%d", callbacks.Load())
	}
}

func TestDispatchRevokeRacingStartHasOneLinearizedOutcome(t *testing.T) {
	for index := 0; index < 128; index++ {
		fixture := committedFixture(t)
		service := authorization.NewService(fixture.reader)
		token := fixture.issue(t, service)
		start := make(chan struct{})
		var callback atomic.Int64
		var dispatchErr error
		var wait sync.WaitGroup
		wait.Add(2)
		go func() {
			defer wait.Done()
			<-start
			dispatchErr = service.Dispatch(context.Background(), token, fixture.binding("tool", "race-handle"), func(context.Context) error {
				callback.Add(1)
				return nil
			})
		}()
		go func() {
			defer wait.Done()
			<-start
			service.Revoke(fixture.decision.DecisionNonce)
		}()
		close(start)
		wait.Wait()
		if dispatchErr == nil && callback.Load() != 1 {
			t.Fatalf("iteration %d: successful dispatch callbacks=%d", index, callback.Load())
		}
		if errors.Is(dispatchErr, authorization.ErrRevokedDecision) && callback.Load() != 0 {
			t.Fatalf("iteration %d: revoked dispatch callbacks=%d", index, callback.Load())
		}
		if dispatchErr != nil && !errors.Is(dispatchErr, authorization.ErrRevokedDecision) {
			t.Fatalf("iteration %d: err=%v", index, dispatchErr)
		}
	}
}

func TestDispatchRegistersCancellationThatLaterRevokeInvokes(t *testing.T) {
	fixture := committedFixture(t)
	service := authorization.NewService(fixture.reader)
	token := fixture.issue(t, service)
	runContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := service.Dispatch(context.Background(), token, fixture.binding("tool", "cancel-handle"), func(registrationContext context.Context) error {
		return authorization.RegisterCancellation(registrationContext, cancel)
	}); err != nil {
		t.Fatal(err)
	}
	service.Revoke(fixture.decision.DecisionNonce)
	select {
	case <-runContext.Done():
	case <-time.After(time.Second):
		t.Fatal("later revoke did not invoke registered cancellation")
	}
}

type committedReader struct {
	transactions  map[protocol.TransactionID]journal.CommittedTransaction
	reads         atomic.Int64
	ignoreJournal bool
}

func (r *committedReader) ReadCommittedTransaction(_ context.Context, ref protocol.JournalRef, transactionID protocol.TransactionID) (journal.CommittedTransaction, error) {
	r.reads.Add(1)
	transaction, ok := r.transactions[transactionID]
	if !ok || !r.ignoreJournal && transaction.Journal != ref {
		return journal.CommittedTransaction{}, errors.New("not committed")
	}
	return protocol.DeepCopy(transaction), nil
}

type committedAuthorizationFixture struct {
	reader    *committedReader
	reference authorization.CommitReference
	decision  protocol.AuthorizationDecision
	consumed  protocol.AuthorizationDecisionConsumedV1
	started   protocol.ActivityStartedV1
}

func committedFixture(t *testing.T) committedAuthorizationFixture {
	t.Helper()
	request := boundRequest(t)
	decision := allowOnce(request)
	decisionDigest, err := canonicaljson.Digest(decision)
	if err != nil {
		t.Fatal(err)
	}
	consumed := protocol.AuthorizationDecisionConsumedV1{
		DecisionNonce: decision.DecisionNonce, DecisionEventID: "decision-event", DecisionDigest: decisionDigest,
		RequestID: request.RequestID, ActivityID: request.ActivityID, CallID: request.CallID, PlanDigest: request.PlanDigest,
		RequestDigest: request.RequestDigest, DispatchDigest: request.DispatchDigest, RuntimeGenerationID: request.RuntimeGenerationID,
	}
	started := protocol.ActivityStartedV1{
		DecisionNonce: decision.DecisionNonce, DecisionEventID: "decision-event", ActivityID: request.ActivityID,
		CallID: request.CallID, PlanDigest: request.PlanDigest, RequestDigest: request.RequestDigest,
		DispatchDigest: request.DispatchDigest, RuntimeGenerationID: request.RuntimeGenerationID, DispatchState: "registered",
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session-1"}
	decisionTx := protocol.TransactionID("decision-tx")
	startTx := protocol.TransactionID("start-tx")
	reader := &committedReader{transactions: map[protocol.TransactionID]journal.CommittedTransaction{}}
	fixture := committedAuthorizationFixture{
		reader: reader, decision: decision, consumed: consumed, started: started,
		reference: authorization.CommitReference{Journal: ref, DecisionTransactionID: decisionTx, DecisionEventID: "decision-event", StartTransactionID: startTx, ConsumedEventID: "consumed-event", StartedEventID: "started-event"},
	}
	fixture.replaceDecision(t)
	fixture.replaceStarted(t)
	return fixture
}

func (f *committedAuthorizationFixture) replaceDecision(t *testing.T) {
	t.Helper()
	payload := mustJSON(t, protocol.AuthorizationDecidedV1{Decision: f.decision})
	f.reader.transactions[f.reference.DecisionTransactionID] = journal.CommittedTransaction{
		Journal: f.reference.Journal, TransactionID: f.reference.DecisionTransactionID,
		Cursor: protocol.CommittedCursor{JournalKind: f.reference.Journal.Kind, JournalID: f.reference.Journal.ID, CommitSeq: 10, TransactionID: f.reference.DecisionTransactionID},
		Events: []protocol.EventEnvelope{{EventID: f.reference.DecisionEventID, TransactionID: f.reference.DecisionTransactionID, Kind: protocol.EventAuthorizationDecided, Payload: payload}},
	}
}

func (f *committedAuthorizationFixture) replaceStarted(t *testing.T) {
	t.Helper()
	f.reader.transactions[f.reference.StartTransactionID] = journal.CommittedTransaction{
		Journal: f.reference.Journal, TransactionID: f.reference.StartTransactionID,
		Cursor: protocol.CommittedCursor{JournalKind: f.reference.Journal.Kind, JournalID: f.reference.Journal.ID, CommitSeq: 11, TransactionID: f.reference.StartTransactionID},
		Events: []protocol.EventEnvelope{
			{EventID: f.reference.ConsumedEventID, TransactionID: f.reference.StartTransactionID, Kind: protocol.EventAuthorizationDecisionConsumed, Payload: mustJSON(t, f.consumed)},
			{EventID: f.reference.StartedEventID, TransactionID: f.reference.StartTransactionID, Kind: protocol.EventActivityStarted, Payload: mustJSON(t, f.started)},
		},
	}
}

func (f committedAuthorizationFixture) issue(t *testing.T, service *authorization.Service) authorization.CommittedToken {
	t.Helper()
	token, err := service.Issue(context.Background(), f.reference)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func (f committedAuthorizationFixture) binding(kind, handleID string) authorization.DispatchBinding {
	r := f.decision.Request
	return authorization.DispatchBinding{Kind: kind, HandleID: handleID, ActivityID: r.ActivityID, CallID: r.CallID, PlanDigest: r.PlanDigest, RequestDigest: r.RequestDigest, DispatchDigest: r.DispatchDigest, RuntimeGenerationID: r.RuntimeGenerationID}
}

func boundRequest(t *testing.T) protocol.AuthorizationRequest {
	t.Helper()
	request := protocol.AuthorizationRequest{
		RequestID: "request-1", Principal: protocol.ActorRef{ID: "principal-1", Kind: protocol.ActorUser}, Actor: protocol.ActorRef{ID: "agent-1", Kind: protocol.ActorAgent},
		SessionID: "session-1", TaskID: "task-1", TurnID: "turn-1", ActivityID: "activity-1", ParentActivityID: "activity-parent-1",
		CallID: "call-1", QueueID: "queue-1", Source: protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "read"}, SourceRevision: "source-r1",
		DescriptorDigest: authDigest('a'), Action: "read", Resources: []protocol.ResourceTarget{{Kind: "file", CanonicalID: "/workspace/a.txt", ParentID: "/workspace", Digest: strings.Repeat("8", 64), Attributes: []protocol.ResourceAttribute{{Name: "access", Value: "read"}}}},
		ExecutionLocus: "local", RequestedProfile: "default", EffectiveProfile: "default", Effect: "observation", Boundary: "workspace", Reversibility: "not_applicable", VerificationCoverage: "full",
		RuntimeGenerationID: "generation-1", PolicyGeneration: "policy-1", PolicyProvenance: []protocol.PolicyProvenance{{Source: "user", Revision: "revision-1", Generation: "policy-1"}},
		PlanDigest: authDigest('b'), RequestDigest: authDigest('c'), DispatchDigest: authDigest('e'),
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("test request: %v", err)
	}
	return request
}

func allowOnce(request protocol.AuthorizationRequest) protocol.AuthorizationDecision {
	return protocol.AuthorizationDecision{
		Request: protocol.DeepCopy(request), Action: "allow",
		Scope:       protocol.CanonicalAuthorizationScope{Capability: request.Action, Source: request.Source, Resources: protocol.DeepCopy(request.Resources), Constraints: []protocol.AuthorizationConstraint{}},
		Constraints: []protocol.AuthorizationConstraint{}, Lifetime: protocol.AuthorizationLifetimeOnce,
		PolicySource: "user", PolicyGeneration: request.PolicyGeneration, Reason: "policy allow", DecidedAt: time.Now().UTC(), PlanDigest: request.PlanDigest, DecisionNonce: "nonce-1",
	}
}

func authDigest(value byte) protocol.Digest {
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat(string(value), 64)}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
