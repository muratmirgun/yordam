package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestSessionStateInitialInspectThenIncrementalReadRange(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session"}
	firstHead := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 2, TransactionID: "txn-1"}
	secondHead := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 4, TransactionID: "txn-2"}
	reader := &sessionStateReader{
		inspection: journal.Inspection{Journal: ref, Head: firstHead, Writable: true, Events: []protocol.EventRecord{
			v2SessionEvent(1, "txn-1", protocol.EventModeChanged, &protocol.ModeChangedV1{Mode: "safe"}),
		}},
		pages: []journal.EventPage{{Events: []protocol.EventRecord{
			v2SessionEvent(3, "txn-2", protocol.EventModelChanged, &protocol.ModelChangedV1{ProviderID: "primary", ModelID: "model-b"}),
		}, Cursor: secondHead, Head: secondHead}},
	}
	projector := app.NewIncrementalSessionStateProjector(reader)
	state, err := projector.Open(t.Context(), ref, app.SessionState{
		Mode: domain.ModeAsk, Selection: domain.ModelSelection{Profile: "primary", Model: "model-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.Mode != domain.ModeSafe || state.Head != firstHead || reader.inspectCalls != 1 {
		t.Fatalf("opened state=%+v inspect=%d", state, reader.inspectCalls)
	}
	state, err = projector.ProjectSessionState(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	if state.Selection != (domain.ModelSelection{Profile: "primary", Model: "model-b"}) || state.Head != secondHead || reader.inspectCalls != 1 || len(reader.ranges) != 1 || reader.ranges[0].After != firstHead {
		t.Fatalf("incremental state=%+v inspect=%d ranges=%+v", state, reader.inspectCalls, reader.ranges)
	}
}

func TestSessionStateOpenReturnsLastCompleteTransactionPrefixReadOnly(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session"}
	prefix := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 2, TransactionID: "txn-valid"}
	head := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 5, TransactionID: "txn-unsupported"}
	reader := &sessionStateReader{inspection: journal.Inspection{
		Journal: ref, Head: head, Writable: true,
		Events: []protocol.EventRecord{
			v2SessionEvent(1, "txn-valid", protocol.EventModeChanged, &protocol.ModeChangedV1{Mode: "safe"}),
			v2SessionEvent(3, "txn-unsupported", protocol.EventModelChanged, &protocol.ModelChangedV1{ProviderID: "primary", ModelID: "model-b"}),
			v2SessionEvent(4, "txn-unsupported", "future.session_policy", &struct{}{}),
		},
	}}

	state, err := app.NewIncrementalSessionStateProjector(reader).Open(t.Context(), ref, app.SessionState{
		Mode: domain.ModeAsk, Selection: domain.ModelSelection{Profile: "primary", Model: "model-a"},
	})
	if !errors.Is(err, app.ErrSessionStateReadOnly) {
		t.Fatalf("error=%v", err)
	}
	if state.Head != prefix || state.Writable || state.Mode != domain.ModeSafe || state.Selection.Model != "model-a" {
		t.Fatalf("validated prefix=%+v", state)
	}
	if len(state.Diagnostics) != 1 || state.Diagnostics[0].Code != "session_state.projection_failed" {
		t.Fatalf("diagnostics=%+v", state.Diagnostics)
	}
}

func TestSessionStateUpdateReturnsLastCompleteTransactionPrefixReadOnly(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session"}
	openedHead := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 2, TransactionID: "txn-open"}
	prefix := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 4, TransactionID: "txn-valid"}
	head := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 7, TransactionID: "txn-unsupported"}
	reader := &sessionStateReader{
		inspection: journal.Inspection{Journal: ref, Head: openedHead, Writable: true, Events: []protocol.EventRecord{
			v2SessionEvent(1, "txn-open", protocol.EventModeChanged, &protocol.ModeChangedV1{Mode: "safe"}),
		}},
		pages: []journal.EventPage{{Events: []protocol.EventRecord{
			v2SessionEvent(3, "txn-valid", protocol.EventModeChanged, &protocol.ModeChangedV1{Mode: "auto"}),
			v2SessionEvent(5, "txn-unsupported", protocol.EventModelChanged, &protocol.ModelChangedV1{ProviderID: "primary", ModelID: "model-b"}),
			v2SessionEvent(6, "txn-unsupported", "future.session_policy", &struct{}{}),
		}, Cursor: head, Head: head}},
	}
	projector := app.NewIncrementalSessionStateProjector(reader)
	state, err := projector.Open(t.Context(), ref, app.SessionState{
		Mode: domain.ModeAsk, Selection: domain.ModelSelection{Profile: "primary", Model: "model-a"},
	})
	if err != nil {
		t.Fatal(err)
	}

	updated, err := projector.ProjectSessionState(t.Context(), state)
	if !errors.Is(err, app.ErrSessionStateReadOnly) {
		t.Fatalf("error=%v", err)
	}
	if updated.Head != prefix || updated.Writable || updated.Mode != domain.ModeAuto || updated.Selection.Model != "model-a" {
		t.Fatalf("validated prefix=%+v", updated)
	}
	if len(updated.Diagnostics) != 1 || updated.Diagnostics[0].Code != "session_state.projection_failed" {
		t.Fatalf("diagnostics=%+v", updated.Diagnostics)
	}
}

func TestSessionStateRejectsUnknownStatefulIncrementWithoutAdvancingCursor(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session"}
	head := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 2, TransactionID: "txn-1"}
	next := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 4, TransactionID: "txn-2"}
	reader := &sessionStateReader{
		inspection: journal.Inspection{Journal: ref, Head: head, Writable: true},
		pages:      []journal.EventPage{{Events: []protocol.EventRecord{v2SessionEvent(3, "txn-2", "future.session_policy", &struct{}{})}, Cursor: next, Head: next}},
	}
	projector := app.NewIncrementalSessionStateProjector(reader)
	state, err := projector.Open(t.Context(), ref, app.SessionState{Mode: domain.ModeAsk})
	if err != nil {
		t.Fatal(err)
	}
	updated, err := projector.ProjectSessionState(t.Context(), state)
	if err == nil || updated.Head != head {
		t.Fatalf("updated=%+v err=%v", updated, err)
	}
}

func TestSessionStateRejectsFinalRangeThatDoesNotReachReportedHead(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session"}
	head := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 2, TransactionID: "txn-1"}
	next := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 4, TransactionID: "txn-2"}
	reported := protocol.CommittedCursor{JournalKind: ref.Kind, JournalID: ref.ID, CommitSeq: 6, TransactionID: "txn-3"}
	reader := &sessionStateReader{
		inspection: journal.Inspection{Journal: ref, Head: head, Writable: true},
		pages: []journal.EventPage{{Events: []protocol.EventRecord{
			v2SessionEvent(3, "txn-2", protocol.EventModelChanged, &protocol.ModelChangedV1{ProviderID: "primary", ModelID: "model-b"}),
		}, Cursor: next, Head: reported}},
	}
	projector := app.NewIncrementalSessionStateProjector(reader)
	state, err := projector.Open(t.Context(), ref, app.SessionState{Mode: domain.ModeAsk})
	if err != nil {
		t.Fatal(err)
	}
	updated, err := projector.ProjectSessionState(t.Context(), state)
	if err == nil || updated.Head != head {
		t.Fatalf("updated=%+v err=%v", updated, err)
	}
}

type sessionStateReader struct {
	inspection   journal.Inspection
	pages        []journal.EventPage
	inspectCalls int
	ranges       []journal.ReadRangeRequest
}

func (r *sessionStateReader) Inspect(context.Context, protocol.JournalRef) (journal.Inspection, error) {
	r.inspectCalls++
	return r.inspection, nil
}

func (r *sessionStateReader) ReadRange(_ context.Context, request journal.ReadRangeRequest) (journal.EventPage, error) {
	r.ranges = append(r.ranges, request)
	if len(r.pages) == 0 {
		return journal.EventPage{Cursor: request.After, Head: request.After}, nil
	}
	page := r.pages[0]
	r.pages = r.pages[1:]
	return page, nil
}

func v2SessionEvent(seq uint64, transactionID protocol.TransactionID, kind string, decoded any) protocol.EventRecord {
	raw, err := json.Marshal(decoded)
	if err != nil {
		panic(err)
	}
	return protocol.EventRecord{Envelope: protocol.EventEnvelope{
		JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session",
		EventID: protocol.EventID("event-" + kind), Seq: seq, TransactionID: transactionID,
		Time: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC), Kind: kind, PayloadVersion: 1, Payload: raw,
	}, Decoded: decoded}
}

func TestProjectSessionStateReplaysChangesInSequenceOrder(t *testing.T) {
	replay := domain.SessionReplay{
		Session: domain.Session{
			Mode:      domain.ModeAsk,
			Selection: domain.ModelSelection{Profile: "primary", Model: "model-a"},
		},
		Events: []domain.DurableEvent{
			durableEvent(3, domain.EventModeChanged, domain.ModeChangedPayload{Mode: domain.ModeAuto}),
			durableEvent(1, domain.EventModelChanged, domain.ModelChangedPayload{Selection: domain.ModelSelection{Profile: "primary", Model: "model-b"}}),
			durableEvent(4, domain.EventModelChanged, map[string]string{"selection": "invalid"}),
			durableEvent(2, domain.EventModeChanged, domain.ModeChangedPayload{Mode: domain.ModeSafe}),
		},
	}

	state := app.ProjectSessionState(replay)
	if state.Mode != domain.ModeAuto || state.Selection != (domain.ModelSelection{Profile: "primary", Model: "model-b"}) {
		t.Fatalf("state=%+v", state)
	}
}

func TestAppAppliesModeAndModelDurabilityFirst(t *testing.T) {
	store := newStateStore(testSession())
	policy := &recordingPolicy{}
	inputs := make(chan agent.RunInput, 1)
	application := app.New(app.Options{
		RuntimeSet: stateRuntimeSet(&fakeRuntime{run: func(_ context.Context, input agent.RunInput) error {
			inputs <- input
			return nil
		}}, nil,
			domain.ModelSelection{Profile: "primary", Model: "model-a"},
			domain.ModelSelection{Profile: "primary", Model: "model-b"}),
		Sessions: store,
		Session:  store.current.Session,
		Replay:   store.current,
		Policy:   policy,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandChangeMode, Mode: domain.ModeAuto}
	modeEvent := receiveEvent(t, application.Events())
	if modeEvent.Kind != app.EventState || modeEvent.Mode != domain.ModeAuto {
		t.Fatalf("mode event=%+v", modeEvent)
	}
	if modeEvent.Session.ID != "" || len(modeEvent.Replay.Events) != 0 {
		t.Fatalf("mode event unexpectedly requested a session replacement: %+v", modeEvent)
	}
	application.Commands() <- app.Command{Kind: app.CommandChangeModel, Selection: domain.ModelSelection{Profile: "primary", Model: "model-b"}}
	modelEvent := receiveEvent(t, application.Events())
	if modelEvent.Kind != app.EventState || modelEvent.Selection.Model != "model-b" {
		t.Fatalf("model event=%+v", modelEvent)
	}

	if got := store.appendKinds(); !reflect.DeepEqual(got, []domain.EventKind{domain.EventModeChanged, domain.EventModelChanged}) {
		t.Fatalf("append kinds=%v", got)
	}
	if got := policy.operations(); !reflect.DeepEqual(got, []string{"mode:auto"}) {
		t.Fatalf("policy operations=%v", got)
	}

	store.setAppendError(domain.EventModeChanged, errors.New("disk full"))
	application.Commands() <- app.Command{Kind: app.CommandChangeMode, Mode: domain.ModeSafe}
	failed := receiveEvent(t, application.Events())
	if failed.Kind != app.EventError || !failed.NonTerminal || !strings.Contains(failed.Message, "disk full") {
		t.Fatalf("failed event=%+v", failed)
	}
	if got := policy.operations(); !reflect.DeepEqual(got, []string{"mode:auto"}) {
		t.Fatalf("failed append changed policy: %v", got)
	}

	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "snapshot"}
	requireTurnAccepted(t, application.Events(), "snapshot")
	input := <-inputs
	if input.Session.Mode != domain.ModeAuto || input.Session.Selection.Model != "model-b" {
		t.Fatalf("turn input state=%+v", input.Session)
	}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventTurnCompleted {
		t.Fatalf("terminal=%+v", event)
	}
}

func TestAppQueuesLastModeAndModelDuringTurnAndAppliesModeFirst(t *testing.T) {
	store := newStateStore(testSession())
	started := make(chan struct{})
	release := make(chan struct{})
	application := app.New(app.Options{
		RuntimeSet: stateRuntimeSet(&fakeRuntime{run: func(context.Context, agent.RunInput) error {
			close(started)
			<-release
			return nil
		}}, nil,
			domain.ModelSelection{Profile: "primary", Model: "model-a"},
			domain.ModelSelection{Profile: "primary", Model: "model-b"},
			domain.ModelSelection{Profile: "other", Model: "model-c"}),
		Sessions: store,
		Session:  store.current.Session,
		Replay:   store.current,
		Policy:   &recordingPolicy{},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "one"}
	<-started
	requireTurnAccepted(t, application.Events(), "one")
	application.Commands() <- app.Command{Kind: app.CommandChangeMode, Mode: domain.ModeSafe}
	application.Commands() <- app.Command{Kind: app.CommandChangeMode, Mode: domain.ModeAuto}
	application.Commands() <- app.Command{Kind: app.CommandChangeModel, Selection: domain.ModelSelection{Profile: "primary", Model: "model-b"}}
	application.Commands() <- app.Command{Kind: app.CommandChangeModel, Selection: domain.ModelSelection{Profile: "other", Model: "model-c"}}
	if got := store.appendKinds(); len(got) != 0 {
		t.Fatalf("settings appended during turn: %v", got)
	}

	close(release)
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventTurnCompleted {
		t.Fatalf("terminal=%+v", event)
	}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventState || event.Mode != domain.ModeAuto {
		t.Fatalf("queued mode event=%+v", event)
	}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventState || event.Selection != (domain.ModelSelection{Profile: "other", Model: "model-c"}) {
		t.Fatalf("queued model event=%+v", event)
	}
	if got := store.appendKinds(); !reflect.DeepEqual(got, []domain.EventKind{domain.EventModeChanged, domain.EventModelChanged}) {
		t.Fatalf("append order=%v", got)
	}
}

func TestAppRejectsUnconfiguredModelBeforeAppend(t *testing.T) {
	store := newStateStore(testSession())
	application := app.New(app.Options{
		RuntimeSet: stateRuntimeSet(nil, nil, domain.ModelSelection{Profile: "primary", Model: "model-a"}),
		Sessions:   store,
		Session:    store.current.Session,
		Replay:     store.current,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandChangeModel, Selection: domain.ModelSelection{Profile: "missing", Model: "model-z"}}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventRejected {
		t.Fatalf("event=%+v", event)
	}
	if got := store.appendKinds(); len(got) != 0 {
		t.Fatalf("invalid model appended: %v", got)
	}
}

func TestAppAcknowledgesAutoShellOnlyAfterDurableAppend(t *testing.T) {
	sequence := &operationSequence{}
	store := newStateStore(testSession())
	store.sequence = sequence
	store.current.Session.Mode = domain.ModeAuto
	policy := &recordingPolicy{sequence: sequence}
	application := app.New(app.Options{Sessions: store, Session: store.current.Session, Replay: store.current, Policy: policy})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandAcknowledgeAutoShell}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventState {
		t.Fatalf("ack event=%+v", event)
	}
	if got := sequence.values(); !reflect.DeepEqual(got, []string{"append:trusted_execution.acknowledged", "ack"}) {
		t.Fatalf("operation order=%v", got)
	}

	application.Commands() <- app.Command{Kind: app.CommandChangeMode, Mode: domain.ModeAsk}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventState {
		t.Fatalf("mode event=%+v", event)
	}
	application.Commands() <- app.Command{Kind: app.CommandAcknowledgeAutoShell}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventRejected {
		t.Fatalf("non-auto ack event=%+v", event)
	}
	if got := policy.operations(); !reflect.DeepEqual(got, []string{"ack", "mode:ask"}) {
		t.Fatalf("policy operations=%v", got)
	}
}

func TestAppAutoShellApprovalStaysPendingWhenAcknowledgementAppendFails(t *testing.T) {
	store := newStateStore(testSession())
	store.current.Session.Mode = domain.ModeAuto
	store.setAppendError(domain.EventTrustedExecutionAcknowledged, errors.New("disk full"))
	policy := &recordingPolicy{}
	application := app.New(app.Options{Sessions: store, Session: store.current.Session, Replay: store.current, Policy: policy, EventBuffer: 2})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = application.Run(ctx) }()

	prompt := ports.PermissionPrompt{Call: domain.PreparedToolRequest{Request: domain.ToolRequest{CallID: "shell-1", Name: "shell"}, CanonicalScope: "/workspace\x00echo secret"}}
	resolved := make(chan permissionResult, 1)
	go func() {
		decision, err := application.Resolve(ctx, prompt)
		resolved <- permissionResult{decision: decision, err: err}
	}()
	permissionEvent := receiveEvent(t, application.Events())
	if permissionEvent.Kind != app.EventPermissionRequested || permissionEvent.Permission == nil {
		t.Fatalf("permission event=%+v", permissionEvent)
	}
	decision := domain.PermissionDecision{Action: domain.PermissionAllow, Lifetime: domain.PermissionOnce, Scope: prompt.Call.CanonicalScope}
	application.Commands() <- app.Command{Kind: app.CommandAcknowledgeAutoShell, CallID: permissionEvent.Permission.Call.Request.CallID, Decision: decision}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventError || !strings.Contains(event.Message, "disk full") {
		t.Fatalf("ack event=%+v", event)
	}
	select {
	case result := <-resolved:
		t.Fatalf("failed acknowledgement resolved shell approval: %+v", result)
	case <-time.After(20 * time.Millisecond):
	}
	if got := policy.operations(); len(got) != 0 {
		t.Fatalf("failed acknowledgement changed policy: %v", got)
	}
	cancel()
	if result := <-resolved; !errors.Is(result.err, context.Canceled) {
		t.Fatalf("pending approval error=%v", result.err)
	}
}

func TestAppOpensSessionOnlyAfterLoadAndWorkspaceVerification(t *testing.T) {
	current := testSession()
	store := newStateStore(current)
	other := current
	other.Session.ID = "other"
	other.Session.Workspace.CanonicalPath = "/different"
	store.loads["other"] = other
	resumed := current
	resumed.Session.ID = "resumed"
	resumed.Events = append(resumed.Events,
		durableEvent(2, domain.EventModeChanged, domain.ModeChangedPayload{Mode: domain.ModeSafe}),
		durableEvent(3, domain.EventModelChanged, domain.ModelChangedPayload{Selection: domain.ModelSelection{Profile: "primary", Model: "model-b"}}),
	)
	store.loads["resumed"] = resumed
	inputs := make(chan agent.RunInput, 1)
	application := app.New(app.Options{
		RuntimeSet: stateRuntimeSet(&fakeRuntime{run: func(_ context.Context, input agent.RunInput) error {
			inputs <- input
			return nil
		}}, nil,
			domain.ModelSelection{Profile: "primary", Model: "model-a"},
			domain.ModelSelection{Profile: "primary", Model: "model-b"}),
		Sessions: store,
		Session:  current.Session,
		Replay:   current,
		Policy:   &recordingPolicy{},
		RestorePolicy: func(domain.SessionReplay) app.MutablePolicy {
			return &recordingPolicy{}
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandOpenSession, SessionID: "other"}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventRejected {
		t.Fatalf("wrong workspace event=%+v", event)
	}
	application.Commands() <- app.Command{Kind: app.CommandOpenSession, SessionID: "resumed"}
	opened := receiveEvent(t, application.Events())
	if opened.Kind != app.EventState || opened.Session.ID != "resumed" || opened.Mode != domain.ModeSafe || opened.Selection.Model != "model-b" {
		t.Fatalf("opened event=%+v", opened)
	}

	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "resumed turn"}
	requireTurnAccepted(t, application.Events(), "resumed turn")
	if input := <-inputs; input.Session.ID != "resumed" || input.Session.Mode != domain.ModeSafe || input.Session.Selection.Model != "model-b" {
		t.Fatalf("resumed input=%+v", input)
	}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventTurnCompleted {
		t.Fatalf("terminal=%+v", event)
	}
}

func TestAppOpenSessionPersistsFallbackForRemovedModel(t *testing.T) {
	current := testSession()
	store := newStateStore(current)
	stale := current
	stale.Session.ID = "stale"
	stale.Session.Selection = domain.ModelSelection{Profile: "removed", Model: "gone"}
	store.loads[stale.Session.ID] = stale
	selection := domain.ModelSelection{Profile: "primary", Model: "model-a"}
	inputs := make(chan agent.RunInput, 1)
	application := app.New(app.Options{
		RuntimeSet: readyRuntimeSet(selection, &fakeRuntime{run: func(_ context.Context, input agent.RunInput) error {
			inputs <- input
			return nil
		}}, "secret"),
		Sessions:  store,
		Session:   current.Session,
		Replay:    current,
		Workspace: current.Session.Workspace,
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandOpenSession, SessionID: stale.Session.ID}
	opened := receiveEvent(t, application.Events())
	if opened.Kind != app.EventState || opened.Session.ID != stale.Session.ID || opened.Selection != selection {
		t.Fatalf("opened=%+v", opened)
	}
	persisted, err := store.Load(t.Context(), stale.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state := app.ProjectSessionState(persisted); state.Selection != selection {
		t.Fatalf("persisted selection=%+v", state.Selection)
	}

	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "resumed"}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventTurnAccepted {
		t.Fatalf("accepted=%+v", event)
	}
	if input := <-inputs; input.Session.ID != stale.Session.ID || input.Session.Selection != selection {
		t.Fatalf("input session=%+v", input.Session)
	}
	_ = receiveEvent(t, application.Events())
}

func TestAppOpenSessionPreservesCurrentWhenFallbackPersistenceFails(t *testing.T) {
	current := testSession()
	store := newStateStore(current)
	stale := current
	stale.Session.ID = "stale"
	stale.Session.Selection = domain.ModelSelection{Profile: "removed", Model: "gone"}
	store.loads[stale.Session.ID] = stale
	store.setAppendError(domain.EventModelChanged, errors.New("disk full"))
	selection := domain.ModelSelection{Profile: "primary", Model: "model-a"}
	inputs := make(chan agent.RunInput, 1)
	restoreCalls := 0
	application := app.New(app.Options{
		RuntimeSet: readyRuntimeSet(selection, &fakeRuntime{run: func(_ context.Context, input agent.RunInput) error {
			inputs <- input
			return nil
		}}, "secret"),
		Sessions:  store,
		Session:   current.Session,
		Replay:    current,
		Workspace: current.Session.Workspace,
		Policy:    &recordingPolicy{},
		RestorePolicy: func(domain.SessionReplay) app.MutablePolicy {
			restoreCalls++
			return &recordingPolicy{}
		},
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandOpenSession, SessionID: stale.Session.ID}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventError || !event.NonTerminal || !strings.Contains(event.Message, "disk full") {
		t.Fatalf("open failure=%+v", event)
	}
	if restoreCalls != 0 {
		t.Fatalf("policy restored before fallback persisted: calls=%d", restoreCalls)
	}
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "still current"}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventTurnAccepted {
		t.Fatalf("accepted=%+v", event)
	}
	if input := <-inputs; input.Session.ID != current.Session.ID || input.Session.Selection != current.Session.Selection {
		t.Fatalf("current session changed=%+v", input.Session)
	}
	_ = receiveEvent(t, application.Events())
}

func TestAppNewSessionUsesDefaultWhenCurrentModelWasRemoved(t *testing.T) {
	current := testSession()
	current.Session.Selection = domain.ModelSelection{Profile: "removed", Model: "gone"}
	store := newStateStore(current)
	selection := domain.ModelSelection{Profile: "primary", Model: "model-a"}
	application := app.New(app.Options{
		RuntimeSet: readyRuntimeSet(selection, &fakeRuntime{run: func(context.Context, agent.RunInput) error { return nil }}, "secret"),
		Sessions:   store,
		Session:    current.Session,
		Replay:     current,
		Workspace:  current.Session.Workspace,
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandNewSession}
	created := receiveEvent(t, application.Events())
	if created.Kind != app.EventState || created.Session.ID != "created" || created.Selection != selection {
		t.Fatalf("created=%+v", created)
	}
}

func TestAppDoesNotReplaceSessionWithoutPolicyRestoration(t *testing.T) {
	current := testSession()
	store := newStateStore(current)
	other := current
	other.Session.ID = "other"
	store.loads["other"] = other
	inputs := make(chan agent.RunInput, 1)
	application := app.New(app.Options{
		RuntimeSet: stateRuntimeSet(&fakeRuntime{run: func(_ context.Context, input agent.RunInput) error {
			inputs <- input
			return nil
		}}, nil, current.Session.Selection),
		Sessions: store,
		Session:  current.Session,
		Replay:   current,
		Policy:   &recordingPolicy{},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandOpenSession, SessionID: "other"}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventError || !strings.Contains(event.Message, "policy restoration") {
		t.Fatalf("open event=%+v", event)
	}
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "still current"}
	requireTurnAccepted(t, application.Events(), "still current")
	if input := <-inputs; input.Session.ID != current.Session.ID {
		t.Fatalf("session replaced after restore failure: %+v", input.Session)
	}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventTurnCompleted {
		t.Fatalf("terminal=%+v", event)
	}
}

func TestAppRejectsSessionChangesDuringActiveOperations(t *testing.T) {
	current := testSession()
	store := newStateStore(current)
	turnStarted := make(chan struct{})
	turnRelease := make(chan struct{})
	compactStarted := make(chan struct{})
	compactRelease := make(chan struct{})
	application := app.New(app.Options{
		RuntimeSet: stateRuntimeSet(&fakeRuntime{run: func(context.Context, agent.RunInput) error {
			close(turnStarted)
			<-turnRelease
			return nil
		}}, nil, current.Session.Selection),
		Compact: func(context.Context) error {
			close(compactStarted)
			<-compactRelease
			return nil
		},
		Sessions:  store,
		Session:   current.Session,
		Replay:    current,
		Workspace: current.Session.Workspace,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "one"}
	<-turnStarted
	requireTurnAccepted(t, application.Events(), "one")
	application.Commands() <- app.Command{Kind: app.CommandNewSession}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventRejected {
		t.Fatalf("new during turn=%+v", event)
	}
	close(turnRelease)
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventTurnCompleted {
		t.Fatalf("turn terminal=%+v", event)
	}

	application.Commands() <- app.Command{Kind: app.CommandCompact}
	<-compactStarted
	application.Commands() <- app.Command{Kind: app.CommandOpenSession, SessionID: "current"}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventRejected {
		t.Fatalf("open during compact=%+v", event)
	}
	close(compactRelease)
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventTurnCompleted {
		t.Fatalf("compact terminal=%+v", event)
	}
}

func TestAppRefreshesReplayOnlyAfterSuccessfulCompaction(t *testing.T) {
	current := testSession()
	store := newStateStore(current)
	compact := func(context.Context) error {
		store.mu.Lock()
		store.current.Events = append(store.current.Events, durableEvent(2, domain.EventContextCompacted, domain.CompactionPayload{FromSeq: 1, ThroughSeq: 1, Summary: "summary"}))
		store.loads[current.Session.ID] = store.current
		store.mu.Unlock()
		return nil
	}
	inputs := make(chan agent.RunInput, 1)
	application := app.New(app.Options{
		RuntimeSet: stateRuntimeSet(&fakeRuntime{run: func(_ context.Context, input agent.RunInput) error {
			inputs <- input
			return nil
		}}, nil, current.Session.Selection),
		Compact:  compact,
		Sessions: store,
		Session:  current.Session,
		Replay:   current,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandCompact}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventTurnCompleted || len(event.Replay.Events) != 2 {
		t.Fatalf("compact event=%+v", event)
	}
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "after compact"}
	requireTurnAccepted(t, application.Events(), "after compact")
	if input := <-inputs; len(input.Replay.Events) != 2 || input.Replay.Events[1].Kind != domain.EventContextCompacted {
		t.Fatalf("refreshed input replay=%+v", input.Replay)
	}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventTurnCompleted {
		t.Fatalf("turn terminal=%+v", event)
	}
}

func TestAppRefreshesReplayAfterCompletedTurn(t *testing.T) {
	current := testSession()
	store := newStateStore(current)
	runtime := &fakeRuntime{run: func(context.Context, agent.RunInput) error {
		_, err := store.Append(context.Background(), current.Session.ID, domain.EventTurnCompleted, domain.TurnTerminalPayload{Reason: "complete"})
		return err
	}}
	application := app.New(app.Options{RuntimeSet: stateRuntimeSet(runtime, nil, current.Session.Selection), Sessions: store, Session: current.Session, Replay: current})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "turn"}
	requireTurnAccepted(t, application.Events(), "turn")
	event := receiveEvent(t, application.Events())
	if event.Kind != app.EventTurnCompleted || len(event.Replay.Events) != len(current.Events)+1 {
		t.Fatalf("terminal event=%+v", event)
	}
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "next"}
	requireTurnAccepted(t, application.Events(), "next")
	// The fake runtime appends again; the input snapshot is covered by the replay on the terminal event.
	_ = receiveEvent(t, application.Events())
}

func TestAppBlocksTurnsAfterReplayRefreshFailure(t *testing.T) {
	current := testSession()
	store := newStateStore(current)
	var runs int
	application := app.New(app.Options{
		RuntimeSet: stateRuntimeSet(&fakeRuntime{run: func(context.Context, agent.RunInput) error {
			runs++
			store.mu.Lock()
			store.loadErr = errors.New("reload failed")
			store.mu.Unlock()
			return nil
		}}, nil, current.Session.Selection),
		Sessions: store,
		Session:  current.Session,
		Replay:   current,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = application.Run(ctx) }()

	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "first"}
	requireTurnAccepted(t, application.Events(), "first")
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventError || !strings.Contains(event.Message, "reload failed") {
		t.Fatalf("refresh event=%+v", event)
	}
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, DraftID: 13, Prompt: "must not use stale replay"}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventRejected || event.DraftID != 13 || event.Draft != "must not use stale replay" || !strings.Contains(event.Message, "context is unavailable") {
		t.Fatalf("blocked event=%+v", event)
	}
	if runs != 1 {
		t.Fatalf("runtime calls=%d want=1", runs)
	}
}

func TestAppPassesCurrentProjectedStateToCompaction(t *testing.T) {
	current := testSession()
	store := newStateStore(current)
	compacted := make(chan agent.RunInput, 1)
	compactErr := errors.New("stop after snapshot")
	compactSession := func(_ context.Context, session domain.Session, replay domain.SessionReplay) error {
		compacted <- agent.RunInput{Session: session, Replay: replay}
		return compactErr
	}
	application := app.New(app.Options{
		RuntimeSet: stateRuntimeSet(nil, compactSession,
			domain.ModelSelection{Profile: "primary", Model: "model-a"},
			domain.ModelSelection{Profile: "primary", Model: "model-b"}),
		Sessions: store,
		Session:  current.Session,
		Replay:   current,
		Policy:   &recordingPolicy{},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandChangeMode, Mode: domain.ModeAuto}
	_ = receiveEvent(t, application.Events())
	application.Commands() <- app.Command{Kind: app.CommandChangeModel, Selection: domain.ModelSelection{Profile: "primary", Model: "model-b"}}
	_ = receiveEvent(t, application.Events())
	application.Commands() <- app.Command{Kind: app.CommandCompact}

	input := <-compacted
	if input.Session.Mode != domain.ModeAuto || input.Session.Selection.Model != "model-b" || len(input.Replay.Events) != 3 {
		t.Fatalf("compaction input=%+v", input)
	}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventError || !errors.Is(event.Err, compactErr) {
		t.Fatalf("compact terminal=%+v", event)
	}
}

func stateRuntimeSet(runtime app.Runtime, compact app.CompactSession, models ...domain.ModelSelection) app.RuntimeSet {
	credentials := make(map[string]string, len(models))
	credentialEnvs := make(map[string]string, len(models))
	for _, selection := range models {
		credentials[selection.Profile] = "configured"
		credentialEnvs[selection.Profile] = "TEST_KEY"
	}
	var defaultSelection domain.ModelSelection
	if len(models) > 0 {
		defaultSelection = models[0]
	}
	return app.RuntimeSet{
		Runtime:          runtime,
		CompactSession:   compact,
		Models:           append([]domain.ModelSelection(nil), models...),
		DefaultSelection: defaultSelection,
		CredentialEnvs:   credentialEnvs,
		Credentials:      credentials,
	}
}

type stateStore struct {
	mu         sync.Mutex
	current    domain.SessionReplay
	loads      map[string]domain.SessionReplay
	appends    []domain.EventKind
	appendErrs map[domain.EventKind]error
	loadErr    error
	sequence   *operationSequence
}

func newStateStore(replay domain.SessionReplay) *stateStore {
	return &stateStore{
		current:    replay,
		loads:      map[string]domain.SessionReplay{replay.Session.ID: replay},
		appendErrs: make(map[domain.EventKind]error),
	}
}

func (s *stateStore) Create(_ context.Context, workspace domain.Workspace, mode domain.PermissionMode, selection domain.ModelSelection) (domain.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session := domain.Session{ID: "created", Workspace: workspace, Title: "New session", Mode: mode, Selection: selection}
	replay := domain.SessionReplay{Session: session, Events: []domain.DurableEvent{durableEvent(1, domain.EventSessionCreated, session)}}
	s.loads[session.ID] = replay
	return session, nil
}

func (s *stateStore) Append(_ context.Context, sessionID string, kind domain.EventKind, payload any) (domain.DurableEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.appendErrs[kind]; err != nil {
		return domain.DurableEvent{}, err
	}
	s.appends = append(s.appends, kind)
	if s.sequence != nil {
		s.sequence.add("append:" + string(kind))
	}
	replay, ok := s.loads[sessionID]
	if !ok {
		return domain.DurableEvent{}, errors.New("session not found")
	}
	event := durableEvent(uint64(len(replay.Events)+1), kind, payload)
	event.SessionID = sessionID
	replay.Events = append(replay.Events, event)
	replay.Session.LastSeq = event.Seq
	replay.Session.UpdatedAt = event.Time
	s.loads[sessionID] = replay
	if s.current.Session.ID == sessionID {
		s.current = replay
	}
	return event, nil
}

func (s *stateStore) Load(_ context.Context, sessionID string) (domain.SessionReplay, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return domain.SessionReplay{}, s.loadErr
	}
	replay, ok := s.loads[sessionID]
	if !ok {
		return domain.SessionReplay{}, errors.New("session not found")
	}
	return replay, nil
}

func (s *stateStore) List(context.Context, domain.Workspace) ([]domain.SessionSummary, error) {
	return nil, nil
}

func (s *stateStore) appendKinds() []domain.EventKind {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.EventKind(nil), s.appends...)
}

func (s *stateStore) setAppendError(kind domain.EventKind, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendErrs[kind] = err
}

type recordingPolicy struct {
	mu       sync.Mutex
	ops      []string
	sequence *operationSequence
}

func (p *recordingPolicy) SetMode(mode domain.PermissionMode) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ops = append(p.ops, "mode:"+string(mode))
	if p.sequence != nil {
		p.sequence.add("mode:" + string(mode))
	}
}

func (p *recordingPolicy) AcknowledgeAutoShell() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ops = append(p.ops, "ack")
	if p.sequence != nil {
		p.sequence.add("ack")
	}
}

func (p *recordingPolicy) operations() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.ops...)
}

type operationSequence struct {
	mu  sync.Mutex
	ops []string
}

func (s *operationSequence) add(value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops = append(s.ops, value)
}

func (s *operationSequence) values() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ops...)
}

func testSession() domain.SessionReplay {
	session := domain.Session{
		ID:        "current",
		Workspace: domain.Workspace{ID: "workspace", CanonicalPath: "/workspace"},
		Title:     "Current",
		Mode:      domain.ModeAsk,
		Selection: domain.ModelSelection{Profile: "primary", Model: "model-a"},
		LastSeq:   1,
	}
	return domain.SessionReplay{Session: session, Events: []domain.DurableEvent{durableEvent(1, domain.EventSessionCreated, session)}}
}

func durableEvent(sequence uint64, kind domain.EventKind, payload any) domain.DurableEvent {
	raw, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return domain.DurableEvent{Seq: sequence, Kind: kind, Payload: raw}
}
