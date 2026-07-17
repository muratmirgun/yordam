package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
)

func TestRunnerPersistsToolLoopInOrder(t *testing.T) {
	provider := &fakeProvider{streams: [][]domain.ModelEvent{
		{{Kind: domain.ModelToolCall, ToolCall: &domain.ToolCall{ID: "c1", Name: "read", Arguments: json.RawMessage(`{"path":"a.go"}`)}}, {Kind: domain.ModelDone}},
		{{Kind: domain.ModelTextDelta, Text: "done"}, {Kind: domain.ModelDone}},
	}}
	sessions := newFakeSessionStore()
	runner := agent.Runner{
		Provider:     provider,
		Tools:        fakeRegistry{tool: fakeTool{result: domain.ToolResult{CallID: "c1", Status: domain.ToolSucceeded, Content: "package a"}}},
		Policy:       allowPolicy{},
		Approver:     denyIfCalled{},
		Sessions:     sessions,
		MaxToolCalls: 32,
		SystemPrompt: "system",
	}
	err := runner.RunTurn(context.Background(), agent.RunInput{
		Session: domain.Session{
			ID:        "s1",
			Mode:      domain.ModeAsk,
			Workspace: domain.Workspace{CanonicalPath: "/tmp/app"},
			Selection: domain.ModelSelection{Profile: "p", Model: "m"},
		},
		Prompt: "inspect",
	})
	if err != nil {
		t.Fatal(err)
	}
	got := sessions.kinds()
	want := []domain.EventKind{
		domain.EventUserMessage,
		domain.EventAssistantMessage,
		domain.EventToolRequested,
		domain.EventPermissionResolved,
		domain.EventToolStarted,
		domain.EventToolResult,
		domain.EventAssistantMessage,
		domain.EventTurnCompleted,
	}
	if !slices.Equal(got, want) {
		t.Fatalf("kinds=%v want=%v", got, want)
	}
}

func TestRunnerRedactsPromptBeforePersistenceAndModelContext(t *testing.T) {
	const sentinel = "top-secret"
	provider := &fakeProvider{streams: [][]domain.ModelEvent{{{Kind: domain.ModelDone}}}}
	sessions := newFakeSessionStore()
	runner := newTestRunner(provider, fakeRegistry{}, allowPolicy{}, denyIfCalled{}, sessions)
	runner.Redact = func(value string) string {
		return strings.ReplaceAll(value, sentinel, "[REDACTED]")
	}

	if err := runner.RunTurn(context.Background(), agent.RunInput{
		Session: testRunInput().Session,
		Prompt:  "inspect " + sentinel,
	}); err != nil {
		t.Fatal(err)
	}

	var persisted domain.MessagePayload
	if err := json.Unmarshal(eventOfKind(t, sessions.recordedEvents(), domain.EventUserMessage).Payload, &persisted); err != nil {
		t.Fatal(err)
	}
	requests := provider.recordedRequests()
	modelPrompt := requests[0].Messages[len(requests[0].Messages)-1].Content
	if persisted.Content != "inspect [REDACTED]" || modelPrompt != persisted.Content {
		t.Fatalf("persisted=%q model=%q", persisted.Content, modelPrompt)
	}
}

func TestRunnerExecutesToolCallsSequentiallyInModelOrder(t *testing.T) {
	provider := &fakeProvider{streams: [][]domain.ModelEvent{
		{
			{Kind: domain.ModelToolCall, ToolCall: toolCall("c2", "read")},
			{Kind: domain.ModelToolCall, ToolCall: toolCall("c1", "read")},
			{Kind: domain.ModelDone},
		},
		{{Kind: domain.ModelTextDelta, Text: "done"}, {Kind: domain.ModelDone}},
	}}
	tool := &recordingTool{}
	sessions := newFakeSessionStore()
	runner := newTestRunner(provider, recordingRegistry{tool: tool}, allowPolicy{}, denyIfCalled{}, sessions)
	runner.MaxToolCalls = 2

	if err := runner.RunTurn(context.Background(), testRunInput()); err != nil {
		t.Fatal(err)
	}
	requests := tool.recordedRequests()
	if len(requests) != 2 || requests[0].CallID != "c2" || requests[1].CallID != "c1" {
		t.Fatalf("tool request order=%v want=[c2 c1]", requestCallIDs(requests))
	}
	wantKinds := []domain.EventKind{
		domain.EventUserMessage,
		domain.EventAssistantMessage,
		domain.EventToolRequested,
		domain.EventPermissionResolved,
		domain.EventToolStarted,
		domain.EventToolResult,
		domain.EventToolRequested,
		domain.EventPermissionResolved,
		domain.EventToolStarted,
		domain.EventToolResult,
		domain.EventAssistantMessage,
		domain.EventTurnCompleted,
	}
	if got := sessions.kinds(); !slices.Equal(got, wantKinds) {
		t.Fatalf("kinds=%v want=%v", got, wantKinds)
	}
	modelRequests := provider.recordedRequests()
	messages := modelRequests[1].Messages
	if got := []string{messages[len(messages)-2].ToolCallID, messages[len(messages)-1].ToolCallID}; !slices.Equal(got, []string{"c2", "c1"}) {
		t.Fatalf("tool message order=%v want=[c2 c1]", got)
	}
}

func TestRunnerPermissionDecisions(t *testing.T) {
	tests := []struct {
		name          string
		policy        ports.PermissionPolicy
		approver      ports.PermissionApprover
		wantKinds     []domain.EventKind
		wantApprovals int
		wantTools     int
		wantStatus    domain.ToolStatus
	}{
		{
			name:          "ask_then_allow",
			policy:        askPolicy{},
			approver:      &recordingApprover{decision: domain.PermissionDecision{Action: domain.PermissionAllow, Lifetime: domain.PermissionOnce, Scope: "/tmp/app"}},
			wantKinds:     []domain.EventKind{domain.EventUserMessage, domain.EventAssistantMessage, domain.EventToolRequested, domain.EventPermissionRequested, domain.EventPermissionResolved, domain.EventToolStarted, domain.EventToolResult, domain.EventAssistantMessage, domain.EventTurnCompleted},
			wantApprovals: 1,
			wantTools:     1,
			wantStatus:    domain.ToolSucceeded,
		},
		{
			name:       "direct_deny",
			policy:     denyPolicy{},
			approver:   denyIfCalled{},
			wantKinds:  []domain.EventKind{domain.EventUserMessage, domain.EventAssistantMessage, domain.EventToolRequested, domain.EventPermissionResolved, domain.EventToolResult, domain.EventAssistantMessage, domain.EventTurnCompleted},
			wantTools:  0,
			wantStatus: domain.ToolDenied,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := &fakeProvider{streams: toolThenFinalStreams("read")}
			tool := &recordingTool{}
			sessions := newFakeSessionStore()
			runner := newTestRunner(provider, recordingRegistry{tool: tool}, test.policy, test.approver, sessions)

			if err := runner.RunTurn(context.Background(), testRunInput()); err != nil {
				t.Fatal(err)
			}
			if got := sessions.kinds(); !slices.Equal(got, test.wantKinds) {
				t.Fatalf("kinds=%v want=%v", got, test.wantKinds)
			}
			if got := tool.callCount(); got != test.wantTools {
				t.Fatalf("tool calls=%d want=%d", got, test.wantTools)
			}
			if approver, ok := test.approver.(*recordingApprover); ok && approver.callCount() != test.wantApprovals {
				t.Fatalf("approval calls=%d want=%d", approver.callCount(), test.wantApprovals)
			}
			assertProviderToolResult(t, provider, test.wantStatus, map[domain.ToolStatus]domain.ErrorKind{
				domain.ToolSucceeded: "",
				domain.ToolDenied:    domain.ErrorPermissionDenied,
			}[test.wantStatus])
		})
	}
}

func TestRunnerEmitsToolStartedOnlyWhenExecutionBegins(t *testing.T) {
	for _, test := range []struct {
		name      string
		policy    ports.PermissionPolicy
		wantStart bool
	}{
		{name: "allowed", policy: allowPolicy{}, wantStart: true},
		{name: "denied", policy: denyPolicy{}, wantStart: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := &fakeProvider{streams: toolThenFinalStreams("read")}
			runner := newTestRunner(provider, recordingRegistry{tool: &recordingTool{}}, test.policy, denyIfCalled{}, newFakeSessionStore())
			var started bool
			runner.Sink = func(event agent.RuntimeEvent) {
				started = started || event.Kind == agent.RuntimeToolStarted
			}

			if err := runner.RunTurn(context.Background(), testRunInput()); err != nil {
				t.Fatal(err)
			}
			if started != test.wantStart {
				t.Fatalf("tool started event=%t want=%t", started, test.wantStart)
			}
		})
	}
}

func TestRunnerGrantsApprovedSessionScopeAfterResolution(t *testing.T) {
	provider := &fakeProvider{streams: toolThenFinalStreams("read")}
	tool := &recordingTool{}
	policy := &recordingGrantPolicy{decision: domain.PermissionDecision{Action: domain.PermissionAsk}}
	sessions := newFakeSessionStore()
	sessions.onAppend = func(kind domain.EventKind) {
		if kind == domain.EventPermissionResolved && policy.grantCount() != 0 {
			t.Fatalf("session grants=%d when permission resolved", policy.grantCount())
		}
	}
	runner := newTestRunner(
		provider,
		recordingRegistry{tool: tool},
		policy,
		&recordingApprover{decision: domain.PermissionDecision{Action: domain.PermissionAllow, Lifetime: domain.PermissionSession, Scope: "/tmp/app"}},
		sessions,
	)

	if err := runner.RunTurn(context.Background(), testRunInput()); err != nil {
		t.Fatal(err)
	}
	if got := policy.recordedGrants(); !slices.Equal(got, []permissionGrant{{tool: "read", scope: "/tmp/app"}}) {
		t.Fatalf("grants=%v", got)
	}
	if got := permissionPayload(t, eventOfKind(t, sessions.recordedEvents(), domain.EventPermissionResolved)).Decision; got.Action != domain.PermissionAllow || got.Lifetime != domain.PermissionSession || got.Scope != "/tmp/app" {
		t.Fatalf("resolved decision=%#v", got)
	}
}

func TestRunnerDoesNotGrantOnceOrDeniedApprovals(t *testing.T) {
	tests := []struct {
		name     string
		decision domain.PermissionDecision
	}{
		{name: "allow once", decision: domain.PermissionDecision{Action: domain.PermissionAllow, Lifetime: domain.PermissionOnce, Scope: "/tmp/app"}},
		{name: "deny session", decision: domain.PermissionDecision{Action: domain.PermissionDeny, Lifetime: domain.PermissionSession, Scope: "/tmp/app"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy := &recordingGrantPolicy{decision: domain.PermissionDecision{Action: domain.PermissionAsk}}
			runner := newTestRunner(
				&fakeProvider{streams: toolThenFinalStreams("read")},
				recordingRegistry{tool: &recordingTool{}},
				policy,
				&recordingApprover{decision: test.decision},
				newFakeSessionStore(),
			)

			if err := runner.RunTurn(context.Background(), testRunInput()); err != nil {
				t.Fatal(err)
			}
			if got := policy.grantCount(); got != 0 {
				t.Fatalf("session grants=%d want=0", got)
			}
		})
	}
}

func TestRunnerRejectsInvalidApprovalResponses(t *testing.T) {
	tests := []struct {
		name     string
		decision domain.PermissionDecision
	}{
		{name: "broader scope", decision: domain.PermissionDecision{Action: domain.PermissionAllow, Lifetime: domain.PermissionSession, Scope: "/tmp"}},
		{name: "ask action", decision: domain.PermissionDecision{Action: domain.PermissionAsk, Lifetime: domain.PermissionOnce, Scope: "/tmp/app"}},
		{name: "unknown action", decision: domain.PermissionDecision{Action: "permit", Lifetime: domain.PermissionOnce, Scope: "/tmp/app"}},
		{name: "missing lifetime", decision: domain.PermissionDecision{Action: domain.PermissionAllow, Scope: "/tmp/app"}},
		{name: "unknown lifetime", decision: domain.PermissionDecision{Action: domain.PermissionAllow, Lifetime: "forever", Scope: "/tmp/app"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tool := &recordingTool{}
			policy := &recordingGrantPolicy{decision: domain.PermissionDecision{Action: domain.PermissionAsk}}
			sessions := newFakeSessionStore()
			runner := newTestRunner(
				&fakeProvider{streams: toolThenFinalStreams("read")},
				recordingRegistry{tool: tool},
				policy,
				&recordingApprover{decision: test.decision},
				sessions,
			)

			if err := runner.RunTurn(context.Background(), testRunInput()); err != nil {
				t.Fatal(err)
			}
			if got := tool.callCount(); got != 0 {
				t.Fatalf("tool calls=%d want=0", got)
			}
			if got := policy.grantCount(); got != 0 {
				t.Fatalf("session grants=%d want=0", got)
			}
			resolved := permissionPayload(t, eventOfKind(t, sessions.recordedEvents(), domain.EventPermissionResolved)).Decision
			if resolved.Action != domain.PermissionDeny || resolved.Lifetime != domain.PermissionOnce || resolved.Scope != "/tmp/app" || resolved.Reason != "invalid approval response" {
				t.Fatalf("resolved decision=%#v", resolved)
			}
		})
	}
}

func TestRunnerRequiresGranterForApprovedSessionScope(t *testing.T) {
	sessions := newFakeSessionStore()
	runner := newTestRunner(
		&fakeProvider{streams: toolThenFinalStreams("read")},
		recordingRegistry{tool: &recordingTool{}},
		askPolicy{},
		&recordingApprover{decision: domain.PermissionDecision{Action: domain.PermissionAllow, Lifetime: domain.PermissionSession, Scope: "/tmp/app"}},
		sessions,
	)

	err := runner.RunTurn(context.Background(), testRunInput())
	if err == nil || err.Error() != "permission policy does not support session grants" {
		t.Fatalf("error=%v", err)
	}
	if slices.Contains(sessions.kinds(), domain.EventPermissionResolved) {
		t.Fatalf("permission resolved before grant: %v", sessions.kinds())
	}
}

func TestRunnerPersistsProviderErrors(t *testing.T) {
	retryable := &domain.TypedError{Kind: domain.ErrorProviderRetryable, Message: "busy"}
	fatalStream := &domain.TypedError{Kind: domain.ErrorProviderFatal, Message: "invalid stream"}
	tests := []struct {
		name      string
		provider  ports.ModelProvider
		wantKind  domain.EventKind
		wantError domain.ErrorKind
	}{
		{
			name: "fatal_start_error",
			provider: providerFunc(func(context.Context, domain.ModelRequest) (<-chan domain.ModelEvent, error) {
				return nil, errors.New("bad response")
			}),
			wantKind:  domain.EventTurnFailed,
			wantError: domain.ErrorProviderFatal,
		},
		{
			name: "typed_start_error",
			provider: providerFunc(func(context.Context, domain.ModelRequest) (<-chan domain.ModelEvent, error) {
				return nil, retryable
			}),
			wantKind:  domain.EventTurnFailed,
			wantError: domain.ErrorProviderRetryable,
		},
		{
			name: "stream_error",
			provider: &fakeProvider{streams: [][]domain.ModelEvent{{
				{Kind: domain.ModelTextDelta, Text: "partial"},
				{Kind: domain.ModelStreamError, Err: errors.New("stream ended")},
			}}},
			wantKind:  domain.EventTurnInterrupted,
			wantError: domain.ErrorProviderInterrupted,
		},
		{
			name: "typed_stream_error",
			provider: &fakeProvider{streams: [][]domain.ModelEvent{{
				{Kind: domain.ModelStreamError, Err: fatalStream},
			}}},
			wantKind:  domain.EventTurnFailed,
			wantError: domain.ErrorProviderFatal,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tool := &recordingTool{}
			sessions := newFakeSessionStore()
			runner := newTestRunner(test.provider, recordingRegistry{tool: tool}, allowPolicy{}, denyIfCalled{}, sessions)

			if err := runner.RunTurn(context.Background(), testRunInput()); err == nil {
				t.Fatal("RunTurn returned nil error")
			}
			terminal := sessions.lastEvent()
			if terminal.Kind != test.wantKind {
				t.Fatalf("last event=%q want=%q", terminal.Kind, test.wantKind)
			}
			if got := terminalPayload(t, terminal).ErrorKind; got != test.wantError {
				t.Fatalf("error kind=%q want=%q", got, test.wantError)
			}
			if got := tool.callCount(); got != 0 {
				t.Fatalf("tool calls=%d want=0", got)
			}
		})
	}
}

func TestRunnerRejectsOverBudgetAssistantBatchBeforePersistence(t *testing.T) {
	provider := &fakeProvider{streams: [][]domain.ModelEvent{{
		{Kind: domain.ModelToolCall, ToolCall: toolCall("c1", "read")},
		{Kind: domain.ModelToolCall, ToolCall: toolCall("c2", "read")},
		{Kind: domain.ModelDone},
	}}}
	tool := &recordingTool{}
	sessions := newFakeSessionStore()
	runner := newTestRunner(provider, recordingRegistry{tool: tool}, allowPolicy{}, denyIfCalled{}, sessions)
	runner.MaxToolCalls = 1

	err := runner.RunTurn(context.Background(), testRunInput())
	if err == nil || err.Error() != "tool call limit 1 reached" {
		t.Fatalf("error=%v want tool limit", err)
	}
	if slices.Contains(sessions.kinds(), domain.EventAssistantMessage) {
		t.Fatalf("over-budget assistant persisted: %v", sessions.kinds())
	}
	if got := tool.callCount(); got != 0 {
		t.Fatalf("tool calls=%d want=0", got)
	}
	if got := sessions.lastKind(); got != domain.EventTurnFailed {
		t.Fatalf("last event=%q want=%q", got, domain.EventTurnFailed)
	}
}

func TestRunnerPersistsCancelledToolResultBeforeInterruption(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tool := &preparedFakeTool{
		preview: domain.PreparedToolRequest{CanonicalScope: "/tmp/app", InsideWorkspace: true, Summary: "read"},
		onExecuteContext: func(executeContext context.Context) domain.ToolResult {
			cancel()
			<-executeContext.Done()
			return domain.ToolResult{CallID: "c1", Status: domain.ToolCancelled, ErrorKind: domain.ErrorCancelled, Content: "cancelled"}
		},
	}
	sessions := newFakeSessionStore()
	sessions.honorCancellation = true
	runner := newTestRunner(
		&fakeProvider{streams: toolThenFinalStreams("read")},
		preparedRegistry{tool: tool},
		allowPolicy{},
		denyIfCalled{},
		sessions,
	)

	err := runner.RunTurn(ctx, testRunInput())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v want context canceled", err)
	}
	wantTail := []domain.EventKind{domain.EventToolStarted, domain.EventToolResult, domain.EventTurnInterrupted}
	kinds := sessions.kinds()
	if len(kinds) < len(wantTail) || !slices.Equal(kinds[len(kinds)-len(wantTail):], wantTail) {
		t.Fatalf("event tail=%v want=%v", kinds, wantTail)
	}
}

func TestRunnerValidatesToolCallRangeBeforeStartingTurn(t *testing.T) {
	for _, max := range []int{-1, 129} {
		t.Run(fmt.Sprintf("max_%d", max), func(t *testing.T) {
			provider := &fakeProvider{streams: toolThenFinalStreams("read")}
			sessions := newFakeSessionStore()
			runner := newTestRunner(provider, recordingRegistry{tool: &recordingTool{}}, allowPolicy{}, denyIfCalled{}, sessions)
			runner.MaxToolCalls = max

			if err := runner.RunTurn(context.Background(), testRunInput()); err == nil || err.Error() != "max tool calls must be 1..128" {
				t.Fatalf("error=%v want range error", err)
			}
			if got := len(sessions.kinds()); got != 0 {
				t.Fatalf("durable events=%d want=0", got)
			}
			if got := provider.callCount(); got != 0 {
				t.Fatalf("provider calls=%d want=0", got)
			}
		})
	}
}

func TestRunnerEmitsTransientDeltasAndPersistsAggregatedText(t *testing.T) {
	provider := &fakeProvider{streams: [][]domain.ModelEvent{{
		{Kind: domain.ModelTextDelta, Text: "hel"},
		{Kind: domain.ModelTextDelta, Text: "lo"},
		{Kind: domain.ModelDone},
	}}}
	sessions := newFakeSessionStore()
	runner := newTestRunner(provider, recordingRegistry{tool: &recordingTool{}}, allowPolicy{}, denyIfCalled{}, sessions)
	var runtimeEvents []agent.RuntimeEvent
	runner.Sink = func(event agent.RuntimeEvent) {
		runtimeEvents = append(runtimeEvents, event)
	}

	if err := runner.RunTurn(context.Background(), testRunInput()); err != nil {
		t.Fatal(err)
	}
	if len(runtimeEvents) != 3 {
		t.Fatalf("runtime events=%#v want state plus two deltas", runtimeEvents)
	}
	if runtimeEvents[0].Kind != agent.RuntimeStateChanged || runtimeEvents[0].State != "streaming_model" {
		t.Fatalf("first runtime event=%#v", runtimeEvents[0])
	}
	if got := []string{runtimeEvents[1].Text, runtimeEvents[2].Text}; !slices.Equal(got, []string{"hel", "lo"}) {
		t.Fatalf("runtime deltas=%v want=[hel lo]", got)
	}
	events := sessions.recordedEvents()
	if got := eventKinds(events); !slices.Equal(got, []domain.EventKind{domain.EventUserMessage, domain.EventAssistantMessage, domain.EventTurnCompleted}) {
		t.Fatalf("durable kinds=%v", got)
	}
	var payload domain.MessagePayload
	if err := json.Unmarshal(events[1].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Content != "hello" {
		t.Fatalf("assistant content=%q want=hello", payload.Content)
	}
	if got := terminalPayload(t, events[2]).ErrorKind; got != "" {
		t.Fatalf("completed error kind=%q want empty", got)
	}
}

func TestRunnerRequiresModelDoneForNormalCompletion(t *testing.T) {
	provider := &fakeProvider{streams: [][]domain.ModelEvent{{{Kind: domain.ModelTextDelta, Text: "partial"}}}}
	sessions := newFakeSessionStore()
	runner := newTestRunner(provider, recordingRegistry{tool: &recordingTool{}}, allowPolicy{}, denyIfCalled{}, sessions)

	err := runner.RunTurn(context.Background(), testRunInput())
	if err == nil || !strings.Contains(err.Error(), "without done") {
		t.Fatalf("error=%v want missing done", err)
	}
	if got := sessions.lastKind(); got != domain.EventTurnInterrupted {
		t.Fatalf("last event=%q want=%q", got, domain.EventTurnInterrupted)
	}
}

func TestRunnerPersistsApproverFailureAsTerminalState(t *testing.T) {
	sessions := newFakeSessionStore()
	runner := newTestRunner(
		&fakeProvider{streams: toolThenFinalStreams("read")},
		recordingRegistry{tool: &recordingTool{}},
		askPolicy{},
		approverFunc(func(context.Context, ports.PermissionPrompt) (domain.PermissionDecision, error) {
			return domain.PermissionDecision{}, errors.New("approval transport failed")
		}),
		sessions,
	)

	err := runner.RunTurn(context.Background(), testRunInput())
	if err == nil || !strings.Contains(err.Error(), "approval transport failed") {
		t.Fatalf("error=%v", err)
	}
	if got := sessions.lastKind(); got != domain.EventTurnFailed {
		t.Fatalf("last event=%q want=%q", got, domain.EventTurnFailed)
	}
}

func TestRunnerRedactsSecretSplitAcrossLiveDeltas(t *testing.T) {
	const sentinel = "top-secret"
	provider := &fakeProvider{streams: [][]domain.ModelEvent{{
		{Kind: domain.ModelTextDelta, Text: "before top-"},
		{Kind: domain.ModelTextDelta, Text: "secret after"},
		{Kind: domain.ModelDone},
	}}}
	runner := newTestRunner(provider, recordingRegistry{tool: &recordingTool{}}, allowPolicy{}, denyIfCalled{}, newFakeSessionStore())
	runner.Redact = func(value string) string { return strings.ReplaceAll(value, sentinel, "[REDACTED]") }
	var visible strings.Builder
	runner.Sink = func(event agent.RuntimeEvent) {
		if event.Kind == agent.RuntimeTextDelta {
			visible.WriteString(event.Text)
		}
	}

	if err := runner.RunTurn(context.Background(), testRunInput()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(visible.String(), sentinel) || !strings.Contains(visible.String(), "[REDACTED]") {
		t.Fatalf("visible stream=%q", visible.String())
	}
}

func TestRunnerRedactsDisplayedPermissionFieldsWithoutChangingApprovalScope(t *testing.T) {
	const sentinel = "permission-secret"
	rawScope := "/tmp/" + sentinel
	tool := &preparedFakeTool{
		preview: domain.PreparedToolRequest{
			CanonicalScope:  rawScope,
			InsideWorkspace: false,
			Summary:         "summary " + sentinel,
			ProposedDiff:    "+" + sentinel,
		},
	}
	sessions := newFakeSessionStore()
	policy := preparedPolicyFunc(func(_ context.Context, _ ports.PermissionContext, preview domain.PreparedToolRequest) domain.PermissionDecision {
		if preview.CanonicalScope != rawScope || preview.Summary != "summary "+sentinel || preview.ProposedDiff != "+"+sentinel {
			t.Fatalf("policy preview was redacted: %#v", preview)
		}
		return domain.PermissionDecision{Action: domain.PermissionAsk}
	})
	approver := approverFunc(func(_ context.Context, prompt ports.PermissionPrompt) (domain.PermissionDecision, error) {
		if strings.Contains(prompt.Call.CanonicalScope, sentinel) || strings.Contains(prompt.Call.Summary, sentinel) || strings.Contains(prompt.Call.ProposedDiff, sentinel) {
			t.Fatalf("displayed permission leaked secret: %#v", prompt.Call)
		}
		return domain.PermissionDecision{Action: domain.PermissionAllow, Lifetime: domain.PermissionOnce, Scope: rawScope}, nil
	})
	runner := newTestRunner(&fakeProvider{streams: toolThenFinalStreams("read")}, preparedRegistry{tool: tool}, policy, approver, sessions)
	runner.Redact = func(value string) string { return strings.ReplaceAll(value, sentinel, "[REDACTED]") }

	if err := runner.RunTurn(context.Background(), testRunInput()); err != nil {
		t.Fatal(err)
	}
	if got := permissionPayload(t, eventOfKind(t, sessions.recordedEvents(), domain.EventPermissionResolved)).Decision; got.Action != domain.PermissionAllow || got.Scope != rawScope {
		t.Fatalf("resolved decision=%#v", got)
	}
}

func TestRunnerUsesDurableContextForAllPostToolEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tool := &preparedFakeTool{
		descriptorName: "edit",
		preview:        domain.PreparedToolRequest{CanonicalScope: "/tmp/app", InsideWorkspace: true, Summary: "edit", FilePlan: &domain.FileChangePlan{CallID: "c1", Path: "/tmp/app/a.txt"}},
		result: domain.ToolResult{
			CallID: "c1", Status: domain.ToolSucceeded, Content: "changed",
			FileChange: &domain.FileChange{CallID: "c1", Path: "/tmp/app/a.txt"},
		},
	}
	sessions := newFakeSessionStore()
	sessions.honorCancellation = true
	sessions.beforeAppend = func(kind domain.EventKind) {
		if kind == domain.EventFileChanged {
			cancel()
		}
	}
	runner := newTestRunner(&fakeProvider{streams: toolThenFinalStreams("edit")}, preparedRegistry{tool: tool, name: "edit"}, allowPolicy{}, denyIfCalled{}, sessions)

	err := runner.RunTurn(ctx, testRunInput())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v want context canceled", err)
	}
	wantTail := []domain.EventKind{domain.EventFileChanged, domain.EventToolResult, domain.EventTurnInterrupted}
	kinds := sessions.kinds()
	if len(kinds) < len(wantTail) || !slices.Equal(kinds[len(kinds)-len(wantTail):], wantTail) {
		t.Fatalf("event tail=%v want=%v", kinds, wantTail)
	}
}

func TestRunnerAttemptsToolResultAndTerminalWhenFileChangeAppendFails(t *testing.T) {
	tool := &preparedFakeTool{
		descriptorName: "edit",
		preview:        domain.PreparedToolRequest{CanonicalScope: "/tmp/app", InsideWorkspace: true, Summary: "edit", FilePlan: &domain.FileChangePlan{CallID: "c1", Path: "/tmp/app/a.txt"}},
		result:         domain.ToolResult{CallID: "c1", Status: domain.ToolSucceeded, Content: "changed", FileChange: &domain.FileChange{CallID: "c1", Path: "/tmp/app/a.txt"}},
	}
	sessions := newFakeSessionStore()
	sessions.appendErrors = map[domain.EventKind]error{domain.EventFileChanged: errors.New("file change append failed")}
	runner := newTestRunner(&fakeProvider{streams: toolThenFinalStreams("edit")}, preparedRegistry{tool: tool, name: "edit"}, allowPolicy{}, denyIfCalled{}, sessions)

	err := runner.RunTurn(context.Background(), testRunInput())
	if err == nil || !strings.Contains(err.Error(), "file change append failed") {
		t.Fatalf("error=%v", err)
	}
	wantTail := []domain.EventKind{domain.EventToolStarted, domain.EventToolResult, domain.EventTurnFailed}
	kinds := sessions.kinds()
	if len(kinds) < len(wantTail) || !slices.Equal(kinds[len(kinds)-len(wantTail):], wantTail) {
		t.Fatalf("event tail=%v want=%v", kinds, wantTail)
	}
}

func TestRunnerToolLoopEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{name: "cancelled_stream", run: testRunnerCancelledStream},
		{name: "ask_then_deny", run: testRunnerAskThenDeny},
		{name: "unknown_tool", run: testRunnerUnknownTool},
		{name: "tool_limit_32", run: testRunnerToolLimit32},
	}
	for _, test := range tests {
		t.Run(test.name, test.run)
	}
}

func TestRunnerStopsWaitingForProviderStreamOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stream := make(chan domain.ModelEvent)
	started := make(chan struct{})
	provider := providerFunc(func(context.Context, domain.ModelRequest) (<-chan domain.ModelEvent, error) {
		close(started)
		return stream, nil
	})
	tool := &recordingTool{}
	sessions := newFakeSessionStore()
	runner := newTestRunner(provider, recordingRegistry{tool: tool}, allowPolicy{}, denyIfCalled{}, sessions)
	done := make(chan error, 1)
	go func() {
		done <- runner.RunTurn(ctx, testRunInput())
	}()
	<-started
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunTurn did not stop waiting for provider stream")
	}
	if got := sessions.lastKind(); got != domain.EventTurnInterrupted {
		t.Fatalf("last event=%q want=%q", got, domain.EventTurnInterrupted)
	}
	if got := tool.callCount(); got != 0 {
		t.Fatalf("tool calls=%d want=0", got)
	}
}

func TestRunnerDoesNotExecuteWhenApprovalCancelsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	provider := &fakeProvider{streams: toolThenFinalStreams("read")}
	tool := &recordingTool{}
	approver := approverFunc(func(context.Context, ports.PermissionPrompt) (domain.PermissionDecision, error) {
		cancel()
		return domain.PermissionDecision{Action: domain.PermissionAllow}, nil
	})
	sessions := newFakeSessionStore()
	runner := newTestRunner(provider, recordingRegistry{tool: tool}, askPolicy{}, approver, sessions)

	err := runner.RunTurn(ctx, testRunInput())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v want context canceled", err)
	}
	if got := sessions.lastKind(); got != domain.EventTurnInterrupted {
		t.Fatalf("last event=%q want=%q", got, domain.EventTurnInterrupted)
	}
	if got := tool.callCount(); got != 0 {
		t.Fatalf("tool calls=%d want=0", got)
	}
}

func TestRunnerDoesNotExecuteWhenToolStartPersistenceCancelsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	provider := &fakeProvider{streams: toolThenFinalStreams("read")}
	tool := &recordingTool{}
	sessions := newFakeSessionStore()
	sessions.onAppend = func(kind domain.EventKind) {
		if kind == domain.EventToolStarted {
			cancel()
		}
	}
	runner := newTestRunner(provider, recordingRegistry{tool: tool}, allowPolicy{}, denyIfCalled{}, sessions)

	err := runner.RunTurn(ctx, testRunInput())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v want context canceled", err)
	}
	if got := sessions.lastKind(); got != domain.EventTurnInterrupted {
		t.Fatalf("last event=%q want=%q", got, domain.EventTurnInterrupted)
	}
	if got := tool.callCount(); got != 0 {
		t.Fatalf("tool calls=%d want=0", got)
	}
	wantTail := []domain.EventKind{domain.EventToolStarted, domain.EventToolResult, domain.EventTurnInterrupted}
	kinds := sessions.kinds()
	if len(kinds) < len(wantTail) || !slices.Equal(kinds[len(kinds)-len(wantTail):], wantTail) {
		t.Fatalf("event tail=%v want=%v", kinds, wantTail)
	}
}

func TestRunnerDoesNotCompleteWhenAssistantPersistenceCancelsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	provider := &fakeProvider{streams: [][]domain.ModelEvent{{
		{Kind: domain.ModelTextDelta, Text: "done"},
		{Kind: domain.ModelDone},
	}}}
	sessions := newFakeSessionStore()
	sessions.onAppend = func(kind domain.EventKind) {
		if kind == domain.EventAssistantMessage {
			cancel()
		}
	}
	runner := newTestRunner(provider, recordingRegistry{tool: &recordingTool{}}, allowPolicy{}, denyIfCalled{}, sessions)

	err := runner.RunTurn(ctx, testRunInput())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v want context canceled", err)
	}
	if got := sessions.lastKind(); got != domain.EventTurnInterrupted {
		t.Fatalf("last event=%q want=%q", got, domain.EventTurnInterrupted)
	}
}

func TestRunnerPersistsInterruptionWhenProviderReturnsCancellation(t *testing.T) {
	provider := providerFunc(func(context.Context, domain.ModelRequest) (<-chan domain.ModelEvent, error) {
		return nil, context.Canceled
	})
	tool := &recordingTool{}
	sessions := newFakeSessionStore()
	runner := newTestRunner(provider, recordingRegistry{tool: tool}, allowPolicy{}, denyIfCalled{}, sessions)

	err := runner.RunTurn(context.Background(), testRunInput())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v want context canceled", err)
	}
	if got := sessions.lastKind(); got != domain.EventTurnInterrupted {
		t.Fatalf("last event=%q want=%q", got, domain.EventTurnInterrupted)
	}
	if got := tool.callCount(); got != 0 {
		t.Fatalf("tool calls=%d want=0", got)
	}
}

func TestRunnerPersistsInterruptionWhenApproverReturnsCancellation(t *testing.T) {
	provider := &fakeProvider{streams: toolThenFinalStreams("read")}
	tool := &recordingTool{}
	approver := approverFunc(func(context.Context, ports.PermissionPrompt) (domain.PermissionDecision, error) {
		return domain.PermissionDecision{}, context.Canceled
	})
	sessions := newFakeSessionStore()
	runner := newTestRunner(provider, recordingRegistry{tool: tool}, askPolicy{}, approver, sessions)

	err := runner.RunTurn(context.Background(), testRunInput())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v want context canceled", err)
	}
	if got := sessions.lastKind(); got != domain.EventTurnInterrupted {
		t.Fatalf("last event=%q want=%q", got, domain.EventTurnInterrupted)
	}
	if got := tool.callCount(); got != 0 {
		t.Fatalf("tool calls=%d want=0", got)
	}
}

func testRunnerCancelledStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tool := &recordingTool{}
	provider := providerFunc(func(context.Context, domain.ModelRequest) (<-chan domain.ModelEvent, error) {
		stream := make(chan domain.ModelEvent)
		go func() {
			stream <- domain.ModelEvent{Kind: domain.ModelToolCall, ToolCall: toolCall("c1", "read")}
			cancel()
			close(stream)
		}()
		return stream, nil
	})
	sessions := newFakeSessionStore()
	runner := newTestRunner(provider, recordingRegistry{tool: tool}, allowPolicy{}, denyIfCalled{}, sessions)

	err := runner.RunTurn(ctx, testRunInput())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v want context canceled", err)
	}
	if got := sessions.lastKind(); got != domain.EventTurnInterrupted {
		t.Fatalf("last event=%q want=%q", got, domain.EventTurnInterrupted)
	}
	if got := tool.callCount(); got != 0 {
		t.Fatalf("tool calls=%d want=0", got)
	}
}

func testRunnerAskThenDeny(t *testing.T) {
	provider := &fakeProvider{streams: toolThenFinalStreams("read")}
	tool := &recordingTool{}
	approver := &recordingApprover{decision: domain.PermissionDecision{Action: domain.PermissionDeny, Lifetime: domain.PermissionOnce, Scope: "/tmp/app", Reason: "not now"}}
	sessions := newFakeSessionStore()
	runner := newTestRunner(provider, recordingRegistry{tool: tool}, askPolicy{}, approver, sessions)

	if err := runner.RunTurn(context.Background(), testRunInput()); err != nil {
		t.Fatal(err)
	}
	if got := sessions.lastKind(); got != domain.EventTurnCompleted {
		t.Fatalf("last event=%q want=%q", got, domain.EventTurnCompleted)
	}
	if got := approver.callCount(); got != 1 {
		t.Fatalf("approval calls=%d want=1", got)
	}
	if got := tool.callCount(); got != 0 {
		t.Fatalf("tool calls=%d want=0", got)
	}
	assertProviderToolResult(t, provider, domain.ToolDenied, domain.ErrorPermissionDenied)
}

func testRunnerUnknownTool(t *testing.T) {
	provider := &fakeProvider{streams: toolThenFinalStreams("missing")}
	tool := &recordingTool{}
	sessions := newFakeSessionStore()
	runner := newTestRunner(provider, recordingRegistry{tool: tool}, preparedPolicyFunc(func(context.Context, ports.PermissionContext, domain.PreparedToolRequest) domain.PermissionDecision {
		t.Fatal("policy called for unknown tool")
		return domain.PermissionDecision{}
	}), denyIfCalled{}, sessions)

	if err := runner.RunTurn(context.Background(), testRunInput()); err != nil {
		t.Fatal(err)
	}
	if got := sessions.lastKind(); got != domain.EventTurnCompleted {
		t.Fatalf("last event=%q want=%q", got, domain.EventTurnCompleted)
	}
	if got := tool.callCount(); got != 0 {
		t.Fatalf("tool calls=%d want=0", got)
	}
	assertProviderToolResult(t, provider, domain.ToolFailed, domain.ErrorToolFailed)
}

func testRunnerToolLimit32(t *testing.T) {
	events := make([]domain.ModelEvent, 0, 34)
	for index := range 33 {
		events = append(events, domain.ModelEvent{Kind: domain.ModelToolCall, ToolCall: toolCall(fmt.Sprintf("c%d", index+1), "read")})
	}
	events = append(events, domain.ModelEvent{Kind: domain.ModelDone})
	provider := &fakeProvider{streams: [][]domain.ModelEvent{events}}
	tool := &recordingTool{}
	sessions := newFakeSessionStore()
	runner := newTestRunner(provider, recordingRegistry{tool: tool}, allowPolicy{}, denyIfCalled{}, sessions)
	runner.MaxToolCalls = 0

	err := runner.RunTurn(context.Background(), testRunInput())
	if err == nil || err.Error() != "tool call limit 32 reached" {
		t.Fatalf("error=%v want tool limit", err)
	}
	if got := sessions.lastKind(); got != domain.EventTurnFailed {
		t.Fatalf("last event=%q want=%q", got, domain.EventTurnFailed)
	}
	requests := tool.recordedRequests()
	if len(requests) != 0 {
		t.Fatalf("tool calls=%d want=0", len(requests))
	}
	if got := provider.callCount(); got != 1 {
		t.Fatalf("provider calls=%d want=1", got)
	}
}

func newTestRunner(provider ports.ModelProvider, registry ports.ToolRegistry, policy ports.PermissionPolicy, approver ports.PermissionApprover, sessions ports.SessionStore) agent.Runner {
	return agent.Runner{
		Provider:     provider,
		Tools:        registry,
		Policy:       policy,
		Approver:     approver,
		Sessions:     sessions,
		MaxToolCalls: 1,
		SystemPrompt: "system",
	}
}

func testRunInput() agent.RunInput {
	return agent.RunInput{
		Session: domain.Session{
			ID:        "s1",
			Mode:      domain.ModeAsk,
			Workspace: domain.Workspace{CanonicalPath: "/tmp/app"},
			Selection: domain.ModelSelection{Profile: "p", Model: "m"},
		},
		Prompt: "inspect",
	}
}

func toolCall(id, name string) *domain.ToolCall {
	return &domain.ToolCall{ID: id, Name: name, Arguments: json.RawMessage(`{"path":"a.go"}`)}
}

func toolThenFinalStreams(name string) [][]domain.ModelEvent {
	return [][]domain.ModelEvent{
		{{Kind: domain.ModelToolCall, ToolCall: toolCall("c1", name)}, {Kind: domain.ModelDone}},
		{{Kind: domain.ModelTextDelta, Text: "done"}, {Kind: domain.ModelDone}},
	}
}

func requestCallIDs(requests []domain.ToolRequest) []string {
	ids := make([]string, len(requests))
	for index, request := range requests {
		ids[index] = request.CallID
	}
	return ids
}

func eventKinds(events []domain.DurableEvent) []domain.EventKind {
	kinds := make([]domain.EventKind, len(events))
	for index, event := range events {
		kinds[index] = event.Kind
	}
	return kinds
}

func terminalPayload(t *testing.T, event domain.DurableEvent) domain.TurnTerminalPayload {
	t.Helper()
	var payload domain.TurnTerminalPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatalf("decode terminal payload: %v", err)
	}
	return payload
}

func permissionPayload(t *testing.T, event domain.DurableEvent) domain.PermissionPayload {
	t.Helper()
	var payload domain.PermissionPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatalf("decode permission payload: %v", err)
	}
	return payload
}

func assertProviderToolResult(t *testing.T, provider *fakeProvider, status domain.ToolStatus, errorKind domain.ErrorKind) {
	t.Helper()
	requests := provider.recordedRequests()
	if len(requests) != 2 {
		t.Fatalf("provider calls=%d want=2", len(requests))
	}
	messages := requests[1].Messages
	if len(messages) == 0 || messages[len(messages)-1].Role != domain.RoleTool {
		t.Fatalf("second provider messages=%#v", messages)
	}
	var result domain.ToolResult
	if err := json.Unmarshal([]byte(messages[len(messages)-1].Content), &result); err != nil {
		t.Fatalf("decode tool result: %v", err)
	}
	if result.Status != status || result.ErrorKind != errorKind {
		t.Fatalf("tool result=%#v want status=%q error=%q", result, status, errorKind)
	}
}

type providerFunc func(context.Context, domain.ModelRequest) (<-chan domain.ModelEvent, error)

func (f providerFunc) Stream(ctx context.Context, request domain.ModelRequest) (<-chan domain.ModelEvent, error) {
	return f(ctx, request)
}

type approverFunc func(context.Context, ports.PermissionPrompt) (domain.PermissionDecision, error)

func (f approverFunc) Resolve(ctx context.Context, prompt ports.PermissionPrompt) (domain.PermissionDecision, error) {
	return f(ctx, prompt)
}

type fakeProvider struct {
	mu       sync.Mutex
	streams  [][]domain.ModelEvent
	requests []domain.ModelRequest
}

func (f *fakeProvider) Stream(_ context.Context, request domain.ModelRequest) (<-chan domain.ModelEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.streams) == 0 {
		return nil, fmt.Errorf("scripted provider exhausted")
	}
	f.requests = append(f.requests, request)
	events := f.streams[0]
	f.streams = f.streams[1:]
	out := make(chan domain.ModelEvent, len(events))
	for _, event := range events {
		out <- event
	}
	close(out)
	return out, nil
}

func (f *fakeProvider) recordedRequests() []domain.ModelRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.ModelRequest(nil), f.requests...)
}

func (f *fakeProvider) callCount() int {
	return len(f.recordedRequests())
}

type fakeSessionStore struct {
	mu                sync.Mutex
	events            []domain.DurableEvent
	onAppend          func(domain.EventKind)
	beforeAppend      func(domain.EventKind)
	appendErrors      map[domain.EventKind]error
	honorCancellation bool
}

func newFakeSessionStore() *fakeSessionStore {
	return &fakeSessionStore{}
}

func (s *fakeSessionStore) Create(context.Context, domain.Workspace, domain.PermissionMode, domain.ModelSelection) (domain.Session, error) {
	return domain.Session{}, nil
}

func (s *fakeSessionStore) Load(context.Context, string) (domain.SessionReplay, error) {
	return domain.SessionReplay{}, nil
}

func (s *fakeSessionStore) List(context.Context, domain.Workspace) ([]domain.SessionSummary, error) {
	return nil, nil
}

func (s *fakeSessionStore) Append(ctx context.Context, sessionID string, kind domain.EventKind, payload any) (domain.DurableEvent, error) {
	if s.beforeAppend != nil {
		s.beforeAppend(kind)
	}
	if s.honorCancellation {
		if err := ctx.Err(); err != nil {
			return domain.DurableEvent{}, err
		}
	}
	if err := s.appendErrors[kind]; err != nil {
		return domain.DurableEvent{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := json.Marshal(payload)
	if err != nil {
		return domain.DurableEvent{}, err
	}
	event := domain.DurableEvent{
		SchemaVersion: 1,
		SessionID:     sessionID,
		Seq:           uint64(len(s.events) + 1),
		Kind:          kind,
		Payload:       raw,
	}
	s.events = append(s.events, event)
	if s.onAppend != nil {
		s.onAppend(kind)
	}
	return event, nil
}

func (s *fakeSessionStore) kinds() []domain.EventKind {
	return eventKinds(s.recordedEvents())
}

func (s *fakeSessionStore) recordedEvents() []domain.DurableEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.DurableEvent(nil), s.events...)
}

func (s *fakeSessionStore) lastKind() domain.EventKind {
	return s.lastEvent().Kind
}

func (s *fakeSessionStore) lastEvent() domain.DurableEvent {
	events := s.recordedEvents()
	if len(events) == 0 {
		return domain.DurableEvent{}
	}
	return events[len(events)-1]
}

type fakeTool struct {
	result domain.ToolResult
}

func (f fakeTool) Descriptor() domain.ToolDescriptor {
	return domain.ToolDescriptor{
		Name:             "read",
		Description:      "read a file",
		ScopeDescription: "exact file",
		InputSchema:      json.RawMessage(`{"type":"object"}`),
		Mutation:         domain.MutationReadOnly,
	}
}

func (f fakeTool) Prepare(_ context.Context, request domain.ToolRequest) (ports.PreparedTool, error) {
	return &preparedFakeCall{
		preview: domain.PreparedToolRequest{
			Request:         request,
			CanonicalScope:  request.Workspace,
			InsideWorkspace: true,
			Summary:         "read",
		},
		result: f.result,
	}, nil
}

type fakeRegistry struct {
	tool fakeTool
}

func (f fakeRegistry) Descriptors() []domain.ToolDescriptor {
	return []domain.ToolDescriptor{f.tool.Descriptor()}
}

func (f fakeRegistry) Lookup(name string) (ports.Tool, bool) {
	return f.tool, name == "read"
}

type allowPolicy struct{}

func (allowPolicy) Evaluate(context.Context, ports.PermissionContext, domain.PreparedToolRequest) domain.PermissionDecision {
	return domain.PermissionDecision{Action: domain.PermissionAllow, Lifetime: domain.PermissionOnce, Scope: "read:a.go"}
}

type denyPolicy struct{}

func (denyPolicy) Evaluate(context.Context, ports.PermissionContext, domain.PreparedToolRequest) domain.PermissionDecision {
	return domain.PermissionDecision{Action: domain.PermissionDeny, Reason: "denied by policy"}
}

type denyIfCalled struct{}

func (denyIfCalled) Resolve(context.Context, ports.PermissionPrompt) (domain.PermissionDecision, error) {
	panic("approver called for allow decision")
}

type askPolicy struct{}

func (askPolicy) Evaluate(context.Context, ports.PermissionContext, domain.PreparedToolRequest) domain.PermissionDecision {
	return domain.PermissionDecision{Action: domain.PermissionAsk}
}

type permissionGrant struct {
	tool  string
	scope string
}

type recordingGrantPolicy struct {
	mu       sync.Mutex
	decision domain.PermissionDecision
	grants   []permissionGrant
}

func (p *recordingGrantPolicy) Evaluate(context.Context, ports.PermissionContext, domain.PreparedToolRequest) domain.PermissionDecision {
	return p.decision
}

func (p *recordingGrantPolicy) GrantSession(tool, scope string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.grants = append(p.grants, permissionGrant{tool: tool, scope: scope})
}

func (p *recordingGrantPolicy) recordedGrants() []permissionGrant {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]permissionGrant(nil), p.grants...)
}

func (p *recordingGrantPolicy) grantCount() int {
	return len(p.recordedGrants())
}

type recordingApprover struct {
	mu       sync.Mutex
	decision domain.PermissionDecision
	calls    int
}

func (a *recordingApprover) Resolve(context.Context, ports.PermissionPrompt) (domain.PermissionDecision, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	return a.decision, nil
}

func (a *recordingApprover) callCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

type recordingTool struct {
	mu       sync.Mutex
	requests []domain.ToolRequest
}

func (t *recordingTool) Descriptor() domain.ToolDescriptor {
	return domain.ToolDescriptor{
		Name:             "read",
		Description:      "read a file",
		ScopeDescription: "exact file",
		InputSchema:      json.RawMessage(`{"type":"object"}`),
		Mutation:         domain.MutationReadOnly,
	}
}

func (t *recordingTool) Prepare(_ context.Context, request domain.ToolRequest) (ports.PreparedTool, error) {
	return recordingPreparedTool{tool: t, request: request}, nil
}

type recordingPreparedTool struct {
	tool    *recordingTool
	request domain.ToolRequest
}

func (p recordingPreparedTool) Preview() domain.PreparedToolRequest {
	return domain.PreparedToolRequest{
		Request:         p.request,
		CanonicalScope:  p.request.Workspace,
		InsideWorkspace: true,
		Summary:         "read",
	}
}

func (p recordingPreparedTool) Execute(context.Context) domain.ToolResult {
	t := p.tool
	t.mu.Lock()
	defer t.mu.Unlock()
	t.requests = append(t.requests, p.request)
	return domain.ToolResult{CallID: p.request.CallID, Status: domain.ToolSucceeded, Content: "ok"}
}

func (t *recordingTool) recordedRequests() []domain.ToolRequest {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]domain.ToolRequest(nil), t.requests...)
}

func (t *recordingTool) callCount() int {
	return len(t.recordedRequests())
}

type recordingRegistry struct {
	tool *recordingTool
}

func (r recordingRegistry) Descriptors() []domain.ToolDescriptor {
	return []domain.ToolDescriptor{r.tool.Descriptor()}
}

func (r recordingRegistry) Lookup(name string) (ports.Tool, bool) {
	return r.tool, name == r.tool.Descriptor().Name
}
