package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
)

func TestRunnerPreparesBeforePolicyAndExecution(t *testing.T) {
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	canonicalScope := filepath.Join(workspace, "a.go")
	order := make([]string, 0, 3)
	preview := domain.PreparedToolRequest{
		CanonicalScope:  canonicalScope,
		InsideWorkspace: true,
		Summary:         "read a.go",
	}
	tool := &preparedFakeTool{
		preview: preview,
		result:  domain.ToolResult{CallID: "c1", Status: domain.ToolSucceeded, Content: "ok"},
		onPrepare: func(request domain.ToolRequest) {
			order = append(order, "prepare")
			if request.Workspace != workspace {
				t.Fatalf("prepare workspace=%q want=%q", request.Workspace, workspace)
			}
		},
		onExecute: func() { order = append(order, "execute") },
	}
	policy := preparedPolicyFunc(func(_ context.Context, _ ports.PermissionContext, call domain.PreparedToolRequest) domain.PermissionDecision {
		order = append(order, "policy")
		if call.CanonicalScope != canonicalScope || !call.InsideWorkspace {
			t.Fatalf("policy call=%#v", call)
		}
		return domain.PermissionDecision{Action: domain.PermissionAllow, Lifetime: domain.PermissionOnce}
	})
	sessions := newFakeSessionStore()
	runner := newTestRunner(
		&fakeProvider{streams: toolThenFinalStreams("read")},
		preparedRegistry{tool: tool},
		policy,
		denyIfCalled{},
		sessions,
	)
	input := testRunInput()
	input.Session.Workspace.CanonicalPath = workspace

	if err := runner.RunTurn(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(order, []string{"prepare", "policy", "execute"}) {
		t.Fatalf("call order=%v", order)
	}

	requested := eventOfKind(t, sessions.recordedEvents(), domain.EventToolRequested)
	var persisted domain.PreparedToolRequest
	if err := json.Unmarshal(requested.Payload, &persisted); err != nil {
		t.Fatal(err)
	}
	preview.Request = domain.ToolRequest{CallID: "c1", Name: "read", Input: json.RawMessage(`{"path":"a.go"}`), Workspace: workspace}
	preview.Mutation = domain.MutationReadOnly
	if !reflect.DeepEqual(persisted, preview) {
		t.Fatalf("persisted preview=%#v want=%#v", persisted, preview)
	}
}

func TestRunnerPersistsFilePlanAndChangeAroundExecution(t *testing.T) {
	plan := &domain.FileChangePlan{
		CallID:         "c1",
		Path:           "/workspace/a.go",
		ExpectedSHA256: "before",
		PlannedSHA256:  "after",
		Diff:           "diff",
	}
	change := &domain.FileChange{
		CallID:       "c1",
		Path:         "/workspace/a.go",
		BeforeSHA256: "before",
		AfterSHA256:  "after",
		Diff:         "diff",
	}
	tool := &preparedFakeTool{
		preview: domain.PreparedToolRequest{CanonicalScope: plan.Path, InsideWorkspace: true, Summary: "edit a.go", FilePlan: plan},
		result:  domain.ToolResult{CallID: "c1", Status: domain.ToolSucceeded, Content: "edited", FileChange: change},
	}
	sessions := newFakeSessionStore()
	runner := newTestRunner(
		&fakeProvider{streams: toolThenFinalStreams("edit")},
		preparedRegistry{tool: tool, name: "edit"},
		preparedPolicyFunc(func(context.Context, ports.PermissionContext, domain.PreparedToolRequest) domain.PermissionDecision {
			return domain.PermissionDecision{Action: domain.PermissionAllow, Lifetime: domain.PermissionOnce}
		}),
		denyIfCalled{},
		sessions,
	)

	if err := runner.RunTurn(context.Background(), testRunInput()); err != nil {
		t.Fatal(err)
	}
	wantOrder := []domain.EventKind{
		domain.EventToolRequested,
		domain.EventPermissionResolved,
		domain.EventFileChangePlanned,
		domain.EventToolStarted,
		domain.EventFileChanged,
		domain.EventToolResult,
	}
	gotOrder := make([]domain.EventKind, 0, len(wantOrder))
	for _, kind := range sessions.kinds() {
		if slices.Contains(wantOrder, kind) {
			gotOrder = append(gotOrder, kind)
		}
	}
	if !slices.Equal(gotOrder, wantOrder) {
		t.Fatalf("file event order=%v want=%v", gotOrder, wantOrder)
	}

	plannedEvent := eventOfKind(t, sessions.recordedEvents(), domain.EventFileChangePlanned)
	var persistedPlan domain.FileChangePlan
	if err := json.Unmarshal(plannedEvent.Payload, &persistedPlan); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(persistedPlan, *plan) {
		t.Fatalf("persisted plan=%#v want=%#v", persistedPlan, *plan)
	}
	changedEvent := eventOfKind(t, sessions.recordedEvents(), domain.EventFileChanged)
	var persistedChange domain.FileChange
	if err := json.Unmarshal(changedEvent.Payload, &persistedChange); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(persistedChange, *change) {
		t.Fatalf("persisted change=%#v want=%#v", persistedChange, *change)
	}
}

func TestRunnerPassesPreparedPreviewToApprover(t *testing.T) {
	preview := domain.PreparedToolRequest{
		CanonicalScope:  "/canonical/a.go",
		InsideWorkspace: false,
		ProposedDiff:    "diff",
		Summary:         "edit outside workspace",
	}
	tool := &preparedFakeTool{
		preview: preview,
		result:  domain.ToolResult{CallID: "c1", Status: domain.ToolSucceeded, Content: "ok"},
	}
	approver := approverFunc(func(_ context.Context, prompt ports.PermissionPrompt) (domain.PermissionDecision, error) {
		preview.Request = domain.ToolRequest{
			CallID:    "c1",
			Name:      "read",
			Input:     json.RawMessage(`{"path":"a.go"}`),
			Workspace: "/tmp/app",
		}
		preview.Mutation = domain.MutationReadOnly
		if prompt.SessionID != "s1" || !reflect.DeepEqual(prompt.Call, preview) {
			t.Fatalf("permission prompt=%#v want call=%#v", prompt, preview)
		}
		return domain.PermissionDecision{Action: domain.PermissionDeny}, nil
	})
	runner := newTestRunner(
		&fakeProvider{streams: toolThenFinalStreams("read")},
		preparedRegistry{tool: tool},
		preparedPolicyFunc(func(context.Context, ports.PermissionContext, domain.PreparedToolRequest) domain.PermissionDecision {
			return domain.PermissionDecision{Action: domain.PermissionAsk}
		}),
		approver,
		newFakeSessionStore(),
	)

	if err := runner.RunTurn(context.Background(), testRunInput()); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerPreparationFailureBypassesPolicyAndReturnsFailedResult(t *testing.T) {
	tool := &preparedFakeTool{prepareErr: errors.New("invalid arguments")}
	runner := newTestRunner(
		&fakeProvider{streams: toolThenFinalStreams("read")},
		preparedRegistry{tool: tool},
		preparedPolicyFunc(func(context.Context, ports.PermissionContext, domain.PreparedToolRequest) domain.PermissionDecision {
			t.Fatal("policy called after preparation failed")
			return domain.PermissionDecision{}
		}),
		denyIfCalled{},
		newFakeSessionStore(),
	)

	if err := runner.RunTurn(context.Background(), testRunInput()); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerDoesNotPrepareOutsidePreviewBeforeDenial(t *testing.T) {
	previewCalls := 0
	tool := &preparedFakeTool{
		preview: domain.PreparedToolRequest{CanonicalScope: "/outside/a.go", InsideWorkspace: false, Summary: "edit outside"},
		onPreparePreview: func() error {
			previewCalls++
			return nil
		},
	}
	runner := newTestRunner(
		&fakeProvider{streams: toolThenFinalStreams("edit")},
		preparedRegistry{tool: tool, name: "edit"},
		preparedPolicyFunc(func(context.Context, ports.PermissionContext, domain.PreparedToolRequest) domain.PermissionDecision {
			return domain.PermissionDecision{Action: domain.PermissionDeny, Lifetime: domain.PermissionOnce, Scope: "/outside/a.go"}
		}),
		denyIfCalled{},
		newFakeSessionStore(),
	)
	if err := runner.RunTurn(context.Background(), testRunInput()); err != nil {
		t.Fatal(err)
	}
	if previewCalls != 0 {
		t.Fatalf("outside preview calls=%d want=0", previewCalls)
	}
}

func TestRunnerDoesNotPrepareDeniedInsideEditPreview(t *testing.T) {
	previewCalls := 0
	tool := &preparedFakeTool{
		descriptorName: "edit",
		preview:        domain.PreparedToolRequest{CanonicalScope: "/workspace/a.go", InsideWorkspace: true, Summary: "edit inside"},
		onPreparePreview: func() error {
			previewCalls++
			return nil
		},
	}
	runner := newTestRunner(
		&fakeProvider{streams: toolThenFinalStreams("edit")},
		preparedRegistry{tool: tool, name: "edit"},
		preparedPolicyFunc(func(context.Context, ports.PermissionContext, domain.PreparedToolRequest) domain.PermissionDecision {
			return domain.PermissionDecision{Action: domain.PermissionDeny, Lifetime: domain.PermissionOnce, Scope: "/workspace/a.go"}
		}),
		denyIfCalled{},
		newFakeSessionStore(),
	)
	if err := runner.RunTurn(context.Background(), testRunInput()); err != nil {
		t.Fatal(err)
	}
	if previewCalls != 0 {
		t.Fatalf("denied preview calls=%d want=0", previewCalls)
	}
}

func TestRunnerRequiresPreviewApprovalBeforeExecutingOutsideEdit(t *testing.T) {
	const scope = "/outside/a.go"
	previewCalls := 0
	executeCalls := 0
	tool := &preparedFakeTool{
		descriptorName: "edit",
		preview:        domain.PreparedToolRequest{CanonicalScope: scope, InsideWorkspace: false, Summary: "edit outside"},
		afterPreview: &domain.PreparedToolRequest{
			CanonicalScope:  scope,
			InsideWorkspace: false,
			Summary:         "edit outside",
			ProposedDiff:    "@@ -1 +1 @@\n-old\n+new",
			FilePlan:        &domain.FileChangePlan{CallID: "c1", Path: scope, ExpectedSHA256: "before", PlannedSHA256: "after", Diff: "@@ -1 +1 @@\n-old\n+new"},
		},
		onPreparePreview: func() error {
			previewCalls++
			return nil
		},
		onExecute: func() { executeCalls++ },
		result:    domain.ToolResult{CallID: "c1", Status: domain.ToolSucceeded},
	}
	prompts := 0
	approver := approverFunc(func(_ context.Context, prompt ports.PermissionPrompt) (domain.PermissionDecision, error) {
		prompts++
		switch prompts {
		case 1:
			if previewCalls != 0 || prompt.Call.ProposedDiff != "" || prompt.Call.FilePlan != nil {
				t.Fatalf("first prompt accessed outside edit: calls=%d prompt=%#v", previewCalls, prompt.Call)
			}
		case 2:
			if previewCalls != 1 || prompt.Call.ProposedDiff == "" || prompt.Call.FilePlan == nil {
				t.Fatalf("second prompt lacks prepared diff: calls=%d prompt=%#v", previewCalls, prompt.Call)
			}
		default:
			t.Fatalf("unexpected prompt %d: %#v", prompts, prompt.Call)
		}
		return domain.PermissionDecision{Action: domain.PermissionAllow, Lifetime: domain.PermissionOnce, Scope: scope}, nil
	})
	runner := newTestRunner(
		&fakeProvider{streams: toolThenFinalStreams("edit")},
		preparedRegistry{tool: tool, name: "edit"},
		preparedPolicyFunc(func(context.Context, ports.PermissionContext, domain.PreparedToolRequest) domain.PermissionDecision {
			return domain.PermissionDecision{Action: domain.PermissionAsk, Lifetime: domain.PermissionOnce, Scope: scope}
		}),
		approver,
		newFakeSessionStore(),
	)
	if err := runner.RunTurn(context.Background(), testRunInput()); err != nil {
		t.Fatal(err)
	}
	if prompts != 2 || previewCalls != 1 || executeCalls != 1 {
		t.Fatalf("prompts=%d previews=%d executes=%d", prompts, previewCalls, executeCalls)
	}
}

func TestRunnerDeniesOutsideEditWhenPreviewApprovalIsDenied(t *testing.T) {
	const scope = "/outside/a.go"
	previewCalls := 0
	executeCalls := 0
	tool := &preparedFakeTool{
		descriptorName: "edit",
		preview:        domain.PreparedToolRequest{CanonicalScope: scope, InsideWorkspace: false, Summary: "edit outside"},
		afterPreview: &domain.PreparedToolRequest{
			CanonicalScope:  scope,
			InsideWorkspace: false,
			Summary:         "edit outside",
			ProposedDiff:    "diff",
			FilePlan:        &domain.FileChangePlan{CallID: "c1", Path: scope, Diff: "diff"},
		},
		onPreparePreview: func() error { previewCalls++; return nil },
		onExecute:        func() { executeCalls++ },
		result:           domain.ToolResult{CallID: "c1", Status: domain.ToolSucceeded},
	}
	prompts := 0
	approver := approverFunc(func(_ context.Context, prompt ports.PermissionPrompt) (domain.PermissionDecision, error) {
		prompts++
		action := domain.PermissionAllow
		if prompts == 2 {
			action = domain.PermissionDeny
		}
		return domain.PermissionDecision{Action: action, Lifetime: domain.PermissionOnce, Scope: prompt.Call.CanonicalScope}, nil
	})
	runner := newTestRunner(
		&fakeProvider{streams: toolThenFinalStreams("edit")},
		preparedRegistry{tool: tool, name: "edit"},
		preparedPolicyFunc(func(context.Context, ports.PermissionContext, domain.PreparedToolRequest) domain.PermissionDecision {
			return domain.PermissionDecision{Action: domain.PermissionAsk, Lifetime: domain.PermissionOnce, Scope: scope}
		}),
		approver,
		newFakeSessionStore(),
	)
	if err := runner.RunTurn(context.Background(), testRunInput()); err != nil {
		t.Fatal(err)
	}
	if prompts != 2 || previewCalls != 1 || executeCalls != 0 {
		t.Fatalf("prompts=%d previews=%d executes=%d", prompts, previewCalls, executeCalls)
	}
}

func eventOfKind(t *testing.T, events []domain.DurableEvent, kind domain.EventKind) domain.DurableEvent {
	t.Helper()
	for _, event := range events {
		if event.Kind == kind {
			return event
		}
	}
	t.Fatalf("event %q not found in %v", kind, eventKinds(events))
	return domain.DurableEvent{}
}

type preparedPolicyFunc func(context.Context, ports.PermissionContext, domain.PreparedToolRequest) domain.PermissionDecision

func (f preparedPolicyFunc) Evaluate(ctx context.Context, permissionContext ports.PermissionContext, call domain.PreparedToolRequest) domain.PermissionDecision {
	return f(ctx, permissionContext, call)
}

type preparedFakeTool struct {
	descriptorName   string
	preview          domain.PreparedToolRequest
	afterPreview     *domain.PreparedToolRequest
	result           domain.ToolResult
	prepareErr       error
	onPrepare        func(domain.ToolRequest)
	onExecute        func()
	onExecuteContext func(context.Context) domain.ToolResult
	onPreparePreview func() error
}

func (t *preparedFakeTool) Descriptor() domain.ToolDescriptor {
	name := t.descriptorName
	if name == "" {
		name = "read"
	}
	mutation := domain.MutationReadOnly
	if name == "edit" {
		mutation = domain.MutationFile
	}
	return domain.ToolDescriptor{Name: name, Description: "test tool", ScopeDescription: "test scope", InputSchema: json.RawMessage(`{"type":"object"}`), Mutation: mutation}
}

func (t *preparedFakeTool) Prepare(_ context.Context, request domain.ToolRequest) (ports.PreparedTool, error) {
	if t.onPrepare != nil {
		t.onPrepare(request)
	}
	if t.prepareErr != nil {
		return nil, t.prepareErr
	}
	preview := t.preview
	preview.Request = request
	return &preparedFakeCall{preview: preview, afterPreview: t.afterPreview, result: t.result, onExecute: t.onExecute, onExecuteContext: t.onExecuteContext, onPreparePreview: t.onPreparePreview}, nil
}

type preparedFakeCall struct {
	preview          domain.PreparedToolRequest
	afterPreview     *domain.PreparedToolRequest
	result           domain.ToolResult
	onExecute        func()
	onExecuteContext func(context.Context) domain.ToolResult
	onPreparePreview func() error
}

func (c *preparedFakeCall) Preview() domain.PreparedToolRequest { return c.preview }

func (c *preparedFakeCall) PreparePreview(context.Context) error {
	if c.onPreparePreview != nil {
		if err := c.onPreparePreview(); err != nil {
			return err
		}
	}
	if c.afterPreview != nil {
		request := c.preview.Request
		c.preview = *c.afterPreview
		c.preview.Request = request
	}
	return nil
}

func (c *preparedFakeCall) Execute(ctx context.Context) domain.ToolResult {
	if c.onExecuteContext != nil {
		return c.onExecuteContext(ctx)
	}
	if c.onExecute != nil {
		c.onExecute()
	}
	return c.result
}

type preparedRegistry struct {
	tool *preparedFakeTool
	name string
}

func (r preparedRegistry) Descriptors() []domain.ToolDescriptor {
	return []domain.ToolDescriptor{r.tool.Descriptor()}
}

func (r preparedRegistry) Lookup(name string) (ports.Tool, bool) {
	want := r.name
	if want == "" {
		want = "read"
	}
	return r.tool, name == want
}
