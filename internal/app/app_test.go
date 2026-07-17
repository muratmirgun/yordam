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
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/secret"
)

func TestAppOwnsRuntimeLifecycleThroughRuntimeSet(t *testing.T) {
	optionsType := reflect.TypeOf(app.Options{})
	for _, legacyField := range []string{"Runtime", "CompactSession", "ConfiguredModels"} {
		if _, exists := optionsType.FieldByName(legacyField); exists {
			t.Errorf("app.Options still exposes legacy generation field %q", legacyField)
		}
	}
	if _, exists := reflect.TypeOf(app.BootstrapOptions{}).FieldByName("Config"); exists {
		t.Error("app.BootstrapOptions still accepts an in-memory config")
	}
}

func TestAppRedactsConfiguredSecretFromEveryPublishedEventField(t *testing.T) {
	const configuredSecret = "configured-event-secret-sentinel"
	runtimeEvents := make(chan agent.RuntimeEvent, 4)
	runtime := &fakeRuntime{run: func(context.Context, agent.RunInput) error {
		runtimeEvents <- agent.RuntimeEvent{Kind: agent.RuntimeStateChanged, State: configuredSecret}
		runtimeEvents <- agent.RuntimeEvent{Kind: agent.RuntimeTextDelta, Text: configuredSecret}
		runtimeEvents <- agent.RuntimeEvent{Kind: agent.RuntimeToolOutput, Progress: &domain.ToolProgress{CallID: configuredSecret, Text: configuredSecret}}
		runtimeEvents <- agent.RuntimeEvent{Kind: agent.RuntimeToolCompleted, Result: &domain.ToolResult{
			CallID:  configuredSecret,
			Content: configuredSecret,
			FileChange: &domain.FileChange{
				Path: configuredSecret,
				Diff: configuredSecret,
			},
			WorkspaceChanges: &domain.WorkspaceChanges{
				Status: configuredSecret,
				Diff:   configuredSecret,
				Notice: configuredSecret,
			},
		}}
		return errors.New(configuredSecret)
	}}
	redactors := secret.NewBinding(secret.New(configuredSecret))
	application := app.New(app.Options{
		RuntimeSet:    testRuntimeSet(runtime),
		RuntimeEvents: runtimeEvents,
		Redactors:     redactors,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: configuredSecret}
	for {
		event := receiveEvent(t, application.Events())
		raw, err := json.Marshal(event)
		if err != nil {
			t.Fatal("marshal app event")
		}
		if strings.Contains(string(raw), configuredSecret) {
			t.Fatal("published app event contains configured secret")
		}
		if event.Err != nil && strings.Contains(event.Err.Error(), configuredSecret) {
			t.Fatal("published app event error contains configured secret")
		}
		if event.Kind == app.EventError {
			break
		}
	}
}

func TestAppRejectsConcurrentTurnAndCancelsActive(t *testing.T) {
	started := make(chan struct{})
	runtime := &fakeRuntime{run: func(ctx context.Context, _ agent.RunInput) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}}
	application := app.New(app.Options{RuntimeSet: testRuntimeSet(runtime)})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "one"}
	<-started
	requireTurnAccepted(t, application.Events(), "one")
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "two"}
	if event := <-application.Events(); event.Kind != app.EventRejected {
		t.Fatalf("event=%s", event.Kind)
	}
	application.Commands() <- app.Command{Kind: app.CommandCancelTurn}
	if event := <-application.Events(); event.Kind != app.EventTurnInterrupted {
		t.Fatalf("event=%s", event.Kind)
	}
}

func TestAppSnapshotsInputAndAllowsNextTurnAfterTerminal(t *testing.T) {
	inputs := make(chan agent.RunInput, 2)
	runtime := &fakeRuntime{run: func(_ context.Context, input agent.RunInput) error {
		inputs <- input
		return nil
	}}
	application := app.New(app.Options{
		RuntimeSet: testRuntimeSet(runtime),
		Input: func(prompt string) (agent.RunInput, error) {
			return agent.RunInput{Prompt: "snapshot:" + prompt}, nil
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	for _, prompt := range []string{"one", "two"} {
		application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: prompt}
		requireTurnAccepted(t, application.Events(), prompt)
		if input := <-inputs; input.Prompt != "snapshot:"+prompt {
			t.Fatalf("input prompt=%q", input.Prompt)
		}
		if event := receiveEvent(t, application.Events()); event.Kind != app.EventTurnCompleted {
			t.Fatalf("terminal event=%+v", event)
		}
	}
}

func TestAppPublishesBufferedRuntimeEventsBeforeTerminal(t *testing.T) {
	for iteration := range 50 {
		runtimeEvents := make(chan agent.RuntimeEvent, 2)
		runs := 0
		runtime := &fakeRuntime{run: func(context.Context, agent.RunInput) error {
			runs++
			runtimeEvents <- agent.RuntimeEvent{Kind: agent.RuntimeTextDelta, Text: "one"}
			runtimeEvents <- agent.RuntimeEvent{Kind: agent.RuntimeTextDelta, Text: "two"}
			return nil
		}}
		application := app.New(app.Options{RuntimeSet: testRuntimeSet(runtime), RuntimeEvents: runtimeEvents, EventBuffer: 4})
		ctx, cancel := context.WithCancel(context.Background())
		go application.Run(ctx)
		application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "prompt"}

		got := []app.EventKind{
			receiveEvent(t, application.Events()).Kind,
			receiveEvent(t, application.Events()).Kind,
			receiveEvent(t, application.Events()).Kind,
			receiveEvent(t, application.Events()).Kind,
		}
		cancel()
		want := []app.EventKind{app.EventTurnAccepted, app.EventTextDelta, app.EventTextDelta, app.EventTurnCompleted}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("iteration %d events=%v want=%v", iteration, got, want)
		}
		if runs != 1 {
			t.Fatalf("iteration %d runtime calls=%d want=1", iteration, runs)
		}
	}
}

func TestAppShutdownWaitsForActiveRuntime(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	runtime := &fakeRuntime{run: func(ctx context.Context, _ agent.RunInput) error {
		close(started)
		<-ctx.Done()
		<-release
		return ctx.Err()
	}}
	application := app.New(app.Options{RuntimeSet: testRuntimeSet(runtime)})
	done := make(chan error, 1)
	go func() { done <- application.Run(context.Background()) }()
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "prompt"}
	<-started
	application.Commands() <- app.Command{Kind: app.CommandShutdown}

	select {
	case err := <-done:
		t.Fatalf("app returned before runtime stopped: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("app did not finish after runtime stopped")
	}
}

func TestAppParentCancellationJoinsRuntimeWithoutEventConsumer(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	runtimeEvents := make(chan agent.RuntimeEvent, 1)
	runtime := &fakeRuntime{run: func(ctx context.Context, _ agent.RunInput) error {
		close(started)
		runtimeEvents <- agent.RuntimeEvent{Kind: agent.RuntimeTextDelta, Text: "buffered"}
		<-ctx.Done()
		<-release
		return ctx.Err()
	}}
	application := app.New(app.Options{RuntimeSet: testRuntimeSet(runtime), RuntimeEvents: runtimeEvents})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- application.Run(ctx) }()
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "prompt"}
	<-started
	cancel()

	select {
	case err := <-done:
		t.Fatalf("app returned before runtime joined: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("app blocked publishing during shutdown without a UI consumer")
	}
}

func TestAppShutdownCommandJoinsRuntimeWithoutEventConsumer(t *testing.T) {
	started := make(chan struct{})
	eventAccepted := make(chan struct{})
	release := make(chan struct{})
	runtimeEvents := make(chan agent.RuntimeEvent)
	runtime := &fakeRuntime{run: func(ctx context.Context, _ agent.RunInput) error {
		close(started)
		runtimeEvents <- agent.RuntimeEvent{Kind: agent.RuntimeTextDelta, Text: "unconsumed"}
		close(eventAccepted)
		<-ctx.Done()
		<-release
		return ctx.Err()
	}}
	application := app.New(app.Options{RuntimeSet: testRuntimeSet(runtime), RuntimeEvents: runtimeEvents})
	done := make(chan error, 1)
	go func() { done <- application.Run(context.Background()) }()
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "prompt"}
	<-started
	<-eventAccepted
	application.Commands() <- app.Command{Kind: app.CommandShutdown}

	select {
	case err := <-done:
		t.Fatalf("app returned before runtime joined: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown command blocked behind unconsumed event publication")
	}
}

func TestAppInputAndRuntimeErrorsEmitErrorEvents(t *testing.T) {
	runtimeErr := errors.New("runtime failed")
	runtime := &fakeRuntime{run: func(context.Context, agent.RunInput) error { return runtimeErr }}
	inputCalls := 0
	application := app.New(app.Options{
		RuntimeSet: testRuntimeSet(runtime),
		Input: func(prompt string) (agent.RunInput, error) {
			inputCalls++
			if inputCalls == 1 {
				return agent.RunInput{}, errors.New("snapshot failed")
			}
			return agent.RunInput{Prompt: prompt}, nil
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "one"}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventError || event.Message != "snapshot failed" {
		t.Fatalf("input event=%+v", event)
	}
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "two"}
	requireTurnAccepted(t, application.Events(), "two")
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventError || !errors.Is(event.Err, runtimeErr) {
		t.Fatalf("runtime event=%+v", event)
	}
}

func TestAppRunsCompactionOnlyWhileIdle(t *testing.T) {
	turnStarted := make(chan struct{})
	runtime := &fakeRuntime{run: func(ctx context.Context, _ agent.RunInput) error {
		close(turnStarted)
		<-ctx.Done()
		return ctx.Err()
	}}
	compactCalls := make(chan struct{}, 1)
	application := app.New(app.Options{
		RuntimeSet: testRuntimeSet(runtime),
		Compact: func(context.Context) error {
			compactCalls <- struct{}{}
			return nil
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "one"}
	<-turnStarted
	requireTurnAccepted(t, application.Events(), "one")
	application.Commands() <- app.Command{Kind: app.CommandCompact}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventRejected {
		t.Fatalf("active compact event=%+v", event)
	}
	application.Commands() <- app.Command{Kind: app.CommandCancelTurn}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventTurnInterrupted {
		t.Fatalf("turn terminal=%+v", event)
	}

	application.Commands() <- app.Command{Kind: app.CommandCompact}
	select {
	case <-compactCalls:
	case <-time.After(time.Second):
		t.Fatal("compaction did not start")
	}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventTurnCompleted {
		t.Fatalf("compact terminal=%+v", event)
	}
}

func TestAppTranslatesRuntimeEvents(t *testing.T) {
	runtimeEvents := make(chan agent.RuntimeEvent)
	application := app.New(app.Options{RuntimeEvents: runtimeEvents})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	progress := domain.ToolProgress{CallID: "call-1", Text: "working"}
	result := domain.ToolResult{CallID: "call-1", Content: "done"}
	tests := []struct {
		runtime agent.RuntimeEvent
		want    app.EventKind
	}{
		{runtime: agent.RuntimeEvent{Kind: agent.RuntimeStateChanged, State: "streaming_model"}, want: app.EventState},
		{runtime: agent.RuntimeEvent{Kind: agent.RuntimeTextDelta, Text: "hello"}, want: app.EventTextDelta},
		{runtime: agent.RuntimeEvent{Kind: agent.RuntimeToolStarted}, want: app.EventToolStarted},
		{runtime: agent.RuntimeEvent{Kind: agent.RuntimeToolOutput, Progress: &progress}, want: app.EventToolOutput},
		{runtime: agent.RuntimeEvent{Kind: agent.RuntimeToolCompleted, Result: &result}, want: app.EventToolCompleted},
	}
	for _, test := range tests {
		runtimeEvents <- test.runtime
		event := receiveEvent(t, application.Events())
		if event.Kind != test.want || event.Runtime != test.runtime {
			t.Fatalf("event=%+v want kind=%q runtime=%+v", event, test.want, test.runtime)
		}
	}
}

func TestAppPermissionBrokerResolvesRegisteredCall(t *testing.T) {
	application := app.New(app.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	prompt := ports.PermissionPrompt{SessionID: "session-1", Call: domain.PreparedToolRequest{
		Request:         domain.ToolRequest{CallID: "call-1", Name: "shell"},
		CanonicalScope:  "/workspace",
		InsideWorkspace: true,
		ProposedDiff:    "diff",
		Summary:         "run command",
	}}
	decision := domain.PermissionDecision{Action: domain.PermissionAllow, Lifetime: domain.PermissionOnce, Scope: prompt.Call.CanonicalScope}
	resolved := make(chan permissionResult, 1)
	go func() {
		got, err := application.Resolve(ctx, prompt)
		resolved <- permissionResult{decision: got, err: err}
	}()

	event := receiveEvent(t, application.Events())
	if event.Kind != app.EventPermissionRequested || event.Permission == nil {
		t.Fatalf("event=%+v", event)
	}
	displayedCallID := event.Permission.Call.Request.CallID
	wantPrompt := prompt
	wantPrompt.Call.Request.CallID = displayedCallID
	if displayedCallID == prompt.Call.Request.CallID || !reflect.DeepEqual(*event.Permission, wantPrompt) {
		t.Fatalf("event=%+v", event)
	}
	application.Commands() <- app.Command{Kind: app.CommandResolvePermission, CallID: displayedCallID, Decision: decision}
	if got := <-resolved; got.err != nil || got.decision != decision {
		t.Fatalf("resolved=%+v", got)
	}

	application.Commands() <- app.Command{Kind: app.CommandResolvePermission, CallID: displayedCallID, Decision: decision}
	if event := <-application.Events(); event.Kind != app.EventRejected || !strings.Contains(event.Message, displayedCallID) {
		t.Fatalf("stale event=%+v", event)
	}
}

func TestAppPermissionBrokerRestoresInternalScopeAfterDisplayedScopeValidation(t *testing.T) {
	const configuredSecret = "permission-internal-scope-secret"
	rawScope := "/workspace/" + configuredSecret
	displayedScope := "/workspace/[REDACTED]"
	application := app.New(app.Options{Redactors: secret.NewBinding(secret.New(configuredSecret))})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	prompt := ports.PermissionPrompt{Call: domain.PreparedToolRequest{
		Request:        domain.ToolRequest{CallID: "scope-restore", Name: "shell"},
		CanonicalScope: displayedScope,
		ApprovalScope:  rawScope,
	}}
	resolved := make(chan permissionResult, 1)
	go func() {
		decision, err := application.Resolve(ctx, prompt)
		resolved <- permissionResult{decision: decision, err: err}
	}()

	event := receiveEvent(t, application.Events())
	if event.Kind != app.EventPermissionRequested || event.Permission == nil {
		t.Fatal("permission request was not published")
	}
	if strings.Contains(event.Permission.Call.ApprovalScope, configuredSecret) || event.Permission.Call.ApprovalScope != displayedScope {
		t.Fatal("published permission did not contain only the displayed scope")
	}
	application.Commands() <- app.Command{
		Kind:   app.CommandResolvePermission,
		CallID: event.Permission.Call.Request.CallID,
		Decision: domain.PermissionDecision{
			Action:   domain.PermissionAllow,
			Lifetime: domain.PermissionOnce,
			Scope:    displayedScope,
		},
	}
	result := <-resolved
	if result.err != nil || result.decision.Scope != rawScope {
		t.Fatal("valid displayed-scope approval did not restore the internal scope")
	}
}

func TestAppPermissionBrokerUsesOpaqueDisplayedCallID(t *testing.T) {
	const configuredSecret = "permission-call-id-secret"
	rawCallID := "provider-" + configuredSecret
	application := app.New(app.Options{Redactors: secret.NewBinding(secret.New(configuredSecret))})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go application.Run(ctx)

	prompt := ports.PermissionPrompt{Call: domain.PreparedToolRequest{
		Request:        domain.ToolRequest{CallID: rawCallID, Name: "shell"},
		CanonicalScope: "/workspace/safe",
	}}
	resolved := make(chan permissionResult, 1)
	go func() {
		decision, err := application.Resolve(ctx, prompt)
		resolved <- permissionResult{decision: decision, err: err}
	}()

	event := receiveEvent(t, application.Events())
	if event.Kind != app.EventPermissionRequested || event.Permission == nil {
		t.Fatal("permission request was not published")
	}
	displayedCallID := event.Permission.Call.Request.CallID
	if displayedCallID == "" || displayedCallID == rawCallID || displayedCallID == "provider-[REDACTED]" || strings.Contains(displayedCallID, configuredSecret) {
		t.Fatal("published permission did not use a safe opaque call ID")
	}
	if prompt.Call.Request.CallID != rawCallID {
		t.Fatal("app mutated the runner-owned internal call ID")
	}
	application.Commands() <- app.Command{
		Kind:   app.CommandResolvePermission,
		CallID: displayedCallID,
		Decision: domain.PermissionDecision{
			Action:   domain.PermissionDeny,
			Lifetime: domain.PermissionOnce,
			Scope:    prompt.Call.CanonicalScope,
		},
	}
	select {
	case result := <-resolved:
		if result.err != nil || result.decision.Action != domain.PermissionDeny {
			t.Fatal("displayed call ID did not resolve the internal pending call")
		}
	case <-time.After(time.Second):
		t.Fatal("displayed call ID stranded Resolve")
	}
}

func TestAppPermissionBrokerRejectsTamperedRawAndStaleCallIDs(t *testing.T) {
	const configuredSecret = "permission-call-id-tamper-secret"
	rawCallID := "provider-" + configuredSecret
	application := app.New(app.Options{Redactors: secret.NewBinding(secret.New(configuredSecret))})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go application.Run(ctx)

	prompt := ports.PermissionPrompt{Call: domain.PreparedToolRequest{
		Request:        domain.ToolRequest{CallID: rawCallID, Name: "shell"},
		CanonicalScope: "/workspace/safe",
	}}
	resolved := make(chan permissionResult, 1)
	go func() {
		decision, err := application.Resolve(ctx, prompt)
		resolved <- permissionResult{decision: decision, err: err}
	}()
	event := receiveEvent(t, application.Events())
	if event.Permission == nil {
		t.Fatal("permission request was not published")
	}
	displayedCallID := event.Permission.Call.Request.CallID

	for _, rejectedCallID := range []string{displayedCallID + "-tampered", rawCallID} {
		application.Commands() <- app.Command{
			Kind:   app.CommandResolvePermission,
			CallID: rejectedCallID,
			Decision: domain.PermissionDecision{
				Action:   domain.PermissionAllow,
				Lifetime: domain.PermissionOnce,
				Scope:    prompt.Call.CanonicalScope,
			},
		}
		rejected := receiveEvent(t, application.Events())
		if rejected.Kind != app.EventRejected || strings.Contains(rejected.Message, configuredSecret) {
			t.Fatal("invalid call ID was not rejected safely")
		}
		select {
		case <-resolved:
			t.Fatal("invalid call ID resolved the pending permission")
		case <-time.After(20 * time.Millisecond):
		}
	}

	application.Commands() <- app.Command{
		Kind:   app.CommandResolvePermission,
		CallID: displayedCallID,
		Decision: domain.PermissionDecision{
			Action:   domain.PermissionDeny,
			Lifetime: domain.PermissionOnce,
			Scope:    prompt.Call.CanonicalScope,
		},
	}
	select {
	case result := <-resolved:
		if result.err != nil {
			t.Fatal("valid displayed call ID could not resolve after rejected IDs")
		}
	case <-time.After(time.Second):
		t.Fatal("valid displayed call ID stranded Resolve")
	}

	replacement := prompt
	replacement.Call.Request.CallID = rawCallID + "-replacement"
	replacementResolved := make(chan permissionResult, 1)
	go func() {
		decision, err := application.Resolve(ctx, replacement)
		replacementResolved <- permissionResult{decision: decision, err: err}
	}()
	replacementEvent := receiveEvent(t, application.Events())
	if replacementEvent.Permission == nil {
		t.Fatal("replacement permission request was not published")
	}
	replacementCallID := replacementEvent.Permission.Call.Request.CallID
	if replacementCallID == displayedCallID {
		t.Fatal("replacement permission reused a stale displayed call ID")
	}
	application.Commands() <- app.Command{
		Kind:     app.CommandResolvePermission,
		CallID:   displayedCallID,
		Decision: domain.PermissionDecision{Action: domain.PermissionDeny, Lifetime: domain.PermissionOnce, Scope: replacement.Call.CanonicalScope},
	}
	if rejected := receiveEvent(t, application.Events()); rejected.Kind != app.EventRejected {
		t.Fatal("stale displayed call ID was not rejected")
	}
	select {
	case <-replacementResolved:
		t.Fatal("stale displayed call ID resolved a later permission")
	case <-time.After(20 * time.Millisecond):
	}
	application.Commands() <- app.Command{
		Kind:     app.CommandResolvePermission,
		CallID:   replacementCallID,
		Decision: domain.PermissionDecision{Action: domain.PermissionDeny, Lifetime: domain.PermissionOnce, Scope: replacement.Call.CanonicalScope},
	}
	select {
	case result := <-replacementResolved:
		if result.err != nil {
			t.Fatal("replacement permission did not resolve with its displayed call ID")
		}
	case <-time.After(time.Second):
		t.Fatal("replacement displayed call ID stranded Resolve")
	}
}

func TestAppPermissionBrokerSeparatesConcurrentCallIDsWithSameRedactedValue(t *testing.T) {
	application := app.New(app.Options{Redactors: secret.NewBinding(secret.New("first-secret", "second-secret")), EventBuffer: 2})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go application.Run(ctx)

	prompts := []ports.PermissionPrompt{
		{Call: domain.PreparedToolRequest{Request: domain.ToolRequest{CallID: "provider-first-secret", Name: "shell"}, CanonicalScope: "/workspace/safe"}},
		{Call: domain.PreparedToolRequest{Request: domain.ToolRequest{CallID: "provider-second-secret", Name: "shell"}, CanonicalScope: "/workspace/safe"}},
	}
	results := []chan permissionResult{make(chan permissionResult, 1), make(chan permissionResult, 1)}
	for index := range prompts {
		index := index
		go func() {
			decision, err := application.Resolve(ctx, prompts[index])
			results[index] <- permissionResult{decision: decision, err: err}
		}()
	}

	displayedCallIDs := make([]string, 0, len(prompts))
	for range prompts {
		event := receiveEvent(t, application.Events())
		if event.Kind != app.EventPermissionRequested || event.Permission == nil {
			t.Fatal("concurrent permission request was not published")
		}
		displayedCallIDs = append(displayedCallIDs, event.Permission.Call.Request.CallID)
	}
	if displayedCallIDs[0] == displayedCallIDs[1] || displayedCallIDs[0] == "provider-[REDACTED]" || displayedCallIDs[1] == "provider-[REDACTED]" {
		t.Fatal("concurrent permissions did not receive distinct opaque call IDs")
	}
	for _, displayedCallID := range displayedCallIDs {
		application.Commands() <- app.Command{
			Kind:   app.CommandResolvePermission,
			CallID: displayedCallID,
			Decision: domain.PermissionDecision{
				Action:   domain.PermissionDeny,
				Lifetime: domain.PermissionOnce,
				Scope:    "/workspace/safe",
			},
		}
	}
	for _, resultChannel := range results {
		select {
		case result := <-resultChannel:
			if result.err != nil {
				t.Fatal("concurrent displayed call ID did not resolve")
			}
		case <-time.After(time.Second):
			t.Fatal("redacted call ID collision stranded Resolve")
		}
	}
}

func TestAppPermissionBrokerRejectsTamperedDisplayedScope(t *testing.T) {
	const configuredSecret = "permission-tamper-secret"
	rawScope := "/workspace/" + configuredSecret
	displayedScope := "/workspace/[REDACTED]"
	application := app.New(app.Options{Redactors: secret.NewBinding(secret.New(configuredSecret))})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	prompt := ports.PermissionPrompt{Call: domain.PreparedToolRequest{
		Request:        domain.ToolRequest{CallID: "scope-tamper", Name: "shell"},
		CanonicalScope: displayedScope,
		ApprovalScope:  rawScope,
	}}
	resolved := make(chan permissionResult, 1)
	go func() {
		decision, err := application.Resolve(ctx, prompt)
		resolved <- permissionResult{decision: decision, err: err}
	}()
	event := receiveEvent(t, application.Events())
	if event.Permission == nil || event.Permission.Call.ApprovalScope != displayedScope {
		t.Fatal("permission request did not expose the expected displayed scope")
	}

	application.Commands() <- app.Command{
		Kind:   app.CommandResolvePermission,
		CallID: event.Permission.Call.Request.CallID,
		Decision: domain.PermissionDecision{
			Action:   domain.PermissionAllow,
			Lifetime: domain.PermissionOnce,
			Scope:    displayedScope + "/tampered",
		},
	}
	if rejected := receiveEvent(t, application.Events()); rejected.Kind != app.EventRejected {
		t.Fatal("tampered displayed scope was not rejected")
	}
	select {
	case <-resolved:
		t.Fatal("tampered displayed scope resolved the pending permission")
	case <-time.After(20 * time.Millisecond):
	}

	application.Commands() <- app.Command{
		Kind:   app.CommandResolvePermission,
		CallID: event.Permission.Call.Request.CallID,
		Decision: domain.PermissionDecision{
			Action:   domain.PermissionDeny,
			Lifetime: domain.PermissionOnce,
			Scope:    displayedScope,
		},
	}
	result := <-resolved
	if result.err != nil || result.decision.Scope != rawScope || result.decision.Action != domain.PermissionDeny {
		t.Fatal("validated follow-up response did not resolve with the internal scope")
	}
}

func TestAppPermissionSanitizationFailureReturnsErrorWithoutPendingLeak(t *testing.T) {
	const configuredSecret = "permission-sanitize-failure-secret"
	application := app.New(app.Options{Redactors: secret.NewBinding(secret.New(configuredSecret))})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	malformed := ports.PermissionPrompt{Call: domain.PreparedToolRequest{
		Request: domain.ToolRequest{
			CallID: "sanitize-failure",
			Name:   "shell",
			Input:  json.RawMessage(`{"value":"permission-sanitize-failure-secret"`),
		},
		ApprovalScope: "/workspace/" + configuredSecret,
	}}
	failed := make(chan permissionResult, 1)
	go func() {
		decision, err := application.Resolve(ctx, malformed)
		failed <- permissionResult{decision: decision, err: err}
	}()

	failureEvent := receiveEvent(t, application.Events())
	if failureEvent.Kind != app.EventError || failureEvent.Permission != nil {
		t.Fatal("sanitization failure published an unresolvable permission event")
	}
	if strings.Contains(failureEvent.Message, configuredSecret) || failureEvent.Err != nil && strings.Contains(failureEvent.Err.Error(), configuredSecret) {
		t.Fatal("sanitization failure event exposed raw content")
	}
	select {
	case result := <-failed:
		if result.err == nil || strings.Contains(result.err.Error(), configuredSecret) {
			t.Fatal("sanitization failure did not return a safe non-nil error")
		}
	case <-time.After(time.Second):
		t.Fatal("sanitization failure stranded Resolve")
	}

	valid := ports.PermissionPrompt{Call: domain.PreparedToolRequest{
		Request:        domain.ToolRequest{CallID: "sanitize-failure", Name: "shell"},
		CanonicalScope: "/workspace/safe",
	}}
	resolved := make(chan permissionResult, 1)
	go func() {
		decision, err := application.Resolve(ctx, valid)
		resolved <- permissionResult{decision: decision, err: err}
	}()
	requestEvent := receiveEvent(t, application.Events())
	if requestEvent.Kind != app.EventPermissionRequested || requestEvent.Permission == nil {
		t.Fatal("sanitization failure left a pending-call leak")
	}
	application.Commands() <- app.Command{
		Kind:   app.CommandResolvePermission,
		CallID: requestEvent.Permission.Call.Request.CallID,
		Decision: domain.PermissionDecision{
			Action:   domain.PermissionDeny,
			Lifetime: domain.PermissionOnce,
			Scope:    valid.Call.CanonicalScope,
		},
	}
	if result := <-resolved; result.err != nil {
		t.Fatal("replacement permission could not resolve after sanitization failure")
	}
}

func TestAppPermissionBrokerRejectsDuplicateCallID(t *testing.T) {
	application := app.New(app.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)
	prompt := ports.PermissionPrompt{Call: domain.PreparedToolRequest{Request: domain.ToolRequest{CallID: "duplicate"}}}

	first := make(chan permissionResult, 1)
	go func() {
		decision, err := application.Resolve(ctx, prompt)
		first <- permissionResult{decision: decision, err: err}
	}()
	if event := <-application.Events(); event.Kind != app.EventPermissionRequested {
		t.Fatalf("event=%+v", event)
	}
	if _, err := application.Resolve(ctx, prompt); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate error=%v", err)
	}

	cancel()
	if got := <-first; !errors.Is(got.err, context.Canceled) {
		t.Fatalf("first error=%v", got.err)
	}
}

func TestAppCancelRemovesPendingPermissionsAndEmitsOneTerminalEvent(t *testing.T) {
	permissionStarted := make(chan struct{})
	var application *app.App
	runtime := &fakeRuntime{run: func(ctx context.Context, _ agent.RunInput) error {
		close(permissionStarted)
		_, err := application.Resolve(ctx, ports.PermissionPrompt{Call: domain.PreparedToolRequest{Request: domain.ToolRequest{CallID: "call-1"}}})
		return err
	}}
	application = app.New(app.Options{RuntimeSet: testRuntimeSet(runtime), EventBuffer: 4})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "one"}
	<-permissionStarted
	requireTurnAccepted(t, application.Events(), "one")
	if event := <-application.Events(); event.Kind != app.EventPermissionRequested {
		t.Fatalf("event=%+v", event)
	}
	application.Commands() <- app.Command{Kind: app.CommandCancelTurn}
	if event := <-application.Events(); event.Kind != app.EventTurnInterrupted {
		t.Fatalf("terminal event=%+v", event)
	}
	application.Commands() <- app.Command{Kind: app.CommandResolvePermission, CallID: "call-1"}
	if event := <-application.Events(); event.Kind != app.EventRejected {
		t.Fatalf("stale event=%+v", event)
	}
	select {
	case event := <-application.Events():
		t.Fatalf("duplicate terminal event=%+v", event)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestAppPermissionBrokerAllowsConcurrentDistinctCalls(t *testing.T) {
	application := app.New(app.Options{EventBuffer: 2})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go application.Run(ctx)

	var wg sync.WaitGroup
	wg.Add(2)
	for _, callID := range []string{"one", "two"} {
		callID := callID
		go func() {
			defer wg.Done()
			_, _ = application.Resolve(ctx, ports.PermissionPrompt{Call: domain.PreparedToolRequest{Request: domain.ToolRequest{CallID: callID}}})
		}()
	}
	for range 2 {
		if event := <-application.Events(); event.Kind != app.EventPermissionRequested {
			t.Fatalf("event=%+v", event)
		}
	}
	cancel()
	wg.Wait()
}

type fakeRuntime struct {
	run func(context.Context, agent.RunInput) error
}

func testRuntimeSet(runtime app.Runtime) app.RuntimeSet {
	return app.RuntimeSet{
		Runtime:        runtime,
		Models:         []domain.ModelSelection{{}},
		Credentials:    map[string]string{"": "configured"},
		CredentialEnvs: map[string]string{"": "TEST_KEY"},
	}
}

type permissionResult struct {
	decision domain.PermissionDecision
	err      error
}

func receiveEvent(t *testing.T, events <-chan app.Event) app.Event {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for app event")
		return app.Event{}
	}
}

func requireTurnAccepted(t *testing.T, events <-chan app.Event, draft string) {
	t.Helper()
	event := receiveEvent(t, events)
	if event.Kind != app.EventTurnAccepted || event.Draft != draft {
		t.Fatalf("turn accepted event=%+v want draft %q", event, draft)
	}
}

func (r *fakeRuntime) RunTurn(ctx context.Context, input agent.RunInput) error {
	return r.run(ctx, input)
}
