package app_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/secret"
)

func TestAppRejectsDraftBeforeTurnWhenRuntimeIsNotReady(t *testing.T) {
	runs := 0
	selection := domain.ModelSelection{Profile: "primary", Model: "a"}
	replay := testSession()
	replay.Session.Selection = selection
	store := newStateStore(replay)
	application := app.New(app.Options{
		RuntimeSet: app.RuntimeSet{
			Runtime:            &fakeRuntime{run: func(context.Context, agent.RunInput) error { runs++; return nil }},
			Models:             []domain.ModelSelection{selection},
			CredentialEnvs:     map[string]string{"primary": "PRIMARY_KEY"},
			Credentials:        map[string]string{"primary": ""},
			DefaultSelection:   selection,
			ConfigurationError: &domain.TypedError{Kind: domain.ErrorConfigurationInvalid, Message: "config missing"},
		},
		Sessions: store,
		Session:  replay.Session,
		Replay:   replay,
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "keep this draft"}
	event := receiveEvent(t, application.Events())
	if event.Kind != app.EventError || event.Draft != "keep this draft" || runs != 0 {
		t.Fatalf("event=%+v runs=%d", event, runs)
	}
	if got := store.appendKinds(); len(got) != 0 {
		t.Fatalf("not-ready turn persisted events=%v", got)
	}
}

func TestAppRejectedTurnReturnsDraft(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	application := app.New(app.Options{RuntimeSet: testRuntimeSet(&fakeRuntime{run: func(context.Context, agent.RunInput) error {
		close(started)
		<-release
		return nil
	}})})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go application.Run(ctx)
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "active"}
	<-started
	requireTurnAccepted(t, application.Events(), "active")
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "retry this"}
	event := receiveEvent(t, application.Events())
	if event.Kind != app.EventRejected || event.Draft != "retry this" {
		t.Fatalf("rejection=%+v", event)
	}
	close(release)
	_ = receiveEvent(t, application.Events())
}

func TestAppReloadSwapsRuntimeOnlyAfterSuccessfulCandidate(t *testing.T) {
	selection := domain.ModelSelection{Profile: "primary", Model: "a"}
	oldRuns := make(chan struct{}, 1)
	newRuns := make(chan struct{}, 1)
	redactors := secret.NewBinding(secret.New("old-secret"))
	oldSet := readyRuntimeSet(selection, &fakeRuntime{run: func(context.Context, agent.RunInput) error { oldRuns <- struct{}{}; return nil }}, "old-secret")
	newSet := readyRuntimeSet(selection, &fakeRuntime{run: func(context.Context, agent.RunInput) error { newRuns <- struct{}{}; return nil }}, "new-secret")
	application := app.New(app.Options{
		RuntimeSet: oldSet,
		Redactors:  redactors,
		ReloadRuntime: func(context.Context, domain.ModelSelection) (app.RuntimeSet, error) {
			return newSet, nil
		},
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandReloadConfig}
	event := receiveEvent(t, application.Events())
	if event.Kind != app.EventReloadCompleted || !event.Applied {
		t.Fatalf("reload event=%+v", event)
	}
	if got := redactors.String("old-secret new-secret"); got != "old-secret [REDACTED]" {
		t.Fatalf("active redactor=%q", got)
	}
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "after reload"}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventTurnAccepted {
		t.Fatalf("accepted event=%+v", event)
	}
	select {
	case <-newRuns:
	case <-time.After(time.Second):
		t.Fatal("new runtime not used")
	}
	_ = receiveEvent(t, application.Events())
	select {
	case <-oldRuns:
		t.Fatal("old runtime used after reload")
	default:
	}
}

func TestAppFailedReloadPreservesRuntimeAndRedactor(t *testing.T) {
	selection := domain.ModelSelection{Profile: "primary", Model: "a"}
	runs := make(chan struct{}, 1)
	redactors := secret.NewBinding(secret.New("old-secret"))
	application := app.New(app.Options{
		RuntimeSet: readyRuntimeSet(selection, &fakeRuntime{run: func(context.Context, agent.RunInput) error { runs <- struct{}{}; return nil }}, "old-secret"),
		Redactors:  redactors,
		Session:    domain.Session{Selection: selection},
		ReloadRuntime: func(context.Context, domain.ModelSelection) (app.RuntimeSet, error) {
			return app.RuntimeSet{}, errors.New("invalid JSONC")
		},
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandReloadConfig}
	event := receiveEvent(t, application.Events())
	if event.Kind != app.EventReloadCompleted || event.Applied || event.Err == nil {
		t.Fatalf("reload event=%+v", event)
	}
	if got := redactors.String("old-secret"); got != "[REDACTED]" {
		t.Fatalf("redactor changed=%q", got)
	}
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "still works"}
	_ = receiveEvent(t, application.Events())
	select {
	case <-runs:
	case <-time.After(time.Second):
		t.Fatal("old runtime not preserved")
	}
}

func TestAppRejectsReloadWhileTurnIsActive(t *testing.T) {
	selection := domain.ModelSelection{Profile: "primary", Model: "a"}
	started := make(chan struct{})
	release := make(chan struct{})
	application := app.New(app.Options{
		RuntimeSet: readyRuntimeSet(selection, &fakeRuntime{run: func(context.Context, agent.RunInput) error {
			close(started)
			<-release
			return nil
		}}, "secret"),
		Session: domain.Session{Selection: selection},
		ReloadRuntime: func(context.Context, domain.ModelSelection) (app.RuntimeSet, error) {
			t.Fatal("reload ran during active turn")
			return app.RuntimeSet{}, nil
		},
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go application.Run(ctx)
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "active"}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventTurnAccepted {
		t.Fatalf("accepted=%+v", event)
	}
	<-started
	application.Commands() <- app.Command{Kind: app.CommandReloadConfig}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventRejected || event.Message != "an operation is already active" {
		t.Fatalf("reload rejection=%+v", event)
	}
	close(release)
	_ = receiveEvent(t, application.Events())
}

func TestAppRejectsReloadWhileCompactionIsActive(t *testing.T) {
	selection := domain.ModelSelection{Profile: "primary", Model: "a"}
	started := make(chan struct{})
	release := make(chan struct{})
	set := readyRuntimeSet(selection, &fakeRuntime{run: func(context.Context, agent.RunInput) error { return nil }}, "secret")
	set.CompactSession = func(context.Context, domain.Session, domain.SessionReplay) error {
		close(started)
		<-release
		return nil
	}
	application := app.New(app.Options{
		RuntimeSet: set,
		Session:    domain.Session{Selection: selection},
		ReloadRuntime: func(context.Context, domain.ModelSelection) (app.RuntimeSet, error) {
			t.Fatal("reload ran during active compaction")
			return app.RuntimeSet{}, nil
		},
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandCompact}
	<-started
	application.Commands() <- app.Command{Kind: app.CommandReloadConfig}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventRejected || event.Message != "an operation is already active" {
		t.Fatalf("reload rejection=%+v", event)
	}
	close(release)
	_ = receiveEvent(t, application.Events())
}

func TestAppReloadPersistsFallbackBeforeActivation(t *testing.T) {
	oldSelection := domain.ModelSelection{Profile: "old", Model: "a"}
	newSelection := domain.ModelSelection{Profile: "new", Model: "b"}
	replay := testSession()
	replay.Session.Selection = oldSelection
	store := newStateStore(replay)
	application := app.New(app.Options{
		RuntimeSet: readyRuntimeSet(oldSelection, &fakeRuntime{run: func(context.Context, agent.RunInput) error { return nil }}, "old-secret"),
		Session:    replay.Session,
		Replay:     replay,
		Sessions:   store,
		ReloadRuntime: func(context.Context, domain.ModelSelection) (app.RuntimeSet, error) {
			return readyRuntimeSet(newSelection, &fakeRuntime{run: func(context.Context, agent.RunInput) error { return nil }}, "new-secret"), nil
		},
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go application.Run(ctx)
	application.Commands() <- app.Command{Kind: app.CommandReloadConfig}
	event := receiveEvent(t, application.Events())
	if event.Kind != app.EventReloadCompleted || !event.Applied || event.Selection != newSelection {
		t.Fatalf("reload=%+v", event)
	}
	if got := store.appendKinds(); len(got) != 1 || got[0] != domain.EventModelChanged {
		t.Fatalf("append kinds=%v", got)
	}
}

func TestAppReloadFallbackFailurePreservesOldGeneration(t *testing.T) {
	oldSelection := domain.ModelSelection{Profile: "old", Model: "a"}
	newSelection := domain.ModelSelection{Profile: "new", Model: "b"}
	replay := testSession()
	replay.Session.Selection = oldSelection
	store := newStateStore(replay)
	store.setAppendError(domain.EventModelChanged, errors.New("disk full"))
	oldRuns := make(chan struct{}, 1)
	newRuns := make(chan struct{}, 1)
	redactors := secret.NewBinding(secret.New("old-secret"))
	application := app.New(app.Options{
		RuntimeSet: readyRuntimeSet(oldSelection, &fakeRuntime{run: func(context.Context, agent.RunInput) error {
			oldRuns <- struct{}{}
			return nil
		}}, "old-secret"),
		Redactors: redactors,
		Session:   replay.Session,
		Replay:    replay,
		Sessions:  store,
		ReloadRuntime: func(context.Context, domain.ModelSelection) (app.RuntimeSet, error) {
			return readyRuntimeSet(newSelection, &fakeRuntime{run: func(context.Context, agent.RunInput) error {
				newRuns <- struct{}{}
				return nil
			}}, "new-secret"), nil
		},
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandReloadConfig}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventReloadCompleted || event.Applied || !strings.Contains(event.Message, "disk full") {
		t.Fatalf("reload=%+v", event)
	}
	if got := redactors.String("old-secret new-secret"); got != "[REDACTED] new-secret" {
		t.Fatalf("redactor changed=%q", got)
	}
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "old remains"}
	requireTurnAccepted(t, application.Events(), "old remains")
	select {
	case <-oldRuns:
	case <-time.After(time.Second):
		t.Fatal("old runtime not retained")
	}
	_ = receiveEvent(t, application.Events())
	select {
	case <-newRuns:
		t.Fatal("new runtime activated before fallback persisted")
	default:
	}
}

func TestAppRejectsModelChangeWhileReloadIsActive(t *testing.T) {
	selectionA := domain.ModelSelection{Profile: "primary", Model: "a"}
	selectionB := domain.ModelSelection{Profile: "primary", Model: "b"}
	replay := testSession()
	replay.Session.Selection = selectionA
	store := newStateStore(replay)
	started := make(chan struct{})
	release := make(chan struct{})
	candidate := readyRuntimeSet(selectionA, &fakeRuntime{run: func(context.Context, agent.RunInput) error { return nil }}, "new-secret")
	candidate.Models = []domain.ModelSelection{selectionA, selectionB}
	application := app.New(app.Options{
		RuntimeSet: candidate,
		Session:    replay.Session,
		Replay:     replay,
		Sessions:   store,
		ReloadRuntime: func(context.Context, domain.ModelSelection) (app.RuntimeSet, error) {
			close(started)
			<-release
			return candidate, nil
		},
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandReloadConfig}
	<-started
	application.Commands() <- app.Command{Kind: app.CommandChangeModel, Selection: selectionB}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventRejected || event.Message != "an operation is already active" {
		t.Fatalf("model rejection=%+v", event)
	}
	if got := store.appendKinds(); len(got) != 0 {
		t.Fatalf("model persisted during reload: %v", got)
	}
	close(release)
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventReloadCompleted || event.Selection != selectionA {
		t.Fatalf("reload=%+v", event)
	}
}

func TestAppMissingCredentialReloadReportsAppliedAndLoaded(t *testing.T) {
	selection := domain.ModelSelection{Profile: "primary", Model: "a"}
	candidate := readyRuntimeSet(selection, &fakeRuntime{run: func(context.Context, agent.RunInput) error { return nil }}, "")
	application := app.New(app.Options{
		RuntimeSet: candidate,
		Session:    domain.Session{Selection: selection},
		ReloadRuntime: func(context.Context, domain.ModelSelection) (app.RuntimeSet, error) {
			return candidate, nil
		},
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandReloadConfig}
	event := receiveEvent(t, application.Events())
	if event.Kind != app.EventReloadCompleted || !event.Applied || event.Err == nil || !event.NonTerminal || event.Message != "configuration reloaded" {
		t.Fatalf("reload=%+v", event)
	}
}

func TestAppMissingCredentialReloadRejectsNextDraftBeforePersistence(t *testing.T) {
	selection := domain.ModelSelection{Profile: "primary", Model: "a"}
	replay := testSession()
	replay.Session.Selection = selection
	store := newStateStore(replay)
	runs := 0
	ready := readyRuntimeSet(selection, &fakeRuntime{run: func(context.Context, agent.RunInput) error { return nil }}, "old-secret")
	missing := readyRuntimeSet(selection, &fakeRuntime{run: func(context.Context, agent.RunInput) error {
		runs++
		return nil
	}}, "")
	application := app.New(app.Options{
		RuntimeSet: ready,
		Session:    replay.Session,
		Replay:     replay,
		Sessions:   store,
		ReloadRuntime: func(context.Context, domain.ModelSelection) (app.RuntimeSet, error) {
			return missing, nil
		},
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandReloadConfig}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventReloadCompleted || !event.Applied || event.Err == nil {
		t.Fatalf("reload=%+v", event)
	}
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "preserve me"}
	event := receiveEvent(t, application.Events())
	if event.Kind != app.EventError || event.Draft != "preserve me" || runs != 0 {
		t.Fatalf("turn=%+v runs=%d", event, runs)
	}
	if got := store.appendKinds(); len(got) != 0 {
		t.Fatalf("not-ready turn persisted events=%v", got)
	}
}

func TestAppTurnGenerationStaysStableUntilReloadAfterCompletion(t *testing.T) {
	selection := domain.ModelSelection{Profile: "primary", Model: "a"}
	aStarted := make(chan struct{})
	aRelease := make(chan struct{})
	bRuns := make(chan struct{}, 1)
	setA := readyRuntimeSet(selection, &fakeRuntime{run: func(context.Context, agent.RunInput) error {
		close(aStarted)
		<-aRelease
		return nil
	}}, "a-secret")
	setB := readyRuntimeSet(selection, &fakeRuntime{run: func(context.Context, agent.RunInput) error {
		bRuns <- struct{}{}
		return nil
	}}, "b-secret")
	application := app.New(app.Options{
		RuntimeSet: setA,
		Session:    domain.Session{Selection: selection},
		ReloadRuntime: func(context.Context, domain.ModelSelection) (app.RuntimeSet, error) {
			return setB, nil
		},
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go application.Run(ctx)

	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "generation a"}
	requireTurnAccepted(t, application.Events(), "generation a")
	<-aStarted
	application.Commands() <- app.Command{Kind: app.CommandReloadConfig}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventRejected {
		t.Fatalf("reload during generation A=%+v", event)
	}
	close(aRelease)
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventTurnCompleted {
		t.Fatalf("generation A terminal=%+v", event)
	}
	application.Commands() <- app.Command{Kind: app.CommandReloadConfig}
	if event := receiveEvent(t, application.Events()); event.Kind != app.EventReloadCompleted || !event.Applied {
		t.Fatalf("reload after A=%+v", event)
	}
	application.Commands() <- app.Command{Kind: app.CommandStartTurn, Prompt: "generation b"}
	requireTurnAccepted(t, application.Events(), "generation b")
	select {
	case <-bRuns:
	case <-time.After(time.Second):
		t.Fatal("generation B runtime not used")
	}
	_ = receiveEvent(t, application.Events())
}

func readyRuntimeSet(selection domain.ModelSelection, runtime app.Runtime, value string) app.RuntimeSet {
	return app.RuntimeSet{
		Runtime:          runtime,
		Models:           []domain.ModelSelection{selection},
		DefaultSelection: selection,
		CredentialEnvs:   map[string]string{selection.Profile: "PRIMARY_KEY"},
		Credentials:      map[string]string{selection.Profile: value},
		Redactor:         secret.New(value),
	}
}
