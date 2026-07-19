package tui

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/cli"
	"github.com/muratmirgun/yordam/internal/domain"
)

func TestRunCreatesTemplateAndStartsNormalConversation(t *testing.T) {
	originalEnsureGlobal, originalBootstrap, originalNewProgram := ensureGlobal, bootstrap, newProgram
	t.Cleanup(func() {
		ensureGlobal, bootstrap, newProgram = originalEnsureGlobal, originalBootstrap, originalNewProgram
	})

	configPath := "/isolated/config.jsonc"
	ensureGlobal = func() (string, bool, error) { return configPath, true, nil }
	bootstrapCalled := false
	bootstrap = func(_ context.Context, options app.BootstrapOptions) (*app.App, app.Snapshot, error) {
		bootstrapCalled = true
		if options.ConfigPath != configPath {
			t.Fatalf("config path=%q want=%q", options.ConfigPath, configPath)
		}
		selection := domain.ModelSelection{Profile: "default", Model: "model"}
		application := app.New(app.Options{})
		return application, app.Snapshot{
			Workspace:          domain.Workspace{CanonicalPath: "/workspace"},
			Session:            domain.Session{ID: "session", Mode: domain.ModeAsk, Selection: selection},
			Models:             []domain.ModelSelection{selection},
			ConfigurationError: errors.New("missing TEST_KEY"),
		}, nil
	}
	var startedModel Model
	newProgram = func(model tea.Model) programRunner {
		var ok bool
		startedModel, ok = model.(Model)
		if !ok {
			t.Fatalf("program model type=%T", model)
		}
		program := newRunTestProgram()
		program.run = func() (tea.Model, error) { return model, nil }
		return program
	}
	options, err := cli.Parse(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := Run(t.Context(), options); err != nil {
		t.Fatal(err)
	}
	if !bootstrapCalled || startedModel.screen != ScreenConversation {
		t.Fatalf("bootstrap=%t screen=%q", bootstrapCalled, startedModel.screen)
	}
	rendered := startedModel.conversation.View()
	if !strings.Contains(rendered, configPath) || !strings.Contains(rendered, "missing TEST_KEY") {
		t.Fatalf("first-launch notices=%q", rendered)
	}
}

func TestRunApplicationReturnsAppErrorAndStopsProgram(t *testing.T) {
	appErr := errors.New("app failed")
	application := &runTestApp{run: func(context.Context) error { return appErr }, started: make(chan struct{})}
	program := newRunTestProgram()

	if err := runApplication(t.Context(), application, program); !errors.Is(err, appErr) {
		t.Fatalf("error=%v", err)
	}
	if !program.quitCalled() {
		t.Fatal("program was not stopped after app failure")
	}
}

func TestRunApplicationReturnsProgramErrorAndWaitsForAppCancellation(t *testing.T) {
	programErr := errors.New("program failed")
	appStopped := make(chan struct{})
	application := &runTestApp{run: func(ctx context.Context) error {
		<-ctx.Done()
		close(appStopped)
		return ctx.Err()
	}, started: make(chan struct{})}
	program := newRunTestProgram()
	program.run = func() (tea.Model, error) {
		<-application.started
		return nil, programErr
	}

	if err := runApplication(t.Context(), application, program); !errors.Is(err, programErr) {
		t.Fatalf("error=%v", err)
	}
	select {
	case <-appStopped:
	default:
		t.Fatal("runApplication returned before app stopped")
	}
}

type runTestApp struct {
	run     func(context.Context) error
	started chan struct{}
}

func (a *runTestApp) Run(ctx context.Context) error {
	close(a.started)
	return a.run(ctx)
}

type runTestProgram struct {
	run      func() (tea.Model, error)
	quit     chan struct{}
	quitOnce sync.Once
}

func newRunTestProgram() *runTestProgram {
	program := &runTestProgram{quit: make(chan struct{})}
	program.run = func() (tea.Model, error) {
		<-program.quit
		return nil, nil
	}
	return program
}

func (p *runTestProgram) Run() (tea.Model, error) { return p.run() }

func (p *runTestProgram) Quit() {
	p.quitOnce.Do(func() { close(p.quit) })
}

func (p *runTestProgram) quitCalled() bool {
	select {
	case <-p.quit:
		return true
	default:
		return false
	}
}
