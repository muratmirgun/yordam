package tui

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/muratmirgun/yordam/internal/cli"
)

func TestRunCreatesTemplateAndStartsNormalConversation(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	options, err := cli.Parse(nil)
	if err != nil {
		t.Fatal(err)
	}
	options.DataDir = t.TempDir()
	application, model, err := prepareRun(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	if application == nil || model.screen != ScreenConversation {
		t.Fatalf("application=%v screen=%q", application != nil, model.screen)
	}
	path := filepath.Join(home, ".config", "yordam", "config.jsonc")
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
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
