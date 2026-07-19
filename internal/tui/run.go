package tui

import (
	"context"
	"errors"
	"fmt"

	tea "charm.land/bubbletea/v2"
	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/cli"
	"github.com/muratmirgun/yordam/internal/config"
)

type applicationRunner interface {
	Run(context.Context) error
}

type programRunner interface {
	Run() (tea.Model, error)
	Quit()
}

var (
	ensureGlobal = config.EnsureGlobal
	bootstrap    = app.Bootstrap
	newProgram   = func(model tea.Model) programRunner { return tea.NewProgram(model) }
)

func Run(ctx context.Context, options cli.Options) error {
	application, model, err := prepareRun(ctx, options)
	if err != nil {
		return err
	}
	return runApplication(ctx, application, newProgram(model))
}

func prepareRun(ctx context.Context, options cli.Options) (*app.App, Model, error) {
	configPath, created, err := ensureGlobal()
	if err != nil {
		return nil, Model{}, fmt.Errorf("ensure config: %w", err)
	}
	application, snapshot, err := bootstrap(ctx, app.BootstrapOptions{ConfigPath: configPath, CLI: options})
	if err != nil {
		return nil, Model{}, err
	}
	model := NewModel(Options{
		Commands:           application.Commands(),
		Events:             application.Events(),
		SessionTitle:       snapshot.Session.Title,
		Mode:               snapshot.Session.Mode,
		Selection:          snapshot.Session.Selection,
		CanonicalWorkspace: snapshot.Workspace.CanonicalPath,
		Sessions:           snapshot.Sessions,
		Models:             snapshot.Models,
	})
	model = model.handleAppEvent(app.Event{
		Kind:      app.EventState,
		Mode:      snapshot.Session.Mode,
		Selection: snapshot.Session.Selection,
		Session:   snapshot.Session,
		Replay:    snapshot.Replay,
		Models:    snapshot.Models,
	})
	if created {
		model = model.handleAppEvent(app.Event{Kind: app.EventNotice, Message: "Created " + configPath + "; edit it and run /reload."})
	}
	if snapshot.ConfigurationError != nil {
		model = model.handleAppEvent(app.Event{
			Kind:        app.EventError,
			Err:         snapshot.ConfigurationError,
			Message:     snapshot.ConfigurationError.Error(),
			NonTerminal: true,
		})
	}
	return application, model, nil
}

func runApplication(ctx context.Context, application applicationRunner, program programRunner) error {
	appContext, cancel := context.WithCancel(ctx)
	defer cancel()
	appDone := make(chan error, 1)
	go func() {
		appDone <- application.Run(appContext)
		program.Quit()
	}()
	_, programErr := program.Run()
	select {
	case appErr := <-appDone:
		cancel()
		return firstSignificantError(appErr, programErr)
	default:
		cancel()
		appErr := <-appDone
		return firstSignificantError(programErr, appErr)
	}
}

func firstSignificantError(errs ...error) error {
	for _, err := range errs {
		if err != nil && !cancellationError(err) {
			return err
		}
	}
	return nil
}

func cancellationError(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, tea.ErrProgramKilled) ||
		errors.Is(err, tea.ErrInterrupted)
}
