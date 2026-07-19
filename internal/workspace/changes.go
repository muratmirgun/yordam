package workspace

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/tools/output"
)

const NonGitNotice = "Non-Git workspace: shell filesystem changes cannot be exhaustively detected."

func Inspect(ctx context.Context, root string, options output.Options) (*domain.WorkspaceChanges, error) {
	isGit, err := inspectGitWorktree(ctx, root, options)
	if err != nil {
		return nil, err
	}
	if !isGit {
		return &domain.WorkspaceChanges{Notice: NonGitNotice}, nil
	}

	status, err := runGit(ctx, root, options, "status", "--short")
	if err != nil {
		return nil, fmt.Errorf("inspect Git status: %w", err)
	}
	diff, err := runGit(ctx, root, options, "diff", "--no-ext-diff", "--no-color", "--")
	if err != nil {
		return nil, fmt.Errorf("inspect Git diff: %w", err)
	}

	artifacts := append([]string(nil), status.ArtifactIDs...)
	artifacts = append(artifacts, diff.ArtifactIDs...)
	return &domain.WorkspaceChanges{
		IsGit:       true,
		Status:      status.Content,
		Diff:        diff.Content,
		ArtifactIDs: artifacts,
	}, nil
}

func inspectGitWorktree(ctx context.Context, root string, options output.Options) (bool, error) {
	buffer := output.New(options)
	defer buffer.Close()
	command := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "--is-inside-work-tree")
	command.Stdout = buffer
	command.Stderr = buffer
	err := command.Run()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return false, ctxErr
	}
	if err != nil {
		var executableError *exec.Error
		var exitError *exec.ExitError
		if errors.As(err, &executableError) || errors.As(err, &exitError) {
			return false, nil
		}
		return false, err
	}
	text, _ := buffer.Snapshot()
	return strings.TrimSpace(text) == "true", nil
}

func runGit(ctx context.Context, root string, options output.Options, arguments ...string) (domain.ToolResult, error) {
	buffer := output.New(options)
	defer buffer.Close()
	commandArguments := append([]string{"-C", root}, arguments...)
	command := exec.CommandContext(ctx, "git", commandArguments...)
	command.Stdout = buffer
	command.Stderr = buffer
	runErr := command.Run()
	result, resultErr := buffer.Result(context.WithoutCancel(ctx))
	if runErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return result, ctxErr
		}
		return result, fmt.Errorf("git %s: %w: %s", arguments[0], runErr, result.Content)
	}
	if resultErr != nil {
		return result, resultErr
	}
	return result, nil
}
