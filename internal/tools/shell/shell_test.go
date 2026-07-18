package shell_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/tools/output"
	"github.com/muratmirgun/yordam/internal/tools/shell"
	"github.com/muratmirgun/yordam/internal/workspace"
)

func TestShellDescriptorAndPreparation(t *testing.T) {
	root := t.TempDir()
	tool := newTool(t, shell.Options{Workspace: root, ShellPath: "/bin/sh", Timeout: time.Second})
	descriptor := tool.Descriptor()
	if descriptor.Name != "shell" || descriptor.Mutation != domain.MutationProcess {
		t.Fatalf("descriptor=%#v", descriptor)
	}
	if err := descriptor.Validate(); err != nil {
		t.Fatal(err)
	}

	prepared, err := tool.Prepare(context.Background(), request(`{"command":"  echo ok  ","cwd":"."}`, root))
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	preview := prepared.Preview()
	if got, want := preview.CanonicalScope, canonical+"\x00echo ok"; got != want {
		t.Fatalf("scope=%q want=%q", got, want)
	}
	if !preview.InsideWorkspace || preview.Summary != "  echo ok  " || preview.Request.CallID != "call-1" {
		t.Fatalf("preview=%#v", preview)
	}
}

func TestDescriptorPlanningCanonicalizesExecutableWithoutSpawn(t *testing.T) {
	root := t.TempDir()
	link := filepath.Join(root, "shell-link")
	if err := os.Symlink("/bin/sh", link); err != nil {
		t.Fatal(err)
	}
	tool := newTool(t, shell.Options{Workspace: root, ShellPath: link, Timeout: time.Second})
	planned, err := tool.Plan(context.Background(), request(`{"command":"touch should-not-exist","cwd":"."}`, root))
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.EvalSymlinks(link)
	if err != nil {
		t.Fatal(err)
	}
	resources := planned.Preview().Resources
	if len(resources) != 2 || resources[1].CanonicalID != canonical {
		t.Fatalf("resources=%#v want executable %q", resources, canonical)
	}
	if _, err := os.Stat(filepath.Join(root, "should-not-exist")); !os.IsNotExist(err) {
		t.Fatalf("planning spawned shell: %v", err)
	}
}

func TestDescriptorRevalidationRejectsReplacedExecutableIdentity(t *testing.T) {
	root := t.TempDir()
	executable := filepath.Join(root, "tool-shell")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nexec /bin/sh \"$@\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	tool := newTool(t, shell.Options{Workspace: root, ShellPath: executable, Timeout: time.Second})
	planned, err := tool.Plan(context.Background(), request(`{"command":"true","cwd":"."}`, root))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(executable); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	_, err = planned.(ports.ResourceRevalidator).Revalidate(context.Background())
	if err == nil || !strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("revalidation error=%v", err)
	}
}

func TestShellRejectsInvalidInput(t *testing.T) {
	root := t.TempDir()
	tool := newTool(t, shell.Options{Workspace: root, ShellPath: "/bin/sh", Timeout: time.Second})
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "empty command", input: `{"command":"  ","cwd":"."}`, want: "command is required"},
		{name: "empty cwd", input: `{"command":"pwd","cwd":""}`, want: "cwd is required"},
		{name: "unknown field", input: `{"command":"pwd","cwd":".","extra":true}`, want: "unknown field"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := tool.Prepare(context.Background(), request(test.input, root))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Prepare error=%v want containing %q", err, test.want)
			}
		})
	}
}

func TestShellStripsExactProviderKeysAndPreservesOtherEnvironment(t *testing.T) {
	root := t.TempDir()
	t.Setenv("YORDAM_API_KEY", "yordam-secret")
	t.Setenv("PROFILE_KEY", "profile-secret")
	t.Setenv("PROFILE_KEY_SUFFIX", "keep-me")
	t.Setenv("ORDINARY_VALUE", "preserved")
	tool := newTool(t, shell.Options{
		Workspace:       root,
		ShellPath:       "/bin/sh",
		ProviderKeyEnvs: []string{"PROFILE_KEY"},
		Timeout:         2 * time.Second,
	})
	prepared, err := tool.Prepare(context.Background(), request(`{"command":"env","cwd":"."}`, root))
	if err != nil {
		t.Fatal(err)
	}
	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolSucceeded || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("result=%#v", result)
	}
	for _, forbidden := range []string{"YORDAM_API_KEY=", "PROFILE_KEY=", "yordam-secret", "profile-secret"} {
		if strings.Contains(result.Content, forbidden) {
			t.Fatalf("environment secret leaked through %q: %s", forbidden, result.Content)
		}
	}
	for _, preserved := range []string{"PROFILE_KEY_SUFFIX=keep-me", "ORDINARY_VALUE=preserved"} {
		if !strings.Contains(result.Content, preserved) {
			t.Fatalf("environment lost %q: %s", preserved, result.Content)
		}
	}
}

func TestShellCancellationKillsProcessGroup(t *testing.T) {
	root := t.TempDir()
	tool := newTool(t, shell.Options{Workspace: root, ShellPath: "/bin/sh", Timeout: 30 * time.Second})
	prepared, err := tool.Prepare(context.Background(), request(`{"command":"sleep 30 & echo $! > child.pid; wait","cwd":"."}`, root))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan domain.ToolResult, 1)
	started := time.Now()
	go func() { done <- prepared.Execute(ctx) }()
	pid := waitForPID(t, filepath.Join(root, "child.pid"))
	cancel()

	select {
	case result := <-done:
		if result.Status != domain.ToolCancelled || result.ErrorKind != domain.ErrorCancelled || result.ExitCode == nil {
			t.Fatalf("result=%#v", result)
		}
		if result.Duration <= 0 || time.Since(started) > 3*time.Second {
			t.Fatalf("duration=%v wall=%v", result.Duration, time.Since(started))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("process group survived cancellation")
	}
	if err := syscall.Kill(pid, 0); err == nil || err != syscall.ESRCH {
		t.Fatalf("child process %d still exists: %v", pid, err)
	}
}

func TestShellCancellationStillReportsWorkspaceChanges(t *testing.T) {
	root := t.TempDir()
	tool := newTool(t, shell.Options{Workspace: root, ShellPath: "/bin/sh", Timeout: 30 * time.Second})
	prepared, err := tool.Prepare(context.Background(), request(`{"command":"touch changed; echo ready > ready; sleep 30","cwd":"."}`, root))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan domain.ToolResult, 1)
	go func() { done <- prepared.Execute(ctx) }()
	_ = waitForPIDFile(t, filepath.Join(root, "ready"))
	cancel()
	result := <-done
	if result.Status != domain.ToolCancelled || result.WorkspaceChanges == nil || result.WorkspaceChanges.Notice != workspace.NonGitNotice {
		t.Fatalf("result=%#v", result)
	}
}

func TestShellTimeoutHasExactClassification(t *testing.T) {
	root := t.TempDir()
	tool := newTool(t, shell.Options{Workspace: root, ShellPath: "/bin/sh", Timeout: 100 * time.Millisecond})
	prepared, err := tool.Prepare(context.Background(), request(`{"command":"sleep 30","cwd":"."}`, root))
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolFailed || result.ErrorKind != domain.ErrorToolTimeout || result.ExitCode == nil {
		t.Fatalf("result=%#v", result)
	}
	if result.Duration <= 0 || time.Since(started) > 3*time.Second {
		t.Fatalf("duration=%v wall=%v", result.Duration, time.Since(started))
	}
}

func TestShellEmitsRedactedProgressBeforeCompletion(t *testing.T) {
	root := t.TempDir()
	const sentinel = "progress-secret"
	updates := make(chan domain.ToolProgress, 16)
	tool := newTool(t, shell.Options{
		Workspace: root,
		ShellPath: "/bin/sh",
		Timeout:   2 * time.Second,
		Output: output.Options{
			Redact: secret.New(sentinel),
		},
		Progress: func(progress domain.ToolProgress) {
			select {
			case updates <- progress:
			default:
			}
		},
	})
	prepared, err := tool.Prepare(context.Background(), request(`{"command":"printf 'first progress-secret\\n'; sleep 0.25; printf 'second\\n'","cwd":"."}`, root))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan domain.ToolResult, 1)
	go func() { done <- prepared.Execute(context.Background()) }()

	select {
	case progress := <-updates:
		if progress.CallID != "call-1" || !strings.Contains(progress.Text, "first") || strings.Contains(progress.Text, sentinel) {
			t.Fatalf("progress=%#v", progress)
		}
	case result := <-done:
		t.Fatalf("shell completed before live progress: %#v", result)
	case <-time.After(time.Second):
		t.Fatal("no live progress received")
	}

	result := <-done
	if result.Status != domain.ToolSucceeded || strings.Contains(result.Content, sentinel) {
		t.Fatalf("result=%#v", result)
	}
	close(updates)
	for progress := range updates {
		if strings.Contains(progress.Text, sentinel) || len(progress.Text) > output.ModelExcerptBytes {
			t.Fatalf("unsafe progress=%#v", progress)
		}
	}
}

func TestShellRejectsCWDSymlinkSwapBeforeLaunch(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	inside := filepath.Join(root, "cwd")
	if err := os.Mkdir(inside, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(inside, link); err != nil {
		t.Fatal(err)
	}
	tool := newTool(t, shell.Options{Workspace: root, ShellPath: "/bin/sh", Timeout: time.Second})
	prepared, err := tool.Prepare(context.Background(), request(`{"command":"touch launched","cwd":"link"}`, root))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}

	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolFailed || result.ErrorKind != domain.ErrorToolFailed || result.ExitCode != nil || !strings.Contains(result.Content, "canonical scope changed") {
		t.Fatalf("result=%#v", result)
	}
	if _, err := os.Stat(filepath.Join(outside, "launched")); !os.IsNotExist(err) {
		t.Fatalf("shell launched after cwd swap: %v", err)
	}
}

func TestShellReturnsCombinedBoundedOutputAndWorkspaceChanges(t *testing.T) {
	root := t.TempDir()
	store := &artifactStore{}
	tool := newTool(t, shell.Options{
		Workspace: root,
		ShellPath: "/bin/sh",
		Timeout:   3 * time.Second,
		Output:    output.Options{Artifacts: store},
	})
	prepared, err := tool.Prepare(context.Background(), request(`{"command":"printf stdout; printf stderr >&2; yes x | head -c 10486000","cwd":"."}`, root))
	if err != nil {
		t.Fatal(err)
	}
	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolSucceeded || result.ExitCode == nil || *result.ExitCode != 0 || !result.Truncated {
		t.Fatalf("result status=%s error=%s exit=%v truncated=%v content bytes=%d", result.Status, result.ErrorKind, result.ExitCode, result.Truncated, len(result.Content))
	}
	if len(result.Content) != output.ModelExcerptBytes || len(result.ArtifactIDs) != 1 || !strings.Contains(result.Content, "x") {
		t.Fatalf("result content bytes=%d artifacts=%v", len(result.Content), result.ArtifactIDs)
	}
	if result.WorkspaceChanges == nil || result.WorkspaceChanges.IsGit || result.WorkspaceChanges.Notice != workspace.NonGitNotice {
		t.Fatalf("workspace changes=%#v", result.WorkspaceChanges)
	}
	if result.Duration <= 0 {
		t.Fatalf("duration=%v", result.Duration)
	}
}

func TestShellUsesExecutableFallback(t *testing.T) {
	root := t.TempDir()
	t.Setenv("SHELL", filepath.Join(root, "not-executable"))
	if err := os.WriteFile(os.Getenv("SHELL"), []byte("not a shell"), 0o600); err != nil {
		t.Fatal(err)
	}
	tool := newTool(t, shell.Options{Workspace: root, Timeout: time.Second})
	prepared, err := tool.Prepare(context.Background(), request(`{"command":"printf fallback","cwd":"."}`, root))
	if err != nil {
		t.Fatal(err)
	}
	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolSucceeded || result.Content != "fallback" {
		t.Fatalf("result=%#v", result)
	}
}

func newTool(t *testing.T, options shell.Options) *shell.Tool {
	t.Helper()
	if options.Output.SessionID == "" {
		options.Output.SessionID = "session-1"
	}
	if options.Output.Artifacts == nil {
		options.Output.Artifacts = &artifactStore{}
	}
	return shell.New(options)
}

func request(input, workspacePath string) domain.ToolRequest {
	return domain.ToolRequest{CallID: "call-1", Name: "shell", Input: json.RawMessage(input), Workspace: workspacePath}
}

func waitForPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil {
			pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
			if err == nil {
				return pid
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
	return 0
}

func waitForPIDFile(t *testing.T, path string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil {
			return string(raw)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
	return ""
}

type artifactStore struct {
	mu        sync.Mutex
	artifacts [][]byte
}

func (s *artifactStore) Put(_ context.Context, sessionID, mediaType string, source io.Reader, limit int64) (domain.Artifact, error) {
	raw, err := io.ReadAll(io.LimitReader(source, limit+1))
	if err != nil {
		return domain.Artifact{}, err
	}
	if int64(len(raw)) > limit {
		return domain.Artifact{}, fmt.Errorf("artifact exceeded limit")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.artifacts = append(s.artifacts, bytes.Clone(raw))
	return domain.Artifact{ID: fmt.Sprintf("artifact-%d", len(s.artifacts)), SessionID: sessionID, MediaType: mediaType, Size: int64(len(raw))}, nil
}

func (*artifactStore) Open(context.Context, domain.Artifact) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}
