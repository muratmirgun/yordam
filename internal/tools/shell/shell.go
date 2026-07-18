package shell

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/safefile"
	"github.com/muratmirgun/yordam/internal/scope"
	toolset "github.com/muratmirgun/yordam/internal/tools"
	"github.com/muratmirgun/yordam/internal/tools/output"
	"github.com/muratmirgun/yordam/internal/workspace"
)

var inputSchema = json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{"command":{"type":"string","minLength":1},"cwd":{"type":"string","minLength":1}},"required":["command","cwd"]}`)

type Input struct {
	Command string `json:"command"`
	CWD     string `json:"cwd"`
}

type Options struct {
	Workspace       string
	ShellPath       string
	ProviderKeyEnvs []string
	Timeout         time.Duration
	Output          output.Options
	Progress        func(domain.ToolProgress)
}

type Tool struct {
	workspace       string
	shellPath       string
	providerKeyEnvs []string
	timeout         time.Duration
	output          output.Options
	progress        func(domain.ToolProgress)
}

func New(options Options) *Tool {
	if options.Output.Artifacts == nil {
		panic("shell: artifact store is required")
	}
	if options.Timeout <= 0 {
		panic("shell: timeout must be positive")
	}
	return &Tool{
		workspace:       options.Workspace,
		shellPath:       resolveShellPath(options.ShellPath),
		providerKeyEnvs: append([]string(nil), options.ProviderKeyEnvs...),
		timeout:         options.Timeout,
		output:          options.Output,
		progress:        options.Progress,
	}
}

func (t *Tool) Descriptor() domain.ToolDescriptor {
	return domain.ToolDescriptor{
		Name:             "shell",
		Description:      "Run a trusted, unsandboxed shell command in a selected working directory.",
		ScopeDescription: "Canonical working directory plus normalized command.",
		InputSchema:      inputSchema,
		Mutation:         domain.MutationProcess,
	}
}

func (t *Tool) CanonicalDescriptor() protocol.ToolDescriptor {
	return toolset.BuiltinCanonicalDescriptor(t.Descriptor(), toolset.ShellClassification())
}

func (*Tool) TrustedClassification() domain.ToolClassification { return toolset.ShellClassification() }

func (t *Tool) Plan(ctx context.Context, request domain.ToolRequest) (ports.PreparedTool, error) {
	var input Input
	if err := decodeStrict(request.Input, &input); err != nil {
		return nil, fmt.Errorf("shell input: %w", err)
	}
	normalizedCommand := strings.TrimSpace(input.Command)
	if normalizedCommand == "" {
		return nil, fmt.Errorf("shell input: command is required")
	}
	if input.CWD == "" {
		return nil, fmt.Errorf("shell input: cwd is required")
	}
	cwd, err := scope.Resolve(t.workspace, input.CWD, false)
	if err != nil {
		return nil, fmt.Errorf("resolve shell cwd: %w", err)
	}
	cwdIdentity, err := anchoredDirectoryIdentity(ctx, cwd.Path)
	if err != nil {
		return nil, fmt.Errorf("stat shell cwd: %w", err)
	}
	shellPath, err := filepath.EvalSymlinks(t.shellPath)
	if err != nil {
		return nil, fmt.Errorf("canonicalize shell executable: %w", err)
	}
	executableIdentity, err := os.Stat(shellPath)
	if err != nil {
		return nil, fmt.Errorf("stat shell executable: %w", err)
	}
	return &prepared{
		request:            request,
		input:              input,
		normalizedCommand:  normalizedCommand,
		cwd:                cwd,
		cwdIdentity:        cwdIdentity,
		workspace:          t.workspace,
		shellPath:          shellPath,
		executableIdentity: executableIdentity,
		providerKeyEnvs:    append([]string(nil), t.providerKeyEnvs...),
		timeout:            t.timeout,
		output:             t.output,
		progress:           t.progress,
	}, nil
}

func (t *Tool) Prepare(ctx context.Context, request domain.ToolRequest) (ports.PreparedTool, error) {
	planned, err := t.Plan(ctx, request)
	if err != nil {
		return nil, err
	}
	prepared := planned.(*prepared)
	if _, err := prepared.Revalidate(ctx); err != nil {
		return nil, err
	}
	return prepared, nil
}

type prepared struct {
	request            domain.ToolRequest
	input              Input
	normalizedCommand  string
	cwd                scope.Resolved
	cwdIdentity        os.FileInfo
	workspace          string
	shellPath          string
	executableIdentity os.FileInfo
	providerKeyEnvs    []string
	timeout            time.Duration
	output             output.Options
	progress           func(domain.ToolProgress)
}

func (p *prepared) Preview() domain.PreparedToolRequest {
	return domain.PreparedToolRequest{
		Request:         p.request,
		Mutation:        domain.MutationProcess,
		CanonicalScope:  p.cwd.Path + "\x00" + p.normalizedCommand,
		InsideWorkspace: p.cwd.Inside,
		Summary:         p.input.Command,
		Resources: []protocol.ResourceTarget{
			{Kind: "directory", CanonicalID: p.cwd.Path, Attributes: []protocol.ResourceAttribute{{Name: "command", Value: p.normalizedCommand}}},
			{Kind: "executable", CanonicalID: p.shellPath},
		},
	}
}

func (p *prepared) Revalidate(ctx context.Context) (domain.PreparedToolRequest, error) {
	if _, err := p.validateCurrentIdentities(ctx); err != nil {
		return domain.PreparedToolRequest{}, err
	}
	return p.Preview(), nil
}

func (p *prepared) validateCurrentIdentities(ctx context.Context) (scope.Resolved, error) {
	current, err := scope.Resolve(p.workspace, p.input.CWD, false)
	if err != nil {
		return scope.Resolved{}, fmt.Errorf("re-resolve shell cwd: %w", err)
	}
	if current.Path != p.cwd.Path || current.Inside != p.cwd.Inside {
		return scope.Resolved{}, fmt.Errorf("canonical scope changed from %q to %q", p.cwd.Path, current.Path)
	}
	info, err := anchoredDirectoryIdentity(ctx, current.Path)
	if err != nil {
		return scope.Resolved{}, fmt.Errorf("stat shell cwd: %w", err)
	}
	if p.cwdIdentity != nil && !os.SameFile(p.cwdIdentity, info) {
		return scope.Resolved{}, fmt.Errorf("shell cwd identity changed after planning: %s", current.Path)
	}
	executable, err := exec.LookPath(p.shellPath)
	if err != nil {
		return scope.Resolved{}, fmt.Errorf("resolve shell executable: %w", err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		return scope.Resolved{}, fmt.Errorf("canonicalize shell executable: %w", err)
	}
	executableIdentity, err := os.Stat(executable)
	if err != nil {
		return scope.Resolved{}, fmt.Errorf("stat shell executable: %w", err)
	}
	if executable != p.shellPath || p.executableIdentity != nil && !os.SameFile(p.executableIdentity, executableIdentity) {
		return scope.Resolved{}, fmt.Errorf("shell executable identity changed after planning: %s", executable)
	}
	return current, nil
}

func (p *prepared) Execute(ctx context.Context) domain.ToolResult {
	started := time.Now()
	buffer := output.New(p.output)
	defer buffer.Close()
	if err := ctx.Err(); err != nil {
		return p.contextResult(ctx, started, buffer, nil)
	}

	runContext, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	command := exec.CommandContext(runContext, p.shellPath, "-lc", p.input.Command)
	configureProcess(command)
	command.Cancel = func() error {
		terminateProcessGroup(command)
		return nil
	}
	command.WaitDelay = 3 * time.Second
	command.Stdout = buffer
	command.Stderr = buffer
	command.Env = strippedEnvironment(os.Environ(), p.providerKeyEnvs)

	current, err := p.validateCurrentIdentities(runContext)
	if err != nil {
		return p.failedBeforeStart(started, buffer, err)
	}
	command.Dir = current.Path
	if err := command.Start(); err != nil {
		if runContext.Err() != nil {
			return p.contextResult(runContext, started, buffer, nil)
		}
		return p.failedBeforeStart(started, buffer, fmt.Errorf("start shell: %w", err))
	}

	progressDone := make(chan struct{})
	progressFinished := make(chan struct{})
	go p.reportProgress(buffer, progressDone, progressFinished)
	waitErr := command.Wait()
	close(progressDone)
	<-progressFinished

	inspectionContext, inspectionCancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	changes, changesErr := workspace.Inspect(inspectionContext, p.cwd.Workspace, p.output)
	inspectionCancel()
	result, resultErr := buffer.Result(context.WithoutCancel(ctx))
	result.CallID = p.request.CallID
	result.ExitCode = exitCode(command)
	result.WorkspaceChanges = changes

	switch {
	case errors.Is(runContext.Err(), context.DeadlineExceeded):
		result.Status = domain.ToolFailed
		result.ErrorKind = domain.ErrorToolTimeout
	case errors.Is(runContext.Err(), context.Canceled):
		result.Status = domain.ToolCancelled
		result.ErrorKind = domain.ErrorCancelled
	case waitErr != nil:
		result.Status = domain.ToolFailed
		result.ErrorKind = domain.ErrorToolFailed
	case resultErr != nil || changesErr != nil:
		result.Status = domain.ToolFailed
		result.ErrorKind = domain.ErrorToolFailed
	default:
		result.Status = domain.ToolSucceeded
	}
	result.Duration = time.Since(started)
	return result
}

func anchoredDirectoryIdentity(ctx context.Context, path string) (os.FileInfo, error) {
	directory, err := safefile.OpenDirectory(ctx, path)
	if err != nil {
		return nil, err
	}
	info, statErr := directory.Stat()
	closeErr := directory.Close()
	if err := errors.Join(statErr, closeErr); err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("not a directory: %s", path)
	}
	return info, nil
}

func (p *prepared) reportProgress(buffer *output.Buffer, done <-chan struct{}, finished chan<- struct{}) {
	defer close(finished)
	if p.progress == nil {
		<-done
		return
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	lastText := ""
	lastTruncated := false
	emitChanged := func() {
		text, truncated := buffer.Snapshot()
		if text == lastText && truncated == lastTruncated {
			return
		}
		lastText = text
		lastTruncated = truncated
		p.progress(domain.ToolProgress{CallID: p.request.CallID, Text: text, Truncated: truncated})
	}
	for {
		select {
		case <-ticker.C:
			emitChanged()
		case <-done:
			emitChanged()
			return
		}
	}
}

func (p *prepared) failedBeforeStart(started time.Time, buffer *output.Buffer, failure error) domain.ToolResult {
	_, _ = fmt.Fprint(buffer, failure)
	result, _ := buffer.Result(context.Background())
	result.CallID = p.request.CallID
	result.Status = domain.ToolFailed
	result.ErrorKind = domain.ErrorToolFailed
	result.Duration = time.Since(started)
	return result
}

func (p *prepared) contextResult(ctx context.Context, started time.Time, buffer *output.Buffer, code *int) domain.ToolResult {
	result, _ := buffer.Result(context.Background())
	result.CallID = p.request.CallID
	result.ExitCode = code
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		result.Status = domain.ToolFailed
		result.ErrorKind = domain.ErrorToolTimeout
	} else {
		result.Status = domain.ToolCancelled
		result.ErrorKind = domain.ErrorCancelled
	}
	result.Duration = time.Since(started)
	return result
}

func exitCode(command *exec.Cmd) *int {
	if command.ProcessState == nil {
		return nil
	}
	code := command.ProcessState.ExitCode()
	return &code
}

func strippedEnvironment(environment, providerKeyEnvs []string) []string {
	names := make(map[string]struct{}, len(providerKeyEnvs)+1)
	names["YORDAM_API_KEY"] = struct{}{}
	for _, name := range providerKeyEnvs {
		if name != "" {
			names[name] = struct{}{}
		}
	}
	filtered := make([]string, 0, len(environment))
	for _, entry := range environment {
		name, _, found := strings.Cut(entry, "=")
		if found {
			if _, stripped := names[name]; stripped {
				continue
			}
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

func resolveShellPath(configured string) string {
	candidate := configured
	if candidate == "" {
		candidate = os.Getenv("SHELL")
	}
	if executable, err := exec.LookPath(candidate); err == nil {
		return executable
	}
	return "/bin/sh"
}

func decodeStrict(raw json.RawMessage, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}
