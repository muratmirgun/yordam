package read

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/safefile"
	"github.com/muratmirgun/yordam/internal/scope"
	"github.com/muratmirgun/yordam/internal/tools/output"
)

const (
	defaultOffset = 1
	defaultLimit  = 200
	maximumLimit  = 2000
	probeBytes    = 8 << 10
)

var inputSchema = json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{"path":{"type":"string"},"offset":{"type":"integer","default":1,"minimum":1},"limit":{"type":"integer","default":200,"minimum":1,"maximum":2000}},"required":["path"]}`)

type Options struct {
	Workspace string
	Output    output.Options
}

type Input struct {
	Path   string `json:"path"`
	Offset int    `json:"offset"`
	Limit  int    `json:"limit"`
}

type Tool struct {
	workspace string
	output    output.Options
}

func New(opts Options) *Tool {
	if opts.Output.Artifacts == nil {
		panic("read: artifact store is required")
	}
	return &Tool{workspace: opts.Workspace, output: opts.Output}
}

func (t *Tool) Descriptor() domain.ToolDescriptor {
	return domain.ToolDescriptor{
		Name:             "read",
		Description:      "Read a UTF-8 text file with stable one-based line numbers.",
		ScopeDescription: "Exact canonical file path.",
		InputSchema:      inputSchema,
		Mutation:         domain.MutationReadOnly,
	}
}

func (t *Tool) Prepare(_ context.Context, request domain.ToolRequest) (ports.PreparedTool, error) {
	input := Input{Offset: defaultOffset, Limit: defaultLimit}
	if err := decodeStrict(request.Input, &input); err != nil {
		return nil, fmt.Errorf("read input: %w", err)
	}
	if input.Path == "" {
		return nil, fmt.Errorf("read input: path is required")
	}
	if input.Offset < 1 {
		return nil, fmt.Errorf("read input: offset must be at least 1")
	}
	if input.Limit < 1 || input.Limit > maximumLimit {
		return nil, fmt.Errorf("read input: limit must be between 1 and %d", maximumLimit)
	}

	resolved, err := scope.Resolve(t.workspace, input.Path, false)
	if err != nil {
		return nil, fmt.Errorf("resolve read path: %w", err)
	}
	info, err := os.Stat(resolved.Path)
	if err != nil {
		return nil, fmt.Errorf("stat read path: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("read path is not a regular file: %s", resolved.Path)
	}
	if resolved.Inside {
		file, err := safefile.OpenRegular(context.Background(), resolved.Path)
		if err != nil {
			return nil, fmt.Errorf("open read path: %w", err)
		}
		inspectErr := inspectTextFile(file, resolved.Path)
		closeErr := file.Close()
		if err := errors.Join(inspectErr, closeErr); err != nil {
			return nil, err
		}
	}

	return &prepared{
		request:    request,
		input:      input,
		target:     resolved,
		workspace:  t.workspace,
		output:     t.output,
		identity:   info,
		beforeOpen: func() error { return nil },
	}, nil
}

type prepared struct {
	request    domain.ToolRequest
	input      Input
	target     scope.Resolved
	workspace  string
	output     output.Options
	identity   os.FileInfo
	beforeOpen func() error
}

func (p *prepared) Preview() domain.PreparedToolRequest {
	return domain.PreparedToolRequest{
		Request:         p.request,
		Mutation:        domain.MutationReadOnly,
		CanonicalScope:  p.target.Path,
		InsideWorkspace: p.target.Inside,
		Summary:         fmt.Sprintf("read %s from line %d (up to %d lines)", p.target.Path, p.input.Offset, p.input.Limit),
	}
}

func (p *prepared) Execute(ctx context.Context) domain.ToolResult {
	started := time.Now()
	current, err := scope.Resolve(p.workspace, p.input.Path, false)
	if err != nil {
		return failed(p.request.CallID, started, fmt.Errorf("re-resolve read path: %w", err))
	}
	if current != p.target {
		return failed(p.request.CallID, started, fmt.Errorf("canonical scope changed from %q to %q", p.target.Path, current.Path))
	}

	if err := p.beforeOpen(); err != nil {
		return failed(p.request.CallID, started, err)
	}
	file, err := safefile.OpenRegular(ctx, current.Path)
	if err != nil {
		return failed(p.request.CallID, started, fmt.Errorf("open read path: %w", err))
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return failed(p.request.CallID, started, fmt.Errorf("stat read path: %w", err))
	}
	if !os.SameFile(p.identity, info) {
		return failed(p.request.CallID, started, fmt.Errorf("read path identity changed after authorization: %s", current.Path))
	}
	if err := inspectTextFile(file, current.Path); err != nil {
		return failed(p.request.CallID, started, err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return failed(p.request.CallID, started, fmt.Errorf("rewind read path: %w", err))
	}

	buffer := output.New(p.output)
	if err := streamLines(ctx, buffer, file, p.input.Offset, p.input.Limit); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return cancelled(p.request.CallID, started, err)
		}
		return failed(p.request.CallID, started, err)
	}
	result, err := buffer.Result(ctx)
	if err != nil {
		return failed(p.request.CallID, started, fmt.Errorf("finalize read output: %w", err))
	}
	result.CallID = p.request.CallID
	result.Duration = time.Since(started)
	return result
}

func inspectTextFile(file *os.File, path string) error {
	probe, err := io.ReadAll(io.LimitReader(file, probeBytes))
	if err != nil {
		return fmt.Errorf("inspect read path: %w", err)
	}
	if bytes.IndexByte(probe, 0) >= 0 {
		return fmt.Errorf("read path appears to be binary: %s", path)
	}
	if !validUTF8Prefix(probe) {
		return fmt.Errorf("read path is not valid UTF-8: %s", path)
	}
	return nil
}

func validUTF8Prefix(value []byte) bool {
	for len(value) > 0 {
		if !utf8.FullRune(value) {
			return true
		}
		r, size := utf8.DecodeRune(value)
		if r == utf8.RuneError && size == 1 {
			return false
		}
		value = value[size:]
	}
	return true
}

func streamLines(ctx context.Context, destination io.Writer, source io.Reader, offset, limit int) error {
	scanner := bufio.NewScanner(source)
	scanner.Buffer(make([]byte, 64<<10), output.RetainedBytes)
	lineNumber := 0
	written := 0
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		lineNumber++
		line := scanner.Bytes()
		if bytes.IndexByte(line, 0) >= 0 {
			return fmt.Errorf("read path appears to be binary at line %d", lineNumber)
		}
		if !utf8.Valid(line) {
			return fmt.Errorf("read path is not valid UTF-8 at line %d", lineNumber)
		}
		if lineNumber < offset {
			continue
		}
		if written == limit {
			break
		}
		if written > 0 {
			if _, err := destination.Write([]byte{'\n'}); err != nil {
				return err
			}
		}
		prefix := strconv.AppendInt(nil, int64(lineNumber), 10)
		prefix = append(prefix, ':', ' ')
		if _, err := destination.Write(prefix); err != nil {
			return err
		}
		if _, err := destination.Write(line); err != nil {
			return err
		}
		written++
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read text: %w", err)
	}
	return nil
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

func failed(callID string, started time.Time, err error) domain.ToolResult {
	return domain.ToolResult{
		CallID:    callID,
		Status:    domain.ToolFailed,
		ErrorKind: domain.ErrorToolFailed,
		Content:   err.Error(),
		Duration:  time.Since(started),
	}
}

func cancelled(callID string, started time.Time, err error) domain.ToolResult {
	return domain.ToolResult{
		CallID:    callID,
		Status:    domain.ToolCancelled,
		ErrorKind: domain.ErrorCancelled,
		Content:   err.Error(),
		Duration:  time.Since(started),
	}
}
