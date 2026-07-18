package search

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/safefile"
	"github.com/muratmirgun/yordam/internal/scope"
	toolset "github.com/muratmirgun/yordam/internal/tools"
	"github.com/muratmirgun/yordam/internal/tools/output"
)

const defaultMaxMatches = 500

var inputSchema = json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{"query":{"type":"string"},"path":{"type":"string","default":"."},"include":{"type":"array","items":{"type":"string"}},"exclude":{"type":"array","items":{"type":"string"}}},"required":["query"]}`)

type Options struct {
	Workspace  string
	LookPath   func(string) (string, error)
	MaxMatches int
	Output     output.Options
}

type Input struct {
	Query   string   `json:"query"`
	Path    string   `json:"path"`
	Include []string `json:"include"`
	Exclude []string `json:"exclude"`
}

type Tool struct {
	workspace  string
	rgPath     string
	maxMatches int
	output     output.Options
}

func New(opts Options) *Tool {
	if opts.Output.Artifacts == nil {
		panic("search: artifact store is required")
	}
	lookPath := opts.LookPath
	if lookPath == nil {
		lookPath = exec.LookPath
	}
	rgPath, err := lookPath("rg")
	if err != nil {
		rgPath = ""
	}
	maxMatches := opts.MaxMatches
	if maxMatches <= 0 || maxMatches > defaultMaxMatches {
		maxMatches = defaultMaxMatches
	}
	return &Tool{workspace: opts.Workspace, rgPath: rgPath, maxMatches: maxMatches, output: opts.Output}
}

func (t *Tool) Descriptor() domain.ToolDescriptor {
	return domain.ToolDescriptor{
		Name:             "search",
		Description:      "Search text files for a fixed string with deterministic path and line ordering.",
		ScopeDescription: "Canonical directory tree.",
		InputSchema:      inputSchema,
		Mutation:         domain.MutationReadOnly,
	}
}

func (t *Tool) CanonicalDescriptor() protocol.ToolDescriptor {
	return toolset.BuiltinCanonicalDescriptor(t.Descriptor(), toolset.SearchClassification())
}

func (*Tool) TrustedClassification() domain.ToolClassification { return toolset.SearchClassification() }

func (t *Tool) Plan(_ context.Context, request domain.ToolRequest) (ports.PreparedTool, error) {
	input := Input{Path: "."}
	if err := decodeStrict(request.Input, &input); err != nil {
		return nil, fmt.Errorf("search input: %w", err)
	}
	if input.Query == "" {
		return nil, fmt.Errorf("search input: query is required")
	}
	if input.Path == "" {
		return nil, fmt.Errorf("search input: path must not be empty")
	}
	if err := validateGlobs(input.Include); err != nil {
		return nil, fmt.Errorf("search include: %w", err)
	}
	if err := validateGlobs(input.Exclude); err != nil {
		return nil, fmt.Errorf("search exclude: %w", err)
	}

	resolved, err := scope.Resolve(t.workspace, input.Path, false)
	if err != nil {
		return nil, fmt.Errorf("resolve search path: %w", err)
	}
	return &prepared{
		request:    request,
		input:      input,
		target:     resolved,
		workspace:  t.workspace,
		rgPath:     t.rgPath,
		maxMatches: t.maxMatches,
		output:     t.output,
		beforeOpen: func() error { return nil },
	}, nil
}

func (t *Tool) Prepare(ctx context.Context, request domain.ToolRequest) (ports.PreparedTool, error) {
	planned, err := t.Plan(ctx, request)
	if err != nil {
		return nil, err
	}
	prepared := planned.(*prepared)
	info, err := os.Stat(prepared.target.Path)
	if err != nil {
		return nil, fmt.Errorf("stat search path: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("search path is not a directory: %s", prepared.target.Path)
	}
	prepared.identity = info
	return prepared, nil
}

type prepared struct {
	request    domain.ToolRequest
	input      Input
	target     scope.Resolved
	workspace  string
	rgPath     string
	maxMatches int
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
		Summary:         fmt.Sprintf("search %s for fixed string %q (up to %d matches)", p.target.Path, p.input.Query, p.maxMatches),
		Resources: []protocol.ResourceTarget{{Kind: "directory", CanonicalID: p.target.Path, Attributes: []protocol.ResourceAttribute{
			{Name: "exclude", Value: strings.Join(p.input.Exclude, "\x00")},
			{Name: "include", Value: strings.Join(p.input.Include, "\x00")},
			{Name: "max_matches", Value: strconv.Itoa(p.maxMatches)},
			{Name: "query", Value: p.input.Query},
		}}},
	}
}

func (p *prepared) Revalidate(_ context.Context) (domain.PreparedToolRequest, error) {
	current, err := scope.Resolve(p.workspace, p.input.Path, false)
	if err != nil {
		return domain.PreparedToolRequest{}, fmt.Errorf("re-resolve search path: %w", err)
	}
	info, err := os.Stat(current.Path)
	if err != nil {
		return domain.PreparedToolRequest{}, fmt.Errorf("stat search path: %w", err)
	}
	if !info.IsDir() {
		return domain.PreparedToolRequest{}, fmt.Errorf("search path is not a directory: %s", current.Path)
	}
	p.target, p.identity = current, info
	return p.Preview(), nil
}

func (p *prepared) Execute(ctx context.Context) domain.ToolResult {
	started := time.Now()
	current, err := scope.Resolve(p.workspace, p.input.Path, false)
	if err != nil {
		return failed(p.request.CallID, started, fmt.Errorf("re-resolve search path: %w", err))
	}
	if current != p.target {
		return failed(p.request.CallID, started, fmt.Errorf("canonical scope changed from %q to %q", p.target.Path, current.Path))
	}
	if err := p.beforeOpen(); err != nil {
		return failed(p.request.CallID, started, err)
	}
	directory, err := safefile.OpenDirectory(ctx, current.Path)
	if err != nil {
		return failed(p.request.CallID, started, fmt.Errorf("open search root: %w", err))
	}
	defer directory.Close()
	root, err := os.OpenRoot(inheritedFilePath(int(directory.Fd())))
	if err != nil {
		return failed(p.request.CallID, started, fmt.Errorf("anchor search root: %w", err))
	}
	defer root.Close()
	info, err := directory.Stat()
	if err != nil {
		return failed(p.request.CallID, started, fmt.Errorf("stat search root: %w", err))
	}
	if !info.IsDir() {
		return failed(p.request.CallID, started, fmt.Errorf("search path is not a directory: %s", current.Path))
	}
	if !os.SameFile(p.identity, info) {
		return failed(p.request.CallID, started, fmt.Errorf("search path identity changed after authorization: %s", current.Path))
	}

	buffer := output.New(p.output)
	defer buffer.Close()
	var truncated bool
	if p.rgPath == "" {
		truncated, err = runFallback(ctx, buffer, root, p.input, p.maxMatches)
	} else {
		truncated, err = runRipgrep(ctx, buffer, p.rgPath, root, p.input, p.maxMatches)
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return cancelled(p.request.CallID, started, err)
		}
		return failed(p.request.CallID, started, err)
	}
	result, err := buffer.Result(ctx)
	if err != nil {
		return failed(p.request.CallID, started, fmt.Errorf("finalize search output: %w", err))
	}
	result.CallID = p.request.CallID
	result.Duration = time.Since(started)
	result.Truncated = result.Truncated || truncated
	return result
}

func runRipgrep(ctx context.Context, destination io.Writer, executable string, root *os.Root, input Input, maxMatches int) (bool, error) {
	matches := make([]rgMatch, 0, maxMatches)
	err := fs.WalkDir(root.FS(), ".", func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 || entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			return err
		}
		relative := strings.TrimPrefix(current, "./")
		if !included(relative, input.Include, input.Exclude) {
			return nil
		}
		file, err := root.Open(current)
		if err != nil {
			return err
		}
		fileMatches, truncated, runErr := runRipgrepFile(ctx, executable, file, relative, input.Query, maxMatches-len(matches))
		closeErr := file.Close()
		if runErr != nil {
			return runErr
		}
		if closeErr != nil {
			return closeErr
		}
		matches = append(matches, fileMatches...)
		if truncated {
			return errMatchLimit
		}
		return nil
	})
	if errors.Is(err, errMatchLimit) {
		return emitMatches(destination, matches, true)
	}
	if err != nil {
		return false, fmt.Errorf("walk rg search root: %w", err)
	}
	return emitMatches(destination, matches, false)
}

func runRipgrepFile(ctx context.Context, executable string, file *os.File, relative, query string, maxMatches int) ([]rgMatch, bool, error) {
	text, err := isSearchableText(file)
	if err != nil || !text {
		return nil, false, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, false, err
	}
	command := exec.CommandContext(ctx, executable, "--line-number", "--no-heading", "--color", "never", "--fixed-strings", query, inheritedFilePath(3))
	command.ExtraFiles = []*os.File{file}
	stderr := &cappedWriter{limit: output.ModelExcerptBytes}
	command.Stderr = stderr
	stdout, err := command.StdoutPipe()
	if err != nil {
		return nil, false, err
	}
	if err := command.Start(); err != nil {
		return nil, false, err
	}
	var matches []rgMatch
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64<<10), output.RetainedBytes)
	for scanner.Scan() {
		if len(matches) == maxMatches {
			_ = command.Process.Kill()
			_ = command.Wait()
			return matches, true, nil
		}
		lineNumber, text, err := parseRipgrepFileLine(scanner.Bytes())
		if err != nil {
			_ = command.Process.Kill()
			_ = command.Wait()
			return nil, false, err
		}
		matches = append(matches, rgMatch{relative: relative, lineNumber: lineNumber, text: append([]byte(nil), text...), textLen: len(text)})
	}
	scanErr := scanner.Err()
	waitErr := command.Wait()
	if scanErr != nil {
		return nil, false, scanErr
	}
	if waitErr == nil {
		return matches, false, nil
	}
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) && exitErr.ExitCode() == 1 {
		return matches, false, nil
	}
	message := strings.TrimSpace(stderr.String())
	if message == "" {
		message = waitErr.Error()
	}
	return nil, false, fmt.Errorf("rg failed: %s", message)
}

func isSearchableText(file *os.File) (bool, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return false, err
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), output.RetainedBytes)
	for scanner.Scan() {
		line := scanner.Bytes()
		if bytes.IndexByte(line, 0) >= 0 || !utf8.Valid(line) {
			return false, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, err
	}
	return true, nil
}

func parseRipgrepFileLine(line []byte) (int, []byte, error) {
	separator := bytes.IndexByte(line, ':')
	if separator < 1 || !utf8.Valid(line) {
		return 0, nil, fmt.Errorf("invalid rg output: %q", line)
	}
	lineNumber, err := strconv.Atoi(string(line[:separator]))
	if err != nil || lineNumber < 1 {
		return 0, nil, fmt.Errorf("invalid rg output: %q", line)
	}
	return lineNumber, line[separator+1:], nil
}

type rgMatch struct {
	relative   string
	lineNumber int
	text       []byte
	textLen    int
}

func retainMatch(matches []rgMatch, maximum int, candidate rgMatch) []rgMatch {
	index := sort.Search(len(matches), func(index int) bool {
		return !matchLess(matches[index], candidate)
	})
	if len(matches) == maximum && index == len(matches) {
		return matches
	}
	matches = append(matches, rgMatch{})
	copy(matches[index+1:], matches[index:])
	matches[index] = candidate
	if len(matches) > maximum {
		matches[len(matches)-1] = rgMatch{}
		matches = matches[:maximum]
	}
	trimRetainedMatchText(matches)
	return matches
}

func matchLess(left, right rgMatch) bool {
	if left.relative != right.relative {
		return left.relative < right.relative
	}
	return left.lineNumber < right.lineNumber
}

func trimRetainedMatchText(matches []rgMatch) {
	streamBytes := 0
	for index := range matches {
		headerBytes := len(matches[index].relative) + 2 + len(strconv.Itoa(matches[index].lineNumber))
		if index > 0 {
			headerBytes++
		}
		streamBytes += headerBytes
		keep := output.RetainedBytes - streamBytes
		if keep < 0 {
			keep = 0
		}
		if keep > matches[index].textLen {
			keep = matches[index].textLen
		}
		if len(matches[index].text) > keep {
			matches[index].text = matches[index].text[:keep]
		}
		if streamBytes < output.RetainedBytes {
			streamBytes += matches[index].textLen
		}
		if streamBytes > output.RetainedBytes {
			streamBytes = output.RetainedBytes
		}
	}
}

func emitMatches(destination io.Writer, matches []rgMatch, matchTruncated bool) (bool, error) {
	byteTruncated := false
	for index, match := range matches {
		if index > 0 {
			if _, err := destination.Write([]byte{'\n'}); err != nil {
				return false, err
			}
		}
		header := fmt.Sprintf("%s:%d:", match.relative, match.lineNumber)
		if _, err := io.WriteString(destination, header); err != nil {
			return false, err
		}
		if _, err := destination.Write(match.text); err != nil {
			return false, err
		}
		if len(match.text) < match.textLen {
			byteTruncated = true
			break
		}
	}
	return matchTruncated || byteTruncated, nil
}

type cappedWriter struct {
	data  []byte
	limit int
}

func (w *cappedWriter) Write(value []byte) (int, error) {
	original := len(value)
	remaining := w.limit - len(w.data)
	if remaining > 0 {
		if len(value) > remaining {
			value = value[:remaining]
		}
		w.data = append(w.data, value...)
	}
	return original, nil
}

func (w *cappedWriter) String() string { return string(w.data) }

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
