package edit

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/recovery"
	"github.com/muratmirgun/yordam/internal/safefile"
	"github.com/muratmirgun/yordam/internal/scope"
	toolset "github.com/muratmirgun/yordam/internal/tools"
	"github.com/muratmirgun/yordam/internal/tools/output"
)

var (
	inputSchema   = json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{"path":{"type":"string"},"expected_sha256":{"type":"string"},"create":{"type":"boolean","default":false},"new_content":{"type":"string"},"replacements":{"type":"array","items":{"type":"object","additionalProperties":false,"properties":{"old":{"type":"string"},"new":{"type":"string"},"all":{"type":"boolean","default":false}},"required":["old","new"]}}},"required":["path"]}`)
	sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	targetLocks   sync.Map
)

type Replacement struct {
	Old string `json:"old"`
	New string `json:"new"`
	All bool   `json:"all"`
}

func (r *Replacement) UnmarshalJSON(value []byte) error {
	var decoded struct {
		Old *string `json:"old"`
		New *string `json:"new"`
		All bool    `json:"all"`
	}
	if err := decodeStrict(value, &decoded); err != nil {
		return err
	}
	if decoded.Old == nil {
		return fmt.Errorf("replacement old is required")
	}
	if decoded.New == nil {
		return fmt.Errorf("replacement new is required")
	}
	r.Old = *decoded.Old
	r.New = *decoded.New
	r.All = decoded.All
	return nil
}

type Input struct {
	Path           string        `json:"path"`
	ExpectedSHA256 string        `json:"expected_sha256"`
	Create         bool          `json:"create"`
	NewContent     string        `json:"new_content"`
	Replacements   []Replacement `json:"replacements"`
}

type Options struct {
	Workspace string
	Output    output.Options
}

type Tool struct {
	workspace string
	output    output.Options
}

func New(opts Options) *Tool {
	if opts.Output.Artifacts == nil {
		panic("edit: artifact store is required")
	}
	return &Tool{workspace: opts.Workspace, output: opts.Output}
}

func (t *Tool) Descriptor() domain.ToolDescriptor {
	return domain.ToolDescriptor{
		Name:             "edit",
		Description:      "Preview and atomically apply exact text replacements or create a text file.",
		ScopeDescription: "Exact canonical file path, or canonical parent plus one new leaf.",
		InputSchema:      inputSchema,
		Mutation:         domain.MutationFile,
	}
}

func (t *Tool) CanonicalDescriptor() protocol.ToolDescriptor {
	return BuiltinDescriptor()
}

// BuiltinDescriptor is the immutable identity and schema authority used to
// distinguish the built-in edit tool from aliases and lookalikes.
func BuiltinDescriptor() protocol.ToolDescriptor {
	return toolset.BuiltinCanonicalDescriptor((&Tool{}).Descriptor(), toolset.EditClassification())
}

func (*Tool) TrustedClassification() domain.ToolClassification { return toolset.EditClassification() }

func (t *Tool) Prepare(_ context.Context, request domain.ToolRequest) (ports.PreparedTool, error) {
	var input Input
	if err := decodeStrict(request.Input, &input); err != nil {
		return nil, fmt.Errorf("edit input: %w", err)
	}
	if input.Path == "" {
		return nil, fmt.Errorf("edit input: path is required")
	}
	if input.Create {
		return t.prepareCreate(request, input)
	}
	return t.prepareExisting(request, input)
}

func (t *Tool) Plan(ctx context.Context, request domain.ToolRequest) (ports.PreparedTool, error) {
	return t.Prepare(ctx, request)
}

func (t *Tool) prepareCreate(request domain.ToolRequest, input Input) (ports.PreparedTool, error) {
	if input.ExpectedSHA256 != "" {
		return nil, fmt.Errorf("edit input: expected_sha256 must be empty when create is true")
	}
	if len(input.Replacements) != 0 {
		return nil, fmt.Errorf("edit input: replacements must be empty when create is true")
	}
	target, err := scope.Resolve(t.workspace, input.Path, true)
	if err != nil {
		return nil, fmt.Errorf("resolve edit path: %w", err)
	}
	prepared := &prepared{request: request, input: input, target: target, workspace: t.workspace, output: t.output, beforeOpenDirectory: func() error { return nil }, beforeRename: func(_, _ string) error { return nil }, syncDirectory: func(directory *os.File) error { return directory.Sync() }}
	return prepared, nil
}

func (t *Tool) prepareExisting(request domain.ToolRequest, input Input) (ports.PreparedTool, error) {
	if !sha256Pattern.MatchString(input.ExpectedSHA256) {
		return nil, fmt.Errorf("edit input: expected_sha256 must be a lowercase 64-character SHA-256")
	}
	if len(input.Replacements) == 0 {
		return nil, fmt.Errorf("edit input: at least one replacement is required")
	}
	target, err := scope.Resolve(t.workspace, input.Path, false)
	if err != nil {
		return nil, fmt.Errorf("resolve edit path: %w", err)
	}
	prepared := &prepared{request: request, input: input, target: target, workspace: t.workspace, output: t.output, beforeOpenDirectory: func() error { return nil }, beforeRename: func(_, _ string) error { return nil }, syncDirectory: func(directory *os.File) error { return directory.Sync() }}
	return prepared, nil
}

func (p *prepared) PreparePreview(ctx context.Context) error {
	p.previewMu.Lock()
	defer p.previewMu.Unlock()
	if p.previewReady {
		return nil
	}
	var before []byte
	after := []byte(p.input.NewContent)
	mode := os.FileMode(0o600)
	expectedHash := ""
	if p.input.Create {
		if _, err := os.Lstat(p.target.Path); err == nil {
			return fmt.Errorf("edit create target already exists: %s", p.target.Path)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("inspect edit create target: %w", err)
		}
	} else {
		var err error
		before, mode, p.identity, err = readRegularFile(p.target.Path)
		if err != nil {
			return err
		}
		actualHash := hashBytes(before)
		if actualHash != p.input.ExpectedSHA256 {
			return stalePreimage(p.input.ExpectedSHA256, actualHash)
		}
		after, err = applyReplacements(before, p.input.Replacements)
		if err != nil {
			return err
		}
		expectedHash = p.input.ExpectedSHA256
	}
	parentHandle, err := safefile.OpenDirectory(ctx, filepath.Dir(p.target.Path))
	if err != nil {
		return fmt.Errorf("open edit parent: %w", err)
	}
	parent, statErr := parentHandle.Stat()
	closeErr := parentHandle.Close()
	if err := errors.Join(statErr, closeErr); err != nil {
		return fmt.Errorf("stat edit parent: %w", err)
	}
	p.parentIdentity = parent
	return p.buildPreview(ctx, before, after, mode, expectedHash)
}

func (p *prepared) buildPreview(ctx context.Context, before, after []byte, mode os.FileMode, expectedHash string) error {
	request, target := p.request, p.target
	relative, err := filepath.Rel(target.Workspace, target.Path)
	if err != nil {
		return fmt.Errorf("label edit diff: %w", err)
	}
	buffer := output.New(p.output)
	defer buffer.Close()
	if err := writeUnifiedDiff(buffer, before, after, relative); err != nil {
		return fmt.Errorf("generate edit diff: %w", err)
	}
	bounded, err := buffer.Result(ctx)
	if err != nil {
		return fmt.Errorf("finalize edit diff: %w", err)
	}
	p.before = bytes.Clone(before)
	p.after = bytes.Clone(after)
	p.mode = mode
	p.plan = domain.FileChangePlan{CallID: request.CallID, Path: target.Path, ExpectedSHA256: expectedHash, PlannedSHA256: hashBytes(after), Diff: bounded.Content, ArtifactIDs: append([]string(nil), bounded.ArtifactIDs...)}
	p.truncated = bounded.Truncated
	p.previewReady = true
	return nil
}

type prepared struct {
	request             domain.ToolRequest
	input               Input
	target              scope.Resolved
	workspace           string
	output              output.Options
	before              []byte
	after               []byte
	mode                os.FileMode
	identity            os.FileInfo
	parentIdentity      os.FileInfo
	plan                domain.FileChangePlan
	truncated           bool
	beforeOpenDirectory func() error
	beforeRename        func(temporary, target string) error
	syncDirectory       func(*os.File) error
	executeMu           sync.Mutex
	executed            bool
	previewMu           sync.Mutex
	previewReady        bool
}

func (p *prepared) Preview() domain.PreparedToolRequest {
	p.previewMu.Lock()
	defer p.previewMu.Unlock()
	plan := p.plan
	plan.ArtifactIDs = append([]string(nil), p.plan.ArtifactIDs...)
	preview := domain.PreparedToolRequest{
		Request:         p.request,
		Mutation:        domain.MutationFile,
		CanonicalScope:  p.target.Path,
		InsideWorkspace: p.target.Inside,
		Summary:         p.summary(),
		Resources:       p.resources(),
	}
	if p.previewReady {
		preview.ProposedDiff = p.plan.Diff
		preview.FilePlan = &plan
	}
	return preview
}

func (p *prepared) resources() []protocol.ResourceTarget {
	resource := protocol.ResourceTarget{Kind: "file", CanonicalID: p.target.Path}
	if p.input.Create {
		resource.ParentID = filepath.Dir(p.target.Path)
		resource.Attributes = []protocol.ResourceAttribute{{Name: "create", Value: "true"}}
	} else {
		resource.Digest = p.input.ExpectedSHA256
	}
	return []protocol.ResourceTarget{resource}
}

func (p *prepared) Revalidate(_ context.Context) (domain.PreparedToolRequest, error) {
	p.previewMu.Lock()
	previewReady := p.previewReady
	p.previewMu.Unlock()
	if previewReady {
		if err := p.verifyCurrent(); err != nil {
			return domain.PreparedToolRequest{}, err
		}
		return p.Preview(), nil
	}
	current, err := scope.Resolve(p.workspace, p.input.Path, p.input.Create)
	if err != nil {
		return domain.PreparedToolRequest{}, fmt.Errorf("re-resolve edit path: %w", err)
	}
	p.previewMu.Lock()
	p.target = current
	p.previewMu.Unlock()
	return p.Preview(), nil
}

func (p *prepared) RecoveryMaterial(_ context.Context) (recovery.Candidate, bool, error) {
	p.previewMu.Lock()
	defer p.previewMu.Unlock()
	if !p.previewReady || p.input.Create {
		return recovery.Candidate{}, false, nil
	}
	preimageDigest := protocol.Digest{Algorithm: protocol.DigestSHA256, Value: hashBytes(p.before)}
	postimageDigest := protocol.Digest{Algorithm: protocol.DigestSHA256, Value: hashBytes(p.after)}
	return recovery.Candidate{Preimage: bytes.Clone(p.before), PreimageDigest: preimageDigest, ExpectedPostimageDigest: postimageDigest, Mode: uint32(p.mode.Perm())}, true, nil
}

func (p *prepared) summary() string {
	if p.input.Create {
		return fmt.Sprintf("create %s", p.target.Path)
	}
	return fmt.Sprintf("edit %s with %d replacement(s)", p.target.Path, len(p.input.Replacements))
}

func (p *prepared) Execute(ctx context.Context) domain.ToolResult {
	started := time.Now()
	p.executeMu.Lock()
	if p.executed {
		p.executeMu.Unlock()
		return failed(p.request.CallID, started, fmt.Errorf("prepared edit already executed"))
	}
	p.executed = true
	p.executeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return cancelled(p.request.CallID, started, err)
	}
	if err := p.PreparePreview(ctx); err != nil {
		return failed(p.request.CallID, started, err)
	}

	lockValue, _ := targetLocks.LoadOrStore(p.target.Path, &sync.Mutex{})
	lock := lockValue.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()

	if err := p.verifyCurrent(); err != nil {
		return failed(p.request.CallID, started, err)
	}
	artifacts := append([]string(nil), p.plan.ArtifactIDs...)
	result := domain.ToolResult{
		CallID:      p.request.CallID,
		Status:      domain.ToolSucceeded,
		Content:     p.plan.Diff,
		ArtifactIDs: artifacts,
		FileChange: &domain.FileChange{
			CallID:       p.request.CallID,
			Path:         p.target.Path,
			BeforeSHA256: p.plan.ExpectedSHA256,
			AfterSHA256:  p.plan.PlannedSHA256,
			Diff:         p.plan.Diff,
			ArtifactIDs:  append([]string(nil), artifacts...),
		},
		Duration:  time.Since(started),
		Truncated: p.truncated,
	}
	changed, err := p.atomicReplace()
	if err != nil {
		result.Status = domain.ToolFailed
		result.ErrorKind = domain.ErrorToolFailed
		result.Content = errors.Join(fmt.Errorf("file content may have changed; durability is uncertain"), err).Error()
		if !changed {
			result.FileChange = nil
		}
	}
	return result
}

func (p *prepared) verifyCurrent() error {
	current, err := scope.Resolve(p.workspace, p.input.Path, p.input.Create)
	if err != nil {
		return fmt.Errorf("re-resolve edit path: %w", err)
	}
	if current != p.target {
		return fmt.Errorf("canonical scope changed from %q to %q", p.target.Path, current.Path)
	}
	if p.input.Create {
		if _, err := os.Lstat(current.Path); err == nil {
			return fmt.Errorf("edit create target appeared after prepare: %s", current.Path)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("inspect edit create target: %w", err)
		}
		return nil
	}

	content, _, identity, err := readRegularFile(current.Path)
	if err != nil {
		return err
	}
	actualHash := hashBytes(content)
	if actualHash != p.plan.ExpectedSHA256 {
		return stalePreimage(p.plan.ExpectedSHA256, actualHash)
	}
	if p.identity != nil && !os.SameFile(p.identity, identity) {
		return fmt.Errorf("edit path identity changed after authorization: %s", current.Path)
	}
	return nil
}

func (p *prepared) atomicReplace() (changed bool, err error) {
	directory := filepath.Dir(p.target.Path)
	if err := p.beforeOpenDirectory(); err != nil {
		return false, err
	}
	directoryHandle, err := safefile.OpenDirectory(context.Background(), directory)
	if err != nil {
		return false, fmt.Errorf("open edit directory: %w", err)
	}
	defer func() { err = errors.Join(err, directoryHandle.Close()) }()
	if err := validateDirectoryIdentity(directoryHandle, p.parentIdentity); err != nil {
		return false, err
	}
	temporaryName, err := randomTemporaryName()
	if err != nil {
		return false, err
	}
	temporary, err := createTemporaryAt(directoryHandle, temporaryName)
	if err != nil {
		return false, fmt.Errorf("create edit temporary: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, temporary.Close())
		}
		if temporaryName != "" {
			if cleanupErr := removeAt(directoryHandle, temporaryName); cleanupErr != nil {
				err = errors.Join(err, cleanupErr)
			} else if syncErr := p.syncDirectory(directoryHandle); syncErr != nil {
				err = errors.Join(err, fmt.Errorf("sync edit temporary cleanup: %w", syncErr))
			}
		}
	}()

	if err := writeAll(temporary, p.after); err != nil {
		return false, fmt.Errorf("write edit temporary: %w", err)
	}
	if err := temporary.Chmod(p.mode); err != nil {
		return false, fmt.Errorf("set edit temporary mode: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return false, fmt.Errorf("sync edit temporary: %w", err)
	}
	if err := temporary.Close(); err != nil {
		closed = true
		return false, fmt.Errorf("close edit temporary: %w", err)
	}
	closed = true

	if err := p.beforeRename(filepath.Join(directory, temporaryName), p.target.Path); err != nil {
		return false, err
	}
	targetName := filepath.Base(p.target.Path)
	if p.input.Create {
		if err := renameNoReplace(directoryHandle, temporaryName, targetName); err != nil {
			return false, fmt.Errorf("rename new edit target: %w", err)
		}
	} else {
		if err := exchangeNames(directoryHandle, temporaryName, targetName); err != nil {
			return false, fmt.Errorf("exchange edit target: %w", err)
		}
		changed = true
		previous, openErr := openAt(directoryHandle, temporaryName)
		if openErr != nil {
			if rollbackErr := exchangeNames(directoryHandle, temporaryName, targetName); rollbackErr != nil {
				return true, errors.Join(fmt.Errorf("open exchanged edit preimage: %w", openErr), fmt.Errorf("rollback edit exchange: %w", rollbackErr))
			}
			if rollbackSyncErr := p.syncDirectory(directoryHandle); rollbackSyncErr != nil {
				return true, errors.Join(fmt.Errorf("open exchanged edit preimage: %w", openErr), fmt.Errorf("sync rollback edit exchange: %w", rollbackSyncErr))
			}
			return false, fmt.Errorf("open exchanged edit preimage: %w", openErr)
		}
		previousContent, readErr := io.ReadAll(previous)
		previousInfo, statErr := previous.Stat()
		closeErr := previous.Close()
		if validationErr := errors.Join(readErr, statErr, closeErr); validationErr != nil {
			if rollbackErr := exchangeNames(directoryHandle, temporaryName, targetName); rollbackErr != nil {
				return true, errors.Join(fmt.Errorf("validate exchanged edit preimage: %w", validationErr), fmt.Errorf("rollback edit exchange: %w", rollbackErr))
			}
			if rollbackSyncErr := p.syncDirectory(directoryHandle); rollbackSyncErr != nil {
				return true, errors.Join(fmt.Errorf("validate exchanged edit preimage: %w", validationErr), fmt.Errorf("sync rollback edit exchange: %w", rollbackSyncErr))
			}
			return false, fmt.Errorf("validate exchanged edit preimage: %w", validationErr)
		}
		if hashBytes(previousContent) != p.plan.ExpectedSHA256 || !os.SameFile(p.identity, previousInfo) {
			if rollbackErr := exchangeNames(directoryHandle, temporaryName, targetName); rollbackErr != nil {
				return true, errors.Join(fmt.Errorf("stale edit preimage exchanged"), fmt.Errorf("rollback edit exchange: %w", rollbackErr))
			}
			if rollbackSyncErr := p.syncDirectory(directoryHandle); rollbackSyncErr != nil {
				return true, errors.Join(fmt.Errorf("stale edit preimage exchanged"), fmt.Errorf("sync rollback edit exchange: %w", rollbackSyncErr))
			}
			changed = false
			return false, stalePreimage(p.plan.ExpectedSHA256, hashBytes(previousContent))
		}
		if err := removeAt(directoryHandle, temporaryName); err != nil {
			return true, fmt.Errorf("remove exchanged edit preimage: %w", err)
		}
		if err := p.syncDirectory(directoryHandle); err != nil {
			return true, fmt.Errorf("sync exchanged edit preimage removal: %w", err)
		}
		temporaryName = ""
	}
	changed = true
	temporaryName = ""

	if err := p.syncDirectory(directoryHandle); err != nil {
		return true, fmt.Errorf("sync edit directory: %w", err)
	}
	return true, nil
}

func readRegularFile(path string) ([]byte, os.FileMode, os.FileInfo, error) {
	file, err := safefile.OpenRegular(context.Background(), path)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("open edit path: %w", err)
	}
	content, readErr := io.ReadAll(file)
	info, statErr := file.Stat()
	closeErr := file.Close()
	if err := errors.Join(readErr, statErr, closeErr); err != nil {
		return nil, 0, nil, fmt.Errorf("read edit path: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, 0, nil, fmt.Errorf("edit path is not a regular file: %s", path)
	}
	return content, info.Mode(), info, nil
}

func randomTemporaryName() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate edit temporary name: %w", err)
	}
	return ".yordam-edit-" + hex.EncodeToString(value), nil
}

func applyReplacements(before []byte, replacements []Replacement) ([]byte, error) {
	content := string(before)
	for index, replacement := range replacements {
		occurrences := strings.Count(content, replacement.Old)
		if replacement.All {
			if occurrences < 1 {
				return nil, fmt.Errorf("edit replacement %d: all=true requires at least one occurrence of old", index+1)
			}
			content = strings.ReplaceAll(content, replacement.Old, replacement.New)
			continue
		}
		if occurrences != 1 {
			return nil, fmt.Errorf("edit replacement %d: all=false requires exactly one occurrence of old, found %d", index+1, occurrences)
		}
		content = strings.Replace(content, replacement.Old, replacement.New, 1)
	}
	return []byte(content), nil
}

func hashBytes(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func stalePreimage(expected, actual string) error {
	return fmt.Errorf("stale preimage: expected SHA-256 %s, got %s", expected, actual)
}

func writeAll(destination io.Writer, content []byte) error {
	for len(content) > 0 {
		written, err := destination.Write(content)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		content = content[written:]
	}
	return nil
}

func removeTemporary(path string) error {
	if path == "" {
		return nil
	}
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
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
