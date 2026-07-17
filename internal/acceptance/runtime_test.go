//go:build acceptance

package acceptance_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/permission"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
	"github.com/muratmirgun/yordam/internal/testsupport/agentfixture"
	"github.com/muratmirgun/yordam/internal/tools"
	edittool "github.com/muratmirgun/yordam/internal/tools/edit"
	"github.com/muratmirgun/yordam/internal/tools/output"
	readtool "github.com/muratmirgun/yordam/internal/tools/read"
	searchtool "github.com/muratmirgun/yordam/internal/tools/search"
	shelltool "github.com/muratmirgun/yordam/internal/tools/shell"
	"github.com/muratmirgun/yordam/internal/tui/components"
	workspacepkg "github.com/muratmirgun/yordam/internal/workspace"
)

type runtimeFixture struct {
	workspace string
	dataDir   string
	store     *jsonl.Store
	session   domain.Session
	replay    domain.SessionReplay
	policy    *permission.SessionPolicy
	provider  *agentfixture.Provider
	approver  *agentfixture.Approver
	runner    *agent.Runner
}

func newRuntimeFixture(t *testing.T, workspace string, mode domain.PermissionMode, streams [][]domain.ModelEvent, approver *agentfixture.Approver, sentinels ...string) *runtimeFixture {
	t.Helper()
	return newRuntimeFixtureWithSelection(t, workspace, mode, domain.ModelSelection{Profile: "test", Model: "test"}, streams, approver, sentinels...)
}

func newRuntimeFixtureWithSelection(t *testing.T, workspace string, mode domain.PermissionMode, selection domain.ModelSelection, streams [][]domain.ModelEvent, approver *agentfixture.Approver, sentinels ...string) *runtimeFixture {
	t.Helper()
	redactor := secret.New(sentinels...)
	dataDir := t.TempDir()
	store := jsonl.New(dataDir, jsonl.Options{Sanitize: redactor.JSON})
	canonical, err := jsonl.WorkspaceFromPath(workspace)
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.Create(context.Background(), canonical, mode, selection)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := store.Load(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if approver == nil {
		approver = &agentfixture.Approver{}
	}
	provider := &agentfixture.Provider{Streams: streams}
	policy := permission.NewSession(mode)
	bounded := output.Options{SessionID: session.ID, Artifacts: store, Redact: redactor}
	registry := tools.NewRegistry(
		readtool.New(readtool.Options{Workspace: workspace, Output: bounded}),
		searchtool.New(searchtool.Options{Workspace: workspace, Output: bounded}),
		edittool.New(edittool.Options{Workspace: workspace, Output: bounded}),
		shelltool.New(shelltool.Options{Workspace: workspace, ShellPath: "/bin/sh", Timeout: 5 * time.Second, Output: bounded}),
	)
	runner := &agent.Runner{
		Provider:     provider,
		Tools:        registry,
		Policy:       policy,
		Approver:     approver,
		Sessions:     store,
		MaxToolCalls: 32,
		SystemPrompt: "acceptance",
		Redact:       redactor.String,
	}
	return &runtimeFixture{workspace: workspace, dataDir: dataDir, store: store, session: session, replay: replay, policy: policy, provider: provider, approver: approver, runner: runner}
}

func (f *runtimeFixture) run(t *testing.T, prompt string) domain.SessionReplay {
	t.Helper()
	if err := f.runner.RunTurn(context.Background(), agent.RunInput{Session: f.session, Replay: f.replay, Prompt: prompt}); err != nil {
		t.Fatal(err)
	}
	replay, err := f.store.Load(context.Background(), f.session.ID)
	if err != nil {
		t.Fatal(err)
	}
	f.replay = replay
	f.session = replay.Session
	return replay
}

type durableToolState struct {
	order     []string
	results   map[string]domain.ToolResult
	decisions map[string]domain.PermissionDecision
	starts    map[string]int
	finalText string
}

func inspectDurableTools(t *testing.T, replay domain.SessionReplay) durableToolState {
	t.Helper()
	state := durableToolState{results: map[string]domain.ToolResult{}, decisions: map[string]domain.PermissionDecision{}, starts: map[string]int{}}
	for _, event := range replay.Events {
		switch event.Kind {
		case domain.EventPermissionResolved:
			var payload domain.PermissionPayload
			decodeEvent(t, event, &payload)
			state.decisions[payload.CallID] = payload.Decision
		case domain.EventToolStarted:
			var payload map[string]string
			decodeEvent(t, event, &payload)
			state.starts[payload["call_id"]]++
		case domain.EventToolResult:
			var payload domain.ToolResultPayload
			decodeEvent(t, event, &payload)
			state.order = append(state.order, payload.Result.CallID)
			state.results[payload.Result.CallID] = payload.Result
		case domain.EventAssistantMessage:
			var payload domain.MessagePayload
			decodeEvent(t, event, &payload)
			if len(payload.ToolCalls) == 0 && payload.Content != "" {
				state.finalText = payload.Content
			}
		}
	}
	return state
}

func acceptFourToolTurn(t *testing.T) {
	workspace := t.TempDir()
	before := []byte("old needle\n")
	writeAcceptanceFile(t, filepath.Join(workspace, "a.txt"), before)
	fixture := newRuntimeFixture(t, workspace, domain.ModeAuto, [][]domain.ModelEvent{
		agentfixture.ToolStream("read", "read", `{"path":"a.txt","offset":1,"limit":10}`),
		agentfixture.ToolStream("search", "search", `{"query":"needle","path":"."}`),
		agentfixture.ToolStream("edit", "edit", editInput("a.txt", before, "old", "new")),
		agentfixture.ToolStream("shell", "shell", `{"command":"printf shell-ok","cwd":"."}`),
		agentfixture.FinalStream("durable final answer"),
	}, nil)
	fixture.policy.AcknowledgeAutoShell()
	state := inspectDurableTools(t, fixture.run(t, "perform all four tools"))
	if !slices.Equal(state.order, []string{"read", "search", "edit", "shell"}) {
		t.Fatalf("tool result order=%v", state.order)
	}
	for _, callID := range state.order {
		if state.results[callID].Status != domain.ToolSucceeded || state.starts[callID] != 1 {
			t.Fatalf("call %s result=%+v starts=%d", callID, state.results[callID], state.starts[callID])
		}
	}
	if state.finalText != "durable final answer" {
		t.Fatalf("final assistant text=%q", state.finalText)
	}
	if raw, err := os.ReadFile(filepath.Join(workspace, "a.txt")); err != nil || string(raw) != "new needle\n" {
		t.Fatalf("edited file=%q err=%v", raw, err)
	}
}

func acceptAskPaths(t *testing.T) {
	workspace := t.TempDir()
	before := []byte("old\n")
	writeAcceptanceFile(t, filepath.Join(workspace, "a.txt"), before)
	approver := &agentfixture.Approver{ResolveFunc: func(prompt ports.PermissionPrompt) domain.PermissionDecision {
		action := domain.PermissionDeny
		if prompt.Call.Request.Name == "edit" {
			action = domain.PermissionAllow
		}
		return domain.PermissionDecision{Action: action, Lifetime: domain.PermissionOnce, Scope: prompt.Call.CanonicalScope, Reason: "scripted acceptance decision"}
	}}
	fixture := newRuntimeFixture(t, workspace, domain.ModeAsk, [][]domain.ModelEvent{
		agentfixture.ToolStream("edit", "edit", editInput("a.txt", before, "old", "new")),
		agentfixture.ToolStream("shell", "shell", `{"command":"touch denied-side-effect","cwd":"."}`),
		agentfixture.FinalStream("ask complete"),
	}, approver)
	state := inspectDurableTools(t, fixture.run(t, "allow edit and deny shell"))
	if state.decisions["edit"].Action != domain.PermissionAllow || state.decisions["shell"].Action != domain.PermissionDeny {
		t.Fatalf("durable decisions=%+v", state.decisions)
	}
	if state.starts["edit"] != 1 || state.starts["shell"] != 0 || state.results["edit"].Status != domain.ToolSucceeded || state.results["shell"].Status != domain.ToolDenied {
		t.Fatalf("starts=%v results=%+v", state.starts, state.results)
	}
	if raw, _ := os.ReadFile(filepath.Join(workspace, "a.txt")); string(raw) != "new\n" {
		t.Fatalf("edit content=%q", raw)
	}
	if _, err := os.Stat(filepath.Join(workspace, "denied-side-effect")); !os.IsNotExist(err) {
		t.Fatalf("denied shell side effect exists: %v", err)
	}
}

func acceptSafeMode(t *testing.T) {
	workspace := t.TempDir()
	before := []byte("safe read\n")
	writeAcceptanceFile(t, filepath.Join(workspace, "a.txt"), before)
	fixture := newRuntimeFixture(t, workspace, domain.ModeSafe, [][]domain.ModelEvent{
		agentfixture.ToolStream("read", "read", `{"path":"a.txt","offset":1,"limit":10}`),
		agentfixture.ToolStream("edit", "edit", editInput("a.txt", before, "safe", "unsafe")),
		agentfixture.ToolStream("shell", "shell", `{"command":"touch denied-side-effect","cwd":"."}`),
		agentfixture.FinalStream("safe complete"),
	}, nil)
	state := inspectDurableTools(t, fixture.run(t, "safe mode requests"))
	if state.results["read"].Status != domain.ToolSucceeded || !strings.Contains(state.results["read"].Content, "safe read") {
		t.Fatalf("read result=%+v", state.results["read"])
	}
	for _, callID := range []string{"edit", "shell"} {
		if state.results[callID].Status != domain.ToolDenied || state.starts[callID] != 0 {
			t.Fatalf("safe call %s result=%+v starts=%d", callID, state.results[callID], state.starts[callID])
		}
	}
	if raw, _ := os.ReadFile(filepath.Join(workspace, "a.txt")); string(raw) != string(before) {
		t.Fatalf("safe edit mutated file: %q", raw)
	}
	if _, err := os.Stat(filepath.Join(workspace, "denied-side-effect")); !os.IsNotExist(err) {
		t.Fatalf("safe shell side effect exists: %v", err)
	}
}

func acceptAutoMode(t *testing.T) {
	workspace := t.TempDir()
	outside := t.TempDir()
	outsidePath := filepath.Join(outside, "outside.txt")
	writeAcceptanceFile(t, outsidePath, []byte("outside\n"))
	before := []byte("inside old\n")
	writeAcceptanceFile(t, filepath.Join(workspace, "inside.txt"), before)
	approver := &agentfixture.Approver{ResolveFunc: func(prompt ports.PermissionPrompt) domain.PermissionDecision {
		return domain.PermissionDecision{Action: domain.PermissionDeny, Lifetime: domain.PermissionOnce, Scope: prompt.Call.CanonicalScope, Reason: "deny before trust"}
	}}
	fixture := newRuntimeFixture(t, workspace, domain.ModeAuto, [][]domain.ModelEvent{
		agentfixture.ToolStream("inside-edit", "edit", editInput("inside.txt", before, "old", "new")),
		agentfixture.ToolStream("outside-read", "read", fmt.Sprintf(`{"path":%q,"offset":1,"limit":10}`, outsidePath)),
		agentfixture.ToolStream("shell-before", "shell", `{"command":"touch before-ack","cwd":"."}`),
		agentfixture.FinalStream("before acknowledgement"),
		agentfixture.ToolStream("shell-after", "shell", `{"command":"touch after-ack","cwd":"."}`),
		agentfixture.FinalStream("after acknowledgement"),
	}, approver)
	first := inspectDurableTools(t, fixture.run(t, "auto before acknowledgement"))
	if first.results["inside-edit"].Status != domain.ToolSucceeded || first.starts["inside-edit"] != 1 {
		t.Fatalf("inside edit result=%+v starts=%d", first.results["inside-edit"], first.starts["inside-edit"])
	}
	if first.decisions["outside-read"].Action != domain.PermissionDeny || first.decisions["shell-before"].Action != domain.PermissionDeny || first.starts["outside-read"] != 0 || first.starts["shell-before"] != 0 {
		t.Fatalf("pre-ack decisions=%+v starts=%v", first.decisions, first.starts)
	}
	prompts := approver.Snapshot()
	if len(prompts) != 2 || prompts[0].Call.Request.Name != "read" || prompts[1].Call.Request.Name != "shell" {
		t.Fatalf("auto prompts=%+v", prompts)
	}
	if _, err := fixture.store.Append(context.Background(), fixture.session.ID, domain.EventTrustedExecutionAcknowledged, domain.TrustedExecutionPayload{Enabled: true}); err != nil {
		t.Fatal(err)
	}
	fixture.policy.AcknowledgeAutoShell()
	second := inspectDurableTools(t, fixture.run(t, "auto after acknowledgement"))
	if second.results["shell-after"].Status != domain.ToolSucceeded || second.starts["shell-after"] != 1 {
		t.Fatalf("post-ack shell result=%+v starts=%d", second.results["shell-after"], second.starts["shell-after"])
	}
	if len(approver.Snapshot()) != 2 {
		t.Fatalf("acknowledged shell unexpectedly asked: %+v", approver.Snapshot())
	}
	if _, err := os.Stat(filepath.Join(workspace, "before-ack")); !os.IsNotExist(err) {
		t.Fatalf("pre-ack shell ran: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "after-ack")); err != nil {
		t.Fatalf("post-ack shell did not run: %v", err)
	}
}

func acceptEditSafety(t *testing.T) {
	workspace := t.TempDir()
	dataDir := t.TempDir()
	store := jsonl.New(dataDir, jsonl.Options{})
	canonical, err := jsonl.WorkspaceFromPath(workspace)
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.Create(context.Background(), canonical, domain.ModeAsk, domain.ModelSelection{Profile: "test", Model: "test"})
	if err != nil {
		t.Fatal(err)
	}
	before := []byte("old\n")
	path := filepath.Join(workspace, "a.txt")
	writeAcceptanceFile(t, path, before)
	tool := edittool.New(edittool.Options{Workspace: workspace, Output: output.Options{SessionID: session.ID, Artifacts: store}})
	prepared, err := tool.Prepare(context.Background(), domain.ToolRequest{CallID: "edit", Name: "edit", Workspace: workspace, Input: json.RawMessage(editInput("a.txt", before, "old", "new"))})
	if err != nil {
		t.Fatal(err)
	}
	previewPreparer, ok := prepared.(ports.PreviewPreparer)
	if !ok {
		t.Fatal("edit does not support preview preparation")
	}
	if err := previewPreparer.PreparePreview(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantDiff := "--- a/a.txt\n+++ b/a.txt\n@@ -1,2 +1,2 @@\n-old\n+new\n \n"
	if preview := prepared.Preview(); preview.ProposedDiff != wantDiff || preview.FilePlan == nil || preview.FilePlan.Diff != wantDiff {
		t.Fatalf("edit preview=%+v want diff=%q", preview, wantDiff)
	}
	writeAcceptanceFile(t, path, []byte("changed elsewhere\n"))
	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolFailed || result.ErrorKind != domain.ErrorToolFailed || !strings.Contains(result.Content, "stale preimage") || result.FileChange != nil {
		t.Fatalf("stale result=%+v", result)
	}
	if raw, _ := os.ReadFile(path); string(raw) != "changed elsewhere\n" {
		t.Fatalf("stale edit overwrote file: %q", raw)
	}
}

func acceptShellCancellation(t *testing.T) {
	workspace := t.TempDir()
	store, session := acceptanceStore(t, workspace)
	tool := shelltool.New(shelltool.Options{Workspace: workspace, ShellPath: "/bin/sh", Timeout: 30 * time.Second, Output: output.Options{SessionID: session.ID, Artifacts: store}})
	prepared, err := tool.Prepare(context.Background(), domain.ToolRequest{CallID: "shell", Name: "shell", Workspace: workspace, Input: json.RawMessage(`{"command":"sleep 30 & echo $! > child.pid; wait","cwd":"."}`)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan domain.ToolResult, 1)
	go func() { done <- prepared.Execute(ctx) }()
	pid := waitForAcceptancePID(t, filepath.Join(workspace, "child.pid"), 3*time.Second)
	started := time.Now()
	cancel()
	select {
	case result := <-done:
		if result.Status != domain.ToolCancelled || result.ErrorKind != domain.ErrorCancelled || time.Since(started) > 3*time.Second {
			t.Fatalf("cancel result=%+v elapsed=%s", result, time.Since(started))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("process group cancellation exceeded three seconds")
	}
	if err := syscall.Kill(pid, 0); err == nil || err != syscall.ESRCH {
		t.Fatalf("child process %d survived: %v", pid, err)
	}
}

func acceptWorkspaceKinds(t *testing.T) {
	for _, gitWorkspace := range []bool{true, false} {
		name := "plain"
		if gitWorkspace {
			name = "git"
		}
		t.Run(name, func(t *testing.T) {
			workspace := t.TempDir()
			if gitWorkspace {
				command := exec.Command("git", "init", "--quiet")
				command.Dir = workspace
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("git init: %v: %s", err, output)
				}
			}
			writeAcceptanceFile(t, filepath.Join(workspace, "a.txt"), []byte("before\n"))
			fixture := newRuntimeFixture(t, workspace, domain.ModeAsk, [][]domain.ModelEvent{
				agentfixture.ToolStream("shell", "shell", `{"command":"printf after > a.txt","cwd":"."}`),
				agentfixture.FinalStream("shell complete"),
			}, &agentfixture.Approver{ResolveFunc: func(prompt ports.PermissionPrompt) domain.PermissionDecision {
				return domain.PermissionDecision{Action: domain.PermissionAllow, Lifetime: domain.PermissionOnce, Scope: prompt.Call.CanonicalScope}
			}})
			result := inspectDurableTools(t, fixture.run(t, "change workspace through shell")).results["shell"]
			if result.Status != domain.ToolSucceeded || result.WorkspaceChanges == nil {
				t.Fatalf("shell result=%+v", result)
			}
			contextPanel := components.NewContext()
			contextPanel.ShowWorkspaceChanges(result.WorkspaceChanges)
			if gitWorkspace {
				if !result.WorkspaceChanges.IsGit || !strings.Contains(result.WorkspaceChanges.Status, "a.txt") {
					t.Fatalf("Git changes=%+v", result.WorkspaceChanges)
				}
			} else if result.WorkspaceChanges.IsGit || result.WorkspaceChanges.Notice != workspacepkg.NonGitNotice || !strings.Contains(contextPanel.View(), workspacepkg.NonGitNotice) {
				t.Fatalf("non-Git changes=%+v view=%q", result.WorkspaceChanges, contextPanel.View())
			}
		})
	}
}

func acceptanceStore(t *testing.T, workspace string) (*jsonl.Store, domain.Session) {
	t.Helper()
	store := jsonl.New(t.TempDir(), jsonl.Options{})
	canonical, err := jsonl.WorkspaceFromPath(workspace)
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.Create(context.Background(), canonical, domain.ModeAsk, domain.ModelSelection{Profile: "test", Model: "test"})
	if err != nil {
		t.Fatal(err)
	}
	return store, session
}

func editInput(path string, before []byte, old, replacement string) string {
	hash := sha256.Sum256(before)
	return fmt.Sprintf(`{"path":%q,"expected_sha256":"%x","replacements":[{"old":%q,"new":%q,"all":false}]}`, path, hash, old, replacement)
}

func writeAcceptanceFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
}

func decodeEvent(t *testing.T, event domain.DurableEvent, destination any) {
	t.Helper()
	if err := json.Unmarshal(event.Payload, destination); err != nil {
		t.Fatalf("decode %s: %v", event.Kind, err)
	}
}

func waitForAcceptancePID(t *testing.T, path string, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil {
			var pid int
			if _, err := fmt.Sscanf(strings.TrimSpace(string(raw)), "%d", &pid); err == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for PID at %s", path)
	return 0
}
