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

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/permission"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
	edittool "github.com/muratmirgun/yordam/internal/tools/edit"
	"github.com/muratmirgun/yordam/internal/tools/output"
	readtool "github.com/muratmirgun/yordam/internal/tools/read"
	searchtool "github.com/muratmirgun/yordam/internal/tools/search"
	shelltool "github.com/muratmirgun/yordam/internal/tools/shell"
	"github.com/muratmirgun/yordam/internal/tui/components"
	workspacepkg "github.com/muratmirgun/yordam/internal/workspace"
)

func acceptFourToolTurn(t *testing.T) {
	workspace := t.TempDir()
	before := []byte("old needle\n")
	writeAcceptanceFile(t, filepath.Join(workspace, "a.txt"), before)
	store, session := acceptanceStore(t, workspace)
	bounded := output.Options{SessionID: session.ID, Artifacts: store}
	results := []domain.ToolResult{
		executePrepared(t, readtool.New(readtool.Options{Workspace: workspace, Output: bounded}), workspace, "read", `{"path":"a.txt","offset":1,"limit":10}`),
		executePrepared(t, searchtool.New(searchtool.Options{Workspace: workspace, Output: bounded}), workspace, "search", `{"query":"needle","path":"."}`),
		executePrepared(t, edittool.New(edittool.Options{Workspace: workspace, Output: bounded}), workspace, "edit", editInput("a.txt", before, "old", "new")),
		executePrepared(t, shelltool.New(shelltool.Options{Workspace: workspace, ShellPath: "/bin/sh", Timeout: 5 * time.Second, Output: bounded}), workspace, "shell", `{"command":"printf shell-ok","cwd":"."}`),
	}
	order := make([]string, 0, len(results))
	for _, result := range results {
		order = append(order, result.CallID)
		if result.Status != domain.ToolSucceeded {
			t.Fatalf("tool result=%+v", result)
		}
	}
	if !slices.Equal(order, []string{"read", "search", "edit", "shell"}) {
		t.Fatalf("tool order=%v", order)
	}
	if raw, _ := os.ReadFile(filepath.Join(workspace, "a.txt")); string(raw) != "new needle\n" {
		t.Fatalf("edited file=%q", raw)
	}
}

func acceptAskPaths(t *testing.T) {
	workspace := t.TempDir()
	before := []byte("old\n")
	writeAcceptanceFile(t, filepath.Join(workspace, "a.txt"), before)
	store, session := acceptanceStore(t, workspace)
	edit := edittool.New(edittool.Options{Workspace: workspace, Output: output.Options{SessionID: session.ID, Artifacts: store}})
	result := executePrepared(t, edit, workspace, "edit", editInput("a.txt", before, "old", "new"))
	if result.Status != domain.ToolSucceeded {
		t.Fatalf("allowed edit=%+v", result)
	}
	// The denied shell is never dispatched.
	if _, err := os.Stat(filepath.Join(workspace, "denied-side-effect")); !os.IsNotExist(err) {
		t.Fatalf("denied shell side effect exists: %v", err)
	}
}

func acceptSafeMode(t *testing.T) {
	workspace := t.TempDir()
	policy := permission.NewSession(domain.ModeSafe)
	readDecision := policy.Evaluate(context.Background(), ports.PermissionContext{Workspace: workspace}, domain.PreparedToolRequest{Mutation: domain.MutationReadOnly, InsideWorkspace: true, CanonicalScope: filepath.Join(workspace, "a.txt")})
	editDecision := policy.Evaluate(context.Background(), ports.PermissionContext{Workspace: workspace}, domain.PreparedToolRequest{Mutation: domain.MutationFile, InsideWorkspace: true, CanonicalScope: filepath.Join(workspace, "a.txt")})
	shellDecision := policy.Evaluate(context.Background(), ports.PermissionContext{Workspace: workspace}, domain.PreparedToolRequest{Mutation: domain.MutationProcess, InsideWorkspace: true, CanonicalScope: workspace})
	if readDecision.Action != domain.PermissionAllow || editDecision.Action != domain.PermissionDeny || shellDecision.Action != domain.PermissionDeny {
		t.Fatalf("safe decisions read=%+v edit=%+v shell=%+v", readDecision, editDecision, shellDecision)
	}
}

func acceptAutoMode(t *testing.T) {
	workspace := t.TempDir()
	policy := permission.NewSession(domain.ModeAuto)
	inside := policy.Evaluate(context.Background(), ports.PermissionContext{Workspace: workspace}, domain.PreparedToolRequest{Mutation: domain.MutationFile, InsideWorkspace: true, CanonicalScope: filepath.Join(workspace, "a.txt")})
	outside := policy.Evaluate(context.Background(), ports.PermissionContext{Workspace: workspace}, domain.PreparedToolRequest{Mutation: domain.MutationReadOnly, InsideWorkspace: false, CanonicalScope: filepath.Join(t.TempDir(), "outside.txt")})
	shell := domain.PreparedToolRequest{Mutation: domain.MutationProcess, InsideWorkspace: true, CanonicalScope: workspace}
	before := policy.Evaluate(context.Background(), ports.PermissionContext{Workspace: workspace}, shell)
	policy.AcknowledgeAutoShell()
	after := policy.Evaluate(context.Background(), ports.PermissionContext{Workspace: workspace}, shell)
	if inside.Action != domain.PermissionAllow || outside.Action != domain.PermissionAsk || before.Action != domain.PermissionAsk || after.Action != domain.PermissionAllow {
		t.Fatalf("auto decisions inside=%+v outside=%+v before=%+v after=%+v", inside, outside, before, after)
	}
}

func acceptEditSafety(t *testing.T) {
	workspace := t.TempDir()
	store, session := acceptanceStore(t, workspace)
	before := []byte("old\n")
	path := filepath.Join(workspace, "a.txt")
	writeAcceptanceFile(t, path, before)
	tool := edittool.New(edittool.Options{Workspace: workspace, Output: output.Options{SessionID: session.ID, Artifacts: store}})
	prepared, err := tool.Prepare(context.Background(), domain.ToolRequest{CallID: "edit", Name: "edit", Workspace: workspace, Input: json.RawMessage(editInput("a.txt", before, "old", "new"))})
	if err != nil {
		t.Fatal(err)
	}
	preview := prepared.(ports.PreviewPreparer)
	if err := preview.PreparePreview(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantDiff := "--- a/a.txt\n+++ b/a.txt\n@@ -1,2 +1,2 @@\n-old\n+new\n \n"
	if got := prepared.Preview().ProposedDiff; got != wantDiff {
		t.Fatalf("diff=%q want=%q", got, wantDiff)
	}
	writeAcceptanceFile(t, path, []byte("changed elsewhere\n"))
	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolFailed || !strings.Contains(result.Content, "stale preimage") {
		t.Fatalf("stale result=%+v", result)
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
	cancel()
	select {
	case result := <-done:
		if result.Status != domain.ToolCancelled {
			t.Fatalf("cancel result=%+v", result)
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
		t.Run(fmt.Sprint(gitWorkspace), func(t *testing.T) {
			workspace := t.TempDir()
			if gitWorkspace {
				command := exec.Command("git", "init", "--quiet")
				command.Dir = workspace
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("git init: %v: %s", err, output)
				}
			}
			store, session := acceptanceStore(t, workspace)
			result := executePrepared(t, shelltool.New(shelltool.Options{Workspace: workspace, ShellPath: "/bin/sh", Timeout: 5 * time.Second, Output: output.Options{SessionID: session.ID, Artifacts: store}}), workspace, "shell", `{"command":"printf after > a.txt","cwd":"."}`)
			if result.Status != domain.ToolSucceeded || result.WorkspaceChanges == nil {
				t.Fatalf("shell result=%+v", result)
			}
			panel := components.NewContext()
			panel.ShowWorkspaceChanges(result.WorkspaceChanges)
			if gitWorkspace && !result.WorkspaceChanges.IsGit {
				t.Fatalf("expected git changes: %+v", result.WorkspaceChanges)
			}
			if !gitWorkspace && (result.WorkspaceChanges.IsGit || result.WorkspaceChanges.Notice != workspacepkg.NonGitNotice || !strings.Contains(panel.View(), workspacepkg.NonGitNotice)) {
				t.Fatalf("non-git changes=%+v", result.WorkspaceChanges)
			}
		})
	}
}

func executePrepared(t *testing.T, tool ports.Tool, workspace, callID, input string) domain.ToolResult {
	t.Helper()
	prepared, err := tool.Prepare(context.Background(), domain.ToolRequest{CallID: callID, Name: callID, Workspace: workspace, Input: json.RawMessage(input)})
	if err != nil {
		t.Fatal(err)
	}
	if preview, ok := prepared.(ports.PreviewPreparer); ok {
		if err := preview.PreparePreview(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	return prepared.Execute(context.Background())
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

func writeLegacyAcceptanceEvent(t *testing.T, root string, workspace domain.Workspace, session *domain.Session, kind domain.EventKind, payload any) {
	t.Helper()
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	eventTime := time.Now().UTC()
	event := domain.DurableEvent{
		SchemaVersion: 1, EventID: "acceptance-legacy-event", SessionID: session.ID,
		Seq: session.LastSeq + 1, Time: eventTime, Kind: kind, Payload: rawPayload,
	}
	rawEvent, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	events, err := os.OpenFile(filepath.Join(sessionDir, "events.jsonl"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := events.Write(append(rawEvent, '\n')); err != nil {
		_ = events.Close()
		t.Fatal(err)
	}
	if err := events.Close(); err != nil {
		t.Fatal(err)
	}
	session.LastSeq = event.Seq
	session.UpdatedAt = eventTime
	metadata, err := json.Marshal(session)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sessionDir, "metadata.json"), metadata, 0o600); err != nil {
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
