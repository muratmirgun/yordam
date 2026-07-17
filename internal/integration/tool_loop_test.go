package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
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
)

func approvalNames(prompts []ports.PermissionPrompt) []string {
	result := make([]string, len(prompts))
	for index, prompt := range prompts {
		result[index] = prompt.Call.Request.Name
	}
	return result
}

func TestAskModeEditAllowShellDeny(t *testing.T) {
	const sentinel = "integration-secret"
	workspacePath := t.TempDir()
	target := filepath.Join(workspacePath, "a.txt")
	initial := []byte("old " + sentinel + "\n")
	if err := os.WriteFile(target, initial, 0o600); err != nil {
		t.Fatal(err)
	}
	before := sha256.Sum256(initial)
	editInput := fmt.Sprintf(`{"path":"a.txt","expected_sha256":"%x","replacements":[{"old":"old","new":"new","all":false}]}`, before)
	provider := &agentfixture.Provider{Streams: [][]domain.ModelEvent{
		agentfixture.ToolStream("read-1", "read", `{"path":"a.txt","offset":1,"limit":10}`),
		agentfixture.ToolStream("search-1", "search", `{"query":"old","path":"."}`),
		agentfixture.ToolStream("edit-1", "edit", editInput),
		agentfixture.ToolStream("shell-1", "shell", `{"command":"touch side-effect","cwd":"."}`),
		{{Kind: domain.ModelTextDelta, Text: "done"}, {Kind: domain.ModelDone}},
	}}
	redactor := secret.New(sentinel)
	store := jsonl.New(t.TempDir(), jsonl.Options{Sanitize: redactor.JSON})
	workspace, err := jsonl.WorkspaceFromPath(workspacePath)
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "test", Model: "test"})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := store.Load(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	bounded := output.Options{SessionID: session.ID, Artifacts: store, Redact: redactor}
	var shellProgress atomic.Int32
	registry := tools.NewRegistry(
		shelltool.New(shelltool.Options{
			Workspace: workspacePath,
			ShellPath: "/bin/sh",
			Timeout:   2 * time.Second,
			Output:    bounded,
			Progress:  func(domain.ToolProgress) { shellProgress.Add(1) },
		}),
		edittool.New(edittool.Options{Workspace: workspacePath, Output: bounded}),
		searchtool.New(searchtool.Options{Workspace: workspacePath, Output: bounded}),
		readtool.New(readtool.Options{Workspace: workspacePath, Output: bounded}),
	)
	approver := &agentfixture.Approver{ResolveFunc: func(prompt ports.PermissionPrompt) domain.PermissionDecision {
		switch prompt.Call.Request.Name {
		case "edit":
			return domain.PermissionDecision{Action: domain.PermissionAllow, Lifetime: domain.PermissionOnce, Scope: prompt.Call.CanonicalScope}
		case "shell":
			return domain.PermissionDecision{Action: domain.PermissionDeny, Lifetime: domain.PermissionOnce, Scope: prompt.Call.CanonicalScope}
		default:
			return domain.PermissionDecision{}
		}
	}}
	runner := agent.Runner{
		Provider:     provider,
		Tools:        registry,
		Policy:       permission.NewSession(domain.ModeAsk),
		Approver:     approver,
		Sessions:     store,
		MaxToolCalls: 32,
		SystemPrompt: "test",
		Redact:       redactor.String,
	}
	if err := runner.RunTurn(context.Background(), agent.RunInput{Session: session, Replay: replay, Prompt: "update a.txt"}); err != nil {
		t.Fatal(err)
	}

	changed, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(changed), "new "+sentinel+"\n"; got != want {
		t.Fatalf("content=%q want=%q", got, want)
	}
	if _, err := os.Stat(filepath.Join(workspacePath, "side-effect")); !os.IsNotExist(err) {
		t.Fatalf("shell side effect exists: %v", err)
	}
	if got := shellProgress.Load(); got != 0 {
		t.Fatalf("denied shell emitted %d progress updates", got)
	}
	if got := approvalNames(approver.Snapshot()); !slices.Equal(got, []string{"edit", "shell"}) {
		t.Fatalf("approval calls=%v", got)
	}
	requests, remaining := provider.Snapshot()
	if len(requests) != 5 || remaining != 0 {
		t.Fatalf("provider requests=%d remaining streams=%d", len(requests), remaining)
	}
	for index, request := range requests {
		var names []string
		for _, descriptor := range request.Tools {
			names = append(names, descriptor.Name)
		}
		if !slices.Equal(names, []string{"read", "search", "edit", "shell"}) {
			t.Fatalf("provider request %d tool order=%v", index+1, names)
		}
	}

	finalReplay, err := store.Load(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertDurableToolLoop(t, finalReplay, sentinel)
}

func assertDurableToolLoop(t *testing.T, replay domain.SessionReplay, sentinel string) {
	t.Helper()
	decisions := map[string]domain.PermissionDecision{}
	results := map[string]domain.ToolResult{}
	var resultOrder []string
	started := map[string]int{}
	planned := 0
	changed := 0
	completed := 0
	finalText := ""
	for _, event := range replay.Events {
		switch event.Kind {
		case domain.EventPermissionResolved:
			var payload domain.PermissionPayload
			decodePayload(t, event, &payload)
			decisions[payload.CallID] = payload.Decision
		case domain.EventToolStarted:
			var payload map[string]string
			decodePayload(t, event, &payload)
			started[payload["call_id"]]++
		case domain.EventToolResult:
			var payload domain.ToolResultPayload
			decodePayload(t, event, &payload)
			results[payload.Result.CallID] = payload.Result
			resultOrder = append(resultOrder, payload.Result.CallID)
		case domain.EventFileChangePlanned:
			var payload domain.FileChangePlan
			decodePayload(t, event, &payload)
			if payload.CallID != "edit-1" {
				t.Fatalf("planned call=%q", payload.CallID)
			}
			planned++
		case domain.EventFileChanged:
			var payload domain.FileChange
			decodePayload(t, event, &payload)
			if payload.CallID != "edit-1" {
				t.Fatalf("changed call=%q", payload.CallID)
			}
			changed++
		case domain.EventAssistantMessage:
			var payload domain.MessagePayload
			decodePayload(t, event, &payload)
			if len(payload.ToolCalls) == 0 && payload.Content != "" {
				finalText = payload.Content
			}
		case domain.EventTurnCompleted:
			var payload domain.TurnTerminalPayload
			decodePayload(t, event, &payload)
			if payload.Reason != "model completed" || payload.ErrorKind != "" {
				t.Fatalf("completion=%#v", payload)
			}
			completed++
		case domain.EventTurnFailed, domain.EventTurnInterrupted:
			t.Fatalf("unexpected terminal event %q", event.Kind)
		}
	}

	if len(decisions) != 4 || decisions["read-1"].Action != domain.PermissionAllow || decisions["search-1"].Action != domain.PermissionAllow || decisions["edit-1"].Action != domain.PermissionAllow || decisions["shell-1"].Action != domain.PermissionDeny {
		t.Fatalf("decisions=%#v", decisions)
	}
	if !slices.Equal(resultOrder, []string{"read-1", "search-1", "edit-1", "shell-1"}) {
		t.Fatalf("result order=%v", resultOrder)
	}
	if len(results) != 4 || results["read-1"].Status != domain.ToolSucceeded || results["search-1"].Status != domain.ToolSucceeded || results["edit-1"].Status != domain.ToolSucceeded || results["shell-1"].Status != domain.ToolDenied {
		t.Fatalf("results=%#v", results)
	}
	if !strings.Contains(results["read-1"].Content, "1: old [REDACTED]") || !strings.Contains(results["search-1"].Content, "a.txt:1:old [REDACTED]") {
		t.Fatalf("read=%q search=%q", results["read-1"].Content, results["search-1"].Content)
	}
	if results["edit-1"].FileChange == nil || results["edit-1"].FileChange.CallID != "edit-1" {
		t.Fatalf("edit result=%#v", results["edit-1"])
	}
	if started["read-1"] != 1 || started["search-1"] != 1 || started["edit-1"] != 1 || started["shell-1"] != 0 || planned != 1 || changed != 1 {
		t.Fatalf("started=%v planned=%d changed=%d", started, planned, changed)
	}
	if finalText != "done" || completed != 1 {
		t.Fatalf("final text=%q completed=%d", finalText, completed)
	}
	raw, err := json.Marshal(replay.Events)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), sentinel) {
		t.Fatalf("secret persisted in durable events: %s", raw)
	}
}

func decodePayload(t *testing.T, event domain.DurableEvent, destination any) {
	t.Helper()
	if err := json.Unmarshal(event.Payload, destination); err != nil {
		t.Fatalf("decode %s payload: %v", event.Kind, err)
	}
}
