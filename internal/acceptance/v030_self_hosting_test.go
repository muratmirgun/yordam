//go:build acceptance

package acceptance_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	contextplanner "github.com/muratmirgun/yordam/internal/context"
	"github.com/muratmirgun/yordam/internal/evidence"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
)

const (
	selfHostOriginalHeading = "# Yordam\n"
	selfHostChangedHeading  = "# Yordam self-hosted\n"
	selfHostTask            = "Use go-development and one child to replace only the README heading, run the focused check, then verify the whole repository."
)

func TestV030SelfHosting(t *testing.T) {
	if testing.Short() {
		t.Skip("real PTY self-hosting acceptance is not a short test")
	}
	checkout := newSelfHostCheckout(t)
	original, err := os.ReadFile(filepath.Join(checkout.Workspace, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(original, []byte(selfHostOriginalHeading)) || bytes.Count(original, []byte(selfHostOriginalHeading)) != 1 {
		t.Fatalf("README fixture does not have one exact heading")
	}
	preimage := sha256.Sum256(original)
	steps := selfHostSuccessSteps(hex.EncodeToString(preimage[:]))
	provider := newScriptedProvider(t, steps)
	defer provider.Close()

	session := checkout.start(t, provider.URL)
	session.WaitFor(t, "self-host/self-host-model", 10*time.Second)
	session.WaitFor(t, "Ask Yordam", 10*time.Second)

	skillsOffset := session.OutputOffset()
	session.Write(t, "/skills\r")
	session.WaitForOrderedAfter(t, skillsOffset, []string{"SKILLS", "go-development", "awaiting-trust", "A: allow project catalog"}, 10*time.Second)
	trustOffset := session.OutputOffset()
	session.Write(t, "a")
	session.WaitForOrderedAfter(t, trustOffset, []string{"Trust actions are unavailable while an operation is active.", "A: allow project catalog"}, 10*time.Second)
	session.Write(t, "\r")

	turnOffset := session.OutputOffset()
	session.Write(t, selfHostTask+"\r")
	session.WaitForAfter(t, turnOffset, "skill go-development [project", 20*time.Second)
	session.WaitForAfter(t, turnOffset, "CHILD", 20*time.Second)
	session.WaitForAfter(t, turnOffset, "Child session:", 30*time.Second)
	session.WaitForAfter(t, turnOffset, "y: allow once | s: allow session | n/Esc: deny", 30*time.Second)
	session.WaitForQuiet(t, 100*time.Millisecond, 5*time.Second)
	parentPermissionOffset := session.OutputOffset()
	session.Write(t, "y")
	waitForSelfHostAuthorization(t, checkout.DataDir, "parent-complete-test", 30*time.Second)
	session.WaitForAfter(t, parentPermissionOffset, "PERMISSION", 30*time.Second)
	session.Write(t, "y")
	session.WaitForAfter(t, turnOffset, "self-hosting change and complete verification succeeded", 420*time.Second)
	session.WaitForQuiet(t, 300*time.Millisecond, 10*time.Second)

	prefixes := captureSelfHostJournalPrefixes(t, checkout.DataDir)
	compactOffset := session.OutputOffset()
	session.Write(t, "/compact\r")
	session.WaitForOrderedAfter(t, compactOffset, []string{"Compacting context: preparing", "Compacting context: summarizing", "Compacting context: persisting"}, 30*time.Second)
	session.WaitForAfter(t, compactOffset, "Latest compacted:", 30*time.Second)
	session.WaitForQuiet(t, 300*time.Millisecond, 10*time.Second)

	session.Write(t, "/quit\r")
	session.WaitForExit(t, 10*time.Second)
	session.AssertRestored(t)
	provider.AssertComplete(t)
	assertSelfHostJournalPrefixes(t, prefixes)

	result := inspectSelfHostedRun(t, checkout, provider, original)
	restarted := checkout.start(t, provider.URL, "--continue")
	restarted.WaitFor(t, "self-hosting change and complete verification succeeded", 15*time.Second)
	restarted.WaitFor(t, "CHILD", 15*time.Second)
	restarted.WaitFor(t, "Latest compacted:", 15*time.Second)
	restarted.WaitFor(t, string(result.child.Session.ID), 15*time.Second)
	restarted.Write(t, "/quit\r")
	restarted.WaitForExit(t, 10*time.Second)
	restarted.AssertRestored(t)
	assertSelfHostJournalPrefixes(t, prefixes)
	if len(provider.Requests) != len(steps) {
		t.Fatalf("restart retried provider effects: requests=%d want=%d", len(provider.Requests), len(steps))
	}
}

func waitForSelfHostAuthorization(t *testing.T, dataDir, callID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		found, err := selfHostAuthorizationRequested(dataDir, callID)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for canonical authorization request %q", callID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func selfHostAuthorizationRequested(dataDir, callID string) (bool, error) {
	found := false
	err := filepath.WalkDir(filepath.Join(dataDir, "workspaces"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if found || entry.IsDir() || entry.Name() != "events.jsonl" {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, line := range bytes.Split(raw, []byte{'\n'}) {
			var envelope protocol.EventEnvelope
			if json.Unmarshal(line, &envelope) != nil || envelope.Kind != protocol.EventAuthorizationRequested {
				continue
			}
			var requested protocol.AuthorizationRequestedV1
			if json.Unmarshal(envelope.Payload, &requested) == nil && requested.Request.CallID == callID {
				found = true
				break
			}
		}
		return nil
	})
	return found, err
}

func selfHostSuccessSteps(readmeSHA string) []providerStep {
	parentTools := []string{"read", "search", "skill", "subagent", "edit", "shell"}
	childTools := []string{"read", "search", "skill", "edit", "shell"}
	return []providerStep{
		{Role: "parent", RequiredTools: parentTools, RequiredText: []string{"go-development"}, SSE: providerToolCallSSE("load-go-development", "skill", `{"name":"go-development"}`)},
		{Role: "parent", RequiredTools: parentTools, RequiredText: []string{"load-go-development", "Inspect the relevant repository files"}, SSE: providerToolCallSSE("delegate-readme", "subagent", `{"task":"replace only the README heading with # Yordam self-hosted","expected_output":"README.md changed and focused verification result","context":"read README.md, fixed-string search # Yordam, exact edit, then git diff --check -- README.md"}`)},
		{Role: "child", RequiredTools: childTools, RequiredText: []string{"replace only the README heading"}, SSE: providerToolCallSSE("child-read-readme", "read", `{"path":"README.md"}`)},
		{Role: "child", RequiredTools: childTools, RequiredText: []string{"child-read-readme", "# Yordam"}, SSE: providerToolCallSSE("child-search-heading", "search", `{"query":"# Yordam","path":"."}`)},
		{Role: "child", RequiredTools: childTools, RequiredText: []string{"child-search-heading"}, SSE: providerToolCallSSE("child-edit-heading", "edit", fmt.Sprintf(`{"path":"README.md","expected_sha256":%q,"replacements":[{"old":"# Yordam\n","new":"# Yordam self-hosted\n"}]}`, readmeSHA))},
		{Role: "child", RequiredTools: childTools, RequiredText: []string{"child-edit-heading"}, SSE: providerToolCallSSE("child-focused-check", "shell", `{"command":"git diff --check -- README.md","cwd":"."}`)},
		{Role: "child", RequiredTools: childTools, RequiredText: []string{"child-focused-check"}, SSE: providerTextSSE("child changed README.md and git diff --check -- README.md passed")},
		{Role: "parent", RequiredTools: parentTools, RequiredText: []string{"delegate-readme", "child changed README.md", "terminal_cursor"}, SSE: providerToolCallSSE("parent-complete-test", "shell", `{"command":"go test ./...","cwd":"."}`)},
		{Role: "parent", RequiredTools: parentTools, RequiredText: []string{"parent-complete-test"}, SSE: providerTextSSE("self-hosting change and complete verification succeeded")},
		{Role: "compaction", RequiredText: []string{"Return only canonical JSON", "normalized_sources"}, SSE: providerTextSSE(`{"goal":"self-host","constraints":["one bounded README heading replacement"],"decisions":["one sequential child"],"files":["README.md"],"commands_and_tests":["git diff --check -- README.md","go test ./..."],"unresolved":[],"children":["child changed README.md and focused check passed"],"skills":["go-development"],"unknown_effects":[]}`)},
	}
}

type selfHostedRun struct {
	parent  journal.SessionInspection
	child   journal.SessionInspection
	receipt protocol.SubagentReceiptV1
	compact protocol.ContextCompactedV1
}

func inspectSelfHostedRun(t *testing.T, checkout selfHostCheckout, provider *scriptedProvider, original []byte) selfHostedRun {
	t.Helper()
	workspace, err := jsonl.WorkspaceFromPath(checkout.Workspace)
	if err != nil {
		t.Fatal(err)
	}
	store := jsonl.New(checkout.DataDir, jsonl.Options{})
	control, err := store.Inspect(t.Context(), protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: protocol.JournalID(workspace.ID)})
	if err != nil || !control.Writable {
		t.Fatalf("inspect workspace control: writable=%t err=%v diagnostics=%+v", control.Writable, err, control.Diagnostics)
	}
	var trust protocol.ProjectSkillTrustChangedV1
	var runtime protocol.RuntimeGenerationManifest
	for _, event := range control.Events {
		switch event.Envelope.Kind {
		case protocol.EventProjectSkillTrustChanged:
			decodeSelfHostEvent(t, event, &trust)
		case protocol.EventRuntimeGenerationActivated:
			var activated protocol.RuntimeGenerationActivatedV1
			decodeSelfHostEvent(t, event, &activated)
			for _, skill := range activated.Manifest.Body.Skills {
				if skill.Identity.Name == "go-development" && skill.State == protocol.SkillStateActive {
					runtime = activated.Manifest
				}
			}
		}
	}
	if trust.WorkspaceID != protocol.WorkspaceID(workspace.ID) || trust.Decision != "allow" || trust.CatalogDigest.Validate() != nil {
		t.Fatalf("project trust is not bound to exact workspace/catalog: %+v", trust)
	}
	if runtime.ID == "" || runtime.Body.SkillCatalogRevision == "" {
		t.Fatalf("active runtime omitted trusted skill catalog: %+v", runtime)
	}

	parent, child := inspectSelfHostSessions(t, store, checkout.DataDir, workspace.ID)
	lineage, err := store.SessionLineage(t.Context(), protocol.SessionID(child.Session.ID))
	if err != nil || lineage == nil || lineage.Kind != journal.LineageSubagent || lineage.ParentSessionID != protocol.SessionID(parent.Session.ID) || lineage.DelegationAttemptID == "" || lineage.ManifestDigest.Validate() != nil {
		t.Fatalf("child identity-only lineage=%+v err=%v", lineage, err)
	}
	var request protocol.SubagentRequestedV1
	var attachment protocol.SubagentResultAttachedV1
	var compact protocol.ContextCompactedV1
	var attachmentTime time.Time
	for _, event := range parent.Journal.Events {
		switch event.Envelope.Kind {
		case protocol.EventSubagentRequested:
			decodeSelfHostEvent(t, event, &request)
		case protocol.EventSubagentResultAttached:
			decodeSelfHostEvent(t, event, &attachment)
			attachmentTime = event.Envelope.Time
		case protocol.EventContextCompacted:
			decodeSelfHostEvent(t, event, &compact)
		}
	}
	var receipt protocol.SubagentReceiptV1
	var receiptTime time.Time
	for _, event := range child.Journal.Events {
		if event.Envelope.Kind == protocol.EventSubagentReceipt {
			decodeSelfHostEvent(t, event, &receipt)
			receiptTime = event.Envelope.Time
		}
	}
	digest, err := canonicaljson.Digest(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if request.Manifest.ChildSessionID != protocol.SessionID(child.Session.ID) || request.Manifest.SkillCatalogRevision != runtime.Body.SkillCatalogRevision || receipt.Status != "succeeded" || receipt.Manifest != request.Manifest || receipt.TerminalCursor != attachment.TerminalCursor || digest != attachment.ReceiptDigest || attachment.ReceiptEvidenceID == "" || attachmentTime.Before(receiptTime) {
		t.Fatalf("request/receipt/attachment chain invalid: request=%+v receipt=%+v attachment=%+v", request, receipt, attachment)
	}
	canonicalWorkspace, err := filepath.EvalSymlinks(checkout.Workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(receipt.ChangedFiles, []string{filepath.Join(canonicalWorkspace, "README.md")}) || !slices.Contains(receipt.CommandsAndTests, "git diff --check -- README.md") {
		t.Fatalf("child receipt facts=%+v", receipt)
	}
	if !selfHostCommandSucceeded(parent.Journal.Events, "go test ./...") || !selfHostCommandSucceeded(child.Journal.Events, "git diff --check -- README.md") {
		t.Fatal("focused child command or complete parent command lacked a canonical success terminal")
	}
	if compact.Validate() != nil || compact.Through.CommitSeq >= parent.Journal.Head.CommitSeq {
		t.Fatalf("compaction range/reference invalid: compact=%+v head=%+v", compact, parent.Journal.Head)
	}
	assertSelfHostEvidenceAndReconstruction(t, checkout.DataDir, workspace.ID, parent, runtime, compact)
	assertSelfHostREADMEDiff(t, checkout.Workspace, original)
	assertSelfHostProviderTrace(t, provider, attachment)
	return selfHostedRun{parent: parent, child: child, receipt: receipt, compact: compact}
}

func inspectSelfHostSessions(t *testing.T, store *jsonl.Store, dataDir, workspaceID string) (journal.SessionInspection, journal.SessionInspection) {
	t.Helper()
	root := filepath.Join(dataDir, "workspaces", workspaceID, "sessions")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var parent, child journal.SessionInspection
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		inspection, inspectErr := store.InspectSession(t.Context(), protocol.SessionID(entry.Name()))
		if inspectErr != nil || !inspection.Journal.Writable {
			t.Fatalf("inspect session %s: writable=%t err=%v", entry.Name(), inspection.Journal.Writable, inspectErr)
		}
		lineage, lineageErr := store.SessionLineage(t.Context(), protocol.SessionID(entry.Name()))
		if lineageErr != nil {
			t.Fatal(lineageErr)
		}
		if lineage != nil && lineage.Kind == journal.LineageSubagent {
			child = inspection
		} else {
			parent = inspection
		}
	}
	if parent.Session.ID == "" || child.Session.ID == "" || len(entries) != 2 {
		t.Fatalf("expected exactly one parent and child session: parent=%s child=%s entries=%d", parent.Session.ID, child.Session.ID, len(entries))
	}
	return parent, child
}

func decodeSelfHostEvent(t *testing.T, event protocol.EventRecord, target any) {
	t.Helper()
	if err := json.Unmarshal(event.Envelope.Payload, target); err != nil {
		t.Fatalf("decode %s at %d: %v", event.Envelope.Kind, event.Envelope.Seq, err)
	}
}

func selfHostCommandSucceeded(events []protocol.EventRecord, command string) bool {
	plans := map[protocol.ActivityID]bool{}
	for _, event := range events {
		if event.Envelope.Kind != protocol.EventExecutionPlanDeclared {
			continue
		}
		var declared protocol.ExecutionPlanDeclaredV1
		if json.Unmarshal(event.Envelope.Payload, &declared) != nil || declared.Plan.Body.Tool.Name != "shell" {
			continue
		}
		for _, resource := range declared.Plan.Body.Resources {
			for _, attribute := range resource.Attributes {
				if attribute.Name == "command" && attribute.Value == command {
					plans[event.Envelope.ActivityID] = true
				}
			}
		}
	}
	for _, event := range events {
		if event.Envelope.Kind == protocol.EventActivitySucceeded && plans[event.Envelope.ActivityID] {
			return true
		}
	}
	return false
}

func assertSelfHostEvidenceAndReconstruction(t *testing.T, dataDir, workspaceID string, parent journal.SessionInspection, runtime protocol.RuntimeGenerationManifest, compact protocol.ContextCompactedV1) {
	t.Helper()
	registry := secret.NewRegistry()
	lease, err := registry.Acquire("self-host-inspection", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	store, err := evidence.New(dataDir, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Verify(t.Context(), compact.SummaryEvidenceID); err != nil {
		t.Fatalf("verify compaction summary evidence: %v", err)
	}
	record, err := store.Get(t.Context(), compact.SummaryEvidenceID)
	if err != nil || record.Body.WorkspaceID != protocol.WorkspaceID(workspaceID) || record.Body.SessionID != protocol.SessionID(parent.Session.ID) || record.Body.Kind != "context_summary" {
		t.Fatalf("compaction summary evidence=%+v err=%v", record, err)
	}
	var taskID protocol.TaskID
	var contractID protocol.OutcomeContractID
	var contractVersion uint32
	for _, event := range parent.Journal.Events {
		if event.Envelope.TaskID != "" {
			taskID = event.Envelope.TaskID
		}
		if event.Envelope.Kind == protocol.EventOutcomeContractDeclared {
			var declared protocol.OutcomeContractDeclaredV1
			decodeSelfHostEvent(t, event, &declared)
			contractID, contractVersion = declared.OutcomeContractID, declared.Version
		}
	}
	if len(runtime.Body.Models) != 1 || taskID == "" || contractID == "" || contractVersion == 0 {
		t.Fatalf("runtime/context identity incomplete")
	}
	plan, err := contextplanner.NewPlanner(runtime.Body.ToolCatalogRevision, contextplanner.NewEvidenceSummaryResolver(store)).Plan(context.Background(), contextplanner.Request{
		Session: protocol.SessionID(parent.Session.ID), TaskID: taskID, OutcomeContractID: contractID, OutcomeContractVersion: contractVersion,
		Events: parent.Journal.Events, Model: runtime.Body.Models[0], OutputReserve: 512,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Body.CompactionRevision != compact.Revision || len(plan.Body.Sources) < 2 || plan.Body.Sources[0].Kind != "compaction_summary" {
		t.Fatalf("restart plan did not reconstruct summary plus suffix: %+v", plan.Body)
	}
}

func assertSelfHostREADMEDiff(t *testing.T, workspace string, original []byte) {
	t.Helper()
	changed, err := os.ReadFile(filepath.Join(workspace, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Replace(original, []byte(selfHostOriginalHeading), []byte(selfHostChangedHeading), 1)
	if !bytes.Equal(changed, want) {
		t.Fatal("self-hosted workspace change was not the one exact heading replacement")
	}
	command := exec.Command("git", "-C", workspace, "diff", "--", "README.md")
	output, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "-# Yordam") || !strings.Contains(string(output), "+# Yordam self-hosted") || strings.Count(string(output), "@@") != 2 {
		t.Fatalf("README diff=%q err=%v", output, err)
	}
	status := harnessGit(t, workspace, "status", "--porcelain=v1", "--untracked-files=all")
	if status != "M README.md" {
		t.Fatalf("self-host workspace status=%q want only README.md", status)
	}
}

func assertSelfHostProviderTrace(t *testing.T, provider *scriptedProvider, attachment protocol.SubagentResultAttachedV1) {
	t.Helper()
	provider.mu.Lock()
	requests := append([]capturedProviderRequest(nil), provider.Requests...)
	provider.mu.Unlock()
	if len(requests) != 10 {
		t.Fatalf("provider requests=%d want exact script of 10", len(requests))
	}
	counts := map[string]int{}
	for _, request := range requests {
		counts[request.Role]++
	}
	if counts["parent"] != 4 || counts["child"] != 5 || counts["compaction"] != 1 {
		t.Fatalf("provider role counts=%v", counts)
	}
	if !strings.Contains(requests[7].Body, string(attachment.ReceiptEvidenceID)) || !strings.Contains(requests[7].Body, attachment.ReceiptDigest.Value) {
		t.Fatal("parent continuation omitted exact attached child receipt")
	}
}

func captureSelfHostJournalPrefixes(t *testing.T, dataDir string) map[string][]byte {
	t.Helper()
	prefixes := map[string][]byte{}
	err := filepath.WalkDir(filepath.Join(dataDir, "workspaces"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() != "events.jsonl" {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		prefixes[path] = raw
		return nil
	})
	if err != nil || len(prefixes) < 3 {
		t.Fatalf("capture canonical journal prefixes: count=%d err=%v", len(prefixes), err)
	}
	return prefixes
}

func assertSelfHostJournalPrefixes(t *testing.T, prefixes map[string][]byte) {
	t.Helper()
	for path, prefix := range prefixes {
		after, err := os.ReadFile(path)
		if err != nil || len(after) < len(prefix) || !bytes.Equal(after[:len(prefix)], prefix) {
			t.Fatalf("journal did not preserve exact byte prefix %s: before=%d after=%d err=%v", path, len(prefix), len(after), err)
		}
	}
}
