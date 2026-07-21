# Durable Tool-Result Transcript Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist provider-visible tool results atomically, rebuild valid tool exchanges on later turns and restarts, and conservatively repair affected historical sessions.

**Architecture:** Add a versioned `tool.message` foundation event and write it in the same journal transaction as the terminal tool activity and evidence. Project tool messages through a transcript state machine that validates call/result pairing, inserts `uncertain` compatibility results only for terminal historical turns, and groups complete exchanges for budgeting and compaction.

**Tech Stack:** Go, versioned foundation event codec, append-only session journal, provider-neutral model protocol, OpenAI-compatible adapter, table-driven Go tests.

## Global Constraints

- Do not rewrite existing journal files.
- Never infer that a historical tool call succeeded.
- A new terminal tool activity and its provider-visible result must commit in one transaction.
- Provider continuation must not start before that transaction commits.
- Recovery remains authoritative for non-terminal turns; compatibility synthesis must not close them.
- Tool-use and matching tool-result sources are an indivisible budgeting and compaction group.
- Preserve provider order for multiple tool calls and provider rounds.
- Do not add a provider-specific persistence format.

---

### Task 1: Add the versioned durable tool-message event

**Files:**
- Modify: `internal/protocol/payloads.go`
- Modify: `internal/protocol/protocol_test.go`
- Modify: `internal/eventcodec/foundation.go`
- Modify: `internal/eventcodec/foundation_test.go`

**Interfaces:**
- Produces: `protocol.EventToolMessage = "tool.message"`
- Produces: `protocol.ToolMessageV1{Results []protocol.ToolResultBlock}`
- Produces: `func (v ToolMessageV1) Validate() error`
- Consumes: `protocol.ToolResultBlock.Validate()` and existing protocol bounds.

- [ ] **Step 1: Write failing protocol validation tests**

Add a table-driven `TestToolMessageValidate` proving that an empty result list, an invalid result, and duplicate call IDs fail, while two distinct ordered results pass:

```go
valid := protocol.ToolResultBlock{CallID: "call-1", Status: "succeeded", Text: "ok"}
tests := []struct {
    name string
    value protocol.ToolMessageV1
    wantErr bool
}{
    {name: "empty", value: protocol.ToolMessageV1{}, wantErr: true},
    {name: "invalid", value: protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{{Status: "failed"}}}, wantErr: true},
    {name: "duplicate", value: protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{valid, valid}}, wantErr: true},
    {name: "ordered", value: protocol.ToolMessageV1{Results: []protocol.ToolResultBlock{valid, protocol.ToolResultBlock{CallID: "call-2", Status: "failed", Text: "no"}}}},
}
```

- [ ] **Step 2: Write a failing foundation codec test**

Register `FoundationDescriptors`, round-trip a `tool.message`, assert the decoded payload is `*protocol.ToolMessageV1`, and assert an envelope outside a session journal is rejected.

- [ ] **Step 3: Run tests and verify RED**

Run: `go test ./internal/protocol ./internal/eventcodec`

Expected: compilation fails because the new event and payload do not exist.

- [ ] **Step 4: Add the protocol event and validator**

```go
const EventToolMessage = "tool.message"

type ToolMessageV1 struct {
    Results []ToolResultBlock `json:"results"`
}

func (v ToolMessageV1) Validate() error {
    if err := ValidateBounds(v); err != nil {
        return fmt.Errorf("tool message bounds: %w", err)
    }
    if len(v.Results) == 0 {
        return fmt.Errorf("tool message requires results")
    }
    seen := make(map[string]struct{}, len(v.Results))
    for _, result := range v.Results {
        if err := result.Validate(); err != nil {
            return err
        }
        if _, duplicate := seen[result.CallID]; duplicate {
            return fmt.Errorf("duplicate tool result %q", result.CallID)
        }
        seen[result.CallID] = struct{}{}
    }
    return nil
}
```

- [ ] **Step 5: Register semantic and envelope validation**

Add the descriptor with redaction class `sensitive` and projection domains `session`, `context`, `activity`. Delegate semantic validation to `ToolMessageV1.Validate`. Require a session journal plus non-empty session, turn, and activity IDs.

- [ ] **Step 6: Run tests and verify GREEN**

Run: `go test ./internal/protocol ./internal/eventcodec`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/protocol/payloads.go internal/protocol/protocol_test.go internal/eventcodec/foundation.go internal/eventcodec/foundation_test.go
git commit -m "feat: add durable tool message event"
```

---

### Task 2: Project and validate complete transcript exchanges

**Files:**
- Modify: `internal/context/planner.go`
- Modify: `internal/context/planner_test.go`
- Modify: `internal/orchestrator/service.go`
- Modify: `internal/orchestrator/service_test.go`

**Interfaces:**
- Produces: one `tool_message` content source per `ContentToolResult`.
- Produces: compatibility source IDs `<assistant-event-id>:legacy-tool-result:<call-id>`.
- Produces: local structural failure before `Provider.Prepare`.
- Consumes: terminal turn events `turn.completed`, `turn.failed`, and `turn.interrupted`.

- [ ] **Step 1: Write failing canonical projection tests**

Create assistant-tool-use plus canonical `ToolMessageV1` events. Assert the plan contains `assistant_message` followed by `tool_message` with matching call IDs.

- [ ] **Step 2: Write failing compatibility tests**

Add `TestContextPlanSynthesizesMissingResultForTerminalHistoricalTurn`: a tool call whose TurnID later has `turn.completed` receives a role-tool compatible result with status `uncertain`, text `historical tool result unavailable`, and compatibility provenance.

Add `TestContextPlanRejectsMissingResultForNonTerminalTurn`: the same call without a terminal event fails with its call ID and `unresolved tool call`.

- [ ] **Step 3: Write failing malformed exchange tests**

Use table cases for duplicate, unknown, and late results. Require call ID and source event ID in each error. Add two-call ordering coverage.

- [ ] **Step 4: Run tests and verify RED**

Run: `go test ./internal/context -run 'Tool|Transcript|Historical' -count=1`

Expected: canonical tool events are ignored and historical calls remain unmatched.

- [ ] **Step 5: Add decoding and source helpers**

```go
func toolMessage(event protocol.EventRecord) (protocol.ToolMessageV1, bool)
func terminalTurns(events []protocol.EventRecord) map[protocol.TurnID]struct{}
func resultSource(id, provenance string, result protocol.ToolResultBlock) (protocol.ContentSource, error)
```

Support decoded pointer, decoded value, and raw payload. Split a multi-result payload into one source per result using deterministic `:<index>` suffixes.

- [ ] **Step 6: Implement a transcript state machine**

```go
type transcriptEntry struct {
    source  protocol.ContentSource
    eventID protocol.EventID
    turnID  protocol.TurnID
}

type pendingExchange struct {
    assistant transcriptEntry
    ordered   []string
    remaining map[string]struct{}
}
```

On assistant tool use, create a pending exchange. On tool result, remove one outstanding ID. Before a new user/assistant message or at end-of-history, synthesize only when the pending turn is terminal; otherwise return an unresolved error. Reject unknown, duplicate, and late results.

- [ ] **Step 7: Map tool sources to provider role `tool`**

Update `contextMessages`:

```go
switch source.Kind {
case "user_message":
    role = "user"
case "assistant_message":
    role = "assistant"
case "tool_message":
    role = "tool"
}
```

Add an orchestrator test asserting `Provider.Prepare` receives assistant/tool roles with matching IDs.

- [ ] **Step 8: Run focused tests and verify GREEN**

Run: `go test ./internal/context ./internal/orchestrator -run 'Tool|Transcript|Historical|ContextMessages' -count=1`

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add internal/context/planner.go internal/context/planner_test.go internal/orchestrator/service.go internal/orchestrator/service_test.go
git commit -m "feat: validate durable tool exchanges in context"
```

---

### Task 3: Persist every tool result atomically and remove the memory overlay

**Files:**
- Modify: `internal/orchestrator/service.go`
- Modify: `internal/orchestrator/service_test.go`
- Modify: `internal/orchestrator/subagent.go`
- Modify: `internal/orchestrator/subagent_test.go`
- Modify: `internal/orchestrator/recovery.go`
- Modify: `internal/orchestrator/recovery_test.go`

**Interfaces:**
- Changes: `appendActivityEvidence` accepts an optional sanitized `*protocol.ToolResultBlock`.
- Changes: tool authorization denial writes a denied result in the decision transaction; provider authorization does not.
- Removes: `extraMessages` and the `extra` parameter of `runProviderActivity`.

- [ ] **Step 1: Write a failing atomic transaction test**

Run a successful observation call. Find the append containing `EventActivitySucceeded`; assert it also contains exactly one `EventToolMessage`, both share one transaction ID, and the result contains the original call ID plus sorted evidence IDs.

- [ ] **Step 2: Write failing terminal-path cases**

Cover success, unknown tool, resource drift, execution error/uncertain, authorization denial, and cancellation. Each terminal provider-visible tool activity must have one same-transaction tool message with a conservative status. Preview-only activity terminals must have none.

- [ ] **Step 3: Write a failing later-turn/restart regression**

Run a tool-using turn against a recording repository. Construct a fresh service and run a later user turn. Assert its first provider request reconstructs the historical assistant call and result without any in-memory state.

- [ ] **Step 4: Write a failing append barrier test**

Make terminal/result append fail and assert no later `Provider.Prepare` or `Provider.Stream` occurs.

- [ ] **Step 5: Run tests and verify RED**

Run: `go test ./internal/orchestrator -run 'ToolResult|ToolMessage|LaterTurn|Restart|Continuation' -count=1`

Expected: terminal transactions lack `tool.message`, and later-turn history is malformed.

- [ ] **Step 6: Extend the shared terminal append**

```go
func (s *Service) appendActivityEvidence(
    ctx context.Context,
    request StartTurnRequest,
    state *turnState,
    activityID protocol.ActivityID,
    label, status string,
    toolResult *protocol.ToolResultBlock,
    records []protocol.EvidenceRecord,
    fileChanges ...*protocol.FileChangedV1,
) error
```

When `toolResult != nil`, append `EventToolMessage` with a deep-copied result to the same `values` slice before `activityEvents`.

- [ ] **Step 7: Sanitize before persistence**

For successful observation/mutation paths, attach evidence IDs, sanitize, then append. For execution errors, build and sanitize `uncertain` or `interrupted_no_effect` before the detached terminal append. Preview-only terminals pass `nil`.

- [ ] **Step 8: Persist synthetic and denied results atomically**

Update `appendSyntheticToolFailure` to include a failed tool message in its transaction. Extend only the tool-authorization path so denial includes `ToolResultBlock{CallID: callID, Status: "denied", Text: sanitizedReason}`; provider authorization passes no tool result.

- [ ] **Step 9: Persist subagent results atomically**

In `attachSubagentReceipt`, build and sanitize the JSON result before `turnEvents`, then include `EventToolMessage` in the same transaction as activity success, evidence links, and `EventSubagentResultAttached`. Mirror the event during recovered attachment in `recovery.go`.

- [ ] **Step 10: Remove the in-memory overlay**

Delete `extraMessages`, remove the `extra []protocol.ModelMessage` parameter, and build every provider request solely from `contextMessages(contextPlan)`. Keep `completedTools` for limits.

- [ ] **Step 11: Run focused tests and verify GREEN**

Run: `go test ./internal/orchestrator -run 'ToolResult|ToolMessage|LaterTurn|Restart|Continuation|Subagent' -count=1`

Expected: PASS.

- [ ] **Step 12: Commit**

```bash
git add internal/orchestrator/service.go internal/orchestrator/service_test.go internal/orchestrator/subagent.go internal/orchestrator/subagent_test.go internal/orchestrator/recovery.go internal/orchestrator/recovery_test.go
git commit -m "fix: persist tool results before continuation"
```

---

### Task 4: Make budgeting and compaction exchange-atomic

**Files:**
- Modify: `internal/context/planner.go`
- Modify: `internal/context/planner_test.go`
- Modify: `internal/compaction/selector.go`
- Modify: `internal/compaction/selector_test.go`

**Interfaces:**
- Produces: groups containing one ordinary source or one assistant tool-use source plus all matching results.
- Produces: compaction cutoffs that cannot divide a tool exchange.

- [ ] **Step 1: Write failing budget tests**

Choose a window where the assistant source fits but the complete assistant/result group does not. Assert both are excluded. Add a case where both fit and remain ordered.

- [ ] **Step 2: Write failing compaction tests**

Build committed events where the default cutoff falls between an assistant call and `tool.message`. Assert the cutoff moves before the assistant so both stay retained. Add a multi-result case.

- [ ] **Step 3: Run tests and verify RED**

Run: `go test ./internal/context ./internal/compaction -run 'Budget.*Tool|Tool.*Boundary|Exchange' -count=1`

Expected: budgeting or compaction splits the exchange.

- [ ] **Step 4: Group before token selection**

```go
type sourceGroup struct {
    sources []protocol.ContentSource
    tokens  int64
}

func groupContextSources(sources []protocol.ContentSource) ([]sourceGroup, error)
```

Reuse transcript call-ID semantics. Include or exclude all sources in one group. Ordinary system/user/assistant sources remain one-source groups.

- [ ] **Step 5: Protect compaction cutoff**

Calculate assistant/result event ranges. If a range crosses `cutoff`, move the cutoff to one event before the assistant. Return `ErrNothingToCompact` when this precedes `start`. Never move forward.

- [ ] **Step 6: Normalize tool events safely**

Teach `normalizedEventText` to decode `ToolMessageV1` and emit only call ID, status, and evidence IDs. Do not include result text or JSON in summary input.

- [ ] **Step 7: Run tests and verify GREEN**

Run: `go test ./internal/context ./internal/compaction -run 'Budget.*Tool|Tool.*Boundary|Exchange|ToolResult' -count=1`

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/context/planner.go internal/context/planner_test.go internal/compaction/selector.go internal/compaction/selector_test.go
git commit -m "fix: keep tool exchanges atomic in context"
```

---

### Task 5: Prove wire validity and run the verification gate

**Files:**
- Modify: `internal/provider/openaicompat/adapter_test.go`
- Modify: `internal/orchestrator/service_test.go`

**Interfaces:**
- Consumes: canonical model messages from Tasks 2-4.
- Produces: regression coverage for the original provider error.

- [ ] **Step 1: Add the wire-history regression**

Send system, user, assistant-with-two-tool-calls, two role-tool messages, and a later user message through the adapter. Capture `/chat/completions` JSON and assert every `tool_calls[].id` has exactly one matching `tool_call_id` before the later user message.

- [ ] **Step 2: Add malformed-history rejection coverage**

Construct an unmatched assistant call and assert Yordam returns a local transcript error before the HTTP server receives a request.

- [ ] **Step 3: Run adapter regressions**

Run: `go test ./internal/provider/openaicompat ./internal/orchestrator -run 'Tool.*History|Missing.*Tool|LaterTurn|Restart' -count=1`

Expected: PASS.

- [ ] **Step 4: Format and run static checks**

```bash
gofmt -w internal/protocol/payloads.go internal/protocol/protocol_test.go internal/eventcodec/foundation.go internal/eventcodec/foundation_test.go internal/context/planner.go internal/context/planner_test.go internal/orchestrator/service.go internal/orchestrator/service_test.go internal/orchestrator/subagent.go internal/orchestrator/subagent_test.go internal/orchestrator/recovery.go internal/orchestrator/recovery_test.go internal/compaction/selector.go internal/compaction/selector_test.go internal/provider/openaicompat/adapter_test.go
go vet ./...
```

Expected: formatting produces no diff beyond the planned files; `go vet` exits 0.

- [ ] **Step 5: Run all tests**

Run: `go test ./... -count=1`

Expected: PASS with no package failures.

- [ ] **Step 6: Inspect scope and hygiene**

```bash
git diff --check
git status --short
git diff --stat HEAD~4..HEAD
```

Expected: no whitespace errors; only planned files and approved documents changed; unrelated pre-existing untracked files remain untouched.

- [ ] **Step 7: Commit**

```bash
git add internal/provider/openaicompat/adapter_test.go internal/orchestrator/service_test.go
git commit -m "test: cover durable tool result transcripts"
```
