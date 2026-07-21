# Durable Tool-Result Transcript Design

## Problem

Yordam durably records assistant messages containing tool-use blocks, but it
keeps the corresponding tool-result messages only in the in-memory
`extraMessages` slice for the current `RunTurn`. A later turn reconstructs the
assistant tool call from the session journal without reconstructing its tool
result. Strict OpenAI-compatible providers reject that malformed transcript
with errors such as `No tool output found for function call <call-id>`.

The current same-turn retention fix preserves results across multiple provider
continuations within one `RunTurn`, but it does not survive the next turn,
process restart, or replay from the journal.

## Goals

- Persist every provider-visible tool result before provider continuation.
- Commit the tool result atomically with the terminal tool activity and its
  evidence links.
- Reconstruct structurally valid tool exchanges on later turns and restarts.
- Keep assistant tool-use and matching tool-result messages together during
  token budgeting and compaction.
- Repair existing sessions that contain historical assistant tool calls but no
  durable tool-result transcript event, without rewriting their journals.
- Reject malformed tool exchanges inside Yordam before provider dispatch.
- Preserve provider-neutral protocol semantics and existing authorization,
  recovery, evidence, and transaction guarantees.

## Non-Goals

- Reconstruct the exact historical output text when it was never persisted.
- Rewrite or migrate existing journal files in place.
- Change tool execution ordering, authorization policy, or evidence storage.
- Add provider-specific recovery behavior.

## Chosen Architecture

Introduce a foundation event named `tool.message` with a versioned payload that
contains one or more validated `ToolResultBlock` values. It is a transcript
event, analogous to `user.message` and `assistant.message`, and projects to a
provider-neutral model message with role `tool`.

The orchestrator sanitizes the final tool result, attaches sorted evidence IDs,
and includes the `tool.message` event in the same journal transaction as the
terminal activity outcome and evidence records/links. A committed terminal tool
activity therefore cannot exist without its provider-visible result for newly
written data. Conversely, an append failure prevents provider continuation.

The context planner projects `tool.message` events directly. The provider loop
then rebuilds every continuation from canonical journal history and no longer
depends on an in-memory `extraMessages` transcript overlay.

This design is preferred over embedding model results in `ActivityOutcomeV1`:
activity lifecycle payloads remain execution-domain records, while transcript
events remain model-domain records. It is also preferred over reconstructing
all results from evidence because evidence does not necessarily retain the
exact bounded provider-visible result and preview/mutation activities can share
one model call ID.

## Protocol and Event Semantics

Add:

```go
const EventToolMessage = "tool.message"

type ToolMessageV1 struct {
    Results []ToolResultBlock `json:"results"`
}
```

`ToolMessageV1.Validate` requires:

- at least one result;
- every result to pass `ToolResultBlock.Validate`;
- non-empty, unique call IDs within the payload;
- deterministic provider order;
- protocol size bounds.

The foundation codec registers `tool.message` at payload version 1. Unknown
future versions retain the existing fail-closed behavior.

Each tool intent may be written as its own `tool.message`. A future batching
optimization may place several results in one event, but the initial
implementation does not require batching.

## Write Path

For every terminal tool outcome, including success, denial, failure,
cancellation, resource drift, and uncertain execution:

1. Produce a `ToolResultBlock` with the original model call ID.
2. Sanitize text or JSON through the active runtime admission boundary.
3. Attach the immutable evidence IDs already produced for the activity.
4. Build the terminal activity, file-change, evidence, and `tool.message`
   events.
5. Append all events in one journal transaction.
6. Cross the terminal-action durability barrier.
7. Permit the next provider continuation.

Synthetic failures follow the same write path. If sanitization or append fails,
the turn stops and no further provider request starts.

The transient `runtime.tool_result_available` application event remains a UI
notification and is not used to reconstruct provider history.

## Read and Compatibility Path

The context planner converts canonical `tool.message` events into content
sources of kind `tool_message`. `contextMessages` maps that kind to model role
`tool`.

For journals written before this change, the planner performs a read-only
compatibility pass. Synthesis is eligible only when the assistant tool call
belongs to a turn that has a later committed terminal event (`turn.completed`,
`turn.failed`, or `turn.interrupted`). A tool call in a non-terminal turn stays
unresolved so normal recovery can handle it.

For each eligible historical turn, the pass:

1. Track assistant tool-use blocks in transcript order.
2. Match canonical `tool.message` results by call ID.
3. For a historical call with no durable result, insert a synthetic tool result
   immediately after its assistant tool-use message:

```text
status: uncertain
text: historical tool result unavailable
```

The synthetic result preserves the original call ID. The planner emits one
role-`tool` message per missing call, in provider order, marks each source with
legacy compatibility provenance, and never claims that the tool succeeded. It
does not modify the source journal or manufacture evidence.

Compatibility synthesis applies only while projecting historical committed
events. It must not authorize continuation past an active unresolved recovery
condition; existing recovery and turn-lane checks remain authoritative.

## Transcript Validation

Before provider preparation, validate the final message sequence as an atomic
tool-exchange state machine:

- every assistant tool-use call ID is unique within its assistant message;
- every tool result references an outstanding call ID;
- every outstanding call receives exactly one result;
- all results for an assistant tool-use message appear before the next user or
  assistant message;
- duplicate, unknown, or late results fail locally with a typed transcript
  error.

Provider adapters continue validating block capabilities, but they no longer
serve as the first structural validation boundary.

## Budgeting and Compaction

Assistant tool-use messages and their matching tool-result messages form one
exchange group. Context token selection includes or excludes the complete
group. It never keeps only the assistant call or only a result.

Compaction ranges must not split an exchange group. When a selected boundary
would split one, the selector moves the boundary to include or exclude the
whole group. A compaction summary may describe the tool outcome, but the
retained suffix must still satisfy transcript validation.

Historical compatibility results participate in grouping exactly like
canonical results, though their provenance remains synthetic.

## Error Handling and Recovery

- Sanitization failure: stop before committing a terminal transcript result.
- Atomic append failure: stop the turn; do not call the provider again.
- Crash before the transaction: existing activity recovery determines the
  effect state; compatibility projection may close only historical committed
  transcript structure and cannot bypass recovery.
- Crash after the transaction: replay observes both terminal outcome and tool
  result.
- Invalid or duplicate canonical results: fail context planning with a local,
  typed diagnostic containing the call ID and source event ID.
- Missing result in an old terminal history: synthesize the conservative
  `uncertain` compatibility result.
- Missing result in a non-terminal turn: fail locally and leave recovery in
  control.

## Testing Strategy

Protocol tests prove payload validation, bounds, duplicate rejection, and codec
round trips.

Orchestrator tests prove:

- the result event is in the same transaction as terminal activity/evidence;
- provider continuation starts only after that transaction commits;
- successful, failed, denied, cancelled, drift, and uncertain paths persist a
  result;
- multiple tool calls and multiple provider rounds preserve provider order;
- a later `RunTurn` reconstructs every prior result without in-memory state;
- an append failure prevents provider continuation.

Context tests prove:

- canonical tool messages project with role `tool`;
- old sessions receive conservative compatibility results;
- duplicate, unknown, and late canonical results fail locally;
- a missing result fails for a non-terminal turn but is synthesized for an
  eligible historical terminal turn;
- budgeting includes or excludes complete exchange groups.

Compaction tests prove boundaries cannot split tool exchanges and compacted
contexts remain transcript-valid.

Adapter regression tests assert the final OpenAI-compatible wire history pairs
every `tool_calls[].id` with exactly one subsequent `tool_call_id` before the
next conversational message.

## Acceptance Criteria

- Starting a new turn after a tool-using turn no longer produces a missing tool
  output provider error.
- Restarting Yordam between the tool turn and the next user turn produces the
  same valid provider transcript.
- Existing affected sessions remain usable without journal rewriting and never
  infer historical success.
- Same-turn multi-round and multi-tool behavior remains valid.
- No provider continuation can start until the matching result is durable.
- Context budgeting and compaction cannot create an unmatched tool call.
- Targeted regression tests and the complete Go test suite pass.
