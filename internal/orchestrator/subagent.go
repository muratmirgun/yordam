package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	receiptprojector "github.com/muratmirgun/yordam/internal/subagent"
	"github.com/muratmirgun/yordam/internal/tooling"
	subagenttool "github.com/muratmirgun/yordam/internal/tools/subagent"
)

// runSubagentIntent is the sole dispatch path for the built-in subagent
// descriptor. It deliberately does not invoke ToolService: doing so would let
// an ordinary tool execution create an unbound child session.
func (s *Service) runSubagentIntent(ctx context.Context, lease managedOperationLease, request StartTurnRequest, state *turnState, intent protocol.ToolUseBlock) (protocol.ToolResultBlock, error) {
	call, err := decodeSubagentCall(intent.Arguments)
	if err != nil {
		return s.rejectSubagentIntent(ctx, request, state, intent, "invalid subagent call")
	}
	limits := request.Runtime.Body.Limits.Subagents
	if !limits.Enabled {
		return s.rejectSubagentIntent(ctx, request, state, intent, "subagents are disabled")
	}
	if state.subagentDepth > 0 {
		return s.rejectSubagentIntent(ctx, request, state, intent, "subagent depth limit reached")
	}
	if state.activeSubagent {
		return s.rejectSubagentIntent(ctx, request, state, intent, "a subagent is already active")
	}
	if state.subagentAttempts >= limits.MaxPerTurn || state.subagentAttempts >= protocol.MaxSubagentAttemptsPerTurn {
		return s.rejectSubagentIntent(ctx, request, state, intent, "subagent attempt limit reached")
	}
	if s.deps.ChildSessions == nil || s.deps.Children == nil || s.deps.ParentSessions == nil || s.deps.Evidence == nil {
		return protocol.ToolResultBlock{}, fmt.Errorf("subagent orchestration dependencies are incomplete")
	}
	if s.deps.Tools == nil || s.deps.Authorization == nil {
		return protocol.ToolResultBlock{}, fmt.Errorf("subagent authorization dependencies are incomplete")
	}
	activityID := protocol.ActivityID(stableID("activity", string(request.Command.CommandID), "subagent", intent.CallID))
	planRequest := tooling.PlanRequest{TurnID: state.turnID, ActivityID: activityID, CallID: intent.CallID, Alias: intent.Alias, Arguments: protocol.DeepCopy(intent.Arguments), RuntimeGenerationID: request.Runtime.ID}
	planner, ok := s.deps.Tools.(interface {
		PlanOrchestratedAuthorization(context.Context, tooling.PlanRequest, string) (tooling.ActionHandle, protocol.ActionPlan, error)
	})
	if !ok {
		return protocol.ToolResultBlock{}, fmt.Errorf("subagent authorization planner is unavailable")
	}
	_, plan, err := planner.PlanOrchestratedAuthorization(ctx, planRequest, subagenttool.Kind)
	if err != nil {
		return protocol.ToolResultBlock{}, err
	}
	authorizationRequest, err := toolAuthorizationRequest(request, state, planRequest, plan, nil)
	if err != nil {
		return protocol.ToolResultBlock{}, err
	}
	label := "subagent-" + intent.CallID
	planned, err := s.activityEvents(*state, request.Runtime.ID, activityID, label+"-planned", []struct {
		kind    string
		payload any
	}{{protocol.EventActivityPlanned, activityPlan(plan, "sequential child orchestration", nil)}, {protocol.EventExecutionPlanDeclared, protocol.ExecutionPlanDeclaredV1{Plan: plan}}, {protocol.EventAuthorizationRequested, protocol.AuthorizationRequestedV1{Request: authorizationRequest}}})
	if err != nil {
		return protocol.ToolResultBlock{}, err
	}
	if err := s.append(ctx, state, label+"-planned", planned); err != nil {
		return protocol.ToolResultBlock{}, err
	}
	state.activeActivityID, state.activeStarted, state.activeDispatched = activityID, false, false
	if _, err := s.authorizeActivity(ctx, request, state, activityID, intent.CallID, label, authorizationRequest); err != nil {
		return protocol.ToolResultBlock{}, err
	}

	childID, err := s.deps.ChildSessions.ReserveSessionID()
	if err != nil {
		return protocol.ToolResultBlock{}, err
	}
	attemptNumber := state.subagentAttempts + 1
	childCommandID := protocol.CommandID(stableID("subagent-child-command", string(request.Command.CommandID), intent.CallID, fmt.Sprint(attemptNumber)))
	manifest := protocol.SubagentManifestV1{
		AttemptID:            protocol.DelegationAttemptID(stableID("subagent-attempt", string(request.Command.CommandID), intent.CallID, fmt.Sprint(attemptNumber))),
		ParentSessionID:      request.SessionID,
		ParentCursor:         state.head,
		ChildSessionID:       childID,
		ChildTaskID:          protocol.TaskID(stableID("task", string(childCommandID))),
		ChildTurnID:          protocol.TurnID(stableID("turn", string(childCommandID))),
		RuntimeGenerationID:  request.Runtime.ID,
		SkillCatalogRevision: request.Runtime.Body.SkillCatalogRevision,
		MaxToolCalls:         limits.MaxToolCalls,
		Deadline:             time.Now().UTC().Add(time.Duration(limits.TimeoutNanos)),
	}
	if err := manifest.Validate(); err != nil {
		return protocol.ToolResultBlock{}, err
	}
	requestEvents, err := s.turnEvents(*state, request.Runtime.ID, "subagent-request-"+intent.CallID, []struct {
		kind    string
		payload any
	}{
		{protocol.EventSubagentRequested, protocol.SubagentRequestedV1{Call: call, Manifest: manifest}},
		{protocol.EventSubagentWaiting, protocol.SubagentWaitingV1{AttemptID: manifest.AttemptID, ChildSessionID: manifest.ChildSessionID}},
	})
	if err != nil {
		return protocol.ToolResultBlock{}, err
	}
	if err := s.append(ctx, state, "subagent-request-"+intent.CallID, requestEvents); err != nil {
		return protocol.ToolResultBlock{}, err
	}
	state.subagentAttempts++
	state.activeSubagent = true
	defer func() { state.activeSubagent = false }()

	var receipt protocol.SubagentReceiptV1
	err = lease.Yield(ctx, func(childParent context.Context) error {
		childCtx, cancel := context.WithDeadline(childParent, manifest.Deadline)
		defer cancel()
		var runErr error
		receipt, runErr = s.deps.Children.RunChild(childCtx, ChildRunRequest{Manifest: manifest, Call: call, Parent: protocol.DeepCopy(request)})
		return runErr
	})
	if err != nil {
		return protocol.ToolResultBlock{}, err
	}
	if !sameSubagentManifest(receipt.Manifest, manifest) || receipt.TerminalCursor.JournalID != protocol.JournalID(childID) || receipt.Validate() != nil {
		return protocol.ToolResultBlock{}, fmt.Errorf("subagent receipt does not bind the requested child")
	}
	if err := s.verifyChildReceipt(ctx, manifest, receipt); err != nil {
		return protocol.ToolResultBlock{}, err
	}
	if err := s.verifyParentHead(ctx, state); err != nil {
		return protocol.ToolResultBlock{}, err
	}
	return s.attachSubagentReceipt(ctx, request, state, intent, activityID, receipt)
}

func (s *Service) verifyParentHead(ctx context.Context, state *turnState) error {
	page, err := s.repository.ReadRange(ctx, journal.ReadRangeRequest{Journal: state.ref, Limit: 1})
	if err != nil {
		return err
	}
	if page.Head != state.head {
		return fmt.Errorf("parent journal changed while subagent was running")
	}
	return nil
}

// verifyChildReceipt closes the handoff boundary: a coordinator cannot merely
// return a plausible receipt; it must point at the exact committed child event.
func (s *Service) verifyChildReceipt(ctx context.Context, manifest protocol.SubagentManifestV1, receipt protocol.SubagentReceiptV1) error {
	inspection, err := s.deps.ChildSessions.InspectSession(ctx, manifest.ChildSessionID)
	if err != nil {
		return err
	}
	if inspection.Journal.Kind != protocol.JournalSession || inspection.Journal.ID != protocol.JournalID(manifest.ChildSessionID) || inspection.Head.CommitSeq < receipt.TerminalCursor.CommitSeq {
		return fmt.Errorf("child receipt journal is not durably committed")
	}
	for _, event := range inspection.Events {
		cursor := protocol.CommittedCursor{JournalKind: event.Envelope.JournalKind, JournalID: event.Envelope.JournalID, CommitSeq: event.Envelope.Seq, TransactionID: event.Envelope.TransactionID}
		if event.Envelope.Kind != protocol.EventSubagentReceipt || cursor != receipt.TerminalCursor {
			continue
		}
		var committed protocol.SubagentReceiptV1
		if err := json.Unmarshal(event.Envelope.Payload, &committed); err == nil && reflect.DeepEqual(committed, receipt) {
			return nil
		}
	}
	return fmt.Errorf("child receipt is absent or does not match its terminal cursor")
}

func decodeSubagentCall(raw json.RawMessage) (protocol.SubagentCallV1, error) {
	var call protocol.SubagentCallV1
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&call); err != nil {
		return protocol.SubagentCallV1{}, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return protocol.SubagentCallV1{}, fmt.Errorf("subagent call contains multiple values")
		}
		return protocol.SubagentCallV1{}, err
	}
	if err := call.Validate(); err != nil {
		return protocol.SubagentCallV1{}, err
	}
	return call, nil
}

func (s *Service) rejectSubagentIntent(ctx context.Context, request StartTurnRequest, state *turnState, intent protocol.ToolUseBlock, reason string) (protocol.ToolResultBlock, error) {
	if err := s.appendSyntheticToolFailure(ctx, request, state, intent, nil, reason); err != nil {
		return protocol.ToolResultBlock{}, err
	}
	return protocol.ToolResultBlock{CallID: intent.CallID, Status: "failed", Text: reason}, nil
}

func (s *Service) attachSubagentReceipt(ctx context.Context, request StartTurnRequest, state *turnState, intent protocol.ToolUseBlock, activityID protocol.ActivityID, receipt protocol.SubagentReceiptV1) (protocol.ToolResultBlock, error) {
	encoded, err := canonicaljson.Marshal(receipt)
	if err != nil {
		return protocol.ToolResultBlock{}, err
	}
	digest, err := canonicaljson.Digest(receipt)
	if err != nil {
		return protocol.ToolResultBlock{}, err
	}
	parent, err := s.deps.ParentSessions.InspectSession(ctx, request.SessionID)
	if err != nil {
		return protocol.ToolResultBlock{}, fmt.Errorf("inspect parent session: %w", err)
	}
	if parent.Session.Workspace.ID == "" {
		return protocol.ToolResultBlock{}, fmt.Errorf("parent session workspace identity is empty")
	}
	candidate := protocol.EvidenceCandidate{
		ID: protocol.EvidenceID(stableID("evidence", string(request.Command.CommandID), string(receipt.Manifest.AttemptID), digest.Value)), Kind: "subagent_receipt",
		WorkspaceID: protocol.WorkspaceID(parent.Session.Workspace.ID), SessionID: request.SessionID, MediaType: "application/json",
		ProducingActivityID: activityID,
		Actor:               protocol.ActorRef{ID: "orchestrator", Kind: protocol.ActorSystem}, Subject: protocol.SubjectRef{Kind: "subagent_attempt", ID: string(receipt.Manifest.AttemptID)}, Content: encoded, Limit: protocol.MaxSubagentReceiptSummaryBytes,
	}
	record, err := s.deps.Evidence.Put(ctx, candidate)
	if err != nil {
		return protocol.ToolResultBlock{}, err
	}
	if err := record.Validate(); err != nil || record.Body.ID != candidate.ID || record.Body.SessionID != request.SessionID || record.Body.ProducingActivityID != candidate.ProducingActivityID || record.Body.Kind != candidate.Kind {
		return protocol.ToolResultBlock{}, fmt.Errorf("subagent receipt evidence binding mismatch")
	}
	values := []struct {
		kind    string
		payload any
	}{
		{protocol.EventActivitySucceeded, protocol.ActivityOutcomeV1{Status: "succeeded", OutputEvidenceIDs: []protocol.EvidenceID{record.Body.ID}}},
		{protocol.EventEvidenceRecorded, protocol.EvidenceRecordedV1{Record: record}},
		{protocol.EventEvidenceLinked, protocol.EvidenceLinkedV1{EvidenceID: record.Body.ID, Subject: candidate.Subject, Relation: "output"}},
		{protocol.EventSubagentResultAttached, protocol.SubagentResultAttachedV1{AttemptID: receipt.Manifest.AttemptID, ChildSessionID: receipt.Manifest.ChildSessionID, TerminalCursor: receipt.TerminalCursor, ReceiptDigest: digest, ReceiptEvidenceID: record.Body.ID}},
	}
	events, err := s.turnEvents(*state, request.Runtime.ID, "subagent-attachment-"+intent.CallID, values)
	if err != nil {
		return protocol.ToolResultBlock{}, err
	}
	for index := range events {
		events[index].ActivityID = candidate.ProducingActivityID
	}
	if err := s.append(ctx, state, "subagent-attachment-"+intent.CallID, events); err != nil {
		return protocol.ToolResultBlock{}, err
	}
	state.activeActivityID, state.activeStarted, state.activeDispatched = "", false, false
	return protocol.ToolResultBlock{CallID: intent.CallID, Status: receipt.Status, JSON: encoded, EvidenceIDs: []protocol.EvidenceID{record.Body.ID}}, nil
}

// canonicalSubagentDescriptor is intentionally kept alongside the interception
// path: aliases and lookalike descriptors are ordinary tools.
func canonicalSubagentDescriptor(descriptor protocol.ToolDescriptor) bool {
	return subagenttool.IsCanonicalDescriptor(descriptor)
}

func sameSubagentManifest(left, right protocol.SubagentManifestV1) bool {
	leftDigest, leftErr := canonicaljson.Digest(left)
	rightDigest, rightErr := canonicaljson.Digest(right)
	return leftErr == nil && rightErr == nil && leftDigest == rightDigest
}

func childReceipt(manifest protocol.SubagentManifestV1, status, summary string, cursor protocol.CommittedCursor, usage protocol.ModelUsage, public *protocol.PublicError, unknown []protocol.ActivityID) protocol.SubagentReceiptV1 {
	if usage == (protocol.ModelUsage{}) {
		usage = unknownUsage()
	}
	if len(summary) > protocol.MaxSubagentReceiptSummaryBytes {
		summary = summary[:protocol.MaxSubagentReceiptSummaryBytes]
	}
	if unknown == nil {
		unknown = []protocol.ActivityID{}
	}
	return protocol.SubagentReceiptV1{Status: status, Summary: summary, Manifest: manifest, TerminalCursor: cursor, ChangedFiles: []string{}, CommandsAndTests: []string{}, Usage: usage, EvidenceIDs: []protocol.EvidenceID{}, UnknownEffects: unknown, Error: protocol.DeepCopy(public)}
}

// projectChildReceipt reads the child's exact durable journal prefix while
// constructing its terminal transaction, so assistant response prose can
// never become effect evidence.
func (s *Service) projectChildReceipt(ctx context.Context, state *turnState, manifest protocol.SubagentManifestV1, status, summary string, cursor protocol.CommittedCursor, usage protocol.ModelUsage, public *protocol.PublicError) (protocol.SubagentReceiptV1, error) {
	// The receipt cursor is predicted for the terminal transaction. Evidence
	// must stop at the actual committed head, never chase that future cursor.
	events, err := s.durablePrefix(ctx, state.ref, state.head)
	if err != nil {
		return protocol.SubagentReceiptV1{}, err
	}
	return receiptprojector.ProjectReceipt(manifest, cursor, status, summary, usage, public, events), nil
}

func (s *Service) durablePrefix(ctx context.Context, ref protocol.JournalRef, through protocol.CommittedCursor) ([]protocol.EventRecord, error) {
	if through.CommitSeq != 0 && (through.JournalKind != ref.Kind || through.JournalID != ref.ID) {
		return nil, fmt.Errorf("durable prefix cursor does not bind journal")
	}
	after := protocol.CommittedCursor{}
	events := make([]protocol.EventRecord, 0)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := s.repository.ReadRange(ctx, journal.ReadRangeRequest{Journal: ref, After: after, Limit: 1000})
		if err != nil {
			return nil, err
		}
		for _, event := range page.Events {
			if through.CommitSeq == 0 {
				events = append(events, event)
				continue
			}
			if event.Envelope.Seq > through.CommitSeq {
				return nil, fmt.Errorf("journal range records exceed durable prefix")
			}
			if event.Envelope.Seq == through.CommitSeq {
				if event.Envelope.TransactionID != through.TransactionID {
					return nil, fmt.Errorf("durable prefix marker does not bind target transaction")
				}
				events = append(events, event)
				return events, nil
			}
			events = append(events, event)
		}
		if page.Cursor.JournalKind != ref.Kind || page.Cursor.JournalID != ref.ID {
			return nil, fmt.Errorf("journal range cursor does not bind journal")
		}
		if through.CommitSeq != 0 && page.Cursor.CommitSeq > through.CommitSeq {
			return nil, fmt.Errorf("journal range cursor exceeds durable prefix")
		}
		if through.CommitSeq != 0 && page.Cursor == through {
			return events, nil
		}
		if through.CommitSeq != 0 && page.Cursor.CommitSeq >= through.CommitSeq {
			return nil, fmt.Errorf("durable prefix marker is absent")
		}
		if !page.More {
			if through.CommitSeq != 0 {
				return nil, fmt.Errorf("journal range ended before durable prefix")
			}
			return events, nil
		}
		if page.Cursor == after || page.Cursor.CommitSeq <= after.CommitSeq {
			return nil, fmt.Errorf("journal range cursor did not advance")
		}
		after = page.Cursor
	}
}

func assistantSummary(message protocol.AssistantMessageV1) string {
	var summary strings.Builder
	for _, block := range message.Blocks {
		if block.Kind == protocol.ContentText {
			summary.WriteString(block.Text)
		}
	}
	return summary.String()
}
