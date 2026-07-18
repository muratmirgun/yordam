package jsonl

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
)

// UpcastState is the complete deterministic state carried between legacy
// records. OpenCalls is keyed by derived turn ID and legacy call ID.
type UpcastState struct {
	SessionID       protocol.SessionID
	TurnOrdinal     uint64
	ActivityOrdinal uint64
	ActiveTurnID    protocol.TurnID
	OpenCalls       map[string][]protocol.ActivityID

	activeTaskID   protocol.TaskID
	turnAnchor     protocol.EventID
	requestedCalls map[string]bool
	startedCalls   map[string]uint64
	uncertainCalls map[string]bool
}

func UpcastV1(source protocol.LegacySource, state UpcastState) (protocol.EventRecord, UpcastState, []protocol.Diagnostic) {
	next := cloneUpcastState(state)
	if next.SessionID == "" {
		next.SessionID = source.SessionID
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(next.SessionID)}
	var diagnostics []protocol.Diagnostic

	if source.Kind == string(domain.EventUserMessage) {
		diagnostics = append(diagnostics, unmatchedCallDiagnostics(ref, source, next)...)
		next.TurnOrdinal++
		next.ActivityOrdinal = 0
		next.turnAnchor = source.EventID
		next.ActiveTurnID = protocol.TurnID(deriveUpcastID("turn", next.SessionID, source.EventID, next.TurnOrdinal, 0))
		next.activeTaskID = protocol.TaskID(deriveUpcastID("task", next.SessionID, source.EventID, next.TurnOrdinal, 0))
		clear(next.OpenCalls)
		clear(next.requestedCalls)
		clear(next.startedCalls)
		clear(next.uncertainCalls)
	}

	envelope := protocol.EventEnvelope{
		SchemaVersion:  protocol.EnvelopeVersion,
		PayloadVersion: 1,
		JournalKind:    protocol.JournalSession,
		JournalID:      protocol.JournalID(next.SessionID),
		EventID:        source.EventID,
		SessionID:      next.SessionID,
		Seq:            source.Seq,
		Time:           source.Time,
		Kind:           source.Kind,
		TaskID:         next.activeTaskID,
		TurnID:         next.ActiveTurnID,
		TransactionID: protocol.TransactionID(deriveUpcastID(
			"transaction", next.SessionID, source.EventID, next.TurnOrdinal, next.ActivityOrdinal,
		)),
	}
	actor := legacyActor(source, next)
	envelope.Actor = &actor

	decoded, activityID, mappedKind, emitted := mapLegacyPayload(source, &next, ref)
	diagnostics = append(diagnostics, emitted...)
	if mappedKind != "" {
		envelope.Kind = mappedKind
	}
	if activityID != "" {
		envelope.ActivityID = activityID
	}
	payload, err := json.Marshal(decoded)
	if err != nil {
		diagnostic := migrationDiagnostic(ref, source, "migration.invalid_payload", err.Error(), nil)
		diagnostics = append(diagnostics, diagnostic)
		decoded = &protocol.DiagnosticV1{Diagnostic: diagnostic}
		envelope.Kind = protocol.EventMigrationDiagnostic
		payload, _ = json.Marshal(decoded)
	}
	envelope.Payload = protocol.CloneRawMessage(payload)

	legacy := protocol.DeepCopy(source)
	legacy.RawEnvelope = protocol.CloneRawMessage(source.RawEnvelope)
	legacy.Payload = protocol.CloneRawMessage(source.Payload)
	record := protocol.EventRecord{
		Envelope: envelope, Decoded: protocol.DeepCopy(decoded),
		RawEnvelope: protocol.CloneRawMessage(source.RawEnvelope), Legacy: &legacy,
	}

	if isLegacyTurnTerminal(source.Kind) {
		diagnostics = append(diagnostics, unmatchedCallDiagnostics(ref, source, next)...)
		next.ActiveTurnID = ""
		next.activeTaskID = ""
		next.turnAnchor = ""
		clear(next.OpenCalls)
		clear(next.requestedCalls)
		clear(next.startedCalls)
		clear(next.uncertainCalls)
	}
	return protocol.CloneEventRecord(record), cloneUpcastState(next), cloneDiagnostics(diagnostics)
}

func mapLegacyPayload(
	source protocol.LegacySource,
	state *UpcastState,
	ref protocol.JournalRef,
) (decoded any, activityID protocol.ActivityID, kind string, diagnostics []protocol.Diagnostic) {
	lossy := func(message string) {
		diagnostics = append(diagnostics, migrationDiagnostic(ref, source, "migration.lossy", message, nil))
	}
	decode := func(destination any) bool {
		if err := json.Unmarshal(source.Payload, destination); err != nil {
			diagnostics = append(diagnostics, migrationDiagnostic(ref, source, "migration.invalid_payload", err.Error(), nil))
			return false
		}
		return true
	}
	switch domain.EventKind(source.Kind) {
	case domain.EventSessionCreated:
		var legacy domain.Session
		if !decode(&legacy) {
			break
		}
		return &protocol.SessionCreatedV1{
			WorkspaceID: protocol.WorkspaceID(legacy.Workspace.ID), CanonicalPath: legacy.Workspace.CanonicalPath,
			Title: legacy.Title, Mode: string(legacy.Mode), ProviderID: protocol.ProviderID(legacy.Selection.Profile),
			ModelID: protocol.ModelID(legacy.Selection.Model),
		}, "", protocol.EventSessionCreated, diagnostics
	case domain.EventSessionTitleChanged:
		var payload struct {
			Title string `json:"title"`
		}
		if decode(&payload) {
			return &protocol.SessionTitleChangedV1{Title: payload.Title}, "", protocol.EventSessionTitleChanged, diagnostics
		}
	case domain.EventModeChanged:
		var payload domain.ModeChangedPayload
		if decode(&payload) {
			return &protocol.ModeChangedV1{Mode: string(payload.Mode)}, "", protocol.EventModeChanged, diagnostics
		}
	case domain.EventModelChanged:
		var payload domain.ModelChangedPayload
		if decode(&payload) {
			return &protocol.ModelChangedV1{ProviderID: protocol.ProviderID(payload.Selection.Profile), ModelID: protocol.ModelID(payload.Selection.Model)}, "", protocol.EventModelChanged, diagnostics
		}
	case domain.EventTrustedExecutionAcknowledged:
		var payload domain.TrustedExecutionPayload
		if decode(&payload) {
			lossy("legacy trusted execution did not record an execution profile")
			return &protocol.TrustedExecutionAcknowledgedV1{Enabled: payload.Enabled, Profile: "unknown"}, "", protocol.EventTrustedExecutionAcknowledged, diagnostics
		}
	case domain.EventUserMessage:
		var payload domain.MessagePayload
		if decode(&payload) {
			return &protocol.UserMessageV1{Content: payload.Content}, "", protocol.EventUserMessage, diagnostics
		}
	case domain.EventAssistantMessage:
		var payload domain.MessagePayload
		if decode(&payload) {
			blocks := make([]protocol.ContentBlock, 0, 1+len(payload.ToolCalls))
			if payload.Content != "" {
				blocks = append(blocks, protocol.ContentBlock{Kind: protocol.ContentText, Text: payload.Content})
			}
			intents := make([]protocol.ToolUseBlock, 0, len(payload.ToolCalls))
			for _, call := range payload.ToolCalls {
				intent := protocol.ToolUseBlock{CallID: call.ID, Alias: call.Name, Arguments: protocol.CloneRawMessage(call.Arguments)}
				intents = append(intents, intent)
				blocks = append(blocks, protocol.ContentBlock{Kind: protocol.ContentToolUse, ToolUse: &intent})
			}
			return &protocol.AssistantMessageV1{Blocks: blocks, ToolIntents: intents}, "", protocol.EventAssistantMessage, diagnostics
		}
	case domain.EventToolRequested:
		var payload domain.PreparedToolRequest
		if decode(&payload) {
			activityID = beginLegacyActivity(source, state, payload.Request.CallID)
			state.requestedCalls[legacyCallKey(state.ActiveTurnID, payload.Request.CallID)] = true
			purpose := strings.TrimSpace(payload.Summary)
			if purpose == "" {
				purpose = "legacy tool request"
			}
			lossy("legacy tool request did not record execution profiles or a canonical v2 action plan")
			actor := protocol.ActorRef{ID: protocol.ActorID(deriveUpcastID("actor", state.SessionID, source.EventID, state.TurnOrdinal, state.ActivityOrdinal)), Kind: protocol.ActorAgent, Source: "legacy_v0.1"}
			return &protocol.ActivityPlannedV1{
				Kind: string(payload.Mutation), Purpose: purpose, PurposeActor: actor, Source: "legacy_v0.1",
				RequestedProfile: "unknown", EffectiveProfile: "unknown",
			}, activityID, protocol.EventActivityPlanned, diagnostics
		}
	case domain.EventPermissionRequested:
		var payload domain.PreparedToolRequest
		if decode(&payload) {
			activityID = findOrBeginLegacyActivity(source, state, payload.Request.CallID)
			lossy("legacy permission request lacked v2 authorization bindings")
			return &protocol.ActivityProgressV1{Message: "legacy permission requested"}, activityID, protocol.EventActivityProgress, diagnostics
		}
	case domain.EventPermissionResolved:
		var payload domain.PermissionPayload
		if decode(&payload) {
			activityID = findOrBeginLegacyActivity(source, state, payload.CallID)
			lossy("legacy permission decision lacked v2 authorization bindings")
			return &protocol.ActivityProgressV1{Message: "legacy permission " + string(payload.Decision.Action)}, activityID, protocol.EventActivityProgress, diagnostics
		}
	case domain.EventToolStarted:
		callID, ok := legacyCallID(source.Payload, false)
		if !ok {
			callID = "malformed:" + string(source.EventID)
			activityID = beginLegacyActivity(source, state, callID)
			key := legacyCallKey(state.ActiveTurnID, callID)
			state.startedCalls[key] = 1
			diagnostics = append(diagnostics, migrationDiagnostic(ref, source, "migration.invalid_payload", "legacy tool start has no usable call ID", nil))
			return &protocol.ActivityOutcomeV1{Status: "uncertain", Reason: "legacy tool start has no usable call ID"}, activityID, protocol.EventActivityUncertain, diagnostics
		}
		key := legacyCallKey(state.ActiveTurnID, callID)
		calls := state.OpenCalls[key]
		if len(calls) == 0 {
			activityID = beginLegacyActivity(source, state, callID)
			state.startedCalls[key] = 1
			state.uncertainCalls[key] = true
			diagnostics = append(diagnostics, migrationDiagnostic(ref, source, "migration.orphan_start", "legacy tool start has no matching request", nil))
			return &protocol.ActivityProgressV1{Message: "legacy tool start observed"}, activityID, protocol.EventActivityProgress, diagnostics
		}
		activityID = calls[len(calls)-1]
		if state.startedCalls[key] > 0 {
			state.startedCalls[key]++
			state.uncertainCalls[key] = true
			diagnostics = append(diagnostics, migrationDiagnostic(ref, source, "migration.duplicate_start", "duplicate legacy tool start makes activity outcome uncertain", nil))
			return &protocol.ActivityOutcomeV1{Status: "uncertain", Reason: "duplicate legacy tool start"}, activityID, protocol.EventActivityUncertain, diagnostics
		}
		state.startedCalls[key] = 1
		lossy("legacy tool start lacked dispatch and authorization bindings")
		return &protocol.ActivityProgressV1{Message: "legacy tool start observed"}, activityID, protocol.EventActivityProgress, diagnostics
	case domain.EventToolResult:
		result, ok := decodeLegacyToolResult(source.Payload)
		if !ok {
			diagnostics = append(diagnostics, migrationDiagnostic(ref, source, "migration.invalid_payload", "legacy tool result has no usable call ID", nil))
			break
		}
		key := legacyCallKey(state.ActiveTurnID, result.CallID)
		calls := state.OpenCalls[key]
		if len(calls) == 0 {
			activityID = orphanLegacyActivity(source, state)
			diagnostics = append(diagnostics, migrationDiagnostic(ref, source, "migration.duplicate_terminal", "legacy tool terminal has no open activity", nil))
			return &protocol.ActivityOutcomeV1{Status: "uncertain", Reason: "unmatched legacy tool terminal"}, activityID, protocol.EventActivityUncertain, diagnostics
		}
		activityID = calls[len(calls)-1]
		if state.startedCalls[key] > 1 {
			state.startedCalls[key]--
			state.uncertainCalls[key] = true
			diagnostics = append(diagnostics, migrationDiagnostic(ref, source, "migration.duplicate_start", "duplicate legacy tool starts make the terminal uncertain", nil))
			return &protocol.ActivityOutcomeV1{Status: "uncertain", Reason: "duplicate legacy tool start"}, activityID, protocol.EventActivityUncertain, diagnostics
		}
		if !state.requestedCalls[key] || state.startedCalls[key] == 0 {
			delete(state.OpenCalls, key)
			delete(state.requestedCalls, key)
			delete(state.startedCalls, key)
			delete(state.uncertainCalls, key)
			diagnostics = append(diagnostics, migrationDiagnostic(ref, source, "migration.out_of_order", "legacy tool terminal requires a matching request followed by a start", nil))
			return &protocol.ActivityOutcomeV1{Status: "uncertain", Reason: "legacy tool lifecycle is incomplete or out of order"}, activityID, protocol.EventActivityUncertain, diagnostics
		}
		uncertain := state.uncertainCalls[key]
		delete(state.OpenCalls, key)
		delete(state.requestedCalls, key)
		delete(state.startedCalls, key)
		delete(state.uncertainCalls, key)
		if uncertain {
			diagnostics = append(diagnostics, migrationDiagnostic(ref, source, "migration.duplicate_start", "duplicate legacy starts keep the activity outcome uncertain", nil))
			diagnostics = append(diagnostics, migrationDiagnostic(ref, source, "migration.duplicate_terminal", "multiple legacy terminals for duplicate starts remain uncertain", nil))
			return &protocol.ActivityOutcomeV1{Status: "uncertain", Reason: "duplicate legacy tool start"}, activityID, protocol.EventActivityUncertain, diagnostics
		}
		if result.Content != "" || len(result.ArtifactIDs) > 0 || result.FileChange != nil || result.WorkspaceChanges != nil {
			diagnostics = append(diagnostics, migrationDiagnostic(ref, source, "migration.legacy_evidence", "legacy tool output is historical evidence, not a verification receipt", nil))
		}
		status, mapped := legacyToolStatus(result.Status)
		return &protocol.ActivityOutcomeV1{Status: status, Reason: string(result.ErrorKind)}, activityID, mapped, diagnostics
	case domain.EventFileChangePlanned:
		var payload domain.FileChangePlan
		if decode(&payload) {
			activityID = findOrBeginLegacyActivity(source, state, payload.CallID)
			diagnostics = append(diagnostics, migrationDiagnostic(ref, source, "migration.legacy_evidence", "legacy diff is historical evidence", nil))
			lossy("legacy file plan lacked a canonical v2 action plan and restore coverage")
			return &protocol.ActivityProgressV1{Message: "legacy file change planned"}, activityID, protocol.EventActivityProgress, diagnostics
		}
	case domain.EventFileChanged:
		var payload domain.FileChange
		if decode(&payload) {
			activityID = findOrBeginLegacyActivity(source, state, payload.CallID)
			diagnostics = append(diagnostics, migrationDiagnostic(ref, source, "migration.legacy_evidence", "legacy file diff is historical evidence", nil))
			before := protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.ToLower(payload.BeforeSHA256)}
			after := protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.ToLower(payload.AfterSHA256)}
			if before.Validate() != nil || after.Validate() != nil {
				lossy("legacy file change lacked complete before/after digests")
				return &protocol.ActivityProgressV1{Message: "legacy file changed"}, activityID, protocol.EventActivityProgress, diagnostics
			}
			return &protocol.FileChangedV1{
				CallID: payload.CallID, Subject: protocol.SubjectRef{Kind: "file", ID: payload.Path},
				Before: before, After: after, EvidenceIDs: []protocol.EvidenceID{},
			}, activityID, protocol.EventFileChanged, diagnostics
		}
	case domain.EventContextCompacted:
		var payload domain.CompactionPayload
		if decode(&payload) {
			lossy("legacy compaction summary lacked immutable evidence and a committed cursor range")
			diagnostic := migrationDiagnostic(ref, source, "migration.lossy", "legacy context compaction retained only in the legacy source", nil)
			return &protocol.DiagnosticV1{Diagnostic: diagnostic}, "", protocol.EventMigrationDiagnostic, diagnostics
		}
	case domain.EventTurnCompleted, domain.EventTurnFailed, domain.EventTurnInterrupted:
		var payload domain.TurnTerminalPayload
		if decode(&payload) {
			status, mapped := legacyTurnStatus(source.Kind)
			return &protocol.TurnTerminalV1{Status: status, Reason: payload.Reason, ErrorCode: string(payload.ErrorKind)}, "", mapped, diagnostics
		}
	}
	diagnostic := migrationDiagnostic(ref, source, "migration.lossy", "legacy payload retained without invented v2 facts", nil)
	diagnostics = append(diagnostics, diagnostic)
	return &protocol.DiagnosticV1{Diagnostic: diagnostic}, activityID, protocol.EventMigrationDiagnostic, diagnostics
}

func cloneUpcastState(state UpcastState) UpcastState {
	clone := state
	clone.OpenCalls = make(map[string][]protocol.ActivityID, len(state.OpenCalls))
	for key, calls := range state.OpenCalls {
		clone.OpenCalls[key] = append([]protocol.ActivityID(nil), calls...)
	}
	clone.requestedCalls = make(map[string]bool, len(state.requestedCalls))
	for key, requested := range state.requestedCalls {
		clone.requestedCalls[key] = requested
	}
	clone.startedCalls = make(map[string]uint64, len(state.startedCalls))
	for key, count := range state.startedCalls {
		clone.startedCalls[key] = count
	}
	clone.uncertainCalls = make(map[string]bool, len(state.uncertainCalls))
	for key, uncertain := range state.uncertainCalls {
		clone.uncertainCalls[key] = uncertain
	}
	return clone
}

func decodeLegacyToolResult(raw json.RawMessage) (domain.ToolResult, bool) {
	var nested domain.ToolResultPayload
	if json.Unmarshal(raw, &nested) == nil && nested.Result.CallID != "" {
		return nested.Result, true
	}
	var flat domain.ToolResult
	if json.Unmarshal(raw, &flat) == nil && flat.CallID != "" {
		return flat, true
	}
	return domain.ToolResult{}, false
}

func deriveUpcastID(kind string, sessionID protocol.SessionID, anchor protocol.EventID, turnOrdinal, activityOrdinal uint64) string {
	bytes := []byte("yordam-v0.2-upcast\x00" + kind + "\x00" + string(sessionID) + "\x00" + string(anchor) + "\x00" + strconv.FormatUint(turnOrdinal, 10) + "\x00" + strconv.FormatUint(activityOrdinal, 10))
	digest := sha256.Sum256(bytes)
	return "derived:" + hex.EncodeToString(digest[:])
}

func legacyActor(source protocol.LegacySource, state UpcastState) protocol.ActorRef {
	kind := protocol.ActorSystem
	switch domain.EventKind(source.Kind) {
	case domain.EventUserMessage, domain.EventModeChanged, domain.EventModelChanged, domain.EventPermissionResolved:
		kind = protocol.ActorUser
	case domain.EventAssistantMessage, domain.EventToolRequested, domain.EventPermissionRequested:
		kind = protocol.ActorAgent
	case domain.EventToolStarted, domain.EventToolResult, domain.EventFileChangePlanned, domain.EventFileChanged:
		kind = protocol.ActorTool
	}
	return protocol.ActorRef{
		ID:   protocol.ActorID(deriveUpcastID("actor", state.SessionID, source.EventID, state.TurnOrdinal, state.ActivityOrdinal)),
		Kind: kind, Source: "legacy_v0.1",
	}
}

func beginLegacyActivity(source protocol.LegacySource, state *UpcastState, callID string) protocol.ActivityID {
	state.ActivityOrdinal++
	activityID := protocol.ActivityID(deriveUpcastID("activity", state.SessionID, source.EventID, state.TurnOrdinal, state.ActivityOrdinal))
	state.OpenCalls[legacyCallKey(state.ActiveTurnID, callID)] = []protocol.ActivityID{activityID}
	return activityID
}

func orphanLegacyActivity(source protocol.LegacySource, state *UpcastState) protocol.ActivityID {
	state.ActivityOrdinal++
	return protocol.ActivityID(deriveUpcastID("activity", state.SessionID, source.EventID, state.TurnOrdinal, state.ActivityOrdinal))
}

func findOrBeginLegacyActivity(source protocol.LegacySource, state *UpcastState, callID string) protocol.ActivityID {
	calls := state.OpenCalls[legacyCallKey(state.ActiveTurnID, callID)]
	if len(calls) > 0 {
		return calls[len(calls)-1]
	}
	return beginLegacyActivity(source, state, callID)
}

func legacyCallKey(turnID protocol.TurnID, callID string) string {
	return string(turnID) + "\x00" + callID
}

func legacyCallID(payload json.RawMessage, nestedResult bool) (string, bool) {
	var value map[string]any
	if json.Unmarshal(payload, &value) != nil {
		return "", false
	}
	if callID, ok := value["call_id"].(string); ok && callID != "" {
		return callID, true
	}
	if nestedResult {
		if result, ok := value["result"].(map[string]any); ok {
			callID, ok := result["call_id"].(string)
			return callID, ok && callID != ""
		}
	}
	return "", false
}

func unmatchedCallDiagnostics(ref protocol.JournalRef, source protocol.LegacySource, state UpcastState) []protocol.Diagnostic {
	keys := make([]string, 0, len(state.OpenCalls))
	for key, calls := range state.OpenCalls {
		if len(calls) > 0 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	diagnostics := make([]protocol.Diagnostic, 0, len(keys))
	for _, key := range keys {
		_, callID, _ := strings.Cut(key, "\x00")
		details, _ := json.Marshal(map[string]any{"call_id": callID, "activity_id": state.OpenCalls[key][len(state.OpenCalls[key])-1]})
		diagnostics = append(diagnostics, migrationDiagnostic(ref, source, "migration.unmatched_activity", "legacy activity has no terminal and remains interrupted/uncertain", details))
	}
	return diagnostics
}

func migrationDiagnostic(ref protocol.JournalRef, source protocol.LegacySource, code, message string, details json.RawMessage) protocol.Diagnostic {
	return protocol.Diagnostic{Code: code, Message: message, Journal: ref, AtSeq: source.Seq, EventID: source.EventID, Details: protocol.CloneRawMessage(details)}
}

func legacyToolStatus(status domain.ToolStatus) (string, string) {
	switch status {
	case domain.ToolSucceeded:
		return "succeeded", protocol.EventActivitySucceeded
	case domain.ToolFailed:
		return "failed", protocol.EventActivityFailed
	case domain.ToolDenied:
		return "denied", protocol.EventActivityDenied
	case domain.ToolCancelled:
		return "cancelled", protocol.EventActivityCancelled
	default:
		return "uncertain", protocol.EventActivityUncertain
	}
}

func legacyTurnStatus(kind string) (string, string) {
	switch domain.EventKind(kind) {
	case domain.EventTurnCompleted:
		return "completed", protocol.EventTurnCompleted
	case domain.EventTurnFailed:
		return "failed", protocol.EventTurnFailed
	default:
		return "interrupted", protocol.EventTurnInterrupted
	}
}

func isLegacyTurnTerminal(kind string) bool {
	legacy := domain.EventKind(kind)
	return legacy == domain.EventTurnCompleted || legacy == domain.EventTurnFailed || legacy == domain.EventTurnInterrupted
}
