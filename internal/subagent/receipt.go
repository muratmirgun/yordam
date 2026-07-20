package subagent

import (
	"encoding/json"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/muratmirgun/yordam/internal/protocol"
)

// ProjectReceipt derives the child result from durable journal records only.
// Assistant text is retained solely as a bounded human summary; it is never
// admitted as proof of changed files, commands, tests, or verification.
func ProjectReceipt(manifest protocol.SubagentManifestV1, terminal protocol.CommittedCursor, status, summary string, fallbackUsage protocol.ModelUsage, public *protocol.PublicError, events []protocol.EventRecord) protocol.SubagentReceiptV1 {
	files := map[string]struct{}{}
	plannedCommands := map[protocol.ActivityID][]string{}
	commands := map[string]struct{}{}
	evidence := map[protocol.EvidenceID]struct{}{}
	started := map[protocol.ActivityID]struct{}{}
	terminalActivities := map[protocol.ActivityID]struct{}{}
	unknown := map[protocol.ActivityID]struct{}{}
	usage, haveUsage := protocol.ModelUsage{}, false

	for _, event := range events {
		if event.Envelope.JournalKind != "" && event.Envelope.JournalKind != protocol.JournalSession || event.Envelope.JournalID != "" && event.Envelope.JournalID != terminal.JournalID || event.Envelope.SessionID != "" && event.Envelope.SessionID != manifest.ChildSessionID {
			continue
		}
		if terminal.CommitSeq != 0 && event.Envelope.Seq > terminal.CommitSeq {
			continue
		}
		if terminal.CommitSeq != 0 && event.Envelope.Seq == terminal.CommitSeq && terminal.TransactionID != "" && event.Envelope.TransactionID != terminal.TransactionID {
			continue
		}
		activityID := event.Envelope.ActivityID
		switch event.Envelope.Kind {
		case protocol.EventFileChanged:
			var payload protocol.FileChangedV1
			if json.Unmarshal(event.Envelope.Payload, &payload) == nil && payload.Subject.Kind == "file" && payload.Subject.ID != "" {
				files[payload.Subject.ID] = struct{}{}
				addEvidence(evidence, payload.EvidenceIDs)
			}
		case protocol.EventExecutionPlanDeclared:
			var payload protocol.ExecutionPlanDeclaredV1
			if json.Unmarshal(event.Envelope.Payload, &payload) == nil && payload.Plan.Body.Tool.Name == "shell" {
				for _, resource := range payload.Plan.Body.Resources {
					for _, attribute := range resource.Attributes {
						if attribute.Name == "command" && attribute.Value != "" {
							plannedCommands[activityID] = append(plannedCommands[activityID], attribute.Value)
						}
					}
				}
			}
		case protocol.EventEvidenceRecorded:
			var payload protocol.EvidenceRecordedV1
			if json.Unmarshal(event.Envelope.Payload, &payload) == nil && payload.Record.Body.ID != "" {
				evidence[payload.Record.Body.ID] = struct{}{}
			}
		case protocol.EventVerificationReceiptRecorded:
			var payload protocol.VerificationReceiptRecordedV1
			if json.Unmarshal(event.Envelope.Payload, &payload) == nil {
				addEvidence(evidence, payload.Receipt.Body.EvidenceIDs)
			}
		case protocol.EventProviderAttemptTerminal:
			var payload protocol.ProviderAttemptTerminalV1
			if json.Unmarshal(event.Envelope.Payload, &payload) == nil && payload.Usage.Validate() == nil {
				usage, haveUsage = addUsage(usage, haveUsage, payload.Usage)
			}
		case protocol.EventActivityStarted:
			if activityID != "" {
				started[activityID] = struct{}{}
			}
		case protocol.EventActivitySucceeded, protocol.EventActivityFailed, protocol.EventActivityDenied, protocol.EventActivityCancelled, protocol.EventActivityInterruptedNoEffect:
			if activityID != "" {
				terminalActivities[activityID] = struct{}{}
			}
			var payload protocol.ActivityOutcomeV1
			if json.Unmarshal(event.Envelope.Payload, &payload) == nil {
				addEvidence(evidence, payload.OutputEvidenceIDs)
			}
		case protocol.EventActivityUncertain:
			if activityID != "" {
				terminalActivities[activityID] = struct{}{}
				unknown[activityID] = struct{}{}
			}
		}
	}
	for activityID := range started {
		if _, complete := terminalActivities[activityID]; !complete {
			unknown[activityID] = struct{}{}
		}
	}
	for activityID := range terminalActivities {
		if _, didStart := started[activityID]; didStart {
			for _, command := range plannedCommands[activityID] {
				commands[command] = struct{}{}
			}
		}
	}
	if len(unknown) != 0 {
		status = "uncertain"
	}
	if !haveUsage {
		usage = fallbackUsage
	}
	if usage == (protocol.ModelUsage{}) {
		usage = unknownUsage()
	}
	summary = boundedUTF8(summary, protocol.MaxSubagentReceiptSummaryBytes)
	return protocol.SubagentReceiptV1{Status: status, Summary: summary, Manifest: manifest, TerminalCursor: terminal, ChangedFiles: sortedStrings(files), CommandsAndTests: sortedStrings(commands), Usage: usage, EvidenceIDs: sortedEvidence(evidence), UnknownEffects: sortedActivities(unknown), Error: protocol.DeepCopy(public)}
}

func boundedUTF8(value string, limit int) string {
	value = strings.ToValidUTF8(value, "�")
	if len(value) <= limit {
		return value
	}
	for limit > 0 && !utf8.ValidString(value[:limit]) {
		limit--
	}
	return value[:limit]
}

func addEvidence(target map[protocol.EvidenceID]struct{}, values []protocol.EvidenceID) {
	for _, value := range values {
		if value != "" {
			target[value] = struct{}{}
		}
	}
}

func sortedStrings(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func sortedEvidence(values map[protocol.EvidenceID]struct{}) []protocol.EvidenceID {
	result := make([]protocol.EvidenceID, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func sortedActivities(values map[protocol.ActivityID]struct{}) []protocol.ActivityID {
	result := make([]protocol.ActivityID, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func addUsage(total protocol.ModelUsage, haveTotal bool, next protocol.ModelUsage) (protocol.ModelUsage, bool) {
	if !haveTotal {
		return next, true
	}
	return protocol.ModelUsage{Input: addUsageValue(total.Input, next.Input), Output: addUsageValue(total.Output, next.Output), Cached: addUsageValue(total.Cached, next.Cached), CacheWrite: addUsageValue(total.CacheWrite, next.CacheWrite), Reasoning: addUsageValue(total.Reasoning, next.Reasoning)}, true
}

func addUsageValue(left, right protocol.UsageValue) protocol.UsageValue {
	if left.State == protocol.UsageUnknown || right.State == protocol.UsageUnknown {
		return protocol.UsageValue{State: protocol.UsageUnknown}
	}
	return protocol.UsageValue{State: protocol.UsageProviderReported, Value: left.Value + right.Value, Provenance: "child_journal"}
}

func unknownUsage() protocol.ModelUsage {
	unknown := protocol.UsageValue{State: protocol.UsageUnknown}
	return protocol.ModelUsage{Input: unknown, Output: unknown, Cached: unknown, CacheWrite: unknown, Reasoning: unknown}
}
