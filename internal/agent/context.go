package agent

import (
	"encoding/json"
	"unicode/utf8"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/secret"
)

const MaxToolResultContextBytes = 32 << 10

func BuildContext(replay domain.SessionReplay, systemPrompt string) []domain.Message {
	messages := []domain.Message{{Role: domain.RoleSystem, Content: systemPrompt}}
	startSeq := uint64(0)
	summary := ""
	for _, event := range replay.Events {
		if event.Kind != domain.EventContextCompacted {
			continue
		}
		var payload domain.CompactionPayload
		if err := json.Unmarshal(event.Payload, &payload); err == nil &&
			payload.Summary != "" &&
			(payload.FromSeq == 0 || payload.FromSeq <= payload.ThroughSeq) &&
			payload.ThroughSeq < event.Seq && payload.ThroughSeq >= startSeq {
			startSeq = payload.ThroughSeq
			summary = payload.Summary
		}
	}
	if summary != "" {
		messages = append(messages, domain.Message{Role: domain.RoleSystem, Content: summary})
	}
	for _, event := range replay.Events {
		if event.Seq <= startSeq {
			continue
		}
		var payload domain.MessagePayload
		switch event.Kind {
		case domain.EventUserMessage:
			if json.Unmarshal(event.Payload, &payload) != nil {
				continue
			}
			messages = append(messages, domain.Message{Role: domain.RoleUser, Content: payload.Content})
		case domain.EventAssistantMessage:
			if json.Unmarshal(event.Payload, &payload) != nil {
				continue
			}
			messages = append(messages, domain.Message{
				Role:      domain.RoleAssistant,
				Content:   payload.Content,
				ToolCalls: payload.ToolCalls,
			})
		case domain.EventToolResult:
			var payload domain.ToolResultPayload
			if json.Unmarshal(event.Payload, &payload) != nil {
				continue
			}
			messages = append(messages, domain.Message{
				Role:       domain.RoleTool,
				Content:    ToolResultContent(payload.Result),
				ToolCallID: payload.Result.CallID,
			})
		}
	}
	return messages
}

func ToolResultContent(result domain.ToolResult) string {
	type modelToolResult struct {
		CallID              string            `json:"call_id"`
		Status              domain.ToolStatus `json:"status"`
		ErrorKind           domain.ErrorKind  `json:"error_kind,omitempty"`
		Content             string            `json:"content"`
		ArtifactIDs         []string          `json:"artifact_ids,omitempty"`
		ExitCode            *int              `json:"exit_code,omitempty"`
		Duration            int64             `json:"duration_ns"`
		Truncated           bool              `json:"truncated"`
		HasFileChange       bool              `json:"has_file_change,omitempty"`
		HasWorkspaceChanges bool              `json:"has_workspace_changes,omitempty"`
	}
	projected := modelToolResult{
		CallID:              boundedString(result.CallID, 1024),
		Status:              result.Status,
		ErrorKind:           result.ErrorKind,
		ArtifactIDs:         boundedStrings(result.ArtifactIDs, 32, 256),
		ExitCode:            result.ExitCode,
		Duration:            int64(result.Duration),
		Truncated:           result.Truncated,
		HasFileChange:       result.FileChange != nil,
		HasWorkspaceChanges: result.WorkspaceChanges != nil,
	}
	base, err := json.Marshal(projected)
	if err != nil {
		return `{"status":"failed","error_kind":"tool_failed","content":"could not encode tool result"}`
	}
	runes := []rune(result.Content)
	low, high := 0, len(runes)
	for low < high {
		middle := low + (high-low+1)/2
		projected.Content = string(runes[:middle])
		encoded, marshalErr := json.Marshal(projected)
		if marshalErr == nil && len(encoded) <= MaxToolResultContextBytes {
			low = middle
		} else {
			high = middle - 1
		}
	}
	projected.Content = string(runes[:low])
	if low < len(runes) {
		projected.Truncated = true
	}
	raw, err := json.Marshal(projected)
	if err != nil || len(raw) > MaxToolResultContextBytes {
		return string(base)
	}
	return string(raw)
}

func ToolResultContentLeased(result domain.ToolResult, lease *secret.Lease) string {
	if lease == nil {
		return ToolResultContent(result)
	}
	result.CallID = lease.String(result.CallID)
	result.Content = lease.String(result.Content)
	for index, id := range result.ArtifactIDs {
		result.ArtifactIDs[index] = lease.String(id)
	}
	return ToolResultContent(result)
}

func boundedString(value string, maximum int) string {
	if len(value) <= maximum {
		return value
	}
	value = value[:maximum]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

func boundedStrings(values []string, count, bytes int) []string {
	if len(values) > count {
		values = values[:count]
	}
	bounded := make([]string, len(values))
	for index, value := range values {
		bounded[index] = boundedString(value, bytes)
	}
	return bounded
}
