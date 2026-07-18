package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
)

const MaxToolResultContextBytes = 32 << 10

func BuildContext(replay domain.SessionReplay, systemPrompt string, lease *secret.Lease) ([]domain.Message, error) {
	if _, err := lease.GenerationID(); err != nil {
		return nil, err
	}
	messages := []domain.Message{{Role: domain.RoleSystem, Content: lease.String(systemPrompt)}}
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
		messages = append(messages, domain.Message{Role: domain.RoleSystem, Content: lease.String(summary)})
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
			messages = append(messages, domain.Message{Role: domain.RoleUser, Content: lease.String(payload.Content)})
		case domain.EventAssistantMessage:
			if json.Unmarshal(event.Payload, &payload) != nil {
				continue
			}
			message, err := admitMessage(domain.Message{
				Role:      domain.RoleAssistant,
				Content:   payload.Content,
				ToolCalls: payload.ToolCalls,
			}, lease)
			if err != nil {
				return nil, err
			}
			messages = append(messages, message)
		case domain.EventToolResult:
			var payload domain.ToolResultPayload
			if json.Unmarshal(event.Payload, &payload) != nil {
				continue
			}
			content, err := ToolResultContentLeased(payload.Result, lease)
			if err != nil {
				return nil, err
			}
			messages = append(messages, domain.Message{
				Role:       domain.RoleTool,
				Content:    content,
				ToolCallID: lease.String(payload.Result.CallID),
			})
		}
	}
	return messages, nil
}

func BuildLegacyContextSources(replay domain.SessionReplay, systemPrompt string, lease *secret.Lease) ([]protocol.ContentSource, error) {
	messages, err := BuildContext(replay, systemPrompt, lease)
	if err != nil {
		return nil, err
	}
	sources := make([]protocol.ContentSource, 0, len(messages))
	for index, message := range messages {
		blocks := make([]protocol.ContentBlock, 0, len(message.ToolCalls)+1)
		if message.Content != "" {
			blocks = append(blocks, protocol.ContentBlock{Kind: protocol.ContentText, Text: message.Content})
		}
		for _, call := range message.ToolCalls {
			blocks = append(blocks, protocol.ContentBlock{Kind: protocol.ContentToolUse, ToolUse: &protocol.ToolUseBlock{CallID: call.ID, Alias: call.Name, Arguments: protocol.CloneRawMessage(call.Arguments)}})
		}
		if len(blocks) == 0 {
			return nil, fmt.Errorf("legacy context message %d is empty", index)
		}
		digest, err := canonicaljson.Digest(blocks)
		if err != nil {
			return nil, err
		}
		source := protocol.ContentSource{
			ID: fmt.Sprintf("legacy-context-%06d", index), Kind: "legacy_" + string(message.Role) + "_message", Scope: "session",
			Provenance: "legacy_context_adapter_v1", Digest: digest, Content: blocks,
		}
		if err := source.Validate(); err != nil {
			return nil, err
		}
		sources = append(sources, source)
	}
	return protocol.DeepCopy(sources), nil
}

func admitMessage(message domain.Message, lease *secret.Lease) (domain.Message, error) {
	if _, err := lease.GenerationID(); err != nil {
		return domain.Message{}, err
	}
	message.Content = lease.String(message.Content)
	message.ToolCallID = lease.String(message.ToolCallID)
	for index := range message.ToolCalls {
		call := &message.ToolCalls[index]
		call.ID = lease.String(call.ID)
		call.Name = lease.String(call.Name)
		if len(call.Arguments) == 0 {
			continue
		}
		var value any
		decoder := json.NewDecoder(bytes.NewReader(call.Arguments))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return domain.Message{}, err
		}
		admitted, err := lease.JSON(value)
		if err != nil {
			return domain.Message{}, err
		}
		call.Arguments = admitted
	}
	return message, nil
}

func admitToolDescriptors(descriptors []domain.ToolDescriptor, lease *secret.Lease) ([]domain.ToolDescriptor, error) {
	if _, err := lease.GenerationID(); err != nil {
		return nil, err
	}
	admitted := make([]domain.ToolDescriptor, len(descriptors))
	for index, descriptor := range descriptors {
		descriptor.Name = lease.String(descriptor.Name)
		descriptor.Description = lease.String(descriptor.Description)
		descriptor.ScopeDescription = lease.String(descriptor.ScopeDescription)
		if len(descriptor.InputSchema) != 0 {
			var schema any
			decoder := json.NewDecoder(bytes.NewReader(descriptor.InputSchema))
			decoder.UseNumber()
			if err := decoder.Decode(&schema); err != nil {
				return nil, err
			}
			raw, err := lease.JSON(schema)
			if err != nil {
				return nil, err
			}
			descriptor.InputSchema = raw
		}
		admitted[index] = descriptor
	}
	return admitted, nil
}

func admitSelection(selection domain.ModelSelection, lease *secret.Lease) (domain.ModelSelection, error) {
	if _, err := lease.GenerationID(); err != nil {
		return domain.ModelSelection{}, err
	}
	selection.Profile = lease.String(selection.Profile)
	selection.Model = lease.String(selection.Model)
	return selection, nil
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

func ToolResultContentLeased(result domain.ToolResult, lease *secret.Lease) (string, error) {
	if _, err := lease.GenerationID(); err != nil {
		return "", err
	}
	result.CallID = lease.String(result.CallID)
	result.Content = lease.String(result.Content)
	for index, id := range result.ArtifactIDs {
		result.ArtifactIDs[index] = lease.String(id)
	}
	return ToolResultContent(result), nil
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
