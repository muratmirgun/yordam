package domain

import (
	"encoding/json"

	"github.com/muratmirgun/yordam/internal/protocol"
)

type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

type Message struct {
	Role       Role       `json:"role"`
	Content    string     `json:"content"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

type ToolCall struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type ModelSelection struct {
	Profile string `json:"profile"`
	Model   string `json:"model"`
}

type ModelRequest struct {
	Selection ModelSelection   `json:"selection"`
	Messages  []Message        `json:"messages"`
	Tools     []ToolDescriptor `json:"tools"`
}

type ModelEventKind string

const (
	ModelTextDelta    ModelEventKind = "text_delta"
	ModelToolCall     ModelEventKind = "tool_call"
	ModelUsageUpdate  ModelEventKind = "usage_update"
	ModelRefusalDelta ModelEventKind = "refusal_delta"
	ModelDone         ModelEventKind = "done"
	ModelStreamError  ModelEventKind = "error"
)

type ModelEvent struct {
	Kind         ModelEventKind       `json:"kind"`
	Text         string               `json:"text,omitempty"`
	ToolCall     *ToolCall            `json:"tool_call,omitempty"`
	Usage        *protocol.ModelUsage `json:"usage,omitempty"`
	Refusal      string               `json:"refusal,omitempty"`
	RequestID    string               `json:"request_id,omitempty"`
	FinishReason string               `json:"finish_reason,omitempty"`
	Err          error                `json:"-"`
}
