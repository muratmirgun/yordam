package domain

type MessagePayload struct {
	Content    string     `json:"content"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

type ToolRequestPayload struct {
	Call ToolCall `json:"call"`
}

type ToolResultPayload struct {
	Result ToolResult `json:"result"`
}

type PermissionPayload struct {
	CallID   string             `json:"call_id"`
	Tool     string             `json:"tool"`
	Decision PermissionDecision `json:"decision"`
}

type ModeChangedPayload struct {
	Mode PermissionMode `json:"mode"`
}

type ModelChangedPayload struct {
	Selection ModelSelection `json:"selection"`
}

type TrustedExecutionPayload struct {
	Enabled bool `json:"enabled"`
}

type CompactionPayload struct {
	FromSeq    uint64 `json:"from_seq"`
	ThroughSeq uint64 `json:"through_seq"`
	Summary    string `json:"summary"`
}

type TurnTerminalPayload struct {
	Reason    string    `json:"reason"`
	ErrorKind ErrorKind `json:"error_kind,omitempty"`
}
