package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

type RuntimeLimits struct {
	MaxToolCalls             int            `json:"max_tool_calls"`
	ShellTimeoutNanos        int64          `json:"shell_timeout_nanos"`
	ApplicationQueueCapacity int            `json:"application_queue_capacity"`
	AutoCompact              bool           `json:"auto_compact"`
	CompactReserveTokens     ValueInt64     `json:"compact_reserve_tokens"`
	Subagents                SubagentLimits `json:"subagents,omitempty"`
	subagentsPresent         bool
}

type SubagentLimits struct {
	Enabled      bool  `json:"enabled"`
	MaxPerTurn   int   `json:"max_per_turn"`
	MaxToolCalls int   `json:"max_tool_calls"`
	TimeoutNanos int64 `json:"timeout_nanos"`
}

func (limits SubagentLimits) Validate() error {
	if limits.MaxPerTurn < 1 || limits.MaxPerTurn > 4 {
		return fmt.Errorf("subagent max per turn must be 1..4")
	}
	if limits.MaxToolCalls < 1 || limits.MaxToolCalls > 64 {
		return fmt.Errorf("subagent max tool calls must be 1..64")
	}
	if limits.TimeoutNanos < int64(time.Second) || limits.TimeoutNanos > int64(1800*time.Second) {
		return fmt.Errorf("subagent timeout must be 1..1800 seconds")
	}
	return nil
}

func (limits RuntimeLimits) Validate() error {
	if err := limits.validateBase(); err != nil {
		return err
	}
	return limits.Subagents.Validate()
}

func (limits RuntimeLimits) ValidatePersisted() error {
	if err := limits.validateBase(); err != nil {
		return err
	}
	if !limits.subagentsPresent {
		return nil
	}
	return limits.Subagents.Validate()
}

func (limits RuntimeLimits) validateBase() error {
	if limits.MaxToolCalls < 1 || limits.MaxToolCalls > 128 || limits.ShellTimeoutNanos <= 0 || limits.ApplicationQueueCapacity <= 0 {
		return fmt.Errorf("runtime limits are invalid")
	}
	return nil
}

func (limits RuntimeLimits) MarshalJSON() ([]byte, error) {
	type wire struct {
		MaxToolCalls             int             `json:"max_tool_calls"`
		ShellTimeoutNanos        int64           `json:"shell_timeout_nanos"`
		ApplicationQueueCapacity int             `json:"application_queue_capacity"`
		AutoCompact              bool            `json:"auto_compact"`
		CompactReserveTokens     ValueInt64      `json:"compact_reserve_tokens"`
		Subagents                *SubagentLimits `json:"subagents,omitempty"`
	}
	encoded := wire{
		MaxToolCalls: limits.MaxToolCalls, ShellTimeoutNanos: limits.ShellTimeoutNanos, ApplicationQueueCapacity: limits.ApplicationQueueCapacity,
		AutoCompact: limits.AutoCompact, CompactReserveTokens: limits.CompactReserveTokens,
	}
	if limits.subagentsPresent || limits.Subagents != (SubagentLimits{}) {
		encoded.Subagents = &limits.Subagents
	}
	return json.Marshal(encoded)
}

func (limits *RuntimeLimits) UnmarshalJSON(data []byte) error {
	type wire struct {
		MaxToolCalls             int             `json:"max_tool_calls"`
		ShellTimeoutNanos        int64           `json:"shell_timeout_nanos"`
		ApplicationQueueCapacity int             `json:"application_queue_capacity"`
		AutoCompact              bool            `json:"auto_compact"`
		CompactReserveTokens     ValueInt64      `json:"compact_reserve_tokens"`
		Subagents                json.RawMessage `json:"subagents"`
	}
	var decoded wire
	if err := strictUnmarshalRuntimeLimits(data, &decoded); err != nil {
		return err
	}
	*limits = RuntimeLimits{
		MaxToolCalls: decoded.MaxToolCalls, ShellTimeoutNanos: decoded.ShellTimeoutNanos, ApplicationQueueCapacity: decoded.ApplicationQueueCapacity,
		AutoCompact: decoded.AutoCompact, CompactReserveTokens: decoded.CompactReserveTokens,
	}
	if len(decoded.Subagents) == 0 {
		return nil
	}
	limits.subagentsPresent = true
	return strictUnmarshalRuntimeLimits(decoded.Subagents, &limits.Subagents)
}

func strictUnmarshalRuntimeLimits(data []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("runtime limits contain multiple JSON values")
		}
		return err
	}
	return nil
}

type RuntimeGenerationBody struct {
	ProviderCatalogRevision string            `json:"provider_catalog_revision"`
	Models                  []ModelDescriptor `json:"models"`
	ToolCatalogRevision     string            `json:"tool_catalog_revision"`
	Tools                   []ToolDescriptor  `json:"tools"`
	SkillCatalogRevision    string            `json:"skill_catalog_revision,omitempty"`
	Skills                  []SkillDescriptor `json:"skills,omitempty"`
	InstructionRevision     string            `json:"instruction_revision"`
	PolicyGeneration        string            `json:"policy_generation"`
	ExecutionProfiles       []string          `json:"execution_profiles"`
	Limits                  RuntimeLimits     `json:"limits"`
}

type RuntimeGenerationManifest struct {
	ID     RuntimeGenerationID   `json:"id"`
	Body   RuntimeGenerationBody `json:"body"`
	Digest Digest                `json:"digest"`
}
