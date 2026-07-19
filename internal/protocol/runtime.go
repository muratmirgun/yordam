package protocol

type RuntimeLimits struct {
	MaxToolCalls             int        `json:"max_tool_calls"`
	ShellTimeoutNanos        int64      `json:"shell_timeout_nanos"`
	ApplicationQueueCapacity int        `json:"application_queue_capacity"`
	AutoCompact              bool       `json:"auto_compact"`
	CompactReserveTokens     ValueInt64 `json:"compact_reserve_tokens"`
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
