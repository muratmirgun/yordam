package protocol

import (
	"encoding/json"
	"fmt"
)

type ToolIdentity struct {
	Source    string `json:"source"`
	Authority string `json:"authority"`
	Name      string `json:"name"`
}

func (i ToolIdentity) Validate() error {
	if i.Source == "" || i.Authority == "" || i.Name == "" {
		return fmt.Errorf("tool identity is incomplete")
	}
	return nil
}

type ResourceAttribute struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type ResourceTarget struct {
	Kind        string              `json:"kind"`
	CanonicalID string              `json:"canonical_id"`
	ParentID    string              `json:"parent_id,omitempty"`
	Digest      string              `json:"digest,omitempty"`
	Attributes  []ResourceAttribute `json:"attributes,omitempty"`
}

func (r ResourceTarget) key() string {
	return r.Kind + "\x00" + r.CanonicalID + "\x00" + r.ParentID + "\x00" + r.Digest
}

func validateSortedUniqueResources(resources []ResourceTarget) error {
	if len(resources) > MaxCollectionMembers {
		return fmt.Errorf("too many resources")
	}
	previous := ""
	for index, resource := range resources {
		if resource.Kind == "" || resource.CanonicalID == "" {
			return fmt.Errorf("resource target is incomplete")
		}
		key := resource.key()
		if index > 0 && key <= previous {
			return fmt.Errorf("resources must be sorted and unique")
		}
		previous = key
		attributePrevious := ""
		for attributeIndex, attribute := range resource.Attributes {
			if attribute.Name == "" {
				return fmt.Errorf("resource attribute name is required")
			}
			attributeKey := attribute.Name + "\x00" + attribute.Value
			if attributeIndex > 0 && attributeKey <= attributePrevious {
				return fmt.Errorf("resource attributes must be sorted and unique")
			}
			attributePrevious = attributeKey
		}
	}
	return nil
}

type ToolDescriptorBody struct {
	Identity             ToolIdentity    `json:"identity"`
	SourceRevision       string          `json:"source_revision"`
	DisplayName          string          `json:"display_name"`
	Description          string          `json:"description"`
	InputSchema          json.RawMessage `json:"input_schema"`
	OutputSchema         json.RawMessage `json:"output_schema,omitempty"`
	Effect               string          `json:"effect"`
	Mutation             string          `json:"mutation"`
	ExecutionLoci        []string        `json:"execution_loci"`
	ClassificationSource string          `json:"classification_source"`
	Idempotency          string          `json:"idempotency"`
	Retry                string          `json:"retry"`
}

type ToolDescriptor struct {
	Body             ToolDescriptorBody `json:"body"`
	DescriptorDigest Digest             `json:"descriptor_digest"`
}

type ToolAliasBinding struct {
	Alias            string       `json:"alias"`
	Identity         ToolIdentity `json:"identity"`
	SourceRevision   string       `json:"source_revision"`
	DescriptorDigest Digest       `json:"descriptor_digest"`
}

type ExposedTool struct {
	Alias       string          `json:"alias"`
	Identity    ToolIdentity    `json:"identity"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

type ToolExposure struct {
	CatalogRevision string             `json:"catalog_revision"`
	Tools           []ExposedTool      `json:"tools"`
	Aliases         []ToolAliasBinding `json:"aliases"`
}

type ActionPlanBody struct {
	CallID               string              `json:"call_id"`
	Tool                 ToolIdentity        `json:"tool"`
	SourceRevision       string              `json:"source_revision"`
	DescriptorDigest     Digest              `json:"descriptor_digest"`
	Action               string              `json:"action"`
	Purpose              string              `json:"purpose"`
	Resources            []ResourceTarget    `json:"resources"`
	ExecutionLocus       string              `json:"execution_locus"`
	Effect               string              `json:"effect"`
	Boundary             string              `json:"boundary"`
	Reversibility        string              `json:"reversibility"`
	VerificationCoverage string              `json:"verification_coverage"`
	RequestedProfile     string              `json:"requested_profile"`
	EffectiveProfile     string              `json:"effective_profile"`
	RuntimeGenerationID  RuntimeGenerationID `json:"runtime_generation_id"`
}

func (b ActionPlanBody) Validate() error {
	if b.CallID == "" || b.SourceRevision == "" || b.Action == "" || b.Purpose == "" || b.ExecutionLocus == "" || b.Effect == "" || b.Boundary == "" || b.Reversibility == "" || b.VerificationCoverage == "" || b.RequestedProfile == "" || b.EffectiveProfile == "" || b.RuntimeGenerationID == "" {
		return fmt.Errorf("action plan body is incomplete")
	}
	if err := b.Tool.Validate(); err != nil {
		return err
	}
	if err := b.DescriptorDigest.Validate(); err != nil {
		return err
	}
	return validateSortedUniqueResources(b.Resources)
}

type ActionPlan struct {
	Body   ActionPlanBody `json:"body"`
	Digest Digest         `json:"digest"`
}

type ExecutionResult struct {
	Outcome    ActivityOutcomeV1   `json:"outcome"`
	ToolResult ToolResultBlock     `json:"tool_result"`
	Evidence   []EvidenceCandidate `json:"evidence"`
}
