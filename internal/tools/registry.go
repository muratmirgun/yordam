package tools

import (
	"fmt"
	"sort"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type Registry struct {
	byName  map[string]ports.Tool
	ordered []domain.ToolDescriptor
}

var builtInRank = map[string]int{
	"read":     0,
	"search":   1,
	"skill":    2,
	"subagent": 3,
	"edit":     4,
	"shell":    5,
}

const BuiltinSourceRevision = "builtin-v1"

func BuiltinCanonicalDescriptor(legacy domain.ToolDescriptor, classification domain.ToolClassification) protocol.ToolDescriptor {
	input, err := canonicaljson.Marshal(legacy.InputSchema)
	if err != nil {
		panic(fmt.Sprintf("canonicalize builtin %s schema: %v", legacy.Name, err))
	}
	body := protocol.ToolDescriptorBody{
		Identity:       protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: legacy.Name},
		SourceRevision: BuiltinSourceRevision, DisplayName: legacy.Name, Description: legacy.Description, InputSchema: input,
		Effect: classification.Effect, Mutation: classification.Mutation, ExecutionLoci: append([]string(nil), classification.ExecutionLoci...),
		ClassificationSource: "trusted_adapter", Idempotency: classification.Idempotency, Retry: classification.Retry,
	}
	digest, err := canonicaljson.Digest(body)
	if err != nil {
		panic(fmt.Sprintf("digest builtin %s descriptor: %v", legacy.Name, err))
	}
	return protocol.ToolDescriptor{Body: body, DescriptorDigest: digest}
}

func ReadClassification() domain.ToolClassification {
	return domain.ToolClassification{Effect: "observation", Mutation: "read_only", ExecutionLoci: []string{"builtin"}, Boundary: "workspace", Reversibility: "not_applicable", VerificationCoverage: "full", Idempotency: "idempotent", Retry: "safe_before_dispatch", RequestedProfile: "restricted", EffectiveProfile: "restricted"}
}

func SearchClassification() domain.ToolClassification { return ReadClassification() }

// SkillClassification is observation-only but its resource is a frozen logical
// catalog entry rather than a filesystem path. Permission validation binds all
// identity attributes before it treats that entry as workspace-scoped.
func SkillClassification() domain.ToolClassification { return ReadClassification() }

// SubagentClassification describes trusted runtime orchestration only. It
// does not authorize any child provider or tool effect, each of which is
// separately planned and authorized in the child turn.
func SubagentClassification() domain.ToolClassification {
	return domain.ToolClassification{Effect: "orchestration", Mutation: "orchestration", ExecutionLoci: []string{"orchestrator"}, Boundary: "runtime", Reversibility: "not_applicable", VerificationCoverage: "full", Idempotency: "conditional", Retry: "never_after_dispatch", RequestedProfile: "configured", EffectiveProfile: "configured"}
}

func EditClassification() domain.ToolClassification {
	return domain.ToolClassification{Effect: "mutation", Mutation: "file", ExecutionLoci: []string{"builtin"}, Boundary: "workspace", Reversibility: "preimage", VerificationCoverage: "preimage_and_postimage", Idempotency: "conditional", Retry: "never_after_dispatch", RequestedProfile: "restricted", EffectiveProfile: "restricted"}
}

func ShellClassification() domain.ToolClassification {
	return domain.ToolClassification{Effect: "mutation", Mutation: "process", ExecutionLoci: []string{"process"}, Boundary: "process", Reversibility: "unknown", VerificationCoverage: "partial", Idempotency: "unknown", Retry: "never_after_dispatch", RequestedProfile: "unsandboxed", EffectiveProfile: "unsandboxed"}
}

func NewRegistry(items ...ports.Tool) *Registry {
	registry := &Registry{byName: make(map[string]ports.Tool, len(items))}
	for _, item := range items {
		descriptor := item.Descriptor()
		if err := descriptor.Validate(); err != nil {
			panic(err)
		}
		if descriptor.ScopeDescription == "" {
			panic(fmt.Sprintf("built-in tool %s has no scope description", descriptor.Name))
		}
		descriptor.InputSchema = append([]byte(nil), descriptor.InputSchema...)
		if _, exists := registry.byName[descriptor.Name]; exists {
			panic(fmt.Sprintf("duplicate tool %s", descriptor.Name))
		}
		registry.byName[descriptor.Name] = item
		registry.ordered = append(registry.ordered, descriptor)
	}
	sort.Slice(registry.ordered, func(left, right int) bool {
		leftRank, leftBuiltin := builtInRank[registry.ordered[left].Name]
		rightRank, rightBuiltin := builtInRank[registry.ordered[right].Name]
		if leftBuiltin != rightBuiltin {
			return leftBuiltin
		}
		if leftBuiltin {
			return leftRank < rightRank
		}
		return registry.ordered[left].Name < registry.ordered[right].Name
	})
	return registry
}

func (r *Registry) Descriptors() []domain.ToolDescriptor {
	descriptors := append([]domain.ToolDescriptor(nil), r.ordered...)
	for index := range descriptors {
		descriptors[index].InputSchema = append([]byte(nil), descriptors[index].InputSchema...)
	}
	return descriptors
}

func (r *Registry) Lookup(name string) (ports.Tool, bool) {
	tool, ok := r.byName[name]
	return tool, ok
}
