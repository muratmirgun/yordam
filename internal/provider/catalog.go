package provider

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
)

var (
	ErrCapabilityUnavailable = errors.New("capability_unavailable")
	ErrModelUnavailable      = errors.New("model_unavailable")
)

type Catalog interface {
	Revision() string
	List() []protocol.ModelDescriptor
	Resolve(protocol.ProviderID, protocol.ModelID) (protocol.ModelDescriptor, bool)
	Negotiate(protocol.ProviderID, protocol.ModelID, []protocol.CapabilityRequirement, string) (protocol.NegotiatedProviderPlan, error)
}

type catalog struct {
	revision string
	resolved map[modelKey]protocol.ModelDescriptor
}

type modelKey struct {
	provider protocol.ProviderID
	model    protocol.ModelID
}

func NewCatalog(revision string, descriptors []protocol.ModelDescriptor) Catalog {
	c := &catalog{revision: revision, resolved: make(map[modelKey]protocol.ModelDescriptor)}
	groups := make(map[modelKey][]protocol.ModelDescriptor)
	for _, descriptor := range descriptors {
		if descriptor.Validate() != nil {
			continue
		}
		key := modelKey{provider: descriptor.ProviderID, model: descriptor.ModelID}
		groups[key] = append(groups[key], protocol.DeepCopy(descriptor))
	}
	for key, candidates := range groups {
		if descriptor, ok := resolveCandidates(candidates); ok {
			c.resolved[key] = descriptor
		}
	}
	return c
}

func (c *catalog) Revision() string { return c.revision }

func (c *catalog) List() []protocol.ModelDescriptor {
	keys := make([]modelKey, 0, len(c.resolved))
	for key := range c.resolved {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].provider == keys[j].provider {
			return keys[i].model < keys[j].model
		}
		return keys[i].provider < keys[j].provider
	})
	result := make([]protocol.ModelDescriptor, 0, len(keys))
	for _, key := range keys {
		result = append(result, protocol.DeepCopy(c.resolved[key]))
	}
	return result
}

func (c *catalog) Resolve(providerID protocol.ProviderID, modelID protocol.ModelID) (protocol.ModelDescriptor, bool) {
	descriptor, ok := c.resolved[modelKey{provider: providerID, model: modelID}]
	return protocol.DeepCopy(descriptor), ok
}

func (c *catalog) Negotiate(providerID protocol.ProviderID, modelID protocol.ModelID, requirements []protocol.CapabilityRequirement, toolRevision string) (protocol.NegotiatedProviderPlan, error) {
	descriptor, ok := c.Resolve(providerID, modelID)
	if !ok {
		return protocol.NegotiatedProviderPlan{}, fmt.Errorf("%w: provider=%q model=%q", ErrModelUnavailable, providerID, modelID)
	}
	if strings.TrimSpace(toolRevision) == "" {
		return protocol.NegotiatedProviderPlan{}, fmt.Errorf("tool exposure revision is required")
	}
	requirements = protocol.DeepCopy(requirements)
	sort.Slice(requirements, func(i, j int) bool { return requirements[i].Capability < requirements[j].Capability })
	facts := make(map[string]string, len(descriptor.Capabilities))
	for _, fact := range descriptor.Capabilities {
		facts[fact.Capability] = fact.State
	}
	warnings := make([]string, 0)
	previous := ""
	for _, requirement := range requirements {
		if err := requirement.Validate(); err != nil {
			return protocol.NegotiatedProviderPlan{}, err
		}
		if requirement.Capability == previous {
			return protocol.NegotiatedProviderPlan{}, fmt.Errorf("duplicate capability requirement %q", requirement.Capability)
		}
		previous = requirement.Capability
		state := facts[requirement.Capability]
		if state == "" {
			state = protocol.CapabilityUnknown
		}
		if requirement.Level == protocol.CapabilityRequired && state != protocol.CapabilitySupported {
			return protocol.NegotiatedProviderPlan{}, fmt.Errorf("%w: capability=%s state=%s", ErrCapabilityUnavailable, requirement.Capability, state)
		}
		if requirement.Level == protocol.CapabilityPreferred && state != protocol.CapabilitySupported {
			warnings = append(warnings, fmt.Sprintf("preferred capability %s is %s", requirement.Capability, state))
		}
	}
	sort.Strings(warnings)
	body := protocol.NegotiatedProviderPlanBody{Descriptor: descriptor, Requirements: requirements, ToolExposureRevision: toolRevision, Warnings: warnings}
	digest, err := canonicaljson.Digest(body)
	if err != nil {
		return protocol.NegotiatedProviderPlan{}, err
	}
	return protocol.NegotiatedProviderPlan{Body: protocol.DeepCopy(body), Digest: digest}, nil
}

func resolveCandidates(candidates []protocol.ModelDescriptor) (protocol.ModelDescriptor, bool) {
	if len(candidates) == 0 {
		return protocol.ModelDescriptor{}, false
	}
	selected := protocol.DeepCopy(candidates[0])
	states := make(map[string]string)
	facts := make(map[string]protocol.CapabilityFact)
	for _, descriptor := range candidates {
		if descriptor.ProviderID != selected.ProviderID || descriptor.ModelID != selected.ModelID || descriptor.AdapterKind != selected.AdapterKind || descriptor.RuntimeGenerationID != selected.RuntimeGenerationID {
			return protocol.ModelDescriptor{}, false
		}
		for _, fact := range descriptor.Capabilities {
			if state, exists := states[fact.Capability]; exists && state != fact.State {
				return protocol.ModelDescriptor{}, false
			}
			states[fact.Capability] = fact.State
			current, exists := facts[fact.Capability]
			if !exists || capabilityPrecedence(fact.Provenance) > capabilityPrecedence(current.Provenance) {
				facts[fact.Capability] = protocol.DeepCopy(fact)
			}
		}
	}
	selected.Capabilities = selected.Capabilities[:0]
	for _, fact := range facts {
		selected.Capabilities = append(selected.Capabilities, fact)
	}
	sort.Slice(selected.Capabilities, func(i, j int) bool { return selected.Capabilities[i].Capability < selected.Capabilities[j].Capability })
	return selected, true
}

func capabilityPrecedence(provenance string) int {
	normalized := strings.NewReplacer(" ", "_", "-", "_").Replace(strings.ToLower(provenance))
	switch normalized {
	case "adapter", "adapter_guarantee":
		return 4
	case "negotiated", "negotiated_fact":
		return 3
	case "bundled", "bundled_metadata", "catalog":
		return 2
	case "configured", "configured_claim":
		return 1
	default:
		return 0
	}
}
