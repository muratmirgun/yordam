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
	ErrCatalogCandidate      = errors.New("catalog_candidate_invalid")
)

type Catalog interface {
	Revision() string
	List() []protocol.ModelDescriptor
	Resolve(protocol.ProviderID, protocol.ModelID) (protocol.ModelDescriptor, bool)
	Negotiate(protocol.ProviderID, protocol.ModelID, []protocol.CapabilityRequirement, string) (protocol.NegotiatedProviderPlan, error)
}

type catalog struct {
	revision        string
	resolved        map[modelKey]protocol.ModelDescriptor
	candidateErrors map[modelKey]error
}

type modelKey struct {
	provider protocol.ProviderID
	model    protocol.ModelID
}

func NewCatalog(revision string, descriptors []protocol.ModelDescriptor) Catalog {
	c := &catalog{revision: revision, resolved: make(map[modelKey]protocol.ModelDescriptor), candidateErrors: make(map[modelKey]error)}
	groups := make(map[modelKey][]protocol.ModelDescriptor)
	for _, descriptor := range descriptors {
		key := modelKey{provider: descriptor.ProviderID, model: descriptor.ModelID}
		if err := descriptor.Validate(); err != nil {
			if key.provider != "" && key.model != "" {
				c.candidateErrors[key] = fmt.Errorf("descriptor validation: %w", err)
			}
			continue
		}
		groups[key] = append(groups[key], protocol.DeepCopy(descriptor))
	}
	for key, candidates := range groups {
		if _, invalid := c.candidateErrors[key]; invalid {
			continue
		}
		descriptor, err := resolveCandidates(candidates)
		if err != nil {
			c.candidateErrors[key] = err
			continue
		}
		c.resolved[key] = descriptor
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
	key := modelKey{provider: providerID, model: modelID}
	if err := c.candidateErrors[key]; err != nil {
		return protocol.NegotiatedProviderPlan{}, fmt.Errorf("%w: provider=%q model=%q: %v", ErrCatalogCandidate, providerID, modelID, err)
	}
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

func resolveCandidates(candidates []protocol.ModelDescriptor) (protocol.ModelDescriptor, error) {
	if len(candidates) == 0 {
		return protocol.ModelDescriptor{}, fmt.Errorf("candidate set is empty")
	}
	seed := candidates[0]
	selected := protocol.ModelDescriptor{
		ProviderID: seed.ProviderID, ModelID: seed.ModelID, AdapterKind: seed.AdapterKind,
		DisplayName: seed.DisplayName, UsageCategories: protocol.DeepCopy(seed.UsageCategories),
		CredentialBindingRef: seed.CredentialBindingRef, SourceRevision: seed.SourceRevision,
		RuntimeGenerationID: seed.RuntimeGenerationID,
	}
	states := make(map[string]string)
	facts := make(map[string]protocol.CapabilityFact)
	pricing := make(map[string]protocol.PricingFact)
	var contextWindow, maximumOutput protocol.ValueInt64
	haveContext, haveMaximum := false, false
	for _, descriptor := range candidates {
		if descriptor.ProviderID != selected.ProviderID || descriptor.ModelID != selected.ModelID || descriptor.AdapterKind != selected.AdapterKind || descriptor.RuntimeGenerationID != selected.RuntimeGenerationID {
			return protocol.ModelDescriptor{}, fmt.Errorf("provider, model, adapter, or runtime generation differs")
		}
		if descriptor.DisplayName != selected.DisplayName {
			return protocol.ModelDescriptor{}, fmt.Errorf("display name has no provenance and differs")
		}
		if !equalStrings(descriptor.UsageCategories, selected.UsageCategories) {
			return protocol.ModelDescriptor{}, fmt.Errorf("usage categories have no provenance and differ")
		}
		if descriptor.CredentialBindingRef != selected.CredentialBindingRef {
			return protocol.ModelDescriptor{}, fmt.Errorf("credential binding has no provenance and differs")
		}
		if descriptor.SourceRevision != selected.SourceRevision {
			return protocol.ModelDescriptor{}, fmt.Errorf("source revision has no provenance and differs")
		}
		var err error
		contextWindow, haveContext, err = mergeInt64Fact(contextWindow, haveContext, descriptor.ContextWindow, "context window")
		if err != nil {
			return protocol.ModelDescriptor{}, err
		}
		maximumOutput, haveMaximum, err = mergeInt64Fact(maximumOutput, haveMaximum, descriptor.MaximumOutput, "maximum output")
		if err != nil {
			return protocol.ModelDescriptor{}, err
		}
		for _, fact := range descriptor.Capabilities {
			if state, exists := states[fact.Capability]; exists && state != fact.State {
				return protocol.ModelDescriptor{}, fmt.Errorf("capability %q contradicts a sourced fact", fact.Capability)
			}
			states[fact.Capability] = fact.State
			current, exists := facts[fact.Capability]
			if !exists || preferProvenance(fact.Provenance, current.Provenance) {
				facts[fact.Capability] = protocol.DeepCopy(fact)
			}
		}
		for _, fact := range descriptor.Pricing {
			current, exists := pricing[fact.Category]
			if exists && (current.PerMillionDecimal != fact.PerMillionDecimal || current.Currency != fact.Currency) {
				return protocol.ModelDescriptor{}, fmt.Errorf("pricing category %q contradicts a sourced fact", fact.Category)
			}
			if !exists || preferProvenance(fact.Provenance, current.Provenance) {
				pricing[fact.Category] = protocol.DeepCopy(fact)
			}
		}
	}
	selected.ContextWindow = contextWindow
	selected.MaximumOutput = maximumOutput
	for _, fact := range facts {
		selected.Capabilities = append(selected.Capabilities, fact)
	}
	sort.Slice(selected.Capabilities, func(i, j int) bool { return selected.Capabilities[i].Capability < selected.Capabilities[j].Capability })
	for _, fact := range pricing {
		selected.Pricing = append(selected.Pricing, fact)
	}
	sort.Slice(selected.Pricing, func(i, j int) bool { return selected.Pricing[i].Category < selected.Pricing[j].Category })
	if err := selected.Validate(); err != nil {
		return protocol.ModelDescriptor{}, fmt.Errorf("resolved descriptor: %w", err)
	}
	return selected, nil
}

func mergeInt64Fact(current protocol.ValueInt64, exists bool, candidate protocol.ValueInt64, label string) (protocol.ValueInt64, bool, error) {
	if !exists {
		return protocol.DeepCopy(candidate), true, nil
	}
	if current.State != candidate.State || current.Value != candidate.Value {
		return protocol.ValueInt64{}, false, fmt.Errorf("%s contradicts a sourced fact", label)
	}
	if preferProvenance(candidate.Provenance, current.Provenance) {
		return protocol.DeepCopy(candidate), true, nil
	}
	return current, true, nil
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func preferProvenance(candidate, current string) bool {
	candidateRank, currentRank := capabilityPrecedence(candidate), capabilityPrecedence(current)
	return candidateRank > currentRank || (candidateRank == currentRank && candidate < current)
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
