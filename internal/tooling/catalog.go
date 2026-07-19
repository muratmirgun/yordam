package tooling

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type catalogEntry struct {
	alias          string
	tool           ports.Tool
	descriptor     protocol.ToolDescriptor
	source         protocol.ToolDescriptor
	classification domain.ToolClassification
}

type Catalog struct {
	revision string
	ordered  []catalogEntry
	byAlias  map[string]catalogEntry
}

var builtinOrder = map[string]int{"read": 0, "search": 1, "edit": 2, "shell": 3}

func NewCatalog(revision string, tools ...ports.Tool) (*Catalog, error) {
	if revision == "" {
		return nil, fmt.Errorf("catalog revision is required")
	}
	catalog := &Catalog{revision: revision, byAlias: make(map[string]catalogEntry, len(tools))}
	identities := make(map[protocol.ToolIdentity]string, len(tools))
	for _, tool := range tools {
		if tool == nil {
			return nil, fmt.Errorf("catalog tool is nil")
		}
		legacy := tool.Descriptor()
		if err := legacy.Validate(); err != nil {
			return nil, fmt.Errorf("tool alias %q: %w", legacy.Name, err)
		}
		if _, exists := catalog.byAlias[legacy.Name]; exists {
			return nil, fmt.Errorf("duplicate tool alias %q", legacy.Name)
		}

		source, supplied, err := sourceDescriptor(tool)
		if err != nil {
			return nil, fmt.Errorf("tool alias %q: %w", legacy.Name, err)
		}
		classification, trusted := classificationFor(tool)
		if err := classification.Validate(); err != nil {
			return nil, fmt.Errorf("tool alias %q: %w", legacy.Name, err)
		}
		body, err := canonicalDescriptorBody(revision, legacy, source, supplied, unwrappedIdentity(tool), classification, trusted)
		if err != nil {
			return nil, fmt.Errorf("tool alias %q: %w", legacy.Name, err)
		}
		digest, err := canonicaljson.Digest(body)
		if err != nil {
			return nil, fmt.Errorf("digest tool alias %q: %w", legacy.Name, err)
		}
		if previous, exists := identities[body.Identity]; exists {
			return nil, fmt.Errorf("duplicate canonical identity for aliases %q and %q", previous, legacy.Name)
		}
		identities[body.Identity] = legacy.Name
		entry := catalogEntry{alias: legacy.Name, tool: tool, descriptor: protocol.ToolDescriptor{Body: body, DescriptorDigest: digest}, source: cloneDescriptor(source), classification: cloneClassification(classification)}
		catalog.byAlias[legacy.Name] = entry
		catalog.ordered = append(catalog.ordered, entry)
	}
	sort.Slice(catalog.ordered, func(i, j int) bool {
		leftRank, leftBuiltin := builtinOrder[catalog.ordered[i].alias]
		rightRank, rightBuiltin := builtinOrder[catalog.ordered[j].alias]
		if leftBuiltin != rightBuiltin {
			return leftBuiltin
		}
		if leftBuiltin {
			return leftRank < rightRank
		}
		return catalog.ordered[i].alias < catalog.ordered[j].alias
	})
	if err := catalog.Expose().Validate(); err != nil {
		return nil, fmt.Errorf("validate tool exposure: %w", err)
	}
	return catalog, nil
}

func sourceDescriptor(tool ports.Tool) (protocol.ToolDescriptor, bool, error) {
	provider, ok := tool.(ports.CanonicalDescriptorProvider)
	if !ok {
		return protocol.ToolDescriptor{}, false, nil
	}
	descriptor := cloneDescriptor(provider.CanonicalDescriptor())
	if descriptor.Body.Identity == (protocol.ToolIdentity{}) && descriptor.DescriptorDigest.IsZero() {
		return protocol.ToolDescriptor{}, false, nil
	}
	canonicalBody, err := normalizeSourceBody(descriptor.Body)
	if err != nil {
		return protocol.ToolDescriptor{}, false, err
	}
	if err := canonicaljson.ValidateDigest(canonicalBody, descriptor.DescriptorDigest); err != nil {
		return protocol.ToolDescriptor{}, false, fmt.Errorf("descriptor digest mismatch: %w", err)
	}
	descriptor.Body = canonicalBody
	return descriptor, true, nil
}

func normalizeSourceBody(body protocol.ToolDescriptorBody) (protocol.ToolDescriptorBody, error) {
	body.InputSchema = cloneRaw(body.InputSchema)
	body.OutputSchema = cloneRaw(body.OutputSchema)
	input, err := canonicaljson.Marshal(body.InputSchema)
	if err != nil {
		return protocol.ToolDescriptorBody{}, fmt.Errorf("canonicalize input schema: %w", err)
	}
	body.InputSchema = input
	if body.OutputSchema != nil {
		output, err := canonicaljson.Marshal(body.OutputSchema)
		if err != nil {
			return protocol.ToolDescriptorBody{}, fmt.Errorf("canonicalize output schema: %w", err)
		}
		body.OutputSchema = output
	}
	body.ExecutionLoci = sortedUnique(body.ExecutionLoci)
	if err := body.Validate(); err != nil {
		return protocol.ToolDescriptorBody{}, err
	}
	return body, nil
}

func canonicalDescriptorBody(revision string, legacy domain.ToolDescriptor, source protocol.ToolDescriptor, supplied bool, fallbackIdentity protocol.ToolIdentity, classification domain.ToolClassification, trusted bool) (protocol.ToolDescriptorBody, error) {
	var body protocol.ToolDescriptorBody
	if supplied {
		body = source.Body
	} else {
		input, err := canonicaljson.Marshal(legacy.InputSchema)
		if err != nil {
			return body, fmt.Errorf("canonicalize input schema: %w", err)
		}
		body = protocol.ToolDescriptorBody{
			Identity:       fallbackIdentity,
			SourceRevision: revision, DisplayName: legacy.Name, Description: legacy.Description, InputSchema: input,
		}
	}
	body.Effect = classification.Effect
	body.Mutation = classification.Mutation
	body.ExecutionLoci = sortedUnique(classification.ExecutionLoci)
	if trusted {
		body.ClassificationSource = "trusted_adapter"
	} else {
		body.ClassificationSource = "conservative_default"
	}
	body.Idempotency = classification.Idempotency
	body.Retry = classification.Retry
	if err := body.Validate(); err != nil {
		return protocol.ToolDescriptorBody{}, err
	}
	return body, nil
}

func unwrappedIdentity(tool ports.Tool) protocol.ToolIdentity {
	typeOf := reflect.TypeOf(tool)
	for typeOf.Kind() == reflect.Pointer {
		typeOf = typeOf.Elem()
	}
	authority := typeOf.PkgPath()
	if authority == "" {
		authority = "unknown_go_package"
	}
	name := typeOf.Name()
	if name == "" {
		name = "unnamed_tool"
	}
	return protocol.ToolIdentity{Source: "untrusted", Authority: authority, Name: name}
}

func classificationFor(tool ports.Tool) (domain.ToolClassification, bool) {
	if provider, ok := tool.(ports.TrustedClassificationProvider); ok {
		classification := provider.TrustedClassification()
		if classification.Effect != "" {
			return cloneClassification(classification), true
		}
	}
	return domain.ToolClassification{
		Effect: "mutation", Mutation: "remote_or_unknown", ExecutionLoci: []string{"process", "remote_or_unknown"},
		Boundary: "remote_or_unknown", Reversibility: "unknown", VerificationCoverage: "none",
		Idempotency: "unknown", Retry: "never_after_dispatch", RequestedProfile: "ask_or_deny", EffectiveProfile: "ask_or_deny",
	}, false
}

func (c *Catalog) Expose() protocol.ToolExposure {
	exposure := protocol.ToolExposure{CatalogRevision: c.revision, Tools: make([]protocol.ExposedTool, 0, len(c.ordered)), Aliases: make([]protocol.ToolAliasBinding, 0, len(c.ordered))}
	for _, entry := range c.ordered {
		body := entry.descriptor.Body
		exposure.Tools = append(exposure.Tools, protocol.ExposedTool{Alias: entry.alias, Identity: body.Identity, Description: body.Description, InputSchema: cloneRaw(body.InputSchema)})
		exposure.Aliases = append(exposure.Aliases, protocol.ToolAliasBinding{Alias: entry.alias, Identity: body.Identity, SourceRevision: body.SourceRevision, DescriptorDigest: entry.descriptor.DescriptorDigest})
	}
	return exposure
}

func (c *Catalog) Descriptor(alias string) (protocol.ToolDescriptor, bool) {
	entry, ok := c.byAlias[alias]
	if !ok {
		return protocol.ToolDescriptor{}, false
	}
	return cloneDescriptor(entry.descriptor), true
}

// SourceAnnotation returns the provider-authored descriptor as an untrusted
// hint. Callers must use Descriptor for authorization semantics.
func (c *Catalog) SourceAnnotation(alias string) (protocol.ToolDescriptor, bool) {
	entry, ok := c.byAlias[alias]
	if !ok || entry.source.Body.Identity == (protocol.ToolIdentity{}) {
		return protocol.ToolDescriptor{}, false
	}
	return cloneDescriptor(entry.source), true
}

func cloneDescriptor(descriptor protocol.ToolDescriptor) protocol.ToolDescriptor {
	descriptor.Body.InputSchema = cloneRaw(descriptor.Body.InputSchema)
	descriptor.Body.OutputSchema = cloneRaw(descriptor.Body.OutputSchema)
	descriptor.Body.ExecutionLoci = append([]string(nil), descriptor.Body.ExecutionLoci...)
	return descriptor
}

func cloneClassification(classification domain.ToolClassification) domain.ToolClassification {
	classification.ExecutionLoci = append([]string(nil), classification.ExecutionLoci...)
	return classification
}

func cloneRaw(raw json.RawMessage) json.RawMessage { return bytes.Clone(raw) }

func sortedUnique(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	if len(result) < 2 {
		return result
	}
	write := 1
	for read := 1; read < len(result); read++ {
		if result[read] == result[write-1] {
			continue
		}
		result[write] = result[read]
		write++
	}
	return result[:write]
}
