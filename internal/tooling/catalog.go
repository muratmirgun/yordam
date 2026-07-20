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

// ToolExposureFilter selects the exact aliases a derived model exposure may
// contain. A supplied revision must already be a stable caller-owned revision;
// otherwise Filter derives one from the parent catalog and selected bindings.
type ToolExposureFilter struct {
	AllowedAliases []string
	Revision       string
}

var builtinOrder = map[string]int{"read": 0, "search": 1, "skill": 2, "subagent": 3, "edit": 4, "shell": 5}

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
	return c.expose(c.revision, c.ordered)
}

// Filter builds an immutable ordered subset. It rejects unknown or duplicate
// aliases so a child cannot silently receive a broader or ambiguous exposure.
func (c *Catalog) Filter(filter ToolExposureFilter) (protocol.ToolExposure, error) {
	if c == nil {
		return protocol.ToolExposure{}, fmt.Errorf("tool catalog is nil")
	}
	allowed := make(map[string]struct{}, len(filter.AllowedAliases))
	for _, alias := range filter.AllowedAliases {
		if _, exists := c.byAlias[alias]; !exists {
			return protocol.ToolExposure{}, fmt.Errorf("unknown tool alias %q", alias)
		}
		if _, duplicate := allowed[alias]; duplicate {
			return protocol.ToolExposure{}, fmt.Errorf("duplicate tool alias %q", alias)
		}
		allowed[alias] = struct{}{}
	}
	entries := make([]catalogEntry, 0, len(allowed))
	for _, entry := range c.ordered {
		if _, include := allowed[entry.alias]; include {
			entries = append(entries, entry)
		}
	}
	revision := filter.Revision
	if revision == "" {
		var err error
		revision, err = derivedExposureRevision(c.revision, entries)
		if err != nil {
			return protocol.ToolExposure{}, err
		}
	}
	exposure := c.expose(revision, entries)
	if err := exposure.Validate(); err != nil {
		return protocol.ToolExposure{}, fmt.Errorf("validate filtered tool exposure: %w", err)
	}
	return exposure, nil
}

// Without derives a child-safe exposure by removing exact registered aliases.
// The derived revision cryptographically binds the retained canonical bindings.
func (c *Catalog) Without(aliases ...string) (protocol.ToolExposure, error) {
	if c == nil {
		return protocol.ToolExposure{}, fmt.Errorf("tool catalog is nil")
	}
	excluded := make(map[string]struct{}, len(aliases))
	for _, alias := range aliases {
		if _, exists := c.byAlias[alias]; !exists {
			return protocol.ToolExposure{}, fmt.Errorf("unknown tool alias %q", alias)
		}
		if _, duplicate := excluded[alias]; duplicate {
			return protocol.ToolExposure{}, fmt.Errorf("duplicate tool alias %q", alias)
		}
		excluded[alias] = struct{}{}
	}
	allowed := make([]string, 0, len(c.ordered)-len(excluded))
	for _, entry := range c.ordered {
		if _, omit := excluded[entry.alias]; !omit {
			allowed = append(allowed, entry.alias)
		}
	}
	return c.Filter(ToolExposureFilter{AllowedAliases: allowed})
}

func (c *Catalog) expose(revision string, entries []catalogEntry) protocol.ToolExposure {
	exposure := protocol.ToolExposure{CatalogRevision: revision, Tools: make([]protocol.ExposedTool, 0, len(entries)), Aliases: make([]protocol.ToolAliasBinding, 0, len(entries))}
	for _, entry := range entries {
		body := entry.descriptor.Body
		exposure.Tools = append(exposure.Tools, protocol.ExposedTool{Alias: entry.alias, Identity: body.Identity, Description: body.Description, InputSchema: cloneRaw(body.InputSchema)})
		exposure.Aliases = append(exposure.Aliases, protocol.ToolAliasBinding{Alias: entry.alias, Identity: body.Identity, SourceRevision: body.SourceRevision, DescriptorDigest: entry.descriptor.DescriptorDigest})
	}
	return exposure
}

func derivedExposureRevision(parent string, entries []catalogEntry) (string, error) {
	type binding struct {
		Alias            string                `json:"alias"`
		Identity         protocol.ToolIdentity `json:"identity"`
		SourceRevision   string                `json:"source_revision"`
		DescriptorDigest protocol.Digest       `json:"descriptor_digest"`
	}
	bindings := make([]binding, 0, len(entries))
	for _, entry := range entries {
		bindings = append(bindings, binding{Alias: entry.alias, Identity: entry.descriptor.Body.Identity, SourceRevision: entry.descriptor.Body.SourceRevision, DescriptorDigest: entry.descriptor.DescriptorDigest})
	}
	digest, err := canonicaljson.Digest(struct {
		Parent   string    `json:"parent"`
		Bindings []binding `json:"bindings"`
	}{Parent: parent, Bindings: bindings})
	if err != nil {
		return "", fmt.Errorf("derive tool exposure revision: %w", err)
	}
	return "derived:" + digest.Value, nil
}

func (c *Catalog) Descriptor(alias string) (protocol.ToolDescriptor, bool) {
	entry, ok := c.byAlias[alias]
	if !ok {
		return protocol.ToolDescriptor{}, false
	}
	return cloneDescriptor(entry.descriptor), true
}

// OrchestratedKind returns an orchestration marker only when the caller's
// descriptor is the exact catalog-bound canonical descriptor. In particular,
// a provider-controlled alias string cannot turn a different tool into an
// orchestrated operation.
func (c *Catalog) OrchestratedKind(alias string, descriptor protocol.ToolDescriptor) (string, bool) {
	entry, ok := c.byAlias[alias]
	if !ok || !reflect.DeepEqual(entry.descriptor, descriptor) {
		return "", false
	}
	marker, ok := entry.tool.(ports.OrchestratedTool)
	if !ok || marker.OrchestratedKind() == "" {
		return "", false
	}
	return marker.OrchestratedKind(), true
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
