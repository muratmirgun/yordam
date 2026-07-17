package tools

import (
	"fmt"
	"sort"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
)

type Registry struct {
	byName  map[string]ports.Tool
	ordered []domain.ToolDescriptor
}

var builtInRank = map[string]int{
	"read":   0,
	"search": 1,
	"edit":   2,
	"shell":  3,
}

var builtInMutation = map[string]domain.MutationKind{
	"read": domain.MutationReadOnly, "search": domain.MutationReadOnly,
	"edit": domain.MutationFile, "shell": domain.MutationProcess,
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
		if _, known := builtInRank[descriptor.Name]; !known {
			panic(fmt.Sprintf("unknown built-in tool %s", descriptor.Name))
		}
		if descriptor.Mutation != builtInMutation[descriptor.Name] {
			panic(fmt.Sprintf("built-in tool %s has mutation %s, want %s", descriptor.Name, descriptor.Mutation, builtInMutation[descriptor.Name]))
		}
		descriptor.InputSchema = append([]byte(nil), descriptor.InputSchema...)
		if _, exists := registry.byName[descriptor.Name]; exists {
			panic(fmt.Sprintf("duplicate tool %s", descriptor.Name))
		}
		registry.byName[descriptor.Name] = item
		registry.ordered = append(registry.ordered, descriptor)
	}
	if len(registry.byName) != len(builtInRank) {
		panic("registry requires read, search, edit, and shell")
	}
	sort.Slice(registry.ordered, func(left, right int) bool {
		return builtInRank[registry.ordered[left].Name] < builtInRank[registry.ordered[right].Name]
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
