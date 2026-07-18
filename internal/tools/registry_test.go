package tools_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/tools"
)

func TestRegistryDescriptorsAreStable(t *testing.T) {
	readTool := registryTool("read")
	searchTool := registryTool("search")
	editTool := registryTool("edit")
	shellTool := registryTool("shell")
	registry := tools.NewRegistry(shellTool, editTool, readTool, searchTool)

	descriptors := registry.Descriptors()
	names := make([]string, 0, len(descriptors))
	for _, descriptor := range descriptors {
		names = append(names, descriptor.Name)
	}
	if !slices.Equal(names, []string{"read", "search", "edit", "shell"}) {
		t.Fatalf("names=%v", names)
	}

	descriptors[0].Name = "changed"
	if got := registry.Descriptors()[0].Name; got != "read" {
		t.Fatalf("registry descriptor mutated through returned slice: %q", got)
	}
	for name, want := range map[string]ports.Tool{
		"read": readTool, "search": searchTool, "edit": editTool, "shell": shellTool,
	} {
		got, ok := registry.Lookup(name)
		if !ok || got != want {
			t.Fatalf("Lookup(%q)=(%v,%v) want (%v,true)", name, got, ok, want)
		}
	}
	if _, ok := registry.Lookup("unknown"); ok {
		t.Fatal("unknown tool found")
	}
}

func TestRegistryAcceptsArbitraryToolsAndRejectsDuplicateAliases(t *testing.T) {
	readTool := registryTool("read")
	searchTool := registryTool("search")
	editTool := registryTool("edit")
	shellTool := registryTool("shell")

	registry := tools.NewRegistry(shellTool, registryTool("inspect"), editTool, readTool, searchTool)
	got := registry.Descriptors()
	names := make([]string, 0, len(got))
	for _, descriptor := range got {
		names = append(names, descriptor.Name)
	}
	if !slices.Equal(names, []string{"read", "search", "edit", "shell", "inspect"}) {
		t.Fatalf("names=%v", names)
	}
	assertPanicsWith(t, "duplicate tool read", func() {
		tools.NewRegistry(readTool, readTool)
	})
}

func registryTool(name string) *descriptorTool {
	mutation := domain.MutationReadOnly
	switch name {
	case "edit":
		mutation = domain.MutationFile
	case "shell":
		mutation = domain.MutationProcess
	}
	return &descriptorTool{descriptor: domain.ToolDescriptor{
		Name:             name,
		Description:      name + " tool",
		ScopeDescription: "test scope",
		InputSchema:      []byte(`{"type":"object"}`),
		Mutation:         mutation,
	}}
}

type descriptorTool struct {
	descriptor domain.ToolDescriptor
}

func (t *descriptorTool) Descriptor() domain.ToolDescriptor {
	return t.descriptor
}

func (*descriptorTool) Prepare(context.Context, domain.ToolRequest) (ports.PreparedTool, error) {
	return nil, fmt.Errorf("not implemented")
}

func assertPanicsWith(t *testing.T, want string, run func()) {
	t.Helper()
	defer func() {
		value := recover()
		if value == nil {
			t.Fatalf("expected panic containing %q", want)
		}
		if got := fmt.Sprint(value); !strings.Contains(got, want) {
			t.Fatalf("panic=%q want containing %q", got, want)
		}
	}()
	run()
}
