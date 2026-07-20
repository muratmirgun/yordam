package subagent

import (
	"context"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestSubagentDescriptorIsCanonicalTrustedAndOrchestrated(t *testing.T) {
	tool := New()
	marker, ok := any(tool).(ports.OrchestratedTool)
	if !ok || marker.OrchestratedKind() != Kind {
		t.Fatalf("marker=%T kind=%q", marker, marker.OrchestratedKind())
	}
	descriptor := tool.Descriptor()
	if descriptor.Name != "subagent" || descriptor.Mutation != domain.MutationProcess || !strings.Contains(string(descriptor.InputSchema), `"task"`) || !strings.Contains(string(descriptor.InputSchema), `"additionalProperties":false`) {
		t.Fatalf("descriptor=%#v", descriptor)
	}
	canonical := tool.CanonicalDescriptor()
	if canonical.Body.Identity != (protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "subagent"}) || canonical.Body.Effect != "orchestration" || canonical.Body.Mutation != "orchestration" || canonical.Body.ClassificationSource != "trusted_adapter" || canonical.DescriptorDigest.Validate() != nil {
		t.Fatalf("canonical=%#v", canonical)
	}
	if !IsCanonicalDescriptor(canonical) {
		t.Fatalf("canonical descriptor was not recognized")
	}
	forged := canonical
	forged.DescriptorDigest.Value = strings.Repeat("0", 64)
	if IsCanonicalDescriptor(forged) {
		t.Fatal("forged digest matched subagent marker")
	}
}

func TestSubagentPlansExactBoundedInputAndFailsClosedOnDirectExecution(t *testing.T) {
	tool := New()
	prepared, err := tool.Plan(context.Background(), domain.ToolRequest{CallID: "call-1", Name: "subagent", Input: []byte(`{"task":"inspect one package","expected_output":"findings","context":"do not mutate"}`)})
	if err != nil {
		t.Fatal(err)
	}
	preview := prepared.Preview()
	if preview.Mutation != domain.MutationProcess || preview.CanonicalScope != "subagent:call-1" || !preview.InsideWorkspace {
		t.Fatalf("preview=%#v", preview)
	}
	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolFailed || result.ErrorKind != domain.ErrorToolFailed || result.Content != ErrOrchestratorDispatchRequired.Error() {
		t.Fatalf("result=%#v", result)
	}
	for name, raw := range map[string]string{
		"missing task":       `{}`,
		"unknown property":   `{"task":"x","extra":true}`,
		"multiple values":    `{"task":"x"} {}`,
		"blank task":         `{"task":"   "}`,
		"task too large":     `{"task":"` + strings.Repeat("x", protocol.MaxSubagentTaskBytes+1) + `"}`,
		"expected too large": `{"task":"x","expected_output":"` + strings.Repeat("x", protocol.MaxSubagentExpectedOutputBytes+1) + `"}`,
		"context too large":  `{"task":"x","context":"` + strings.Repeat("x", protocol.MaxSubagentContextBytes+1) + `"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := tool.Prepare(context.Background(), domain.ToolRequest{CallID: "call-1", Name: "subagent", Input: []byte(raw)}); err == nil {
				t.Fatal("invalid input was planned")
			}
		})
	}
}
