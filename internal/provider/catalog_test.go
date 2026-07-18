package provider

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestRequiredUnknownCapabilityFailsBeforeProvider(t *testing.T) {
	catalog := NewCatalog("rev-1", []protocol.ModelDescriptor{descriptorWith(protocol.CapabilityStructuredOutput, protocol.CapabilityUnknown)})
	_, err := catalog.Negotiate("openai", "model-a", []protocol.CapabilityRequirement{{Capability: protocol.CapabilityStructuredOutput, Level: protocol.CapabilityRequired}}, "tools-rev")
	if !errors.Is(err, ErrCapabilityUnavailable) {
		t.Fatalf("err=%v", err)
	}
}

func TestCatalogReturnsDeepCopiesAndCanonicalPlans(t *testing.T) {
	descriptor := descriptorWith(protocol.CapabilityTextInput, protocol.CapabilitySupported)
	catalog := NewCatalog("rev-1", []protocol.ModelDescriptor{descriptor})

	listed := catalog.List()
	listed[0].Capabilities[0].State = protocol.CapabilityUnsupported
	resolved, ok := catalog.Resolve("openai", "model-a")
	if !ok || resolved.Capabilities[0].State != protocol.CapabilitySupported {
		t.Fatalf("resolved=%#v ok=%v", resolved, ok)
	}

	plan, err := catalog.Negotiate("openai", "model-a", []protocol.CapabilityRequirement{
		{Capability: protocol.CapabilityTextOutput, Level: protocol.CapabilityPreferred},
		{Capability: protocol.CapabilityTextInput, Level: protocol.CapabilityRequired},
	}, "tools-rev")
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Body.Requirements) != 2 || plan.Body.Requirements[0].Capability != protocol.CapabilityTextInput || len(plan.Body.Warnings) != 1 {
		t.Fatalf("plan=%#v", plan)
	}
	want, err := canonicaljson.Digest(plan.Body)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Digest != want {
		t.Fatalf("digest=%v want=%v", plan.Digest, want)
	}
}

func TestCatalogRejectsLowerPrecedenceCapabilityContradiction(t *testing.T) {
	high := descriptorWith(protocol.CapabilityToolUse, protocol.CapabilitySupported)
	high.Capabilities[0].Provenance = "adapter_guarantee"
	low := descriptorWith(protocol.CapabilityToolUse, protocol.CapabilityUnsupported)
	low.Capabilities[0].Provenance = "configured_claim"
	catalog := NewCatalog("rev-1", []protocol.ModelDescriptor{low, high})
	if _, ok := catalog.Resolve("openai", "model-a"); ok {
		t.Fatal("contradictory candidate resolved")
	}
}

func TestCatalogResolutionIsInputOrderIndependentAcrossSourcedFacts(t *testing.T) {
	configured := descriptorWith(protocol.CapabilityToolUse, protocol.CapabilitySupported)
	configured.ContextWindow.Provenance = "configured_claim"
	configured.MaximumOutput.Provenance = "configured_claim"
	configured.Capabilities[0].Provenance = "configured_claim"
	configured.Pricing = []protocol.PricingFact{{Category: "input", PerMillionDecimal: "1", Currency: "USD", Provenance: "configured_claim"}}
	adapter := protocol.DeepCopy(configured)
	adapter.ContextWindow.Provenance = "adapter_guarantee"
	adapter.MaximumOutput.Provenance = "adapter_guarantee"
	adapter.Capabilities[0].Provenance = "adapter_guarantee"
	adapter.Pricing[0].Provenance = "bundled_metadata"

	forward, ok := NewCatalog("rev-1", []protocol.ModelDescriptor{configured, adapter}).Resolve("openai", "model-a")
	if !ok {
		t.Fatal("forward catalog did not resolve")
	}
	reversed, ok := NewCatalog("rev-1", []protocol.ModelDescriptor{adapter, configured}).Resolve("openai", "model-a")
	if !ok {
		t.Fatal("reversed catalog did not resolve")
	}
	if !reflect.DeepEqual(forward, reversed) {
		t.Fatalf("forward=%#v reversed=%#v", forward, reversed)
	}
	if forward.ContextWindow.Provenance != "adapter_guarantee" || forward.MaximumOutput.Provenance != "adapter_guarantee" || forward.Capabilities[0].Provenance != "adapter_guarantee" || forward.Pricing[0].Provenance != "bundled_metadata" {
		t.Fatalf("resolved=%#v", forward)
	}
}

func TestCatalogRejectsContradictoryOrUnprovenancedCandidateFacts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ModelDescriptor)
	}{
		{name: "context window", mutate: func(candidate *protocol.ModelDescriptor) {
			candidate.ContextWindow.Value = 4096
			candidate.ContextWindow.Provenance = "configured_claim"
		}},
		{name: "maximum output", mutate: func(candidate *protocol.ModelDescriptor) {
			candidate.MaximumOutput.Value = 1024
			candidate.MaximumOutput.Provenance = "configured_claim"
		}},
		{name: "usage categories", mutate: func(candidate *protocol.ModelDescriptor) { candidate.UsageCategories = []string{"output"} }},
		{name: "credential binding", mutate: func(candidate *protocol.ModelDescriptor) { candidate.CredentialBindingRef = "credential:other" }},
		{name: "source revision", mutate: func(candidate *protocol.ModelDescriptor) { candidate.SourceRevision = "rev-other" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			high := descriptorWith(protocol.CapabilityToolUse, protocol.CapabilitySupported)
			high.ContextWindow.Provenance = "adapter_guarantee"
			high.MaximumOutput.Provenance = "adapter_guarantee"
			low := protocol.DeepCopy(high)
			test.mutate(&low)
			catalog := NewCatalog("rev-1", []protocol.ModelDescriptor{low, high})
			if _, ok := catalog.Resolve("openai", "model-a"); ok {
				t.Fatal("contradictory candidate resolved")
			}
			_, err := catalog.Negotiate("openai", "model-a", nil, "tools-rev")
			if !errors.Is(err, ErrCatalogCandidate) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestCatalogRejectsContradictoryPricingClaims(t *testing.T) {
	high := descriptorWith(protocol.CapabilityToolUse, protocol.CapabilitySupported)
	high.Pricing = []protocol.PricingFact{{Category: "input", PerMillionDecimal: "1", Currency: "USD", Provenance: "bundled_metadata"}}
	low := protocol.DeepCopy(high)
	low.Pricing[0] = protocol.PricingFact{Category: "input", PerMillionDecimal: "2", Currency: "USD", Provenance: "configured_claim"}
	catalog := NewCatalog("rev-1", []protocol.ModelDescriptor{low, high})
	if _, ok := catalog.Resolve("openai", "model-a"); ok {
		t.Fatal("contradictory pricing candidate resolved")
	}
	if _, err := catalog.Negotiate("openai", "model-a", nil, "tools-rev"); !errors.Is(err, ErrCatalogCandidate) {
		t.Fatalf("error=%v", err)
	}
}

func descriptorWith(capability, state string) protocol.ModelDescriptor {
	return protocol.ModelDescriptor{
		ProviderID: "openai", ModelID: "model-a", AdapterKind: "openai_compatible", DisplayName: "Model A",
		ContextWindow:   protocol.ValueInt64{State: protocol.ValueKnown, Value: 8192, Provenance: "bundled_metadata"},
		MaximumOutput:   protocol.ValueInt64{State: protocol.ValueKnown, Value: 2048, Provenance: "bundled_metadata"},
		Capabilities:    []protocol.CapabilityFact{{Capability: capability, State: state, Provenance: "adapter_guarantee", RuntimeGenerationID: "generation-1"}},
		UsageCategories: []string{}, Pricing: []protocol.PricingFact{}, CredentialBindingRef: "credential:openai",
		SourceRevision: "rev-1", RuntimeGenerationID: "generation-1",
	}
}

func testDigest(fill byte) protocol.Digest {
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat(string(fill), 64)}
}
