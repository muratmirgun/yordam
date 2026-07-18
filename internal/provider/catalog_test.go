package provider

import (
	"errors"
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
