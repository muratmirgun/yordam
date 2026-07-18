package tooling

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestCatalogAcceptsDynamicToolsRetainsBuiltinAliasOrderAndCopiesSchemas(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","properties":{"value":{"type":"string"}}}`)
	items := []ports.Tool{catalogTool("shell", nil), catalogTool("inspect", schema), catalogTool("edit", nil), catalogTool("read", nil), catalogTool("search", nil)}
	catalog, err := NewCatalog("revision-1", items...)
	if err != nil {
		t.Fatal(err)
	}
	exposure := catalog.Expose()
	aliases := make([]string, 0, len(exposure.Tools))
	for _, tool := range exposure.Tools {
		aliases = append(aliases, tool.Alias)
	}
	want := []string{"read", "search", "edit", "shell", "inspect"}
	if strings.Join(aliases, ",") != strings.Join(want, ",") {
		t.Fatalf("aliases=%v", aliases)
	}
	for index, alias := range want[:4] {
		identity := exposure.Aliases[index].Identity
		if exposure.Tools[index].Alias != alias || identity != (protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: alias}) {
			t.Fatalf("binding[%d]=%#v", index, exposure.Aliases[index])
		}
	}
	exposure.Tools[0].InputSchema[0] = '['
	exposure.Aliases[0].Alias = "changed"
	again := catalog.Expose()
	if again.Tools[0].InputSchema[0] != '{' || again.Aliases[0].Alias != "read" {
		t.Fatal("catalog exposure was mutable through returned values")
	}
	schema[0] = '['
	if catalog.Expose().Tools[4].InputSchema[0] != '{' {
		t.Fatal("catalog retained caller-owned schema")
	}
}

func TestCatalogRejectsAliasCanonicalIdentityAndDescriptorDigestCollisions(t *testing.T) {
	identity := protocol.ToolIdentity{Source: "mcp", Authority: "server-a", Name: "remote"}
	first := catalogExternalTool("first", identity, trustedRemoteClassification())
	second := catalogExternalTool("second", identity, trustedRemoteClassification())
	if _, err := NewCatalog("revision-1", first, second); err == nil || !strings.Contains(err.Error(), "canonical identity") {
		t.Fatalf("identity collision error=%v", err)
	}
	if _, err := NewCatalog("revision-1", catalogTool("read", nil), catalogTool("read", nil)); err == nil || !strings.Contains(err.Error(), "alias") {
		t.Fatalf("alias collision error=%v", err)
	}
	bad := catalogExternalTool("bad", protocol.ToolIdentity{Source: "mcp", Authority: "server-a", Name: "bad"}, trustedRemoteClassification())
	bad.canonical.DescriptorDigest.Value = strings.Repeat("0", 64)
	if _, err := NewCatalog("revision-1", bad); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("digest mismatch error=%v", err)
	}
}

func TestDescriptorDigestChangesWithTrustedSemanticsAndCannotBeLoweredBySource(t *testing.T) {
	identity := protocol.ToolIdentity{Source: "mcp", Authority: "server-a", Name: "write_record"}
	remote := catalogExternalTool("records", identity, trustedRemoteClassification())
	remote.canonical.Body.Effect = "observation"
	remote.canonical.Body.Mutation = "read_only"
	remote.canonical.Body.Idempotency = "idempotent"
	remote.canonical.DescriptorDigest, _ = canonicaljson.Digest(remote.canonical.Body)

	catalog, err := NewCatalog("revision-1", remote)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, ok := catalog.Descriptor("records")
	if !ok {
		t.Fatal("descriptor not found")
	}
	if descriptor.Body.Effect != "mutation" || descriptor.Body.Mutation != "remote" || descriptor.Body.Idempotency != "unknown" || descriptor.Body.ClassificationSource != "trusted_adapter" {
		t.Fatalf("trusted classification was lowered: %#v", descriptor.Body)
	}
	source, ok := catalog.SourceAnnotation("records")
	if !ok || source.Body.Effect != "observation" || source.Body.Mutation != "read_only" {
		t.Fatalf("source annotation was not retained separately: %#v, %v", source, ok)
	}

	changed := catalogExternalTool("records", identity, trustedRemoteClassification())
	classification := changed.classification
	classification.Retry = "retry_with_idempotency_key"
	changed.classification = classification
	other, err := NewCatalog("revision-1", changed)
	if err != nil {
		t.Fatal(err)
	}
	changedDescriptor, _ := other.Descriptor("records")
	if changedDescriptor.DescriptorDigest == descriptor.DescriptorDigest {
		t.Fatal("descriptor digest did not change with trusted semantics")
	}
}

func TestCatalogUnknownSourceUsesConservativeClassification(t *testing.T) {
	unknown := catalogExternalTool("unknown", protocol.ToolIdentity{Source: "plugin", Authority: "unreviewed", Name: "unknown"}, domain.ToolClassification{})
	catalog, err := NewCatalog("revision-1", unknown)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, _ := catalog.Descriptor("unknown")
	if descriptor.Body.Effect != "mutation" || descriptor.Body.Mutation != "remote_or_unknown" || descriptor.Body.ClassificationSource != "conservative_default" || descriptor.Body.Retry != "never_after_dispatch" {
		t.Fatalf("unknown source was not conservative: %#v", descriptor.Body)
	}
}

func catalogTool(alias string, schema json.RawMessage) *fakeCatalogTool {
	if schema == nil {
		schema = json.RawMessage(`{"type":"object"}`)
	}
	mutation := domain.MutationReadOnly
	if alias == "edit" {
		mutation = domain.MutationFile
	} else if alias == "shell" {
		mutation = domain.MutationProcess
	}
	return &fakeCatalogTool{legacy: domain.ToolDescriptor{Name: alias, Description: alias + " tool", ScopeDescription: "canonical target", InputSchema: schema, Mutation: mutation}}
}

func catalogExternalTool(alias string, identity protocol.ToolIdentity, classification domain.ToolClassification) *fakeCatalogTool {
	body := protocol.ToolDescriptorBody{
		Identity: identity, SourceRevision: "source-r1", DisplayName: alias, Description: alias + " source tool",
		InputSchema: json.RawMessage(`{"type":"object"}`), Effect: "observation", Mutation: "read_only",
		ExecutionLoci: []string{"remote"}, ClassificationSource: "source_annotation", Idempotency: "idempotent", Retry: "safe_before_dispatch",
	}
	digest, _ := canonicaljson.Digest(body)
	return &fakeCatalogTool{
		legacy:    domain.ToolDescriptor{Name: alias, Description: body.Description, ScopeDescription: "canonical target", InputSchema: body.InputSchema, Mutation: domain.MutationProcess},
		canonical: protocol.ToolDescriptor{Body: body, DescriptorDigest: digest}, classification: classification,
	}
}

func trustedRemoteClassification() domain.ToolClassification {
	return domain.ToolClassification{Effect: "mutation", Mutation: "remote", ExecutionLoci: []string{"remote"}, Boundary: "remote", Reversibility: "unknown", VerificationCoverage: "provider_reported", Idempotency: "unknown", Retry: "never_after_dispatch", RequestedProfile: "networked", EffectiveProfile: "networked"}
}

type fakeCatalogTool struct {
	legacy         domain.ToolDescriptor
	canonical      protocol.ToolDescriptor
	classification domain.ToolClassification
}

func (t *fakeCatalogTool) Descriptor() domain.ToolDescriptor                { return t.legacy }
func (t *fakeCatalogTool) CanonicalDescriptor() protocol.ToolDescriptor     { return t.canonical }
func (t *fakeCatalogTool) TrustedClassification() domain.ToolClassification { return t.classification }
func (*fakeCatalogTool) Prepare(context.Context, domain.ToolRequest) (ports.PreparedTool, error) {
	return nil, nil
}

func (*fakeCatalogTool) Plan(_ context.Context, request domain.ToolRequest) (ports.PreparedTool, error) {
	return &fakePreparedTool{request: request}, nil
}

type fakePreparedTool struct{ request domain.ToolRequest }

func (p *fakePreparedTool) Preview() domain.PreparedToolRequest {
	return domain.PreparedToolRequest{Request: p.request, InsideWorkspace: true, Resources: []protocol.ResourceTarget{{Kind: "record", CanonicalID: "record-1"}}}
}

func (*fakePreparedTool) Execute(context.Context) domain.ToolResult { return domain.ToolResult{} }

func (p *fakePreparedTool) Revalidate(context.Context) (domain.PreparedToolRequest, error) {
	return p.Preview(), nil
}
