package protocol_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/protocol"
)

func protocolDigest(fill byte) protocol.Digest {
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat(string(fill), 64)}
}

func validModelDescriptor() protocol.ModelDescriptor {
	return protocol.ModelDescriptor{
		ProviderID: "provider", ModelID: "model", AdapterKind: "openai_compatible", DisplayName: "Model",
		ContextWindow:   protocol.ValueInt64{State: protocol.ValueKnown, Value: 8192, Provenance: "catalog"},
		MaximumOutput:   protocol.ValueInt64{State: protocol.ValueKnown, Value: 2048, Provenance: "catalog"},
		Capabilities:    []protocol.CapabilityFact{{Capability: protocol.CapabilityTextInput, State: protocol.CapabilitySupported, Provenance: "adapter", RuntimeGenerationID: "generation"}},
		UsageCategories: []string{"input"}, Pricing: []protocol.PricingFact{{Category: "input", PerMillionDecimal: "1.25", Currency: "USD", Provenance: "catalog"}},
		CredentialBindingRef: "env:OPENAI_API_KEY", SourceRevision: "r1", RuntimeGenerationID: "generation",
	}
}

func validToolDescriptorBody() protocol.ToolDescriptorBody {
	return protocol.ToolDescriptorBody{
		Identity: protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "read"}, SourceRevision: "r1", DisplayName: "Read", Description: "Read a file",
		InputSchema: json.RawMessage(`{"type":"object"}`), Effect: "observation", Mutation: "read_only", ExecutionLoci: []string{"builtin"},
		ClassificationSource: "trusted_adapter", Idempotency: "idempotent", Retry: "safe_before_dispatch",
	}
}

func validAuthorizationDecision() protocol.AuthorizationDecision {
	digest := protocolDigest('a')
	resource := protocol.ResourceTarget{Kind: "file", CanonicalID: "/workspace/a"}
	source := protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "read"}
	constraint := protocol.AuthorizationConstraint{Name: "path_prefix", Operator: "equals", Value: json.RawMessage(`"/workspace"`)}
	request := protocol.AuthorizationRequest{
		RequestID: "request", Principal: protocol.ActorRef{ID: "user", Kind: protocol.ActorUser}, Actor: protocol.ActorRef{ID: "agent", Kind: protocol.ActorAgent},
		SessionID: "session", ActivityID: "activity", CallID: "call", QueueID: "queue", Source: source, SourceRevision: "r1", DescriptorDigest: digest,
		Action: "read", Resources: []protocol.ResourceTarget{resource}, ExecutionLocus: "builtin", RequestedProfile: "restricted", EffectiveProfile: "restricted",
		Effect: "observation", Boundary: "workspace", Reversibility: "not_applicable", VerificationCoverage: "full", RuntimeGenerationID: "generation", PolicyGeneration: "policy-r1",
		PolicyProvenance: []protocol.PolicyProvenance{{Source: "platform", Revision: "r1", Generation: "policy-r1"}}, PlanDigest: digest, RequestDigest: digest, DispatchDigest: digest,
	}
	return protocol.AuthorizationDecision{
		Request: request, Action: "allow", Scope: protocol.CanonicalAuthorizationScope{Capability: "read", Source: source, Resources: []protocol.ResourceTarget{resource}, Constraints: []protocol.AuthorizationConstraint{constraint}},
		Constraints: []protocol.AuthorizationConstraint{constraint}, Lifetime: "once", PolicySource: "platform", PolicyGeneration: "policy-r1", Reason: "allowed",
		DecidedAt: time.Unix(1, 0).UTC(), PlanDigest: digest, DecisionNonce: "nonce",
	}
}

func validEnvelope() protocol.EventEnvelope {
	return protocol.EventEnvelope{
		SchemaVersion: 2, PayloadVersion: 1,
		JournalKind: protocol.JournalSession, JournalID: "01J00000000000000000000000",
		EventID: "01J00000000000000000000001", SessionID: "01J00000000000000000000000",
		Seq: 1, Time: time.Unix(1, 0).UTC(), Kind: protocol.EventSessionCreated,
		Actor:               &protocol.ActorRef{ID: "01J00000000000000000000002", Kind: protocol.ActorSystem},
		RuntimeGenerationID: "01J00000000000000000000003", TransactionID: "01J00000000000000000000004",
		Payload: json.RawMessage(`{"workspace_id":"w","canonical_path":"/w","title":"New session","mode":"ask","provider_id":"p","model_id":"m"}`),
	}
}

func TestEventEnvelopeRequiresV2JournalIdentityAndPayloadVersion(t *testing.T) {
	t.Parallel()
	event := validEnvelope()
	if err := event.ValidateEnvelope(); err != nil {
		t.Fatal(err)
	}

	invalid := []protocol.EventEnvelope{event, event, event, event, event, event}
	invalid[0].SchemaVersion = 1
	invalid[1].PayloadVersion = 0
	invalid[2].SessionID = ""
	invalid[3].JournalID = "other"
	invalid[4].Payload = json.RawMessage(`{`)
	invalid[5].TransactionID = ""
	for i := range invalid {
		if err := invalid[i].ValidateEnvelope(); err == nil {
			t.Fatalf("invalid envelope %d accepted", i)
		}
	}

	control := event
	control.JournalKind = protocol.JournalWorkspaceControl
	control.JournalID = "workspace"
	control.SessionID = ""
	if err := control.ValidateEnvelope(); err != nil {
		t.Fatal(err)
	}
	control.SessionID = "session"
	if err := control.ValidateEnvelope(); err == nil {
		t.Fatal("workspace-control envelope with session ID accepted")
	}
}

func TestProtocolOneOfAndContentAvailabilityValidation(t *testing.T) {
	t.Parallel()
	if err := (protocol.ContentBlock{Kind: protocol.ContentText, Text: "x"}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (protocol.ContentBlock{Kind: protocol.ContentText, Text: "x", JSON: json.RawMessage(`{}`)}).Validate(); err == nil {
		t.Fatal("ambiguous content block accepted")
	}
	if err := (protocol.SubscriptionItem{}).Validate(); err == nil {
		t.Fatal("empty subscription item accepted")
	}
	if err := (protocol.CancelCommandV1{TurnID: "turn", ControlOperationID: "control"}).Validate(); err == nil {
		t.Fatal("ambiguous cancellation accepted")
	}
	if err := (protocol.AuthorizationDecisionConsumedV1{ActivityID: "activity", ControlOperationID: "control"}).Validate(); err == nil {
		t.Fatal("ambiguous authorization consumption accepted")
	}

	available := protocol.EvidenceRecord{Body: protocol.EvidenceRecordBody{
		ID: "e", Kind: "output", WorkspaceID: "w", Availability: protocol.ContentAvailable,
		Blob:      &protocol.BlobRef{Digest: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("a", 64)}},
		MediaType: "text/plain", Size: 1, ProducingActivityID: "a", Actor: protocol.ActorRef{ID: "actor", Kind: protocol.ActorTool},
		Subject: protocol.SubjectRef{Kind: "file", ID: "/x"}, CreatedAt: time.Unix(1, 0).UTC(),
	}, Digest: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("b", 64)}}
	if err := available.Validate(); err != nil {
		t.Fatal(err)
	}
	available.Body.Availability = protocol.ContentMissing
	if err := available.Validate(); err == nil {
		t.Fatal("missing evidence with blob accepted")
	}
	available.Body.Availability = protocol.ContentAvailable
	available.Digest = protocol.Digest{}
	if err := available.Validate(); err == nil {
		t.Fatal("evidence record without digest accepted")
	}
}

func TestProtocolEnumCorrelationAndBoundsValidation(t *testing.T) {
	t.Parallel()
	if err := (protocol.EventCorrelation{JournalKind: protocol.JournalSession, JournalID: "s", SessionID: "s"}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (protocol.EventCorrelation{JournalKind: protocol.JournalWorkspaceControl, JournalID: "w", SessionID: "s"}).Validate(); err == nil {
		t.Fatal("workspace correlation with session accepted")
	}
	if err := (protocol.StateChangedV1{From: string(protocol.TaskPending), To: string(protocol.TaskRunning)}).ValidateTask(); err != nil {
		t.Fatal(err)
	}
	if err := (protocol.StateChangedV1{From: string(protocol.TaskCompleted), To: string(protocol.TaskRunning)}).ValidateTask(); err == nil {
		t.Fatal("terminal task transition accepted")
	}

	command := protocol.Command{ProtocolVersion: protocol.ApplicationProtocolVersion, CommandID: "c", Actor: protocol.ActorRef{ID: "u", Kind: protocol.ActorUser}, IdempotencyKey: "k", RequestDigest: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("a", 64)}, Kind: "start_turn", PayloadVersion: 1, Payload: json.RawMessage(`{"prompt":"x"}`)}
	if err := command.Validate(); err != nil {
		t.Fatal(err)
	}
	command.Payload = json.RawMessage(`{"prompt":"` + strings.Repeat("x", protocol.MaxStringBytes+1) + `"}`)
	if err := command.Validate(); err == nil {
		t.Fatal("oversized application string accepted")
	}
}

func TestDeepCopyBreaksAllMutableAliases(t *testing.T) {
	t.Parallel()
	original := validEnvelope()
	copy := protocol.CloneEventEnvelope(original)
	copy.Payload[2] = 'X'
	copy.Actor.Source = "changed"
	if string(original.Payload) == string(copy.Payload) || original.Actor.Source == copy.Actor.Source {
		t.Fatal("event envelope clone retained mutable aliases")
	}

	block := protocol.ContentBlock{Kind: protocol.ContentToolResult, ToolResult: &protocol.ToolResultBlock{CallID: "c", Status: "ok", JSON: json.RawMessage(`{"x":1}`), EvidenceIDs: []protocol.EvidenceID{"e1"}}}
	cloned := protocol.DeepCopy(block)
	cloned.ToolResult.JSON[2] = 'y'
	cloned.ToolResult.EvidenceIDs[0] = "e2"
	if string(block.ToolResult.JSON) == string(cloned.ToolResult.JSON) || block.ToolResult.EvidenceIDs[0] == cloned.ToolResult.EvidenceIDs[0] {
		t.Fatal("generic deep copy retained nested aliases")
	}
}

func TestValidateBoundsCoversEnvelopeFieldsAndCollections(t *testing.T) {
	t.Parallel()
	event := validEnvelope()
	event.Kind = strings.Repeat("x", protocol.MaxStringBytes+1)
	if err := event.ValidateEnvelope(); err == nil {
		t.Fatal("oversized envelope string accepted")
	}
	if err := protocol.ValidateBounds(make([]string, protocol.MaxCollectionMembers+1)); err == nil {
		t.Fatal("oversized collection accepted")
	}
}

func TestEnvelopeAndCommandEnforceWholeWireSize(t *testing.T) {
	t.Parallel()
	event := validEnvelope()
	event.EventID = protocol.EventID(strings.Repeat("e", protocol.MaxStringBytes))
	event.Kind = strings.Repeat("k", protocol.MaxStringBytes)
	if err := event.ValidateEnvelope(); err == nil {
		t.Fatal("event line larger than 2 MiB accepted")
	}

	command := protocol.Command{ProtocolVersion: protocol.ApplicationProtocolVersion, CommandID: protocol.CommandID(strings.Repeat("c", protocol.MaxStringBytes)), Actor: protocol.ActorRef{ID: "u", Kind: protocol.ActorUser}, IdempotencyKey: strings.Repeat("k", protocol.MaxStringBytes), RequestDigest: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("a", 64)}, Kind: "start_turn", PayloadVersion: 1, Payload: json.RawMessage(`{}`)}
	if err := command.Validate(); err == nil {
		t.Fatal("command larger than 2 MiB accepted")
	}
}

func TestModelEventRequiresOneMatchingPayload(t *testing.T) {
	t.Parallel()
	event := protocol.ModelEvent{Kind: protocol.ModelEventContentDelta, Sequence: 1, Delta: &protocol.ContentDelta{BlockID: "b", Kind: protocol.ContentText, Text: "x"}}
	if err := event.Validate(); err != nil {
		t.Fatal(err)
	}
	event.Error = &protocol.ProviderError{Code: "failed", Message: "failure"}
	if err := event.Validate(); err == nil {
		t.Fatal("ambiguous model event accepted")
	}
}

func TestRecursiveModelAndToolValidatorsFailClosed(t *testing.T) {
	t.Parallel()

	descriptor := validModelDescriptor()
	if err := descriptor.Validate(); err != nil {
		t.Fatalf("valid model descriptor rejected: %v", err)
	}
	invalidDescriptor := descriptor
	invalidDescriptor.Capabilities = append([]protocol.CapabilityFact(nil), descriptor.Capabilities...)
	invalidDescriptor.Capabilities[0].Capability = "future_unregistered_capability"
	if err := invalidDescriptor.Validate(); err == nil {
		t.Fatal("unknown model capability accepted")
	}
	invalidDescriptor = descriptor
	invalidDescriptor.ContextWindow.Provenance = ""
	if err := invalidDescriptor.Validate(); err == nil {
		t.Fatal("known context-window fact without provenance accepted")
	}
	invalidDescriptor = descriptor
	invalidDescriptor.Pricing = append([]protocol.PricingFact(nil), descriptor.Pricing...)
	invalidDescriptor.Pricing[0].PerMillionDecimal = "1e3"
	if err := invalidDescriptor.Validate(); err == nil {
		t.Fatal("non-canonical pricing decimal accepted")
	}

	body := validToolDescriptorBody()
	if err := body.Validate(); err != nil {
		t.Fatalf("valid tool descriptor rejected: %v", err)
	}
	invalidBodies := []protocol.ToolDescriptorBody{body, body, body, body}
	invalidBodies[0].InputSchema = json.RawMessage(`[]`)
	invalidBodies[1].ExecutionLoci = []string{"builtin", "builtin"}
	invalidBodies[2].ClassificationSource = ""
	invalidBodies[3].Retry = ""
	for index, invalid := range invalidBodies {
		if err := invalid.Validate(); err == nil {
			t.Fatalf("invalid tool descriptor %d accepted", index)
		}
	}

	exposure := protocol.ToolExposure{
		CatalogRevision: "tools-r1",
		Tools:           []protocol.ExposedTool{{Alias: "read", Identity: body.Identity, Description: body.Description, InputSchema: protocol.CloneRawMessage(body.InputSchema)}},
		Aliases:         []protocol.ToolAliasBinding{{Alias: "read", Identity: body.Identity, SourceRevision: body.SourceRevision, DescriptorDigest: protocolDigest('a')}},
	}
	if err := exposure.Validate(); err != nil {
		t.Fatalf("valid tool exposure rejected: %v", err)
	}
	exposure.Tools[0].InputSchema = json.RawMessage(`[]`)
	if err := exposure.Validate(); err == nil {
		t.Fatal("non-object exposed-tool schema accepted")
	}
	exposure.Tools[0].InputSchema = json.RawMessage(`{"type":"object"}`)
	exposure.Aliases[0].Identity.Name = "other"
	if err := exposure.Validate(); err == nil {
		t.Fatal("tool exposure alias identity mismatch accepted")
	}

	source := protocol.ContentSource{ID: "instructions", Kind: "system", Scope: "workspace", Provenance: "file", Digest: protocolDigest('b'), Content: []protocol.ContentBlock{{Kind: protocol.ContentText, Text: "Be precise."}}}
	if err := source.Validate(); err != nil {
		t.Fatalf("valid content source rejected: %v", err)
	}
	source.Content[0] = protocol.ContentBlock{Kind: protocol.ContentText, JSON: json.RawMessage(`{}`)}
	if err := source.Validate(); err == nil {
		t.Fatal("invalid nested content block accepted")
	}
	if err := (protocol.ExcludedContentSource{ID: "large", Reason: "", Digest: protocolDigest('c')}).Validate(); err == nil {
		t.Fatal("excluded content source without reason accepted")
	}
}

func TestAuthorizationDecisionBindsLifetimePolicyPlanScopeAndConstraints(t *testing.T) {
	t.Parallel()
	decision := validAuthorizationDecision()
	if err := decision.Validate(); err != nil {
		t.Fatalf("valid authorization decision rejected: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*protocol.AuthorizationDecision)
	}{
		{"lifetime", func(d *protocol.AuthorizationDecision) { d.Lifetime = "forever" }},
		{"policy generation", func(d *protocol.AuthorizationDecision) { d.PolicyGeneration = "policy-r2" }},
		{"plan digest", func(d *protocol.AuthorizationDecision) { d.PlanDigest = protocolDigest('b') }},
		{"scope capability", func(d *protocol.AuthorizationDecision) { d.Scope.Capability = "write" }},
		{"scope source", func(d *protocol.AuthorizationDecision) { d.Scope.Source.Name = "edit" }},
		{"scope resources", func(d *protocol.AuthorizationDecision) { d.Scope.Resources[0].CanonicalID = "/workspace/b" }},
		{"constraint JSON", func(d *protocol.AuthorizationDecision) { d.Constraints[0].Value = json.RawMessage(`1e3`) }},
		{"constraint repetition", func(d *protocol.AuthorizationDecision) { d.Scope.Constraints[0].Operator = "prefix" }},
	}
	for _, test := range cases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			candidate := protocol.DeepCopy(decision)
			test.mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatalf("invalid authorization %s accepted", test.name)
			}
		})
	}
}

func TestApplicationEventUsesKindAwareWorkspaceControlCorrelation(t *testing.T) {
	t.Parallel()
	workspaceCorrelation := protocol.EventCorrelation{JournalKind: protocol.JournalWorkspaceControl, JournalID: "workspace", TaskID: "task", TurnID: "turn", ActivityID: "activity"}
	if err := workspaceCorrelation.Validate(); err != nil {
		t.Fatalf("workspace causation lineage rejected: %v", err)
	}

	event := protocol.ApplicationEvent{
		ProtocolVersion: protocol.ApplicationProtocolVersion, StreamEventID: "stream-event", Correlation: workspaceCorrelation,
		Time: time.Unix(1, 0).UTC(), Kind: protocol.EventControlOperationStarted, Classification: "durable", PayloadVersion: 1, Payload: json.RawMessage(`{}`),
	}
	if err := event.Validate(); err == nil {
		t.Fatal("control-operation application event without control-operation ID accepted")
	}
	event.Correlation.ControlOperationID = "control"
	if err := event.Validate(); err != nil {
		t.Fatalf("control-operation application event with optional causation rejected: %v", err)
	}

	event.Kind = protocol.EventRuntimeGenerationActivated
	event.Correlation.ControlOperationID = ""
	if err := event.Validate(); err != nil {
		t.Fatalf("non-operation workspace event without control-operation ID rejected: %v", err)
	}

	event.Kind = protocol.EventControlOperationStarted
	event.Correlation = protocol.EventCorrelation{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session"}
	if err := event.Validate(); err == nil {
		t.Fatal("session-labeled control-operation application event accepted")
	}

	event.Correlation = protocol.EventCorrelation{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", ControlOperationID: "control"}
	if err := event.Validate(); err == nil {
		t.Fatal("session application event with control-operation ID accepted")
	}
}
