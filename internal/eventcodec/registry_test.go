package eventcodec_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/eventcodec"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type descriptorAliasPayload struct {
	createdAt time.Time
	Mutable   json.RawMessage `json:"mutable"`
}

func envelope(kind string, payload any) json.RawMessage {
	raw, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	envelope, err := json.Marshal(protocol.EventEnvelope{
		SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1,
		JournalKind: protocol.JournalSession, JournalID: "session", EventID: "event", SessionID: "session",
		Seq: 1, Time: time.Unix(1, 0).UTC(), Kind: kind, Actor: &protocol.ActorRef{ID: "system", Kind: protocol.ActorSystem},
		RuntimeGenerationID: "generation", TransactionID: "transaction", Payload: raw,
	})
	if err != nil {
		panic(err)
	}
	return envelope
}

func envelopeFor(t *testing.T, event protocol.EventEnvelope, payload any) json.RawMessage {
	t.Helper()
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	event.SchemaVersion = protocol.EnvelopeVersion
	event.PayloadVersion = 1
	event.Payload = rawPayload
	if event.EventID == "" {
		event.EventID = "event"
	}
	if event.Seq == 0 {
		event.Seq = 1
	}
	if event.Time.IsZero() {
		event.Time = time.Unix(1, 0).UTC()
	}
	if event.TransactionID == "" {
		event.TransactionID = "transaction"
	}
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func testDigest(fill byte) protocol.Digest {
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat(string(fill), 64)}
}

func registryModelDescriptor() protocol.ModelDescriptor {
	return protocol.ModelDescriptor{
		ProviderID: "provider", ModelID: "model", AdapterKind: "openai_compatible", DisplayName: "Model",
		ContextWindow:   protocol.ValueInt64{State: protocol.ValueKnown, Value: 8192, Provenance: "catalog"},
		MaximumOutput:   protocol.ValueInt64{State: protocol.ValueKnown, Value: 2048, Provenance: "catalog"},
		Capabilities:    []protocol.CapabilityFact{{Capability: protocol.CapabilityTextInput, State: protocol.CapabilitySupported, Provenance: "adapter", RuntimeGenerationID: "generation"}},
		UsageCategories: []string{"input"}, Pricing: []protocol.PricingFact{{Category: "input", PerMillionDecimal: "1.25", Currency: "USD", Provenance: "catalog"}},
		CredentialBindingRef: "env:OPENAI_API_KEY", SourceRevision: "r1", RuntimeGenerationID: "generation",
	}
}

func registryToolDescriptorBody() protocol.ToolDescriptorBody {
	return protocol.ToolDescriptorBody{
		Identity: protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "read"}, SourceRevision: "r1", DisplayName: "Read", Description: "Read a file",
		InputSchema: json.RawMessage(`{"type":"object"}`), Effect: "observation", Mutation: "read_only", ExecutionLoci: []string{"builtin"},
		ClassificationSource: "trusted_adapter", Idempotency: "idempotent", Retry: "safe_before_dispatch",
	}
}

func registryActionPlan(t *testing.T) protocol.ActionPlan {
	t.Helper()
	body := protocol.ActionPlanBody{
		CallID: "call", Tool: protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "read"}, SourceRevision: "r1", DescriptorDigest: testDigest('a'),
		Action: "read", Purpose: "inspect", Resources: []protocol.ResourceTarget{{Kind: "file", CanonicalID: "/x"}}, ExecutionLocus: "builtin", Effect: "observation",
		Boundary: "workspace", Reversibility: "not_applicable", VerificationCoverage: "full", RequestedProfile: "restricted", EffectiveProfile: "restricted", RuntimeGenerationID: "generation",
	}
	digest, err := canonicaljson.Digest(body)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.ActionPlan{Body: body, Digest: digest}
}

func registryAuthorizationDecision() protocol.AuthorizationDecidedV1 {
	digest := testDigest('a')
	source := protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "read"}
	resource := protocol.ResourceTarget{Kind: "file", CanonicalID: "/workspace/a"}
	request := protocol.AuthorizationRequest{
		RequestID: "request", Principal: protocol.ActorRef{ID: "user", Kind: protocol.ActorUser}, Actor: protocol.ActorRef{ID: "agent", Kind: protocol.ActorAgent},
		SessionID: "session", ActivityID: "activity", CallID: "call", QueueID: "queue", Source: source, SourceRevision: "r1", DescriptorDigest: digest,
		Action: "read", Resources: []protocol.ResourceTarget{resource}, ExecutionLocus: "builtin", RequestedProfile: "restricted", EffectiveProfile: "restricted",
		Effect: "observation", Boundary: "workspace", Reversibility: "not_applicable", VerificationCoverage: "full", RuntimeGenerationID: "generation", PolicyGeneration: "policy-r1",
		PolicyProvenance: []protocol.PolicyProvenance{{Source: "platform", Revision: "r1", Generation: "policy-r1"}}, PlanDigest: digest, RequestDigest: digest, DispatchDigest: digest,
	}
	return protocol.AuthorizationDecidedV1{Decision: protocol.AuthorizationDecision{
		Request: request, Action: "allow", Scope: protocol.CanonicalAuthorizationScope{Capability: "read", Source: source, Resources: []protocol.ResourceTarget{resource}, Constraints: []protocol.AuthorizationConstraint{}},
		Constraints: []protocol.AuthorizationConstraint{}, Lifetime: protocol.AuthorizationLifetimeOnce, PolicySource: "platform", PolicyGeneration: "policy-r1", Reason: "allowed",
		DecidedAt: time.Unix(1, 0).UTC(), PlanDigest: digest, DecisionNonce: "nonce",
	}}
}

func registryDecisionEnvelope(t *testing.T, decision protocol.AuthorizationDecidedV1) protocol.EventEnvelope {
	t.Helper()
	payload, err := json.Marshal(decision)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.EventEnvelope{
		SchemaVersion: protocol.EnvelopeVersion, PayloadVersion: 1, JournalKind: protocol.JournalSession, JournalID: "session", EventID: "decision-event", SessionID: "session",
		Seq: 1, Time: time.Unix(1, 0).UTC(), Kind: protocol.EventAuthorizationDecided, ActivityID: "activity", RuntimeGenerationID: "generation", TransactionID: "transaction", Payload: payload,
	}
}

func TestFoundationRegistryDecodesStrictlyAndPreservesUnknownRaw(t *testing.T) {
	t.Parallel()
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	raw := envelope(protocol.EventSessionCreated, protocol.SessionCreatedV1{WorkspaceID: "w", CanonicalPath: "/w", Title: "x", Mode: "ask", ProviderID: "p", ModelID: "m"})
	record, err := registry.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := record.Decoded.(*protocol.SessionCreatedV1); !ok {
		t.Fatalf("decoded type=%T", record.Decoded)
	}
	if err := registry.Validate(record); err != nil {
		t.Fatal(err)
	}

	unknown := envelope("future.event", map[string]any{"x": 1})
	record, err = registry.Decode(unknown)
	var kindErr *eventcodec.UnknownKindError
	if !errors.As(err, &kindErr) || string(record.RawEnvelope) != string(unknown) {
		t.Fatalf("unknown err=%v raw=%s", err, record.RawEnvelope)
	}

	var generic map[string]any
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatal(err)
	}
	generic["payload_version"] = 2
	unsupported, _ := json.Marshal(generic)
	record, err = registry.Decode(unsupported)
	var versionErr *eventcodec.UnsupportedPayloadVersionError
	if !errors.As(err, &versionErr) || string(record.RawEnvelope) != string(unsupported) {
		t.Fatalf("version err=%v raw=%s", err, record.RawEnvelope)
	}

	bad := envelope(protocol.EventSessionCreated, map[string]any{"workspace_id": "w", "canonical_path": "/w", "title": "x", "mode": "ask", "provider_id": "p", "model_id": "m", "extra": true})
	if _, err := registry.Decode(bad); err == nil {
		t.Fatal("known payload with unknown field accepted")
	}
}

func TestFoundationRegistryRejectsDuplicateDescriptorsAndInvalidSemantics(t *testing.T) {
	t.Parallel()
	descriptor := eventcodec.Descriptor{Kind: "x", Version: 1, New: func() any { return new(struct{}) }}
	if _, err := eventcodec.New([]eventcodec.Descriptor{descriptor, descriptor}); err == nil {
		t.Fatal("duplicate descriptor accepted")
	}
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	record, err := registry.Decode(envelope(protocol.EventTaskCreated, protocol.TaskCreatedV1{Goal: "", OutcomeContractID: "contract", ContractVersion: 1}))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Validate(record); err == nil {
		t.Fatal("empty task goal accepted")
	}
}

func TestFoundationRegistryRecomputesBodyDigestsAndEnforcesJournalFamilies(t *testing.T) {
	t.Parallel()
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	body := protocol.ActionPlanBody{CallID: "call", Tool: protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "read"}, SourceRevision: "r1", DescriptorDigest: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("a", 64)}, Action: "read", Purpose: "inspect", Resources: []protocol.ResourceTarget{{Kind: "file", CanonicalID: "/x"}}, ExecutionLocus: "local", Effect: "read", Boundary: "workspace", Reversibility: "not_applicable", VerificationCoverage: "full", RequestedProfile: "restricted", EffectiveProfile: "restricted", RuntimeGenerationID: "generation"}
	digest, err := canonicaljson.Digest(body)
	if err != nil {
		t.Fatal(err)
	}
	record, err := registry.Decode(envelope(protocol.EventExecutionPlanDeclared, protocol.ExecutionPlanDeclaredV1{Plan: protocol.ActionPlan{Body: body, Digest: digest}}))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Validate(record); err != nil {
		t.Fatal(err)
	}
	decoded := record.Decoded.(*protocol.ExecutionPlanDeclaredV1)
	decoded.Plan.Digest.Value = strings.Repeat("b", 64)
	if err := registry.Validate(record); err == nil {
		t.Fatal("mismatched action plan digest accepted")
	}

	controlRaw := envelope(protocol.EventRuntimeGenerationActivated, protocol.RuntimeGenerationActivatedV1{})
	if _, err := registry.Decode(controlRaw); err == nil {
		t.Fatal("workspace-control event accepted in session journal")
	}
}

func TestFoundationRegistryDeepCopiesInputAndDecodedPayload(t *testing.T) {
	t.Parallel()
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	raw := envelope(protocol.EventAssistantMessage, protocol.AssistantMessageV1{Blocks: []protocol.ContentBlock{{Kind: protocol.ContentJSON, JSON: json.RawMessage(`{"x":1}`)}}})
	record, err := registry.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	raw[0] = 'x'
	message := record.Decoded.(*protocol.AssistantMessageV1)
	message.Blocks[0].JSON[2] = 'y'
	second, err := registry.Decode(record.RawEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	if second.Decoded.(*protocol.AssistantMessageV1).Blocks[0].JSON[2] == 'y' {
		t.Fatal("registry retained decoded mutable aliases")
	}
}

func TestFoundationDescriptorsCoverEveryKindWithMetadata(t *testing.T) {
	t.Parallel()
	descriptors := eventcodec.FoundationDescriptors()
	if len(descriptors) != 61 {
		t.Fatalf("descriptor count=%d want=61", len(descriptors))
	}
	for _, descriptor := range descriptors {
		if descriptor.Kind == "" || descriptor.Version != 1 || descriptor.New == nil || descriptor.ValidateStructural == nil || descriptor.ValidateSemantic == nil || descriptor.RedactionClass == "" || len(descriptor.ProjectionDomains) == 0 {
			t.Fatalf("incomplete descriptor: %#v", descriptor)
		}
	}
}

func TestFoundationRegistryRejectsMissingRequiredFieldsAndWrongTerminalKind(t *testing.T) {
	t.Parallel()
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	missing := envelope(protocol.EventSessionCreated, map[string]any{"workspace_id": "w", "canonical_path": "/w", "title": "x", "mode": "ask", "provider_id": "p"})
	if _, err := registry.Decode(missing); err == nil {
		t.Fatal("payload missing required model_id accepted")
	}

	record, err := registry.Decode(envelope(protocol.EventTurnFailed, protocol.TurnTerminalV1{Status: "completed", Reason: "wrong terminal"}))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Validate(record); err == nil {
		t.Fatal("completed status accepted for turn.failed")
	}

	record, err = registry.Decode(envelope(protocol.EventTurnStateChanged, protocol.StateChangedV1{From: string(protocol.TurnCompleted), To: string(protocol.TurnRunning)}))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Validate(record); err == nil {
		t.Fatal("turn transition out of terminal state accepted")
	}
}

func TestRegistryValidateRejectsDecodedPayloadThatDiffersFromEnvelope(t *testing.T) {
	t.Parallel()
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	record, err := registry.Decode(envelope(protocol.EventSessionTitleChanged, protocol.SessionTitleChangedV1{Title: "before"}))
	if err != nil {
		t.Fatal(err)
	}
	record.Decoded.(*protocol.SessionTitleChangedV1).Title = "after"
	if err := registry.Validate(record); err == nil {
		t.Fatal("decoded payload/envelope mismatch accepted")
	}
}

func TestRegistryValidateAcceptsExplicitZeroForOptionalField(t *testing.T) {
	t.Parallel()
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	record, err := registry.Decode(envelope(protocol.EventSessionLifecycleChanged, protocol.StateChangedV1{From: "active", To: "archived", Reason: ""}))
	if err != nil {
		t.Fatal(err)
	}
	var rawEnvelope map[string]json.RawMessage
	if err := json.Unmarshal(record.RawEnvelope, &rawEnvelope); err != nil {
		t.Fatal(err)
	}
	rawEnvelope["payload"] = json.RawMessage(`{"from":"active","to":"archived","reason":""}`)
	record.RawEnvelope, _ = json.Marshal(rawEnvelope)
	record.Envelope.Payload = json.RawMessage(`{"from":"active","to":"archived","reason":""}`)
	if err := registry.Validate(record); err != nil {
		t.Fatalf("explicit optional zero rejected: %v", err)
	}
}

func TestValidateAuthorizationConsumptionMatchesDecisionAndStartBindings(t *testing.T) {
	t.Parallel()
	digest := testDigest('a')
	decision := registryAuthorizationDecision()
	decisionDigest, err := canonicaljson.Digest(decision.Decision)
	if err != nil {
		t.Fatal(err)
	}
	consumed := protocol.AuthorizationDecisionConsumedV1{DecisionNonce: "nonce", DecisionEventID: "decision-event", DecisionDigest: decisionDigest, RequestID: "request", ActivityID: "activity", CallID: "call", PlanDigest: digest, RequestDigest: digest, DispatchDigest: digest, RuntimeGenerationID: "generation"}
	started := protocol.ActivityStartedV1{DecisionNonce: "nonce", DecisionEventID: "decision-event", ActivityID: "activity", CallID: "call", PlanDigest: digest, RequestDigest: digest, DispatchDigest: digest, RuntimeGenerationID: "generation", DispatchState: "dispatched"}
	envelope := registryDecisionEnvelope(t, decision)
	if err := eventcodec.ValidateAuthorizationConsumption(consumed, envelope, decision, started); err != nil {
		t.Fatal(err)
	}
	started.CallID = "other"
	if err := eventcodec.ValidateAuthorizationConsumption(consumed, envelope, decision, started); err == nil {
		t.Fatal("mismatched start binding accepted")
	}
	started.CallID = "call"

	wrongKind := envelope
	wrongKind.Kind = protocol.EventAuthorizationRequested
	if err := eventcodec.ValidateAuthorizationConsumption(consumed, wrongKind, decision, started); err == nil {
		t.Fatal("non-authorization.decided envelope accepted as committed decision")
	}

	differentDecision := protocol.DeepCopy(decision)
	differentDecision.Decision.Reason = "different committed reason"
	differentPayload, err := json.Marshal(differentDecision)
	if err != nil {
		t.Fatal(err)
	}
	mismatchedPayload := envelope
	mismatchedPayload.Payload = differentPayload
	if err := eventcodec.ValidateAuthorizationConsumption(consumed, mismatchedPayload, decision, started); err == nil {
		t.Fatal("supplied decision that differs from committed envelope payload accepted")
	}

	wrongJournal := envelope
	wrongJournal.JournalID = "other"
	wrongJournal.SessionID = "other"
	if err := eventcodec.ValidateAuthorizationConsumption(consumed, wrongJournal, decision, started); err == nil {
		t.Fatal("decision journal/correlation mismatch accepted")
	}

	wrongVersion := envelope
	wrongVersion.PayloadVersion = 2
	if err := eventcodec.ValidateAuthorizationConsumption(consumed, wrongVersion, decision, started); err == nil {
		t.Fatal("unsupported decision payload version accepted")
	}
}

func TestRegistryRejectsTypedNilDescriptorFactoryAndBreaksCustomAliases(t *testing.T) {
	t.Parallel()
	var typedNil *descriptorAliasPayload
	if _, err := eventcodec.New([]eventcodec.Descriptor{{Kind: "typed.nil", Version: 1, New: func() any { return typedNil }}}); err == nil {
		t.Fatal("descriptor factory returning a typed nil pointer accepted")
	}

	prototype := &descriptorAliasPayload{createdAt: time.Unix(1, 0).UTC()}
	registry, err := eventcodec.New([]eventcodec.Descriptor{{Kind: "alias.payload", Version: 1, New: func() any { return prototype }}})
	if err != nil {
		t.Fatal(err)
	}
	record, err := registry.Decode(envelope("alias.payload", map[string]any{"mutable": map[string]any{"x": 1}}))
	if err != nil {
		t.Fatal(err)
	}
	decoded := record.Decoded.(*descriptorAliasPayload)
	decoded.Mutable[2] = 'y'
	if prototype.Mutable[2] == 'y' {
		t.Fatal("decoded custom descriptor retained mutable alias through an unexported immutable field")
	}
	if !decoded.createdAt.Equal(prototype.createdAt) {
		t.Fatal("deep copy did not preserve an unexported immutable field")
	}
}

func TestFoundationRegistryRequiresExactEnvelopePayloadIdentity(t *testing.T) {
	t.Parallel()
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}

	digest := testDigest('a')
	request := protocol.AuthorizationRequest{
		RequestID: "request", Principal: protocol.ActorRef{ID: "principal", Kind: protocol.ActorUser}, Actor: protocol.ActorRef{ID: "actor", Kind: protocol.ActorAgent},
		SessionID: "session", TaskID: "task", TurnID: "turn", ActivityID: "activity", ParentActivityID: "parent", CallID: "call", QueueID: "queue",
		Source: protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "read"}, SourceRevision: "r1", DescriptorDigest: digest,
		Action: "read", Resources: []protocol.ResourceTarget{{Kind: "file", CanonicalID: "/x"}}, ExecutionLocus: "local", RequestedProfile: "restricted", EffectiveProfile: "restricted",
		Effect: "observation", Boundary: "workspace", Reversibility: "not_applicable", VerificationCoverage: "full", RuntimeGenerationID: "generation", PolicyGeneration: "policy",
		PolicyProvenance: []protocol.PolicyProvenance{{Source: "platform", Revision: "r1", Generation: "policy"}}, PlanDigest: digest, RequestDigest: digest, DispatchDigest: digest,
	}
	base := protocol.EventEnvelope{
		JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", Kind: protocol.EventAuthorizationRequested,
		TaskID: "task", TurnID: "turn", ActivityID: "activity", ParentActivityID: "parent", RuntimeGenerationID: "generation",
	}
	record, err := registry.Decode(envelopeFor(t, base, protocol.AuthorizationRequestedV1{Request: request}))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Validate(record); err != nil {
		t.Fatalf("valid identity rejected: %v", err)
	}

	mutations := []struct {
		name   string
		mutate func(*protocol.EventEnvelope)
	}{
		{"task omitted", func(e *protocol.EventEnvelope) { e.TaskID = "" }},
		{"task mismatch", func(e *protocol.EventEnvelope) { e.TaskID = "other" }},
		{"turn omitted", func(e *protocol.EventEnvelope) { e.TurnID = "" }},
		{"turn mismatch", func(e *protocol.EventEnvelope) { e.TurnID = "other" }},
		{"activity omitted", func(e *protocol.EventEnvelope) { e.ActivityID = "" }},
		{"activity mismatch", func(e *protocol.EventEnvelope) { e.ActivityID = "other" }},
		{"parent activity omitted", func(e *protocol.EventEnvelope) { e.ParentActivityID = "" }},
		{"parent activity mismatch", func(e *protocol.EventEnvelope) { e.ParentActivityID = "other" }},
		{"runtime omitted", func(e *protocol.EventEnvelope) { e.RuntimeGenerationID = "" }},
		{"runtime mismatch", func(e *protocol.EventEnvelope) { e.RuntimeGenerationID = "other" }},
	}
	for _, mutation := range mutations {
		mutation := mutation
		t.Run(mutation.name, func(t *testing.T) {
			candidate := protocol.CloneEventRecord(record)
			mutation.mutate(&candidate.Envelope)
			if err := registry.Validate(candidate); err == nil {
				t.Fatalf("%s accepted", mutation.name)
			}
		})
	}

	started := protocol.ActivityStartedV1{DecisionNonce: "nonce", DecisionEventID: "decision", ActivityID: "activity", CallID: "call", PlanDigest: digest, RequestDigest: digest, DispatchDigest: digest, RuntimeGenerationID: "generation", DispatchState: "dispatched"}
	startedRecord, err := registry.Decode(envelopeFor(t, protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", Kind: protocol.EventActivityStarted, RuntimeGenerationID: "generation"}, started))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Validate(startedRecord); err == nil {
		t.Fatal("activity.started without matching envelope activity ID accepted")
	}
	startedRecord.Envelope.ActivityID = "activity"
	if err := registry.Validate(startedRecord); err != nil {
		t.Fatalf("valid activity.started identity rejected: %v", err)
	}

	evidenceBody := protocol.EvidenceRecordBody{ID: "evidence", Kind: "output", WorkspaceID: "workspace", SessionID: "session", Availability: protocol.ContentMissing, MediaType: "text/plain", ProducingActivityID: "activity", Actor: protocol.ActorRef{ID: "tool", Kind: protocol.ActorTool}, Subject: protocol.SubjectRef{Kind: "file", ID: "/x"}, CreatedAt: time.Unix(1, 0).UTC()}
	evidenceDigest, err := canonicaljson.Digest(evidenceBody)
	if err != nil {
		t.Fatal(err)
	}
	evidenceRecord, err := registry.Decode(envelopeFor(t, protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", Kind: protocol.EventEvidenceRecorded}, protocol.EvidenceRecordedV1{Record: protocol.EvidenceRecord{Body: evidenceBody, Digest: evidenceDigest}}))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Validate(evidenceRecord); err == nil {
		t.Fatal("evidence.recorded without matching envelope activity ID accepted")
	}
	evidenceRecord.Envelope.ActivityID = "activity"
	if err := registry.Validate(evidenceRecord); err != nil {
		t.Fatalf("valid evidence.recorded identity rejected: %v", err)
	}

	manifestBody := protocol.RuntimeGenerationBody{ProviderCatalogRevision: "providers", Models: []protocol.ModelDescriptor{}, ToolCatalogRevision: "tools", Tools: []protocol.ToolDescriptor{}, InstructionRevision: "instructions", PolicyGeneration: "policy", ExecutionProfiles: []string{"restricted"}, Limits: protocol.RuntimeLimits{MaxToolCalls: 1, ShellTimeoutNanos: 1, ApplicationQueueCapacity: 1}}
	manifestDigest, err := canonicaljson.Digest(manifestBody)
	if err != nil {
		t.Fatal(err)
	}
	manifest := protocol.RuntimeGenerationManifest{ID: "generation", Body: manifestBody, Digest: manifestDigest}
	runtimeRecord, err := registry.Decode(envelopeFor(t, protocol.EventEnvelope{JournalKind: protocol.JournalWorkspaceControl, JournalID: "workspace", Kind: protocol.EventRuntimeGenerationActivated}, protocol.RuntimeGenerationActivatedV1{Manifest: manifest}))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Validate(runtimeRecord); err == nil {
		t.Fatal("runtime_generation.activated without matching envelope runtime generation accepted")
	}
	runtimeRecord.Envelope.RuntimeGenerationID = "generation"
	if err := registry.Validate(runtimeRecord); err != nil {
		t.Fatalf("valid runtime_generation.activated identity rejected: %v", err)
	}

	marker := protocol.TransactionCommittedV1{TransactionID: "transaction", FirstSeq: 1, LastSeq: 1, EventCount: 1, Digest: digest}
	markerRecord, err := registry.Decode(envelopeFor(t, protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", Kind: protocol.EventTransactionCommitted, TransactionID: "other"}, marker))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Validate(markerRecord); err == nil {
		t.Fatal("transaction.committed with mismatched envelope transaction ID accepted")
	}
}

func TestFoundationRegistryKeepsGrantRevocationSessionOnly(t *testing.T) {
	t.Parallel()
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	raw := envelopeFor(t, protocol.EventEnvelope{JournalKind: protocol.JournalWorkspaceControl, JournalID: "workspace", Kind: protocol.EventAuthorizationGrantRevoked}, protocol.AuthorizationGrantRevokedV1{GrantID: "grant", Reason: "revoked", RevocationEpoch: 1})
	if _, err := registry.Decode(raw); err == nil {
		t.Fatal("authorization.grant_revoked accepted in workspace-control journal")
	}
}

func TestFoundationRegistryBindsDiagnosticJournalToEnvelope(t *testing.T) {
	t.Parallel()
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{protocol.EventMigrationDiagnostic, protocol.EventRecoveryDiagnostic} {
		t.Run(fmt.Sprintf("%s mismatch", kind), func(t *testing.T) {
			payload := protocol.DiagnosticV1{Diagnostic: protocol.Diagnostic{Code: "corruption", Message: "bad tail", Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: "other"}}}
			record, err := registry.Decode(envelopeFor(t, protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", Kind: kind}, payload))
			if err != nil {
				t.Fatal(err)
			}
			if err := registry.Validate(record); err == nil {
				t.Fatal("diagnostic journal mismatch accepted")
			}
		})
	}
}

func TestFoundationRegistryValidatesNestedBodiesBeforeOuterDigests(t *testing.T) {
	t.Parallel()
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}

	t.Run("negotiated provider descriptor", func(t *testing.T) {
		descriptor := registryModelDescriptor()
		descriptor.Capabilities[0].Capability = "future_unregistered_capability"
		plan := protocol.NegotiatedProviderPlan{Body: protocol.NegotiatedProviderPlanBody{Descriptor: descriptor, Requirements: []protocol.CapabilityRequirement{{Capability: protocol.CapabilityTextInput, Level: protocol.CapabilityRequired}}, ToolExposureRevision: "tools-r1", Warnings: []string{}}, Digest: testDigest('f')}
		record, err := registry.Decode(envelopeFor(t, protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", Kind: protocol.EventProviderCapabilityDecided, RuntimeGenerationID: "generation"}, protocol.ProviderCapabilityDecidedV1{Plan: plan, Status: "rejected", Missing: []string{}}))
		if err != nil {
			t.Fatal(err)
		}
		err = registry.Validate(record)
		if err == nil || !strings.Contains(err.Error(), "invalid capability") {
			t.Fatalf("nested model descriptor was not rejected before plan digest: %v", err)
		}
	})

	t.Run("context source digest", func(t *testing.T) {
		source := protocol.ContentSource{ID: "instructions", Kind: "system", Scope: "workspace", Provenance: "file", Digest: testDigest('e'), Content: []protocol.ContentBlock{{Kind: protocol.ContentText, Text: "Be precise."}}}
		body := protocol.ContextPlanBody{Sources: []protocol.ContentSource{source}, Excluded: []protocol.ExcludedContentSource{}, EstimatedInputTokens: protocol.ValueInt64{State: protocol.ValueKnown, Value: 4, Provenance: "estimator"}, OutputReserve: 16, ContextWindow: protocol.ValueInt64{State: protocol.ValueKnown, Value: 8192, Provenance: "catalog"}, CompactionRevision: "compact-r1", ToolExposureRevision: "tools-r1"}
		plan := protocol.ContextPlan{Body: body, Digest: testDigest('f')}
		record, err := registry.Decode(envelopeFor(t, protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", Kind: protocol.EventContextPlanRecorded}, protocol.ContextPlanRecordedV1{Plan: plan}))
		if err != nil {
			t.Fatal(err)
		}
		err = registry.Validate(record)
		if err == nil || !strings.Contains(err.Error(), "content source") {
			t.Fatalf("content source digest was not rejected before context-plan digest: %v", err)
		}
	})

	t.Run("runtime tool descriptor", func(t *testing.T) {
		toolBody := registryToolDescriptorBody()
		toolBody.Retry = ""
		toolDigest, err := canonicaljson.Digest(toolBody)
		if err != nil {
			t.Fatal(err)
		}
		body := protocol.RuntimeGenerationBody{ProviderCatalogRevision: "providers", Models: []protocol.ModelDescriptor{registryModelDescriptor()}, ToolCatalogRevision: "tools", Tools: []protocol.ToolDescriptor{{Body: toolBody, DescriptorDigest: toolDigest}}, InstructionRevision: "instructions", PolicyGeneration: "policy", ExecutionProfiles: []string{"restricted"}, Limits: protocol.RuntimeLimits{MaxToolCalls: 1, ShellTimeoutNanos: 1, ApplicationQueueCapacity: 1}}
		manifest := protocol.RuntimeGenerationManifest{ID: "generation", Body: body, Digest: testDigest('f')}
		record, err := registry.Decode(envelopeFor(t, protocol.EventEnvelope{JournalKind: protocol.JournalWorkspaceControl, JournalID: "workspace", Kind: protocol.EventRuntimeGenerationActivated, RuntimeGenerationID: "generation"}, protocol.RuntimeGenerationActivatedV1{Manifest: manifest}))
		if err != nil {
			t.Fatal(err)
		}
		err = registry.Validate(record)
		if err == nil || !strings.Contains(err.Error(), "tool descriptor") {
			t.Fatalf("nested tool descriptor was not rejected before manifest digest: %v", err)
		}
	})
}

func TestFoundationRegistryBindsEveryNestedRepeatedIdentity(t *testing.T) {
	t.Parallel()
	registry, err := eventcodec.New(eventcodec.FoundationDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	digest := testDigest('a')

	assertIdentity := func(t *testing.T, event protocol.EventEnvelope, payload any, fields ...string) {
		t.Helper()
		record, err := registry.Decode(envelopeFor(t, event, payload))
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.Validate(record); err != nil {
			t.Fatalf("valid identity rejected: %v", err)
		}
		for _, field := range fields {
			field := field
			for _, mutation := range []string{"omitted", "mismatch"} {
				mutation := mutation
				t.Run(field+" "+mutation, func(t *testing.T) {
					candidate := protocol.CloneEventRecord(record)
					value := ""
					if mutation == "mismatch" {
						value = "other"
					}
					switch field {
					case "task":
						candidate.Envelope.TaskID = protocol.TaskID(value)
					case "turn":
						candidate.Envelope.TurnID = protocol.TurnID(value)
					case "activity":
						candidate.Envelope.ActivityID = protocol.ActivityID(value)
					case "runtime":
						candidate.Envelope.RuntimeGenerationID = protocol.RuntimeGenerationID(value)
					}
					if err := registry.Validate(candidate); err == nil {
						t.Fatalf("%s identity %s accepted", field, mutation)
					}
				})
			}
		}
	}

	plan := registryActionPlan(t)
	actionCases := []struct {
		name    string
		kind    string
		payload any
		event   protocol.EventEnvelope
	}{
		{"file change plan", protocol.EventFileChangePlanned, protocol.FileChangePlannedV1{Plan: plan}, protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", RuntimeGenerationID: "generation"}},
		{"activity plan", protocol.EventActivityPlanned, protocol.ActivityPlannedV1{Kind: "tool", Purpose: "inspect", PurposeActor: protocol.ActorRef{ID: "agent", Kind: protocol.ActorAgent}, Source: "agent", Plan: &plan, InputEvidenceIDs: []protocol.EvidenceID{}, RequestedProfile: "restricted", EffectiveProfile: "restricted"}, protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", RuntimeGenerationID: "generation"}},
		{"execution plan", protocol.EventExecutionPlanDeclared, protocol.ExecutionPlanDeclaredV1{Plan: plan}, protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", RuntimeGenerationID: "generation"}},
		{"control plan", protocol.EventControlOperationPlanned, protocol.ControlOperationPlannedV1{ControlOperationID: "control", Kind: "reload", Purpose: "reload", Plan: plan}, protocol.EventEnvelope{JournalKind: protocol.JournalWorkspaceControl, JournalID: "workspace", RuntimeGenerationID: "generation"}},
	}
	for _, test := range actionCases {
		t.Run(test.name, func(t *testing.T) {
			test.event.Kind = test.kind
			assertIdentity(t, test.event, test.payload, "runtime")
		})
	}

	descriptor := registryModelDescriptor()
	providerBody := protocol.NegotiatedProviderPlanBody{Descriptor: descriptor, Requirements: []protocol.CapabilityRequirement{{Capability: protocol.CapabilityTextInput, Level: protocol.CapabilityRequired}}, ToolExposureRevision: "tools-r1", Warnings: []string{}}
	providerDigest, err := canonicaljson.Digest(providerBody)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("provider plan", func(t *testing.T) {
		assertIdentity(t, protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", Kind: protocol.EventProviderCapabilityDecided, RuntimeGenerationID: "generation"}, protocol.ProviderCapabilityDecidedV1{Plan: protocol.NegotiatedProviderPlan{Body: providerBody, Digest: providerDigest}, Status: "accepted", Missing: []string{}}, "runtime")
	})

	checkpoint := protocol.CheckpointBody{ID: "checkpoint", SessionID: "session", TaskID: "task", TurnID: "turn", EventHead: protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "session", CommitSeq: 1, TransactionID: "head"}, ContextPlanDigest: digest, OutcomeContractID: "contract", ContractVersion: 1, RuntimeGenerationID: "generation", PlanDigest: digest, Coverage: []protocol.CheckpointCoverage{}, CreatedAt: time.Unix(1, 0).UTC()}
	t.Run("checkpoint planned", func(t *testing.T) {
		assertIdentity(t, protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", Kind: protocol.EventCheckpointPlanned, TaskID: "task", TurnID: "turn", RuntimeGenerationID: "generation"}, protocol.CheckpointPlannedV1{Body: checkpoint, PlanDigest: digest}, "task", "turn", "runtime")
	})
	t.Run("checkpoint session mismatch reaches identity validator", func(t *testing.T) {
		mismatched := protocol.DeepCopy(checkpoint)
		mismatched.SessionID = "other"
		record, err := registry.Decode(envelopeFor(t, protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", Kind: protocol.EventCheckpointPlanned, TaskID: "task", TurnID: "turn", RuntimeGenerationID: "generation"}, protocol.CheckpointPlannedV1{Body: mismatched, PlanDigest: digest}))
		if err != nil {
			t.Fatal(err)
		}
		err = registry.Validate(record)
		if err == nil || !strings.Contains(err.Error(), "checkpoint session ID does not match envelope") {
			t.Fatalf("checkpoint session mismatch did not reach identity validator: %v", err)
		}
	})
	checkpointDigest, err := canonicaljson.Digest(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("checkpoint ready", func(t *testing.T) {
		assertIdentity(t, protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", Kind: protocol.EventCheckpointReady, TaskID: "task", TurnID: "turn", RuntimeGenerationID: "generation"}, protocol.CheckpointReadyV1{Body: checkpoint, Digest: checkpointDigest, RecoveryMaterialIDs: []protocol.RecoveryMaterialID{}}, "task", "turn", "runtime")
	})

	receiptBody := protocol.VerificationReceiptBody{ID: "receipt", TaskID: "task", OutcomeContractID: "contract", ContractVersion: 1, CriterionID: "criterion", ActivityID: "activity", Subject: protocol.SubjectRef{Kind: "file", ID: "/x"}, VerifierID: "verifier", VerifierVersion: "v1", RequestedProfile: "restricted", EffectiveProfile: "restricted", StartedAt: time.Unix(1, 0).UTC(), TerminalAt: time.Unix(2, 0).UTC(), Status: "passed", Coverage: "full", EvidenceIDs: []protocol.EvidenceID{}, UnsupportedConclusions: []string{}}
	receiptDigest, err := canonicaljson.Digest(receiptBody)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("verification receipt", func(t *testing.T) {
		assertIdentity(t, protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", Kind: protocol.EventVerificationReceiptRecorded, TaskID: "task", ActivityID: "activity"}, protocol.VerificationReceiptRecordedV1{Receipt: protocol.VerificationReceipt{Body: receiptBody, Digest: receiptDigest}}, "task", "activity")
	})

	consumed := protocol.AuthorizationDecisionConsumedV1{DecisionNonce: "nonce", DecisionEventID: "decision", DecisionDigest: digest, RequestID: "request", ActivityID: "activity", CallID: "call", PlanDigest: digest, RequestDigest: digest, DispatchDigest: digest, RuntimeGenerationID: "generation"}
	t.Run("authorization consumption", func(t *testing.T) {
		assertIdentity(t, protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", Kind: protocol.EventAuthorizationDecisionConsumed, ActivityID: "activity", RuntimeGenerationID: "generation"}, consumed, "activity", "runtime")
	})

	started := protocol.ActivityStartedV1{DecisionNonce: "nonce", DecisionEventID: "decision", ActivityID: "activity", CallID: "call", PlanDigest: digest, RequestDigest: digest, DispatchDigest: digest, RuntimeGenerationID: "generation", DispatchState: "dispatched"}
	t.Run("activity start", func(t *testing.T) {
		assertIdentity(t, protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", Kind: protocol.EventActivityStarted, ActivityID: "activity", RuntimeGenerationID: "generation"}, started, "activity", "runtime")
	})

	decision := registryAuthorizationDecision()
	t.Run("authorization decision", func(t *testing.T) {
		assertIdentity(t, protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", Kind: protocol.EventAuthorizationDecided, ActivityID: "activity", RuntimeGenerationID: "generation"}, decision, "activity", "runtime")
	})

	evidenceBody := protocol.EvidenceRecordBody{ID: "evidence", Kind: "output", WorkspaceID: "workspace", SessionID: "session", Availability: protocol.ContentMissing, MediaType: "text/plain", ProducingActivityID: "activity", Actor: protocol.ActorRef{ID: "tool", Kind: protocol.ActorTool}, Subject: protocol.SubjectRef{Kind: "file", ID: "/x"}, CreatedAt: time.Unix(1, 0).UTC()}
	evidenceDigest, err := canonicaljson.Digest(evidenceBody)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("evidence record", func(t *testing.T) {
		assertIdentity(t, protocol.EventEnvelope{JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", Kind: protocol.EventEvidenceRecorded, ActivityID: "activity"}, protocol.EvidenceRecordedV1{Record: protocol.EvidenceRecord{Body: evidenceBody, Digest: evidenceDigest}}, "activity")
	})

	controlStarted := protocol.ControlOperationStartedV1{ControlOperationID: "control", DecisionEventID: "decision", DecisionNonce: "nonce", PlanDigest: digest, RequestDigest: digest, DispatchDigest: digest, RuntimeGenerationID: "generation"}
	t.Run("control operation start", func(t *testing.T) {
		assertIdentity(t, protocol.EventEnvelope{JournalKind: protocol.JournalWorkspaceControl, JournalID: "workspace", Kind: protocol.EventControlOperationStarted, RuntimeGenerationID: "generation"}, controlStarted, "runtime")
	})

	manifestBody := protocol.RuntimeGenerationBody{ProviderCatalogRevision: "providers", Models: []protocol.ModelDescriptor{}, ToolCatalogRevision: "tools", Tools: []protocol.ToolDescriptor{}, InstructionRevision: "instructions", PolicyGeneration: "policy", ExecutionProfiles: []string{"restricted"}, Limits: protocol.RuntimeLimits{MaxToolCalls: 1, ShellTimeoutNanos: 1, ApplicationQueueCapacity: 1}}
	manifestDigest, err := canonicaljson.Digest(manifestBody)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("runtime generation", func(t *testing.T) {
		assertIdentity(t, protocol.EventEnvelope{JournalKind: protocol.JournalWorkspaceControl, JournalID: "workspace", Kind: protocol.EventRuntimeGenerationActivated, RuntimeGenerationID: "generation"}, protocol.RuntimeGenerationActivatedV1{Manifest: protocol.RuntimeGenerationManifest{ID: "generation", Body: manifestBody, Digest: manifestDigest}}, "runtime")
	})
}
