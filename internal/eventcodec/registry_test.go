package eventcodec_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/eventcodec"
	"github.com/muratmirgun/yordam/internal/protocol"
)

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
	digest := protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("a", 64)}
	decision := protocol.AuthorizationDecidedV1{Decision: protocol.AuthorizationDecision{
		Request:       protocol.AuthorizationRequest{RequestID: "request", SessionID: "session", ActivityID: "activity", CallID: "call", PlanDigest: digest, RequestDigest: digest, DispatchDigest: digest, RuntimeGenerationID: "generation"},
		DecisionNonce: "nonce",
	}}
	decisionDigest, err := canonicaljson.Digest(decision.Decision)
	if err != nil {
		t.Fatal(err)
	}
	consumed := protocol.AuthorizationDecisionConsumedV1{DecisionNonce: "nonce", DecisionEventID: "decision-event", DecisionDigest: decisionDigest, RequestID: "request", ActivityID: "activity", CallID: "call", PlanDigest: digest, RequestDigest: digest, DispatchDigest: digest, RuntimeGenerationID: "generation"}
	started := protocol.ActivityStartedV1{DecisionNonce: "nonce", DecisionEventID: "decision-event", ActivityID: "activity", CallID: "call", PlanDigest: digest, RequestDigest: digest, DispatchDigest: digest, RuntimeGenerationID: "generation", DispatchState: "dispatched"}
	if err := eventcodec.ValidateAuthorizationConsumption(consumed, protocol.EventEnvelope{EventID: "decision-event"}, decision, started); err != nil {
		t.Fatal(err)
	}
	started.CallID = "other"
	if err := eventcodec.ValidateAuthorizationConsumption(consumed, protocol.EventEnvelope{EventID: "decision-event"}, decision, started); err == nil {
		t.Fatal("mismatched start binding accepted")
	}
}
