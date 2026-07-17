package protocol_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/protocol"
)

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
