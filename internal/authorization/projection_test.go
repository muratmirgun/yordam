package authorization_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestProjectionAuthorizationRetainsBindingsGrantsRevocationAndConsumedNonce(t *testing.T) {
	projector := authorization.Projector{}
	state := projector.Zero(sessionRef())
	request := authorizationRequest()
	state = applyAuthorization(t, projector, state, authorizationRecord(protocol.EventAuthorizationRequested, &protocol.AuthorizationRequestedV1{Request: request}))
	decision := authorizationDecision(request, protocol.AuthorizationLifetimeSession)
	state = applyAuthorization(t, projector, state, authorizationRecord(protocol.EventAuthorizationDecided, &protocol.AuthorizationDecidedV1{Decision: decision}))
	state = applyAuthorization(t, projector, state, authorizationRecord(protocol.EventAuthorizationDecisionConsumed, consumed(decision)))

	if !projector.NonceConsumed(state, decision.DecisionNonce) {
		t.Fatal("decision nonce was not consumed")
	}
	grant, ok := projector.LookupGrant(state, string(decision.DecisionNonce))
	if !ok || grant.Revoked || grant.Decision.Request.RequestID != request.RequestID {
		t.Fatalf("grant=%+v ok=%v", grant, ok)
	}
	state = applyAuthorization(t, projector, state, authorizationRecord(protocol.EventAuthorizationGrantRevoked, &protocol.AuthorizationGrantRevokedV1{
		GrantID: string(decision.DecisionNonce), Reason: "operator revoked", RevocationEpoch: 2,
	}))
	grant, _ = projector.LookupGrant(state, string(decision.DecisionNonce))
	if !grant.Revoked || grant.RevocationEpoch != 2 {
		t.Fatalf("revoked grant=%+v", grant)
	}
	if _, err := projector.Apply(state, authorizationRecord(protocol.EventAuthorizationDecisionConsumed, consumed(decision))); err == nil || !strings.Contains(err.Error(), "consumed") {
		t.Fatalf("nonce reuse error=%v", err)
	}
}

func TestProjectionAuthorizationRejectsChangedDecisionBinding(t *testing.T) {
	projector := authorization.Projector{}
	state := projector.Zero(sessionRef())
	request := authorizationRequest()
	state = applyAuthorization(t, projector, state, authorizationRecord(protocol.EventAuthorizationRequested, &protocol.AuthorizationRequestedV1{Request: request}))
	decision := authorizationDecision(request, protocol.AuthorizationLifetimeOnce)
	decision.Request.DispatchDigest = testDigest("f")
	if _, err := projector.Apply(state, authorizationRecord(protocol.EventAuthorizationDecided, &protocol.AuthorizationDecidedV1{Decision: decision})); err == nil {
		t.Fatal("changed exact request binding accepted")
	}
}

func TestProjectionCommandRejectsChangedDigestAndReturnsImmutableTerminalResult(t *testing.T) {
	projector := authorization.Projector{}
	state := projector.Zero(sessionRef())
	accepted := &protocol.CommandAcceptedV1{CommandID: "command", RequestDigest: testDigest("a"), IdempotencyKey: "idempotency"}
	state = applyAuthorization(t, projector, state, commandRecord(protocol.EventCommandAccepted, accepted))
	state = applyAuthorization(t, projector, state, commandRecord(protocol.EventCommandAccepted, accepted))

	changed := *accepted
	changed.RequestDigest = testDigest("b")
	if _, err := projector.Apply(state, commandRecord(protocol.EventCommandAccepted, &changed)); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("changed digest error=%v", err)
	}
	result := protocol.CommandResult{
		ProtocolVersion: protocol.ApplicationProtocolVersion, CommandID: "command", Status: "completed", RequestDigest: accepted.RequestDigest,
		Cursor: protocol.ApplicationCursor{}, PayloadVersion: 1, Payload: json.RawMessage(`{"ok":true}`),
	}
	rawResult, _ := json.Marshal(result)
	state = applyAuthorization(t, projector, state, commandRecord(protocol.EventCommandCompleted, &protocol.CommandCompletedV1{
		CommandID: "command", RequestDigest: accepted.RequestDigest, Status: "completed", Result: rawResult,
	}))

	lookup, ok := projector.LookupCommand(state, sessionRef(), "command")
	if !ok || lookup.Result == nil || lookup.IdempotencyKey != "idempotency" || lookup.Result.Status != "completed" {
		t.Fatalf("lookup=%+v ok=%v", lookup, ok)
	}
	lookup.Result.Payload[0] ^= 1
	again, _ := projector.LookupCommand(state, sessionRef(), "command")
	if !json.Valid(again.Result.Payload) {
		t.Fatal("command lookup aliases projection state")
	}
}

func TestProjectionAuthorizationRejectsUnknownStatefulEvent(t *testing.T) {
	projector := authorization.Projector{}
	unknown := record("future.authorization_policy", "", &struct{}{})
	if _, err := projector.Apply(projector.Zero(sessionRef()), unknown); err == nil {
		t.Fatal("unknown stateful event was skipped")
	}
}

func applyAuthorization(t *testing.T, projector authorization.Projector, state authorization.Projection, record protocol.EventRecord) authorization.Projection {
	t.Helper()
	next, err := projector.Apply(state, record)
	if err != nil {
		t.Fatal(err)
	}
	return next
}

func authorizationRequest() protocol.AuthorizationRequest {
	return protocol.AuthorizationRequest{
		RequestID: "request", Principal: protocol.ActorRef{ID: "user", Kind: protocol.ActorUser}, Actor: protocol.ActorRef{ID: "agent", Kind: protocol.ActorAgent},
		SessionID: "session", TaskID: "task", TurnID: "turn", ActivityID: "activity", CallID: "call", QueueID: "queue",
		Source: protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "read"}, SourceRevision: "revision", DescriptorDigest: testDigest("1"),
		Action: "read", Resources: []protocol.ResourceTarget{{Kind: "file", CanonicalID: "README.md"}}, ExecutionLocus: "local",
		RequestedProfile: "restricted local", EffectiveProfile: "restricted local", Effect: "observe", Boundary: "workspace", Reversibility: "exact",
		VerificationCoverage: "exact", RuntimeGenerationID: "generation", PolicyGeneration: "policy",
		PolicyProvenance: []protocol.PolicyProvenance{{Source: "builtin", Revision: "1", Generation: "policy"}},
		PlanDigest:       testDigest("2"), RequestDigest: testDigest("3"), DispatchDigest: testDigest("4"),
	}
}

func authorizationDecision(request protocol.AuthorizationRequest, lifetime string) protocol.AuthorizationDecision {
	return protocol.AuthorizationDecision{
		Request: request, Action: "allow", Scope: protocol.CanonicalAuthorizationScope{Capability: request.Action, Source: request.Source, Resources: request.Resources},
		Lifetime: lifetime, PolicySource: "builtin", PolicyGeneration: request.PolicyGeneration, Reason: "policy allow",
		DecidedAt: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC), PlanDigest: request.PlanDigest, DecisionNonce: "nonce",
	}
}

func consumed(decision protocol.AuthorizationDecision) *protocol.AuthorizationDecisionConsumedV1 {
	decisionDigest, _ := canonicaljson.Digest(decision)
	return &protocol.AuthorizationDecisionConsumedV1{
		DecisionNonce: decision.DecisionNonce, DecisionEventID: "event-authorization.decided", DecisionDigest: decisionDigest, RequestID: decision.Request.RequestID,
		ActivityID: decision.Request.ActivityID, CallID: decision.Request.CallID, PlanDigest: decision.Request.PlanDigest,
		RequestDigest: decision.Request.RequestDigest, DispatchDigest: decision.Request.DispatchDigest, RuntimeGenerationID: decision.Request.RuntimeGenerationID,
	}
}

func authorizationRecord(kind string, decoded any) protocol.EventRecord {
	return record(kind, "activity", decoded)
}

func commandRecord(kind string, decoded any) protocol.EventRecord { return record(kind, "", decoded) }

func record(kind string, activityID protocol.ActivityID, decoded any) protocol.EventRecord {
	raw, _ := json.Marshal(decoded)
	return protocol.EventRecord{Envelope: protocol.EventEnvelope{
		JournalKind: protocol.JournalSession, JournalID: "session", SessionID: "session", EventID: protocol.EventID("event-" + kind),
		Time: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC), Kind: kind, PayloadVersion: 1, TaskID: "task", TurnID: "turn", ActivityID: activityID, Payload: raw,
	}, Decoded: decoded}
}

func sessionRef() protocol.JournalRef {
	return protocol.JournalRef{Kind: protocol.JournalSession, ID: "session"}
}

func testDigest(fill string) protocol.Digest {
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat(fill, 64)}
}
