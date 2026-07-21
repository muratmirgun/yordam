package orchestrator

import (
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestValidateProjectSkillTrustControlBindsEveryTrustTargetField(t *testing.T) {
	valid := validProjectSkillTrustControlRequest(t)
	if err := validateControlRequest(valid, false); err != nil {
		t.Fatalf("exact trust control rejected: %v", err)
	}

	for name, mutate := range map[string]func(*ControlRequest){
		"event workspace differs from journal": func(request *ControlRequest) {
			request.Event.Payload = mustCanonical(protocol.ProjectSkillTrustChangedV1{WorkspaceID: "other-workspace", CatalogDigest: trustTestDigest(), Decision: "allow"})
		},
		"malformed trust payload": func(request *ControlRequest) {
			request.Event.Payload = []byte(`{"workspace_id":"workspace-skill-trust"}`)
		},
		"wrong tool": func(request *ControlRequest) {
			request.Plan.Body.Tool.Name = "setting"
			redigestTrustTestPlan(t, request)
		},
		"wrong action": func(request *ControlRequest) {
			request.Plan.Body.Action = "runtime.setting.mode"
			redigestTrustTestPlan(t, request)
		},
		"wrong boundary": func(request *ControlRequest) {
			request.Plan.Body.Boundary = "session"
			redigestTrustTestPlan(t, request)
		},
		"wrong resource kind": func(request *ControlRequest) {
			request.Plan.Body.Resources[0].Kind = "session_journal"
			redigestTrustTestPlan(t, request)
		},
		"wrong resource canonical id": func(request *ControlRequest) {
			request.Plan.Body.Resources[0].CanonicalID = "other-workspace"
			redigestTrustTestPlan(t, request)
		},
		"wrong resource digest": func(request *ControlRequest) {
			request.Plan.Body.Resources[0].Digest = "sha256:" + repeatedDigest("b").Value
			redigestTrustTestPlan(t, request)
		},
		"wrong decision attribute": func(request *ControlRequest) {
			request.Plan.Body.Resources[0].Attributes[0].Value = "deny"
			redigestTrustTestPlan(t, request)
		},
		"extra decision attribute": func(request *ControlRequest) {
			request.Plan.Body.Resources[0].Attributes = append(request.Plan.Body.Resources[0].Attributes, protocol.ResourceAttribute{Name: "extra", Value: "x"})
			redigestTrustTestPlan(t, request)
		},
	} {
		t.Run(name, func(t *testing.T) {
			request := protocol.DeepCopy(valid)
			mutate(&request)
			if err := validateControlRequest(request, false); err == nil {
				t.Fatal("mismatched trust control was accepted")
			}
		})
	}
}

func validProjectSkillTrustControlRequest(t *testing.T) ControlRequest {
	t.Helper()
	runtime := validRuntimeManifest(t, "observation")
	workspace := protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace-skill-trust"}
	head := protocol.CommittedCursor{JournalKind: workspace.Kind, JournalID: workspace.ID, CommitSeq: 1, TransactionID: "workspace-head"}
	trust := protocol.ProjectSkillTrustChangedV1{WorkspaceID: protocol.WorkspaceID(workspace.ID), CatalogDigest: trustTestDigest(), Decision: "allow"}
	descriptor, err := canonicaljson.Digest(struct {
		Name string `json:"name"`
	}{"runtime.skill.trust"})
	if err != nil {
		t.Fatal(err)
	}
	body := protocol.ActionPlanBody{
		CallID: "skill-trust-call", Tool: protocol.ToolIdentity{Source: "runtime", Authority: "yordam", Name: "skill-trust"}, SourceRevision: "runtime-v1",
		DescriptorDigest: descriptor, Action: "runtime.skill.trust", Purpose: "record explicit project skill catalog trust",
		Resources:      []protocol.ResourceTarget{{Kind: "skill_catalog", CanonicalID: string(trust.WorkspaceID), Digest: trust.CatalogDigest.Algorithm + ":" + trust.CatalogDigest.Value, Attributes: []protocol.ResourceAttribute{{Name: "decision", Value: trust.Decision}}}},
		ExecutionLocus: "runtime", Effect: "mutation", Boundary: "workspace_control", Reversibility: "exact", VerificationCoverage: "exact",
		RequestedProfile: "restricted", EffectiveProfile: "restricted", RuntimeGenerationID: runtime.ID,
	}
	planDigest, err := canonicaljson.Digest(body)
	if err != nil {
		t.Fatal(err)
	}
	actor := protocol.ActorRef{ID: "user-skill-trust", Kind: protocol.ActorUser}
	return ControlRequest{
		Command:     CommandMetadata{CommandID: "command-skill-trust", IdempotencyKey: "key-skill-trust", RequestDigest: repeatedDigest("a"), Actor: actor},
		OperationID: "operation-skill-trust", Kind: OperationControl, Journal: workspace, ExpectedHead: head, TransactionID: "trust-terminal",
		Runtime: runtime, Plan: protocol.ActionPlan{Body: body, Digest: planDigest},
		Event: protocol.ProposedEvent{EventID: "event-skill-trust", Time: time.Unix(1, 0).UTC(), PayloadVersion: 1, Kind: protocol.EventProjectSkillTrustChanged, Actor: &actor, RuntimeGenerationID: runtime.ID, Payload: mustCanonical(trust)},
	}
}

func trustTestDigest() protocol.Digest { return repeatedDigest("a") }

func redigestTrustTestPlan(t *testing.T, request *ControlRequest) {
	t.Helper()
	digest, err := canonicaljson.Digest(request.Plan.Body)
	if err != nil {
		t.Fatal(err)
	}
	request.Plan.Digest = digest
}
