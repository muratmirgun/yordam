package provider

import (
	"context"
	"errors"
	"testing"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type recordingAdapter struct {
	calls int
}

func (a *recordingAdapter) Kind() string { return "openai_compatible" }

func (a *recordingAdapter) Normalize(_ context.Context, request protocol.ModelRequest) (PreparedRequest, error) {
	a.calls++
	return NewPreparedRequest(a.Kind(), protocol.DeepCopy(request))
}

func TestServicePrepareIsEffectFreeAndBindsOpaqueHandle(t *testing.T) {
	descriptor := descriptorWith(protocol.CapabilityTextInput, protocol.CapabilitySupported)
	catalog := NewCatalog("rev-1", []protocol.ModelDescriptor{descriptor})
	plan, err := catalog.Negotiate("openai", "model-a", []protocol.CapabilityRequirement{{Capability: protocol.CapabilityTextInput, Level: protocol.CapabilityRequired}}, "tools-rev")
	if err != nil {
		t.Fatal(err)
	}
	adapter := &recordingAdapter{}
	service, err := NewService(catalog, []Adapter{adapter})
	if err != nil {
		t.Fatal(err)
	}
	request := protocol.ModelRequest{RequestID: "request-1", ProviderID: "openai", ModelID: "model-a", Messages: []protocol.ModelMessage{{Role: "user", Blocks: []protocol.ContentBlock{{Kind: protocol.ContentText, Text: "hello"}}}}, Requirements: plan.Body.Requirements, Plan: plan}
	contextDigest := testDigest('c')
	handle, err := service.Prepare(context.Background(), "activity-1", "call-1", request, contextDigest)
	if err != nil {
		t.Fatal(err)
	}
	if !handle.Valid() || adapter.calls != 1 || handle.activityID != "activity-1" || handle.callID != "call-1" || handle.contextPlanDigest != contextDigest || handle.runtimeGenerationID != "generation-1" {
		t.Fatalf("handle=%#v calls=%d", handle, adapter.calls)
	}
	wantRequest, _ := canonicaljson.Digest(request)
	if handle.requestDigest != wantRequest {
		t.Fatalf("request digest=%v want=%v", handle.requestDigest, wantRequest)
	}
	wantDispatch, _ := canonicaljson.Digest(dispatchBinding{RequestDigest: wantRequest, ContextPlanDigest: contextDigest, ProviderPlanDigest: plan.Digest, RuntimeGenerationID: "generation-1"})
	if handle.dispatchDigest != wantDispatch {
		t.Fatalf("dispatch digest=%v want=%v", handle.dispatchDigest, wantDispatch)
	}
	if (ProviderHandle{}).Valid() {
		t.Fatal("zero handle is valid")
	}
}

func TestServiceRejectsUnnegotiatedOrMismatchedRequestsBeforeAdapter(t *testing.T) {
	descriptor := descriptorWith(protocol.CapabilityStructuredOutput, protocol.CapabilityUnknown)
	adapter := &recordingAdapter{}
	service, err := NewService(NewCatalog("rev-1", []protocol.ModelDescriptor{descriptor}), []Adapter{adapter})
	if err != nil {
		t.Fatal(err)
	}
	requirements := []protocol.CapabilityRequirement{{Capability: protocol.CapabilityStructuredOutput, Level: protocol.CapabilityRequired}}
	body := protocol.NegotiatedProviderPlanBody{Descriptor: descriptor, Requirements: requirements, ToolExposureRevision: "tools-r1", Warnings: []string{}}
	digest, err := canonicaljson.Digest(body)
	if err != nil {
		t.Fatal(err)
	}
	request := protocol.ModelRequest{RequestID: "request-1", ProviderID: "openai", ModelID: "model-a", Requirements: requirements, Plan: protocol.NegotiatedProviderPlan{Body: body, Digest: digest}}
	if _, err := service.Prepare(context.Background(), "activity", "call", request, testDigest('d')); !errors.Is(err, ErrCapabilityUnavailable) {
		t.Fatalf("unnegotiated request error=%v", err)
	}
	if adapter.calls != 0 {
		t.Fatalf("adapter calls=%d", adapter.calls)
	}
}
