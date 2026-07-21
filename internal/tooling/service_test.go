package tooling

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/authorization"
	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/tools/edit"
	"github.com/muratmirgun/yordam/internal/tools/output"
	"github.com/muratmirgun/yordam/internal/tools/read"
	subagenttool "github.com/muratmirgun/yordam/internal/tools/subagent"
)

func TestPlanOrchestratedAuthorizationAcceptsOnlyExactCanonicalMarkerAndBindsRequest(t *testing.T) {
	catalog, err := NewCatalog("revision-1", subagenttool.New())
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(catalog)
	request := planRequest("subagent", `{"task":"bounded child"}`)
	_, first, err := service.PlanOrchestratedAuthorization(context.Background(), request, subagenttool.Kind)
	if err != nil {
		t.Fatal(err)
	}
	if first.Body.Effect != "orchestration" || first.Body.Tool != subagenttool.BuiltinDescriptor().Body.Identity || first.Body.Action != "subagent" || first.Body.ExecutionLocus != "orchestrator" {
		t.Fatalf("orchestrated authorization plan lost canonical authority shape: %+v", first)
	}
	service.mu.Lock()
	planned := len(service.actions)
	service.mu.Unlock()
	if planned != 0 {
		t.Fatalf("orchestrated authorization left %d dispatchable tool handles", planned)
	}
	changed := request
	changed.CallID = "different-call"
	_, second, err := service.PlanOrchestratedAuthorization(context.Background(), changed, subagenttool.Kind)
	if err != nil {
		t.Fatal(err)
	}
	if second.Digest == first.Digest {
		t.Fatal("orchestrated authorization digest did not bind the exact call")
	}
	changed = request
	changed.TurnID = "different-turn"
	_, third, err := service.PlanOrchestratedAuthorization(context.Background(), changed, subagenttool.Kind)
	if err != nil {
		t.Fatal(err)
	}
	if third.Digest != first.Digest {
		t.Fatal("action-plan digest unexpectedly included queue identity; authorization request owns turn binding")
	}
	if _, _, err := service.PlanOrchestratedAuthorization(context.Background(), request, "lookalike"); err == nil {
		t.Fatal("mismatched orchestrated marker was accepted")
	}
	if _, _, err := service.PlanPreviewInspection(context.Background(), request); err == nil {
		t.Fatal("ordinary observation-preview seam accepted orchestrated tool")
	}
}

func TestExecutionResultCarriesOnlyStructuredFileChangeFacts(t *testing.T) {
	result := executionResult(domain.ToolResult{
		CallID:  "edit-call",
		Status:  domain.ToolSucceeded,
		Content: "untrusted diff text",
		FileChange: &domain.FileChange{
			CallID:       "edit-call",
			Path:         "/workspace/target.txt",
			BeforeSHA256: strings.Repeat("a", 64),
			AfterSHA256:  strings.Repeat("b", 64),
			Diff:         "must not enter the structured effect",
		},
	})
	if result.FileChange == nil {
		t.Fatal("structured file change was dropped")
	}
	want := protocol.FileChangedV1{
		CallID:      "edit-call",
		Subject:     protocol.SubjectRef{Kind: "file", ID: "/workspace/target.txt"},
		Before:      protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("a", 64)},
		After:       protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("b", 64)},
		EvidenceIDs: []protocol.EvidenceID{},
	}
	if !reflect.DeepEqual(*result.FileChange, want) {
		t.Fatalf("file change=%+v want=%+v", *result.FileChange, want)
	}

	created := executionResult(domain.ToolResult{CallID: "create-call", Status: domain.ToolSucceeded, FileChange: &domain.FileChange{CallID: "create-call", Path: "/workspace/new.txt", AfterSHA256: strings.Repeat("c", 64)}})
	if created.FileChange == nil || created.FileChange.Before != (protocol.Digest{Algorithm: protocol.DigestSHA256, Value: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}) {
		t.Fatalf("create preimage=%+v", created.FileChange)
	}

	uncertain := executionResult(domain.ToolResult{CallID: "edit-uncertain", Status: domain.ToolFailed, FileChange: &domain.FileChange{CallID: "edit-uncertain", Path: "/workspace/target.txt", BeforeSHA256: strings.Repeat("d", 64), AfterSHA256: strings.Repeat("e", 64)}})
	if uncertain.Outcome.Status != "uncertain" || uncertain.ToolResult.Status != "uncertain" || uncertain.FileChange == nil {
		t.Fatalf("possible file effect was not conservative: %+v", uncertain)
	}
}

func TestDispatchToolExecuteConsumesHandleAndTokenBeforeOneEffect(t *testing.T) {
	tool := newDispatchTool("mutate", trustedRemoteClassification())
	catalog, err := NewCatalog("revision-1", tool)
	if err != nil {
		t.Fatal(err)
	}
	gate := &countingDispatchGate{}
	service := NewService(catalog, gate)
	handle, _, err := service.Plan(context.Background(), planRequest("mutate", `{}`))
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Execute(context.Background(), handle, authorization.CommittedToken{})
	if err != nil {
		t.Fatal(err)
	}
	if result.ToolResult.Status != "succeeded" || tool.effects.Load() != 1 {
		t.Fatalf("result=%#v effects=%d", result, tool.effects.Load())
	}
	if _, err := service.Execute(context.Background(), handle, authorization.CommittedToken{}); !errors.Is(err, authorization.ErrAlreadyDispatched) {
		t.Fatalf("repeat err=%v", err)
	}
	if tool.effects.Load() != 1 {
		t.Fatalf("repeat effects=%d", tool.effects.Load())
	}
}

func TestDispatchToolCancellationJoinsEffectCleanupBeforeReturning(t *testing.T) {
	tool := newDispatchTool("mutate", trustedRemoteClassification())
	started := make(chan struct{})
	cleanup := make(chan struct{})
	tool.execute = func(ctx context.Context, request domain.ToolRequest) domain.ToolResult {
		close(started)
		<-ctx.Done()
		<-cleanup
		return domain.ToolResult{CallID: request.CallID, Status: domain.ToolCancelled}
	}
	catalog, err := NewCatalog("revision-1", tool)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(catalog, &countingDispatchGate{})
	handle, _, err := service.Plan(context.Background(), planRequest("mutate", `{}`))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, executeErr := service.Execute(ctx, handle, authorization.CommittedToken{})
		done <- executeErr
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		t.Fatalf("execute returned before effect cleanup: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(cleanup)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("execute error=%v", err)
	}
}

func TestDispatchToolConcurrentDoubleExecuteProducesOneEffect(t *testing.T) {
	tool := newDispatchTool("mutate", trustedRemoteClassification())
	catalog, err := NewCatalog("revision-1", tool)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(catalog, &countingDispatchGate{})
	handle, _, err := service.Plan(context.Background(), planRequest("mutate", `{}`))
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			_, executeErr := service.Execute(context.Background(), handle, authorization.CommittedToken{})
			errs <- executeErr
		}()
	}
	close(start)
	var success, repeated int
	for range 2 {
		err := <-errs
		if err == nil {
			success++
		} else if errors.Is(err, authorization.ErrAlreadyDispatched) {
			repeated++
		} else {
			t.Fatalf("err=%v", err)
		}
	}
	if success != 1 || repeated != 1 || tool.effects.Load() != 1 {
		t.Fatalf("success=%d repeated=%d effects=%d", success, repeated, tool.effects.Load())
	}
}

func TestDispatchToolRejectsZeroTokenBeforeEffect(t *testing.T) {
	tool := newDispatchTool("mutate", trustedRemoteClassification())
	catalog, err := NewCatalog("revision-1", tool)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(catalog, authorization.NewService(nil))
	handle, _, err := service.Plan(context.Background(), planRequest("mutate", `{}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Execute(context.Background(), handle, authorization.CommittedToken{}); !errors.Is(err, authorization.ErrInvalidCommittedToken) {
		t.Fatalf("zero err=%v", err)
	}
	if tool.effects.Load() != 0 {
		t.Fatalf("zero token effects=%d", tool.effects.Load())
	}
	if tool.revalidations.Load() != 0 {
		t.Fatalf("zero token crossed revalidation boundary=%d", tool.revalidations.Load())
	}
}

func TestPreviewDispatchRejectionOpensNoResourceAndReturnsObservationAuthority(t *testing.T) {
	classification := domain.ToolClassification{Effect: "mutation", Mutation: "file", ExecutionLoci: []string{"builtin"}, Boundary: "workspace", Reversibility: "preimage", VerificationCoverage: "full", Idempotency: "conditional", Retry: "never_after_dispatch", RequestedProfile: "restricted", EffectiveProfile: "restricted"}
	tool := newDispatchTool("edit_preview", classification)
	catalog, err := NewCatalog("revision-1", tool)
	if err != nil {
		t.Fatal(err)
	}
	rejecting := NewService(catalog, authorization.NewService(nil))
	rejectedHandle, rejectedPlan, err := rejecting.PlanPreviewInspection(context.Background(), planRequest("edit_preview", `{}`))
	if err != nil {
		t.Fatal(err)
	}
	if rejectedPlan.Body.Effect != "observation" || rejectedPlan.Body.Purpose != "inspect" || rejectedPlan.Body.Action == tool.CanonicalDescriptor().Body.Identity.Name {
		t.Fatalf("preview plan retained mutation authority: %#v", rejectedPlan.Body)
	}
	if _, _, _, err := rejecting.PreparePreview(context.Background(), rejectedHandle, authorization.CommittedToken{}); !errors.Is(err, authorization.ErrInvalidCommittedToken) {
		t.Fatalf("rejected preview err=%v", err)
	}
	if tool.opens.Load() != 0 {
		t.Fatalf("rejected preview opens=%d", tool.opens.Load())
	}
	if tool.revalidations.Load() != 0 {
		t.Fatalf("rejected preview crossed revalidation boundary=%d", tool.revalidations.Load())
	}

	allowed := NewService(catalog, &countingDispatchGate{})
	handle, plan, err := allowed.PlanPreviewInspection(context.Background(), planRequest("edit_preview", `{}`))
	if err != nil {
		t.Fatal(err)
	}
	preview, returnedPlan, evidence, err := allowed.PreparePreview(context.Background(), handle, authorization.CommittedToken{})
	if err != nil {
		t.Fatal(err)
	}
	if tool.opens.Load() != 1 || preview.handleID != handle.id || returnedPlan.Digest != plan.Digest || len(evidence) != 1 || len(evidence[0].Content) == 0 {
		t.Fatalf("opens=%d preview=%#v plan=%#v evidence=%#v", tool.opens.Load(), preview, returnedPlan, evidence)
	}
	if strings.Contains(string(evidence[0].Content), "before") || strings.Contains(string(evidence[0].Content), "after") {
		t.Fatalf("preview evidence was returned before redaction: %q", evidence[0].Content)
	}
	mutationRequest := planRequest("edit_preview", `{}`)
	plansBeforeMutation := tool.plans.Load()
	_, mutationPlan, err := allowed.PlanMutation(context.Background(), preview, mutationRequest)
	if err != nil {
		t.Fatal(err)
	}
	if mutationPlan.Body.Effect != "mutation" || mutationPlan.Digest == plan.Digest {
		t.Fatalf("mutation plan=%#v preview=%#v", mutationPlan, plan)
	}
	if tool.plans.Load() != plansBeforeMutation {
		t.Fatalf("mutation discarded observed prepared state and replanned: before=%d after=%d", plansBeforeMutation, tool.plans.Load())
	}
	allowed.mu.Lock()
	actionsBeforeRepeat := len(allowed.actions)
	allowed.mu.Unlock()
	if _, _, err := allowed.PlanMutation(context.Background(), preview, mutationRequest); err == nil {
		t.Fatal("consumed preview result created a second mutation plan")
	}
	allowed.mu.Lock()
	actionsAfterRepeat := len(allowed.actions)
	allowed.mu.Unlock()
	if actionsAfterRepeat != actionsBeforeRepeat || tool.effects.Load() != 0 {
		t.Fatalf("repeated preview changed state or executed: actions=%d want=%d effects=%d", actionsAfterRepeat, actionsBeforeRepeat, tool.effects.Load())
	}
}

func TestPreviewMutationRejectsUnrelatedObservationWithoutFallback(t *testing.T) {
	observationClassification := domain.ToolClassification{
		Effect: "observation", Mutation: "read_only", ExecutionLoci: []string{"remote"}, Boundary: "remote",
		Reversibility: "not_applicable", VerificationCoverage: "provider_reported", Idempotency: "idempotent",
		Retry: "safe_before_dispatch", RequestedProfile: "networked", EffectiveProfile: "networked",
	}
	observe := newDispatchTool("observe", observationClassification)
	mutate := newDispatchTool("mutate", trustedRemoteClassification())
	catalog, err := NewCatalog("revision-1", observe, mutate)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(catalog)
	handle, plan, err := service.PlanPreviewInspection(context.Background(), planRequest("observe", `{}`))
	if err != nil {
		t.Fatal(err)
	}
	plansBefore := mutate.plans.Load()
	if _, _, err := service.PlanMutation(context.Background(), PreviewResult{handleID: handle.id, observationDigest: plan.Digest}, planRequest("mutate", `{}`)); err == nil {
		t.Fatal("unrelated observation fell back to a fresh mutation plan")
	}
	if mutate.plans.Load() != plansBefore || mutate.effects.Load() != 0 {
		t.Fatalf("rejected observation replanned or executed: plans=%d effects=%d", mutate.plans.Load(), mutate.effects.Load())
	}
}

func TestPreviewMutationRequiresExactHandleRequestPreparedStateAndEvidence(t *testing.T) {
	classification := domain.ToolClassification{Effect: "mutation", Mutation: "file", ExecutionLoci: []string{"builtin"}, Boundary: "workspace", Reversibility: "preimage", VerificationCoverage: "full", Idempotency: "conditional", Retry: "never_after_dispatch", RequestedProfile: "restricted", EffectiveProfile: "restricted"}
	for name, mutate := range map[string]func(*PreviewResult, *PlanRequest){
		"handle": func(preview *PreviewResult, _ *PlanRequest) { preview.handleID = "other" },
		"observation": func(preview *PreviewResult, _ *PlanRequest) {
			preview.observationDigest = protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("a", 64)}
		},
		"evidence": func(preview *PreviewResult, _ *PlanRequest) {
			preview.evidenceDigests = []protocol.Digest{{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("b", 64)}}
		},
		"alias": func(_ *PreviewResult, request *PlanRequest) { request.Alias = "other" },
		"input": func(_ *PreviewResult, request *PlanRequest) { request.Arguments = json.RawMessage(`{"changed":true}`) },
		"call":  func(_ *PreviewResult, request *PlanRequest) { request.CallID = "other-call" },
	} {
		t.Run(name, func(t *testing.T) {
			tool := newDispatchTool("edit_preview", classification)
			other := newDispatchTool("other", classification)
			catalog, err := NewCatalog("revision-1", tool, other)
			if err != nil {
				t.Fatal(err)
			}
			service := NewService(catalog, &countingDispatchGate{})
			request := planRequest("edit_preview", `{}`)
			handle, _, err := service.PlanPreviewInspection(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			preview, _, _, err := service.PreparePreview(context.Background(), handle, authorization.CommittedToken{})
			if err != nil {
				t.Fatal(err)
			}
			mutate(&preview, &request)
			plansBefore := tool.plans.Load() + other.plans.Load()
			if _, _, err := service.PlanMutation(context.Background(), preview, request); err == nil {
				t.Fatal("mismatched preview mutation was accepted")
			}
			if got := tool.plans.Load() + other.plans.Load(); got != plansBefore || tool.effects.Load()+other.effects.Load() != 0 {
				t.Fatalf("mismatch replanned or executed: plans=%d want=%d effects=%d", got, plansBefore, tool.effects.Load()+other.effects.Load())
			}
		})
	}
	for _, mismatch := range []string{"prepared", "effect"} {
		t.Run(mismatch, func(t *testing.T) {
			tool := newDispatchTool("edit_preview", classification)
			catalog, err := NewCatalog("revision-1", tool)
			if err != nil {
				t.Fatal(err)
			}
			service := NewService(catalog, &countingDispatchGate{})
			request := planRequest("edit_preview", `{}`)
			handle, _, err := service.PlanPreviewInspection(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			preview, _, _, err := service.PreparePreview(context.Background(), handle, authorization.CommittedToken{})
			if err != nil {
				t.Fatal(err)
			}
			service.mu.Lock()
			if mismatch == "prepared" {
				service.actions[handle.id].prepared.(*dispatchPrepared).request.Input = json.RawMessage(`{"changed":true}`)
			} else {
				service.actions[handle.id].entry.classification.Effect = "observation"
			}
			service.mu.Unlock()
			plansBefore := tool.plans.Load()
			if _, _, err := service.PlanMutation(context.Background(), preview, request); err == nil {
				t.Fatal("mutated preview state was accepted")
			}
			if tool.plans.Load() != plansBefore || tool.effects.Load() != 0 {
				t.Fatalf("state mismatch replanned or executed: plans=%d want=%d effects=%d", tool.plans.Load(), plansBefore, tool.effects.Load())
			}
		})
	}
}

func TestPreviewMutationAllowsASeparateMutationActivity(t *testing.T) {
	classification := domain.ToolClassification{Effect: "mutation", Mutation: "file", ExecutionLoci: []string{"builtin"}, Boundary: "workspace", Reversibility: "preimage", VerificationCoverage: "full", Idempotency: "conditional", Retry: "never_after_dispatch", RequestedProfile: "restricted", EffectiveProfile: "restricted"}
	tool := newDispatchTool("edit_preview", classification)
	catalog, err := NewCatalog("revision-1", tool)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(catalog, &countingDispatchGate{})
	request := planRequest("edit_preview", `{}`)
	handle, _, err := service.PlanPreviewInspection(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	preview, _, _, err := service.PreparePreview(context.Background(), handle, authorization.CommittedToken{})
	if err != nil {
		t.Fatal(err)
	}
	request.ActivityID = "mutation-activity"
	_, plan, err := service.PlanMutation(context.Background(), preview, request)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Body.Effect != "mutation" || plan.Body.CallID != request.CallID {
		t.Fatalf("mutation plan=%#v", plan)
	}
}

type countingDispatchGate struct {
	mu   sync.Mutex
	used bool
}

func (g *countingDispatchGate) Dispatch(ctx context.Context, _ authorization.CommittedToken, _ authorization.DispatchBinding, callback func(context.Context) error) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.used {
		return authorization.ErrAlreadyDispatched
	}
	g.used = true
	return callback(ctx)
}

type dispatchTool struct {
	alias          string
	descriptor     protocol.ToolDescriptor
	classification domain.ToolClassification
	effects        atomic.Int64
	opens          atomic.Int64
	revalidations  atomic.Int64
	plans          atomic.Int64
	execute        func(context.Context, domain.ToolRequest) domain.ToolResult
}

func newDispatchTool(alias string, classification domain.ToolClassification) *dispatchTool {
	body := protocol.ToolDescriptorBody{Identity: protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: alias}, SourceRevision: "source-r1", DisplayName: alias, Description: alias, InputSchema: json.RawMessage(`{"type":"object"}`), Effect: classification.Effect, Mutation: classification.Mutation, ExecutionLoci: append([]string(nil), classification.ExecutionLoci...), ClassificationSource: "trusted_adapter", Idempotency: classification.Idempotency, Retry: classification.Retry}
	digest, _ := canonicaljson.Digest(body)
	return &dispatchTool{alias: alias, descriptor: protocol.ToolDescriptor{Body: body, DescriptorDigest: digest}, classification: classification}
}

func (t *dispatchTool) Descriptor() domain.ToolDescriptor {
	mutation := domain.MutationFile
	if t.classification.Mutation == "read_only" {
		mutation = domain.MutationReadOnly
	} else if t.classification.Mutation == "process" || t.classification.Mutation == "remote" {
		mutation = domain.MutationProcess
	}
	return domain.ToolDescriptor{Name: t.alias, Description: t.alias, ScopeDescription: "target", InputSchema: json.RawMessage(`{"type":"object"}`), Mutation: mutation}
}
func (t *dispatchTool) CanonicalDescriptor() protocol.ToolDescriptor     { return t.descriptor }
func (t *dispatchTool) TrustedClassification() domain.ToolClassification { return t.classification }
func (t *dispatchTool) Prepare(context.Context, domain.ToolRequest) (ports.PreparedTool, error) {
	return nil, errors.New("legacy prepare is unavailable")
}
func (t *dispatchTool) Plan(_ context.Context, request domain.ToolRequest) (ports.PreparedTool, error) {
	t.plans.Add(1)
	return &dispatchPrepared{request: request, tool: t}, nil
}

type dispatchPrepared struct {
	request domain.ToolRequest
	tool    *dispatchTool
}

func (p *dispatchPrepared) Preview() domain.PreparedToolRequest {
	return domain.PreparedToolRequest{Request: p.request, Mutation: domain.MutationFile, CanonicalScope: "/workspace/file", InsideWorkspace: true, Summary: "preview", ProposedDiff: "-before\n+after", Resources: []protocol.ResourceTarget{{Kind: "file", CanonicalID: "/workspace/file"}}}
}

func (p *dispatchPrepared) Revalidate(context.Context) (domain.PreparedToolRequest, error) {
	p.tool.revalidations.Add(1)
	return p.Preview(), nil
}
func (p *dispatchPrepared) PreparePreview(context.Context) error { p.tool.opens.Add(1); return nil }
func (p *dispatchPrepared) Execute(ctx context.Context) domain.ToolResult {
	p.tool.effects.Add(1)
	if p.tool.execute != nil {
		return p.tool.execute(ctx, p.request)
	}
	return domain.ToolResult{CallID: p.request.CallID, Status: domain.ToolSucceeded, Content: "effect"}
}

func TestPlanDigestUsesCanonicalResourcesAndHandleIsOpaque(t *testing.T) {
	workspace := t.TempDir()
	target := filepath.Join(workspace, "target.txt")
	if err := os.WriteFile(target, []byte("secret\n"), 0o000); err != nil {
		t.Fatal(err)
	}
	tool := read.New(read.Options{Workspace: workspace, Output: output.Options{SessionID: "s", Artifacts: discardArtifacts{}}})
	catalog, err := NewCatalog("revision-1", tool)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(catalog)
	handle, plan, err := service.PlanPreviewInspection(context.Background(), planRequest("read", `{"path":"target.txt","limit":2,"offset":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(handle, ActionHandle{}) {
		t.Fatal("zero action handle returned")
	}
	raw, err := json.Marshal(handle)
	if err != nil || string(raw) != "{}" {
		t.Fatalf("handle leaked through JSON: %s %v", raw, err)
	}
	canonicalTarget, err := filepath.EvalSymlinks(target)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Body.Tool != (protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "read"}) || plan.Body.Resources[0].CanonicalID != canonicalTarget {
		t.Fatalf("plan=%#v", plan)
	}
	if plan.Body.Action != "read" || plan.Body.Effect != "observation" {
		t.Fatalf("native observation was relabeled as preview authority: %#v", plan.Body)
	}
	if err := plan.Body.Validate(); err != nil {
		t.Fatal(err)
	}
	if want, err := canonicalPlanDigest(plan.Body); err != nil || want != plan.Digest {
		t.Fatalf("digest=%#v want=%#v err=%v", plan.Digest, want, err)
	}
}

func TestPlanDigestRejectsNoncanonicalDuplicateArguments(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "target.txt"), []byte("content\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tool := read.New(read.Options{Workspace: workspace, Output: output.Options{SessionID: "s", Artifacts: discardArtifacts{}}})
	catalog, err := NewCatalog("revision-1", tool)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = NewService(catalog).Plan(context.Background(), planRequest("read", `{"path":"target.txt","path":"target.txt"}`))
	if err == nil || !strings.Contains(err.Error(), "duplicate object key") {
		t.Fatalf("duplicate plan arguments error=%v", err)
	}
}

func TestRevalidateReportsChangedEditCanonicalScopeWithoutDispatch(t *testing.T) {
	workspace := t.TempDir()
	first := filepath.Join(workspace, "first.txt")
	second := filepath.Join(workspace, "second.txt")
	link := filepath.Join(workspace, "target.txt")
	before := []byte("before\n")
	for _, path := range []string{first, second} {
		if err := os.WriteFile(path, before, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(first, link); err != nil {
		t.Fatal(err)
	}
	tool := edit.New(edit.Options{Workspace: workspace, Output: output.Options{SessionID: "s", Artifacts: discardArtifacts{}}})
	catalog, err := NewCatalog("revision-1", tool)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(catalog)
	sum := sha256.Sum256(before)
	arguments := fmt.Sprintf(`{"path":"target.txt","expected_sha256":"%s","replacements":[{"old":"before","new":"after"}]}`, hex.EncodeToString(sum[:]))
	handle, original, err := service.Plan(context.Background(), planRequest("edit", arguments))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(second, link); err != nil {
		t.Fatal(err)
	}
	canonicalSecond, err := filepath.EvalSymlinks(second)
	if err != nil {
		t.Fatal(err)
	}
	current, changed, err := service.Revalidate(context.Background(), handle)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || current.Digest == original.Digest || current.Body.Resources[0].CanonicalID != canonicalSecond {
		t.Fatalf("changed=%v original=%#v current=%#v", changed, original, current)
	}
	if got, err := os.ReadFile(second); err != nil || string(got) != string(before) {
		t.Fatalf("revalidation dispatched edit: %q %v", got, err)
	}
	if _, _, err := service.Revalidate(context.Background(), handle); err == nil || !strings.Contains(err.Error(), "already revalidated") {
		t.Fatalf("one-shot handle error=%v", err)
	}
}

func TestPlanDigestUsesCanonicalActionAndExternalBoundary(t *testing.T) {
	identity := protocol.ToolIdentity{Source: "mcp", Authority: "server-a", Name: "write_record"}
	external := catalogExternalTool("records", identity, trustedRemoteClassification())
	catalog, err := NewCatalog("revision-1", external)
	if err != nil {
		t.Fatal(err)
	}
	_, plan, err := NewService(catalog).Plan(context.Background(), planRequest("records", `{}`))
	if err != nil {
		t.Fatal(err)
	}
	if plan.Body.Action != "write_record" || plan.Body.Tool != identity || plan.Body.Boundary != "remote" {
		t.Fatalf("plan used provider alias as canonical action: %#v", plan.Body)
	}

	workspace := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outside, []byte("outside\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	reader := read.New(read.Options{Workspace: workspace, Output: output.Options{SessionID: "s", Artifacts: discardArtifacts{}}})
	builtins, err := NewCatalog("revision-1", reader)
	if err != nil {
		t.Fatal(err)
	}
	arguments, _ := json.Marshal(map[string]string{"path": outside})
	request := planRequest("read", string(arguments))
	_, plan, err = NewService(builtins).Plan(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Body.Boundary != "filesystem_external" {
		t.Fatalf("outside resource retained workspace boundary: %#v", plan.Body)
	}
}

func TestPreviewMutationConcurrentRevalidateRejectsUnrelatedObservation(t *testing.T) {
	observationClassification := domain.ToolClassification{
		Effect: "observation", Mutation: "read_only", ExecutionLoci: []string{"remote"}, Boundary: "remote",
		Reversibility: "not_applicable", VerificationCoverage: "provider_reported", Idempotency: "idempotent",
		Retry: "safe_before_dispatch", RequestedProfile: "networked", EffectiveProfile: "networked",
	}
	observe := catalogExternalTool("observe", protocol.ToolIdentity{Source: "mcp", Authority: "server-a", Name: "observe"}, observationClassification)
	mutate := catalogExternalTool("mutate", protocol.ToolIdentity{Source: "mcp", Authority: "server-a", Name: "mutate"}, trustedRemoteClassification())
	catalog, err := NewCatalog("revision-1", observe, mutate)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(catalog)
	for index := 0; index < 128; index++ {
		observationRequest := planRequest("observe", `{}`)
		observationRequest.CallID = fmt.Sprintf("observe-%d", index)
		handle, plan, err := service.PlanPreviewInspection(context.Background(), observationRequest)
		if err != nil {
			t.Fatal(err)
		}
		preview := PreviewResult{handleID: handle.id, observationDigest: plan.Digest}
		mutationRequest := planRequest("mutate", `{}`)
		mutationRequest.CallID = fmt.Sprintf("mutate-%d", index)
		start := make(chan struct{})
		var wait sync.WaitGroup
		var mutationPlan protocol.ActionPlan
		var mutationErr error
		wait.Add(2)
		go func() {
			defer wait.Done()
			<-start
			_, _, _ = service.Revalidate(context.Background(), handle)
		}()
		go func() {
			defer wait.Done()
			<-start
			_, mutationPlan, mutationErr = service.PlanMutation(context.Background(), preview, mutationRequest)
		}()
		close(start)
		wait.Wait()
		if mutationErr == nil || !reflect.DeepEqual(mutationPlan, protocol.ActionPlan{}) {
			t.Fatalf("iteration %d unrelated preview created mutation plan: plan=%#v err=%v", index, mutationPlan, mutationErr)
		}
	}
}

func planRequest(alias, arguments string) PlanRequest {
	return PlanRequest{TurnID: "turn-1", ActivityID: "activity-1", CallID: "call-1", Alias: alias, Arguments: json.RawMessage(arguments), RuntimeGenerationID: "generation-1"}
}

func canonicalPlanDigest(body protocol.ActionPlanBody) (protocol.Digest, error) {
	return canonicalDigest(body)
}

type discardArtifacts struct{}

func (discardArtifacts) Put(_ context.Context, sessionID, mediaType string, source io.Reader, limit int64) (domain.Artifact, error) {
	written, err := io.Copy(io.Discard, io.LimitReader(source, limit+1))
	return domain.Artifact{ID: "artifact", SessionID: sessionID, MediaType: mediaType, Size: written}, err
}

func (discardArtifacts) Open(context.Context, domain.Artifact) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}
