package tooling

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/tools/edit"
	"github.com/muratmirgun/yordam/internal/tools/output"
	"github.com/muratmirgun/yordam/internal/tools/read"
)

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

func TestPlanMutationConcurrentRevalidateHasConsistentSnapshot(t *testing.T) {
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
		if mutationErr != nil || mutationPlan.Body.Tool != mutate.canonical.Body.Identity {
			t.Fatalf("iteration %d observed inconsistent preview snapshot: plan=%#v err=%v", index, mutationPlan, mutationErr)
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
