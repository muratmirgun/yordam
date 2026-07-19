package skill

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/skills"
	"github.com/muratmirgun/yordam/internal/tools/output"
)

func TestSkillCapturesCatalogEntryAndReturnsCanonicalContent(t *testing.T) {
	workspace := t.TempDir()
	content := []byte("---\nname: go-testing\ndescription: Test Go changes.\n---\nUse go test.\n")
	catalog := testCatalog(t, workspace, content, protocol.SkillSourceGlobal, "")
	tool := New(catalog, output.Options{SessionID: "s", Artifacts: discardArtifacts{}})
	prepared, err := tool.Prepare(context.Background(), domain.ToolRequest{CallID: "call", Name: "skill", Input: json.RawMessage(`{"name":"go-testing"}`)})
	if err != nil {
		t.Fatal(err)
	}
	preview := prepared.Preview()
	if !preview.InsideWorkspace || preview.CanonicalScope == "go-testing" || len(preview.Resources) != 1 || preview.Resources[0].Kind != "skill" {
		t.Fatalf("preview=%#v", preview)
	}
	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolSucceeded {
		t.Fatalf("result=%#v", result)
	}
	var got struct {
		Identity    protocol.SkillIdentity `json:"identity"`
		Description string                 `json:"description"`
		Source      protocol.SkillSource   `json:"source"`
		Digest      protocol.Digest        `json:"digest"`
		Provenance  struct {
			CanonicalPath       string                       `json:"canonical_path"`
			WorkspaceID         protocol.WorkspaceID         `json:"workspace_id"`
			RuntimeGenerationID protocol.RuntimeGenerationID `json:"runtime_generation_id"`
		} `json:"provenance"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal([]byte(result.Content), &got); err != nil {
		t.Fatal(err)
	}
	if got.Identity.Name != "go-testing" || got.Description != "Test Go changes." || got.Source != protocol.SkillSourceGlobal || got.Digest != got.Identity.ContentDigest || got.Provenance.CanonicalPath != got.Identity.CanonicalPath || got.Provenance.RuntimeGenerationID != "generation" || got.Content != string(content) {
		t.Fatalf("result body=%#v", got)
	}
}

func TestSkillRejectsStrictAndUnavailableInputs(t *testing.T) {
	workspace := t.TempDir()
	catalog := testCatalog(t, workspace, []byte("---\nname: go-testing\ndescription: Test Go changes.\n---\nbody\n"), protocol.SkillSourceGlobal, "")
	tool := New(catalog, output.Options{SessionID: "s", Artifacts: discardArtifacts{}})
	for _, raw := range []string{"{}", `{"name":null}`, `{"name":"go/testing"}`, `{"name":"Go-testing"}`, `{"name":"go--testing"}`, `{"name":"missing"}`, `{"name":"go-testing","extra":true}`, `{"name":"go-testing"} {}`} {
		if _, err := tool.Prepare(context.Background(), domain.ToolRequest{CallID: "c", Name: "skill", Input: json.RawMessage(raw)}); err == nil {
			t.Fatalf("input %s was accepted", raw)
		}
	}
}

func TestSkillPreviewAndRevalidationBindFrozenIdentity(t *testing.T) {
	workspace := t.TempDir()
	content := []byte("---\nname: go-testing\ndescription: Test Go changes.\n---\nbody\n")
	catalog := testCatalog(t, workspace, content, protocol.SkillSourceGlobal, "")
	prepared, err := New(catalog, output.Options{SessionID: "s", Artifacts: discardArtifacts{}}).Prepare(context.Background(), domain.ToolRequest{CallID: "c", Name: "skill", Input: json.RawMessage(`{"name":"go-testing"}`)})
	if err != nil {
		t.Fatal(err)
	}
	first := prepared.Preview()
	second, err := prepared.(interface {
		Revalidate(context.Context) (domain.PreparedToolRequest, error)
	}).Revalidate(context.Background())
	if err != nil || first.CanonicalScope != second.CanonicalScope || len(first.Resources) != 1 || first.Resources[0].Digest[:7] != "sha256:" || !strings.Contains(first.CanonicalScope, "generation") {
		t.Fatalf("first=%#v second=%#v err=%v", first, second, err)
	}
	attrs := first.Resources[0].Attributes
	if !slicesEqual(attrs, []protocol.ResourceAttribute{{Name: "runtime_generation", Value: "generation"}, {Name: "source", Value: "global"}, {Name: "workspace_id", Value: ""}}) {
		t.Fatalf("attrs=%#v", attrs)
	}
}

func TestSkillOutputIsBoundedAndRedactsRegisteredSecrets(t *testing.T) {
	workspace := t.TempDir()
	content := []byte("---\nname: go-testing\ndescription: Test Go changes.\n---\n" + strings.Repeat("secret c2VjcmV0 ", 7000))
	catalog := testCatalog(t, workspace, content, protocol.SkillSourceGlobal, "")
	registry := secret.NewRegistry()
	lease, err := registry.Acquire("generation", [][]byte{[]byte("secret")})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	artifacts := &recordingArtifacts{}
	prepared, err := New(catalog, output.Options{SessionID: "s", Artifacts: artifacts, Admission: lease}).Prepare(context.Background(), domain.ToolRequest{CallID: "c", Name: "skill", Input: json.RawMessage(`{"name":"go-testing"}`)})
	if err != nil {
		t.Fatal(err)
	}
	result := prepared.Execute(context.Background())
	if result.Status != domain.ToolSucceeded || len(result.ArtifactIDs) != 1 || len(result.Content) > output.ModelExcerptBytes || strings.Contains(result.Content, "secret") || strings.Contains(result.Content, "c2VjcmV0") || strings.Contains(string(artifacts.data), "secret") || strings.Contains(string(artifacts.data), "c2VjcmV0") {
		t.Fatalf("result=%#v", result)
	}
}

func TestSkillClosedAdmissionAndCancellationFailClosed(t *testing.T) {
	workspace := t.TempDir()
	catalog := testCatalog(t, workspace, []byte("---\nname: go-testing\ndescription: Test Go changes.\n---\nbody\n"), protocol.SkillSourceGlobal, "")
	registry := secret.NewRegistry()
	lease, err := registry.Acquire("generation", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	prepared, err := New(catalog, output.Options{SessionID: "s", Artifacts: discardArtifacts{}, Admission: lease}).Prepare(context.Background(), domain.ToolRequest{CallID: "closed", Name: "skill", Input: json.RawMessage(`{"name":"go-testing"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if result := prepared.Execute(context.Background()); result.Status != domain.ToolFailed || result.Content == "" {
		t.Fatalf("closed lease result=%#v", result)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	prepared, err = New(catalog, output.Options{SessionID: "s", Artifacts: discardArtifacts{}}).Prepare(context.Background(), domain.ToolRequest{CallID: "cancelled", Name: "skill", Input: json.RawMessage(`{"name":"go-testing"}`)})
	if err != nil {
		t.Fatal(err)
	}
	result := prepared.Execute(ctx)
	if result.Status != domain.ToolCancelled || result.ErrorKind != domain.ErrorCancelled || result.Content != "" || len(result.ArtifactIDs) != 0 {
		t.Fatalf("cancelled result=%#v", result)
	}
}

func TestSkillProjectShadowAndInactiveCatalogEntries(t *testing.T) {
	workspace := t.TempDir()
	global := []byte("---\nname: go-testing\ndescription: Global.\n---\nglobal body\n")
	project := []byte("---\nname: go-testing\ndescription: Project.\n---\nproject body\n")
	makeCatalog := func(policy config.ProjectSkillPolicy) skills.Catalog {
		t.Helper()
		candidates := []skills.Candidate{testCandidate(t, workspace, global, protocol.SkillSourceGlobal, ""), testCandidate(t, workspace, project, protocol.SkillSourceProject, "workspace")}
		catalog, err := skills.Build(skills.BuildOptions{Discovery: skills.Discovery{Candidates: candidates}, Policy: policy, Workspace: domain.Workspace{ID: "workspace", CanonicalPath: workspace}, Generation: "generation"})
		if err != nil {
			t.Fatal(err)
		}
		return catalog
	}
	prepared, err := New(makeCatalog(config.ProjectSkillsAllow), output.Options{SessionID: "s", Artifacts: discardArtifacts{}}).Prepare(context.Background(), domain.ToolRequest{CallID: "c", Name: "skill", Input: json.RawMessage(`{"name":"go-testing"}`)})
	if err != nil {
		t.Fatal(err)
	}
	result := prepared.Execute(context.Background())
	if !strings.Contains(result.Content, "project body") || strings.Contains(result.Content, "global body") || !strings.Contains(result.Content, `"source":"project"`) {
		t.Fatalf("shadow result=%s", result.Content)
	}
	for _, policy := range []config.ProjectSkillPolicy{config.ProjectSkillsAsk, config.ProjectSkillsDeny} {
		catalog := makeCatalog(policy)
		if loaded, ok := catalog.Load("go-testing"); !ok || loaded.Identity.Source != protocol.SkillSourceGlobal {
			t.Fatalf("policy=%s loaded=%#v ok=%v", policy, loaded, ok)
		}
	}
}

func TestSkillLoadsCatalogExactlyOnceAndNeverAfterPrepare(t *testing.T) {
	workspace := t.TempDir()
	base := testCatalog(t, workspace, []byte("---\nname: go-testing\ndescription: Test Go changes.\n---\nfrozen body\n"), protocol.SkillSourceGlobal, "")
	counting := &countingCatalog{Catalog: base}
	prepared, err := New(counting, output.Options{SessionID: "s", Artifacts: discardArtifacts{}}).Prepare(context.Background(), domain.ToolRequest{CallID: "c", Name: "skill", Input: json.RawMessage(`{"name":"go-testing"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if counting.loads != 1 {
		t.Fatalf("prepare loads=%d", counting.loads)
	}
	if _, err := prepared.(interface {
		Revalidate(context.Context) (domain.PreparedToolRequest, error)
	}).Revalidate(context.Background()); err != nil {
		t.Fatal(err)
	}
	result := prepared.Execute(context.Background())
	if counting.loads != 1 || result.Status != domain.ToolSucceeded || !strings.Contains(result.Content, "frozen body") {
		t.Fatalf("loads=%d result=%#v", counting.loads, result)
	}
}

func TestSkillDescriptorAndClassificationAreCanonical(t *testing.T) {
	workspace := t.TempDir()
	tool := New(testCatalog(t, workspace, []byte("---\nname: go-testing\ndescription: Test Go changes.\n---\nbody\n"), protocol.SkillSourceGlobal, ""), output.Options{SessionID: "s", Artifacts: discardArtifacts{}})
	descriptor := tool.Descriptor()
	if string(descriptor.InputSchema) != `{"type":"object","additionalProperties":false,"required":["name"],"properties":{"name":{"type":"string","pattern":"^[a-z0-9]+(?:-[a-z0-9]+)*$"}}}` || descriptor.Name != "skill" || descriptor.Mutation != domain.MutationReadOnly {
		t.Fatalf("descriptor=%#v", descriptor)
	}
	canonical := tool.CanonicalDescriptor()
	if !reflect.DeepEqual(canonical, BuiltinDescriptor()) || canonical.Body.Identity != (protocol.ToolIdentity{Source: "builtin", Authority: "yordam", Name: "skill"}) || tool.TrustedClassification().Effect != "observation" || tool.TrustedClassification().Mutation != "read_only" {
		t.Fatalf("canonical=%#v classification=%#v", canonical, tool.TrustedClassification())
	}
}

func slicesEqual(left, right []protocol.ResourceAttribute) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

type countingCatalog struct {
	skills.Catalog
	loads int
}

func (c *countingCatalog) Load(name string) (skills.LoadedSkill, bool) {
	c.loads++
	return c.Catalog.Load(name)
}

func testCandidate(t *testing.T, workspace string, content []byte, source protocol.SkillSource, workspaceID protocol.WorkspaceID) skills.Candidate {
	t.Helper()
	sum := sha256.Sum256(content)
	path := "/skills/global/go-testing/SKILL.md"
	if source == protocol.SkillSourceProject {
		path = workspace + "/.yordam/skills/go-testing/SKILL.md"
	}
	description := "Global."
	if source == protocol.SkillSourceProject {
		description = "Project."
	}
	return skills.Candidate{Name: "go-testing", Description: description, Content: content, Source: source, CanonicalPath: path, WorkspaceID: workspaceID, ContentDigest: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: hex.EncodeToString(sum[:])}}
}

func testCatalog(t *testing.T, workspace string, content []byte, source protocol.SkillSource, workspaceID protocol.WorkspaceID) skills.Catalog {
	t.Helper()
	sum := sha256.Sum256(content)
	name := "go-testing"
	path := "/skills/global/go-testing/SKILL.md"
	if source == protocol.SkillSourceProject {
		path = workspace + "/.yordam/skills/go-testing/SKILL.md"
	}
	catalog, err := skills.Build(skills.BuildOptions{Discovery: skills.Discovery{Candidates: []skills.Candidate{{Name: name, Description: "Test Go changes.", Content: content, Source: source, CanonicalPath: path, WorkspaceID: workspaceID, ContentDigest: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: hex.EncodeToString(sum[:])}}}}, Policy: config.ProjectSkillsAllow, Workspace: domain.Workspace{ID: "workspace", CanonicalPath: workspace}, Generation: "generation"})
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

type discardArtifacts struct{}

func (discardArtifacts) Put(_ context.Context, sessionID, mediaType string, source io.Reader, limit int64) (domain.Artifact, error) {
	_, err := io.Copy(io.Discard, source)
	return domain.Artifact{ID: "artifact", SessionID: sessionID, MediaType: mediaType}, err
}
func (discardArtifacts) Open(context.Context, domain.Artifact) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}

type recordingArtifacts struct{ data []byte }

func (s *recordingArtifacts) Put(_ context.Context, sessionID, mediaType string, source io.Reader, limit int64) (domain.Artifact, error) {
	value, err := io.ReadAll(io.LimitReader(source, limit+1))
	s.data = append([]byte(nil), value...)
	return domain.Artifact{ID: "artifact", SessionID: sessionID, MediaType: mediaType, Size: int64(len(value)), Truncated: int64(len(value)) > limit}, err
}
func (*recordingArtifacts) Open(context.Context, domain.Artifact) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}
