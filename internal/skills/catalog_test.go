package skills

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func catalogDigest(raw []byte) protocol.Digest {
	sum := sha256.Sum256(raw)
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: fmtHex(sum[:])}
}

func catalogCandidate(source protocol.SkillSource, name, description, body string) Candidate {
	content := validSkill(name, description, body)
	candidate := Candidate{Name: name, Description: description, Content: content, Source: source, CanonicalPath: "/skills/" + string(source) + "/" + name + "/SKILL.md", ContentDigest: catalogDigest(content)}
	if source == protocol.SkillSourceProject {
		candidate.WorkspaceID = "workspace"
		candidate.CanonicalPath = "/workspace/.yordam/skills/" + name + "/SKILL.md"
	}
	return candidate
}

func catalogOptions(candidates []Candidate, policy config.ProjectSkillPolicy, trust *TrustState) BuildOptions {
	return BuildOptions{Discovery: Discovery{Candidates: candidates}, Policy: policy, Trust: trust, Workspace: domain.Workspace{ID: "workspace", CanonicalPath: "/workspace"}, Generation: "generation"}
}

func TestBuildResolvesPolicyTrustAndShadowing(t *testing.T) {
	global := catalogCandidate(protocol.SkillSourceGlobal, "go-testing", "global", "global body\n")
	project := catalogCandidate(protocol.SkillSourceProject, "go-testing", "project", "project body\n")
	plainProject := catalogCandidate(protocol.SkillSourceProject, "project-only", "project only", "body\n")
	base := catalogOptions([]Candidate{global, project, plainProject}, config.ProjectSkillsAsk, nil)
	projectDigest, err := ProjectCatalogDigest(base.Discovery.Candidates)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name       string
		policy     config.ProjectSkillPolicy
		trust      *TrustState
		wantStates map[protocol.SkillSource]string
		wantActive []string
	}{
		{"allow", config.ProjectSkillsAllow, nil, map[protocol.SkillSource]string{protocol.SkillSourceGlobal: protocol.SkillStateShadowed, protocol.SkillSourceProject: protocol.SkillStateActive}, []string{"go-testing", "project-only"}},
		{"deny", config.ProjectSkillsDeny, nil, map[protocol.SkillSource]string{protocol.SkillSourceGlobal: protocol.SkillStateActive, protocol.SkillSourceProject: protocol.SkillStateDenied}, []string{"go-testing"}},
		{"ask nil", config.ProjectSkillsAsk, nil, map[protocol.SkillSource]string{protocol.SkillSourceGlobal: protocol.SkillStateActive, protocol.SkillSourceProject: protocol.SkillStateAwaitingTrust}, []string{"go-testing"}},
		{"ask exact allow", config.ProjectSkillsAsk, &TrustState{WorkspaceID: "workspace", CatalogDigest: projectDigest, Decision: config.ProjectSkillsAllow, Cursor: validTrustCursor()}, map[protocol.SkillSource]string{protocol.SkillSourceGlobal: protocol.SkillStateShadowed, protocol.SkillSourceProject: protocol.SkillStateActive}, []string{"go-testing", "project-only"}},
		{"ask exact deny", config.ProjectSkillsAsk, &TrustState{WorkspaceID: "workspace", CatalogDigest: projectDigest, Decision: config.ProjectSkillsDeny, Cursor: validTrustCursor()}, map[protocol.SkillSource]string{protocol.SkillSourceGlobal: protocol.SkillStateActive, protocol.SkillSourceProject: protocol.SkillStateDenied}, []string{"go-testing"}},
		{"ask stale", config.ProjectSkillsAsk, &TrustState{WorkspaceID: "workspace", CatalogDigest: catalogDigest([]byte("stale")), Decision: config.ProjectSkillsAllow, Cursor: validTrustCursor()}, map[protocol.SkillSource]string{protocol.SkillSourceGlobal: protocol.SkillStateActive, protocol.SkillSourceProject: protocol.SkillStateAwaitingTrust}, []string{"go-testing"}},
		{"ask wrong workspace", config.ProjectSkillsAsk, &TrustState{WorkspaceID: "other", CatalogDigest: projectDigest, Decision: config.ProjectSkillsAllow, Cursor: trustCursor("other", 1)}, map[protocol.SkillSource]string{protocol.SkillSourceGlobal: protocol.SkillStateActive, protocol.SkillSourceProject: protocol.SkillStateAwaitingTrust}, []string{"go-testing"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			options := base
			options.Policy, options.Trust = test.policy, test.trust
			catalog, err := Build(options)
			if err != nil {
				t.Fatal(err)
			}
			snapshot := catalog.Snapshot()
			if snapshot.Digest != projectDigest {
				t.Fatalf("trust-neutral digest = %#v, want %#v", snapshot.Digest, projectDigest)
			}
			var active []string
			for _, descriptor := range snapshot.Active {
				active = append(active, descriptor.Identity.Name)
			}
			if !reflect.DeepEqual(active, test.wantActive) {
				t.Fatalf("active = %#v, want %#v", active, test.wantActive)
			}
			for _, descriptor := range snapshot.Discovered {
				if descriptor.Identity.Name == "go-testing" && descriptor.State != test.wantStates[descriptor.Identity.Source] {
					t.Fatalf("%s state = %q", descriptor.Identity.Source, descriptor.State)
				}
			}
		})
	}
}

func TestBuildDigestRevisionValidationAndImmutability(t *testing.T) {
	project := catalogCandidate(protocol.SkillSourceProject, "go-testing", "description", "body\n")
	global := catalogCandidate(protocol.SkillSourceGlobal, "global", "description", "body\n")
	options := catalogOptions([]Candidate{project, global}, config.ProjectSkillsAllow, nil)
	first, err := Build(options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Build(options)
	if err != nil {
		t.Fatal(err)
	}
	if first.Snapshot().Revision != second.Snapshot().Revision {
		t.Fatal("same inputs produced different revision")
	}
	if first.Snapshot().Digest != second.Snapshot().Digest {
		t.Fatal("same inputs produced different digest")
	}
	changedMetadata := options
	changedMetadata.Discovery.Candidates = append([]Candidate(nil), options.Discovery.Candidates...)
	changedMetadata.Discovery.Candidates[0].Description = "changed"
	changedMetadata.Discovery.Candidates[0].Content = validSkill("go-testing", "changed", "body\n")
	changedMetadata.Discovery.Candidates[0].ContentDigest = catalogDigest(changedMetadata.Discovery.Candidates[0].Content)
	changed, err := Build(changedMetadata)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Snapshot().Digest == first.Snapshot().Digest {
		t.Fatal("changed metadata did not change project digest")
	}
	projectDigestInput := []Candidate{catalogCandidate(protocol.SkillSourceProject, "go-testing", "description", "body\n")}
	projectDigestChangedPath := append([]Candidate(nil), projectDigestInput...)
	projectDigestChangedPath[0].CanonicalPath = "/workspace/.yordam/skills/else/SKILL.md"
	beforePath, err := ProjectCatalogDigest(projectDigestInput)
	if err != nil {
		t.Fatal(err)
	}
	afterPath, err := ProjectCatalogDigest(projectDigestChangedPath)
	if err != nil {
		t.Fatal(err)
	}
	if beforePath == afterPath {
		t.Fatal("project path did not change trust-neutral digest")
	}
	projectPath := catalogOptions([]Candidate{catalogCandidate(protocol.SkillSourceProject, "go-testing", "description", "body\n")}, config.ProjectSkillsAsk, nil)
	projectPathAllow := projectPath
	projectPathAllow.Policy = config.ProjectSkillsAllow
	projectAsk, err := Build(projectPath)
	if err != nil {
		t.Fatal(err)
	}
	projectAllow, err := Build(projectPathAllow)
	if err != nil {
		t.Fatal(err)
	}
	if projectAsk.Snapshot().Digest != projectAllow.Snapshot().Digest || projectAsk.Snapshot().Revision == projectAllow.Snapshot().Revision {
		t.Fatal("trust resolution changed digest or did not change revision")
	}
	options.Discovery.Candidates[0].Content[0] = 'x'
	loaded, ok := first.Load("go-testing")
	if !ok || !bytes.Contains(loaded.Content, []byte("name: go-testing")) {
		t.Fatal("catalog retained caller content alias")
	}
	loaded.Content[0] = 'x'
	loadedAgain, _ := first.Load("go-testing")
	if loadedAgain.Content[0] == 'x' {
		t.Fatal("loaded content aliases catalog")
	}
	snapshot := first.Snapshot()
	snapshot.Active[0].Description = "changed"
	if first.Snapshot().Active[0].Description == "changed" {
		t.Fatal("snapshot aliases catalog")
	}
	metadata := first.Metadata()
	metadata[0].Description = "changed"
	if first.Metadata()[0].Description == "changed" {
		t.Fatal("metadata aliases catalog")
	}
	for _, name := range []string{"missing", "../go-testing", "/go-testing", "go-testing/extra"} {
		if _, ok := first.Load(name); ok {
			t.Fatalf("Load(%q) unexpectedly succeeded", name)
		}
	}
}

func TestBuildRejectsForgedDuplicateAndOverActiveCandidates(t *testing.T) {
	forged := catalogCandidate(protocol.SkillSourceGlobal, "go-testing", "description", "body\n")
	forged.ContentDigest = catalogDigest([]byte("forged"))
	if _, err := Build(catalogOptions([]Candidate{forged}, config.ProjectSkillsAsk, nil)); err == nil {
		t.Fatal("forged digest accepted")
	}
	duplicate := catalogCandidate(protocol.SkillSourceGlobal, "go-testing", "description", "body\n")
	if _, err := Build(catalogOptions([]Candidate{duplicate, duplicate}, config.ProjectSkillsAsk, nil)); err == nil {
		t.Fatal("duplicate source/name accepted")
	}
	for _, mutate := range []func(*Candidate){
		func(value *Candidate) { value.Name = "wrong" },
		func(value *Candidate) { value.Description = "wrong" },
		func(value *Candidate) { value.Source = "wrong" },
		func(value *Candidate) { value.CanonicalPath = "relative" },
		func(value *Candidate) { value.WorkspaceID = "wrong" },
	} {
		candidate := catalogCandidate(protocol.SkillSourceProject, "go-testing", "description", "body\n")
		mutate(&candidate)
		if _, err := Build(catalogOptions([]Candidate{candidate}, config.ProjectSkillsAsk, nil)); err == nil {
			t.Fatal("forged candidate binding accepted")
		}
	}
	noGeneration := catalogOptions([]Candidate{catalogCandidate(protocol.SkillSourceGlobal, "go-testing", "description", "body\n")}, config.ProjectSkillsAsk, nil)
	noGeneration.Generation = ""
	if _, err := Build(noGeneration); err == nil {
		t.Fatal("empty generation accepted")
	}
	var candidates []Candidate
	for index := 0; index <= MaxActiveSkills; index++ {
		candidates = append(candidates, catalogCandidate(protocol.SkillSourceGlobal, "skill"+strings.Repeat("a", index), "description", "body\n"))
	}
	if _, err := Build(catalogOptions(candidates, config.ProjectSkillsAsk, nil)); err == nil {
		t.Fatal("too many active skills accepted")
	}
	// More than 128 discovered records are permitted when trust makes project
	// records unavailable and the remaining global active names stay bounded.
	var unavailable []Candidate
	for index := 0; index <= MaxActiveSkills; index++ {
		unavailable = append(unavailable, catalogCandidate(protocol.SkillSourceProject, "project"+strings.Repeat("a", index), "description", "body\n"))
	}
	unavailable = append(unavailable, catalogCandidate(protocol.SkillSourceGlobal, "global", "description", "body\n"))
	if _, err := Build(catalogOptions(unavailable, config.ProjectSkillsAsk, nil)); err != nil {
		t.Fatalf("inactive discovered overflow rejected: %v", err)
	}
}

func TestBuildRetainsInvalidDiscoveryDiagnosticsWithoutBlockingGlobal(t *testing.T) {
	global := catalogCandidate(protocol.SkillSourceGlobal, "go-testing", "description", "body\n")
	diagnostic := protocol.Diagnostic{Code: "skills.project.entry_rejected", Message: "skill discovery rejected untrusted filesystem input", Details: []byte(`{"name":"go-testing","reason":"invalid"}`)}
	options := catalogOptions([]Candidate{global}, config.ProjectSkillsAsk, nil)
	options.Discovery.Diagnostics = []protocol.Diagnostic{diagnostic}
	catalog, err := Build(options)
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Snapshot().Active) != 1 || catalog.Snapshot().Active[0].Identity.Source != protocol.SkillSourceGlobal || len(catalog.Snapshot().Diagnostics) != 1 {
		t.Fatalf("snapshot = %#v", catalog.Snapshot())
	}
}

func TestBuildRetainsDistinctDiagnosticKeys(t *testing.T) {
	global := catalogCandidate(protocol.SkillSourceGlobal, "go-testing", "description", "body\n")
	options := catalogOptions([]Candidate{global}, config.ProjectSkillsAsk, nil)
	options.Discovery.Diagnostics = []protocol.Diagnostic{{Code: "skills.global.rejected", Message: "rejected", AtSeq: 2, Details: []byte(`{"path":"two"}`)}, {Code: "skills.global.rejected", Message: "rejected", AtSeq: 1, Details: []byte(`{"path":"one"}`)}}
	catalog, err := Build(options)
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Snapshot().Diagnostics) != 2 || catalog.Snapshot().Diagnostics[0].AtSeq != 1 {
		t.Fatalf("diagnostics = %#v", catalog.Snapshot().Diagnostics)
	}
}

func TestBuildRejectsMalformedTrustState(t *testing.T) {
	project := catalogCandidate(protocol.SkillSourceProject, "go-testing", "description", "body\n")
	digest, err := ProjectCatalogDigest([]Candidate{project})
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*TrustState){
		func(value *TrustState) { value.Decision = config.ProjectSkillsAsk },
		func(value *TrustState) { value.CatalogDigest = protocol.Digest{} },
		func(value *TrustState) { value.Cursor.JournalKind = protocol.JournalSession },
		func(value *TrustState) { value.Cursor.JournalID = "other" },
		func(value *TrustState) { value.Cursor.CommitSeq = 0 },
	} {
		trust := &TrustState{WorkspaceID: "workspace", CatalogDigest: digest, Decision: config.ProjectSkillsAllow, Cursor: validTrustCursor()}
		mutate(trust)
		if _, err := Build(catalogOptions([]Candidate{project}, config.ProjectSkillsAsk, trust)); err == nil {
			t.Fatal("malformed trust accepted")
		}
	}
}

func TestBuildDoesNotReadFilesystemAfterFreezing(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "SKILL.md")
	content := validSkill("go-testing", "description", "body\n")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	candidate := catalogCandidate(protocol.SkillSourceGlobal, "go-testing", "description", "body\n")
	candidate.CanonicalPath = path
	catalog, err := Build(catalogOptions([]Candidate{candidate}, config.ProjectSkillsAsk, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, validSkill("go-testing", "description", "replacement\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, ok := catalog.Load("go-testing")
	if !ok || bytes.Contains(loaded.Content, []byte("replacement")) {
		t.Fatal("Load read filesystem after Build")
	}
}

func TestLoadOnlyReturnsFrozenActiveCanonicalNames(t *testing.T) {
	project := catalogCandidate(protocol.SkillSourceProject, "project-only", "description", "body\n")
	for _, policy := range []config.ProjectSkillPolicy{config.ProjectSkillsAsk, config.ProjectSkillsDeny} {
		catalog, err := Build(catalogOptions([]Candidate{project}, policy, nil))
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := catalog.Load("project-only"); ok {
			t.Fatalf("%s project skill loaded", policy)
		}
	}
	global := catalogCandidate(protocol.SkillSourceGlobal, "go-testing", "global", "body\n")
	shadowing := catalogCandidate(protocol.SkillSourceProject, "go-testing", "project", "body\n")
	catalog, err := Build(catalogOptions([]Candidate{global, shadowing}, config.ProjectSkillsAllow, nil))
	if err != nil {
		t.Fatal(err)
	}
	loaded, ok := catalog.Load("go-testing")
	if !ok || loaded.Identity.Source != protocol.SkillSourceProject {
		t.Fatalf("shadowed active load = %#v / %v", loaded, ok)
	}
}
