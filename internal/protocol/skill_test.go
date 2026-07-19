package protocol_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/protocol"
)

func validSkillIdentity(source protocol.SkillSource) protocol.SkillIdentity {
	identity := protocol.SkillIdentity{
		Name: "go-testing", Source: source, CanonicalPath: "/skills/go-testing/SKILL.md",
		ContentDigest: protocolDigest('a'), RuntimeGenerationID: "generation",
	}
	if source == protocol.SkillSourceProject {
		identity.WorkspaceID = "workspace"
	}
	return identity
}

func validSkillDescriptor(source protocol.SkillSource) protocol.SkillDescriptor {
	return protocol.SkillDescriptor{Identity: validSkillIdentity(source), Description: "Test Go changes.", State: protocol.SkillStateActive}
}

func TestSkillIdentityValidatesCanonicalSourceBindings(t *testing.T) {
	if err := validSkillIdentity(protocol.SkillSourceGlobal).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := validSkillIdentity(protocol.SkillSourceProject).Validate(); err != nil {
		t.Fatal(err)
	}

	invalid := []protocol.SkillIdentity{
		{Name: "Go-testing", Source: protocol.SkillSourceGlobal, CanonicalPath: "/skills/go-testing/SKILL.md", ContentDigest: protocolDigest('a'), RuntimeGenerationID: "generation"},
		{Name: "go--testing", Source: protocol.SkillSourceGlobal, CanonicalPath: "/skills/go-testing/SKILL.md", ContentDigest: protocolDigest('a'), RuntimeGenerationID: "generation"},
		{Name: "-go-testing", Source: protocol.SkillSourceGlobal, CanonicalPath: "/skills/go-testing/SKILL.md", ContentDigest: protocolDigest('a'), RuntimeGenerationID: "generation"},
		{Name: "go-testing-", Source: protocol.SkillSourceGlobal, CanonicalPath: "/skills/go-testing/SKILL.md", ContentDigest: protocolDigest('a'), RuntimeGenerationID: "generation"},
		{Name: "go-testing", Source: "remote", CanonicalPath: "/skills/go-testing/SKILL.md", ContentDigest: protocolDigest('a'), RuntimeGenerationID: "generation"},
		{Name: "go-testing", Source: protocol.SkillSourceGlobal, CanonicalPath: "skills/go-testing/SKILL.md", ContentDigest: protocolDigest('a'), RuntimeGenerationID: "generation"},
		{Name: "go-testing", Source: protocol.SkillSourceGlobal, CanonicalPath: "/skills/../go-testing/SKILL.md", ContentDigest: protocolDigest('a'), RuntimeGenerationID: "generation"},
		{Name: "go-testing", Source: protocol.SkillSourceGlobal, CanonicalPath: "/skills/go-testing/SKILL.md", WorkspaceID: "workspace", ContentDigest: protocolDigest('a'), RuntimeGenerationID: "generation"},
		{Name: "go-testing", Source: protocol.SkillSourceProject, CanonicalPath: "/skills/go-testing/SKILL.md", ContentDigest: protocolDigest('a'), RuntimeGenerationID: "generation"},
		{Name: "go-testing", Source: protocol.SkillSourceProject, CanonicalPath: "/skills/go-testing/SKILL.md", WorkspaceID: "workspace", ContentDigest: protocol.Digest{}, RuntimeGenerationID: "generation"},
		{Name: "go-testing", Source: protocol.SkillSourceProject, CanonicalPath: "/skills/go-testing/SKILL.md", WorkspaceID: "workspace", ContentDigest: protocolDigest('a')},
	}
	for index, identity := range invalid {
		if err := identity.Validate(); err == nil {
			t.Fatalf("invalid identity %d accepted: %+v", index, identity)
		}
	}
}

func TestSkillDescriptorValidatesStatesAndShadowTarget(t *testing.T) {
	project := validSkillDescriptor(protocol.SkillSourceProject)
	global := validSkillIdentity(protocol.SkillSourceGlobal)
	project.Shadows = &global
	if err := project.Validate(); err != nil {
		t.Fatal(err)
	}

	for _, state := range []string{"denied", "awaiting-trust", "invalid", "shadowed"} {
		descriptor := validSkillDescriptor(protocol.SkillSourceGlobal)
		descriptor.State = state
		if err := descriptor.Validate(); err != nil {
			t.Fatalf("state %q rejected: %v", state, err)
		}
	}

	invalid := validSkillDescriptor(protocol.SkillSourceProject)
	invalid.State = "enabled"
	if err := invalid.Validate(); err == nil {
		t.Fatal("unknown state accepted")
	}
	invalid = validSkillDescriptor(protocol.SkillSourceProject)
	self := invalid.Identity
	invalid.Shadows = &self
	if err := invalid.Validate(); err == nil {
		t.Fatal("self shadow accepted")
	}
	invalid = validSkillDescriptor(protocol.SkillSourceProject)
	otherName := validSkillIdentity(protocol.SkillSourceGlobal)
	otherName.Name = "other"
	invalid.Shadows = &otherName
	if err := invalid.Validate(); err == nil {
		t.Fatal("different-name shadow accepted")
	}
	invalid = validSkillDescriptor(protocol.SkillSourceProject)
	sameSource := invalid.Identity
	sameSource.CanonicalPath = "/elsewhere/go-testing/SKILL.md"
	invalid.Shadows = &sameSource
	if err := invalid.Validate(); err == nil {
		t.Fatal("same-source shadow accepted")
	}
	globalDescriptor := validSkillDescriptor(protocol.SkillSourceGlobal)
	projectIdentity := validSkillIdentity(protocol.SkillSourceProject)
	globalDescriptor.Shadows = &projectIdentity
	if err := globalDescriptor.Validate(); err == nil {
		t.Fatal("global skill shadowing a project skill accepted")
	}
}

func TestSkillCatalogSnapshotRequiresStableBoundedLists(t *testing.T) {
	global := validSkillDescriptor(protocol.SkillSourceGlobal)
	global.State = protocol.SkillStateShadowed
	project := validSkillDescriptor(protocol.SkillSourceProject)
	project.Shadows = &global.Identity
	catalog := protocol.SkillCatalogSnapshot{
		Revision: "skills-r1", Digest: protocolDigest('b'),
		Active:      []protocol.SkillDescriptor{project},
		Discovered:  []protocol.SkillDescriptor{global, project},
		Diagnostics: []protocol.Diagnostic{},
	}
	if err := catalog.Validate(); err != nil {
		t.Fatal(err)
	}
	decodedEquivalent := catalog
	decodedEquivalent.Active = protocol.DeepCopy(catalog.Active)
	if err := decodedEquivalent.Validate(); err != nil {
		t.Fatalf("equivalent independently decoded active descriptor rejected: %v", err)
	}
	globalActive := validSkillDescriptor(protocol.SkillSourceGlobal)
	projectActive := validSkillDescriptor(protocol.SkillSourceProject)
	crossSourceActive := protocol.SkillCatalogSnapshot{
		Revision: "skills-r1", Digest: protocolDigest('b'),
		Active:     protocol.DeepCopy([]protocol.SkillDescriptor{globalActive, projectActive}),
		Discovered: []protocol.SkillDescriptor{globalActive, projectActive},
	}
	if err := crossSourceActive.Validate(); err == nil {
		t.Fatal("active catalog accepted the same canonical name from both sources")
	}
	projectAwaitingTrust := validSkillDescriptor(protocol.SkillSourceProject)
	projectAwaitingTrust.State = protocol.SkillStateAwaitingTrust
	beforeTrust := protocol.SkillCatalogSnapshot{
		Revision: "skills-r1", Digest: protocolDigest('b'),
		Active:     []protocol.SkillDescriptor{globalActive},
		Discovered: []protocol.SkillDescriptor{globalActive, projectAwaitingTrust},
	}
	if err := beforeTrust.Validate(); err != nil {
		t.Fatalf("global active plus project awaiting trust rejected: %v", err)
	}
	mixedGeneration := beforeTrust
	mixedGeneration.Discovered = protocol.DeepCopy(beforeTrust.Discovered)
	mixedGeneration.Discovered[1].Identity.RuntimeGenerationID = "other-generation"
	if err := mixedGeneration.Validate(); err == nil {
		t.Fatal("catalog accepted discovered skills from mixed runtime generations")
	}

	unsorted := catalog
	unsorted.Discovered = []protocol.SkillDescriptor{project, global}
	if err := unsorted.Validate(); err == nil {
		t.Fatal("unsorted catalog accepted")
	}
	duplicate := catalog
	duplicate.Active = append(duplicate.Active, project)
	if err := duplicate.Validate(); err == nil {
		t.Fatal("duplicate active descriptor accepted")
	}
	ambiguous := catalog
	secondProject := project
	secondProject.Identity.CanonicalPath = "/other/go-testing/SKILL.md"
	secondProject.Identity.ContentDigest = protocolDigest('c')
	ambiguous.Discovered = append(ambiguous.Discovered, secondProject)
	if err := ambiguous.Validate(); err == nil {
		t.Fatal("ambiguous shadow target accepted")
	}
	missingRevision := catalog
	missingRevision.Revision = ""
	if err := missingRevision.Validate(); err == nil {
		t.Fatal("catalog without revision accepted")
	}
	missingDigest := catalog
	missingDigest.Digest = protocol.Digest{}
	if err := missingDigest.Validate(); err == nil {
		t.Fatal("catalog without digest accepted")
	}
	tooMany := catalog
	tooMany.Active = make([]protocol.SkillDescriptor, protocol.MaxActiveSkills+1)
	for index := range tooMany.Active {
		tooMany.Active[index] = project
		tooMany.Active[index].Identity.Name = "skill" + strings.Repeat("a", index)
	}
	if err := tooMany.Validate(); err == nil {
		t.Fatal("oversized active catalog accepted")
	}
}

func TestProjectSkillTrustChangedValidatesDurableDecision(t *testing.T) {
	valid := protocol.ProjectSkillTrustChangedV1{WorkspaceID: "workspace", CatalogDigest: protocolDigest('a'), Decision: "allow"}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	valid.Decision = "deny"
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, decision := range []string{"", "ask", "Allow"} {
		invalid := valid
		invalid.Decision = decision
		if err := invalid.Validate(); err == nil {
			t.Fatalf("decision %q accepted", decision)
		}
	}
}

func TestSkillRuntimeAndApplicationDTOsOnlyExposeMetadata(t *testing.T) {
	descriptor := validSkillDescriptor(protocol.SkillSourceGlobal)
	body := protocol.RuntimeGenerationBody{SkillCatalogRevision: "skills-r1", Skills: []protocol.SkillDescriptor{descriptor}}
	cloned := protocol.DeepCopy(body)
	cloned.Skills[0].Description = "changed"
	if body.Skills[0].Description == cloned.Skills[0].Description {
		t.Fatal("runtime body clone retained skill aliases")
	}
	projection := protocol.DurableProjection{Skills: protocol.SkillCatalogSnapshot{Revision: "skills-r1", Digest: protocolDigest('a'), Active: []protocol.SkillDescriptor{descriptor}}}
	raw, err := json.Marshal(projection)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"content":`) || strings.Contains(string(raw), "SKILL.md body") {
		t.Fatalf("skill metadata DTO leaked content: %s", raw)
	}
}
