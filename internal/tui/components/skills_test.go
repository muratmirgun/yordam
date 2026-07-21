package components_test

import (
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/tui/components"
)

func TestSkillsRendersFrozenMetadataWithoutBodiesOrPaths(t *testing.T) {
	snapshot := testSkillCatalog(t)
	skills := components.NewSkills(components.SkillScreenOptions{
		Snapshot: snapshot,
	})
	view := skills.View(100)
	for _, want := range []string{"SKILLS", "go-testing", "project", "awaiting-trust", "sha256:aaaaaaa", "shadows global", "duplicate skill"} {
		if !strings.Contains(view, want) {
			t.Fatalf("view missing %q:\n%s", want, view)
		}
	}
	for _, forbidden := range []string{"/private/project/.yordam/skills", "PRIVATE-SKILL-BODY", `"path"`} {
		if strings.Contains(view, forbidden) {
			t.Fatalf("view exposed %q:\n%s", forbidden, view)
		}
	}
	if strings.Count(view, "duplicate skill") != 1 || !strings.Contains(view, "duplicate skill ×2") {
		t.Fatalf("diagnostics were not deduplicated:\n%s", view)
	}
}

func TestSkillsUsesNarrowLayoutAndPolicyBoundTrustActions(t *testing.T) {
	snapshot := testSkillCatalog(t)
	skills := components.NewSkills(components.SkillScreenOptions{
		Snapshot: snapshot,
	})
	skills.Move(1) // sorted global row, then the separate awaiting project row.
	narrow := skills.View(36)
	if strings.Contains(narrow, "awaiting project guidance") || !strings.Contains(narrow, "go-testing") || !strings.Contains(narrow, "A: allow") {
		t.Fatalf("narrow view=%q", narrow)
	}
	if decision, ok := skills.TrustDecision("a"); !ok || decision != config.ProjectSkillsAllow {
		t.Fatalf("allow decision=%q ok=%t", decision, ok)
	}
	if decision, ok := skills.TrustDecision("d"); !ok || decision != config.ProjectSkillsDeny {
		t.Fatalf("deny decision=%q ok=%t", decision, ok)
	}
	// The trust record applies to the catalog digest, not an individual row.
	// An already active project catalog must still be able to reverse allow to
	// deny (or the reverse) without waiting for its files to change.
	skills.Move(1)
	if decision, ok := skills.TrustDecision("d"); !ok || decision != config.ProjectSkillsDeny {
		t.Fatalf("re-decision=%q ok=%t", decision, ok)
	}
	allow := snapshot.Clone()
	allow.ProjectPolicy = config.ProjectSkillsAllow
	deny := snapshot.Clone()
	deny.ProjectPolicy = config.ProjectSkillsDeny
	for _, option := range []components.SkillScreenOptions{
		{Snapshot: allow}, {Snapshot: deny}, {Snapshot: snapshot, Stale: true}, {Snapshot: snapshot, OperationActive: true},
	} {
		candidate := components.NewSkills(option)
		if _, ok := candidate.TrustDecision("a"); ok {
			t.Fatalf("trust unexpectedly enabled for %+v", option)
		}
	}
}

func TestSkillsUnavailableSnapshotDoesNotOfferTrust(t *testing.T) {
	skills := components.NewSkills(components.SkillScreenOptions{Snapshot: app.SkillSnapshot{WorkspaceID: "workspace-1"}})
	if _, ok := skills.TrustDecision("a"); ok || !strings.Contains(skills.View(80), "Skills are unavailable") || strings.Contains(skills.View(80), "fixed to .") {
		t.Fatalf("unavailable skills view=%q", skills.View(80))
	}
}

func testSkillCatalog(t *testing.T) app.SkillSnapshot {
	t.Helper()
	global := protocol.SkillIdentity{Name: "go-testing", Source: protocol.SkillSourceGlobal, CanonicalPath: "/global/skills/go-testing/SKILL.md", ContentDigest: skillDigest('b'), RuntimeGenerationID: "runtime-1"}
	project := protocol.SkillIdentity{Name: "go-testing", Source: protocol.SkillSourceProject, CanonicalPath: "/private/project/.yordam/skills/go-testing/SKILL.md", WorkspaceID: "workspace-1", ContentDigest: skillDigest('a'), RuntimeGenerationID: "runtime-1"}
	awaiting := protocol.SkillIdentity{Name: "awaiting", Source: protocol.SkillSourceProject, CanonicalPath: "/private/project/.yordam/skills/awaiting/SKILL.md", WorkspaceID: "workspace-1", ContentDigest: skillDigest('c'), RuntimeGenerationID: "runtime-1"}
	active := protocol.SkillDescriptor{Identity: project, Description: "project test guidance", State: protocol.SkillStateActive, Shadows: &global}
	catalog := protocol.SkillCatalogSnapshot{
		Revision: "skills-v1", Digest: skillDigest('d'),
		Discovered: []protocol.SkillDescriptor{
			{Identity: global, Description: "global test guidance", State: protocol.SkillStateShadowed},
			{Identity: awaiting, Description: "awaiting project guidance", State: protocol.SkillStateAwaitingTrust},
			active,
		},
		Active: []protocol.SkillDescriptor{active},
		Diagnostics: []protocol.Diagnostic{
			{Code: "skills.project.entry_rejected", Message: "duplicate skill", Details: []byte(`{"entry":1}`)},
			{Code: "skills.project.entry_rejected", Message: "duplicate skill", Details: []byte(`{"entry":2}`)},
		},
	}
	if err := catalog.Validate(); err != nil {
		t.Fatalf("invalid test catalog: %v", err)
	}
	snapshot := app.NewSkillSnapshot("workspace-1", catalog, config.ProjectSkillsAsk)
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("invalid safe snapshot: %v", err)
	}
	return snapshot
}

func skillDigest(character byte) protocol.Digest {
	return protocol.Digest{Algorithm: "sha256", Value: strings.Repeat(string(character), 64)}
}
