package tui_test

import (
	"errors"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/tui"
)

func TestSkillsScreenRoutesTrustsFrozenCatalogAndGoesStaleAfterReload(t *testing.T) {
	commands := make(chan app.Command, 4)
	initial := tuiSkillSnapshot('a')
	model := tui.NewModel(tui.Options{Commands: commands, Skills: initial})
	model = tui.SubmitForTest(model, "/skills")
	if got := model.ScreenForTest(); got != "skills" {
		t.Fatalf("screen=%q", got)
	}
	view := model.View().Content
	for _, forbidden := range []string{"/private/workspace", "PRIVATE-SKILL-BODY"} {
		if strings.Contains(view, forbidden) {
			t.Fatalf("skills view exposed %q:\n%s", forbidden, view)
		}
	}

	model = tui.PressForTest(model, "a")
	got := tui.CommandsForTest(commands)
	if len(got) != 1 || got[0].Kind != app.CommandTrustSkillCatalog || got[0].SkillTrust.WorkspaceID != initial.WorkspaceID || got[0].SkillTrust.CatalogDigest != initial.CatalogDigest || got[0].SkillTrust.Decision != string(config.ProjectSkillsAllow) {
		t.Fatalf("trust command=%+v", got)
	}

	changed := tuiSkillSnapshot('b')
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventReloadCompleted, Applied: true, Skills: &changed})
	model = tui.PressForTest(model, "d")
	if got := tui.CommandsForTest(commands); len(got) != 0 {
		t.Fatalf("stale screen sent=%+v", got)
	}
	if view := model.View().Content; !strings.Contains(view, "Catalog changed") {
		t.Fatalf("stale view=%s", view)
	}
	model = tui.PressForTest(model, "esc")
	if got := model.ScreenForTest(); got != "conversation" {
		t.Fatalf("esc screen=%q", got)
	}
}

func TestSkillsReloadWithSameCatalogDigestRefreshesStateButFailurePreservesDisplay(t *testing.T) {
	initial := tuiSkillSnapshot('a')
	model := tui.NewModel(tui.Options{Skills: initial})
	model = tui.SubmitForTest(model, "/skills")
	activated := initial.Clone()
	activated.Discovered[0].State = protocol.SkillStateActive
	activated.Active = []app.SkillViewRecord{activated.Discovered[0]}
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventReloadCompleted, Applied: true, Skills: &activated})
	if view := model.View().Content; !strings.Contains(view, "active") || strings.Contains(view, "awaiting-trust") {
		t.Fatalf("same digest reload did not refresh display:\n%s", view)
	}
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventReloadCompleted, Err: errors.New("reload failed")})
	if view := model.View().Content; !strings.Contains(view, "active") || strings.Contains(view, "awaiting-trust") {
		t.Fatalf("failed reload replaced displayed catalog:\n%s", view)
	}
}

func TestSkillToolCardProjectsOnlySafeProvenance(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.UpdateForTest(model, tea.WindowSizeMsg{Width: 100, Height: 30})
	content := `{"identity":{"name":"go-testing","source":"project","canonical_path":"/private/workspace/.yordam/skills/go-testing/SKILL.md","workspace_id":"workspace-1","content_digest":{"algorithm":"sha256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"runtime_generation_id":"runtime-1"},"source":"project","digest":{"algorithm":"sha256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"content":"PRIVATE-SKILL-BODY"}`
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventToolCompleted, Runtime: agent.RuntimeEvent{Result: &domain.ToolResult{CallID: "call-1", Status: domain.ToolSucceeded, Content: content}, Skill: &domain.SkillProvenance{Name: "go-testing", Source: protocol.SkillSourceProject, Digest: protocol.Digest{Algorithm: "sha256", Value: strings.Repeat("a", 64)}}}})
	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 1 || blocks[0].Name != "skill go-testing [project sha256:aaaaaaa]" || strings.Contains(blocks[0].Name, "/private/workspace") || strings.Contains(blocks[0].Name, "PRIVATE-SKILL-BODY") {
		t.Fatalf("collapsed card=%+v", blocks)
	}
	view := model.View().Content
	for _, want := range []string{"TOOL skill go-testing [project sha256:aaaaaaa] [completed]"} {
		if !strings.Contains(view, want) {
			t.Fatalf("card missing %q:\n%s", want, view)
		}
	}
	if strings.Contains(view, "/private/workspace") || strings.Contains(view, "PRIVATE-SKILL-BODY") {
		t.Fatalf("skill card exposed path:\n%s", view)
	}
	model = tui.PressForTest(model, "ctrl+e")
	expanded := model.ConversationBlocksForTest()
	if len(expanded) != 1 || expanded[0].Collapsed || !strings.Contains(expanded[0].Content, "PRIVATE-SKILL-BODY") {
		t.Fatalf("expanded tool output lost body: %+v", expanded)
	}
}

func TestLookalikeToolOutputDoesNotGainSkillProvenance(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	lookalike := `{"identity":{"name":"go-testing","source":"project"},"content":"PRIVATE-SKILL-BODY"}`
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventToolCompleted, Runtime: agent.RuntimeEvent{Result: &domain.ToolResult{CallID: "call-2", Status: domain.ToolSucceeded, Content: lookalike}}})
	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 1 || blocks[0].Name != "" || strings.Contains(model.View().Content, "skill go-testing") {
		t.Fatalf("lookalike gained provenance: blocks=%+v view=%s", blocks, model.View().Content)
	}
}

func TestHostileSkillProvenanceCannotInjectCardHeading(t *testing.T) {
	model := tui.NewModel(tui.OptionsForTest())
	model = tui.ApplyAppEventForTest(model, app.Event{Kind: app.EventToolCompleted, Runtime: agent.RuntimeEvent{Result: &domain.ToolResult{CallID: "call-3", Status: domain.ToolSucceeded, Content: "bounded"}, Skill: &domain.SkillProvenance{Name: "good\nINJECTED", Source: protocol.SkillSourceProject, Digest: protocol.Digest{Algorithm: "sha256", Value: strings.Repeat("a", 64)}}}})
	blocks := model.ConversationBlocksForTest()
	if len(blocks) != 1 || blocks[0].Name != "" || strings.Contains(model.View().Content, "INJECTED") {
		t.Fatalf("hostile provenance rendered: blocks=%+v view=%s", blocks, model.View().Content)
	}
}

func tuiSkillSnapshot(digestByte byte) app.SkillSnapshot {
	digest := protocol.Digest{Algorithm: "sha256", Value: strings.Repeat(string(digestByte), 64)}
	identity := protocol.SkillIdentity{Name: "go-testing", Source: protocol.SkillSourceProject, CanonicalPath: "/private/workspace/.yordam/skills/go-testing/SKILL.md", WorkspaceID: "workspace-1", ContentDigest: digest, RuntimeGenerationID: "runtime-1"}
	descriptor := protocol.SkillDescriptor{Identity: identity, Description: "safe metadata", State: protocol.SkillStateAwaitingTrust}
	return app.NewSkillSnapshot("workspace-1", protocol.SkillCatalogSnapshot{Revision: "skills-v1", Digest: digest, Discovered: []protocol.SkillDescriptor{descriptor}}, config.ProjectSkillsAsk)
}
