package app

import (
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/skills"
)

func TestSkillSnapshotProjectsOnlySafeMetadataAndExactTrustDigest(t *testing.T) {
	digest := protocol.Digest{Algorithm: "sha256", Value: strings.Repeat("a", 64)}
	identity := protocol.SkillIdentity{Name: "go-testing", Source: protocol.SkillSourceProject, CanonicalPath: "/private/workspace/.yordam/skills/go-testing/SKILL.md", WorkspaceID: "workspace-1", ContentDigest: digest, RuntimeGenerationID: "runtime-1"}
	descriptor := protocol.SkillDescriptor{Identity: identity, Description: "safe description", State: protocol.SkillStateAwaitingTrust}
	catalog := protocol.SkillCatalogSnapshot{Revision: "skills-v1", Digest: digest, Discovered: []protocol.SkillDescriptor{descriptor}, Diagnostics: []protocol.Diagnostic{{Code: "skills.project.entry_rejected", Message: "rejected", Details: []byte(`{"path":"/private/workspace","body":"PRIVATE-SKILL-BODY"}`)}}}
	snapshot := NewSkillSnapshot("workspace-1", catalog, config.ProjectSkillsAsk)
	if err := snapshot.Validate(); err != nil || snapshot.CatalogDigest != digest {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	raw := "" + snapshot.Discovered[0].Name + snapshot.Discovered[0].Description + snapshot.Diagnostics[0].Message
	if strings.Contains(raw, "/private/workspace") || strings.Contains(raw, "PRIVATE-SKILL-BODY") || snapshot.Discovered[0].Digest != digest {
		t.Fatalf("unsafe snapshot=%+v", snapshot)
	}
}

func TestRuntimeSkillSnapshotRedactsDisplayStringsWithoutChangingTrustDigest(t *testing.T) {
	configuredSecret := "catalog-secret"
	digest := protocol.Digest{Algorithm: "sha256", Value: strings.Repeat("a", 64)}
	identity := protocol.SkillIdentity{Name: "go-testing", Source: protocol.SkillSourceProject, CanonicalPath: "/workspace/.yordam/skills/go-testing/SKILL.md", WorkspaceID: "workspace-1", ContentDigest: digest, RuntimeGenerationID: "runtime-1"}
	descriptor := protocol.SkillDescriptor{Identity: identity, Description: "uses " + configuredSecret, State: protocol.SkillStateAwaitingTrust}
	catalog := protocol.SkillCatalogSnapshot{Revision: "skills-v1", Digest: digest, Discovered: []protocol.SkillDescriptor{descriptor}, Diagnostics: []protocol.Diagnostic{{Code: "skills.project.entry_rejected", Message: "bad " + configuredSecret}}}
	runtime := RuntimeSet{WorkspaceID: "workspace-1", ProjectSkillPolicy: config.ProjectSkillsAsk, Redactor: secret.New(configuredSecret), Skills: snapshotCatalog{snapshot: catalog}}
	snapshot := runtime.SkillSnapshot()
	if snapshot.CatalogDigest != digest || strings.Contains(snapshot.Discovered[0].Description, configuredSecret) || strings.Contains(snapshot.Diagnostics[0].Message, configuredSecret) {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}

type snapshotCatalog struct{ snapshot protocol.SkillCatalogSnapshot }

func (c snapshotCatalog) Snapshot() protocol.SkillCatalogSnapshot {
	return protocol.DeepCopy(c.snapshot)
}
func (snapshotCatalog) Load(string) (skills.LoadedSkill, bool) { return skills.LoadedSkill{}, false }
func (c snapshotCatalog) Metadata() []protocol.SkillDescriptor {
	return protocol.DeepCopy(c.snapshot.Active)
}
