package skills

import (
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
)

const (
	MaxSkillBytes   = 128 << 10
	MaxActiveSkills = 128
)

type Metadata struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type Candidate struct {
	Name          string               `json:"name"`
	Description   string               `json:"description"`
	Content       []byte               `json:"content"`
	Source        protocol.SkillSource `json:"source"`
	CanonicalPath string               `json:"canonical_path"`
	WorkspaceID   protocol.WorkspaceID `json:"workspace_id,omitempty"`
	ContentDigest protocol.Digest      `json:"content_digest"`
}

type DiscoveryOptions struct {
	Workspace    domain.Workspace             `json:"workspace"`
	GlobalRoot   string                       `json:"global_root"`
	ProjectRoot  string                       `json:"project_root"`
	GenerationID protocol.RuntimeGenerationID `json:"generation_id"`
}

type Discovery struct {
	Candidates  []Candidate           `json:"candidates"`
	Diagnostics []protocol.Diagnostic `json:"diagnostics"`
}
