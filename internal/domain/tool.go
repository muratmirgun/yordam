package domain

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/muratmirgun/yordam/internal/protocol"
)

type MutationKind string

const (
	MutationReadOnly MutationKind = "read_only"
	MutationFile     MutationKind = "file"
	MutationProcess  MutationKind = "process"
)

var toolNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

type ToolDescriptor struct {
	Name             string          `json:"name"`
	Description      string          `json:"description"`
	ScopeDescription string          `json:"scope_description"`
	InputSchema      json.RawMessage `json:"input_schema"`
	Mutation         MutationKind    `json:"mutation"`
}

// ToolClassification is supplied by a trusted adapter. Provider-authored
// descriptor annotations are deliberately not used for authorization.
type ToolClassification struct {
	Effect               string
	Mutation             string
	ExecutionLoci        []string
	Boundary             string
	Reversibility        string
	VerificationCoverage string
	Idempotency          string
	Retry                string
	RequestedProfile     string
	EffectiveProfile     string
}

func (c ToolClassification) Validate() error {
	if c.Effect == "" || c.Mutation == "" || len(c.ExecutionLoci) == 0 || c.Boundary == "" || c.Reversibility == "" || c.VerificationCoverage == "" || c.Idempotency == "" || c.Retry == "" || c.RequestedProfile == "" || c.EffectiveProfile == "" {
		return fmt.Errorf("tool classification is incomplete")
	}
	for _, locus := range c.ExecutionLoci {
		if locus == "" {
			return fmt.Errorf("tool execution locus is empty")
		}
	}
	return nil
}

func (d ToolDescriptor) Validate() error {
	if !toolNamePattern.MatchString(d.Name) {
		return fmt.Errorf("invalid tool name %q", d.Name)
	}
	if d.Description == "" || d.ScopeDescription == "" || !json.Valid(d.InputSchema) {
		return fmt.Errorf("invalid descriptor for %q", d.Name)
	}
	switch d.Mutation {
	case MutationReadOnly, MutationFile, MutationProcess:
		return nil
	default:
		return fmt.Errorf("invalid mutation kind %q", d.Mutation)
	}
}

type ToolRequest struct {
	CallID    string          `json:"call_id"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
	Workspace string          `json:"workspace"`
}

type FileChangePlan struct {
	CallID         string   `json:"call_id"`
	Path           string   `json:"path"`
	ExpectedSHA256 string   `json:"expected_sha256"`
	PlannedSHA256  string   `json:"planned_sha256"`
	Diff           string   `json:"diff"`
	ArtifactIDs    []string `json:"artifact_ids,omitempty"`
}

type FileChange struct {
	CallID       string   `json:"call_id"`
	Path         string   `json:"path"`
	BeforeSHA256 string   `json:"before_sha256"`
	AfterSHA256  string   `json:"after_sha256"`
	Diff         string   `json:"diff"`
	ArtifactIDs  []string `json:"artifact_ids,omitempty"`
}

type PreparedToolRequest struct {
	Request         ToolRequest               `json:"request"`
	Mutation        MutationKind              `json:"mutation"`
	CanonicalScope  string                    `json:"canonical_scope"`
	InsideWorkspace bool                      `json:"inside_workspace"`
	ProposedDiff    string                    `json:"proposed_diff,omitempty"`
	Summary         string                    `json:"summary"`
	FilePlan        *FileChangePlan           `json:"file_plan,omitempty"`
	ApprovalScope   string                    `json:"-"`
	Resources       []protocol.ResourceTarget `json:"resources,omitempty"`
}

type ToolStatus string

const (
	ToolSucceeded ToolStatus = "succeeded"
	ToolFailed    ToolStatus = "failed"
	ToolDenied    ToolStatus = "denied"
	ToolCancelled ToolStatus = "cancelled"
)

type WorkspaceChanges struct {
	IsGit       bool     `json:"is_git"`
	Status      string   `json:"status"`
	Diff        string   `json:"diff"`
	Notice      string   `json:"notice"`
	ArtifactIDs []string `json:"artifact_ids,omitempty"`
}

type ToolResult struct {
	CallID           string            `json:"call_id"`
	Status           ToolStatus        `json:"status"`
	ErrorKind        ErrorKind         `json:"error_kind,omitempty"`
	Content          string            `json:"content"`
	ArtifactIDs      []string          `json:"artifact_ids,omitempty"`
	FileChange       *FileChange       `json:"file_change,omitempty"`
	WorkspaceChanges *WorkspaceChanges `json:"workspace_changes,omitempty"`
	ExitCode         *int              `json:"exit_code,omitempty"`
	Duration         time.Duration     `json:"duration"`
	Truncated        bool              `json:"truncated"`
}

// SkillProvenance is trusted, metadata-only activity-plan provenance for a
// loaded skill. It is deliberately separate from tool output, which remains
// untrusted text and may only be expanded as the bounded/redacted result.
type SkillProvenance struct {
	Name   string               `json:"name"`
	Source protocol.SkillSource `json:"source"`
	Digest protocol.Digest      `json:"digest"`
}

func (p SkillProvenance) Validate() error {
	if !validSkillProvenanceName(p.Name) || (p.Source != protocol.SkillSourceGlobal && p.Source != protocol.SkillSourceProject) {
		return fmt.Errorf("invalid skill provenance")
	}
	return p.Digest.Validate()
}

func validSkillProvenanceName(name string) bool {
	if name == "" || name[0] == '-' || name[len(name)-1] == '-' {
		return false
	}
	previousHyphen := false
	for index := range name {
		character := name[index]
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' {
			previousHyphen = false
			continue
		}
		if character == '-' && !previousHyphen {
			previousHyphen = true
			continue
		}
		return false
	}
	return true
}

type ToolProgress struct {
	CallID    string `json:"call_id"`
	Text      string `json:"text"`
	Truncated bool   `json:"truncated"`
}
