package domain

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"
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
	Request         ToolRequest     `json:"request"`
	Mutation        MutationKind    `json:"mutation"`
	CanonicalScope  string          `json:"canonical_scope"`
	InsideWorkspace bool            `json:"inside_workspace"`
	ProposedDiff    string          `json:"proposed_diff,omitempty"`
	Summary         string          `json:"summary"`
	FilePlan        *FileChangePlan `json:"file_plan,omitempty"`
	ApprovalScope   string          `json:"-"`
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

type ToolProgress struct {
	CallID    string `json:"call_id"`
	Text      string `json:"text"`
	Truncated bool   `json:"truncated"`
}
