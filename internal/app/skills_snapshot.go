package app

import (
	"fmt"
	"strings"

	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/protocol"
)

// SkillViewRecord is a deliberately metadata-only catalog record. It is not a
// protocol SkillDescriptor: canonical paths and workspace-internal identity
// fields are intentionally absent at the UI boundary.
type SkillViewRecord struct {
	Name        string                `json:"name"`
	Source      protocol.SkillSource  `json:"source"`
	Digest      protocol.Digest       `json:"digest"`
	Description string                `json:"description"`
	State       string                `json:"state"`
	Shadows     *protocol.SkillSource `json:"shadows,omitempty"`
}

type SkillDiagnosticView struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Count   int    `json:"count"`
}

// SkillSnapshot is the safe, frozen catalog seam for local UI consumers. The
// CatalogDigest is the exact durable trust binding; no filesystem path, skill
// body, or diagnostic detail crosses this boundary.
type SkillSnapshot struct {
	WorkspaceID   protocol.WorkspaceID      `json:"workspace_id"`
	CatalogDigest protocol.Digest           `json:"catalog_digest"`
	Revision      string                    `json:"revision"`
	Active        []SkillViewRecord         `json:"active"`
	Discovered    []SkillViewRecord         `json:"discovered"`
	Diagnostics   []SkillDiagnosticView     `json:"diagnostics"`
	ProjectPolicy config.ProjectSkillPolicy `json:"project_policy"`
}

func NewSkillSnapshot(workspaceID protocol.WorkspaceID, catalog protocol.SkillCatalogSnapshot, policy config.ProjectSkillPolicy) SkillSnapshot {
	snapshot := SkillSnapshot{
		WorkspaceID: workspaceID, CatalogDigest: protocol.DeepCopy(catalog.Digest), Revision: catalog.Revision,
		Active: projectSkillRecords(catalog.Active), Discovered: projectSkillRecords(catalog.Discovered),
		Diagnostics: projectSkillDiagnostics(catalog.Diagnostics), ProjectPolicy: policy,
	}
	return snapshot
}

func (s SkillSnapshot) Clone() SkillSnapshot { return protocol.DeepCopy(s) }

func (s SkillSnapshot) Validate() error {
	if s.WorkspaceID == "" || strings.TrimSpace(s.Revision) == "" || s.CatalogDigest.Validate() != nil {
		return fmt.Errorf("invalid skill snapshot binding")
	}
	if s.ProjectPolicy != config.ProjectSkillsAsk && s.ProjectPolicy != config.ProjectSkillsAllow && s.ProjectPolicy != config.ProjectSkillsDeny {
		return fmt.Errorf("invalid skill snapshot policy")
	}
	for _, record := range append(append([]SkillViewRecord(nil), s.Active...), s.Discovered...) {
		if record.Name == "" || record.Digest.Validate() != nil || strings.TrimSpace(record.Description) == "" {
			return fmt.Errorf("invalid skill snapshot record")
		}
	}
	return nil
}

func projectSkillRecords(descriptors []protocol.SkillDescriptor) []SkillViewRecord {
	result := make([]SkillViewRecord, len(descriptors))
	for index, descriptor := range descriptors {
		result[index] = SkillViewRecord{Name: descriptor.Identity.Name, Source: descriptor.Identity.Source, Digest: protocol.DeepCopy(descriptor.Identity.ContentDigest), Description: descriptor.Description, State: descriptor.State}
		if descriptor.Shadows != nil {
			shadow := descriptor.Shadows.Source
			result[index].Shadows = &shadow
		}
	}
	return result
}

func projectSkillDiagnostics(diagnostics []protocol.Diagnostic) []SkillDiagnosticView {
	indices := make(map[string]int, len(diagnostics))
	result := make([]SkillDiagnosticView, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		message := strings.TrimSpace(diagnostic.Message)
		if message == "" {
			message = strings.TrimSpace(diagnostic.Code)
		}
		if message == "" {
			continue
		}
		key := diagnostic.Code + "\x00" + message
		if index, found := indices[key]; found {
			result[index].Count++
			continue
		}
		indices[key] = len(result)
		result = append(result, SkillDiagnosticView{Code: diagnostic.Code, Message: message, Count: 1})
	}
	return result
}
