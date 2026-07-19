package skills

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
)

type TrustState struct {
	WorkspaceID   protocol.WorkspaceID      `json:"workspace_id"`
	CatalogDigest protocol.Digest           `json:"catalog_digest"`
	Decision      config.ProjectSkillPolicy `json:"decision"`
	Cursor        protocol.CommittedCursor  `json:"cursor"`
}

type BuildOptions struct {
	Discovery  Discovery                    `json:"discovery"`
	Policy     config.ProjectSkillPolicy    `json:"policy"`
	Trust      *TrustState                  `json:"trust,omitempty"`
	Workspace  domain.Workspace             `json:"workspace"`
	Generation protocol.RuntimeGenerationID `json:"generation"`
}

type LoadedSkill struct {
	Identity    protocol.SkillIdentity `json:"identity"`
	Description string                 `json:"description"`
	Content     []byte                 `json:"content"`
}

type Catalog interface {
	Snapshot() protocol.SkillCatalogSnapshot
	Metadata() []protocol.SkillDescriptor
	Load(name string) (LoadedSkill, bool)
}

type frozenCatalog struct {
	snapshot protocol.SkillCatalogSnapshot
	loaded   map[string]LoadedSkill
}

type catalogDigestCandidate struct {
	Name          string               `json:"name"`
	Description   string               `json:"description"`
	Source        protocol.SkillSource `json:"source"`
	CanonicalPath string               `json:"canonical_path"`
	WorkspaceID   protocol.WorkspaceID `json:"workspace_id,omitempty"`
	ContentDigest protocol.Digest      `json:"content_digest"`
}

type catalogRevisionBody struct {
	Generation  protocol.RuntimeGenerationID `json:"generation"`
	TrustDigest protocol.Digest              `json:"trust_digest"`
	Active      []protocol.SkillDescriptor   `json:"active"`
	Discovered  []protocol.SkillDescriptor   `json:"discovered"`
	Diagnostics []protocol.Diagnostic        `json:"diagnostics"`
}

// ProjectCatalogDigest is deliberately computed before policy/trust resolution.
// Its narrow preimage makes it a durable key for exactly the inspected project
// skill set, without treating global input or a resolution choice as trust data.
func ProjectCatalogDigest(candidates []Candidate) (protocol.Digest, error) {
	project := make([]catalogDigestCandidate, 0)
	for _, candidate := range candidates {
		if candidate.Source != protocol.SkillSourceProject {
			continue
		}
		project = append(project, catalogDigestCandidate{Name: candidate.Name, Description: candidate.Description, Source: candidate.Source, CanonicalPath: candidate.CanonicalPath, WorkspaceID: candidate.WorkspaceID, ContentDigest: candidate.ContentDigest})
	}
	sort.Slice(project, func(i, j int) bool { return digestCandidateKey(project[i]) < digestCandidateKey(project[j]) })
	return canonicaljson.Digest(project)
}

func Build(options BuildOptions) (Catalog, error) {
	if options.Workspace.ID == "" || !filepath.IsAbs(options.Workspace.CanonicalPath) || filepath.Clean(options.Workspace.CanonicalPath) != options.Workspace.CanonicalPath || options.Generation == "" {
		return nil, fmt.Errorf("invalid skill catalog build binding")
	}
	if options.Policy != config.ProjectSkillsAsk && options.Policy != config.ProjectSkillsAllow && options.Policy != config.ProjectSkillsDeny {
		return nil, fmt.Errorf("invalid project skill policy")
	}
	candidates, err := freezeCandidates(options.Discovery.Candidates, options.Workspace)
	if err != nil {
		return nil, err
	}
	diagnostics := sortedCatalogDiagnostics(protocol.DeepCopy(options.Discovery.Diagnostics))
	trustDigest, err := ProjectCatalogDigest(candidates)
	if err != nil {
		return nil, fmt.Errorf("digest project skills: %w", err)
	}
	if options.Trust != nil {
		if err := validateTrustState(*options.Trust); err != nil {
			return nil, err
		}
	}
	projectState := resolveProjectState(options.Policy, options.Trust, protocol.WorkspaceID(options.Workspace.ID), trustDigest)
	discovered := make([]protocol.SkillDescriptor, 0, len(candidates))
	bySourceName := make(map[string]int, len(candidates))
	for _, candidate := range candidates {
		state := protocol.SkillStateActive
		if candidate.Source == protocol.SkillSourceProject {
			state = projectState
		}
		descriptor := protocol.SkillDescriptor{Identity: candidateIdentity(candidate, options.Generation), Description: candidate.Description, State: state}
		bySourceName[sourceNameKey(candidate.Source, candidate.Name)] = len(discovered)
		discovered = append(discovered, descriptor)
	}
	for index := range discovered {
		descriptor := &discovered[index]
		if descriptor.Identity.Source != protocol.SkillSourceProject || descriptor.State != protocol.SkillStateActive {
			continue
		}
		globalIndex, exists := bySourceName[sourceNameKey(protocol.SkillSourceGlobal, descriptor.Identity.Name)]
		if !exists || discovered[globalIndex].State != protocol.SkillStateActive {
			continue
		}
		shadow := protocol.DeepCopy(discovered[globalIndex].Identity)
		descriptor.Shadows = &shadow
		discovered[globalIndex].State = protocol.SkillStateShadowed
	}
	sort.Slice(discovered, func(i, j int) bool { return descriptorKey(discovered[i]) < descriptorKey(discovered[j]) })
	active := make([]protocol.SkillDescriptor, 0, len(discovered))
	loaded := make(map[string]LoadedSkill, len(discovered))
	for _, descriptor := range discovered {
		if descriptor.State != protocol.SkillStateActive {
			continue
		}
		active = append(active, protocol.DeepCopy(descriptor))
		candidate := candidates[sourceCandidateIndex(candidates, descriptor.Identity)]
		loaded[descriptor.Identity.Name] = LoadedSkill{Identity: protocol.DeepCopy(descriptor.Identity), Description: descriptor.Description, Content: bytes.Clone(candidate.Content)}
	}
	if len(active) > MaxActiveSkills {
		return nil, fmt.Errorf("skill catalog exceeds %d active skills", MaxActiveSkills)
	}
	revisionDigest, err := canonicaljson.Digest(catalogRevisionBody{Generation: options.Generation, TrustDigest: trustDigest, Active: active, Discovered: discovered, Diagnostics: diagnostics})
	if err != nil {
		return nil, fmt.Errorf("digest resolved skill catalog: %w", err)
	}
	snapshot := protocol.SkillCatalogSnapshot{Revision: "skills-v1-" + revisionDigest.Value, Digest: trustDigest, Active: active, Discovered: discovered, Diagnostics: diagnostics}
	if err := snapshot.Validate(); err != nil {
		return nil, fmt.Errorf("validate resolved skill catalog: %w", err)
	}
	return frozenCatalog{snapshot: protocol.DeepCopy(snapshot), loaded: protocol.DeepCopy(loaded)}, nil
}

func (c frozenCatalog) Snapshot() protocol.SkillCatalogSnapshot { return protocol.DeepCopy(c.snapshot) }
func (c frozenCatalog) Metadata() []protocol.SkillDescriptor {
	return protocol.DeepCopy(c.snapshot.Active)
}
func (c frozenCatalog) Load(name string) (LoadedSkill, bool) {
	if !validName(name) || strings.Contains(name, "/") {
		return LoadedSkill{}, false
	}
	value, ok := c.loaded[name]
	return protocol.DeepCopy(value), ok
}

func freezeCandidates(input []Candidate, workspace domain.Workspace) ([]Candidate, error) {
	result := make([]Candidate, 0, len(input))
	seen := make(map[string]struct{}, len(input))
	for _, original := range input {
		candidate := protocol.DeepCopy(original)
		if candidate.Source != protocol.SkillSourceGlobal && candidate.Source != protocol.SkillSourceProject || !validName(candidate.Name) || !filepath.IsAbs(candidate.CanonicalPath) || filepath.Clean(candidate.CanonicalPath) != candidate.CanonicalPath {
			return nil, fmt.Errorf("invalid discovered skill candidate")
		}
		if candidate.Source == protocol.SkillSourceProject {
			if candidate.WorkspaceID != protocol.WorkspaceID(workspace.ID) || candidate.CanonicalPath != filepath.Join(workspace.CanonicalPath, ".yordam", "skills", candidate.Name, "SKILL.md") {
				return nil, fmt.Errorf("project skill workspace binding changed")
			}
		} else if candidate.WorkspaceID != "" {
			return nil, fmt.Errorf("global skill has workspace binding")
		}
		if err := candidate.ContentDigest.Validate(); err != nil {
			return nil, fmt.Errorf("candidate content digest: %w", err)
		}
		metadata, normalized, err := Parse(candidate.Name, candidate.Content)
		if err != nil || metadata.Name != candidate.Name || metadata.Description != candidate.Description || !bytes.Equal(normalized, candidate.Content) {
			return nil, fmt.Errorf("candidate content and metadata binding is invalid")
		}
		if candidate.ContentDigest != sha256Digest(candidate.Content) {
			return nil, fmt.Errorf("candidate content digest changed")
		}
		key := sourceNameKey(candidate.Source, candidate.Name)
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate discovered skill %s", key)
		}
		seen[key] = struct{}{}
		candidate.Content = bytes.Clone(candidate.Content)
		result = append(result, candidate)
	}
	sort.Slice(result, func(i, j int) bool { return candidateSortKey(result[i]) < candidateSortKey(result[j]) })
	return result, nil
}

func sha256Digest(value []byte) protocol.Digest {
	// Discovery uses SHA-256 over the admitted bytes. Keeping this helper local
	// avoids coupling catalog resolution to the filesystem scanner.
	sum := sha256.Sum256(value)
	return protocol.Digest{Algorithm: protocol.DigestSHA256, Value: hex.EncodeToString(sum[:])}
}

func candidateIdentity(candidate Candidate, generation protocol.RuntimeGenerationID) protocol.SkillIdentity {
	return protocol.SkillIdentity{Name: candidate.Name, Source: candidate.Source, CanonicalPath: candidate.CanonicalPath, WorkspaceID: candidate.WorkspaceID, ContentDigest: candidate.ContentDigest, RuntimeGenerationID: generation}
}

func resolveProjectState(policy config.ProjectSkillPolicy, trust *TrustState, workspace protocol.WorkspaceID, digest protocol.Digest) string {
	switch policy {
	case config.ProjectSkillsAllow:
		return protocol.SkillStateActive
	case config.ProjectSkillsDeny:
		return protocol.SkillStateDenied
	case config.ProjectSkillsAsk:
		if trust != nil && trust.WorkspaceID == workspace && trust.CatalogDigest == digest {
			if trust.Decision == config.ProjectSkillsAllow {
				return protocol.SkillStateActive
			}
			if trust.Decision == config.ProjectSkillsDeny {
				return protocol.SkillStateDenied
			}
		}
	}
	return protocol.SkillStateAwaitingTrust
}

func validateTrustState(value TrustState) error {
	if value.WorkspaceID == "" || (value.Decision != config.ProjectSkillsAllow && value.Decision != config.ProjectSkillsDeny) || value.CatalogDigest.Validate() != nil || value.Cursor.Validate() != nil || value.Cursor.JournalKind != protocol.JournalWorkspaceControl || value.Cursor.JournalID != protocol.JournalID(value.WorkspaceID) {
		return fmt.Errorf("invalid project skill trust state")
	}
	return nil
}

func digestCandidateKey(value catalogDigestCandidate) string {
	return string(value.Source) + "\x00" + value.Name + "\x00" + value.ContentDigest.Algorithm + "\x00" + value.ContentDigest.Value + "\x00" + value.Description + "\x00" + value.CanonicalPath + "\x00" + string(value.WorkspaceID)
}
func descriptorKey(value protocol.SkillDescriptor) string {
	return string(value.Identity.Source) + "\x00" + value.Identity.Name + "\x00" + value.Identity.ContentDigest.Algorithm + "\x00" + value.Identity.ContentDigest.Value
}
func sourceNameKey(source protocol.SkillSource, name string) string {
	return string(source) + "\x00" + name
}
func sourceCandidateIndex(candidates []Candidate, identity protocol.SkillIdentity) int {
	for index, candidate := range candidates {
		if candidate.Source == identity.Source && candidate.Name == identity.Name && candidate.ContentDigest == identity.ContentDigest {
			return index
		}
	}
	panic("resolved descriptor has no candidate")
}

func sortedCatalogDiagnostics(input []protocol.Diagnostic) []protocol.Diagnostic {
	sort.Slice(input, func(left, right int) bool {
		return catalogDiagnosticKey(input[left]) < catalogDiagnosticKey(input[right])
	})
	if len(input) == 0 {
		return input
	}
	result := input[:0]
	previous := ""
	for _, diagnostic := range input {
		key := catalogDiagnosticKey(diagnostic)
		if key != previous {
			result, previous = append(result, diagnostic), key
		}
	}
	return result
}

func catalogDiagnosticKey(value protocol.Diagnostic) string {
	return value.Code + "\x00" + value.Message + "\x00" + string(value.Journal.Kind) + "\x00" + string(value.Journal.ID) + "\x00" + strconv.FormatUint(value.AtSeq, 10) + "\x00" + string(value.EventID) + "\x00" + string(value.Details)
}
