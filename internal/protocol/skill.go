package protocol

import (
	"fmt"
	"path"
	"strings"
)

type SkillSource string

const (
	SkillSourceGlobal  SkillSource = "global"
	SkillSourceProject SkillSource = "project"
)

const (
	SkillStateActive        = "active"
	SkillStateDenied        = "denied"
	SkillStateAwaitingTrust = "awaiting-trust"
	SkillStateInvalid       = "invalid"
	SkillStateShadowed      = "shadowed"
)

type SkillIdentity struct {
	Name                string              `json:"name"`
	Source              SkillSource         `json:"source"`
	CanonicalPath       string              `json:"canonical_path"`
	WorkspaceID         WorkspaceID         `json:"workspace_id,omitempty"`
	ContentDigest       Digest              `json:"content_digest"`
	RuntimeGenerationID RuntimeGenerationID `json:"runtime_generation_id"`
}

func (i SkillIdentity) Validate() error {
	if !validSkillName(i.Name) || !validSkillSource(i.Source) || !validCanonicalSkillPath(i.CanonicalPath) || i.RuntimeGenerationID == "" {
		return fmt.Errorf("invalid skill identity")
	}
	if (i.Source == SkillSourceProject) != (i.WorkspaceID != "") {
		return fmt.Errorf("invalid skill workspace binding")
	}
	if err := i.ContentDigest.Validate(); err != nil {
		return fmt.Errorf("skill content digest: %w", err)
	}
	return nil
}

func validSkillSource(source SkillSource) bool {
	return source == SkillSourceGlobal || source == SkillSourceProject
}

func validSkillName(name string) bool {
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

func validCanonicalSkillPath(value string) bool {
	return path.IsAbs(value) && path.Clean(value) == value
}

type SkillDescriptor struct {
	Identity    SkillIdentity  `json:"identity"`
	Description string         `json:"description"`
	State       string         `json:"state"`
	Shadows     *SkillIdentity `json:"shadows,omitempty"`
}

func (d SkillDescriptor) Validate() error {
	if err := ValidateBounds(d); err != nil {
		return err
	}
	if err := d.Identity.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(d.Description) == "" || !validSkillState(d.State) {
		return fmt.Errorf("invalid skill descriptor")
	}
	if d.Shadows == nil {
		return nil
	}
	if err := d.Shadows.Validate(); err != nil {
		return fmt.Errorf("shadow identity: %w", err)
	}
	if d.Identity.Name != d.Shadows.Name || d.Identity.Source != SkillSourceProject || d.Shadows.Source != SkillSourceGlobal || d.Identity == *d.Shadows {
		return fmt.Errorf("invalid skill shadow relationship")
	}
	if d.State != SkillStateActive {
		return fmt.Errorf("only active skills may shadow another skill")
	}
	return nil
}

func validSkillState(state string) bool {
	switch state {
	case SkillStateActive, SkillStateDenied, SkillStateAwaitingTrust, SkillStateInvalid, SkillStateShadowed:
		return true
	default:
		return false
	}
}

type SkillCatalogSnapshot struct {
	Revision    string            `json:"revision"`
	Digest      Digest            `json:"digest"`
	Active      []SkillDescriptor `json:"active"`
	Discovered  []SkillDescriptor `json:"discovered"`
	Diagnostics []Diagnostic      `json:"diagnostics"`
}

func (s SkillCatalogSnapshot) Validate() error {
	if err := ValidateBounds(s); err != nil {
		return err
	}
	if strings.TrimSpace(s.Revision) == "" {
		return fmt.Errorf("skill catalog revision is required")
	}
	if err := s.Digest.Validate(); err != nil {
		return fmt.Errorf("skill catalog digest: %w", err)
	}
	if len(s.Active) > MaxActiveSkills {
		return fmt.Errorf("skill catalog exceeds %d active skills", MaxActiveSkills)
	}
	discovered := make(map[skillSourceName]SkillDescriptor, len(s.Discovered))
	runtimeGenerationID := RuntimeGenerationID("")
	if err := validateSkillDescriptorList(s.Discovered, "discovered", func(descriptor SkillDescriptor) error {
		key := skillSourceName{source: descriptor.Identity.Source, name: descriptor.Identity.Name}
		if _, duplicate := discovered[key]; duplicate {
			return fmt.Errorf("duplicate discovered skill %s/%s", key.source, key.name)
		}
		if runtimeGenerationID == "" {
			runtimeGenerationID = descriptor.Identity.RuntimeGenerationID
		} else if descriptor.Identity.RuntimeGenerationID != runtimeGenerationID {
			return fmt.Errorf("discovered skills span multiple runtime generations")
		}
		discovered[key] = descriptor
		return nil
	}); err != nil {
		return err
	}
	activeNames := make(map[string]struct{}, len(s.Active))
	active := make(map[skillSourceName]struct{}, len(s.Active))
	if err := validateSkillDescriptorList(s.Active, "active", func(descriptor SkillDescriptor) error {
		key := skillSourceName{source: descriptor.Identity.Source, name: descriptor.Identity.Name}
		if descriptor.State != SkillStateActive {
			return fmt.Errorf("active catalog contains non-active skill")
		}
		if _, duplicate := activeNames[key.name]; duplicate {
			return fmt.Errorf("duplicate active skill name %s", key.name)
		}
		found, exists := discovered[key]
		if !exists || !sameSkillDescriptor(found, descriptor) {
			return fmt.Errorf("active skill is not an exact discovered descriptor")
		}
		activeNames[key.name] = struct{}{}
		active[key] = struct{}{}
		return nil
	}); err != nil {
		return err
	}
	for key, descriptor := range discovered {
		_, isActive := active[key]
		if (descriptor.State == SkillStateActive) != isActive {
			return fmt.Errorf("skill active list does not match discovered state")
		}
		if descriptor.Shadows == nil {
			if descriptor.State == SkillStateShadowed && !hasActiveShadowTarget(discovered, descriptor.Identity) {
				return fmt.Errorf("shadowed skill has no active shadow")
			}
			continue
		}
		targetKey := skillSourceName{source: descriptor.Shadows.Source, name: descriptor.Shadows.Name}
		target, exists := discovered[targetKey]
		if !exists || target.Identity != *descriptor.Shadows || target.State != SkillStateShadowed {
			return fmt.Errorf("skill shadow target is not a matching shadowed descriptor")
		}
	}
	return validateSkillDiagnostics(s.Diagnostics)
}

type skillSourceName struct {
	source SkillSource
	name   string
}

func validateSkillDescriptorList(values []SkillDescriptor, label string, validate func(SkillDescriptor) error) error {
	previous := ""
	for _, descriptor := range values {
		if err := descriptor.Validate(); err != nil {
			return fmt.Errorf("%s skill descriptor: %w", label, err)
		}
		key := skillDescriptorSortKey(descriptor)
		if key <= previous && previous != "" {
			return fmt.Errorf("%s skills must be sorted and unique", label)
		}
		previous = key
		if err := validate(descriptor); err != nil {
			return err
		}
	}
	return nil
}

func skillDescriptorSortKey(descriptor SkillDescriptor) string {
	identity := descriptor.Identity
	return string(identity.Source) + "\x00" + identity.Name + "\x00" + identity.ContentDigest.Algorithm + "\x00" + identity.ContentDigest.Value
}

func sameSkillDescriptor(left, right SkillDescriptor) bool {
	if left.Identity != right.Identity || left.Description != right.Description || left.State != right.State {
		return false
	}
	if left.Shadows == nil || right.Shadows == nil {
		return left.Shadows == nil && right.Shadows == nil
	}
	return *left.Shadows == *right.Shadows
}

func hasActiveShadowTarget(discovered map[skillSourceName]SkillDescriptor, target SkillIdentity) bool {
	count := 0
	for _, descriptor := range discovered {
		if descriptor.State == SkillStateActive && descriptor.Shadows != nil && *descriptor.Shadows == target {
			count++
		}
	}
	return count == 1
}

func validateSkillDiagnostics(values []Diagnostic) error {
	previous := ""
	for _, diagnostic := range values {
		key := diagnostic.Code + "\x00" + diagnostic.Message + "\x00" + string(diagnostic.Journal.Kind) + "\x00" + string(diagnostic.Journal.ID) + "\x00" + string(diagnostic.EventID)
		if key <= previous && previous != "" {
			return fmt.Errorf("skill diagnostics must be sorted and unique")
		}
		previous = key
	}
	return nil
}

type ProjectSkillTrustChangedV1 struct {
	WorkspaceID   WorkspaceID `json:"workspace_id"`
	CatalogDigest Digest      `json:"catalog_digest"`
	Decision      string      `json:"decision"`
}

func (p ProjectSkillTrustChangedV1) Validate() error {
	if err := ValidateBounds(p); err != nil {
		return err
	}
	if p.WorkspaceID == "" || (p.Decision != "allow" && p.Decision != "deny") {
		return fmt.Errorf("invalid project skill trust decision")
	}
	if err := p.CatalogDigest.Validate(); err != nil {
		return fmt.Errorf("project skill trust catalog digest: %w", err)
	}
	return nil
}
