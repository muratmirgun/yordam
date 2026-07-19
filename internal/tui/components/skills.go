package components

import (
	"fmt"
	"strings"

	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/protocol"
)

// SkillScreenOptions is the metadata-only, generation-bound input for the
// local skills screen. It intentionally has no filesystem handles or skill
// content: opening the screen must never trigger a fresh discovery scan.
type SkillScreenOptions struct {
	Snapshot        app.SkillSnapshot
	Stale           bool
	OperationActive bool
}

// Skills renders a frozen catalog and tracks a selected discovered skill.
type Skills struct {
	options SkillScreenOptions
	cursor  int
}

func NewSkills(options SkillScreenOptions) Skills {
	options.Snapshot = options.Snapshot.Clone()
	return Skills{options: options}
}

func (s *Skills) SetStale(stale bool)            { s.options.Stale = stale }
func (s *Skills) SetOperationActive(active bool) { s.options.OperationActive = active }

func (s *Skills) Move(delta int) {
	items := s.options.Snapshot.Discovered
	if len(items) == 0 {
		s.cursor = 0
		return
	}
	s.cursor = (s.cursor + delta + len(items)) % len(items)
}

// TrustDecision returns a durable decision for the displayed project catalog.
// A prior allow or deny remains reversible, so this is intentionally bound to
// the whole catalog rather than to an individual awaiting-trust row. Fixed
// policy, a stale screen, and an active operation cannot submit a decision.
func (s Skills) TrustDecision(key string) (config.ProjectSkillPolicy, bool) {
	if !s.canTrust() {
		return "", false
	}
	switch key {
	case "a":
		return config.ProjectSkillsAllow, true
	case "d":
		return config.ProjectSkillsDeny, true
	default:
		return "", false
	}
}

func (s Skills) canTrust() bool {
	if s.options.Snapshot.ProjectPolicy != config.ProjectSkillsAsk || s.options.Stale || s.options.OperationActive || s.options.Snapshot.WorkspaceID == "" || s.options.Snapshot.CatalogDigest.Validate() != nil {
		return false
	}
	for _, descriptor := range s.options.Snapshot.Discovered {
		if descriptor.Source == protocol.SkillSourceProject {
			return true
		}
	}
	return false
}

func (s Skills) View(width int) string {
	lines := []string{"SKILLS"}
	if s.options.Snapshot.CatalogDigest.Validate() != nil || s.options.Snapshot.Revision == "" {
		return strings.Join(append(lines, "Skills are unavailable until configuration reloads.", "Esc/Enter: return to conversation"), "\n")
	}
	if len(s.options.Snapshot.Discovered) == 0 {
		lines = append(lines, "No discovered skills.")
	} else {
		for index, descriptor := range s.options.Snapshot.Discovered {
			marker := "  "
			if index == min(max(s.cursor, 0), len(s.options.Snapshot.Discovered)-1) {
				marker = "> "
			}
			line := marker + descriptor.Name + " [" + string(descriptor.Source) + "] " + descriptor.State + " " + shortDigest(descriptor.Digest)
			if descriptor.Shadows != nil {
				line += " shadows " + string(*descriptor.Shadows)
			}
			lines = append(lines, line)
			if width >= 72 {
				lines = append(lines, "    "+descriptor.Description)
			}
		}
	}
	if diagnostics := skillDiagnostics(s.options.Snapshot.Diagnostics); len(diagnostics) > 0 {
		lines = append(lines, "Diagnostics:")
		for _, diagnostic := range diagnostics {
			lines = append(lines, "- "+diagnostic)
		}
	}
	if s.canTrust() {
		lines = append(lines, "A: allow project catalog | D: deny project catalog")
	} else if s.options.Stale {
		lines = append(lines, "Catalog changed; reopen /skills before deciding.")
	} else if s.options.OperationActive {
		lines = append(lines, "Trust actions are unavailable while an operation is active.")
	} else if s.options.Snapshot.ProjectPolicy == config.ProjectSkillsAllow || s.options.Snapshot.ProjectPolicy == config.ProjectSkillsDeny {
		lines = append(lines, "Project skill policy is fixed to "+string(s.options.Snapshot.ProjectPolicy)+".")
	}
	return strings.Join(lines, "\n")
}

func skillDiagnostics(diagnostics []app.SkillDiagnosticView) []string {
	result := make([]string, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		message := strings.TrimSpace(diagnostic.Message)
		if message == "" {
			message = strings.TrimSpace(diagnostic.Code)
		}
		if message == "" {
			continue
		}
		if diagnostic.Count > 1 {
			result = append(result, fmt.Sprintf("%s ×%d", message, diagnostic.Count))
			continue
		}
		result = append(result, message)
	}
	return result
}

func shortDigest(digest protocol.Digest) string {
	if digest.Algorithm == "" || digest.Value == "" {
		return "digest:unknown"
	}
	value := digest.Value
	if len(value) > 7 {
		value = value[:7]
	}
	return fmt.Sprintf("%s:%s", digest.Algorithm, value)
}
