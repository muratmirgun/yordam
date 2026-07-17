package permission

import (
	"encoding/json"
	"sync"

	"github.com/muratmirgun/yordam/internal/domain"
)

type SessionPolicy struct {
	mu        sync.RWMutex
	mode      domain.PermissionMode
	grants    map[string]struct{}
	autoShell bool
}

func NewSession(mode domain.PermissionMode) *SessionPolicy {
	return &SessionPolicy{mode: mode, grants: map[string]struct{}{}}
}

func (p *SessionPolicy) SetMode(mode domain.PermissionMode) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mode == domain.ModeAuto && mode != domain.ModeAuto {
		p.autoShell = false
	}
	p.mode = mode
}

func (p *SessionPolicy) AcknowledgeAutoShell() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mode == domain.ModeAuto {
		p.autoShell = true
	}
}

func (p *SessionPolicy) GrantSession(tool, scope string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.grants[grantKey(tool, scope)] = struct{}{}
}

func Restore(replay domain.SessionReplay) *SessionPolicy {
	policy := NewSession(replay.Session.Mode)
	for _, event := range replay.Events {
		switch event.Kind {
		case domain.EventModeChanged:
			var payload domain.ModeChangedPayload
			if json.Unmarshal(event.Payload, &payload) == nil {
				policy.SetMode(payload.Mode)
			}
		case domain.EventPermissionResolved:
			var payload domain.PermissionPayload
			if json.Unmarshal(event.Payload, &payload) == nil && payload.Decision.Action == domain.PermissionAllow && payload.Decision.Lifetime == domain.PermissionSession {
				policy.GrantSession(payload.Tool, payload.Decision.Scope)
			}
		case domain.EventTrustedExecutionAcknowledged:
			var payload domain.TrustedExecutionPayload
			if json.Unmarshal(event.Payload, &payload) == nil && payload.Enabled {
				policy.AcknowledgeAutoShell()
			}
		}
	}
	return policy
}

func grantKey(tool, scope string) string {
	return tool + "\x00" + scope
}
