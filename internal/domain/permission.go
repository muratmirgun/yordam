package domain

import "fmt"

type PermissionMode string

const (
	ModeSafe PermissionMode = "safe"
	ModeAsk  PermissionMode = "ask"
	ModeAuto PermissionMode = "auto"
)

func (m PermissionMode) Validate() error {
	switch m {
	case ModeSafe, ModeAsk, ModeAuto:
		return nil
	default:
		return fmt.Errorf("invalid permission mode %q", m)
	}
}

type PermissionAction string

const (
	PermissionAllow PermissionAction = "allow"
	PermissionAsk   PermissionAction = "ask"
	PermissionDeny  PermissionAction = "deny"
)

type PermissionLifetime string

const (
	PermissionOnce    PermissionLifetime = "once"
	PermissionSession PermissionLifetime = "session"
)

type PermissionDecision struct {
	Action       PermissionAction   `json:"action"`
	Lifetime     PermissionLifetime `json:"lifetime"`
	Scope        string             `json:"scope"`
	Reason       string             `json:"reason"`
	PolicySource string             `json:"policy_source,omitempty"`
}
