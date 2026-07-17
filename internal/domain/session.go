package domain

import "time"

type Workspace struct {
	ID            string `json:"id"`
	CanonicalPath string `json:"canonical_path"`
}

type Session struct {
	ID        string         `json:"id"`
	Workspace Workspace      `json:"workspace"`
	Title     string         `json:"title"`
	Mode      PermissionMode `json:"mode"`
	Selection ModelSelection `json:"selection"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	LastSeq   uint64         `json:"last_seq"`
}

type SessionSummary struct {
	ID        string         `json:"id"`
	Title     string         `json:"title"`
	Mode      PermissionMode `json:"mode"`
	UpdatedAt time.Time      `json:"updated_at"`
}

type SessionReplay struct {
	Session      Session
	Events       []DurableEvent
	RecoveryNote string
	ReadOnly     bool
}

type Artifact struct {
	ID        string `json:"id"`
	SessionID string `json:"session_id"`
	MediaType string `json:"media_type"`
	Path      string `json:"path"`
	Size      int64  `json:"size"`
	Truncated bool   `json:"truncated"`
}
