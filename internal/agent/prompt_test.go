package agent_test

import (
	"testing"

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/domain"
)

func TestComposeSystemPromptDescribesPermissionMode(t *testing.T) {
	tests := []struct {
		name string
		mode domain.PermissionMode
		want string
	}{
		{
			name: "safe",
			mode: domain.ModeSafe,
			want: "base\n\nSafe mode: read/search are limited to the workspace; edit and shell are denied. Never claim that shell is sandboxed.",
		},
		{
			name: "ask",
			mode: domain.ModeAsk,
			want: "base\n\nAsk mode: outside reads, edits, and shell require visible user approval. Never claim that shell is sandboxed.",
		},
		{
			name: "auto",
			mode: domain.ModeAuto,
			want: "base\n\nAuto mode: inside file operations may run automatically; outside file access still asks, and shell is trusted unsandboxed execution requiring session acknowledgement. Never claim that shell is sandboxed.",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := agent.ComposeSystemPrompt("base", test.mode); got != test.want {
				t.Fatalf("prompt=%q want=%q", got, test.want)
			}
		})
	}
}
