package agent

import (
	"fmt"

	"github.com/muratmirgun/yordam/internal/domain"
)

func ComposeSystemPrompt(base string, mode domain.PermissionMode) string {
	rules := map[domain.PermissionMode]string{
		domain.ModeSafe: "Safe mode: read/search are limited to the workspace; edit and shell are denied.",
		domain.ModeAsk:  "Ask mode: outside reads, edits, and shell require visible user approval.",
		domain.ModeAuto: "Auto mode: inside file operations may run automatically; outside file access still asks, and shell is trusted unsandboxed execution requiring session acknowledgement.",
	}
	return fmt.Sprintf("%s\n\n%s Never claim that shell is sandboxed.", base, rules[mode])
}
