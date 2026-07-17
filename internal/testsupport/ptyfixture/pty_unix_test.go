//go:build darwin || linux

package ptyfixture

import (
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/secret"
)

func TestSecretDiagnosticFormattingRedactsConfiguredSentinel(t *testing.T) {
	const sentinel = "pty-diagnostic-secret-sentinel"
	session := &Session{diagnosticRedactor: secret.New(sentinel).String}
	session.output.WriteString("ordinary terminal output " + sentinel)

	formatted := session.formatDiagnostic("timed out; output=%q", session.Output())
	if strings.Contains(formatted, sentinel) {
		t.Fatal("secret-bearing PTY diagnostic contains the configured sentinel")
	}
	if !strings.Contains(formatted, "ordinary terminal output") || !strings.Contains(formatted, "[REDACTED]") {
		t.Fatal("secret-bearing PTY diagnostic discarded useful redacted output")
	}
}
