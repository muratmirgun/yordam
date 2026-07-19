//go:build darwin || linux

package ptyfixture

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/secret"
)

func TestScriptedSSEFixtureIsStable(t *testing.T) {
	gotHash := sha256.Sum256([]byte(ScriptedSSE()))
	if got, want := hex.EncodeToString(gotHash[:]), "c2cabffc9c925fe2d55c53bb62e4586f63462c40d8d961402bca801342a2de56"; got != want {
		t.Fatalf("scripted SSE hash = %s, want %s", got, want)
	}
}

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
