//go:build darwin || linux

package ptyfixture

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/charmbracelet/x/vt"
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

func TestCurrentScreenExcludesReplacedTerminalHistory(t *testing.T) {
	session := &Session{emulator: vt.NewEmulator(12, 3)}
	writer := lockedWriter{session: session}
	if _, err := writer.Write([]byte("PERMISSION")); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("\r\x1b[2KREADY")); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(session.Output(), "PERMISSION") {
		t.Fatal("raw PTY history did not retain replaced content")
	}
	if screen := session.CurrentScreen(); strings.Contains(screen, "PERMISSION") || !strings.Contains(screen, "READY") {
		t.Fatalf("current screen=%q", screen)
	}
}
