//go:build darwin || linux

package ptytest

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/muratmirgun/yordam/internal/testsupport/ptyfixture"
)

func TestVersionAndInteractiveTerminalRestoration(t *testing.T) {
	binary := buildYordam(t)
	output, err := exec.Command(binary, "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("version: output=%q err=%v", output, err)
	}
	if got, want := string(output), "yordam dev (none, unknown)\n"; got != want {
		t.Fatalf("version output=%q want=%q", got, want)
	}

	output, err = exec.Command(binary, "--unknown").CombinedOutput()
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) || exitError.ExitCode() != 2 {
		t.Fatalf("parse error: output=%q err=%v", output, err)
	}
	if got, want := string(output), "yordam: flag provided but not defined: -unknown\n"; got != want {
		t.Fatalf("parse output=%q want=%q", got, want)
	}
}

func TestFirstRunCreatesTemplateAndRestoresTerminal(t *testing.T) {
	workspace := t.TempDir()
	home := t.TempDir()
	dataDir := t.TempDir()

	session := startYordam(t, workspace, home, "--data-dir", dataDir)
	session.waitFor(t, "Ask Yordam", 3*time.Second)
	session.waitFor(t, "Created", 3*time.Second)
	session.write(t, string([]byte{3}))
	session.waitForExit(t, 3*time.Second)
	session.assertRestored(t)

	configPath := filepath.Join(home, ".config", "yordam", "config.jsonc")
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"$schema"`) || !strings.Contains(string(raw), `"model": "openai/your-model-id"`) || !strings.Contains(string(raw), `"apiKeyEnv": "OPENAI_API_KEY"`) {
		t.Fatalf("generated config=%s", raw)
	}
	assertFileMode(t, filepath.Dir(configPath), 0o700)
	assertFileMode(t, configPath, 0o600)
}

func TestScriptedConversationResizeAndCleanExit(t *testing.T) {
	fixture := readSSEFixture(t)
	server := newSSEServer(t, fixture)
	defer server.Close()
	workspace := t.TempDir()
	home := t.TempDir()
	writeConfig(t, home, server.URL, "PTY_CONVERSATION_KEY")
	dataDir := t.TempDir()

	session := startYordam(t, workspace, home, "--data-dir", dataDir)
	session.waitFor(t, "mode: ask", 3*time.Second)
	session.write(t, "hello from PTY\r")
	session.waitFor(t, "scripted PTY response", 3*time.Second)
	session.resetOutput()
	if err := pty.Setsize(session.Terminal(), &pty.Winsize{Rows: 24, Cols: 80}); err != nil {
		t.Fatal(err)
	}
	if err := session.Command().Process.Signal(syscall.SIGWINCH); err != nil {
		t.Fatal(err)
	}
	session.waitFor(t, "~", 3*time.Second)
	session.write(t, string([]byte{3}))
	session.waitForExit(t, 3*time.Second)
	session.assertRestored(t)

	assertTreeContains(t, dataDir, "hello from PTY")
	assertTreeContains(t, dataDir, "scripted PTY response")
}

func TestEscCancellationTerminatesShellProcessGroup(t *testing.T) {
	workspace := t.TempDir()
	childPIDPath := filepath.Join(workspace, "child.pid")
	command := "sh -c 'trap \"\" TERM; echo $$ > child.pid; while :; do sleep 1; done'"
	arguments := fmt.Sprintf(`{"command":%q,"cwd":"."}`, command)
	sse := fmt.Sprintf("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"shell-call\",\"type\":\"function\",\"function\":{\"name\":\"shell\",\"arguments\":%q}}]}}]}\n\ndata: [DONE]\n\n", arguments)
	server := newSSEServer(t, sse)
	defer server.Close()
	home := t.TempDir()
	writeConfig(t, home, server.URL, "PTY_CANCEL_KEY")

	session := startYordam(t, workspace, home, "--data-dir", t.TempDir(), "--shell-timeout", "30s")
	session.waitFor(t, "mode: ask", 3*time.Second)
	session.write(t, "run cancellation fixture\r")
	session.waitFor(t, "PERMISSION", 3*time.Second)
	session.write(t, "y")
	childPID := waitForPID(t, childPIDPath, 3*time.Second)

	cancelStarted := time.Now()
	session.write(t, string([]byte{0x1b}))
	waitForProcessGone(t, childPID, 3*time.Second)
	if elapsed := time.Since(cancelStarted); elapsed > 3*time.Second {
		t.Fatalf("process group cancellation took %s", elapsed)
	}
	session.write(t, string([]byte{3}))
	session.waitForExit(t, 3*time.Second)
	session.assertRestored(t)
}

func TestActiveCtrlCConfirmationWaitsForShellProcessGroup(t *testing.T) {
	workspace := t.TempDir()
	childPIDPath := filepath.Join(workspace, "child.pid")
	command := "sh -c 'trap \"\" TERM; echo $$ > child.pid; while :; do sleep 1; done'"
	arguments := fmt.Sprintf(`{"command":%q,"cwd":"."}`, command)
	sse := fmt.Sprintf("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"shell-call\",\"type\":\"function\",\"function\":{\"name\":\"shell\",\"arguments\":%q}}]}}]}\n\ndata: [DONE]\n\n", arguments)
	server := newSSEServer(t, sse)
	defer server.Close()
	home := t.TempDir()
	writeConfig(t, home, server.URL, "PTY_CTRL_C_KEY")

	session := startYordam(t, workspace, home, "--data-dir", t.TempDir(), "--shell-timeout", "30s")
	session.waitFor(t, "mode: ask", 3*time.Second)
	session.write(t, "run active exit fixture\r")
	session.waitFor(t, "PERMISSION", 3*time.Second)
	session.write(t, "y")
	childPID := waitForPID(t, childPIDPath, 3*time.Second)
	session.write(t, string([]byte{3}))
	session.waitFor(t, "Cancel active turn and exit?", 3*time.Second)
	session.write(t, "y")
	waitForProcessGone(t, childPID, 3*time.Second)
	session.waitForExit(t, 3*time.Second)
	session.assertRestored(t)
}

type ptySession struct{ *ptyfixture.Session }

func startYordam(t *testing.T, workspace, home string, arguments ...string) *ptySession {
	t.Helper()
	return &ptySession{Session: ptyfixture.Start(t, buildYordam(t), workspace, cleanEnvironment(os.Environ(), home), arguments...)}
}

func (s *ptySession) write(t *testing.T, value string) {
	s.Write(t, value)
}

func (s *ptySession) waitFor(t *testing.T, value string, timeout time.Duration) {
	s.WaitFor(t, value, timeout)
}

func (s *ptySession) waitForExit(t *testing.T, timeout time.Duration) {
	s.WaitForExit(t, timeout)
}

func (s *ptySession) outputString() string {
	return s.Output()
}

func (s *ptySession) resetOutput() {
	s.ResetOutput()
}

func (s *ptySession) assertRestored(t *testing.T) {
	s.AssertRestored(t)
}

func buildYordam(t *testing.T) string {
	return ptyfixture.CachedYordam(t)
}

func repositoryRoot() string {
	return ptyfixture.RepositoryRoot()
}

func readSSEFixture(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repositoryRoot(), "internal", "tui", "testdata", "scripted-sse.txt"))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func newSSEServer(t *testing.T, fixture string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/chat/completions" {
			t.Errorf("request path=%q", request.URL.Path)
			http.NotFound(response, request)
			return
		}
		response.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(response, fixture)
	}))
}

func writeConfig(t *testing.T, home, baseURL, keyEnvironment string) {
	t.Helper()
	path := filepath.Join(home, ".config", "yordam", "config.jsonc")
	body := fmt.Sprintf(`{
  "model": "default/test-model",
  "provider": {
    "default": {
      "options": {
        "baseURL": %q,
        "apiKeyEnv": %q
      },
      "models": {"test-model": {}}
    }
  },
  "limits": {
    "maxToolCalls": 32,
    "shellTimeoutSeconds": 120
  }
}
`, baseURL+"/v1", keyEnvironment)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(keyEnvironment, "pty-key-value")
}

func cleanEnvironment(environment []string, home string) []string {
	blocked := map[string]bool{
		"YORDAM_API_KEY":  true,
		"YORDAM_BASE_URL": true,
		"YORDAM_MODEL":    true,
		"YORDAM_PROFILE":  true,
		"HOME":            true,
	}
	cleaned := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		name, _, _ := strings.Cut(entry, "=")
		if !blocked[name] && name != "TERM" {
			cleaned = append(cleaned, entry)
		}
	}
	return append(cleaned, "TERM=xterm-256color", "HOME="+home)
}

func assertFileMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode=%o want=%o", path, got, want)
	}
}

func waitForPID(t *testing.T, path string, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(raw)))
			if parseErr == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for child PID at %s", path)
	return 0
}

func waitForProcessGone(t *testing.T, pid int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("process %d survived cancellation", pid)
}

func assertTreeContains(t *testing.T, root, value string) {
	t.Helper()
	found := false
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		raw, readErr := os.ReadFile(path)
		if readErr == nil && strings.Contains(string(raw), value) {
			found = true
		}
		return readErr
	})
	if !found {
		t.Fatalf("%q was not persisted under %s", value, root)
	}
}

func assertTreeOmits(t *testing.T, root, value string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if strings.Contains(string(raw), value) {
			return fmt.Errorf("%s contains secret", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
