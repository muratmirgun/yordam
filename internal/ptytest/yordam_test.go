//go:build darwin || linux

package ptytest

import (
	"bytes"
	"encoding/json"
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
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/testsupport/ptyfixture"
)

const ptyCredential = "pty-key-value"

func TestCleanEnvironmentScrubsTemplateCredentialAndKeepsArbitraryFixtureKey(t *testing.T) {
	cleaned := cleanEnvironment([]string{
		"HOME=/inherited",
		"TERM=unsafe",
		"OPENAI_API_KEY=inherited-template-credential",
		"PTY_ARBITRARY_KEY=preserved",
	}, "/isolated-home")
	joined := strings.Join(cleaned, "\n")
	if strings.Contains(joined, "OPENAI_API_KEY=") {
		t.Fatal("clean PTY environment retained the template credential")
	}
	if !strings.Contains(joined, "PTY_ARBITRARY_KEY=preserved") || !strings.Contains(joined, "HOME=/isolated-home") {
		t.Fatal("clean PTY environment discarded an arbitrary fixture key or isolated HOME")
	}
}

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
	const environmentSecret = "first-run-template-secret"
	workspace := t.TempDir()
	home := t.TempDir()
	dataDir := t.TempDir()

	session := startYordamWithEnvironment(t, workspace, home, []string{"OPENAI_API_KEY=" + environmentSecret}, []string{environmentSecret}, "--data-dir", dataDir)
	resizePTY(t, session, 220)
	session.waitFor(t, "Ask Yordam", 3*time.Second)
	session.waitFor(t, ".config/yordam/config.jsonc", 3*time.Second)
	session.waitFor(t, "/reload", 3*time.Second)
	session.write(t, string([]byte{3}))
	session.waitForExit(t, 3*time.Second)
	session.assertRestored(t)

	configPath := filepath.Join(home, ".config", "yordam", "config.jsonc")
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		`"$schema": "` + config.SchemaURL + `"`,
		`"model": "openai/your-model-id"`,
		`"baseURL": "https://api.openai.com/v1"`,
		`"apiKeyEnv": "OPENAI_API_KEY"`,
	} {
		if !strings.Contains(string(raw), expected) {
			t.Fatal("generated config is missing a required template field")
		}
	}
	if strings.Contains(string(raw), environmentSecret) {
		t.Fatal("generated config contains an environment secret")
	}
	if strings.Contains(session.outputString(), environmentSecret) {
		t.Fatal("PTY output contains an environment secret")
	}
	assertTreeOmits(t, home, environmentSecret)
	assertTreeOmits(t, dataDir, environmentSecret)
	if strings.Contains(string(raw), `"apiKey"`) {
		t.Fatal("generated config contains an unsupported raw credential field")
	}
	assertFileMode(t, filepath.Dir(configPath), 0o700)
	assertFileMode(t, configPath, 0o600)
}

func TestEditAndReloadPreservesSessionAndSecretBoundaries(t *testing.T) {
	const reloadSecret = "reload-secret"
	const responseText = "assistant response after reload"
	fixture := fmt.Sprintf("data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\ndata: [DONE]\n\n", responseText)
	server, requests := newRecordingSSEServer(t, fixture, reloadSecret)
	defer server.Close()
	workspace := t.TempDir()
	home := t.TempDir()
	dataDir := t.TempDir()
	debugLog := filepath.Join(t.TempDir(), "debug.jsonl")
	configPath := filepath.Join(home, ".config", "yordam", "config.jsonc")

	session := startYordamWithEnvironment(t, workspace, home, []string{"YORDAM_API_KEY=" + reloadSecret}, []string{reloadSecret},
		"--data-dir", dataDir,
		"--debug-log", debugLog,
	)
	resizePTY(t, session, 220)
	session.waitFor(t, "Ask Yordam", 3*time.Second)
	session.waitFor(t, "/reload", 3*time.Second)
	identityBefore := singleSessionIdentity(t, dataDir)
	replaceConfigAtomically(t, configPath, jsoncConfig(server.URL+"/v1", "YORDAM_API_KEY", "openai", "test-model"))
	reloadOffset := session.OutputOffset()
	session.write(t, "/reload\r")
	session.WaitForAfter(t, reloadOffset, "openai/test-model", 3*time.Second)
	session.WaitForAfter(t, reloadOffset, "configuration reloaded", 3*time.Second)
	session.write(t, "hello after reload\r")
	session.waitFor(t, responseText, 3*time.Second)
	session.WaitForQuiet(t, 300*time.Millisecond, 3*time.Second)
	if got := requests.Load(); got != 1 {
		t.Fatalf("provider requests=%d want=1", got)
	}
	if identityAfter := singleSessionIdentity(t, dataDir); identityAfter != identityBefore {
		t.Fatalf("session identity changed across reload")
	}
	session.write(t, string([]byte{3}))
	session.waitForExit(t, 3*time.Second)
	session.assertRestored(t)

	if strings.Contains(session.outputString(), reloadSecret) {
		t.Fatal("PTY output contains the reload credential")
	}
	assertTreeOmits(t, home, reloadSecret)
	assertTreeOmits(t, dataDir, reloadSecret)
	assertTreeOmits(t, filepath.Dir(debugLog), reloadSecret)
	assertTreeContains(t, dataDir, "hello after reload")
	assertTreeContains(t, dataDir, responseText)
}

func TestFailedReloadKeepsOldRuntimeAndMissingKeyAppliesStateWithoutRequest(t *testing.T) {
	const oldSecret = "old-runtime-secret"
	const oldResponse = "old runtime still answers"
	fixture := fmt.Sprintf("data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\ndata: [DONE]\n\n", oldResponse)
	server, requests := newRecordingSSEServer(t, fixture, oldSecret)
	defer server.Close()
	workspace := t.TempDir()
	home := t.TempDir()
	dataDir := t.TempDir()
	debugLog := filepath.Join(t.TempDir(), "debug.jsonl")
	configPath := filepath.Join(home, ".config", "yordam", "config.jsonc")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		t.Fatal(err)
	}
	replaceConfigAtomically(t, configPath, jsoncConfig(server.URL+"/v1", "OLD_RELOAD_KEY", "old", "stable-model"))

	session := startYordamWithEnvironment(t, workspace, home, []string{"OLD_RELOAD_KEY=" + oldSecret}, []string{oldSecret},
		"--data-dir", dataDir,
		"--debug-log", debugLog,
	)
	resizePTY(t, session, 220)
	session.waitFor(t, "old/stable-model", 3*time.Second)
	identityBefore := singleSessionIdentity(t, dataDir)

	replaceConfigAtomically(t, configPath, "{\n  \"model\":,\n}\n")
	invalidOffset := session.OutputOffset()
	session.write(t, "/reload\r")
	session.WaitForAfter(t, invalidOffset, "invalid JSONC", 3*time.Second)
	statusOffset := session.OutputOffset()
	resizePTY(t, session, 219)
	session.WaitForAfter(t, statusOffset, "old/stable-model", 3*time.Second)
	session.write(t, "prompt after invalid reload\r")
	session.waitFor(t, oldResponse, 3*time.Second)
	session.WaitForQuiet(t, 300*time.Millisecond, 3*time.Second)
	if got := requests.Load(); got != 1 {
		t.Fatalf("old runtime provider requests=%d want=1", got)
	}

	resizePTY(t, session, 360)
	replaceConfigAtomically(t, configPath, jsoncConfig(server.URL+"/v1", "MISSING_RELOAD_KEY", "missing", "new-model"))
	missingOffset := session.OutputOffset()
	session.write(t, "/reload\r")
	session.WaitForAfter(t, missingOffset, "missing/new-model", 3*time.Second)
	session.WaitForAfter(t, missingOffset, "configuration reloaded", 3*time.Second)
	session.WaitForAfter(t, missingOffset, "restart Yordam", 3*time.Second)

	const restoredDraft = "draft restored after missing key"
	debugOffset := debugLogSize(t, debugLog)
	session.write(t, restoredDraft+"\r")
	waitForDebugAppErrorAfter(t, debugLog, debugOffset, 3*time.Second)
	redrawOffset := session.OutputOffset()
	resizePTY(t, session, 121)
	session.WaitForAfter(t, redrawOffset, restoredDraft, 3*time.Second)
	if got := requests.Load(); got != 1 {
		t.Fatalf("missing-key runtime made a provider request: requests=%d", got)
	}
	if identityAfter := singleSessionIdentity(t, dataDir); identityAfter != identityBefore {
		t.Fatal("session identity changed across failed or missing-key reload")
	}
	session.write(t, string([]byte{3}))
	session.waitForExit(t, 3*time.Second)
	session.assertRestored(t)
	if strings.Contains(session.outputString(), oldSecret) {
		t.Fatal("PTY output contains the old runtime credential")
	}
	assertTreeOmits(t, home, oldSecret)
	assertTreeOmits(t, dataDir, oldSecret)
	assertTreeOmits(t, filepath.Dir(debugLog), oldSecret)
	assertTreeOmits(t, dataDir, restoredDraft)
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
	return startYordamWithEnvironment(t, workspace, home, nil, []string{ptyCredential}, arguments...)
}

func startYordamWithEnvironment(t *testing.T, workspace, home string, extraEnvironment, secrets []string, arguments ...string) *ptySession {
	t.Helper()
	environment := append(cleanEnvironment(os.Environ(), home), extraEnvironment...)
	redactor := secret.New(secrets...)
	return &ptySession{Session: ptyfixture.StartRedacted(t, redactor.String, buildYordam(t), workspace, environment, arguments...)}
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
			t.Error("provider request used an unexpected path")
			http.NotFound(response, request)
			return
		}
		response.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(response, fixture)
	}))
}

func newRecordingSSEServer(t *testing.T, fixture, expectedCredential string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	requests := &atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/chat/completions" {
			t.Error("provider request used an unexpected path")
			http.NotFound(response, request)
			return
		}
		if request.Header.Get("Authorization") != "Bearer "+expectedCredential {
			t.Error("provider request did not use the configured credential")
		}
		requests.Add(1)
		response.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(response, fixture)
	}))
	return server, requests
}

func writeConfig(t *testing.T, home, baseURL, keyEnvironment string) {
	t.Helper()
	path := filepath.Join(home, ".config", "yordam", "config.jsonc")
	body := jsoncConfig(baseURL+"/v1", keyEnvironment, "default", "test-model")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(keyEnvironment, ptyCredential)
}

func jsoncConfig(baseURL, keyEnvironment, provider, model string) string {
	return fmt.Sprintf(`{
  "$schema": %q,
  // PTY fixture exercises comments and trailing commas.
  "model": %q,
  "provider": {
    %q: {
      "options": {
        "baseURL": %q,
        "apiKeyEnv": %q,
      },
      "models": {%q: {}},
    },
  },
  "limits": {
    "maxToolCalls": 32,
    "shellTimeoutSeconds": 120,
  },
}
`, config.SchemaURL, provider+"/"+model, provider, baseURL, keyEnvironment, model)
}

func replaceConfigAtomically(t *testing.T, path, body string) {
	t.Helper()
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	temporary, err := os.CreateTemp(directory, ".pty-config-*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		t.Fatal(err)
	}
	if _, err := temporary.WriteString(body); err != nil {
		_ = temporary.Close()
		t.Fatal(err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		t.Fatal(err)
	}
	if err := temporary.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		t.Fatal(err)
	}
}

func debugLogSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal("cannot inspect debug log")
	}
	return info.Size()
}

func waitForDebugAppErrorAfter(t *testing.T, path string, offset int64, timeout time.Duration) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		raw, err := os.ReadFile(path)
		if err == nil {
			if int64(len(raw)) < offset {
				t.Fatal("debug log was replaced while waiting for an app error")
			}
			for _, line := range bytes.Split(raw[offset:], []byte{'\n'}) {
				var event struct {
					Event string `json:"event"`
					Kind  string `json:"kind"`
				}
				if json.Unmarshal(line, &event) == nil && event.Event == "app_event" && event.Kind == "error" {
					return
				}
			}
		} else if !os.IsNotExist(err) {
			t.Fatal("cannot read debug log while waiting for an app error")
		}
		select {
		case <-deadline.C:
			t.Fatal("timed out waiting for a new debug app error event")
		case <-ticker.C:
		}
	}
}

func singleSessionIdentity(t *testing.T, dataDir string) string {
	t.Helper()
	workspaces, err := os.ReadDir(filepath.Join(dataDir, "workspaces"))
	if err != nil {
		t.Fatal(err)
	}
	workspaces = onlyDirectories(workspaces)
	if len(workspaces) != 1 {
		t.Fatalf("workspace data directories=%d want=1", len(workspaces))
	}
	sessions, err := os.ReadDir(filepath.Join(dataDir, "workspaces", workspaces[0].Name(), "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	sessions = onlyDirectories(sessions)
	if len(sessions) != 1 {
		t.Fatalf("session data directories=%d want=1", len(sessions))
	}
	return workspaces[0].Name() + "/" + sessions[0].Name()
}

func onlyDirectories(entries []os.DirEntry) []os.DirEntry {
	directories := entries[:0]
	for _, entry := range entries {
		if entry.IsDir() {
			directories = append(directories, entry)
		}
	}
	return directories
}

func resizePTY(t *testing.T, session *ptySession, columns uint16) {
	t.Helper()
	if err := pty.Setsize(session.Terminal(), &pty.Winsize{Rows: 32, Cols: columns}); err != nil {
		t.Fatal(err)
	}
	if err := session.Command().Process.Signal(syscall.SIGWINCH); err != nil {
		t.Fatal(err)
	}
}

func cleanEnvironment(environment []string, home string) []string {
	blocked := map[string]bool{
		"OPENAI_API_KEY":  true,
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
	scanFailed := false
	forbidden := false
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			if err != nil {
				scanFailed = true
				return filepath.SkipAll
			}
			return nil
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			scanFailed = true
			return filepath.SkipAll
		}
		if strings.Contains(string(raw), value) {
			forbidden = true
			return filepath.SkipAll
		}
		return nil
	})
	if scanFailed {
		t.Fatal("secret tree scan failed")
	}
	if forbidden {
		t.Fatal("secret tree scan found a forbidden value")
	}
}
