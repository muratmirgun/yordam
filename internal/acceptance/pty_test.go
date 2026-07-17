//go:build acceptance

package acceptance_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/testsupport/ptyfixture"
)

func acceptSingleBinaryStartup(t *testing.T) {
	const startupSecret = "v010-startup-template-secret"
	binary := ptyfixture.BuildYordam(t, t.TempDir())
	info, err := os.Stat(binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("built executable info=%v err=%v", info, err)
	}
	output, err := exec.Command(binary, "--version").CombinedOutput()
	if err != nil || !strings.HasPrefix(string(output), "yordam dev ") {
		t.Fatalf("version output=%q err=%v", output, err)
	}

	fresh := t.TempDir()
	home := t.TempDir()
	dataDir := filepath.Join(fresh, "data")
	session := ptyfixture.StartRedacted(t, secret.New(startupSecret).String, binary, fresh, cleanPTYEnvironment(home, []string{"YORDAM_API_KEY=" + startupSecret}),
		"--data-dir", dataDir,
	)
	session.WaitFor(t, "Ask Yordam", 3*time.Second)
	session.Write(t, string([]byte{3}))
	session.WaitForExit(t, 3*time.Second)
	session.AssertRestored(t)

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
			t.Fatal("first-run config is missing a required template field")
		}
	}
	if strings.Contains(string(raw), `"apiKey"`) || strings.Contains(string(raw), startupSecret) || strings.Contains(session.Output(), startupSecret) {
		t.Fatal("first-run output persisted a credential value or raw-key field")
	}
	assertAcceptanceMode(t, filepath.Dir(configPath), 0o700)
	assertAcceptanceMode(t, configPath, 0o600)
	if identity := singleAcceptanceSessionIdentity(t, dataDir); identity == "" {
		t.Fatal("single-binary startup did not create a session")
	}
}

func acceptSecretFreeConfigReload(t *testing.T) {
	const sentinel = "v010-first-run-secret"
	const responseText = "acceptance response after reload"
	server, requests := newAcceptanceSSEServer(t, fmt.Sprintf("data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\ndata: [DONE]\n\n", responseText), sentinel)
	defer server.Close()
	root := t.TempDir()
	home := t.TempDir()
	configPath := filepath.Join(home, ".config", "yordam", "config.jsonc")
	dataDir := filepath.Join(root, "data")
	debugLog := filepath.Join(root, "debug", "debug.jsonl")
	environment := cleanPTYEnvironment(home, []string{"YORDAM_API_KEY=" + sentinel})
	session := ptyfixture.StartRedacted(t, secret.New(sentinel).String, ptyfixture.CachedYordam(t), root, environment,
		"--data-dir", dataDir,
		"--debug-log", debugLog,
	)
	session.WaitFor(t, "Ask Yordam", 3*time.Second)
	identityBefore := singleAcceptanceSessionIdentity(t, dataDir)
	if err := config.SaveGlobal(configPath, acceptanceConfig(server.URL+"/v1", "YORDAM_API_KEY", map[string][]string{"default": {"acceptance-model"}})); err != nil {
		t.Fatal(err)
	}
	reloadOffset := session.OutputOffset()
	session.Write(t, "/reload\r")
	session.WaitForAfter(t, reloadOffset, "default/acceptance-model", 3*time.Second)
	session.WaitForAfter(t, reloadOffset, "configuration reloaded", 3*time.Second)
	session.Write(t, "hello from acceptance reload\r")
	session.WaitFor(t, responseText, 3*time.Second)
	session.WaitForQuiet(t, 300*time.Millisecond, 3*time.Second)
	if got := requests.Load(); got != 1 {
		t.Fatalf("provider requests=%d want=1", got)
	}
	if identityAfter := singleAcceptanceSessionIdentity(t, dataDir); identityAfter != identityBefore {
		t.Fatal("acceptance reload replaced the active session")
	}
	session.Write(t, string([]byte{3}))
	session.WaitForExit(t, 3*time.Second)
	session.AssertRestored(t)
	if strings.Contains(session.Output(), sentinel) {
		t.Fatal("acceptance PTY output contains the configured credential")
	}

	cfg, err := config.Load(config.LoadOptions{ConfigPath: configPath, LookupEnv: func(name string) (string, bool) {
		if name == "YORDAM_API_KEY" {
			return sentinel, true
		}
		return "", false
	}})
	if err != nil {
		t.Fatal(err)
	}
	profile := cfg.Profiles["default"]
	if cfg.ActiveProfile != "default" || profile.BaseURL != server.URL+"/v1" || profile.DefaultModel != "acceptance-model" || profile.APIKeyEnv != "YORDAM_API_KEY" {
		t.Fatalf("persisted reload config=%+v", cfg)
	}
	assertTreeOmits(t, root, sentinel)
	assertTreeOmits(t, home, sentinel)
	assertAcceptanceMode(t, filepath.Dir(configPath), 0o700)
	assertAcceptanceMode(t, configPath, 0o600)
}

func newAcceptanceSSEServer(t *testing.T, fixture, expectedCredential string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	requests := &atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/chat/completions" {
			t.Errorf("request path=%q", request.URL.Path)
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

func cleanPTYEnvironment(home string, extra []string) []string {
	blocked := map[string]bool{
		"YORDAM_API_KEY":  true,
		"YORDAM_BASE_URL": true,
		"YORDAM_MODEL":    true,
		"YORDAM_PROFILE":  true,
		"TERM":            true,
		"HOME":            true,
	}
	cleaned := make([]string, 0, len(os.Environ())+len(extra)+1)
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if !blocked[name] {
			cleaned = append(cleaned, entry)
		}
	}
	cleaned = append(cleaned, "TERM=xterm-256color", "HOME="+home)
	return append(cleaned, extra...)
}

func assertTreeOmits(t *testing.T, root, forbidden string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(raw), forbidden) {
			return fmt.Errorf("%s contains sentinel", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func singleAcceptanceSessionIdentity(t *testing.T, dataDir string) string {
	t.Helper()
	workspaces, err := os.ReadDir(filepath.Join(dataDir, "workspaces"))
	if err != nil {
		t.Fatal(err)
	}
	workspaces = acceptanceDirectories(workspaces)
	if len(workspaces) != 1 {
		t.Fatalf("workspace data directories=%d want=1", len(workspaces))
	}
	sessions, err := os.ReadDir(filepath.Join(dataDir, "workspaces", workspaces[0].Name(), "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	sessions = acceptanceDirectories(sessions)
	if len(sessions) != 1 {
		t.Fatalf("session data directories=%d want=1", len(sessions))
	}
	return workspaces[0].Name() + "/" + sessions[0].Name()
}

func acceptanceDirectories(entries []os.DirEntry) []os.DirEntry {
	directories := entries[:0]
	for _, entry := range entries {
		if entry.IsDir() {
			directories = append(directories, entry)
		}
	}
	return directories
}

func assertAcceptanceMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode=%o want=%o", path, got, want)
	}
}
