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
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/testsupport/ptyfixture"
)

func acceptSingleBinaryStartup(t *testing.T) {
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
	session := ptyfixture.Start(t, binary, fresh, cleanPTYEnvironment(home, nil),
		"--data-dir", filepath.Join(fresh, "data"),
	)
	session.WaitFor(t, "Ask Yordam", 3*time.Second)
	session.Write(t, string([]byte{3}))
	session.WaitForExit(t, 3*time.Second)
	session.AssertRestored(t)
}

func acceptSecretFreeSetup(t *testing.T) {
	const sentinel = "v010-first-run-secret"
	server := newSSEServer(t, "data: [DONE]\n\n")
	defer server.Close()
	root := t.TempDir()
	home := t.TempDir()
	configPath := filepath.Join(home, ".config", "yordam", "config.jsonc")
	dataDir := filepath.Join(root, "data")
	environment := cleanPTYEnvironment(home, []string{"YORDAM_API_KEY=" + sentinel})
	session := ptyfixture.Start(t, ptyfixture.CachedYordam(t), root, environment,
		"--data-dir", dataDir,
	)
	session.WaitFor(t, "Ask Yordam", 3*time.Second)
	if err := config.SaveGlobal(configPath, acceptanceConfig(server.URL+"/v1", "YORDAM_API_KEY", map[string][]string{"default": {"acceptance-model"}})); err != nil {
		t.Fatal(err)
	}
	session.Write(t, "/reload\r")
	session.WaitFor(t, "default/acceptance-model", 3*time.Second)
	session.WaitFor(t, "configuration reloaded", 3*time.Second)
	session.Write(t, string([]byte{3}))
	session.WaitForExit(t, 3*time.Second)
	session.AssertRestored(t)

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
		t.Fatalf("persisted setup config=%+v", cfg)
	}
	assertTreeOmits(t, root, sentinel)
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
