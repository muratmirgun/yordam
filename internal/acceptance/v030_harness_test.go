//go:build acceptance

package acceptance_test

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/testsupport/ptyfixture"
)

type selfHostCheckout struct {
	SourceCommit string
	SourceRoot   string
	Workspace    string
	Home         string
	DataDir      string
	DebugLog     string
	Binary       string
}

func TestSelfHostHarnessPreparesExactReleasedYordam(t *testing.T) {
	checkout := newSelfHostCheckout(t)
	if checkout.SourceCommit == "" || checkout.SourceRoot == checkout.Workspace {
		t.Fatalf("self-host checkout is not exact and isolated: %+v", checkout)
	}
	for _, root := range []string{checkout.SourceRoot, checkout.Workspace} {
		head, err := ptyfixture.CleanHead(root)
		if err != nil || head != checkout.SourceCommit {
			t.Fatalf("checkout root=%s head=%q want=%q err=%v", root, head, checkout.SourceCommit, err)
		}
	}
	assertSelfHostCloneIdentity(t, checkout.SourceRoot, checkout.Workspace, "README.md")
	info, err := os.Stat(checkout.Binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("self-host binary info=%v err=%v", info, err)
	}
}

func newSelfHostCheckout(t *testing.T) selfHostCheckout {
	t.Helper()
	releaseInput := ptyfixture.RepositoryRoot()
	before, err := ptyfixture.TreeDigest(releaseInput)
	if err != nil {
		t.Fatal(err)
	}
	commit, err := ptyfixture.CleanHead(releaseInput)
	if err != nil {
		t.Fatalf("self-hosting requires clean exact-commit release input: %v", err)
	}
	root := t.TempDir()
	checkout := selfHostCheckout{
		SourceCommit: commit,
		SourceRoot:   filepath.Join(root, "source"),
		Workspace:    filepath.Join(root, "workspace"),
		Home:         filepath.Join(root, "home"),
		DataDir:      filepath.Join(root, "data"),
		DebugLog:     filepath.Join(root, "debug", "self-host.jsonl"),
		Binary:       filepath.Join(root, "bin", "yordam"),
	}
	if err := ptyfixture.CloneExact(releaseInput, checkout.SourceRoot, commit); err != nil {
		t.Fatal(err)
	}
	if err := ptyfixture.CloneExact(releaseInput, checkout.Workspace, commit); err != nil {
		t.Fatal(err)
	}
	assertSelfHostCloneIdentity(t, releaseInput, checkout.Workspace, "README.md")
	date := harnessGit(t, checkout.SourceRoot, "show", "-s", "--format=%cI", commit)
	if err := ptyfixture.BuildExact(checkout.SourceRoot, checkout.Binary, "v0.3.0-self-host", commit, date, "./cmd/yordam"); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command(checkout.Binary, "--version").CombinedOutput()
	want := fmt.Sprintf("yordam v0.3.0-self-host (%s, %s)", commit, date)
	if err != nil || strings.TrimSpace(string(output)) != want {
		t.Fatalf("self-host binary identity=%q want=%q err=%v", output, want, err)
	}
	if status := harnessGit(t, checkout.Workspace, "status", "--porcelain=v1", "--untracked-files=all"); status != "" {
		t.Fatalf("self-host workspace begins dirty: %q", status)
	}
	t.Cleanup(func() {
		after, digestErr := ptyfixture.TreeDigest(releaseInput)
		if digestErr != nil {
			t.Errorf("hash release input after self-host fixture: %v", digestErr)
		} else if after != before {
			t.Errorf("self-host fixture mutated release input: before=%s after=%s", before, after)
		}
	})
	return checkout
}

func (c selfHostCheckout) start(t *testing.T, serverURL string, args ...string) *ptyfixture.Session {
	t.Helper()
	providerURL, err := url.Parse(serverURL)
	if err != nil || providerURL.Scheme != "http" || providerURL.User != nil || providerURL.RawQuery != "" || providerURL.Fragment != "" || providerURL.Path != "" || !selfHostLoopback(providerURL.Hostname()) {
		t.Fatalf("self-host provider must be a local HTTP server: %q", serverURL)
	}
	temporary := filepath.Join(filepath.Dir(c.Home), "tmp")
	if err := os.MkdirAll(c.Home, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(temporary, 0o700); err != nil {
		t.Fatal(err)
	}
	const fixtureKey = "self-host-local-provider-key"
	configPath := filepath.Join(c.Home, ".config", "yordam", "config.jsonc")
	reserve := int64(1024)
	cfg := config.Config{
		ActiveProfile: "self-host",
		Profiles: map[string]config.Profile{"self-host": {
			BaseURL: strings.TrimRight(serverURL, "/") + "/v1", APIKeyEnv: "YORDAM_SELF_HOST_KEY",
			Models: []string{"self-host-model"}, DefaultModel: "self-host-model",
			ModelContextWindows: map[string]int64{"self-host-model": 8192},
		}},
		MaxToolCalls: 32, ShellTimeoutSeconds: 300,
		Context:   config.ContextConfig{AutoCompact: true, CompactReserveTokens: &reserve},
		Skills:    config.SkillConfig{ProjectPolicy: config.ProjectSkillsAsk},
		Subagents: config.SubagentConfig{Enabled: true, MaxPerTurn: 1, MaxToolCalls: 16, TimeoutSeconds: 120},
	}
	if err := config.SaveGlobal(configPath, cfg); err != nil {
		t.Fatal(err)
	}
	arguments := []string{"--mode", "auto", "--data-dir", c.DataDir, "--debug-log", c.DebugLog}
	arguments = append(arguments, args...)
	environment := cleanPTYEnvironment(c.Home, nil)
	filtered := environment[:0]
	for _, entry := range environment {
		name, _, _ := strings.Cut(entry, "=")
		if name != "TMPDIR" && name != "TMP" && name != "TEMP" {
			filtered = append(filtered, entry)
		}
	}
	filtered = append(filtered, "TMPDIR="+temporary, "TMP="+temporary, "TEMP="+temporary, "YORDAM_SELF_HOST_KEY="+fixtureKey)
	return ptyfixture.StartRedacted(t, secret.New(fixtureKey).String, c.Binary, c.Workspace, filtered, arguments...)
}

func selfHostLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}

func assertSelfHostCloneIdentity(t *testing.T, source, clone, tracked string) {
	t.Helper()
	sourceInfo, err := os.Stat(filepath.Join(source, tracked))
	if err != nil {
		t.Fatal(err)
	}
	cloneInfo, err := os.Stat(filepath.Join(clone, tracked))
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(sourceInfo, cloneInfo) {
		t.Fatalf("self-host clone hard-linked tracked file %s", tracked)
	}
}

func TestSelfHostHarnessClonesExactCommitWithoutHardlinksOrSourceMutation(t *testing.T) {
	source := newHarnessRepository(t)
	before, err := ptyfixture.TreeDigest(source)
	if err != nil {
		t.Fatal(err)
	}
	commit, err := ptyfixture.CleanHead(source)
	if err != nil {
		t.Fatal(err)
	}

	buildRoot := filepath.Join(t.TempDir(), "source")
	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := ptyfixture.CloneExact(source, buildRoot, commit); err != nil {
		t.Fatal(err)
	}
	if err := ptyfixture.CloneExact(source, workspace, commit); err != nil {
		t.Fatal(err)
	}

	for _, clone := range []string{buildRoot, workspace} {
		got, err := ptyfixture.CleanHead(clone)
		if err != nil || got != commit {
			t.Fatalf("clone=%s commit=%q want=%q err=%v", clone, got, commit, err)
		}
		status := harnessGit(t, clone, "status", "--porcelain", "--untracked-files=all")
		if status != "" {
			t.Fatalf("clone=%s is dirty: %q", clone, status)
		}
		sourceInfo, err := os.Stat(filepath.Join(source, "README.md"))
		if err != nil {
			t.Fatal(err)
		}
		cloneInfo, err := os.Stat(filepath.Join(clone, "README.md"))
		if err != nil {
			t.Fatal(err)
		}
		if os.SameFile(sourceInfo, cloneInfo) {
			t.Fatalf("clone=%s hard-linked tracked README.md", clone)
		}
	}

	after, err := ptyfixture.TreeDigest(source)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("clone preparation mutated release input: before=%s after=%s", before, after)
	}
}

func TestSelfHostHarnessRejectsDirtyReleaseInput(t *testing.T) {
	source := newHarnessRepository(t)
	if err := os.WriteFile(filepath.Join(source, "untracked.txt"), []byte("dirty\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ptyfixture.CleanHead(source); err == nil || !strings.Contains(err.Error(), "dirty") {
		t.Fatalf("CleanHead error=%v, want dirty release input", err)
	}
}

func TestSelfHostHarnessBuildBindsExactVersionCommitAndDate(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("self-hosting release targets Darwin and Linux")
	}
	source := newHarnessRepository(t)
	commit, err := ptyfixture.CleanHead(source)
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "fixture")
	if err := ptyfixture.BuildExact(source, binary, "v0.3.0-self-host", commit, "2026-07-21T00:00:00Z", "./cmd/fixture"); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command(binary).CombinedOutput()
	if err != nil {
		t.Fatalf("run exact build: %v: %s", err, output)
	}
	want := "v0.3.0-self-host " + commit + " 2026-07-21T00:00:00Z"
	if strings.TrimSpace(string(output)) != want {
		t.Fatalf("build identity=%q want=%q", output, want)
	}
}

func newHarnessRepository(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"README.md":                       "# Yordam\n",
		"go.mod":                          "module example.com/selfhostfixture\n\ngo 1.26.4\n",
		"internal/buildinfo/buildinfo.go": "package buildinfo\n\nvar Version = \"dev\"\nvar Commit = \"none\"\nvar Date = \"unknown\"\n",
		"cmd/fixture/main.go":             "package main\n\nimport (\"fmt\"; \"example.com/selfhostfixture/internal/buildinfo\")\nfunc main() { fmt.Printf(\"%s %s %s\\n\", buildinfo.Version, buildinfo.Commit, buildinfo.Date) }\n",
	}
	for name, content := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	harnessGit(t, root, "init", "--quiet")
	harnessGit(t, root, "config", "user.name", "Yordam Acceptance")
	harnessGit(t, root, "config", "user.email", "acceptance@yordam.invalid")
	harnessGit(t, root, "add", "--all")
	harnessGit(t, root, "commit", "--quiet", "-m", "fixture")
	return root
}

func harnessGit(t *testing.T, root string, arguments ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", root}, arguments...)...)
	command.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", arguments, err, output)
	}
	return strings.TrimSpace(string(output))
}
