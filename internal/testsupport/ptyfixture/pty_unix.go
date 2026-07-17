//go:build darwin || linux

package ptyfixture

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/creack/pty"
)

type Session struct {
	command  *exec.Cmd
	terminal *os.File
	done     chan error
	mu       sync.Mutex
	output   bytes.Buffer
}

func Start(t testing.TB, binary, workspace string, environment []string, arguments ...string) *Session {
	t.Helper()
	command := exec.Command(binary, arguments...)
	command.Dir = workspace
	command.Env = append([]string(nil), environment...)
	terminal, err := pty.StartWithSize(command, &pty.Winsize{Rows: 32, Cols: 120})
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{command: command, terminal: terminal, done: make(chan error, 1)}
	go func() {
		_, _ = io.Copy(lockedWriter{session: session}, terminal)
	}()
	go func() { session.done <- command.Wait() }()
	t.Cleanup(func() {
		_ = terminal.Close()
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			<-session.done
		}
	})
	return session
}

func (s *Session) Write(t testing.TB, value string) {
	t.Helper()
	if _, err := s.terminal.Write([]byte(value)); err != nil {
		t.Fatal(err)
	}
}

func (s *Session) WaitFor(t testing.TB, value string, timeout time.Duration) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if strings.Contains(s.Output(), value) {
			return
		}
		select {
		case err := <-s.done:
			t.Fatalf("process exited before %q: err=%v output=%q", value, err, s.Output())
		case <-deadline.C:
			t.Fatalf("timed out waiting for %q; output=%q", value, s.Output())
		case <-ticker.C:
		}
	}
}

func (s *Session) WaitForExit(t testing.TB, timeout time.Duration) {
	t.Helper()
	select {
	case err := <-s.done:
		if err != nil {
			t.Fatalf("process exit: %v output=%q", err, s.Output())
		}
	case <-time.After(timeout):
		t.Fatalf("TUI did not restore terminal and exit; output=%q", s.Output())
	}
}

func (s *Session) Output() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.output.String()
}

func (s *Session) ResetOutput() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.output.Reset()
}

func (s *Session) AssertRestored(t testing.TB) {
	t.Helper()
	if !strings.Contains(s.Output(), "\x1b[?1049l") {
		t.Fatalf("alternate screen was not restored: output=%q", s.Output())
	}
}

func (s *Session) Terminal() *os.File { return s.terminal }
func (s *Session) Command() *exec.Cmd { return s.command }

type lockedWriter struct{ session *Session }

func (w lockedWriter) Write(value []byte) (int, error) {
	w.session.mu.Lock()
	defer w.session.mu.Unlock()
	return w.session.output.Write(value)
}

func BuildYordam(t testing.TB, directory string) string {
	t.Helper()
	path := filepath.Join(directory, "yordam")
	command := exec.Command("go", "build", "-o", path, "./cmd/yordam")
	command.Dir = RepositoryRoot()
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v: %s", err, output)
	}
	return path
}

var (
	buildOnce   sync.Once
	builtBinary string
	buildErr    error
)

func CachedYordam(t testing.TB) string {
	t.Helper()
	buildOnce.Do(func() {
		directory, err := os.MkdirTemp("", "yordam-pty-")
		if err != nil {
			buildErr = err
			return
		}
		builtBinary = filepath.Join(directory, "yordam")
		command := exec.Command("go", "build", "-o", builtBinary, "./cmd/yordam")
		command.Dir = RepositoryRoot()
		if output, err := command.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("go build: %w: %s", err, output)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return builtBinary
}

func RepositoryRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("cannot determine repository root")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}
