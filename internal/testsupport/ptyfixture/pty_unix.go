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

	"github.com/charmbracelet/x/vt"
	"github.com/creack/pty"
)

type Session struct {
	command            *exec.Cmd
	terminal           *os.File
	done               chan error
	diagnosticRedactor func(string) string
	mu                 sync.Mutex
	output             bytes.Buffer
	emulator           *vt.Emulator
	emulatorStop       func()
	outputDone         chan struct{}
}

const scriptedSSE = "data: {\"choices\":[{\"delta\":{\"content\":\"scripted PTY response\"}}]}\n\ndata: [DONE]\n"

func ScriptedSSE() string { return scriptedSSE }

func Start(t testing.TB, binary, workspace string, environment []string, arguments ...string) *Session {
	return start(t, nil, binary, workspace, environment, arguments...)
}

func StartRedacted(t testing.TB, redact func(string) string, binary, workspace string, environment []string, arguments ...string) *Session {
	return start(t, redact, binary, workspace, environment, arguments...)
}

func start(t testing.TB, redact func(string) string, binary, workspace string, environment []string, arguments ...string) *Session {
	t.Helper()
	command := exec.Command(binary, arguments...)
	command.Dir = workspace
	command.Env = append([]string(nil), environment...)
	terminal, err := pty.StartWithSize(command, &pty.Winsize{Rows: 32, Cols: 120})
	if err != nil {
		t.Fatal(formatDiagnostic(redact, "start PTY: %v", err))
	}
	emulator, stopEmulator := newPTYEmulator(120, 32)
	session := &Session{command: command, terminal: terminal, done: make(chan error, 1), diagnosticRedactor: redact, emulator: emulator, emulatorStop: stopEmulator, outputDone: make(chan struct{})}
	go func() {
		defer close(session.outputDone)
		_, _ = io.Copy(lockedWriter{session: session}, terminal)
	}()
	go func() { session.done <- command.Wait() }()
	t.Cleanup(func() {
		_ = terminal.Close()
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			<-session.done
		}
		<-session.outputDone
		session.emulatorStop()
	})
	return session
}

func newPTYEmulator(width, height int) (*vt.Emulator, func()) {
	emulator := vt.NewEmulator(width, height)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		buffer := make([]byte, 256)
		for {
			if _, err := emulator.Read(buffer); err != nil {
				return
			}
			select {
			case <-stop:
				return
			default:
			}
		}
	}()
	var once sync.Once
	return emulator, func() {
		once.Do(func() {
			close(stop)
			// Wake the response reader without racing Emulator.Close against Read.
			// DEC operating-status query deterministically emits a response.
			wakeDone := make(chan struct{})
			go func() {
				_, _ = emulator.Write([]byte("\x1b[5n"))
				close(wakeDone)
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			select {
			case <-wakeDone:
			case <-time.After(time.Second):
			}
		})
	}
}

func (s *Session) Write(t testing.TB, value string) {
	t.Helper()
	if _, err := s.terminal.Write([]byte(value)); err != nil {
		t.Fatal(s.formatDiagnostic("write PTY: %v", err))
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
			t.Fatal(s.formatDiagnostic("process exited before %q: err=%v output=%q", value, err, s.Output()))
		case <-deadline.C:
			t.Fatal(s.formatDiagnostic("timed out waiting for %q; output=%q", value, s.Output()))
		case <-ticker.C:
		}
	}
}

func (s *Session) WaitForExit(t testing.TB, timeout time.Duration) {
	t.Helper()
	select {
	case err := <-s.done:
		if err != nil {
			t.Fatal(s.formatDiagnostic("process exit: %v output=%q", err, s.Output()))
		}
	case <-time.After(timeout):
		t.Fatal(s.formatDiagnostic("TUI did not restore terminal and exit; output=%q", s.Output()))
	}
}

func (s *Session) Output() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.output.String()
}

func (s *Session) OutputOffset() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.output.Len()
}

// CurrentScreen returns the normalized text currently visible in the PTY,
// excluding content that later ANSI updates replaced or cleared.
func (s *Session) CurrentScreen() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.emulator == nil {
		return ""
	}
	return s.emulator.String()
}

func (s *Session) WaitForCurrentScreen(t testing.TB, value string, timeout time.Duration) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if strings.Contains(s.CurrentScreen(), value) {
			return
		}
		select {
		case err := <-s.done:
			t.Fatal(s.formatDiagnostic("process exited before current-screen marker %q: err=%v screen=%q output=%q", value, err, s.CurrentScreen(), s.Output()))
		case <-deadline.C:
			t.Fatal(s.formatDiagnostic("timed out waiting for current-screen marker %q; screen=%q output=%q", value, s.CurrentScreen(), s.Output()))
		case <-ticker.C:
		}
	}
}

func (s *Session) WaitForAfter(t testing.TB, offset int, value string, timeout time.Duration) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		output, valid := s.outputAfter(offset)
		if !valid {
			t.Fatal(s.formatDiagnostic("invalid PTY output offset %d", offset))
		}
		if strings.Contains(output, value) {
			return
		}
		select {
		case err := <-s.done:
			t.Fatal(s.formatDiagnostic("process exited before post-offset marker %q: err=%v output=%q", value, err, output))
		case <-deadline.C:
			t.Fatal(s.formatDiagnostic("timed out waiting for post-offset marker %q; output=%q", value, output))
		case <-ticker.C:
		}
	}
}

func (s *Session) WaitForOrderedAfter(t testing.TB, offset int, values []string, timeout time.Duration) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		output, valid := s.outputAfter(offset)
		if !valid {
			t.Fatal(s.formatDiagnostic("invalid PTY output offset %d", offset))
		}
		if containsOrdered(output, values) {
			return
		}
		select {
		case err := <-s.done:
			t.Fatal(s.formatDiagnostic("process exited before ordered post-offset markers %q: err=%v output=%q", values, err, output))
		case <-deadline.C:
			t.Fatal(s.formatDiagnostic("timed out waiting for ordered post-offset markers %q; output=%q", values, output))
		case <-ticker.C:
		}
	}
}

func containsOrdered(output string, values []string) bool {
	for _, value := range values {
		index := strings.Index(output, value)
		if index < 0 {
			return false
		}
		output = output[index+len(value):]
	}
	return true
}

func (s *Session) WaitForQuiet(t testing.TB, quiet, timeout time.Duration) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	lastLength := -1
	quietSince := time.Now()
	for {
		length := s.OutputOffset()
		if length != lastLength {
			lastLength = length
			quietSince = time.Now()
		} else if time.Since(quietSince) >= quiet {
			return
		}
		select {
		case err := <-s.done:
			t.Fatal(s.formatDiagnostic("process exited before PTY became quiet: err=%v output=%q", err, s.Output()))
		case <-deadline.C:
			t.Fatal(s.formatDiagnostic("timed out waiting for quiet PTY output=%q", s.Output()))
		case <-ticker.C:
		}
	}
}

func (s *Session) outputAfter(offset int) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if offset < 0 || offset > s.output.Len() {
		return "", false
	}
	return s.output.String()[offset:], true
}

func (s *Session) ResetOutput() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.output.Reset()
}

func (s *Session) AssertRestored(t testing.TB) {
	t.Helper()
	if !strings.Contains(s.Output(), "\x1b[?1049l") {
		t.Fatal(s.formatDiagnostic("alternate screen was not restored: output=%q", s.Output()))
	}
}

func (s *Session) formatDiagnostic(format string, arguments ...any) string {
	return formatDiagnostic(s.diagnosticRedactor, format, arguments...)
}

func formatDiagnostic(redact func(string) string, format string, arguments ...any) string {
	diagnostic := fmt.Sprintf(format, arguments...)
	if redact != nil {
		return redact(diagnostic)
	}
	return diagnostic
}

func (s *Session) Terminal() *os.File { return s.terminal }
func (s *Session) Command() *exec.Cmd { return s.command }

type lockedWriter struct{ session *Session }

func (w lockedWriter) Write(value []byte) (int, error) {
	w.session.mu.Lock()
	defer w.session.mu.Unlock()
	written, err := w.session.output.Write(value)
	if err != nil || w.session.emulator == nil {
		return written, err
	}
	if _, emulatorErr := w.session.emulator.Write(value); emulatorErr != nil {
		return written, emulatorErr
	}
	return written, nil
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
