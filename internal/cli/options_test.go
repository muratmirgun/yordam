package cli_test

import (
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/cli"
	"github.com/muratmirgun/yordam/internal/domain"
)

func TestParseContinueModeAndModel(t *testing.T) {
	got, err := cli.Parse([]string{"--continue", "--mode", "auto", "--profile", "local", "--model", "m1"})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Continue || got.Mode != domain.ModeAuto || got.Profile != "local" || got.Model != "m1" {
		t.Fatalf("options=%#v", got)
	}
}

func TestParseRejectsContinueWithSession(t *testing.T) {
	if _, err := cli.Parse([]string{"--continue", "--session", "01JABC"}); err == nil {
		t.Fatal("conflicting session selectors accepted")
	}
}

func TestParseDefaultsToAsk(t *testing.T) {
	got, err := cli.Parse(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Mode != domain.ModeAsk {
		t.Fatalf("mode=%s", got.Mode)
	}
	if got.MaxToolCalls != 32 {
		t.Fatalf("max tool calls=%d", got.MaxToolCalls)
	}
	if got.ShellTimeout != 120*time.Second {
		t.Fatalf("shell timeout=%s", got.ShellTimeout)
	}
	if got.ModeSet || got.MaxToolsSet || got.TimeoutSet {
		t.Fatalf("default options marked explicitly set: %#v", got)
	}
}

func TestParseAllValueOptions(t *testing.T) {
	got, err := cli.Parse([]string{
		"--session", "01JABC",
		"--mode", "safe",
		"--profile", "local",
		"--model", "m1",
		"--base-url", "http://localhost:11434/v1",
		"--data-dir", "/tmp/data",
		"--debug-log", "/tmp/debug.jsonl",
		"--max-tool-calls", "64",
		"--shell-timeout", "2m",
		"--version",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Session != "01JABC" || got.Mode != domain.ModeSafe || got.Profile != "local" || got.Model != "m1" {
		t.Fatalf("session/model options=%#v", got)
	}
	if got.BaseURL != "http://localhost:11434/v1" || got.DataDir != "/tmp/data" || got.DebugLog != "/tmp/debug.jsonl" {
		t.Fatalf("path options=%#v", got)
	}
	if got.MaxToolCalls != 64 || got.ShellTimeout != 2*time.Minute || !got.Version {
		t.Fatalf("runtime options=%#v", got)
	}
	if !got.ModeSet || !got.ProfileSet || !got.ModelSet || !got.BaseURLSet || !got.MaxToolsSet || !got.TimeoutSet {
		t.Fatalf("explicit options not tracked: %#v", got)
	}
}

func TestParseRejectsConfigOverride(t *testing.T) {
	if _, err := cli.Parse([]string{"--config", "/tmp/config.jsonc"}); err == nil {
		t.Fatal("--config override accepted")
	}
}

func TestParseTracksExplicitDefaultValuedOverrides(t *testing.T) {
	got, err := cli.Parse([]string{"--mode", "ask", "--max-tool-calls", "32", "--shell-timeout", "120s"})
	if err != nil {
		t.Fatal(err)
	}
	if !got.ModeSet || !got.MaxToolsSet || !got.TimeoutSet {
		t.Fatalf("explicit defaults not tracked: %#v", got)
	}
}

func TestParseRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "unknown flag", args: []string{"--unknown"}},
		{name: "positional argument", args: []string{"prompt"}},
		{name: "invalid mode", args: []string{"--mode", "root"}},
		{name: "invalid duration", args: []string{"--shell-timeout", "later"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := cli.Parse(test.args); err == nil {
				t.Fatalf("Parse(%q) accepted invalid input", test.args)
			}
		})
	}
}

func TestParseMaxToolCallsRange(t *testing.T) {
	for _, value := range []string{"1", "128"} {
		t.Run("accepts "+value, func(t *testing.T) {
			if _, err := cli.Parse([]string{"--max-tool-calls", value}); err != nil {
				t.Fatalf("Parse(%q): %v", value, err)
			}
		})
	}
	for _, value := range []string{"0", "129"} {
		t.Run("rejects "+value, func(t *testing.T) {
			if _, err := cli.Parse([]string{"--max-tool-calls", value}); err == nil {
				t.Fatalf("Parse(%q) accepted out-of-range value", value)
			}
		})
	}
}

func TestParseShellTimeoutRange(t *testing.T) {
	for _, value := range []string{"1s", "30m"} {
		t.Run("accepts "+value, func(t *testing.T) {
			if _, err := cli.Parse([]string{"--shell-timeout", value}); err != nil {
				t.Fatalf("Parse(%q): %v", value, err)
			}
		})
	}
	for _, value := range []string{"999ms", "30m1s"} {
		t.Run("rejects "+value, func(t *testing.T) {
			if _, err := cli.Parse([]string{"--shell-timeout", value}); err == nil {
				t.Fatalf("Parse(%q) accepted out-of-range value", value)
			}
		})
	}
}
