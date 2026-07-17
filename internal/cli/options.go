package cli

import (
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
)

type Options struct {
	Continue     bool
	Session      string
	Mode         domain.PermissionMode
	Profile      string
	Model        string
	BaseURL      string
	DataDir      string
	DebugLog     string
	MaxToolCalls int
	ShellTimeout time.Duration
	Version      bool
	ModeSet      bool
	ProfileSet   bool
	ModelSet     bool
	BaseURLSet   bool
	MaxToolsSet  bool
	TimeoutSet   bool
}

func Parse(args []string) (Options, error) {
	var out Options
	fs := flag.NewFlagSet("yordam", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.BoolVar(&out.Continue, "continue", false, "resume latest session for cwd")
	fs.StringVar(&out.Session, "session", "", "resume session id")
	mode := fs.String("mode", "ask", "safe, ask, or auto")
	fs.StringVar(&out.Profile, "profile", "", "model profile")
	fs.StringVar(&out.Model, "model", "", "model name")
	fs.StringVar(&out.BaseURL, "base-url", "", "OpenAI-compatible base URL")
	fs.StringVar(&out.DataDir, "data-dir", "", "session data directory")
	fs.StringVar(&out.DebugLog, "debug-log", "", "opt-in redacted JSON log path")
	fs.IntVar(&out.MaxToolCalls, "max-tool-calls", 32, "completed tool calls per turn")
	timeout := fs.Duration("shell-timeout", 120*time.Second, "shell timeout")
	fs.BoolVar(&out.Version, "version", false, "print version")
	if err := fs.Parse(args); err != nil {
		return out, err
	}
	if fs.NArg() != 0 {
		return out, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	out.Mode = domain.PermissionMode(*mode)
	if err := out.Mode.Validate(); err != nil {
		return out, err
	}
	out.ShellTimeout = *timeout
	fs.Visit(func(flag *flag.Flag) {
		switch flag.Name {
		case "mode":
			out.ModeSet = true
		case "profile":
			out.ProfileSet = true
		case "model":
			out.ModelSet = true
		case "base-url":
			out.BaseURLSet = true
		case "max-tool-calls":
			out.MaxToolsSet = true
		case "shell-timeout":
			out.TimeoutSet = true
		}
	})
	if out.Continue && out.Session != "" {
		return out, fmt.Errorf("--continue and --session are mutually exclusive")
	}
	if out.MaxToolCalls < 1 || out.MaxToolCalls > 128 {
		return out, fmt.Errorf("--max-tool-calls must be 1..128")
	}
	if out.ShellTimeout < time.Second || out.ShellTimeout > 30*time.Minute {
		return out, fmt.Errorf("--shell-timeout must be 1s..30m")
	}
	return out, nil
}
