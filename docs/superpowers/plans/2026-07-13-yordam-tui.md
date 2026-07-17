# Yordam Adaptive TUI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce the runnable `yordam` binary with first-run setup, adaptive hybrid conversation UI, session resume, model/mode controls, permission and diff surfaces, and clean cancellation/terminal restoration.

**Architecture:** A Bubble Tea root model owns UI-only state and communicates exclusively through the app command/event bridge. Focused components render conversation, composer, context, permission, sessions, and setup; the composition root wires config, JSONL sessions, provider, tools, permission policy, agent runner, app, and TUI.

**Tech Stack:** Go 1.26.4, `charm.land/bubbletea/v2` v2.0.8, `charm.land/bubbles/v2` v2.1.1, `charm.land/lipgloss/v2` v2.0.5, `github.com/creack/pty` v1.1.24 for PTY tests.

## Global Constraints

- Complete Phases 1-3 and Gates 1-3 first.
- Main layout is conversation-first; context panel appears only for diffs, permissions, errors, or expanded details.
- At widths below 100 columns, context replaces the stream instead of splitting it.
- `Enter` submits; `Ctrl+J` inserts newline; `Esc` priority is modal, context, active turn; `Ctrl+P` opens the command palette; `Ctrl+O` toggles context; `Ctrl+C` exits or confirms during a turn.
- Required commands: `/new`, `/sessions`, `/mode`, `/model`, `/compact`, `/help`, `/quit`.
- `yordam` creates a new session; `yordam --continue` resumes the latest for canonical cwd; `--session` requires the same workspace.
- Default mode is `ask`.
- Profile/model and mode changes during a turn queue until the turn ends.
- Base URL/model/profile may persist; API key values never persist.
- TUI code must not bypass app commands to call providers or tools.

---

### Task 1: Add CLI options and build info

**Files:**
- Create: `internal/buildinfo/buildinfo.go`
- Create: `internal/cli/options.go`
- Create: `internal/cli/options_test.go`

**Interfaces:**
- Consumes: platform config/data path helpers.
- Produces: `cli.Parse([]string) (Options, error)` and build metadata used by the composition task.

- [ ] **Step 1: Write exact CLI parsing tests**

```go
package cli_test

import (
    "testing"
    "github.com/muratmirgun/yordam/internal/cli"
    "github.com/muratmirgun/yordam/internal/domain"
)

func TestParseContinueModeAndModel(t *testing.T) {
    got, err := cli.Parse([]string{"--continue","--mode","auto","--profile","local","--model","m1"})
    if err != nil { t.Fatal(err) }
    if !got.Continue || got.Mode != domain.ModeAuto || got.Profile != "local" || got.Model != "m1" { t.Fatalf("options=%#v", got) }
}

func TestParseRejectsContinueWithSession(t *testing.T) {
    if _, err := cli.Parse([]string{"--continue","--session","01JABC"}); err == nil { t.Fatal("conflicting session selectors accepted") }
}

func TestParseDefaultsToAsk(t *testing.T) {
    got, err := cli.Parse(nil)
    if err != nil { t.Fatal(err) }
    if got.Mode != domain.ModeAsk { t.Fatalf("mode=%s", got.Mode) }
}
```

- [ ] **Step 2: Run CLI tests**

Run: `go test ./internal/cli -v`

Expected: FAIL because CLI package is missing.

- [ ] **Step 3: Implement standard-library parsing and build metadata**

```go
// internal/buildinfo/buildinfo.go
package buildinfo

var Version = "dev"
var Commit = "none"
var Date = "unknown"
```

```go
// internal/cli/options.go
package cli

import (
    "flag"
    "fmt"
    "io"
    "time"

    "github.com/muratmirgun/yordam/internal/domain"
)

type Options struct {
    Continue bool
    Session string
    Mode domain.PermissionMode
    Profile string
    Model string
    BaseURL string
    ConfigPath string
    DataDir string
    DebugLog string
    MaxToolCalls int
    ShellTimeout time.Duration
    Version bool
}

func Parse(args []string) (Options,error) {
    var out Options
    fs:=flag.NewFlagSet("yordam",flag.ContinueOnError); fs.SetOutput(io.Discard)
    fs.BoolVar(&out.Continue,"continue",false,"resume latest session for cwd")
    fs.StringVar(&out.Session,"session","","resume session id")
    mode:=fs.String("mode","ask","safe, ask, or auto")
    fs.StringVar(&out.Profile,"profile","","model profile")
    fs.StringVar(&out.Model,"model","","model name")
    fs.StringVar(&out.BaseURL,"base-url","","OpenAI-compatible base URL")
    fs.StringVar(&out.ConfigPath,"config","","global config path")
    fs.StringVar(&out.DataDir,"data-dir","","session data directory")
    fs.StringVar(&out.DebugLog,"debug-log","","opt-in redacted JSON log path")
    fs.IntVar(&out.MaxToolCalls,"max-tool-calls",32,"completed tool calls per turn")
    timeout:=fs.Duration("shell-timeout",120*time.Second,"shell timeout")
    fs.BoolVar(&out.Version,"version",false,"print version")
    if err:=fs.Parse(args); err!=nil{return out,err}
    if fs.NArg()!=0{return out,fmt.Errorf("unexpected arguments: %v",fs.Args())}
    out.Mode=domain.PermissionMode(*mode); if err:=out.Mode.Validate();err!=nil{return out,err}
    out.ShellTimeout=*timeout
    if out.Continue&&out.Session!=""{return out,fmt.Errorf("--continue and --session are mutually exclusive")}
    if out.MaxToolCalls<1||out.MaxToolCalls>128{return out,fmt.Errorf("--max-tool-calls must be 1..128")}
    if out.ShellTimeout<time.Second||out.ShellTimeout>30*time.Minute{return out,fmt.Errorf("--shell-timeout must be 1s..30m")}
    return out,nil
}
```

- [ ] **Step 4: Run CLI tests**

Run: `gofmt -w internal/buildinfo internal/cli && go test ./internal/cli -v && go test ./...`

Expected: PASS.

- [ ] **Step 5: Commit CLI contract**

```bash
git add internal/buildinfo internal/cli
git commit -m "feat: define Yordam CLI options"
```

### Task 2: Build the root Bubble Tea state and adaptive layout

**Files:**
- Create: `internal/tui/model.go`
- Create: `internal/tui/update.go`
- Create: `internal/tui/view.go`
- Create: `internal/tui/keys.go`
- Test helper: `internal/tui/export_test.go`
- Test: `internal/tui/model_test.go`
- Test: `internal/tui/testdata/view-80.golden`
- Test: `internal/tui/testdata/view-120.golden`
- Test: `internal/tui/testdata/view-160.golden`

**Interfaces:**
- Consumes: app command/event channels.
- Produces: root `tui.Model` implementing Bubble Tea v2 `Init`, `Update`, and `View`.

- [ ] **Step 1: Write resize and Esc-priority tests**

```go
package tui_test

import (
    "testing"
    tea "charm.land/bubbletea/v2"
    "github.com/muratmirgun/yordam/internal/tui"
)

func TestAdaptiveContextLayout(t *testing.T) {
    model:=tui.NewModel(tui.OptionsForTest())
    model=tui.UpdateForTest(model,tea.WindowSizeMsg{Width:120,Height:40})
    model=tui.OpenContextForTest(model)
    if got:=model.LayoutForTest();got!="split"{t.Fatalf("layout=%s",got)}
    model=tui.UpdateForTest(model,tea.WindowSizeMsg{Width:80,Height:30})
    if got:=model.LayoutForTest();got!="context_only"{t.Fatalf("layout=%s",got)}
}

func TestEscPriorityModalThenContextThenTurn(t *testing.T) {
    model:=tui.ModelWithModalContextAndActiveTurnForTest()
    model=tui.PressForTest(model,"esc")
    if model.HasModalForTest()||!model.ContextOpenForTest()||!model.TurnActiveForTest(){t.Fatal("first esc priority wrong")}
    model=tui.PressForTest(model,"esc")
    if model.ContextOpenForTest()||!model.TurnActiveForTest(){t.Fatal("second esc priority wrong")}
    model=tui.PressForTest(model,"esc")
    if !model.CancelSentForTest(){t.Fatal("third esc did not cancel")}
}
```

- [ ] **Step 2: Run root TUI tests**

Run: `go test ./internal/tui -run 'TestAdaptive|TestEscPriority' -v`

Expected: FAIL because TUI model is missing.

- [ ] **Step 3: Implement explicit UI state and layout calculation**

Add the pinned UI modules:

```bash
go get charm.land/bubbletea/v2@v2.0.8
go get charm.land/lipgloss/v2@v2.0.5
```

Use these exact root states:

```go
type Screen string
const ( ScreenConversation Screen="conversation"; ScreenSessions Screen="sessions"; ScreenSetup Screen="setup"; ScreenHelp Screen="help" )
type Layout string
const ( LayoutStream Layout="stream"; LayoutSplit Layout="split"; LayoutContextOnly Layout="context_only" )
type ModalKind string
const ( ModalNone ModalKind=""; ModalPermission ModalKind="permission"; ModalCommandPalette ModalKind="command_palette"; ModalConfirmExit ModalKind="confirm_exit" )
```

The root model stores `width`, `height`, `screen`, `contextOpen`, `modal`, `turnActive`, `queuedMode`, `queuedSelection`, session title, effective mode, profile/model, canonical workspace, app command sender, app event receiver, focused component, and a `cancelSent` observation flag. Every conversation view starts with one status row containing title, mode, profile/model, and workspace; narrow views truncate the middle workspace segment without hiding mode/model. `layout()` returns context-only below 100 columns, split when context is open at 100 or more, and stream otherwise. Implement `updateMessage(tea.Msg) Model` and `handleKey(string) Model`; production `Update` delegates to them, so tests exercise the same state transitions.

Add a table-driven root key test: `ctrl+p` sets `ModalCommandPalette`; `ctrl+o` toggles context twice; inactive `ctrl+c` sends `CommandShutdown`; active `ctrl+c` opens `ModalConfirmExit` and only explicit confirmation sends cancellation followed by shutdown. Modal routing always precedes these global actions.

Expose only test-build helpers in `export_test.go`:

```go
package tui

import (
    tea "charm.land/bubbletea/v2"
    "github.com/muratmirgun/yordam/internal/app"
)

func OptionsForTest() Options {
    commands := make(chan app.Command, 8)
    events := make(chan app.Event, 8)
    return Options{Commands:commands, Events:events}
}
func UpdateForTest(model Model, message tea.Msg) Model { return model.updateMessage(message) }
func OpenContextForTest(model Model) Model { model.contextOpen = true; return model }
func (model Model) LayoutForTest() string { return string(model.layout()) }
func ModelWithModalContextAndActiveTurnForTest() Model { model:=NewModel(OptionsForTest()); model.modal=ModalPermission; model.contextOpen=true; model.turnActive=true; return model }
func PressForTest(model Model, key string) Model { return model.handleKey(key) }
func (model Model) HasModalForTest() bool { return model.modal != ModalNone }
func (model Model) ContextOpenForTest() bool { return model.contextOpen }
func (model Model) TurnActiveForTest() bool { return model.turnActive }
func (model Model) CancelSentForTest() bool { return model.cancelSent }
```

`View()` returns `tea.NewView(rendered)` and sets `AltScreen=true`. `Init()` starts one command waiting on the app event channel. Every received app event schedules the next wait command.

- [ ] **Step 4: Add and verify exact golden views**

Render a fixed fixture containing one user message, one assistant message, one completed read tool, and an open diff context at widths 80/120/160. Store full rendered strings in the three golden files. Run:

`gofmt -w internal/tui && go test ./internal/tui -run 'TestAdaptive|TestEscPriority|TestGolden' -v`

Expected: PASS; 80 columns is context-only and 120/160 are split.

- [ ] **Step 5: Commit root state/layout**

```bash
git add go.mod go.sum internal/tui
git commit -m "feat: add adaptive Bubble Tea root layout"
```

### Task 3: Implement conversation stream and multi-line composer

**Files:**
- Create: `internal/tui/components/conversation.go`
- Create: `internal/tui/components/composer.go`
- Test: `internal/tui/components/conversation_test.go`
- Test: `internal/tui/components/composer_test.go`
- Modify: `internal/tui/model.go`
- Modify: `internal/tui/update.go`
- Modify: `internal/tui/view.go`

**Interfaces:**
- Consumes: app text/tool/state events and `CommandStartTurn`.
- Produces: scrollable conversation blocks, `Enter` submit, `Ctrl+J` newline, disabled submission during an active turn.

- [ ] **Step 1: Write composer submit/newline/active-turn tests**

```go
func TestComposerEnterSubmitsAndCtrlJAddsNewline(t *testing.T) {
    sent:=[]string{}
    composer:=components.NewComposer(func(value string){sent=append(sent,value)})
    composer.SetValue("first")
    composer.Update(key("ctrl+j")); composer.Insert("second")
    if composer.Value()!="first\nsecond"{t.Fatalf("value=%q",composer.Value())}
    composer.Update(key("enter"))
    if !slices.Equal(sent,[]string{"first\nsecond"})||composer.Value()!=""{t.Fatalf("sent=%v value=%q",sent,composer.Value())}
    composer.SetActiveTurn(true); composer.SetValue("blocked"); composer.Update(key("enter"))
    if len(sent)!=1{t.Fatalf("submitted during active turn: %v",sent)}
}
```

- [ ] **Step 2: Run component tests**

Run: `go test ./internal/tui/components -run 'TestComposer|TestConversation' -v`

Expected: FAIL because components are missing.

- [ ] **Step 3: Implement component contracts**

Run `go get charm.land/bubbles/v2@v2.1.1`, then implement the component contracts.

Conversation block kinds are `user`, `assistant`, `tool`, `error`, and `notice`. Each block has a stable ID so streaming deltas append to the current assistant block rather than creating one block per token. Tool blocks start collapsed and expose status, name, duration, and truncation.

Composer wraps Bubbles v2 textarea but owns key semantics:

```go
type SubmitFunc func(string)
type Composer struct { input textarea.Model; submit SubmitFunc; activeTurn bool }
func NewComposer(SubmitFunc) Composer
func (c *Composer) SetValue(string)
func (c Composer) Value() string
func (c *Composer) SetActiveTurn(bool)
func (c Composer) Update(tea.Msg) (Composer,tea.Cmd)
func (c Composer) View() string
```

Intercept `Ctrl+J` before textarea update and insert `\n`. Intercept `Enter`, trim only outer blank lines, submit non-empty content when inactive, and clear after submission. Paste and ordinary editing remain delegated to textarea.

- [ ] **Step 4: Wire app events and run component/root tests**

Map `app.EventTextDelta` to append assistant text, `app.EventToolOutput` to replace the active shell block's bounded live snapshot, `app.EventToolCompleted` to finalize a tool block, terminal events to turn inactive, and submit to `app.CommandStartTurn`. Run:

`gofmt -w internal/tui && go test -race ./internal/tui/... -v`

Expected: PASS; one streaming response remains one assistant block.

- [ ] **Step 5: Commit conversation/composer**

```bash
git add internal/tui
git commit -m "feat: add conversation stream and composer"
```

### Task 4: Implement permissions, context/diff, and cancellation surfaces

**Files:**
- Create: `internal/tui/components/context.go`
- Create: `internal/tui/components/permission.go`
- Test: `internal/tui/components/context_test.go`
- Test: `internal/tui/components/permission_test.go`
- Modify: `internal/tui/update.go`
- Modify: `internal/tui/view.go`

**Interfaces:**
- Consumes: prepared permission requests and app permission-resolution command.
- Produces: diff preview, `y` allow-once, `s` allow-session, `n`/`Esc` deny, auto-shell warning, context toggle.

- [ ] **Step 1: Write exact permission key tests**

```go
func TestPermissionModalResolutions(t *testing.T) {
    cases:=[]struct{key string;action domain.PermissionAction;lifetime domain.PermissionLifetime}{{"y",domain.PermissionAllow,domain.PermissionOnce},{"s",domain.PermissionAllow,domain.PermissionSession},{"n",domain.PermissionDeny,domain.PermissionOnce},{"esc",domain.PermissionDeny,domain.PermissionOnce}}
    for _,tc:=range cases{t.Run(tc.key,func(t *testing.T){var got domain.PermissionDecision;modal:=components.NewPermission(preparedEdit(),func(d domain.PermissionDecision){got=d});modal.Update(key(tc.key));if got.Action!=tc.action||got.Lifetime!=tc.lifetime{t.Fatalf("got=%#v",got)}})}
}
```

- [ ] **Step 2: Run permission/context tests**

Run: `go test ./internal/tui/components -run 'TestPermission|TestContext' -v`

Expected: FAIL until the permission and context components exist.

- [ ] **Step 3: Add permission modal behavior**

Use the Phase 1 `PermissionLifetime` values. The app applies a session grant only for `allow + session`; deny never creates a grant.

The permission modal renders tool name, canonical scope, inside/outside marker, summary, and proposed diff. Shell auto acknowledgement uses separate copy stating that shell is not sandboxed, may access outside files/network, and inherits non-provider environment variables. Accepting sends the dedicated acknowledge command before resolving the pending shell request.

Context component supports `diff`, `tool`, and `error` tabs; `Enter` expands/collapses and `Esc` closes. Shell results render bounded Git status/diff when `WorkspaceChanges.IsGit` is true and the exact non-Git limitation notice otherwise. The root routes keys to the modal first, context second, active-turn cancellation third.

When an app error unwraps to `domain.TypedError{Kind:ErrorContextTooLarge}`, open the error context with an explicit `Run /compact` action; never trim or silently discard replay history. Other typed errors show their category and redacted message without crashing the program.

- [ ] **Step 4: Run UI and agent permission integration tests**

Run: `gofmt -w internal/domain internal/app internal/permission internal/tui && go test -race ./internal/permission ./internal/app ./internal/tui/... -v`

Expected: PASS; `Esc` on a permission modal produces a durable deny and does not cancel the turn.

- [ ] **Step 5: Commit permission/diff UI**

```bash
git add internal/domain internal/app internal/permission internal/tui
git commit -m "feat: add permission and diff surfaces"
```

### Task 5: Implement first-run setup, model/mode commands, and sessions

**Files:**
- Create: `internal/config/save.go`
- Test: `internal/config/save_test.go`
- Create: `internal/app/session_state.go`
- Test: `internal/app/session_state_test.go`
- Create: `internal/tui/components/setup.go`
- Create: `internal/tui/components/sessions.go`
- Create: `internal/tui/components/palette.go`
- Test: `internal/tui/components/setup_test.go`
- Test: `internal/tui/components/sessions_test.go`
- Test: `internal/tui/components/palette_test.go`
- Modify: `internal/app/commands.go`
- Modify: `internal/app/app.go`
- Modify: `internal/tui/keys.go`
- Modify: `internal/tui/update.go`

**Interfaces:**
- Consumes: config, session list/load/create, app bridge.
- Produces: secure profile save, `/new`, `/sessions`, `/mode`, `/model`, `/compact`, `/help`, `/quit`.

- [ ] **Step 1: Write secret-free config save and session picker tests**

```go
func TestSaveGlobalNeverWritesAPIKey(t *testing.T) {
    path:=filepath.Join(t.TempDir(),"yordam","config.toml")
    cfg:=config.Config{ActiveProfile:"primary",Profiles:map[string]config.Profile{"primary":{BaseURL:"https://llm.example/v1",APIKeyEnv:"PRIMARY_KEY",Models:[]string{"m"},DefaultModel:"m"}},MaxToolCalls:32,ShellTimeoutSeconds:120}
    if err:=config.SaveGlobal(path,cfg);err!=nil{t.Fatal(err)}
    raw,err:=os.ReadFile(path);if err!=nil{t.Fatal(err)}
    if strings.Contains(string(raw),"actual-secret"){t.Fatal("secret persisted")}
    info,_:=os.Stat(path);if info.Mode().Perm()!=0o600{t.Fatalf("mode=%o",info.Mode().Perm())}
}

func TestSessionPickerFiltersAndSelects(t *testing.T) {
    picker:=components.NewSessions([]domain.SessionSummary{{ID:"1",Title:"fix auth"},{ID:"2",Title:"cache work"}})
    picker.SetFilter("auth")
    if got:=picker.Visible();len(got)!=1||got[0].ID!="1"{t.Fatalf("visible=%v",got)}
    if selected:=picker.Select();selected!="1"{t.Fatalf("selected=%q",selected)}
}
```

- [ ] **Step 2: Run setup/session tests**

Run: `go test ./internal/config ./internal/tui/components -run 'TestSaveGlobal|TestSessionPicker' -v`

Expected: FAIL until save/setup/session components exist.

- [ ] **Step 3: Implement atomic global config save and setup fields**

`SaveGlobal` validates config, TOML-encodes only exported config fields, writes a `0o600` same-directory temp file, syncs, renames, and syncs the parent directory. It rejects any config field whose name contains `key` except `api_key_env` during a reflection-based safety test.

Setup fields are profile name, base URL, key environment name, model, and process-only secret. Persist the first four; hold the secret in a TUI-owned byte slice, pass it to bootstrap through an in-memory option, and zero that slice after client creation.

First-run setup is a pre-runtime Bubble Tea stage inside `tui.Run`: load/validate global config; on not-found or no usable resolved profile, run `components.Setup` without constructing provider/tools/app. Submit calls `SaveGlobal` for the non-secret fields and returns the process-only key bytes to `tui.Run`; cancel exits cleanly. `tui.Run` then calls `app.Bootstrap` once with the saved config and in-memory key, zeroes the setup slice immediately after bootstrap returns, and starts the normal app-backed model. The setup component never receives an app/provider/tool handle, and the normal TUI still communicates only through app commands/events.

Sessions picker sorts by `UpdatedAt` descending, filters title/ID, and sends `CommandOpenSession`. `/new` sends `CommandNewSession`; `/mode` and `/model` open pickers and queue changes if the turn is active.

`new_session` and `open_session` are rejected while a turn or compaction is active. Opening a session loads/replays it first, verifies its workspace matches the canonical current workspace, then replaces UI state only after the load succeeds.

Add `new_session` and `open_session` command kinds and a `SessionID` command field. `session_state.go` implements `ProjectSessionState(replay)`: start from replay metadata, replay `mode.changed` and `model.changed` payloads in sequence order, and return the effective mode/selection used by the next turn. On resume, bootstrap uses that projected state and `permission.Restore(replay)`.

Mode/model application is durability-first. If no turn is active, append `mode.changed` or `model.changed`; only after append succeeds update in-memory state/policy and emit the new state. During a turn, retain one latest queued mode and one latest queued selection, then apply mode followed by model after the terminal turn event. Validate a model selection against configured profile/model pairs before append. `CommandAcknowledgeAutoShell` is accepted only in auto mode: append `trusted_execution.acknowledged`, then call `AcknowledgeAutoShell`; leaving auto clears it through `SetMode`. Add table tests for immediate changes, last-write-wins queued changes, append failure leaving state unchanged, resume projection, and acknowledgement surviving resume only when no later event leaves auto.

`/compact` is available only while idle; during a turn it produces a visible rejected event. On success it refreshes replay from the store so the next request uses the new durable summary; on interruption it leaves the previous context unchanged.

- [ ] **Step 4: Add slash-command table tests and run suites**

Add one table row per required command with expected app command or screen. Run:

`gofmt -w internal/config internal/app internal/tui && go test -race ./internal/config ./internal/app ./internal/tui/... -v`

Expected: PASS; persisted TOML contains no secret value.

- [ ] **Step 5: Commit setup and navigation**

```bash
git add internal/config internal/app internal/tui
git commit -m "feat: add setup model mode and session flows"
```

### Task 6: Compose the runtime, run the TUI, and pass PTY Gate 4

**Files:**
- Create: `internal/app/bootstrap.go`
- Create: `internal/tui/run.go`
- Create: `internal/ptytest/yordam_test.go`
- Create: `internal/tui/testdata/scripted-sse.txt`
- Create: `cmd/yordam/main.go`
- Modify: `docs/superpowers/plans/2026-07-13-yordam-v0.1-roadmap.md`

**Interfaces:**
- Consumes: every Phase 1-4 package.
- Produces: runnable `yordam`, clean terminal lifecycle, scripted interactive smoke evidence.

- [ ] **Step 1: Write version, first-screen, resize, and cancellation PTY tests**

```go
func TestVersionAndInteractiveTerminalRestoration(t *testing.T) {
    binary:=buildYordam(t)
    output,err:=exec.Command(binary,"--version").CombinedOutput()
    if err!=nil||!strings.Contains(string(output),"yordam dev"){t.Fatalf("output=%q err=%v",output,err)}
    command:=exec.Command(binary,"--config",fixtureConfig(t),"--data-dir",t.TempDir())
    terminal,err:=pty.Start(command);if err!=nil{t.Fatal(err)}
    defer terminal.Close()
    waitFor(t,terminal,"ask")
    if _,err:=terminal.Write([]byte{0x1b});err!=nil{t.Fatal(err)}
    if _,err:=terminal.Write([]byte{3});err!=nil{t.Fatal(err)}
    done:=make(chan error,1);go func(){done<-command.Wait()}()
    select{case <-done:case<-time.After(3*time.Second):t.Fatal("TUI did not restore terminal and exit")}
}
```

- [ ] **Step 2: Run PTY test and observe missing composition**

Run: `go test ./internal/ptytest -run TestVersionAndInteractiveTerminalRestoration -v`

Expected: FAIL because bootstrap and `tui.Run` are missing.

- [ ] **Step 3: Implement composition root and TUI lifecycle**

Run `go get github.com/creack/pty@v1.1.24` before implementing the PTY fixture helpers.

`app.Bootstrap` must:

1. canonicalize cwd and derive workspace;
2. receive validated config plus optional process-only key from the pre-runtime setup stage and resolve profile/model;
3. create JSONL store under resolved data dir;
4. create/resume the requested session and reject workspace mismatch;
5. build redactor from actual provider key values and, only when `--debug-log` is set, open that path with `0o600` and create the redacted logger;
6. create the profile router/provider with redaction, artifacts, bounded-output read/search/edit/shell tools, fixed registry, restored session policy, agent runner, and app bridge;
7. return the app plus initial UI snapshot without starting Bubble Tea.

`tui.Run` starts `app.Run` in a cancellable goroutine, runs `tea.NewProgram(NewModel(...)).Run()`, cancels the app on exit, waits for app shutdown, and returns the first non-cancellation error. The root view uses alt-screen mode and Bubble Tea restores terminal state.

Embed deterministic SSE fixtures only in tests; production always uses the configured HTTP endpoint.

Create the process entry point only after `tui.Run` exists, keeping every earlier commit buildable:

```go
// cmd/yordam/main.go
package main

import (
    "context"
    "fmt"
    "os"

    "github.com/muratmirgun/yordam/internal/buildinfo"
    "github.com/muratmirgun/yordam/internal/cli"
    "github.com/muratmirgun/yordam/internal/tui"
)

func main() {
    options,err:=cli.Parse(os.Args[1:]); if err!=nil{fmt.Fprintln(os.Stderr,"yordam:",err);os.Exit(2)}
    if options.Version { fmt.Printf("yordam %s (%s, %s)\n",buildinfo.Version,buildinfo.Commit,buildinfo.Date);return }
    if err:=tui.Run(context.Background(),options);err!=nil{fmt.Fprintln(os.Stderr,"yordam:",err);os.Exit(1)}
}
```

- [ ] **Step 4: Run Gate 4 commands**

```bash
go mod tidy
gofmt -w cmd internal
go test -race ./internal/tui/... ./internal/ptytest/...
go test ./...
go vet ./...
go run ./cmd/yordam --version
git diff --check
```

Expected: tests PASS; version output starts `yordam dev`; TUI smoke test exits within three seconds and restores the terminal.

- [ ] **Step 5: Mark Gate 4 and commit**

Change only Gate 4 in the roadmap to `[x]`, then run:

```bash
git add go.mod go.sum cmd internal docs/superpowers/plans/2026-07-13-yordam-v0.1-roadmap.md
git commit -m "feat: ship adaptive Yordam TUI"
```
