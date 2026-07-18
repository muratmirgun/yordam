package app

import "github.com/muratmirgun/yordam/internal/domain"

type CommandKind string

const (
	CommandStartTurn             CommandKind = "start_turn"
	CommandStartTask             CommandKind = "start_task"
	CommandStartControlOperation CommandKind = "start_control_operation"
	CommandCancel                CommandKind = "cancel"
	CommandCancelTurn            CommandKind = "cancel_turn"
	CommandResolvePermission     CommandKind = "resolve_permission"
	CommandChangeMode            CommandKind = "change_mode"
	CommandChangeModel           CommandKind = "change_model"
	CommandAcknowledgeAutoShell  CommandKind = "acknowledge_auto_shell"
	CommandCompact               CommandKind = "compact"
	CommandReloadConfig          CommandKind = "reload_config"
	CommandNewSession            CommandKind = "new_session"
	CommandOpenSession           CommandKind = "open_session"
	CommandShutdown              CommandKind = "shutdown"
	CommandRequestSnapshot       CommandKind = "request_snapshot"
	CommandSubscribeEvents       CommandKind = "subscribe_events"
)

type Command struct {
	Kind      CommandKind
	DraftID   uint64
	Prompt    string
	CallID    string
	Decision  domain.PermissionDecision
	Mode      domain.PermissionMode
	Selection domain.ModelSelection
	SessionID string
}
