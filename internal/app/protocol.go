package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/orchestrator"
	"github.com/muratmirgun/yordam/internal/protocol"
)

const (
	CommandKindStartTask             = "start_task"
	CommandKindStartControlOperation = "start_control_operation"
	CommandKindCancel                = "cancel"
	CommandKindRequestSnapshot       = "request_snapshot"
	CommandKindSubscribeEvents       = "subscribe_events"
)

const (
	codeInvalidProtocolVersion = "invalid_protocol_version"
	codeInvalidCommand         = "invalid_command"
	codeInvalidPayload         = "invalid_payload"
	codeUnsupportedCommand     = "unsupported_command"
	codeInvalidRequestDigest   = "invalid_request_digest"
	codeIdempotencyConflict    = "idempotency_conflict"
)

// Service is the versioned application boundary. Implementations return Go
// errors only for unavailable infrastructure; request failures are stable
// PublicError values in serializable results or subscription terminals.
type Service interface {
	Execute(context.Context, protocol.Command) (protocol.CommandResult, error)
	Snapshot(context.Context, protocol.SnapshotRequest) (protocol.ApplicationSnapshot, error)
	SnapshotAndSubscribe(context.Context, protocol.SnapshotRequest) (protocol.ApplicationSnapshot, Subscription, error)
	Subscribe(context.Context, protocol.SubscriptionRequest) (Subscription, error)
}

type Subscription interface {
	Next(context.Context) (protocol.SubscriptionItem, error)
	Close() error
}

type ApplicationOrchestrator interface {
	LookupCommand(context.Context, protocol.JournalRef, protocol.CommandID, protocol.Digest) (protocol.CommandResult, bool, error)
	CommitPureCommand(context.Context, orchestrator.CommandMetadata, orchestrator.PureCommandCompletion) (protocol.CommandResult, error)
}

// CommandDispatcher is the composition seam used by Task 12. It receives the
// exact orchestrator metadata and a strictly decoded payload; it cannot append
// journal events through this package.
type CommandDispatcher interface {
	DispatchCommand(context.Context, orchestrator.CommandMetadata, protocol.Command, any) (protocol.CommandResult, error)
}

type ProtocolServiceOptions struct {
	Orchestrator     ApplicationOrchestrator
	Dispatcher       CommandDispatcher
	Broker           *Broker
	WorkspaceControl protocol.JournalRef
}

type ProtocolService struct {
	orchestrator     ApplicationOrchestrator
	dispatcher       CommandDispatcher
	broker           *Broker
	workspaceControl protocol.JournalRef
}

func NewProtocolService(options ProtocolServiceOptions) (*ProtocolService, error) {
	if options.Orchestrator == nil {
		return nil, fmt.Errorf("application orchestrator is required")
	}
	if err := options.WorkspaceControl.Validate(); err != nil || options.WorkspaceControl.Kind != protocol.JournalWorkspaceControl {
		return nil, fmt.Errorf("workspace-control journal is invalid")
	}
	return &ProtocolService{
		orchestrator: options.Orchestrator, dispatcher: options.Dispatcher,
		broker: options.Broker, workspaceControl: options.WorkspaceControl,
	}, nil
}

func NewApplicationService(options ProtocolServiceOptions) (*ProtocolService, error) {
	return NewProtocolService(options)
}

type protocolError struct {
	code      string
	message   string
	retryable bool
	cause     error
}

func (e *protocolError) Error() string { return e.message }
func (e *protocolError) Unwrap() error { return e.cause }

func ErrorCode(err error) string {
	var typed *protocolError
	if errors.As(err, &typed) {
		return typed.code
	}
	if errors.Is(err, orchestrator.ErrIdempotencyConflict) {
		return codeIdempotencyConflict
	}
	return ""
}

func requestError(code, message string, cause error) error {
	return &protocolError{code: code, message: message, cause: cause}
}

type commandPayloadFactory func() any

var commandPayloads = map[string]commandPayloadFactory{
	string(CommandStartTurn):            func() any { return &protocol.StartTurnCommandV1{} },
	CommandKindStartTask:                func() any { return &protocol.StartTaskCommandV1{} },
	CommandKindStartControlOperation:    func() any { return &protocol.StartControlOperationCommandV1{} },
	CommandKindCancel:                   func() any { return &protocol.CancelCommandV1{} },
	string(CommandCancelTurn):           func() any { return &protocol.CancelCommandV1{} },
	string(CommandResolvePermission):    func() any { return &protocol.ResolvePermissionCommandV1{} },
	string(CommandChangeMode):           func() any { return &protocol.ChangeModeCommandV1{} },
	string(CommandChangeModel):          func() any { return &protocol.ChangeModelCommandV1{} },
	string(CommandAcknowledgeAutoShell): func() any { return &protocol.TrustedShellCommandV1{} },
	string(CommandOpenSession):          func() any { return &protocol.OpenSessionCommandV1{} },
	string(CommandCompact):              func() any { return &protocol.EmptyCommandV1{} },
	string(CommandReloadConfig):         func() any { return &protocol.EmptyCommandV1{} },
	string(CommandTrustSkillCatalog):    func() any { return &protocol.SkillTrustCommandV1{} },
	string(CommandNewSession):           func() any { return &protocol.EmptyCommandV1{} },
	string(CommandShutdown):             func() any { return &protocol.EmptyCommandV1{} },
	CommandKindRequestSnapshot:          func() any { return &protocol.EmptyCommandV1{} },
	CommandKindSubscribeEvents:          func() any { return &protocol.EmptyCommandV1{} },
}

func DecodeCommandPayload(command protocol.Command) (any, error) {
	if command.ProtocolVersion != protocol.ApplicationProtocolVersion {
		return nil, requestError(codeInvalidProtocolVersion, "unsupported application protocol version", nil)
	}
	if command.PayloadVersion != 1 {
		return nil, requestError(codeInvalidPayload, "unsupported command payload version", nil)
	}
	if err := command.Validate(); err != nil {
		return nil, requestError(codeInvalidCommand, "invalid application command", err)
	}
	if err := validateCommandExpectation(command.Expected); err != nil {
		return nil, requestError(codeInvalidCommand, "invalid command cursor expectation", err)
	}
	factory, ok := commandPayloads[command.Kind]
	if !ok {
		return nil, requestError(codeUnsupportedCommand, "unsupported application command", nil)
	}
	payload := factory()
	decoder := json.NewDecoder(bytes.NewReader(command.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(payload); err != nil {
		return nil, requestError(codeInvalidPayload, "invalid command payload", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, requestError(codeInvalidPayload, "invalid command payload", err)
	}
	if err := validateDecodedCommand(command.Kind, payload); err != nil {
		return nil, requestError(codeInvalidPayload, "invalid command payload", err)
	}
	return payload, nil
}

func validateCommandExpectation(expectation *protocol.CommandExpectation) error {
	if expectation == nil {
		return nil
	}
	if expectation.WorkspaceControl != nil {
		if *expectation.WorkspaceControl != (protocol.CommittedCursor{}) && (expectation.WorkspaceControl.Validate() != nil || expectation.WorkspaceControl.JournalKind != protocol.JournalWorkspaceControl) {
			return fmt.Errorf("workspace-control cursor is invalid")
		}
	}
	if expectation.Session != nil {
		if err := expectation.Session.Validate(); err != nil || expectation.Session.JournalKind != protocol.JournalSession || expectation.SelectedSessionID == "" || expectation.Session.JournalID != protocol.JournalID(expectation.SelectedSessionID) {
			return fmt.Errorf("selected-session cursor is invalid")
		}
	}
	return nil
}

func ValidateApplicationCursor(cursor protocol.ApplicationCursor, selected protocol.SessionID) error {
	if err := cursor.WorkspaceControl.Validate(); err != nil || cursor.WorkspaceControl.JournalKind != protocol.JournalWorkspaceControl {
		return fmt.Errorf("workspace-control cursor is invalid")
	}
	if cursor.Stream.Epoch == "" {
		return fmt.Errorf("stream cursor epoch is required")
	}
	if selected == "" {
		if cursor.SelectedSession != nil || len(cursor.RelatedSessions) != 0 {
			return fmt.Errorf("unselected cursor carries a session component")
		}
		return nil
	}
	if cursor.SelectedSession == nil || cursor.SelectedSession.Validate() != nil || cursor.SelectedSession.JournalKind != protocol.JournalSession || cursor.SelectedSession.JournalID != protocol.JournalID(selected) {
		return fmt.Errorf("selected-session cursor is invalid")
	}
	return validateRelatedSessionCursors(cursor.RelatedSessions, cursor.SelectedSession)
}

func validateRelatedSessionCursors(related []protocol.CommittedCursor, selected *protocol.CommittedCursor) error {
	for index, cursor := range related {
		if cursor.Validate() != nil || cursor.JournalKind != protocol.JournalSession || selected == nil || cursor.JournalID == selected.JournalID || index > 0 && related[index-1].JournalID >= cursor.JournalID {
			return fmt.Errorf("related-session cursor is invalid")
		}
	}
	return nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
}

func validateDecodedCommand(kind string, payload any) error {
	switch value := payload.(type) {
	case *protocol.StartTurnCommandV1:
		if strings.TrimSpace(value.Prompt) == "" {
			return fmt.Errorf("prompt is required")
		}
	case *protocol.StartTaskCommandV1:
		if strings.TrimSpace(value.Goal) == "" {
			return fmt.Errorf("goal is required")
		}
		for _, criterion := range value.Criteria {
			if criterion.ID == "" || criterion.Description == "" || criterion.VerificationMethod == "" || criterion.ExpectedEvidenceKind == "" {
				return fmt.Errorf("task criterion is incomplete")
			}
		}
	case *protocol.StartControlOperationCommandV1:
		if value.ControlOperationID == "" || strings.TrimSpace(value.Kind) == "" || strings.TrimSpace(value.Purpose) == "" {
			return fmt.Errorf("control operation is incomplete")
		}
		if err := protocol.ValidateRawJSON(value.Payload); err != nil {
			return err
		}
	case *protocol.CancelCommandV1:
		return value.Validate()
	case *protocol.ResolvePermissionCommandV1:
		if value.Response.RequestID == "" || value.Response.Action == "" || value.Response.Lifetime == "" || value.Response.ScopeDigest.Validate() != nil || value.Response.Actor.Validate() != nil {
			return fmt.Errorf("permission response is incomplete")
		}
	case *protocol.ChangeModeCommandV1:
		if value.Mode == "" {
			return fmt.Errorf("mode is required")
		}
	case *protocol.ChangeModelCommandV1:
		if value.ProviderID == "" || value.ModelID == "" {
			return fmt.Errorf("model selection is incomplete")
		}
	case *protocol.TrustedShellCommandV1:
		if value.RequestID == "" {
			return fmt.Errorf("trusted-shell request ID is required")
		}
	case *protocol.OpenSessionCommandV1:
		if value.SessionID == "" {
			return fmt.Errorf("session ID is required")
		}
	case *protocol.SkillTrustCommandV1:
		return value.Validate()
	case *protocol.EmptyCommandV1:
	default:
		return fmt.Errorf("unsupported decoded command %q", kind)
	}
	return nil
}

func CanonicalRequestDigest(command protocol.Command) (protocol.Digest, error) {
	validated := protocol.CloneCommand(command)
	if validated.RequestDigest.Validate() != nil {
		validated.RequestDigest = protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("0", 64)}
	}
	decoded, err := DecodeCommandPayload(validated)
	if err != nil {
		return protocol.Digest{}, err
	}
	actor := command.Actor
	actor.ID = protocol.ActorID(strings.TrimSpace(string(actor.ID)))
	actor.Source = strings.TrimSpace(actor.Source)
	if err := actor.Validate(); err != nil {
		return protocol.Digest{}, requestError(codeInvalidCommand, "invalid command actor", err)
	}
	return canonicaljson.Digest(struct {
		ProtocolVersion uint32                       `json:"protocol_version"`
		Actor           protocol.ActorRef            `json:"actor"`
		Expected        *protocol.CommandExpectation `json:"expected,omitempty"`
		Kind            string                       `json:"kind"`
		PayloadVersion  uint32                       `json:"payload_version"`
		Payload         any                          `json:"payload"`
	}{command.ProtocolVersion, actor, protocol.DeepCopy(command.Expected), command.Kind, command.PayloadVersion, decoded})
}

func (s *ProtocolService) Execute(ctx context.Context, command protocol.Command) (protocol.CommandResult, error) {
	decoded, err := DecodeCommandPayload(command)
	if err != nil {
		return failedCommand(command, ErrorCode(err), err.Error(), false), nil
	}
	digest, err := CanonicalRequestDigest(command)
	if err != nil {
		return failedCommand(command, ErrorCode(err), err.Error(), false), nil
	}
	if digest != command.RequestDigest {
		return failedCommand(command, codeInvalidRequestDigest, "request digest does not match canonical command", false), nil
	}
	ref, err := s.commandJournal(command, decoded)
	if err != nil {
		return failedCommand(command, ErrorCode(err), err.Error(), false), nil
	}
	if durable, ok, lookupErr := s.orchestrator.LookupCommand(ctx, ref, command.CommandID, digest); lookupErr != nil {
		if errors.Is(lookupErr, orchestrator.ErrIdempotencyConflict) {
			return failedCommand(command, codeIdempotencyConflict, "command ID was already used for a different request", false), nil
		}
		if errors.Is(lookupErr, context.Canceled) || errors.Is(lookupErr, context.DeadlineExceeded) {
			return failedCommand(command, "cancelled", "command execution was cancelled", false), nil
		}
		return protocol.CommandResult{}, lookupErr
	} else if ok {
		return durable, nil
	}
	metadata := orchestrator.CommandMetadata{CommandID: command.CommandID, IdempotencyKey: command.IdempotencyKey, RequestDigest: digest, Actor: command.Actor}
	if command.Kind == CommandKindRequestSnapshot || command.Kind == CommandKindSubscribeEvents {
		return s.executePure(ctx, command, metadata, ref)
	}
	if s.dispatcher == nil {
		return failedCommand(command, "service_unavailable", "command dispatcher is unavailable", true), nil
	}
	result, dispatchErr := s.dispatcher.DispatchCommand(ctx, metadata, protocol.CloneCommand(command), protocol.DeepCopy(decoded))
	if dispatchErr != nil {
		return commandFailureResult(command, dispatchErr)
	}
	return result, nil
}

func (s *ProtocolService) commandJournal(command protocol.Command, decoded any) (protocol.JournalRef, error) {
	workspaceCommand := command.Kind == CommandKindStartControlOperation || command.Kind == string(CommandReloadConfig) ||
		command.Kind == string(CommandTrustSkillCatalog) ||
		command.Kind == string(CommandNewSession) || command.Kind == string(CommandOpenSession) ||
		command.Kind == string(CommandChangeMode) || command.Kind == string(CommandChangeModel) ||
		command.Kind == string(CommandAcknowledgeAutoShell) ||
		command.Kind == CommandKindRequestSnapshot || command.Kind == CommandKindSubscribeEvents
	if cancel, ok := decoded.(*protocol.CancelCommandV1); ok && cancel.ControlOperationID != "" {
		workspaceCommand = true
	}
	if workspaceCommand {
		if command.Expected != nil && command.Expected.WorkspaceControl != nil && *command.Expected.WorkspaceControl != (protocol.CommittedCursor{}) && cursorJournal(*command.Expected.WorkspaceControl) != s.workspaceControl {
			return protocol.JournalRef{}, requestError(codeInvalidCommand, "workspace-control expectation does not match service", nil)
		}
		return s.workspaceControl, nil
	}
	if command.Expected == nil || command.Expected.SelectedSessionID == "" || command.Expected.Session == nil {
		return protocol.JournalRef{}, requestError(codeInvalidCommand, "session command requires selected-session expectation", nil)
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(command.Expected.SelectedSessionID)}
	if cursorJournal(*command.Expected.Session) != ref {
		return protocol.JournalRef{}, requestError(codeInvalidCommand, "session expectation does not match selected session", nil)
	}
	return ref, nil
}

func (s *ProtocolService) executePure(ctx context.Context, command protocol.Command, metadata orchestrator.CommandMetadata, ref protocol.JournalRef) (protocol.CommandResult, error) {
	var payload json.RawMessage
	var err error
	if command.Kind == CommandKindRequestSnapshot {
		if s.broker == nil {
			return failedCommand(command, "service_unavailable", "snapshot service is unavailable", true), nil
		}
		selected := protocol.SessionID("")
		if command.Expected != nil {
			selected = command.Expected.SelectedSessionID
		}
		snapshot, snapshotErr := s.broker.Snapshot(ctx, protocol.SnapshotRequest{ProtocolVersion: protocol.ApplicationProtocolVersion, SelectedSessionID: selected, Consumer: "command", QueueCapacity: 1})
		if snapshotErr != nil {
			return protocol.CommandResult{}, snapshotErr
		}
		payload, err = canonicaljson.Marshal(snapshot)
	} else {
		cursor := protocol.ApplicationCursor{}
		if command.Expected != nil {
			if command.Expected.WorkspaceControl != nil {
				cursor.WorkspaceControl = *command.Expected.WorkspaceControl
			}
			cursor.SelectedSession = protocol.DeepCopy(command.Expected.Session)
		}
		if s.broker != nil {
			cursor.Stream = protocol.StreamCursor{Epoch: s.broker.Epoch()}
		}
		payload, err = canonicaljson.Marshal(struct {
			Cursor protocol.ApplicationCursor `json:"cursor"`
		}{cursor})
	}
	if err != nil {
		return protocol.CommandResult{}, err
	}
	expected, err := expectedHead(command, ref)
	if err != nil {
		return failedCommand(command, codeInvalidCommand, err.Error(), false), nil
	}
	result, err := s.orchestrator.CommitPureCommand(ctx, metadata, orchestrator.PureCommandCompletion{Journal: ref, ExpectedHead: expected, PayloadVersion: 1, Payload: payload})
	if errors.Is(err, orchestrator.ErrIdempotencyConflict) {
		return failedCommand(command, codeIdempotencyConflict, "command ID was already used for a different request", false), nil
	}
	if err != nil {
		return commandFailureResult(command, err)
	}
	return result, nil
}

func expectedHead(command protocol.Command, ref protocol.JournalRef) (protocol.CommittedCursor, error) {
	if command.Expected == nil {
		return protocol.CommittedCursor{}, fmt.Errorf("pure command requires an expected cursor")
	}
	if ref.Kind == protocol.JournalWorkspaceControl && command.Expected.WorkspaceControl != nil {
		return *command.Expected.WorkspaceControl, nil
	}
	if ref.Kind == protocol.JournalSession && command.Expected.Session != nil {
		return *command.Expected.Session, nil
	}
	return protocol.CommittedCursor{}, fmt.Errorf("pure command expected cursor is missing")
}

func failedCommand(command protocol.Command, code, message string, retryable bool) protocol.CommandResult {
	if code == "" {
		code = codeInvalidCommand
	}
	return protocol.CommandResult{
		ProtocolVersion: protocol.ApplicationProtocolVersion, CommandID: command.CommandID, Status: "failed",
		RequestDigest: command.RequestDigest, PayloadVersion: 1, Payload: json.RawMessage(`{}`),
		Error: &protocol.PublicError{Code: code, Message: message, Retryable: retryable},
	}
}

func commandFailureResult(command protocol.Command, err error) (protocol.CommandResult, error) {
	switch {
	case errors.Is(err, orchestrator.ErrCommitUncertain):
		return failedCommand(command, "commit_uncertain", "command commit outcome is uncertain", false), nil
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return failedCommand(command, "cancelled", "command execution was cancelled", false), nil
	}
	if code := ErrorCode(err); code != "" {
		return failedCommand(command, code, err.Error(), false), nil
	}
	return protocol.CommandResult{}, err
}

func (s *ProtocolService) SnapshotAndSubscribe(ctx context.Context, request protocol.SnapshotRequest) (protocol.ApplicationSnapshot, Subscription, error) {
	if s.broker == nil {
		return protocol.ApplicationSnapshot{}, nil, fmt.Errorf("snapshot service is unavailable")
	}
	return s.broker.SnapshotAndSubscribe(ctx, request)
}

func (s *ProtocolService) Snapshot(ctx context.Context, request protocol.SnapshotRequest) (protocol.ApplicationSnapshot, error) {
	return s.broker.Snapshot(ctx, request)
}

func (s *ProtocolService) Subscribe(ctx context.Context, request protocol.SubscriptionRequest) (Subscription, error) {
	if s.broker == nil {
		return nil, fmt.Errorf("subscription service is unavailable")
	}
	return s.broker.Subscribe(ctx, request)
}

func cursorJournal(cursor protocol.CommittedCursor) protocol.JournalRef {
	return protocol.JournalRef{Kind: cursor.JournalKind, ID: cursor.JournalID}
}

var _ Service = (*ProtocolService)(nil)
