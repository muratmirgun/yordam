package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/orchestrator"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
)

type Runner struct {
	Provider         ports.ModelProvider
	Tools            ports.ToolRegistry
	Policy           ports.PermissionPolicy
	Approver         ports.PermissionApprover
	Sessions         ports.SessionStore
	MaxToolCalls     int
	SystemPrompt     string
	Redact           func(string) string
	Admission        *secret.Lease
	NewDeltaRedactor func() DeltaRedactor
	Sink             Sink
}

type DeltaRedactor interface {
	Write(string) string
	Close() string
}

type bufferedDeltaRedactor struct {
	redact  func(string) string
	pending string
}

type passthroughDeltaRedactor struct{}

func (passthroughDeltaRedactor) Write(value string) string { return value }
func (passthroughDeltaRedactor) Close() string             { return "" }

func (r *bufferedDeltaRedactor) Write(value string) string {
	r.pending += value
	return ""
}

func (r *bufferedDeltaRedactor) Close() string {
	value := r.pending
	if r.redact != nil {
		value = r.redact(value)
	}
	r.pending = ""
	return value
}

type RunInput struct {
	Session domain.Session
	Replay  domain.SessionReplay
	Prompt  string
}

type TurnOrchestrator interface {
	RunTurn(context.Context, orchestrator.StartTurnRequest) (orchestrator.RunResult, error)
}

// OrchestratedRunner is the Gate 1 compatibility facade. Runner remains the
// production implementation until composition switches atomically in Task 12.
type OrchestratedRunner struct {
	Orchestrator TurnOrchestrator
	Prepare      func(context.Context, RunInput) (orchestrator.StartTurnRequest, error)
}

func (r OrchestratedRunner) RunTurn(ctx context.Context, input RunInput) error {
	if r.Orchestrator == nil || r.Prepare == nil {
		return fmt.Errorf("orchestrated runner is not configured")
	}
	if input.Session.ID == "" || strings.TrimSpace(input.Prompt) == "" {
		return fmt.Errorf("legacy run input is incomplete")
	}
	request, err := r.Prepare(ctx, input)
	if err != nil {
		return err
	}
	request.SessionID = protocol.SessionID(input.Session.ID)
	request.Prompt = input.Prompt
	_, err = r.Orchestrator.RunTurn(ctx, request)
	return err
}

func (r Runner) emit(event RuntimeEvent) {
	if r.Sink != nil {
		r.Sink(event)
	}
}

func providerErrorKind(err error) domain.ErrorKind {
	var typed *domain.TypedError
	if errors.As(err, &typed) {
		return typed.Kind
	}
	return domain.ErrorProviderFatal
}

func (r Runner) appendTerminal(ctx context.Context, sessionID string, kind domain.EventKind, payload domain.TurnTerminalPayload, cause error) error {
	payload.Reason = r.redact(payload.Reason)
	_, appendErr := r.Sessions.Append(context.WithoutCancel(ctx), sessionID, kind, payload)
	if appendErr != nil {
		appendErr = fmt.Errorf("append %s: %w", kind, appendErr)
	}
	return errors.Join(cause, appendErr)
}

func (r Runner) interrupt(ctx context.Context, sessionID string, err error) error {
	return r.appendTerminal(ctx, sessionID, domain.EventTurnInterrupted, domain.TurnTerminalPayload{Reason: err.Error(), ErrorKind: domain.ErrorCancelled}, err)
}

func (r Runner) RunTurn(ctx context.Context, input RunInput) error {
	if input.Prompt == "" {
		return fmt.Errorf("prompt is empty")
	}
	max := r.MaxToolCalls
	if max == 0 {
		max = 32
	}
	if max < 1 || max > 128 {
		return fmt.Errorf("max tool calls must be 1..128")
	}
	messages, err := BuildContext(input.Replay, ComposeSystemPrompt(r.SystemPrompt, input.Session.Mode), r.Admission)
	if err != nil {
		return fmt.Errorf("build admitted model context: %w", err)
	}
	prompt := r.redact(input.Prompt)
	if _, err := r.Sessions.Append(ctx, input.Session.ID, domain.EventUserMessage, domain.MessagePayload{Content: prompt}); err != nil {
		return err
	}

	messages = append(messages, domain.Message{Role: domain.RoleUser, Content: prompt})
	completedTools := 0
	for {
		if err := ctx.Err(); err != nil {
			return r.interrupt(ctx, input.Session.ID, err)
		}

		r.emit(RuntimeEvent{Kind: RuntimeStateChanged, State: "streaming_model"})
		selection, err := admitSelection(input.Session.Selection, r.Admission)
		if err != nil {
			return r.appendTerminal(ctx, input.Session.ID, domain.EventTurnFailed, domain.TurnTerminalPayload{Reason: err.Error(), ErrorKind: domain.ErrorProviderFatal}, err)
		}
		descriptors, err := admitToolDescriptors(r.Tools.Descriptors(), r.Admission)
		if err != nil {
			return r.appendTerminal(ctx, input.Session.ID, domain.EventTurnFailed, domain.TurnTerminalPayload{Reason: err.Error(), ErrorKind: domain.ErrorProviderFatal}, err)
		}
		stream, err := r.Provider.Stream(ctx, domain.ModelRequest{
			Selection: selection,
			Messages:  messages,
			Tools:     descriptors,
		})
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return r.interrupt(ctx, input.Session.ID, err)
			}
			return r.appendTerminal(ctx, input.Session.ID, domain.EventTurnFailed, domain.TurnTerminalPayload{Reason: err.Error(), ErrorKind: providerErrorKind(err)}, err)
		}

		var text strings.Builder
		calls := make([]domain.ToolCall, 0)
		modelDone := false
		var deltaRedactor DeltaRedactor = passthroughDeltaRedactor{}
		if r.NewDeltaRedactor != nil {
			deltaRedactor = r.NewDeltaRedactor()
		} else if r.Redact != nil {
			deltaRedactor = &bufferedDeltaRedactor{redact: r.Redact}
		} else if r.Admission != nil {
			if admitted, err := r.Admission.RedactionStream(); err == nil {
				deltaRedactor = admitted
			}
		}
	streamLoop:
		for {
			var event domain.ModelEvent
			var ok bool
			select {
			case <-ctx.Done():
				return r.interrupt(ctx, input.Session.ID, ctx.Err())
			case event, ok = <-stream:
				if !ok {
					break streamLoop
				}
			}
			if event.Err != nil {
				kind := domain.ErrorProviderInterrupted
				var typed *domain.TypedError
				if errors.As(event.Err, &typed) {
					kind = typed.Kind
				}
				terminal := domain.EventTurnFailed
				if kind == domain.ErrorProviderInterrupted {
					terminal = domain.EventTurnInterrupted
				}
				if delta := deltaRedactor.Close(); delta != "" {
					r.emit(RuntimeEvent{Kind: RuntimeTextDelta, Text: delta})
				}
				return r.appendTerminal(ctx, input.Session.ID, terminal, domain.TurnTerminalPayload{Reason: event.Err.Error(), ErrorKind: kind}, event.Err)
			}
			if event.Kind == domain.ModelTextDelta {
				text.WriteString(event.Text)
				if delta := deltaRedactor.Write(event.Text); delta != "" {
					r.emit(RuntimeEvent{Kind: RuntimeTextDelta, Text: delta})
				}
			}
			if event.Kind == domain.ModelToolCall && event.ToolCall != nil {
				calls = append(calls, r.redactToolCall(*event.ToolCall))
			}
			if event.Kind == domain.ModelDone {
				modelDone = true
			}
		}
		if delta := deltaRedactor.Close(); delta != "" {
			r.emit(RuntimeEvent{Kind: RuntimeTextDelta, Text: delta})
		}
		if err := ctx.Err(); err != nil {
			return r.interrupt(ctx, input.Session.ID, err)
		}
		if !modelDone {
			err := fmt.Errorf("provider stream closed without done")
			return r.appendTerminal(ctx, input.Session.ID, domain.EventTurnInterrupted, domain.TurnTerminalPayload{Reason: err.Error(), ErrorKind: domain.ErrorProviderInterrupted}, err)
		}
		textValue := text.String()
		textValue = r.redact(textValue)
		if completedTools+len(calls) > max {
			err := fmt.Errorf("tool call limit %d reached", max)
			return r.appendTerminal(ctx, input.Session.ID, domain.EventTurnFailed, domain.TurnTerminalPayload{Reason: err.Error(), ErrorKind: domain.ErrorToolFailed}, err)
		}

		if textValue != "" || len(calls) > 0 {
			if _, err := r.Sessions.Append(ctx, input.Session.ID, domain.EventAssistantMessage, domain.MessagePayload{Content: textValue, ToolCalls: calls}); err != nil {
				return err
			}
			messages = append(messages, domain.Message{Role: domain.RoleAssistant, Content: textValue, ToolCalls: calls})
		}
		if err := ctx.Err(); err != nil {
			return r.interrupt(ctx, input.Session.ID, err)
		}
		if len(calls) == 0 {
			_, err := r.Sessions.Append(ctx, input.Session.ID, domain.EventTurnCompleted, domain.TurnTerminalPayload{Reason: "model completed"})
			return err
		}

		for _, call := range calls {
			request := domain.ToolRequest{
				CallID:    call.ID,
				Name:      call.Name,
				Input:     call.Arguments,
				Workspace: input.Session.Workspace.CanonicalPath,
			}
			var result domain.ToolResult
			var postToolErr error
			tool, ok := r.Tools.Lookup(call.Name)
			if !ok {
				result = domain.ToolResult{CallID: call.ID, Status: domain.ToolFailed, ErrorKind: domain.ErrorToolFailed, Content: "unknown tool"}
			} else {
				prepared, err := tool.Prepare(ctx, request)
				if err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						return r.interrupt(ctx, input.Session.ID, err)
					}
					result = domain.ToolResult{CallID: call.ID, Status: domain.ToolFailed, ErrorKind: domain.ErrorToolFailed, Content: err.Error()}
				} else {
					preview := prepared.Preview()
					preview.Mutation = tool.Descriptor().Mutation
					expectedApprovalScope := approvalScope(preview)
					decision := r.Policy.Evaluate(ctx, ports.PermissionContext{
						SessionID: input.Session.ID,
						Mode:      input.Session.Mode,
						Workspace: input.Session.Workspace.CanonicalPath,
					}, preview)
					preview = r.redactPreview(preview)
					preparer, canPreparePreview := prepared.(ports.PreviewPreparer)
					deferredOutsidePreview := canPreparePreview && preview.Mutation == domain.MutationFile && !preview.InsideWorkspace && preview.FilePlan == nil && decision.Action == domain.PermissionAsk
					if canPreparePreview && (decision.Action == domain.PermissionAllow || decision.Action == domain.PermissionAsk && preview.InsideWorkspace) {
						if err := preparer.PreparePreview(ctx); err != nil {
							result = domain.ToolResult{CallID: call.ID, Status: domain.ToolFailed, ErrorKind: domain.ErrorToolFailed, Content: err.Error()}
							goto persistResult
						}
						preview = prepared.Preview()
						preview.Mutation = tool.Descriptor().Mutation
						expectedApprovalScope = approvalScope(preview)
						preview = r.redactPreview(preview)
					}
					requestedPreview := preview
					if deferredOutsidePreview {
						requestedPreview.Summary = "inspect " + preview.CanonicalScope + " to prepare edit preview"
					}
					if _, err := r.Sessions.Append(ctx, input.Session.ID, domain.EventToolRequested, requestedPreview); err != nil {
						return err
					}
					approved := false
					if decision.Action == domain.PermissionAsk {
						if _, err := r.Sessions.Append(ctx, input.Session.ID, domain.EventPermissionRequested, requestedPreview); err != nil {
							return err
						}
						decision, err = r.Approver.Resolve(ctx, ports.PermissionPrompt{SessionID: input.Session.ID, Call: requestedPreview})
						if err != nil {
							if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
								return r.interrupt(ctx, input.Session.ID, err)
							}
							return r.appendTerminal(ctx, input.Session.ID, domain.EventTurnFailed, domain.TurnTerminalPayload{Reason: err.Error(), ErrorKind: domain.ErrorToolFailed}, err)
						}
						approved = true
					}
					if err := ctx.Err(); err != nil {
						return r.interrupt(ctx, input.Session.ID, err)
					}
					if approved && !validApprovalResponse(decision, expectedApprovalScope) {
						decision = domain.PermissionDecision{Action: domain.PermissionDeny, Lifetime: domain.PermissionOnce, Scope: expectedApprovalScope, Reason: "invalid approval response"}
					}
					if approved && decision.Action == domain.PermissionAllow {
						if deferredOutsidePreview {
							inspectionDecision := decision
							inspectionDecision.Lifetime = domain.PermissionOnce
							inspectionDecision.Reason = "authorized outside edit preview inspection"
							if _, err := r.Sessions.Append(ctx, input.Session.ID, domain.EventPermissionResolved, domain.PermissionPayload{CallID: call.ID, Tool: call.Name, Decision: inspectionDecision}); err != nil {
								return err
							}
						}
						if canPreparePreview && preview.FilePlan == nil {
							if err := preparer.PreparePreview(ctx); err != nil {
								result = domain.ToolResult{CallID: call.ID, Status: domain.ToolFailed, ErrorKind: domain.ErrorToolFailed, Content: err.Error()}
								goto persistResult
							}
							preview = prepared.Preview()
							preview.Mutation = tool.Descriptor().Mutation
							expectedApprovalScope = approvalScope(preview)
							preview = r.redactPreview(preview)
						}
						if deferredOutsidePreview {
							if _, err := r.Sessions.Append(ctx, input.Session.ID, domain.EventToolRequested, preview); err != nil {
								return err
							}
							if _, err := r.Sessions.Append(ctx, input.Session.ID, domain.EventPermissionRequested, preview); err != nil {
								return err
							}
							decision, err = r.Approver.Resolve(ctx, ports.PermissionPrompt{SessionID: input.Session.ID, Call: preview})
							if err != nil {
								if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
									return r.interrupt(ctx, input.Session.ID, err)
								}
								return r.appendTerminal(ctx, input.Session.ID, domain.EventTurnFailed, domain.TurnTerminalPayload{Reason: err.Error(), ErrorKind: domain.ErrorToolFailed}, err)
							}
							if !validApprovalResponse(decision, expectedApprovalScope) {
								decision = domain.PermissionDecision{Action: domain.PermissionDeny, Lifetime: domain.PermissionOnce, Scope: expectedApprovalScope, Reason: "invalid approval response"}
							}
						}
					}
					if err := ctx.Err(); err != nil {
						return r.interrupt(ctx, input.Session.ID, err)
					}
					var granter ports.PermissionGranter
					if approved && decision.Action == domain.PermissionAllow && decision.Lifetime == domain.PermissionSession {
						var ok bool
						granter, ok = r.Policy.(ports.PermissionGranter)
						if !ok {
							return fmt.Errorf("permission policy does not support session grants")
						}
					}
					if _, err := r.Sessions.Append(ctx, input.Session.ID, domain.EventPermissionResolved, domain.PermissionPayload{CallID: call.ID, Tool: call.Name, Decision: decision}); err != nil {
						return err
					}
					if granter != nil {
						granter.GrantSession(preview.Request.Name, expectedApprovalScope)
					}
					if err := ctx.Err(); err != nil {
						return r.interrupt(ctx, input.Session.ID, err)
					}

					if decision.Action == domain.PermissionDeny {
						result = domain.ToolResult{CallID: call.ID, Status: domain.ToolDenied, ErrorKind: domain.ErrorPermissionDenied, Content: "permission denied"}
					} else {
						if preview.FilePlan != nil {
							if _, err := r.Sessions.Append(ctx, input.Session.ID, domain.EventFileChangePlanned, preview.FilePlan); err != nil {
								return err
							}
						}
						if _, err := r.Sessions.Append(ctx, input.Session.ID, domain.EventToolStarted, map[string]string{"call_id": call.ID}); err != nil {
							return err
						}
						if err := ctx.Err(); err != nil {
							result = domain.ToolResult{CallID: call.ID, Status: domain.ToolCancelled, ErrorKind: domain.ErrorCancelled, Content: err.Error()}
							goto persistResult
						}
						r.emit(RuntimeEvent{Kind: RuntimeToolStarted})
						result = prepared.Execute(ctx)
						resultContext := context.WithoutCancel(ctx)
						if result.FileChange != nil {
							if _, err := r.Sessions.Append(resultContext, input.Session.ID, domain.EventFileChanged, result.FileChange); err != nil {
								postToolErr = err
							}
						}
					}
				}
			}
		persistResult:
			result = r.redactResult(result)
			resultContext := context.WithoutCancel(ctx)
			if _, err := r.Sessions.Append(resultContext, input.Session.ID, domain.EventToolResult, domain.ToolResultPayload{Result: result}); err != nil {
				return r.appendTerminal(resultContext, input.Session.ID, domain.EventTurnFailed, domain.TurnTerminalPayload{Reason: err.Error(), ErrorKind: domain.ErrorToolFailed}, err)
			}
			r.emit(RuntimeEvent{Kind: RuntimeToolCompleted, Result: &result})
			if postToolErr != nil {
				return r.appendTerminal(resultContext, input.Session.ID, domain.EventTurnFailed, domain.TurnTerminalPayload{Reason: postToolErr.Error(), ErrorKind: domain.ErrorToolFailed}, postToolErr)
			}
			toolContent, err := ToolResultContentLeased(result, r.Admission)
			if err != nil {
				return r.appendTerminal(resultContext, input.Session.ID, domain.EventTurnFailed, domain.TurnTerminalPayload{Reason: err.Error(), ErrorKind: domain.ErrorToolFailed}, err)
			}
			messages = append(messages, domain.Message{Role: domain.RoleTool, ToolCallID: r.redact(call.ID), Content: toolContent})
			completedTools++
			if err := ctx.Err(); err != nil {
				return r.interrupt(ctx, input.Session.ID, err)
			}
		}
	}
}

func (r Runner) redactPreview(preview domain.PreparedToolRequest) domain.PreparedToolRequest {
	if r.Redact == nil && r.Admission == nil {
		return preview
	}
	preview.Summary = r.redact(preview.Summary)
	preview.ProposedDiff = r.redact(preview.ProposedDiff)
	preview.ApprovalScope = r.redact(preview.ApprovalScope)
	preview.CanonicalScope = r.redact(preview.CanonicalScope)
	if preview.FilePlan != nil {
		plan := *preview.FilePlan
		plan.Diff = r.redact(plan.Diff)
		preview.FilePlan = &plan
	}
	return preview
}

func (r Runner) redact(value string) string {
	if r.Admission != nil {
		value = r.Admission.String(value)
	}
	if r.Redact != nil {
		value = r.Redact(value)
	}
	return value
}

func (r Runner) redactToolCall(call domain.ToolCall) domain.ToolCall {
	call.ID = r.redact(call.ID)
	call.Name = r.redact(call.Name)
	if r.Admission == nil || len(call.Arguments) == 0 {
		return call
	}
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(call.Arguments)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		call.Arguments = json.RawMessage(`{"error":"tool arguments unavailable"}`)
		return call
	}
	redacted, err := r.Admission.JSON(value)
	if err != nil {
		call.Arguments = json.RawMessage(`{"error":"tool arguments unavailable"}`)
		return call
	}
	call.Arguments = redacted
	return call
}

func (r Runner) redactResult(result domain.ToolResult) domain.ToolResult {
	result.CallID = r.redact(result.CallID)
	result.Content = r.redact(result.Content)
	for index, id := range result.ArtifactIDs {
		result.ArtifactIDs[index] = r.redact(id)
	}
	if result.FileChange != nil {
		change := *result.FileChange
		change.CallID = r.redact(change.CallID)
		change.Path = r.redact(change.Path)
		change.Diff = r.redact(change.Diff)
		for index, id := range change.ArtifactIDs {
			change.ArtifactIDs[index] = r.redact(id)
		}
		result.FileChange = &change
	}
	if result.WorkspaceChanges != nil {
		changes := *result.WorkspaceChanges
		changes.Status = r.redact(changes.Status)
		changes.Diff = r.redact(changes.Diff)
		changes.Notice = r.redact(changes.Notice)
		for index, id := range changes.ArtifactIDs {
			changes.ArtifactIDs[index] = r.redact(id)
		}
		result.WorkspaceChanges = &changes
	}
	return result
}

func validApprovalResponse(decision domain.PermissionDecision, scope string) bool {
	if decision.Action != domain.PermissionAllow && decision.Action != domain.PermissionDeny {
		return false
	}
	if decision.Lifetime != domain.PermissionOnce && decision.Lifetime != domain.PermissionSession {
		return false
	}
	return decision.Scope == scope
}

func approvalScope(preview domain.PreparedToolRequest) string {
	if preview.ApprovalScope != "" {
		return preview.ApprovalScope
	}
	return preview.CanonicalScope
}
