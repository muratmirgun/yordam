package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/orchestrator"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestApplicationCommandCodecRejectsUnknownVersionKindAndFields(t *testing.T) {
	command := applicationCommand(t, "command-codec", string(app.CommandStartTurn), protocol.StartTurnCommandV1{Prompt: "hello"})
	decoded, err := app.DecodeCommandPayload(command)
	if err != nil {
		t.Fatal(err)
	}
	if payload, ok := decoded.(*protocol.StartTurnCommandV1); !ok || payload.Prompt != "hello" {
		t.Fatalf("decoded=%T %+v", decoded, decoded)
	}

	for name, mutate := range map[string]func(*protocol.Command){
		"protocol version": func(command *protocol.Command) { command.ProtocolVersion++ },
		"payload version":  func(command *protocol.Command) { command.PayloadVersion++ },
		"kind":             func(command *protocol.Command) { command.Kind = "unknown" },
		"unknown field": func(command *protocol.Command) {
			command.Payload = json.RawMessage(`{"prompt":"hello","service":true}`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := protocol.CloneCommand(command)
			mutate(&changed)
			if _, err := app.DecodeCommandPayload(changed); app.ErrorCode(err) == "" {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestApplicationCommandDigestBindsActorKindExpectationAndPayload(t *testing.T) {
	base := applicationCommand(t, "command-digest", string(app.CommandStartTurn), protocol.StartTurnCommandV1{Prompt: "hello"})
	mutations := []func(*protocol.Command){
		func(command *protocol.Command) { command.Actor.ID = "other-user" },
		func(command *protocol.Command) {
			command.Kind = app.CommandKindStartTask
			command.Payload = json.RawMessage(`{"goal":"hello"}`)
		},
		func(command *protocol.Command) { command.Expected.SelectedSessionID = "other-session" },
		func(command *protocol.Command) { command.Payload = json.RawMessage(`{"prompt":"different"}`) },
	}
	for index, mutate := range mutations {
		changed := protocol.CloneCommand(base)
		mutate(&changed)
		got, err := app.CanonicalRequestDigest(changed)
		if err != nil {
			t.Fatal(err)
		}
		if got == base.RequestDigest {
			t.Fatalf("mutation %d was not digest-bound", index)
		}
	}
}

func TestApplicationCursorValidationRejectsIncompleteOrMismatchedVectors(t *testing.T) {
	workspace := committedCursor(protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace-1"}, 1, "workspace")
	session := committedCursor(protocol.JournalRef{Kind: protocol.JournalSession, ID: "session-1"}, 1, "session")
	valid := protocol.ApplicationCursor{WorkspaceControl: workspace, SelectedSession: &session, Stream: protocol.StreamCursor{Epoch: "epoch-1", Seq: 2}}
	if err := app.ValidateApplicationCursor(valid, "session-1"); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*protocol.ApplicationCursor){
		"workspace": func(cursor *protocol.ApplicationCursor) { cursor.WorkspaceControl = protocol.CommittedCursor{} },
		"session":   func(cursor *protocol.ApplicationCursor) { cursor.SelectedSession.JournalID = "other" },
		"stream":    func(cursor *protocol.ApplicationCursor) { cursor.Stream.Epoch = "" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := protocol.DeepCopy(valid)
			mutate(&changed)
			if err := app.ValidateApplicationCursor(changed, "session-1"); err == nil {
				t.Fatal("invalid cursor vector was accepted")
			}
		})
	}
}

func TestApplicationCursorValidatesSortedUniqueRelatedSessionProvenance(t *testing.T) {
	workspace := committedCursor(protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace-1"}, 1, "workspace")
	parent := committedCursor(protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"}, 2, "parent")
	childA := committedCursor(protocol.JournalRef{Kind: protocol.JournalSession, ID: "child-a"}, 3, "child-a")
	childB := committedCursor(protocol.JournalRef{Kind: protocol.JournalSession, ID: "child-b"}, 4, "child-b")
	valid := protocol.ApplicationCursor{WorkspaceControl: workspace, SelectedSession: &parent, RelatedSessions: []protocol.CommittedCursor{childA, childB}, Stream: protocol.StreamCursor{Epoch: "epoch-1"}}
	if err := app.ValidateApplicationCursor(valid, "parent"); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*protocol.ApplicationCursor){
		"unsorted": func(cursor *protocol.ApplicationCursor) {
			cursor.RelatedSessions[0], cursor.RelatedSessions[1] = cursor.RelatedSessions[1], cursor.RelatedSessions[0]
		},
		"duplicate": func(cursor *protocol.ApplicationCursor) { cursor.RelatedSessions[1] = cursor.RelatedSessions[0] },
		"selected":  func(cursor *protocol.ApplicationCursor) { cursor.RelatedSessions[0] = parent },
		"workspace": func(cursor *protocol.ApplicationCursor) { cursor.RelatedSessions[0] = workspace },
	} {
		t.Run(name, func(t *testing.T) {
			changed := protocol.DeepCopy(valid)
			mutate(&changed)
			if err := app.ValidateApplicationCursor(changed, "parent"); err == nil {
				t.Fatal("invalid related-session provenance accepted")
			}
		})
	}
}

func TestApplicationCursorRejectsOversizedRelatedSessionProvenance(t *testing.T) {
	workspace := committedCursor(protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace-1"}, 1, "workspace")
	parent := committedCursor(protocol.JournalRef{Kind: protocol.JournalSession, ID: "parent"}, 2, "parent")
	related := make([]protocol.CommittedCursor, protocol.MaxCollectionMembers+1)
	for index := range related {
		related[index] = committedCursor(protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(fmt.Sprintf("child-%05d", index))}, 1, "head")
	}
	cursor := protocol.ApplicationCursor{WorkspaceControl: workspace, SelectedSession: &parent, RelatedSessions: related, Stream: protocol.StreamCursor{Epoch: "epoch"}}
	if err := app.ValidateApplicationCursor(cursor, "parent"); err == nil {
		t.Fatal("oversized related-session provenance accepted")
	}
}

func TestCommandIdempotencyReplaysDurableResultAndConflictsOnChangedPrincipal(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session-1"}
	head := committedCursor(ref, 1, "initial")
	backend := &memoryCommandBackend{head: head, results: make(map[protocol.CommandID]protocol.CommandResult), digests: make(map[protocol.CommandID]protocol.Digest)}
	service, err := app.NewProtocolService(app.ProtocolServiceOptions{
		Orchestrator:     backend,
		Dispatcher:       backend,
		WorkspaceControl: protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	command := applicationCommand(t, "command-replay", string(app.CommandStartTurn), protocol.StartTurnCommandV1{Prompt: "hello"})
	command.Expected.Session = &head
	command.RequestDigest, _ = app.CanonicalRequestDigest(command)

	first, err := service.Execute(t.Context(), command)
	if err != nil || first.Status != "completed" {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	second, err := service.Execute(t.Context(), command)
	if err != nil || !reflect.DeepEqual(second, first) || backend.dispatches != 1 {
		t.Fatalf("second=%+v dispatches=%d err=%v", second, backend.dispatches, err)
	}

	changed := protocol.CloneCommand(command)
	changed.Actor.ID = "other-user"
	changed.RequestDigest, _ = app.CanonicalRequestDigest(changed)
	conflict, err := service.Execute(t.Context(), changed)
	if err != nil || conflict.Error == nil || conflict.Error.Code != "idempotency_conflict" || backend.dispatches != 1 {
		t.Fatalf("conflict=%+v dispatches=%d err=%v", conflict, backend.dispatches, err)
	}
	if backend.lastMetadata != (orchestrator.CommandMetadata{CommandID: command.CommandID, IdempotencyKey: command.IdempotencyKey, RequestDigest: command.RequestDigest, Actor: command.Actor}) {
		t.Fatalf("metadata=%+v", backend.lastMetadata)
	}
}

func TestProtocolExecutePropagatesDispatcherInfrastructureFailures(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session-1"}
	head := committedCursor(ref, 1, "initial")
	backend := &memoryCommandBackend{
		head: head, results: make(map[protocol.CommandID]protocol.CommandResult), digests: make(map[protocol.CommandID]protocol.Digest),
		dispatchErr: errors.New("provider credential should not cross the protocol boundary"),
	}
	service, err := app.NewProtocolService(app.ProtocolServiceOptions{
		Orchestrator: backend, Dispatcher: backend,
		WorkspaceControl: protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	command := applicationCommand(t, "command-failure", string(app.CommandStartTurn), protocol.StartTurnCommandV1{Prompt: "hello"})
	command.Expected.Session = &head
	command.RequestDigest, err = app.CanonicalRequestDigest(command)
	if err != nil {
		t.Fatal(err)
	}

	result, err := service.Execute(t.Context(), command)
	if !errors.Is(err, backend.dispatchErr) || !reflect.DeepEqual(result, protocol.CommandResult{}) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestProtocolExecuteSerializesClassifiedDispatcherFailures(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session-1"}
	head := committedCursor(ref, 1, "initial")
	backend := &memoryCommandBackend{head: head, results: make(map[protocol.CommandID]protocol.CommandResult), digests: make(map[protocol.CommandID]protocol.Digest), dispatchErr: orchestrator.ErrCommitUncertain}
	service, err := app.NewProtocolService(app.ProtocolServiceOptions{Orchestrator: backend, Dispatcher: backend, WorkspaceControl: protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace-1"}})
	if err != nil {
		t.Fatal(err)
	}
	command := applicationCommand(t, "command-uncertain", string(app.CommandStartTurn), protocol.StartTurnCommandV1{Prompt: "hello"})
	command.Expected.Session = &head
	command.RequestDigest, err = app.CanonicalRequestDigest(command)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Execute(t.Context(), command)
	if err != nil || result.Error == nil || result.Error.Code != "commit_uncertain" || result.Error.Retryable {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestProtocolExecutePropagatesLookupInfrastructureFailure(t *testing.T) {
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: "session-1"}
	head := committedCursor(ref, 1, "initial")
	lookupErr := errors.New("lookup transport unavailable")
	backend := &memoryCommandBackend{head: head, results: make(map[protocol.CommandID]protocol.CommandResult), digests: make(map[protocol.CommandID]protocol.Digest), lookupErr: lookupErr}
	service, err := app.NewProtocolService(app.ProtocolServiceOptions{Orchestrator: backend, Dispatcher: backend, WorkspaceControl: protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: "workspace-1"}})
	if err != nil {
		t.Fatal(err)
	}
	command := applicationCommand(t, "command-lookup-error", string(app.CommandStartTurn), protocol.StartTurnCommandV1{Prompt: "hello"})
	command.Expected.Session = &head
	command.RequestDigest, err = app.CanonicalRequestDigest(command)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Execute(t.Context(), command)
	if !errors.Is(err, lookupErr) || !reflect.DeepEqual(result, protocol.CommandResult{}) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

type memoryCommandBackend struct {
	mu           sync.Mutex
	head         protocol.CommittedCursor
	results      map[protocol.CommandID]protocol.CommandResult
	digests      map[protocol.CommandID]protocol.Digest
	dispatches   int
	dispatchErr  error
	lookupErr    error
	lastMetadata orchestrator.CommandMetadata
}

func (b *memoryCommandBackend) LookupCommand(_ context.Context, _ protocol.JournalRef, id protocol.CommandID, digest protocol.Digest) (protocol.CommandResult, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.lookupErr != nil {
		return protocol.CommandResult{}, false, b.lookupErr
	}
	known, ok := b.digests[id]
	if ok && known != digest {
		return protocol.CommandResult{}, false, orchestrator.ErrIdempotencyConflict
	}
	result, ok := b.results[id]
	return protocol.DeepCopy(result), ok, nil
}

func (b *memoryCommandBackend) CommitPureCommand(_ context.Context, metadata orchestrator.CommandMetadata, completion orchestrator.PureCommandCompletion) (protocol.CommandResult, error) {
	return protocol.CommandResult{}, errors.New("unexpected pure command")
}

func (b *memoryCommandBackend) DispatchCommand(_ context.Context, metadata orchestrator.CommandMetadata, _ protocol.Command, _ any) (protocol.CommandResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.dispatches++
	if b.dispatchErr != nil {
		return protocol.CommandResult{}, b.dispatchErr
	}
	b.lastMetadata = metadata
	result := protocol.CommandResult{
		ProtocolVersion: protocol.ApplicationProtocolVersion,
		CommandID:       metadata.CommandID,
		Status:          "completed",
		RequestDigest:   metadata.RequestDigest,
		Cursor:          protocol.ApplicationCursor{SelectedSession: &b.head},
		PayloadVersion:  1,
		Payload:         json.RawMessage(`{"ok":true}`),
	}
	b.digests[metadata.CommandID] = metadata.RequestDigest
	b.results[metadata.CommandID] = protocol.DeepCopy(result)
	return result, nil
}

func applicationCommand(t *testing.T, id protocol.CommandID, kind string, payload any) protocol.Command {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	command := protocol.Command{
		ProtocolVersion: protocol.ApplicationProtocolVersion,
		CommandID:       id,
		Actor:           protocol.ActorRef{ID: "user-1", Kind: protocol.ActorUser},
		IdempotencyKey:  "key-" + string(id),
		Expected: &protocol.CommandExpectation{
			SelectedSessionID: "session-1",
		},
		Kind:           kind,
		PayloadVersion: 1,
		Payload:        raw,
		RequestDigest:  protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("0", 64)},
	}
	command.RequestDigest, err = app.CanonicalRequestDigest(command)
	if err != nil {
		t.Fatal(err)
	}
	return command
}
