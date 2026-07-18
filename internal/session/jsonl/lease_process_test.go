package jsonl_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
)

const turnLeaseHelperEnv = "YORDAM_TURN_LEASE_HELPER"

func TestCrossProcessTurnLeaseAllowsOneProcess(t *testing.T) {
	root := t.TempDir()
	store := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(session.ID)}
	initial, err := store.Head(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}

	processA := turnLeaseHelperCommand(root, protocol.SessionID(session.ID), "turn-a", initial, "accept-and-block")
	stdout, err := processA.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	processA.Stderr = os.Stderr
	if err := processA.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "READY" {
		_ = processA.Process.Kill()
		_ = processA.Wait()
		t.Fatalf("process A failed to acquire and commit: %q err=%v", scanner.Text(), scanner.Err())
	}
	activeHead, err := store.Head(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}

	assertTurnLeaseHelper(t, turnLeaseHelperCommand(root, protocol.SessionID(session.ID), "turn-b", initial, "acquire"), "turn_lease_held")
	if err := processA.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := processA.Wait(); err == nil {
		t.Fatal("killed process A exited successfully")
	}
	assertTurnLeaseHelper(t, turnLeaseHelperCommand(root, protocol.SessionID(session.ID), "turn-c", activeHead, "acquire"), "turn_recovery_required")

	recoveryLease, err := store.AcquireTurnRecoveryLease(context.Background(), protocol.SessionID(session.ID), "turn-a", activeHead)
	if err != nil {
		t.Fatal(err)
	}
	terminalPayload, err := canonicaljson.Marshal(protocol.TurnTerminalV1{Status: "interrupted", Reason: "test fixture simulates future orchestrator recovery"})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := store.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: ref, ExpectedHead: activeHead, TransactionID: "txn-turn-interrupted",
		Events: []protocol.ProposedEvent{{
			EventID: "evt-turn-interrupted", Time: time.Date(2026, 7, 18, 12, 2, 0, 0, time.UTC), PayloadVersion: 1,
			Kind: protocol.EventTurnInterrupted, SessionID: protocol.SessionID(session.ID), TurnID: "turn-a", Payload: terminalPayload,
		}},
	})
	if err != nil || terminal.Status != journal.AppendCommitted {
		t.Fatalf("terminal fixture append=%+v err=%v", terminal, err)
	}
	if err := recoveryLease.Release(context.Background(), terminal.Cursor); err != nil {
		t.Fatal(err)
	}
	assertTurnLeaseHelper(t, turnLeaseHelperCommand(root, protocol.SessionID(session.ID), "turn-d", terminal.Cursor, "acquire"), "ACQUIRED")
}

func TestTurnLeaseHelperProcess(t *testing.T) {
	if os.Getenv(turnLeaseHelperEnv) != "1" {
		return
	}
	root := os.Getenv("YORDAM_LEASE_ROOT")
	sessionID := protocol.SessionID(os.Getenv("YORDAM_LEASE_SESSION"))
	turnID := protocol.TurnID(os.Getenv("YORDAM_LEASE_TURN"))
	seq, err := strconv.ParseUint(os.Getenv("YORDAM_LEASE_SEQ"), 10, 64)
	if err != nil {
		fmt.Println("bad_seq")
		return
	}
	head := protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(sessionID), CommitSeq: seq, TransactionID: protocol.TransactionID(os.Getenv("YORDAM_LEASE_TXN"))}
	store := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}})
	lease, err := store.AcquireTurnLease(context.Background(), sessionID, turnID, head)
	if err != nil {
		switch {
		case errors.Is(err, journal.ErrTurnLeaseHeld):
			fmt.Println("turn_lease_held")
		case errors.Is(err, journal.ErrTurnRecoveryRequired):
			fmt.Println("turn_recovery_required")
		default:
			fmt.Printf("ERROR:%v\n", err)
		}
		return
	}
	_ = lease
	if os.Getenv("YORDAM_LEASE_MODE") != "accept-and-block" {
		fmt.Println("ACQUIRED")
		return
	}
	payload, err := canonicaljson.Marshal(protocol.TurnAcceptedV1{CommandID: "command-a", Goal: "hold a durable turn", OutcomeContractID: "contract-a", ContractVersion: 1})
	if err != nil {
		fmt.Printf("ERROR:%v\n", err)
		return
	}
	result, err := store.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(sessionID)}, ExpectedHead: head,
		TransactionID: "txn-turn-accepted",
		Compatibility: &journal.CompatibilityDeclaration{ReaderVersion: protocol.EnvelopeVersion, WriterVersion: protocol.EnvelopeVersion, LegacyHead: head},
		Events: []protocol.ProposedEvent{{
			EventID: "evt-turn-accepted", Time: time.Date(2026, 7, 18, 12, 1, 0, 0, time.UTC), PayloadVersion: 1,
			Kind: protocol.EventTurnAccepted, SessionID: sessionID, TurnID: turnID, Payload: payload,
		}},
	})
	if err != nil || result.Status != journal.AppendCommitted {
		fmt.Printf("ERROR:append=%+v err=%v\n", result, err)
		return
	}
	fmt.Println("READY")
	select {}
}

func turnLeaseHelperCommand(root string, sessionID protocol.SessionID, turnID protocol.TurnID, head protocol.CommittedCursor, mode string) *exec.Cmd {
	command := exec.Command(os.Args[0], "-test.run=^TestTurnLeaseHelperProcess$", "-test.v=false")
	command.Env = append(os.Environ(),
		turnLeaseHelperEnv+"=1",
		"YORDAM_LEASE_ROOT="+root,
		"YORDAM_LEASE_SESSION="+string(sessionID),
		"YORDAM_LEASE_TURN="+string(turnID),
		"YORDAM_LEASE_SEQ="+strconv.FormatUint(head.CommitSeq, 10),
		"YORDAM_LEASE_TXN="+string(head.TransactionID),
		"YORDAM_LEASE_MODE="+mode,
	)
	return command
}

func assertTurnLeaseHelper(t *testing.T, command *exec.Cmd, want string) {
	t.Helper()
	raw, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("lease helper failed: %v output=%q", err, raw)
	}
	fields := strings.Fields(string(raw))
	if len(fields) == 0 || fields[0] != want {
		got := strings.TrimSpace(string(raw))
		t.Fatalf("lease helper=%q want=%q", got, want)
	}
}

type gateEncoder struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (e *gateEncoder) EncodeProposed(event protocol.ProposedEvent) (json.RawMessage, error) {
	if event.EventID == "evt-blocked" {
		e.once.Do(func() { close(e.entered) })
		<-e.release
	}
	return protocol.CloneRawMessage(event.Payload), nil
}

func TestUnrelatedSessionsAppendConcurrently(t *testing.T) {
	root := t.TempDir()
	encoder := &gateEncoder{entered: make(chan struct{}), release: make(chan struct{})}
	store := jsonl.New(root, jsonl.Options{Encoder: encoder})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	firstRef := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(first.ID)}
	secondRef := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(second.ID)}
	firstHead, _ := store.Head(context.Background(), firstRef)
	secondHead, _ := store.Head(context.Background(), secondRef)
	firstDone := make(chan error, 1)
	go func() {
		_, err := store.AppendBatch(context.Background(), legacyCompatibilityRequest(t, firstRef, firstHead, "txn-first", "evt-blocked"))
		firstDone <- err
	}()
	select {
	case <-encoder.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first session did not enter append")
	}
	secondDone := make(chan error, 1)
	go func() {
		_, err := store.AppendBatch(context.Background(), legacyCompatibilityRequest(t, secondRef, secondHead, "txn-second", "evt-unrelated"))
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unrelated session append waited for another journal")
	}
	close(encoder.release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

func TestLegacyLockSymlinkSubstitutionIsReadOnly(t *testing.T) {
	root := t.TempDir()
	setup := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	session, err := setup.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(session.ID)}
	head, err := setup.Head(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "journal.lock")
	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside.lock")
	if err := os.WriteFile(outside, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, lockPath); err != nil {
		t.Fatal(err)
	}
	store := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}})
	before, err := os.ReadFile(filepath.Join(filepath.Dir(lockPath), "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendBatch(context.Background(), legacyCompatibilityRequest(t, ref, head, "txn-symlink", "evt-symlink")); err == nil {
		t.Fatal("symlinked legacy lock permitted append")
	}
	after, err := os.ReadFile(filepath.Join(filepath.Dir(lockPath), "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("symlinked lock append mutated journal")
	}
}

func TestConcurrentLegacyLockInitializationUsesOneStableInode(t *testing.T) {
	root := t.TempDir()
	setup := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	session, err := setup.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(session.ID)}
	head, err := setup.Head(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	for _, name := range []string{"lock-set.json", "journal.lock", "turn.lock"} {
		if err := os.Remove(filepath.Join(sessionDir, name)); err != nil {
			t.Fatal(err)
		}
	}
	initializers := []*exec.Cmd{
		legacyAppendHelperCommand(root, protocol.SessionID(session.ID), head, "init-a"),
		legacyAppendHelperCommand(root, protocol.SessionID(session.ID), head, "init-b"),
	}
	for _, command := range initializers {
		command.Env = append(command.Env, "YORDAM_INITIALIZE_LOCKS=1")
	}
	initializerOutputs := make([]bytes.Buffer, len(initializers))
	for index, command := range initializers {
		command.Stdout = &initializerOutputs[index]
		command.Stderr = &initializerOutputs[index]
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
	}
	for index, command := range initializers {
		err := command.Wait()
		fields := strings.Fields(initializerOutputs[index].String())
		if err != nil || len(fields) == 0 || fields[0] != "INITIALIZED" {
			t.Fatalf("initializer %d: err=%v output=%q", index, err, initializerOutputs[index].String())
		}
	}
	commands := []*exec.Cmd{
		legacyAppendHelperCommand(root, protocol.SessionID(session.ID), head, "a"),
		legacyAppendHelperCommand(root, protocol.SessionID(session.ID), head, "b"),
	}
	outputs := make([]bytes.Buffer, len(commands))
	for index, command := range commands {
		command.Stdout = &outputs[index]
		command.Stderr = &outputs[index]
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
	}
	statuses := map[string]int{}
	for index, command := range commands {
		if err := command.Wait(); err != nil {
			t.Fatalf("legacy helper %d: %v output=%q", index, err, outputs[index].String())
		}
		fields := strings.Fields(outputs[index].String())
		if len(fields) == 0 {
			t.Fatalf("legacy helper %d had no status", index)
		}
		statuses[fields[0]]++
	}
	if statuses[string(journal.AppendCommitted)] != 1 || statuses[string(journal.AppendConflict)] != 1 {
		t.Fatalf("legacy append statuses=%v", statuses)
	}
	for _, name := range []string{"lock-set.json", "journal.lock", "turn.lock"} {
		info, err := os.Lstat(filepath.Join(sessionDir, name))
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Fatalf("%s info=%v err=%v", name, info, err)
		}
	}
}

func TestSubprocessJournalLockUnlinkRecreateRejectsWriter(t *testing.T) {
	root := t.TempDir()
	setup := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	session, err := setup.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(session.ID)}
	head, err := setup.Head(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
	gate := filepath.Join(t.TempDir(), "continue")
	command := legacyAppendHelperCommand(root, protocol.SessionID(session.ID), head, "substitute")
	command.Env = append(command.Env, "YORDAM_APPEND_GATE="+gate)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "READY" {
		t.Fatalf("writer readiness=%q err=%v", scanner.Text(), scanner.Err())
	}
	lockPath := filepath.Join(sessionDir, "journal.lock")
	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lockPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(gate, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if !scanner.Scan() || scanner.Text() != "REJECTED" {
		t.Fatalf("writer result=%q err=%v", scanner.Text(), scanner.Err())
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
	inspection, err := setup.Inspect(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Head != head {
		t.Fatalf("substituted writer advanced head to %+v want %+v", inspection.Head, head)
	}
}

func TestMissingInitializedJournalLockOpensInspectionReadOnly(t *testing.T) {
	root := t.TempDir()
	store := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	ref := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(session.ID)}
	lockPath := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "journal.lock")
	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	inspection, err := store.Inspect(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Writable {
		t.Fatalf("missing initialized lock inspection remained writable: %+v", inspection)
	}
}

func TestLegacyAppendHelperProcess(t *testing.T) {
	if os.Getenv("YORDAM_LEGACY_APPEND_HELPER") != "1" {
		return
	}
	root := os.Getenv("YORDAM_APPEND_ROOT")
	sessionID := protocol.SessionID(os.Getenv("YORDAM_APPEND_SESSION"))
	seq, err := strconv.ParseUint(os.Getenv("YORDAM_APPEND_SEQ"), 10, 64)
	if err != nil {
		fmt.Println("ERROR")
		return
	}
	head := protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: protocol.JournalID(sessionID), CommitSeq: seq, TransactionID: protocol.TransactionID(os.Getenv("YORDAM_APPEND_TXN"))}
	suffix := os.Getenv("YORDAM_APPEND_SUFFIX")
	gate := os.Getenv("YORDAM_APPEND_GATE")
	encoder := journal.Encoder(passthroughEncoder{})
	if gate != "" {
		encoder = &processGateEncoder{gate: gate}
	}
	store := jsonl.New(root, jsonl.Options{Encoder: encoder})
	if os.Getenv("YORDAM_INITIALIZE_LOCKS") == "1" {
		err := store.InitializeJournalLocks(context.Background(), protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(sessionID)})
		if err != nil {
			fmt.Printf("ERROR:%v\n", err)
			return
		}
		fmt.Println("INITIALIZED")
		return
	}
	result, err := store.AppendBatch(context.Background(), legacyCompatibilityRequest(t,
		protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(sessionID)}, head,
		protocol.TransactionID("txn-"+suffix), protocol.EventID("evt-"+suffix)))
	if gate != "" {
		if err != nil {
			fmt.Println("REJECTED")
		} else {
			fmt.Printf("UNSAFE:%s\n", result.Status)
		}
		return
	}
	if err != nil {
		fmt.Printf("ERROR:%v\n", err)
		return
	}
	fmt.Println(result.Status)
}

type processGateEncoder struct {
	gate string
	once sync.Once
}

func (e *processGateEncoder) EncodeProposed(event protocol.ProposedEvent) (json.RawMessage, error) {
	e.once.Do(func() { fmt.Println("READY") })
	for {
		if _, err := os.Stat(e.gate); err == nil {
			return protocol.CloneRawMessage(event.Payload), nil
		}
		time.Sleep(time.Millisecond)
	}
}

func legacyAppendHelperCommand(root string, sessionID protocol.SessionID, head protocol.CommittedCursor, suffix string) *exec.Cmd {
	command := exec.Command(os.Args[0], "-test.run=^TestLegacyAppendHelperProcess$", "-test.v=false")
	command.Env = append(os.Environ(),
		"YORDAM_LEGACY_APPEND_HELPER=1",
		"YORDAM_APPEND_ROOT="+root,
		"YORDAM_APPEND_SESSION="+string(sessionID),
		"YORDAM_APPEND_SEQ="+strconv.FormatUint(head.CommitSeq, 10),
		"YORDAM_APPEND_TXN="+string(head.TransactionID),
		"YORDAM_APPEND_SUFFIX="+suffix,
	)
	return command
}

func legacyCompatibilityRequest(t *testing.T, ref protocol.JournalRef, head protocol.CommittedCursor, transactionID protocol.TransactionID, eventID protocol.EventID) journal.AppendRequest {
	t.Helper()
	payload, err := canonicaljson.Marshal(protocol.TaskCreatedV1{Goal: "concurrency test", OutcomeContractID: "contract", ContractVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	return journal.AppendRequest{
		Journal: ref, ExpectedHead: head, TransactionID: transactionID,
		Compatibility: &journal.CompatibilityDeclaration{ReaderVersion: protocol.EnvelopeVersion, WriterVersion: protocol.EnvelopeVersion, LegacyHead: head},
		Events: []protocol.ProposedEvent{{
			EventID: eventID, Time: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC), PayloadVersion: 1,
			Kind: protocol.EventTaskCreated, SessionID: protocol.SessionID(ref.ID), TaskID: "task", Payload: payload,
		}},
	}
}
