package jsonl_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
)

func TestWorkspaceControlJournalUsesWorkspaceIdentity(t *testing.T) {
	root := t.TempDir()
	workspacePath := t.TempDir()
	alias := filepath.Join(t.TempDir(), "workspace-alias")
	if err := os.Symlink(workspacePath, alias); err != nil {
		t.Fatal(err)
	}
	workspace, err := jsonl.WorkspaceFromPath(workspacePath)
	if err != nil {
		t.Fatal(err)
	}
	aliased, err := jsonl.WorkspaceFromPath(alias)
	if err != nil {
		t.Fatal(err)
	}
	store := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}})
	first, err := store.EnsureWorkspaceControl(context.Background(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	second, err := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}}).EnsureWorkspaceControl(context.Background(), aliased)
	if err != nil {
		t.Fatal(err)
	}
	want := protocol.JournalRef{Kind: protocol.JournalWorkspaceControl, ID: protocol.JournalID(workspace.ID)}
	if first != want || second != want {
		t.Fatalf("control refs first=%+v second=%+v want=%+v", first, second, want)
	}
	inspection, err := store.Inspect(context.Background(), want)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Journal != want || !inspection.Writable || len(inspection.Events) != 0 {
		t.Fatalf("control inspection=%+v", inspection)
	}
	payload, err := canonicaljson.Marshal(protocol.DiagnosticV1{Diagnostic: protocol.Diagnostic{Code: "control.created", Message: "control journal is writable", Journal: want, AtSeq: 1}})
	if err != nil {
		t.Fatal(err)
	}
	appended, err := store.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: want, TransactionID: "txn-control-first",
		Events: []protocol.ProposedEvent{{
			EventID: "evt-control-first", Time: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC), PayloadVersion: 1,
			Kind: protocol.EventRecoveryDiagnostic, Payload: payload,
		}},
	})
	if err != nil || appended.Status != journal.AppendCommitted || appended.Cursor.JournalID != want.ID {
		t.Fatalf("first control append=%+v err=%v", appended, err)
	}
	controlDir := filepath.Join(root, "workspaces", workspace.ID, "control")
	for _, name := range []string{"events.jsonl", "metadata.json", "journal.lock", "lock-set.json"} {
		info, err := os.Lstat(filepath.Join(controlDir, name))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode=%v", name, info.Mode())
		}
	}
}
