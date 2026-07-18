package jsonl_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
)

type explicitRecoveryDetails struct {
	ObservedTailDigest protocol.Digest `json:"observed_tail_digest"`
	ValidPrefixBytes   int64           `json:"valid_prefix_bytes"`
	SourceBytes        int64           `json:"source_bytes"`
}

func recoveryRequestFromInspection(t *testing.T, store *jsonl.Store, sessionID string, operationID protocol.ControlOperationID, transactionID protocol.TransactionID) journal.RecoveryRequest {
	t.Helper()
	inspection, err := store.InspectSession(context.Background(), protocol.SessionID(sessionID))
	if err != nil {
		t.Fatal(err)
	}
	for _, diagnostic := range inspection.Journal.Diagnostics {
		if diagnostic.Code != "recovery.available" {
			continue
		}
		var details explicitRecoveryDetails
		if err := json.Unmarshal(diagnostic.Details, &details); err != nil {
			t.Fatal(err)
		}
		return journal.RecoveryRequest{
			OperationID: operationID, Journal: inspection.Journal.Journal,
			ExpectedHead: inspection.Journal.Head, ObservedTailDigest: details.ObservedTailDigest,
			TransactionID: transactionID,
		}
	}
	t.Fatal("inspection did not expose recovery.available")
	return journal.RecoveryRequest{}
}

func TestLoadInspectsAndExplicitRecoveryPreservesIncompleteFinalLine(t *testing.T) {
	root := t.TempDir()
	store := jsonl.New(root, jsonl.Options{
		Clock:   func() time.Time { return time.Unix(1, 0).UTC() },
		Entropy: strings.NewReader(strings.Repeat("b", 2048)),
	})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.Create(
		context.Background(),
		workspace,
		domain.ModeAsk,
		domain.ModelSelection{Profile: "p", Model: "m"},
	)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(workspace.CanonicalPath, "untouched.txt")
	if err := os.WriteFile(target, []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), session.ID, domain.EventToolStarted, map[string]string{
		"call_id": "c1",
		"path":    target,
		"content": "after",
	}); err != nil {
		t.Fatal(err)
	}
	eventsPath := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "events.jsonl")
	tail := []byte(`{"schema_version":1`)
	file, err := os.OpenFile(eventsPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(tail); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	before, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := store.Load(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.ReadOnly {
		t.Fatal("incomplete tail opened writable before explicit recovery")
	}
	if !strings.Contains(replay.RecoveryNote, "incomplete_final_fragment") {
		t.Fatalf("note=%q", replay.RecoveryNote)
	}
	if len(replay.Events) != 2 {
		t.Fatalf("events=%d want validated prefix of 2", len(replay.Events))
	}
	unchanged, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(unchanged, before) {
		t.Fatal("Load mutated the incomplete source journal")
	}

	request := recoveryRequestFromInspection(t, store, session.ID, "operation-incomplete-tail", "txn-incomplete-tail")
	result, err := store.RecoverSession(context.Background(), request)
	if err != nil || result.Status != "recovered" {
		t.Fatalf("explicit recovery result=%+v err=%v", result, err)
	}
	recoveredLog, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasSuffix(recoveredLog, []byte{'\n'}) {
		t.Fatal("recovered event log is not newline terminated")
	}
	if bytes.HasSuffix(recoveredLog, tail) {
		t.Fatal("incomplete tail remains in the active journal")
	}
	artifactsDir := filepath.Join(filepath.Dir(eventsPath), "artifacts")
	entries, err := os.ReadDir(artifactsDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || !strings.HasPrefix(entries[0].Name(), "recovery-tail-") {
		t.Fatalf("recovery artifacts=%v want one explicit tail artifact", entries)
	}
	recoveredTail, err := os.ReadFile(filepath.Join(artifactsDir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(recoveredTail, tail) {
		t.Fatalf("recovery artifact=%q want %q", recoveredTail, tail)
	}
	contents, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "before" {
		t.Fatalf("replay mutated tool target: %q", contents)
	}

	again, err := store.RecoverSession(context.Background(), request)
	if err != nil || again.Status != "already_recovered" || again.Cursor != result.Cursor {
		t.Fatalf("restarted explicit recovery result=%+v err=%v", again, err)
	}
}

func TestLoadLeavesEarlierCorruptionReadOnlyAndUnchanged(t *testing.T) {
	tests := []struct {
		name    string
		corrupt func(t *testing.T, line []byte) []byte
	}{
		{
			name: "invalid JSON",
			corrupt: func(t *testing.T, _ []byte) []byte {
				t.Helper()
				return []byte(`{"schema_version":`)
			},
		},
		{
			name: "invalid envelope",
			corrupt: func(t *testing.T, line []byte) []byte {
				t.Helper()
				var event domain.DurableEvent
				if err := json.Unmarshal(line, &event); err != nil {
					t.Fatal(err)
				}
				event.SchemaVersion = 2
				encoded, err := json.Marshal(event)
				if err != nil {
					t.Fatal(err)
				}
				return encoded
			},
		},
		{
			name: "invalid sequence",
			corrupt: func(t *testing.T, line []byte) []byte {
				t.Helper()
				var event domain.DurableEvent
				if err := json.Unmarshal(line, &event); err != nil {
					t.Fatal(err)
				}
				event.Seq = 1
				encoded, err := json.Marshal(event)
				if err != nil {
					t.Fatal(err)
				}
				return encoded
			},
		},
		{
			name: "oversized complete line",
			corrupt: func(t *testing.T, _ []byte) []byte {
				t.Helper()
				return bytes.Repeat([]byte("x"), (2<<20)+1)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			store, workspace, session := createTestSession(t, root)
			if _, err := store.Append(
				context.Background(),
				session.ID,
				domain.EventToolStarted,
				map[string]string{"call_id": "c1"},
			); err != nil {
				t.Fatal(err)
			}
			sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
			eventsPath := filepath.Join(sessionDir, "events.jsonl")
			metadataPath := filepath.Join(sessionDir, "metadata.json")
			raw, err := os.ReadFile(eventsPath)
			if err != nil {
				t.Fatal(err)
			}
			lines := bytes.Split(bytes.TrimSuffix(raw, []byte{'\n'}), []byte{'\n'})
			lines[1] = test.corrupt(t, lines[1])
			corruptLog := bytes.Join(lines, []byte{'\n'})
			corruptLog = append(corruptLog, '\n')
			corruptLog = append(corruptLog, []byte(`{"schema_version":1`)...)
			if err := os.WriteFile(eventsPath, corruptLog, 0o600); err != nil {
				t.Fatal(err)
			}
			beforeMetadata, err := os.ReadFile(metadataPath)
			if err != nil {
				t.Fatal(err)
			}

			replay, err := store.Load(context.Background(), session.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !replay.ReadOnly {
				t.Fatal("corrupt log was opened writable")
			}
			if !strings.Contains(replay.RecoveryNote, "corruption at sequence 2") {
				t.Fatalf("note=%q", replay.RecoveryNote)
			}
			if len(replay.Events) != 1 || replay.Events[0].Seq != 1 {
				t.Fatalf("events=%v want only validated sequence 1", replay.Events)
			}
			afterLog, err := os.ReadFile(eventsPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(afterLog, corruptLog) {
				t.Fatal("read-only replay changed corrupt log")
			}
			afterMetadata, err := os.ReadFile(metadataPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(afterMetadata, beforeMetadata) {
				t.Fatal("read-only replay repaired metadata")
			}
			artifacts, err := os.ReadDir(filepath.Join(sessionDir, "artifacts"))
			if err != nil {
				t.Fatal(err)
			}
			if len(artifacts) != 0 {
				t.Fatalf("read-only replay created artifacts: %v", artifacts)
			}
		})
	}
}

func TestLoadTreatsNewlineTerminatedInvalidFinalLineAsCorruption(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	eventsPath := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "events.jsonl")
	file, err := os.OpenFile(eventsPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("not-json\n"); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}

	replay, err := store.Load(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.ReadOnly || !strings.Contains(replay.RecoveryNote, "corruption at sequence 2") {
		t.Fatalf("read_only=%v note=%q", replay.ReadOnly, replay.RecoveryNote)
	}
	after, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("newline-terminated corruption was changed")
	}
}

func TestLoadReportsLaggingMetadataWithoutRepair(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	if _, err := store.Append(
		context.Background(),
		session.ID,
		domain.EventUserMessage,
		map[string]string{"content": "durable"},
	); err != nil {
		t.Fatal(err)
	}
	metadataPath := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "metadata.json")
	var metadata domain.Session
	raw, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &metadata); err != nil {
		t.Fatal(err)
	}
	metadata.LastSeq = 1
	rolledBack, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metadataPath, rolledBack, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(
		context.Background(),
		session.ID,
		domain.EventUserMessage,
		map[string]string{"content": "blocked"},
	); err == nil || !strings.Contains(err.Error(), "session sequence mismatch") {
		t.Fatalf("append error=%v want session sequence mismatch", err)
	}

	replay, err := store.Load(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.ReadOnly || replay.Session.LastSeq != 2 || len(replay.Events) != 2 || !strings.Contains(replay.RecoveryNote, "metadata_lags_journal") {
		t.Fatalf("replay=%+v", replay)
	}
	raw, err = os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.LastSeq != 1 {
		t.Fatalf("pure Load repaired metadata last_seq=%d want persisted 1", metadata.LastSeq)
	}
	_, err = store.Append(
		context.Background(),
		session.ID,
		domain.EventUserMessage,
		map[string]string{"content": "next"},
	)
	if err == nil || !strings.Contains(err.Error(), "session sequence mismatch") {
		t.Fatalf("append error=%v want unresolved metadata mismatch", err)
	}
}

func TestLoadTreatsMetadataAheadOfCompleteLogAsReadOnlyCorruption(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(t *testing.T, eventsPath, metadataPath string)
	}{
		{
			name: "metadata ahead",
			prepare: func(t *testing.T, _, metadataPath string) {
				t.Helper()
				metadata := readSessionMetadata(t, metadataPath)
				metadata.LastSeq++
				writeJSONFile(t, metadataPath, metadata)
			},
		},
		{
			name: "truncated complete event",
			prepare: func(t *testing.T, eventsPath, _ string) {
				t.Helper()
				raw, err := os.ReadFile(eventsPath)
				if err != nil {
					t.Fatal(err)
				}
				firstNewline := bytes.IndexByte(raw, '\n')
				if firstNewline < 0 {
					t.Fatal("event log has no complete first event")
				}
				if err := os.WriteFile(eventsPath, raw[:firstNewline+1], 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			store, workspace, session := createTestSession(t, root)
			if _, err := store.Append(context.Background(), session.ID, domain.EventUserMessage, map[string]string{"content": "durable"}); err != nil {
				t.Fatal(err)
			}
			sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
			eventsPath := filepath.Join(sessionDir, "events.jsonl")
			metadataPath := filepath.Join(sessionDir, "metadata.json")
			test.prepare(t, eventsPath, metadataPath)
			before, err := snapshotTree(sessionDir)
			if err != nil {
				t.Fatal(err)
			}

			replay, err := store.Load(context.Background(), session.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !replay.ReadOnly || !strings.Contains(replay.RecoveryNote, "storage corruption") {
				t.Fatalf("read_only=%v note=%q want visible storage corruption", replay.ReadOnly, replay.RecoveryNote)
			}
			if replay.Session.LastSeq <= uint64(len(replay.Events)) {
				t.Fatalf("metadata last_seq=%d validated events=%d want metadata ahead", replay.Session.LastSeq, len(replay.Events))
			}
			assertSessionTreeUnchanged(t, sessionDir, before)

			if _, err := store.Append(context.Background(), session.ID, domain.EventUserMessage, map[string]string{"content": "blocked"}); err == nil {
				t.Fatal("Append made a metadata-ahead session writable")
			}
			assertSessionTreeUnchanged(t, sessionDir, before)
		})
	}
}

func TestMissingEventLogFailsClosedWithoutMutation(t *testing.T) {
	for _, test := range []struct {
		name         string
		metadataSeq  uint64
		wantReadOnly bool
	}{
		{name: "published metadata", metadataSeq: 1, wantReadOnly: true},
		{name: "zero metadata", metadataSeq: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			store, workspace, session := createTestSession(t, root)
			sessionDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID)
			eventsPath := filepath.Join(sessionDir, "events.jsonl")
			metadataPath := filepath.Join(sessionDir, "metadata.json")
			metadata := readSessionMetadata(t, metadataPath)
			metadata.LastSeq = test.metadataSeq
			writeJSONFile(t, metadataPath, metadata)
			if err := os.Remove(eventsPath); err != nil {
				t.Fatal(err)
			}
			before, err := snapshotTree(sessionDir)
			if err != nil {
				t.Fatal(err)
			}

			replay, loadErr := store.Load(context.Background(), session.ID)
			if test.wantReadOnly {
				if loadErr != nil {
					t.Fatal(loadErr)
				}
				if !replay.ReadOnly || !strings.Contains(replay.RecoveryNote, "storage corruption") || !strings.Contains(replay.RecoveryNote, "missing") {
					t.Fatalf("replay=%+v want missing-log storage corruption", replay)
				}
			} else if loadErr == nil || !strings.Contains(loadErr.Error(), "storage corruption") {
				t.Fatalf("Load error=%v want fail-closed storage corruption", loadErr)
			}
			assertSessionTreeUnchanged(t, sessionDir, before)
			if _, err := os.Lstat(eventsPath); !os.IsNotExist(err) {
				t.Fatalf("Load created missing event log: %v", err)
			}

			if _, err := store.Append(context.Background(), session.ID, domain.EventUserMessage, map[string]string{"content": "blocked"}); err == nil {
				t.Fatal("Append recreated a missing event log")
			}
			assertSessionTreeUnchanged(t, sessionDir, before)
			if _, err := os.Lstat(eventsPath); !os.IsNotExist(err) {
				t.Fatalf("Append created missing event log: %v", err)
			}
		})
	}
}

func readSessionMetadata(t *testing.T, path string) domain.Session {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var metadata domain.Session
	if err := json.Unmarshal(raw, &metadata); err != nil {
		t.Fatal(err)
	}
	return metadata
}

func assertSessionTreeUnchanged(t *testing.T, sessionDir string, before []byte) {
	t.Helper()
	after, err := snapshotTree(sessionDir)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("session tree changed on corruption:\nbefore:\n%s\nafter:\n%s", before, after)
	}
}

func TestLoadReportsUnmatchedToolStartWithoutAppending(t *testing.T) {
	root := t.TempDir()
	store, workspace, session := createTestSession(t, root)
	target := filepath.Join(workspace.CanonicalPath, "target.txt")
	if err := os.WriteFile(target, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(
		context.Background(),
		session.ID,
		domain.EventToolStarted,
		map[string]string{"call_id": "c1", "path": target, "content": "mutated"},
	); err != nil {
		t.Fatal(err)
	}

	for attempt := range 2 {
		replay, err := store.Load(context.Background(), session.ID)
		if err != nil {
			t.Fatal(err)
		}
		if replay.ReadOnly || len(replay.Events) != 2 || !strings.Contains(replay.RecoveryNote, "migration.unmatched_activity") {
			t.Fatalf("attempt %d replay=%+v", attempt+1, replay)
		}
	}
	contents, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "original" {
		t.Fatalf("replay re-executed mutation: %q", contents)
	}
	eventsPath := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "events.jsonl")
	if events := readEvents(t, eventsPath); len(events) != 2 {
		t.Fatalf("durable events=%d want unchanged source with 2", len(events))
	}
}

func TestLoadMarksOrphanToolStartAndTerminalUncertain(t *testing.T) {
	store, _, session := createTestSession(t, t.TempDir())
	if _, err := store.Append(
		context.Background(),
		session.ID,
		domain.EventToolStarted,
		map[string]string{"call_id": "c1"},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(
		context.Background(),
		session.ID,
		domain.EventToolResult,
		map[string]string{"call_id": "c1", "status": "succeeded"},
	); err != nil {
		t.Fatal(err)
	}

	replay, err := store.Load(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replay.ReadOnly || len(replay.Events) != 3 || !strings.Contains(replay.RecoveryNote, "migration.out_of_order") {
		t.Fatalf("replay=%+v", replay)
	}
}

func TestPutCapsRetainedArtifactAtExactlyTenMiB(t *testing.T) {
	root := t.TempDir()
	store, _, session := createTestSession(t, root)
	source := io.MultiReader(
		strings.NewReader(strings.Repeat("x", int(jsonl.MaxArtifactBytes))),
		strings.NewReader("overflow"),
	)

	artifact, err := store.Put(
		context.Background(),
		session.ID,
		"text/plain",
		source,
		jsonl.MaxArtifactBytes+1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if jsonl.MaxArtifactBytes != 10<<20 {
		t.Fatalf("MaxArtifactBytes=%d want 10 MiB", jsonl.MaxArtifactBytes)
	}
	if artifact.Size != jsonl.MaxArtifactBytes || !artifact.Truncated {
		t.Fatalf("artifact size=%d truncated=%v", artifact.Size, artifact.Truncated)
	}
	info, err := os.Stat(artifact.Path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != jsonl.MaxArtifactBytes || info.Mode().Perm() != 0o600 {
		t.Fatalf("artifact stat size=%d mode=%#o", info.Size(), info.Mode().Perm())
	}
	reader, err := store.Open(context.Background(), artifact)
	if err != nil {
		t.Fatal(err)
	}
	contents, err := io.ReadAll(reader)
	closeErr := reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if int64(len(contents)) != jsonl.MaxArtifactBytes || bytes.Contains(contents, []byte("overflow")) {
		t.Fatalf("opened artifact bytes=%d", len(contents))
	}
}

func TestArtifactOperationsRejectEscapingPathsAndSymlinks(t *testing.T) {
	t.Run("Open rejects paths not bound to the artifact", func(t *testing.T) {
		root := t.TempDir()
		store, workspace, session := createTestSession(t, root)
		artifact, err := store.Put(context.Background(), session.ID, "text/plain", strings.NewReader("safe"), 100)
		if err != nil {
			t.Fatal(err)
		}
		hostile := artifact
		hostile.Path = filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "metadata.json")
		if reader, err := store.Open(context.Background(), hostile); err == nil {
			reader.Close()
			t.Fatal("opened a non-artifact file inside the store")
		}
		hostile.Path = filepath.Join(t.TempDir(), "outside")
		if reader, err := store.Open(context.Background(), hostile); err == nil {
			reader.Close()
			t.Fatal("opened a path outside the store")
		}
	})

	t.Run("Open rejects artifact symlink escape", func(t *testing.T) {
		root := t.TempDir()
		store, _, session := createTestSession(t, root)
		artifact, err := store.Put(context.Background(), session.ID, "text/plain", strings.NewReader("safe"), 100)
		if err != nil {
			t.Fatal(err)
		}
		outside := filepath.Join(t.TempDir(), "outside.txt")
		if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(artifact.Path); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, artifact.Path); err != nil {
			t.Fatal(err)
		}
		if reader, err := store.Open(context.Background(), artifact); err == nil {
			reader.Close()
			t.Fatal("followed artifact symlink outside its artifacts directory")
		}
	})

	t.Run("Put rejects artifacts directory symlink escape", func(t *testing.T) {
		root := t.TempDir()
		store, workspace, session := createTestSession(t, root)
		artifactsDir := filepath.Join(root, "workspaces", workspace.ID, "sessions", session.ID, "artifacts")
		if err := os.Remove(artifactsDir); err != nil {
			t.Fatal(err)
		}
		outside := t.TempDir()
		if err := os.Symlink(outside, artifactsDir); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Put(context.Background(), session.ID, "text/plain", strings.NewReader("escape"), 100); err == nil {
			t.Fatal("wrote artifact through escaping directory symlink")
		}
		entries, err := os.ReadDir(outside)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("files escaped store: %v", entries)
		}
	})
}
