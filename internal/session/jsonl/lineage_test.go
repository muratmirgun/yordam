package jsonl_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/canonicaljson"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
)

func TestLineageComposesOnlyAnchoredParentPrefix(t *testing.T) {
	root := t.TempDir()
	store := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	parent, err := store.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	parentRef := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(parent.ID)}
	legacyHead, err := store.Head(context.Background(), parentRef)
	if err != nil {
		t.Fatal(err)
	}
	acceptedPayload, err := canonicaljson.Marshal(protocol.TurnAcceptedV1{CommandID: "command-parent", Goal: "active at fork", OutcomeContractID: "contract-parent", ContractVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := store.AppendBatch(context.Background(), journal.AppendRequest{
		Journal: parentRef, ExpectedHead: legacyHead, TransactionID: "txn-parent-accepted",
		Compatibility: &journal.CompatibilityDeclaration{ReaderVersion: protocol.EnvelopeVersion, WriterVersion: protocol.EnvelopeVersion, LegacyHead: legacyHead},
		Events:        []protocol.ProposedEvent{{EventID: "evt-parent-accepted", Time: parent.UpdatedAt.Add(time.Second), PayloadVersion: 1, Kind: protocol.EventTurnAccepted, SessionID: protocol.SessionID(parent.ID), TurnID: "turn-parent", Payload: acceptedPayload}},
	})
	if err != nil || accepted.Status != journal.AppendCommitted {
		t.Fatalf("parent accepted=%+v err=%v", accepted, err)
	}
	anchor := accepted.Cursor
	later := appendCommitted(t, v2JournalFixture{repo: store, ref: parentRef}, anchor, "txn-after-anchor", "evt-after-anchor")
	if later.Status != journal.AppendCommitted {
		t.Fatalf("later append=%+v", later)
	}
	lineage := journal.SessionLineage{
		ParentSessionID: protocol.SessionID(parent.ID), ParentCursor: anchor,
		CheckpointDigest: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: fmt.Sprintf("%x", sha256.Sum256([]byte("checkpoint")))},
	}
	child, err := store.CreateWithLineage(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"}, &lineage)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := store.SessionLineage(context.Background(), protocol.SessionID(child.ID))
	if err != nil {
		t.Fatal(err)
	}
	if stored == nil || *stored != lineage {
		t.Fatalf("stored lineage=%+v want=%+v", stored, lineage)
	}
	page, err := store.ReadComposedRange(context.Background(), journal.ComposedReadRequest{SessionID: protocol.SessionID(child.ID), Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) < 3 {
		t.Fatalf("composed events=%d want parent prefix plus atomic child bootstrap", len(page.Events))
	}
	parentEvents := 0
	childEvents := 0
	childCursor := protocol.CommittedCursor{}
	inheritedInterruption := false
	for _, event := range page.Events {
		if event.Cursor.ViewSessionID != protocol.SessionID(child.ID) {
			t.Fatalf("view session=%q", event.Cursor.ViewSessionID)
		}
		switch event.Cursor.OriginSessionID {
		case protocol.SessionID(parent.ID):
			parentEvents++
			if event.Cursor.OriginCursor.CommitSeq > anchor.CommitSeq || event.Record.Envelope.EventID == "evt-after-anchor" {
				t.Fatalf("parent event crossed anchor: %+v", event)
			}
		case protocol.SessionID(child.ID):
			childEvents++
			if childCursor == (protocol.CommittedCursor{}) {
				childCursor = event.Cursor.OriginCursor
			} else if event.Cursor.OriginCursor != childCursor {
				t.Fatalf("child bootstrap was split across cursors: first=%+v event=%+v", childCursor, event.Cursor)
			}
			if event.Record.Envelope.Kind == protocol.EventTurnInterrupted && event.Record.Envelope.TurnID == "turn-parent" {
				inheritedInterruption = true
			}
		default:
			t.Fatalf("unexpected origin %q", event.Cursor.OriginSessionID)
		}
	}
	if parentEvents != 3 || childEvents == 0 || !inheritedInterruption {
		t.Fatalf("origin counts parent=%d child=%d inherited_interruption=%v", parentEvents, childEvents, inheritedInterruption)
	}
}

func TestLegacyCheckpointLineageWithoutKindStillComposesParentPrefix(t *testing.T) {
	root := t.TempDir()
	store := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	parent, err := store.Create(t.Context(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	parentRef := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(parent.ID)}
	parentHead, err := store.Head(t.Context(), parentRef)
	if err != nil {
		t.Fatal(err)
	}
	legacyLineage := journal.SessionLineage{
		ParentSessionID:  protocol.SessionID(parent.ID),
		ParentCursor:     parentHead,
		CheckpointDigest: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("a", 64)},
	}
	child, err := store.CreateWithLineage(t.Context(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"}, &legacyLineage)
	if err != nil {
		t.Fatal(err)
	}
	lineagePath := filepath.Join(root, "workspaces", workspace.ID, "sessions", child.ID, "lineage.json")
	raw, err := os.ReadFile(lineagePath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"kind"`) {
		t.Fatalf("legacy lineage unexpectedly wrote a kind: %s", raw)
	}
	assertLineageWireFamily(t, raw, false)
	stored, err := store.SessionLineage(t.Context(), protocol.SessionID(child.ID))
	if err != nil {
		t.Fatal(err)
	}
	if stored == nil || stored.Kind != "" || *stored != legacyLineage {
		t.Fatalf("stored legacy lineage=%+v want=%+v", stored, legacyLineage)
	}
	page, err := store.ReadComposedRange(t.Context(), journal.ComposedReadRequest{SessionID: protocol.SessionID(child.ID), Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) == 0 || page.Events[0].Cursor.OriginSessionID != protocol.SessionID(parent.ID) {
		t.Fatalf("legacy checkpoint did not compose parent prefix: %+v", page)
	}
}

func TestSubagentLineageIsIdentityOnlyAndRequiresExactParentAnchor(t *testing.T) {
	root := t.TempDir()
	store := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	parent, err := store.Create(t.Context(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	parentRef := protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(parent.ID)}
	anchor, err := store.Head(t.Context(), parentRef)
	if err != nil {
		t.Fatal(err)
	}
	lineage := journal.SessionLineage{
		Kind:                journal.LineageSubagent,
		ParentSessionID:     protocol.SessionID(parent.ID),
		ParentCursor:        anchor,
		DelegationAttemptID: "attempt-1",
		ManifestDigest:      protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("b", 64)},
	}
	reservedID, err := store.ReserveSessionID()
	if err != nil {
		t.Fatal(err)
	}
	child, err := store.CreateWithIdentity(t.Context(), reservedID, workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"}, &lineage)
	if err != nil {
		t.Fatal(err)
	}
	if child.ID != string(reservedID) {
		t.Fatalf("child ID=%q want reserved %q", child.ID, reservedID)
	}
	raw, err := os.ReadFile(filepath.Join(root, "workspaces", workspace.ID, "sessions", child.ID, "lineage.json"))
	if err != nil {
		t.Fatal(err)
	}
	assertLineageWireFamily(t, raw, true)
	page, err := store.ReadComposedRange(t.Context(), journal.ComposedReadRequest{SessionID: reservedID, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) == 0 {
		t.Fatal("identity-only child has no bootstrap events")
	}
	for _, event := range page.Events {
		if event.Cursor.OriginSessionID != reservedID {
			t.Fatalf("identity-only child composed parent event: %+v", event)
		}
	}
	badCursor := lineage
	badCursor.ParentCursor.CommitSeq++
	if _, err := store.CreateWithLineage(t.Context(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"}, &badCursor); err == nil || !strings.Contains(err.Error(), "anchored committed prefix") {
		t.Fatalf("bad parent cursor err=%v", err)
	}
}

func assertLineageWireFamily(t *testing.T, raw []byte, subagent bool) {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	_, hasKind := fields["kind"]
	_, hasCheckpoint := fields["checkpoint_digest"]
	_, hasAttempt := fields["delegation_attempt_id"]
	_, hasManifest := fields["manifest_digest"]
	if subagent {
		if !hasKind || hasCheckpoint || !hasAttempt || !hasManifest {
			t.Fatalf("subagent lineage wire fields=%s", raw)
		}
		return
	}
	if hasKind || !hasCheckpoint || hasAttempt || hasManifest {
		t.Fatalf("legacy checkpoint lineage wire fields=%s", raw)
	}
}

func TestSubagentLineageRejectsIncompletePayloadAndForeignWorkspace(t *testing.T) {
	root := t.TempDir()
	store := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	parent, err := store.Create(t.Context(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := store.Head(t.Context(), protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(parent.ID)})
	if err != nil {
		t.Fatal(err)
	}
	valid := journal.SessionLineage{
		Kind:                journal.LineageSubagent,
		ParentSessionID:     protocol.SessionID(parent.ID),
		ParentCursor:        anchor,
		DelegationAttemptID: "attempt-1",
		ManifestDigest:      protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("c", 64)},
	}
	for _, test := range []struct {
		name    string
		lineage journal.SessionLineage
	}{
		{name: "missing attempt", lineage: func() journal.SessionLineage { value := valid; value.DelegationAttemptID = ""; return value }()},
		{name: "missing manifest", lineage: func() journal.SessionLineage { value := valid; value.ManifestDigest = protocol.Digest{}; return value }()},
		{name: "checkpoint mixed in", lineage: func() journal.SessionLineage {
			value := valid
			value.CheckpointDigest = protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("d", 64)}
			return value
		}()},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := store.CreateWithLineage(t.Context(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"}, &test.lineage); err == nil || !strings.Contains(err.Error(), "exactly one payload family") {
				t.Fatalf("error=%v", err)
			}
		})
	}
	otherWorkspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateWithLineage(t.Context(), otherWorkspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"}, &valid); err == nil || !strings.Contains(err.Error(), "another workspace") {
		t.Fatalf("foreign workspace error=%v", err)
	}
}

func TestSubagentLineageAllowsStorageDepthBeyondProductDepthOne(t *testing.T) {
	root := t.TempDir()
	store := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	parent, err := store.Create(t.Context(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	parentHead, err := store.Head(t.Context(), protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(parent.ID)})
	if err != nil {
		t.Fatal(err)
	}
	firstLineage := journal.SessionLineage{Kind: journal.LineageSubagent, ParentSessionID: protocol.SessionID(parent.ID), ParentCursor: parentHead, DelegationAttemptID: "attempt-1", ManifestDigest: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("e", 64)}}
	first, err := store.CreateWithLineage(t.Context(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"}, &firstLineage)
	if err != nil {
		t.Fatal(err)
	}
	firstHead, err := store.Head(t.Context(), protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(first.ID)})
	if err != nil {
		t.Fatal(err)
	}
	secondLineage := journal.SessionLineage{Kind: journal.LineageSubagent, ParentSessionID: protocol.SessionID(first.ID), ParentCursor: firstHead, DelegationAttemptID: "attempt-2", ManifestDigest: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("f", 64)}}
	second, err := store.CreateWithLineage(t.Context(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"}, &secondLineage)
	if err != nil {
		t.Fatalf("storage incorrectly enforced product depth one: %v", err)
	}
	page, err := store.ReadComposedRange(t.Context(), journal.ComposedReadRequest{SessionID: protocol.SessionID(second.ID), Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range page.Events {
		if event.Cursor.OriginSessionID != protocol.SessionID(second.ID) {
			t.Fatalf("nested subagent inherited parent context: %+v", event)
		}
	}
}

func TestLineageRejectsMissingParentCycleAndDepth65(t *testing.T) {
	root := t.TempDir()
	store := jsonl.New(root, jsonl.Options{Encoder: passthroughEncoder{}})
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	digest := protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("1", 64)}
	missing := journal.SessionLineage{
		ParentSessionID:  "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ParentCursor:     protocol.CommittedCursor{JournalKind: protocol.JournalSession, JournalID: "01ARZ3NDEKTSV4RRFFQ69G5FAV", CommitSeq: 1, TransactionID: "legacy:missing"},
		CheckpointDigest: digest,
	}
	if _, err := store.CreateWithLineage(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"}, &missing); !errors.Is(err, journal.ErrLineageParentMissing) {
		t.Fatalf("missing parent err=%v", err)
	}

	var sessions []domain.Session
	var heads []protocol.CommittedCursor
	for range 66 {
		session, err := store.Create(context.Background(), workspace, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"})
		if err != nil {
			t.Fatal(err)
		}
		head, err := store.Head(context.Background(), protocol.JournalRef{Kind: protocol.JournalSession, ID: protocol.JournalID(session.ID)})
		if err != nil {
			t.Fatal(err)
		}
		sessions = append(sessions, session)
		heads = append(heads, head)
	}
	for index := 1; index < len(sessions); index++ {
		writeLineageFixture(t, root, workspace.ID, sessions[index].ID, journal.SessionLineage{
			ParentSessionID: protocol.SessionID(sessions[index-1].ID), ParentCursor: heads[index-1], CheckpointDigest: digest,
		})
	}
	if _, err := store.ReadComposedRange(context.Background(), journal.ComposedReadRequest{SessionID: protocol.SessionID(sessions[len(sessions)-1].ID), Limit: 1000}); !errors.Is(err, journal.ErrLineageDepthExceeded) {
		t.Fatalf("depth-65 err=%v", err)
	}

	writeLineageFixture(t, root, workspace.ID, sessions[0].ID, journal.SessionLineage{
		ParentSessionID: protocol.SessionID(sessions[1].ID), ParentCursor: heads[1], CheckpointDigest: digest,
	})
	if _, err := store.ReadComposedRange(context.Background(), journal.ComposedReadRequest{SessionID: protocol.SessionID(sessions[1].ID), Limit: 100}); !errors.Is(err, journal.ErrLineageCycle) {
		t.Fatalf("cycle err=%v", err)
	}
}

func writeLineageFixture(t *testing.T, root, workspaceID, sessionID string, lineage journal.SessionLineage) {
	t.Helper()
	raw := fmt.Sprintf(`{"parent_session_id":%q,"parent_cursor":{"journal_kind":%q,"journal_id":%q,"commit_seq":%d,"transaction_id":%q},"checkpoint_digest":{"algorithm":%q,"value":%q}}`,
		lineage.ParentSessionID, lineage.ParentCursor.JournalKind, lineage.ParentCursor.JournalID,
		lineage.ParentCursor.CommitSeq, lineage.ParentCursor.TransactionID,
		lineage.CheckpointDigest.Algorithm, lineage.CheckpointDigest.Value)
	path := filepath.Join(root, "workspaces", workspaceID, "sessions", sessionID, "lineage.json")
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
}
