package jsonl_test

import (
	"context"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/journal"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
)

var _ ports.ArtifactStore = (*jsonl.Store)(nil)
var _ journal.Repository = (*jsonl.Store)(nil)

func TestListDoesNotCrossWorkspace(t *testing.T) {
	store := jsonl.New(t.TempDir(), jsonl.Options{})
	first, _ := jsonl.WorkspaceFromPath(t.TempDir())
	second, _ := jsonl.WorkspaceFromPath(t.TempDir())
	if _, err := store.Create(context.Background(), first, domain.ModeAsk, domain.ModelSelection{Profile: "p", Model: "m"}); err != nil {
		t.Fatal(err)
	}
	got, err := store.List(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("cross-workspace sessions: %v", got)
	}
}
