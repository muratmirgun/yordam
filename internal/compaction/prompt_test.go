package compaction_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/compaction"
	"github.com/muratmirgun/yordam/internal/protocol"
)

func TestBuildSummaryRequestCarriesContractSelectionAndPriorSummary(t *testing.T) {
	t.Parallel()
	selection := promptSelection(t)
	prior := promptSource(t, "prior", "prior summary")
	request, err := compaction.BuildSummaryRequest(selection, &prior)
	if err != nil {
		t.Fatal(err)
	}
	if len(request.Messages) != 2 || request.Messages[0].Role != "system" || request.Messages[1].Role != "user" {
		t.Fatalf("request=%+v", request)
	}
	text := request.Messages[0].Blocks[0].Text + "\n" + request.Messages[1].Blocks[0].Text
	for _, field := range []string{"goal", "constraints", "decisions", "files", "commands_and_tests", "unresolved", "children", "skills", "unknown_effects", "prior summary"} {
		if !strings.Contains(text, field) {
			t.Fatalf("request omitted %q: %s", field, text)
		}
	}
}

func TestBuildSummaryRequestRejectsPriorThatExceedsSelectionInputLimit(t *testing.T) {
	t.Parallel()
	selection := promptSelection(t)
	selection.InputLimitBytes = 600
	prior := promptSource(t, "prior", strings.Repeat("p", 600))
	if _, err := compaction.BuildSummaryRequest(selection, &prior); err == nil {
		t.Fatal("accepted prior content beyond the model-visible selection limit")
	}
}

func TestParseSummaryRequiresExactCanonicalContractAndBounds(t *testing.T) {
	t.Parallel()
	valid := []byte(`{"skills":[],"goal":"ship","constraints":[],"decisions":[],"files":[],"commands_and_tests":[],"unresolved":[],"children":[],"unknown_effects":[]}`)
	admitted, err := compaction.ParseSummary(valid)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"children":[],"commands_and_tests":[],"constraints":[],"decisions":[],"files":[],"goal":"ship","skills":[],"unknown_effects":[],"unresolved":[]}`
	if string(admitted) != want {
		t.Fatalf("canonical=%s want=%s", admitted, want)
	}
	for _, raw := range [][]byte{
		[]byte(`{"goal":"x","constraints":[],"decisions":[],"files":[],"commands_and_tests":[],"unresolved":[],"children":[],"skills":[],"unknown_effects":[],"extra":true}`),
		[]byte(`{"goal":"x","constraints":[],"decisions":[],"files":[],"commands_and_tests":[],"unresolved":[],"children":[],"skills":[]}`),
		[]byte{0xff, 0xfe},
		[]byte(`{"goal":[],"constraints":[],"decisions":[],"files":[],"commands_and_tests":[],"unresolved":[],"children":[],"skills":[],"unknown_effects":[]}`),
		[]byte(`{"goal":"x","constraints":{},"decisions":[],"files":[],"commands_and_tests":[],"unresolved":[],"children":[],"skills":[],"unknown_effects":[]}`),
		append([]byte(`{"goal":"`), append(bytes.Repeat([]byte("x"), 128*1024), []byte(`","constraints":[],"decisions":[],"files":[],"commands_and_tests":[],"unresolved":[],"children":[],"skills":[],"unknown_effects":[]}`)...)...),
	} {
		if _, err := compaction.ParseSummary(raw); err == nil {
			t.Fatalf("accepted invalid summary %q", raw[:min(len(raw), 40)])
		}
	}
}

func TestRevisionUsesCanonicalSelectionAndSummaryOnly(t *testing.T) {
	t.Parallel()
	selection := promptSelection(t)
	summary, err := compaction.ParseSummary([]byte(`{"goal":"ship","constraints":[],"decisions":[],"files":[],"commands_and_tests":[],"unresolved":[],"children":[],"skills":[],"unknown_effects":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	first, err := compaction.Revision(selection, summary)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 64 || first != strings.ToLower(first) {
		t.Fatalf("revision=%q", first)
	}
	selection.Trigger = compaction.TriggerAutomatic
	second, err := compaction.Revision(selection, summary)
	if err != nil || second != first {
		t.Fatalf("trigger changed revision: %q %q err=%v", first, second, err)
	}
	changed, _ := compaction.ParseSummary([]byte(`{"goal":"changed","constraints":[],"decisions":[],"files":[],"commands_and_tests":[],"unresolved":[],"children":[],"skills":[],"unknown_effects":[]}`))
	third, err := compaction.Revision(selection, changed)
	if err != nil || third == first {
		t.Fatalf("summary did not change revision: %q %q err=%v", first, third, err)
	}
}

func promptSelection(t *testing.T) compaction.Selection {
	t.Helper()
	source := promptSource(t, "event-1", "journal facts")
	digest, err := compaction.SourceDigest([]protocol.ContentSource{source})
	if err != nil {
		t.Fatal(err)
	}
	return compaction.Selection{From: selectionCursor(1), Through: selectionCursor(2), Trigger: compaction.TriggerManual, SummarizedEventIDs: []protocol.EventID{"event-1", "event-2"}, Sources: []protocol.ContentSource{source}, SourceDigest: digest}
}

func promptSource(t *testing.T, id, text string) protocol.ContentSource {
	t.Helper()
	content := []protocol.ContentBlock{{Kind: protocol.ContentText, Text: text}}
	raw, err := json.Marshal(content)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.ContentSource{ID: id, Kind: "compaction_event", Scope: "session", Provenance: "journal:session", Digest: protocol.Digest{Algorithm: protocol.DigestSHA256, Value: strings.Repeat("a", 64)}, Content: []protocol.ContentBlock{{Kind: protocol.ContentText, Text: string(raw)}}}
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}
