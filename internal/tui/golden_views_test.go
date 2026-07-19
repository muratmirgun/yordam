package tui_test

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

var goldenViews = map[int]string{
	80: `Command bridge | /Users/murat/oss/t~ommand-bridge-demo | mode: ask | local/gpt-5
--------------------------------------------------------------------------------
CONTEXT
Auto compaction: unavailable
[diff] | tool | error
DIFF internal/app/app.go
@@ -1,3 +1,3 @@
-old bridge
+typed command bridge
Enter: collapse | Esc: close | /compact
`,
	120: `Command bridge | /Users/murat/oss/tui-yordam-v0.1/workspaces/command-bridge-demo | mode: ask | local/gpt-5
------------------------------------------------------------------------------------------------------------------------
USER                                                                           | CONTEXT
Inspect internal/app/app.go.                                                   | Auto compaction: unavailable
                                                                               | [diff] | tool | error
ASSISTANT                                                                      | DIFF internal/app/app.go
The command bridge keeps UI and runtime separate.                              | @@ -1,3 +1,3 @@
                                                                               | -old bridge
TOOL read [completed]                                                          | +typed command bridge
internal/app/app.go:1-40                                                       | Enter: collapse | Esc: close | /compact
------------------------------------------------------------------------------------------------------------------------
┃ Ask Yordam
`,
	160: `Command bridge | /Users/murat/oss/tui-yordam-v0.1/workspaces/command-bridge-demo | mode: ask | local/gpt-5
----------------------------------------------------------------------------------------------------------------------------------------------------------------
USER                                                                                                     | CONTEXT
Inspect internal/app/app.go.                                                                             | Auto compaction: unavailable
                                                                                                         | [diff] | tool | error
ASSISTANT                                                                                                | DIFF internal/app/app.go
The command bridge keeps UI and runtime separate.                                                        | @@ -1,3 +1,3 @@
                                                                                                         | -old bridge
TOOL read [completed]                                                                                    | +typed command bridge
internal/app/app.go:1-40                                                                                 | Enter: collapse | Esc: close | /compact
----------------------------------------------------------------------------------------------------------------------------------------------------------------
┃ Ask Yordam
`,
}

func goldenView(width int) (string, bool) {
	view, ok := goldenViews[width]
	return view, ok
}

func TestGoldenViewFixturesAreStable(t *testing.T) {
	wantHashes := map[int]string{
		80:  "bda97e79e7a3f82e6878d1b7e06cd6c87cc199ee71bcf4aa675dbd46d8c45ef6",
		120: "d52598bd9a0ac95d77e5baf8d7393fa1d61f9aa2b786645b39b1d7e75321c8cb",
		160: "67f33ecb7fbf6310368ff4be947951a2e691eed455889c2ab36f20772617080a",
	}

	for width, wantHash := range wantHashes {
		view, ok := goldenView(width)
		if !ok {
			t.Fatalf("golden view missing for width %d", width)
		}
		gotHash := sha256.Sum256([]byte(view))
		if got := hex.EncodeToString(gotHash[:]); got != wantHash {
			t.Fatalf("golden view hash for width %d = %s, want %s", width, got, wantHash)
		}
	}

	if _, ok := goldenView(100); ok {
		t.Fatal("golden view unexpectedly exists for width 100")
	}
}
