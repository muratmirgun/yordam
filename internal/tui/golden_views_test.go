package tui_test

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

var goldenViews = map[int]string{
	80: `Command bridge | /Users/murat/oss/t~ommand-bridge-demo | mode: ask | local/gpt-5
--------------------------------------------------------------------------------
[diff] | tool | error
DIFF internal/app/app.go
@@ -1,3 +1,3 @@
-old bridge
+typed command bridge
Enter: collapse | Esc: close
`,
	120: `Command bridge | /Users/murat/oss/tui-yordam-v0.1/workspaces/command-bridge-demo | mode: ask | local/gpt-5
------------------------------------------------------------------------------------------------------------------------
USER                                                                           | [diff] | tool | error
Inspect internal/app/app.go.                                                   | DIFF internal/app/app.go
                                                                               | @@ -1,3 +1,3 @@
ASSISTANT                                                                      | -old bridge
The command bridge keeps UI and runtime separate.                              | +typed command bridge
                                                                               | Enter: collapse | Esc: close
TOOL read [completed]                                                          |
internal/app/app.go:1-40                                                       |
------------------------------------------------------------------------------------------------------------------------
┃ Ask Yordam
`,
	160: `Command bridge | /Users/murat/oss/tui-yordam-v0.1/workspaces/command-bridge-demo | mode: ask | local/gpt-5
----------------------------------------------------------------------------------------------------------------------------------------------------------------
USER                                                                                                     | [diff] | tool | error
Inspect internal/app/app.go.                                                                             | DIFF internal/app/app.go
                                                                                                         | @@ -1,3 +1,3 @@
ASSISTANT                                                                                                | -old bridge
The command bridge keeps UI and runtime separate.                                                        | +typed command bridge
                                                                                                         | Enter: collapse | Esc: close
TOOL read [completed]                                                                                    |
internal/app/app.go:1-40                                                                                 |
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
		80:  "d17a5cdf3802eaa46ebb55d89b8f84c718f3c6d3e733e723bc7e491a5d52ce7b",
		120: "9cc5a002c6d54c846bc0276dc0a015fd90f87c860c689ff0e03f30c527d0ad35",
		160: "c2ae70b230c41db36e6a3a0b601c5add6c75a5cb70b05c2ca71fbb6543f53a03",
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
