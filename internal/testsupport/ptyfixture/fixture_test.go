package ptyfixture

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTreeDigestIncludesUntrackedBytesAndExcludesGitBookkeeping(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("one"), 0o600); err != nil {
		t.Fatal(err)
	}
	first, err := TreeDigest(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".git", "index"), []byte("ignored"), 0o600); err != nil {
		t.Fatal(err)
	}
	ignored, err := TreeDigest(root)
	if err != nil || ignored != first {
		t.Fatalf("Git bookkeeping affected tree digest: first=%s ignored=%s err=%v", first, ignored, err)
	}
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("two"), 0o600); err != nil {
		t.Fatal(err)
	}
	changed, err := TreeDigest(root)
	if err != nil || changed == first {
		t.Fatalf("worktree byte change not detected: first=%s changed=%s err=%v", first, changed, err)
	}
}
