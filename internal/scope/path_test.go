package scope_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/muratmirgun/yordam/internal/scope"
)

func TestResolveCanonicalizesInsideAndOutsidePaths(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	canonicalOutside, err := filepath.EvalSymlinks(outside)
	if err != nil {
		t.Fatal(err)
	}
	insidePath := filepath.Join(root, "inside.txt")
	if err := os.WriteFile(insidePath, []byte("ok"), 0o600); err != nil {
		t.Fatal(err)
	}
	outPath := filepath.Join(outside, "outside.txt")
	if err := os.WriteFile(outPath, []byte("no"), 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		target     string
		wantPath   string
		wantInside bool
	}{
		{name: "relative inside", target: "inside.txt", wantPath: filepath.Join(canonicalRoot, "inside.txt"), wantInside: true},
		{name: "absolute inside", target: insidePath, wantPath: filepath.Join(canonicalRoot, "inside.txt"), wantInside: true},
		{name: "relative traversal", target: filepath.Join("..", filepath.Base(outside), "outside.txt"), wantPath: filepath.Join(canonicalOutside, "outside.txt"), wantInside: false},
		{name: "absolute outside", target: outPath, wantPath: filepath.Join(canonicalOutside, "outside.txt"), wantInside: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := scope.Resolve(root, test.target, false)
			if err != nil {
				t.Fatal(err)
			}
			if resolved.Path != test.wantPath || resolved.Inside != test.wantInside {
				t.Fatalf("resolved=%#v want path=%q inside=%v", resolved, test.wantPath, test.wantInside)
			}
		})
	}
}

func TestResolveHandlesSymlinkEscapeAndOneMissingLeaf(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	canonicalOutside, err := filepath.EvalSymlinks(outside)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}

	escaped, err := scope.Resolve(root, filepath.Join("escape", "new.txt"), true)
	if err != nil {
		t.Fatal(err)
	}
	if escaped.Path != filepath.Join(canonicalOutside, "new.txt") || escaped.Inside {
		t.Fatalf("symlink escape accepted: %#v", escaped)
	}

	missing, err := scope.Resolve(root, "new.txt", true)
	if err != nil {
		t.Fatal(err)
	}
	if missing.Path != filepath.Join(canonicalRoot, "new.txt") || !missing.Inside {
		t.Fatalf("missing=%#v", missing)
	}

	if _, err := scope.Resolve(root, filepath.Join("missing", "new.txt"), true); err == nil {
		t.Fatal("accepted more than one missing path component")
	}
}

func TestResolveRejectsDanglingSymlinkAsMissingLeaf(t *testing.T) {
	root := t.TempDir()
	outPath := filepath.Join(t.TempDir(), "missing.txt")
	if err := os.Symlink(outPath, filepath.Join(root, "dangling")); err != nil {
		t.Fatal(err)
	}

	if _, err := scope.Resolve(root, "dangling", true); err == nil {
		t.Fatal("accepted an existing dangling symlink as a missing leaf")
	}
}
