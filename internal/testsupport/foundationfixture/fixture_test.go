package foundationfixture

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

var expectedNames = []string{
	"evidence-digest-mismatch", "incomplete-batch", "invalid-known-payload",
	"invalid-sequence", "lineage-cycle", "missing-evidence-blob", "mixed-v1-v2",
	"unknown-future-kind", "unsupported-envelope-version", "unsupported-payload-version",
	"v1-stale-edit-recovery", "v1-truncated-final", "v1-unmatched-tool-start", "v1-valid",
}

func TestNamesAndDigest(t *testing.T) {
	if got := Names(); !reflect.DeepEqual(got, expectedNames) {
		t.Fatalf("Names() = %q, want %q", got, expectedNames)
	}
	if got, want := Digest(), "c20a9601d4a87b7105c2938d57abd9591bef932bd157d563f3cc5d5dd39ccaeb"; got != want {
		t.Fatalf("Digest() = %q, want %q", got, want)
	}
}

func TestMaterializeSelectedScenarioByteIdentical(t *testing.T) {
	root := t.TempDir()
	if err := Materialize(root, "v1-valid"); err != nil {
		t.Fatalf("Materialize() error = %v", err)
	}

	want, err := os.ReadFile(filepath.Join("..", "..", "acceptance", "testdata", "foundation", "v1-valid", "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "v1-valid", "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("materialized events.jsonl differs from the source fixture: got %d bytes, want %d bytes", len(got), len(want))
	}
	if _, err := os.Stat(filepath.Join(root, "invalid-sequence")); !os.IsNotExist(err) {
		t.Fatalf("unselected scenario stat error = %v, want not exist", err)
	}
}

func TestMaterializeRejectsUnknownAndDuplicateNames(t *testing.T) {
	for _, names := range [][]string{{"unknown"}, {"v1-valid", "v1-valid"}} {
		if err := Materialize(t.TempDir(), names...); err == nil {
			t.Fatalf("Materialize(%q) error = nil, want rejection", names)
		}
	}
}

func TestMaterializeRequiresAbsoluteRoot(t *testing.T) {
	if err := Materialize("relative-root", "v1-valid"); err == nil {
		t.Fatal("Materialize() error = nil, want rejection for relative root")
	}
}

func TestValidateDefinitionsRejectsUnsafeOrDuplicatePaths(t *testing.T) {
	for _, path := range []string{"../escape", "/absolute", "safe/../../escape", ""} {
		t.Run(path, func(t *testing.T) {
			err := validateDefinitions([]definition{{name: "fixture", files: []fileDefinition{{path: path, data: "x"}}}})
			if err == nil {
				t.Fatalf("validateDefinitions(%q) error = nil, want rejection", path)
			}
		})
	}
	if err := validateDefinitions([]definition{{
		name:  "fixture",
		files: []fileDefinition{{path: "same", data: "one"}, {path: "same", data: "two"}},
	}}); err == nil {
		t.Fatal("validateDefinitions() error = nil, want rejection for duplicate paths")
	}
}

func TestReadReturnsDefensiveCopy(t *testing.T) {
	first, ok := Read("v1-valid", "events.jsonl")
	if !ok {
		t.Fatal("Read() returned ok = false")
	}
	first[0] ^= 0xff
	second, ok := Read("v1-valid", "events.jsonl")
	if !ok || bytes.Equal(first, second) {
		t.Fatal("Read() did not return a defensive copy")
	}
}

func TestReadMatchesEverySourceFixtureFile(t *testing.T) {
	fixtureRoot := filepath.Join("..", "..", "acceptance", "testdata", "foundation")
	err := filepath.WalkDir(fixtureRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || entry.Name() == "fixtures.sha256" {
			return err
		}
		relative, err := filepath.Rel(fixtureRoot, path)
		if err != nil {
			return err
		}
		name, file := filepath.Split(relative)
		name = filepath.Clean(name)
		want, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		got, ok := Read(name, file)
		if !ok || !bytes.Equal(got, want) {
			t.Errorf("Read(%q, %q) does not match source", name, file)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
