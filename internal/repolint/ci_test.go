package repolint_test

import (
	"os"
	"strings"
	"testing"
)

func TestCIWorkflowContainsRequiredGates(t *testing.T) {
	raw, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	required := []string{
		"contents: read",
		"test:",
		"cross-build:",
		"secret-hygiene:",
		"os: [ubuntu-latest, macos-latest]",
		"runs-on: ${{ matrix.os }}",
		"uses: actions/checkout@v7",
		"uses: actions/setup-go@v6",
		"go-version-file: go.mod",
		"cache: true",
		"run: ./scripts/check.sh",
		"GOOS: darwin",
		"GOOS: linux",
		"GOARCH: amd64",
		"GOARCH: arm64",
		"CGO_ENABLED=0 GOOS=${{ matrix.GOOS }} GOARCH=${{ matrix.GOARCH }} go build -trimpath -o dist/yordam-${{ matrix.GOOS }}-${{ matrix.GOARCH }} ./cmd/yordam",
		"YORDAM_API_KEY: synthetic-ci-secret",
		"PROFILE_KEY: synthetic-profile-secret",
		"TEST_OUTPUT_ROOT: ${{ runner.temp }}/yordam-secret-scan",
		"go test -tags acceptance ./internal/acceptance -run 'TestV010Acceptance/secret_hygiene' -count=1 -v",
		"TMPDIR: ${{ env.TEST_OUTPUT_ROOT }}/tmp",
		"rg -uuu 'synthetic-(ci|profile)-secret' \"$TEST_OUTPUT_ROOT\"",
		"[[ $status -eq 1 ]]",
	}
	for _, needle := range required {
		if !strings.Contains(text, needle) {
			t.Errorf("workflow missing %q", needle)
		}
	}
	if strings.Contains(text, "windows-latest") || strings.Contains(text, "GOOS: windows") {
		t.Error("workflow must not include Windows")
	}
	if strings.Contains(text, "github.workspace }}") {
		t.Error("secret scan output must be outside the checkout")
	}
	if got := strings.Count(text, "GOOS:"); got != 4 {
		t.Errorf("workflow has %d cross-build targets, want 4", got)
	}
}
