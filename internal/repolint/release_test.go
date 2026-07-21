package repolint_test

import (
	"os"
	"strings"
	"testing"
)

func TestReleaseConfigTargetsOnlyApprovedPlatforms(t *testing.T) {
	raw, err := os.ReadFile("../../.goreleaser.yml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, needle := range []string{
		"version: 2",
		"project_name: yordam",
		"main: ./cmd/yordam",
		"env: [CGO_ENABLED=0]",
		"goos: [darwin, linux]",
		"goarch: [amd64, arm64]",
		"formats: [tar.gz]",
		"checksums.txt",
		"artifacts: archive",
		"spdx-json",
		"internal/buildinfo.Version={{.Version}}",
		"internal/buildinfo.Commit={{.Commit}}",
		"internal/buildinfo.Date={{.Date}}",
	} {
		if !strings.Contains(text, needle) {
			t.Errorf("release config missing %q", needle)
		}
	}
	for _, forbidden := range []string{"windows", "freebsd", "386"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("release config contains unapproved target %q", forbidden)
		}
	}
}

func TestReleaseWorkflowEnforcesPreflightAndSignedTagPublish(t *testing.T) {
	raw, err := os.ReadFile("../../.github/workflows/release.yml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, needle := range []string{
		"workflow_dispatch:",
		"tags: [\"v*\"]",
		"contents: read",
		"verify:",
		"package:",
		"smoke:",
		"publish:",
		"./scripts/check.sh",
		"verification.verified",
		"RELEASE_MAINTAINER_ALLOWLIST: ${{ vars.RELEASE_MAINTAINER_ALLOWLIST }}",
		"GITHUB_ACTOR",
		"release actor is not an authorized maintainer",
		"v[0-9]+\\.[0-9]+\\.[0-9]+",
		"github.com/goreleaser/goreleaser/v2@v2.17.0",
		"github.com/anchore/syft/cmd/syft@v1.46.0",
		"goreleaser release --snapshot --clean --skip=publish",
		"go test -tags acceptance ./internal/acceptance -run TestV030SelfHostedRuntime -count=1 -v",
		"name: release-candidate",
		"needs: [verify, package, smoke]",
		"github.event_name == 'push'",
		"needs.verify.outputs.publish == 'true'",
		"contents: write",
		"GH_REPO: ${{ github.repository }}",
		"--verify-tag",
		`awk -v name="$(basename "$archive")" '$2 == name {print $1}' dist/checksums.txt`,
	} {
		if !strings.Contains(text, needle) {
			t.Errorf("release workflow missing %q", needle)
		}
	}
	for _, row := range []string{
		"{runner: ubuntu-24.04, os: linux, arch: amd64}",
		"{runner: ubuntu-24.04-arm, os: linux, arch: arm64}",
		"{runner: macos-15-intel, os: darwin, arch: amd64}",
		"{runner: macos-15, os: darwin, arch: arm64}",
	} {
		if !strings.Contains(text, row) {
			t.Errorf("release workflow missing native runner row %q", row)
		}
	}
	if got := strings.Count(text, "{runner:"); got != 4 {
		t.Errorf("release workflow has %d native runner rows, want 4", got)
	}
	if strings.Contains(text, "windows") {
		t.Fatal("release workflow must not include Windows")
	}
	if strings.Count(text, "uses: actions/upload-artifact@") != 1 {
		t.Fatal("release workflow must upload the packaged artifact exactly once")
	}
	if strings.Count(text, "uses: actions/download-artifact@") != 2 {
		t.Fatal("release workflow must reuse the packaged artifact in smoke and publish")
	}
	if strings.Count(text, "contents: write") != 1 {
		t.Fatal("only the publish job may have write permission")
	}
	if strings.Contains(text, `grep -F "  $(basename`) {
		t.Fatal("release checksum lookup must match the exact filename, not a substring")
	}
	packageIndex := strings.Index(text, "goreleaser release --snapshot --clean --skip=publish")
	acceptanceIndex := strings.Index(text, "go test -tags acceptance ./internal/acceptance -run TestV030SelfHostedRuntime -count=1 -v")
	if packageIndex < 0 || acceptanceIndex < packageIndex {
		t.Fatal("release-candidate acceptance must run after snapshot packaging")
	}
	for _, secret := range []string{"OPENAI_API_KEY", "ANTHROPIC_API_KEY", "YORDAM_API_KEY", "PROFILE_KEY"} {
		if strings.Contains(text, secret) {
			t.Errorf("release workflow must not contain model credential %q", secret)
		}
	}
}
