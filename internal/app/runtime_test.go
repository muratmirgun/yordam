package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/agent"
	"github.com/muratmirgun/yordam/internal/cli"
	"github.com/muratmirgun/yordam/internal/config"
	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/permission"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/secret"
	"github.com/muratmirgun/yordam/internal/session/jsonl"
	"github.com/muratmirgun/yordam/internal/testsupport/agentfixture"
)

func TestNewUsesEmptyRedactorBindingWhenNoneIsSupplied(t *testing.T) {
	application := New(Options{RuntimeSet: RuntimeSet{Redactor: secret.New("candidate-secret")}})
	if got := application.redactors.String("candidate-secret"); got != "candidate-secret" {
		t.Fatalf("implicit binding inherited candidate secrets: %q", got)
	}
}

func TestRuntimeSetReadinessClassifiesModelsAndCredentials(t *testing.T) {
	set := RuntimeSet{
		Models:         []domain.ModelSelection{{Profile: "primary", Model: "a"}, {Profile: "secondary", Model: "b"}},
		CredentialEnvs: map[string]string{"primary": "PRIMARY_KEY", "secondary": "SECONDARY_KEY"},
		Credentials:    map[string]string{"primary": "secret", "secondary": ""},
		configPath:     "/home/user/.config/yordam/config.jsonc",
	}
	if err := set.Ready(domain.ModelSelection{Profile: "primary", Model: "a"}); err != nil {
		t.Fatal(err)
	}
	for selection, want := range map[domain.ModelSelection]string{
		{Profile: "secondary", Model: "b"}: "SECONDARY_KEY",
		{Profile: "missing", Model: "x"}:   "not configured",
	} {
		err := set.Ready(selection)
		var typed *domain.TypedError
		if err == nil || !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), "/home/user/.config/yordam/config.jsonc") || !strings.Contains(err.Error(), "restart") && want == "SECONDARY_KEY" || !asTypedConfiguration(err, &typed) {
			t.Fatalf("selection=%+v error=%v", selection, err)
		}
	}
}

func TestRuntimeSetReadyReturnsConfigurationErrorBeforeCompatibilityBypass(t *testing.T) {
	configuration := configurationError("/home/user/.config/yordam/config.jsonc", "configuration is invalid; edit the file and run /reload", nil)
	set := RuntimeSet{ConfigurationError: configuration, unchecked: true}

	if got := set.Ready(domain.ModelSelection{}); !errors.Is(got, configuration) {
		t.Fatalf("Ready() error = %v, want configuration error %v", got, configuration)
	}
}

func TestRuntimeBuilderBindsProvidersToolsLimitsAndCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(response, "data: [DONE]\n\n")
	}))
	defer server.Close()

	t.Setenv("PRIMARY_KEY", "primary-secret")
	t.Setenv("SECONDARY_KEY", "secondary-secret")
	t.Setenv("ORDINARY_VALUE", "preserved")
	cfg := loadRuntimeConfig(t, server.URL+"/v1", 1)
	builder := newRuntimeBuilderForTest(t, server.Client())
	set, err := builder.build(cfg, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	wantModels := []domain.ModelSelection{{Profile: "primary", Model: "a"}, {Profile: "primary", Model: "b"}, {Profile: "secondary", Model: "c"}}
	if !slices.Equal(set.Models, wantModels) || set.DefaultSelection != wantModels[0] {
		t.Fatalf("models=%+v default=%+v", set.Models, set.DefaultSelection)
	}
	if set.Credentials["primary"] != "primary-secret" || set.Credentials["secondary"] != "secondary-secret" {
		t.Fatalf("credentials not bound by provider")
	}
	if summary := fmt.Sprintf("%+v", set); strings.Contains(summary, "primary-secret") || strings.Contains(summary, "secondary-secret") {
		t.Fatalf("runtime set summary exposes credentials: %s", summary)
	}
	serialized, err := json.Marshal(set)
	if err != nil {
		t.Fatalf("serialize runtime set summary: %v", err)
	}
	if strings.Contains(string(serialized), "primary-secret") || strings.Contains(string(serialized), "secondary-secret") {
		t.Fatalf("serialized runtime set summary exposes credentials: %s", serialized)
	}
	if _, ok := set.Runtime.(*agent.OrchestratedRunner); !ok || set.Manifest.Body.Limits.MaxToolCalls != 7 || set.Manifest.Body.Limits.ShellTimeoutNanos != int64(time.Second) {
		t.Fatalf("runtime=%T limits=%+v", set.Runtime, set.Manifest.Body.Limits)
	}
	if len(set.ProviderCatalog.List()) != 3 || len(set.Manifest.Body.Tools) != 4 {
		t.Fatalf("provider models=%d tool descriptors=%d", len(set.ProviderCatalog.List()), len(set.Manifest.Body.Tools))
	}
}

func TestRuntimeGenerationManifestIsImmutableAcrossCandidateBuilds(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "generation-a")
	cfg := loadRuntimeConfig(t, "https://example.invalid/v1", 120)
	builder := newRuntimeBuilderForTest(t, nil)
	first, err := builder.build(cfg, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Validate(); err != nil {
		t.Fatalf("first runtime set: %v", err)
	}
	want := protocol.DeepCopy(first.Manifest)

	t.Setenv("PRIMARY_KEY", "generation-b")
	second, err := builder.build(cfg, domain.ModelSelection{Profile: "secondary", Model: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Validate(); err != nil {
		t.Fatalf("second runtime set: %v", err)
	}
	if first.Manifest.ID == second.Manifest.ID {
		t.Fatalf("runtime generations were not independently identified: first=%+v second=%+v", first.Manifest, second.Manifest)
	}
	if got := first.Manifest; !reflect.DeepEqual(got, want) {
		t.Fatalf("generation A changed while preparing B:\n got: %#v\nwant: %#v", got, want)
	}

	second.Manifest.Body.Models[0].DisplayName = "mutated candidate"
	second.Manifest.Body.Tools[0].Body.InputSchema[0] = '['
	if got := first.Manifest; !reflect.DeepEqual(got, want) {
		t.Fatalf("candidate manifest aliases active generation:\n got: %#v\nwant: %#v", got, want)
	}
	if err := second.Validate(); err == nil {
		t.Fatal("tampered candidate manifest passed validation")
	}
}

func TestRuntimeCompositionUsesOnlyOrchestratedRunner(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "primary-secret")
	builder := newRuntimeBuilderForTest(t, nil)
	set, err := builder.build(loadRuntimeConfig(t, "https://example.invalid/v1", 120), domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := set.Runtime.(*agent.OrchestratedRunner); !ok {
		t.Fatalf("production runtime=%T, want *agent.OrchestratedRunner", set.Runtime)
	}
	if set.Orchestrator == nil || set.ProviderCatalog == nil || set.ProviderService == nil || set.ToolService == nil || set.AuthorizationService == nil || set.Broker == nil || set.ApplicationService == nil || set.LegacyAdapter == nil {
		t.Fatalf("foundation composition is incomplete: %+v", set)
	}
}

func TestRuntimeGenerationsKeepImmutableRedactors(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "generation-a")
	cfg := loadRuntimeConfig(t, "https://example.invalid/v1", 120)
	builder := newRuntimeBuilderForTest(t, nil)
	first, err := builder.build(cfg, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("PRIMARY_KEY", "generation-b")
	second, err := builder.build(cfg, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	if got := first.Redactor.String("generation-a generation-b"); got != "[REDACTED] generation-b" {
		t.Fatalf("first redactor=%q", got)
	}
	if got := second.Redactor.String("generation-a generation-b"); got != "generation-a [REDACTED]" {
		t.Fatalf("second redactor=%q", got)
	}
	firstRunner, ok := first.Runtime.(*agent.OrchestratedRunner)
	if !ok {
		t.Fatalf("first runtime=%T", first.Runtime)
	}
	release, err := firstRunner.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	first.retireSecrets()
	if got := first.Redactor.String("generation-a generation-b"); got != "[REDACTED] generation-b" {
		t.Fatalf("active first generation redactor=%q", got)
	}
	release()
	if _, err := firstRunner.Acquire(); err == nil {
		t.Fatal("retired generation accepted a new turn lease")
	}
}

func TestRuntimeGenerationBootstrapBindingAdvancesStoreWhileOldOutputIsImmutable(t *testing.T) {
	const generationA = "generation-a"
	const generationB = "generation-b"
	t.Setenv("PRIMARY_KEY", generationA)

	configPath := filepath.Join(t.TempDir(), "config.jsonc")
	if err := os.WriteFile(configPath, []byte(`{
  "model": "primary/a",
  "provider": {
    "primary": {
      "options": {"baseURL": "https://example.invalid/v1", "apiKeyEnv": "PRIMARY_KEY"},
      "models": {"a": {}}
    }
  }
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	application, _, err := Bootstrap(t.Context(), BootstrapOptions{
		ConfigPath: configPath,
		CLI: cli.Options{
			Mode:         domain.ModeAsk,
			DataDir:      t.TempDir(),
			MaxToolCalls: 32,
			ShellTimeout: 120 * time.Second,
		},
		CWD: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	firstSet := application.runtimeSet
	firstRunner, ok := firstSet.Runtime.(*agent.OrchestratedRunner)
	if !ok {
		t.Fatalf("first runtime=%T", application.runtimeSet.Runtime)
	}
	release, err := firstRunner.Acquire()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- application.Run(ctx) }()
	t.Setenv("PRIMARY_KEY", generationB)
	application.Commands() <- Command{Kind: CommandReloadConfig}
	select {
	case event := <-application.Events():
		if event.Kind != EventReloadCompleted || !event.Applied || event.Err != nil {
			t.Fatalf("reload event=%+v", event)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for reload")
	}

	if got := firstSet.Redactor.String(generationA + " " + generationB); got != "[REDACTED] "+generationB {
		t.Fatalf("active old-generation redactor=%q", got)
	}
	if got := application.redactors.String(generationA + " " + generationB); got != generationA+" [REDACTED]" {
		t.Fatalf("new production binding=%q", got)
	}
	release()
	if _, err := firstRunner.Acquire(); err == nil {
		t.Fatal("retired runtime started a new producer")
	}

	application.Commands() <- Command{Kind: CommandShutdown}
	if err := <-done; err != nil {
		t.Fatalf("app shutdown: %v", err)
	}
}

func TestRuntimeBuilderRedactsConfiguredAndOverrideCredentials(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "configured-secret")
	t.Setenv("YORDAM_API_KEY", "override-secret")
	cfg := loadRuntimeConfig(t, "https://example.invalid/v1", 120)
	builder := newRuntimeBuilderForTest(t, nil)
	set, err := builder.build(cfg, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	if set.Credentials["primary"] != "override-secret" {
		t.Fatalf("effective credential=%q", set.Credentials["primary"])
	}
	if got := set.Redactor.String("configured-secret override-secret"); got != "[REDACTED] [REDACTED]" {
		t.Fatalf("redacted=%q", got)
	}
	if set.Admission == nil || set.RuntimeGenerationID == "" {
		t.Fatalf("runtime has no generation lease: %+v", set)
	}
	if got := set.Redactor.String("Y29uZmlndXJlZC1zZWNyZXQ="); got != "[REDACTED]" {
		t.Fatalf("encoded redacted=%q", got)
	}
}

func TestRuntimeBuilderRegistersGenerationInSharedProductionRegistry(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "shared-production-secret")
	shared := secret.NewRegistry()
	builder := newRuntimeBuilderForTest(t, nil)
	builder.secrets = shared
	set, err := builder.build(loadRuntimeConfig(t, "https://example.invalid/v1", 120), domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	producer, err := shared.AcquireExisting(set.RuntimeGenerationID)
	if err != nil {
		t.Fatalf("runtime generation not registered in shared registry: %v", err)
	}
	defer func() { _ = producer.Close() }()
	if got := producer.String("c2hhcmVkLXByb2R1Y3Rpb24tc2VjcmV0"); got != "[REDACTED]" {
		t.Fatalf("shared generation redaction=%q", got)
	}
}

func TestRuntimeSetBindsApproverToItsRunner(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "primary-secret")
	builder := newRuntimeBuilderForTest(t, nil)
	set, err := builder.build(loadRuntimeConfig(t, "https://example.invalid/v1", 120), domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	_, ok := set.Runtime.(*agent.OrchestratedRunner)
	if !ok {
		t.Fatalf("runtime=%T", set.Runtime)
	}
	approver := &agentfixture.Approver{}
	set.BindApprover(approver)
}

func TestRuntimeBuilderFallsBackToRootWhenCurrentModelWasRemoved(t *testing.T) {
	t.Setenv("PRIMARY_KEY", "primary-secret")
	cfg := loadRuntimeConfig(t, "https://example.invalid/v1", 120)
	builder := newRuntimeBuilderForTest(t, nil)
	set, err := builder.build(cfg, domain.ModelSelection{Profile: "removed", Model: "gone"})
	if err != nil {
		t.Fatal(err)
	}
	if set.DefaultSelection != (domain.ModelSelection{Profile: "primary", Model: "a"}) {
		t.Fatalf("default selection=%+v", set.DefaultSelection)
	}
}

func asTypedConfiguration(err error, target **domain.TypedError) bool {
	return errors.As(err, target) && (*target).Kind == domain.ErrorConfigurationInvalid
}

func loadRuntimeConfig(t *testing.T, baseURL string, timeout int) config.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.jsonc")
	body := fmt.Sprintf(`{
  "model": "primary/a",
  "provider": {
    "primary": {
      "options": {"baseURL": %q, "apiKeyEnv": "PRIMARY_KEY"},
      "models": {"b": {}, "a": {}}
    },
    "secondary": {
      "options": {"baseURL": %q, "apiKeyEnv": "SECONDARY_KEY"},
      "models": {"c": {}}
    }
  },
  "limits": {"maxToolCalls": 7, "shellTimeoutSeconds": %d}
}`, baseURL, baseURL, timeout)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.LoadOptions{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func newRuntimeBuilderForTest(t *testing.T, client *http.Client) runtimeBuilder {
	t.Helper()
	workspace, err := jsonl.WorkspaceFromPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store := jsonl.New(t.TempDir(), jsonl.Options{})
	session, err := store.Create(t.Context(), workspace, domain.ModeAsk, domain.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	return runtimeBuilder{
		configPath:    "/home/user/.config/yordam/config.jsonc",
		cli:           cli.Options{MaxToolCalls: 32, ShellTimeout: 120 * time.Second},
		workspace:     workspace,
		store:         store,
		policy:        newPolicyBinding(permission.NewSession(domain.ModeAsk)),
		activeSession: &sessionBinding{id: session.ID},
		runtimeEvents: make(chan agent.RuntimeEvent, 64),
		httpClient:    client,
	}
}
