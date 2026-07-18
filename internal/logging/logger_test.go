package logging_test

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/muratmirgun/yordam/internal/logging"
	"github.com/muratmirgun/yordam/internal/secret"
)

func TestLoggerDropsSensitiveKeysAndRedactsNestedValues(t *testing.T) {
	const configuredSecret = "top-\"secret\""
	var destination bytes.Buffer
	logger := logging.New(&destination, secret.New(configuredSecret))

	err := logger.Event("provider_error", map[string]any{
		"Authorization": "Bearer " + configuredSecret,
		"message":       "failed " + configuredSecret,
		"nested": []any{
			map[string]any{"Proxy-Authorization": configuredSecret, "api_KEY": configuredSecret},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := destination.String()
	if strings.Contains(got, configuredSecret) {
		t.Fatalf("secret remained in log: %s", got)
	}

	var event map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(destination.Bytes()), &event); err != nil {
		t.Fatal(err)
	}
	assertSafeLogTree(t, event)
}

func TestLoggerIsDisabledWithNilDestination(t *testing.T) {
	logger := logging.New(nil, secret.New("secret"))
	if err := logger.Event("ignored", map[string]any{"unsupported": func() {}}); err != nil {
		t.Fatalf("disabled logger returned an error: %v", err)
	}
}

func TestLoggerAcceptsNilFields(t *testing.T) {
	var destination bytes.Buffer
	logger := logging.New(&destination, secret.New())
	if err := logger.Event("started", nil); err != nil {
		t.Fatal(err)
	}
	if got := destination.String(); got != "{\"event\":\"started\"}\n" {
		t.Fatalf("log=%q", got)
	}
}

func TestLoggerPreservesLargeIntegers(t *testing.T) {
	const sequence = ^uint64(0)
	var destination bytes.Buffer
	logger := logging.New(&destination, secret.New())
	if err := logger.Event("checkpoint", map[string]any{"sequence": sequence}); err != nil {
		t.Fatal(err)
	}

	var event struct {
		Sequence uint64 `json:"sequence"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(destination.Bytes()), &event); err != nil {
		t.Fatal(err)
	}
	if event.Sequence != sequence {
		t.Fatalf("sequence=%d want=%d log=%s", event.Sequence, sequence, destination.Bytes())
	}
}

func TestLoggerWritesConcurrentEventsAsCompleteJSONLines(t *testing.T) {
	var destination bytes.Buffer
	logger := logging.New(&destination, secret.New("secret"))
	const events = 64

	var wait sync.WaitGroup
	for index := range events {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := logger.Event("worker", map[string]any{"index": index, "message": "secret"}); err != nil {
				t.Errorf("event %d: %v", index, err)
			}
		}()
	}
	wait.Wait()

	lines := bytes.Split(bytes.TrimSpace(destination.Bytes()), []byte{'\n'})
	if len(lines) != events {
		t.Fatalf("lines=%d want=%d", len(lines), events)
	}
	for index, line := range lines {
		if !json.Valid(line) || bytes.Contains(line, []byte("secret")) {
			t.Fatalf("line %d is unsafe or invalid: %s", index, line)
		}
	}
}

func TestLoggerUsesCurrentRedactorBinding(t *testing.T) {
	var destination bytes.Buffer
	binding := secret.NewBinding(secret.New("old-secret"))
	logger := logging.New(&destination, binding)
	if err := logger.Event("old-secret", map[string]any{"message": "old-secret new-secret"}); err != nil {
		t.Fatal(err)
	}
	binding.Replace(secret.New("new-secret"))
	if err := logger.Event("new-secret", map[string]any{"message": "old-secret new-secret"}); err != nil {
		t.Fatal(err)
	}

	lines := bytes.Split(bytes.TrimSpace(destination.Bytes()), []byte{'\n'})
	if len(lines) != 2 {
		t.Fatalf("lines=%d log=%q", len(lines), destination.String())
	}
	wantMessages := []string{"[REDACTED] new-secret", "old-secret [REDACTED]"}
	for index, line := range lines {
		var event struct {
			Event   string `json:"event"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("line %d: %v", index, err)
		}
		if event.Event != "[REDACTED]" || event.Message != wantMessages[index] {
			t.Fatalf("line %d event=%+v want message=%q", index, event, wantMessages[index])
		}
	}
}

func TestLeasedLoggerRedactsEncodedSecretVariants(t *testing.T) {
	registry := secret.NewRegistry()
	lease, err := registry.Acquire("generation-a", [][]byte{[]byte("logger-secret")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Close() }()
	var destination bytes.Buffer
	logger, err := logging.NewLeased(&destination, lease)
	if err != nil {
		t.Fatal(err)
	}
	if err := logger.Event("provider", map[string]any{"message": "bG9nZ2VyLXNlY3JldA=="}); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(destination.Bytes(), []byte("bG9nZ2VyLXNlY3JldA==")) {
		t.Fatalf("encoded secret remained in log: %s", destination.Bytes())
	}
}

func TestLeasedLoggerRejectsMissingLease(t *testing.T) {
	if _, err := logging.NewLeased(io.Discard, nil); err == nil {
		t.Fatal("leased logger accepted a nil lease")
	}
}

func assertSafeLogTree(t *testing.T, value any) {
	t.Helper()
	switch typed := value.(type) {
	case map[string]any:
		for key, item := range typed {
			switch strings.ToLower(key) {
			case "authorization", "proxy-authorization", "api_key", "api-key":
				t.Fatalf("sensitive key %q remained in log tree: %#v", key, value)
			}
			assertSafeLogTree(t, item)
		}
	case []any:
		for _, item := range typed {
			assertSafeLogTree(t, item)
		}
	case string:
		if strings.Contains(typed, "top-") {
			t.Fatalf("sensitive value remained: %s", typed)
		}
	}
}
