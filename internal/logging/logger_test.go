package logging_test

import (
	"bytes"
	"encoding/json"
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
	if err := logger.Event("old", map[string]any{"message": "old-secret"}); err != nil {
		t.Fatal(err)
	}
	binding.Replace(secret.New("new-secret"))
	if err := logger.Event("new", map[string]any{"message": "new-secret"}); err != nil {
		t.Fatal(err)
	}
	if got := destination.String(); strings.Contains(got, "old-secret") || strings.Contains(got, "new-secret") || strings.Count(got, "[REDACTED]") != 2 {
		t.Fatalf("unsafe dynamic log=%q", got)
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
