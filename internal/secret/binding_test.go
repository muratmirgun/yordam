package secret_test

import (
	"encoding/json"
	"runtime"
	"sync"
	"testing"

	"github.com/muratmirgun/yordam/internal/secret"
)

func TestBindingReplacesCurrentRedactorAndPreservesSnapshots(t *testing.T) {
	binding := secret.NewBinding(secret.New("old-secret"))
	snapshot := binding.Snapshot()
	assertRedactionMethods(t, binding, "old-secret new-secret", "[REDACTED] new-secret")

	binding.Replace(secret.New("new-secret"))
	assertRedactionMethods(t, binding, "old-secret new-secret", "old-secret [REDACTED]")
	assertRedactionMethods(t, snapshot, "old-secret new-secret", "[REDACTED] new-secret")
}

func TestBindingRedactionMethodsAreSafeDuringReplacement(t *testing.T) {
	binding := secret.NewBinding(secret.New("alpha"))
	redactors := []secret.Redactor{secret.New("alpha"), secret.New("beta")}
	start := make(chan struct{})
	done := make(chan struct{})

	var readers sync.WaitGroup
	for range 100 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			<-start
			for range 100 {
				if got := binding.String("alpha beta"); !validConcurrentRedaction(got) {
					t.Errorf("String=%q", got)
				}
				if got := string(binding.Bytes([]byte("alpha beta"))); !validConcurrentRedaction(got) {
					t.Errorf("Bytes=%q", got)
				}
				raw, err := binding.JSON("alpha beta")
				var got string
				if err != nil {
					t.Errorf("JSON error=%v", err)
				} else if err := json.Unmarshal(raw, &got); err != nil {
					t.Errorf("JSON=%s error=%v", raw, err)
				} else if !validConcurrentRedaction(got) {
					t.Errorf("JSON value=%q", got)
				}
			}
		}()
	}

	var replacer sync.WaitGroup
	replacer.Add(1)
	go func() {
		defer replacer.Done()
		<-start
		for {
			for _, redactor := range redactors {
				select {
				case <-done:
					return
				default:
					binding.Replace(redactor)
				}
			}
			runtime.Gosched()
		}
	}()

	close(start)
	readers.Wait()
	close(done)
	replacer.Wait()
}

func assertRedactionMethods(t *testing.T, redactor secret.Redacting, value, want string) {
	t.Helper()
	if got := redactor.String(value); got != want {
		t.Errorf("String=%q want=%q", got, want)
	}
	if got := string(redactor.Bytes([]byte(value))); got != want {
		t.Errorf("Bytes=%q want=%q", got, want)
	}
	raw, err := redactor.JSON(value)
	if err != nil {
		t.Fatalf("JSON error=%v", err)
	}
	var got string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("JSON=%s error=%v", raw, err)
	}
	if got != want {
		t.Errorf("JSON value=%q want=%q", got, want)
	}
}

func validConcurrentRedaction(value string) bool {
	return value == "[REDACTED] beta" || value == "alpha [REDACTED]"
}
