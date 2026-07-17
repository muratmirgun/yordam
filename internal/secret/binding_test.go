package secret_test

import (
	"bytes"
	"sync"
	"testing"

	"github.com/muratmirgun/yordam/internal/secret"
)

func TestBindingReplacesCurrentRedactorAndPreservesSnapshots(t *testing.T) {
	binding := secret.NewBinding(secret.New("old-secret"))
	snapshot := binding.Snapshot()
	if got := binding.String("old-secret"); got != "[REDACTED]" {
		t.Fatalf("old binding=%q", got)
	}
	binding.Replace(secret.New("new-secret"))
	if got := binding.String("new-secret"); got != "[REDACTED]" {
		t.Fatalf("new binding=%q", got)
	}
	if got := snapshot.String("old-secret new-secret"); got != "[REDACTED] new-secret" {
		t.Fatalf("snapshot changed=%q", got)
	}
}

func TestBindingRedactionMethodsAreSafeDuringReplacement(t *testing.T) {
	binding := secret.NewBinding(secret.New("alpha"))
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 100 {
				_ = binding.String("alpha beta")
				_ = binding.Bytes([]byte("alpha beta"))
				raw, err := binding.JSON(map[string]string{"value": "alpha beta"})
				if err != nil || !bytes.Contains(raw, []byte("[REDACTED]")) && !bytes.Contains(raw, []byte("alpha")) {
					t.Errorf("JSON=%s err=%v", raw, err)
				}
			}
		}()
	}
	for range 100 {
		binding.Replace(secret.New("alpha"))
		binding.Replace(secret.New("beta"))
	}
	wait.Wait()
}
