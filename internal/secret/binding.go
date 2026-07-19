package secret

import (
	"encoding/json"
	"fmt"
	"sync"
)

type Redacting interface {
	String(string) string
	Bytes([]byte) []byte
	JSON(any) (json.RawMessage, error)
}

// AcquireLease returns an independently owned lease for the currently bound
// runtime generation. Bindings backed only by compatibility redactors cannot
// be used by production admission sinks.
func (b *Binding) AcquireLease() (*Lease, error) {
	if b == nil {
		return nil, ErrLeaseClosed
	}
	b.mu.RLock()
	lease, ok := b.current.(*Lease)
	b.mu.RUnlock()
	if !ok || lease == nil {
		return nil, fmt.Errorf("generation-bound admission is required: %w", ErrLeaseClosed)
	}
	return lease.Derive()
}

type Binding struct {
	mu      sync.RWMutex
	current Redacting
}

var _ Redacting = Redactor{}
var _ Redacting = (*Binding)(nil)

func NewBinding(initial Redacting) *Binding {
	if initial == nil {
		initial = New()
	}
	return &Binding{current: initial}
}

func (b *Binding) Replace(next Redacting) {
	if next == nil {
		next = New()
	}
	b.mu.Lock()
	b.current = next
	b.mu.Unlock()
}

func (b *Binding) Snapshot() Redacting {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.current
}

func (b *Binding) Scanner() *AdmissionScanner {
	snapshot := b.Snapshot()
	if admitted, ok := snapshot.(interface{ Scanner() *AdmissionScanner }); ok {
		return admitted.Scanner()
	}
	return NewAdmissionScanner()
}

func (b *Binding) String(value string) string { return b.Snapshot().String(value) }

func (b *Binding) Bytes(value []byte) []byte { return b.Snapshot().Bytes(value) }

func (b *Binding) JSON(value any) (json.RawMessage, error) { return b.Snapshot().JSON(value) }
