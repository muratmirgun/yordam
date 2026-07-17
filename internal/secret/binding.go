package secret

import (
	"encoding/json"
	"sync"
)

type Redacting interface {
	String(string) string
	Bytes([]byte) []byte
	JSON(any) (json.RawMessage, error)
}

type Binding struct {
	mu      sync.RWMutex
	current Redactor
}

var _ Redacting = Redactor{}
var _ Redacting = (*Binding)(nil)

func NewBinding(initial Redactor) *Binding { return &Binding{current: initial} }

func (b *Binding) Replace(next Redactor) {
	b.mu.Lock()
	b.current = next
	b.mu.Unlock()
}

func (b *Binding) Snapshot() Redactor {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.current
}

func (b *Binding) String(value string) string { return b.Snapshot().String(value) }

func (b *Binding) Bytes(value []byte) []byte { return b.Snapshot().Bytes(value) }

func (b *Binding) JSON(value any) (json.RawMessage, error) { return b.Snapshot().JSON(value) }
