package secret

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/muratmirgun/yordam/internal/protocol"
)

var (
	ErrGenerationRetired  = errors.New("secret generation is retired")
	ErrGenerationUnknown  = errors.New("secret generation is unknown")
	ErrGenerationConflict = errors.New("secret generation values conflict")
	ErrLeaseClosed        = errors.New("secret producer lease is closed")
)

type Registry struct {
	mu          sync.Mutex
	generations map[protocol.RuntimeGenerationID]*generation
}

type generation struct {
	id       protocol.RuntimeGenerationID
	variants [][]byte
	redactor Redactor
	retired  bool
	leases   int
	streams  int
}

// Lease pins one generation's scanner and redactor for a producer lifetime.
type Lease struct {
	mu       sync.Mutex
	registry *Registry
	entry    *generation
	scanner  *AdmissionScanner
	redactor Redactor
	closed   bool
}

type LeasedRedactionStream struct {
	mu        sync.Mutex
	admission *AdmissionStream
	redaction *Stream
	closed    bool
}

func NewRegistry() *Registry {
	return &Registry{generations: make(map[protocol.RuntimeGenerationID]*generation)}
}

// Acquire registers a generation on first use and returns a producer lease.
// Later acquisitions must name the same set of raw secret values.
func (r *Registry) Acquire(id protocol.RuntimeGenerationID, values [][]byte) (*Lease, error) {
	if r == nil {
		return nil, fmt.Errorf("secret registry is required")
	}
	if id == "" {
		return nil, fmt.Errorf("runtime generation is required")
	}
	variants := expandVariants(values)
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.generations[id]
	if entry == nil {
		entry = &generation{id: id, variants: variants, redactor: redactorForVariants(variants)}
		r.generations[id] = entry
	} else {
		if entry.retired {
			return nil, ErrGenerationRetired
		}
		if len(values) != 0 && !equalVariants(entry.variants, variants) {
			return nil, ErrGenerationConflict
		}
	}
	entry.leases++
	return r.newLeaseLocked(entry), nil
}

// AcquireExisting returns a producer lease without changing registered values.
func (r *Registry) AcquireExisting(id protocol.RuntimeGenerationID) (*Lease, error) {
	if r == nil {
		return nil, fmt.Errorf("secret registry is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.generations[id]
	if entry == nil {
		return nil, ErrGenerationUnknown
	}
	if entry.retired {
		return nil, ErrGenerationRetired
	}
	entry.leases++
	return r.newLeaseLocked(entry), nil
}

func (r *Registry) newLeaseLocked(entry *generation) *Lease {
	lease := &Lease{registry: r, entry: entry, redactor: entry.redactor}
	lease.scanner = newAdmissionScanner(entry.variants, func() {
		r.mu.Lock()
		entry.streams++
		r.mu.Unlock()
	}, func() {
		r.mu.Lock()
		entry.streams--
		r.releaseRetiredLocked(entry)
		r.mu.Unlock()
	})
	return lease
}

func (r *Registry) Retire(id protocol.RuntimeGenerationID) error {
	if r == nil {
		return fmt.Errorf("secret registry is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.generations[id]
	if entry == nil {
		return ErrGenerationUnknown
	}
	entry.retired = true
	r.releaseRetiredLocked(entry)
	return nil
}

func (r *Registry) releaseRetiredLocked(entry *generation) {
	if entry.retired && entry.leases == 0 && entry.streams == 0 {
		entry.variants = nil
		entry.redactor = New()
	}
}

func (l *Lease) Scanner() *AdmissionScanner {
	if l == nil {
		return NewAdmissionScanner()
	}
	return l.scanner
}

func (l *Lease) Stream() (*AdmissionStream, error) {
	if l == nil {
		return nil, ErrLeaseClosed
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, ErrLeaseClosed
	}
	return l.scanner.Stream(), nil
}

func (l *Lease) RedactionStream() (*LeasedRedactionStream, error) {
	admission, err := l.Stream()
	if err != nil {
		return nil, err
	}
	return &LeasedRedactionStream{admission: admission, redaction: l.redactor.Stream()}, nil
}

func (s *LeasedRedactionStream) Write(value string) string {
	if s == nil {
		return value
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ""
	}
	s.admission.Write([]byte(value))
	return s.redaction.Write(value)
}

func (s *LeasedRedactionStream) Close() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ""
	}
	s.closed = true
	s.admission.Close()
	return s.redaction.Close()
}

func (l *Lease) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	registry, entry := l.registry, l.entry
	l.mu.Unlock()
	registry.mu.Lock()
	entry.leases--
	registry.releaseRetiredLocked(entry)
	registry.mu.Unlock()
	return nil
}

func (l *Lease) String(value string) string {
	if l == nil {
		return value
	}
	return l.redactor.String(value)
}

func (l *Lease) Bytes(value []byte) []byte {
	if l == nil {
		return bytes.Clone(value)
	}
	return l.redactor.Bytes(value)
}

func (l *Lease) JSON(value any) (json.RawMessage, error) {
	if l == nil {
		return New().JSON(value)
	}
	redacted, err := l.redactor.JSON(value)
	if err != nil {
		return nil, err
	}
	if l.scanner.Scan(redacted) {
		return json.RawMessage(`{"content":"[REDACTED]"}`), nil
	}
	return redacted, nil
}

func redactorForVariants(variants [][]byte) Redactor {
	values := make([]string, len(variants))
	for index, variant := range variants {
		values[index] = string(variant)
	}
	return New(values...)
}

func equalVariants(left, right [][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !bytes.Equal(left[index], right[index]) {
			return false
		}
	}
	return true
}
