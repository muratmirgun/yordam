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
	streams  map[*LeasedRedactionStream]struct{}
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
		scanner := NewAdmissionScanner()
		scanner.revoke()
		return scanner
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		scanner := NewAdmissionScanner()
		scanner.revoke()
		return scanner
	}
	return l.scanner
}

// Derive returns an independently owned producer lease for this generation.
// A derived lease remains valid when its parent closes, but no new producer can
// be derived after the generation is retired.
func (l *Lease) Derive() (*Lease, error) {
	if l == nil {
		return nil, ErrLeaseClosed
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil, ErrLeaseClosed
	}
	registry, id := l.registry, l.entry.id
	l.mu.Unlock()
	return registry.AcquireExisting(id)
}

// Admit rejects a secret variant and also rejects use after lease close.
func (l *Lease) Admit(value []byte) error {
	if l == nil {
		return ErrLeaseClosed
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrLeaseClosed
	}
	if l.scanner.Scan(value) {
		return ErrSecretDetected
	}
	return nil
}

func (l *Lease) GenerationID() (protocol.RuntimeGenerationID, error) {
	if l == nil {
		return "", ErrLeaseClosed
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return "", ErrLeaseClosed
	}
	return l.entry.id, nil
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
	if l == nil {
		return nil, ErrLeaseClosed
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, ErrLeaseClosed
	}
	stream := &LeasedRedactionStream{admission: l.scanner.Stream(), redaction: l.redactor.Stream()}
	if l.streams == nil {
		l.streams = make(map[*LeasedRedactionStream]struct{})
	}
	l.streams[stream] = struct{}{}
	return stream, nil
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
	output := s.redaction.Close()
	s.redaction.revoke()
	return output
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
	for stream := range l.streams {
		stream.mu.Lock()
		stream.closed = true
		stream.admission.Close()
		stream.redaction.revoke()
		stream.mu.Unlock()
	}
	l.streams = nil
	l.scanner.revoke()
	l.redactor = New()
	l.mu.Unlock()
	registry.mu.Lock()
	entry.leases--
	registry.releaseRetiredLocked(entry)
	registry.mu.Unlock()
	return nil
}

func (l *Lease) String(value string) string {
	if l == nil {
		return "[REDACTED]"
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return "[REDACTED]"
	}
	return l.redactor.String(value)
}

func (l *Lease) Bytes(value []byte) []byte {
	if l == nil {
		return []byte("[REDACTED]")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return []byte("[REDACTED]")
	}
	return l.redactor.Bytes(value)
}

func (l *Lease) JSON(value any) (json.RawMessage, error) {
	if l == nil {
		return nil, ErrLeaseClosed
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, ErrLeaseClosed
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
