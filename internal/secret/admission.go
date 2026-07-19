package secret

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"sync"
)

var ErrSecretDetected = errors.New("registered secret detected")

// AdmissionScanner detects the raw and exact encoded forms of registered
// secrets. It is immutable and safe for concurrent use.
type AdmissionScanner struct {
	mu            sync.RWMutex
	variants      [][]byte
	longest       int
	revoked       bool
	onStreamOpen  func()
	onStreamClose func()
}

// AdmissionStream scans chunked input while retaining only the overlap needed
// to detect a variant split across adjacent writes.
type AdmissionStream struct {
	mu       sync.Mutex
	scanner  *AdmissionScanner
	overlap  []byte
	detected bool
	closed   bool
}

func NewAdmissionScanner(values ...[]byte) *AdmissionScanner {
	return newAdmissionScanner(expandVariants(values), nil, nil)
}

func newAdmissionScanner(variants [][]byte, onOpen, onClose func()) *AdmissionScanner {
	scanner := &AdmissionScanner{variants: cloneByteSlices(variants), onStreamOpen: onOpen, onStreamClose: onClose}
	for _, variant := range scanner.variants {
		if len(variant) > scanner.longest {
			scanner.longest = len(variant)
		}
	}
	return scanner
}

func (s *AdmissionScanner) Scan(value []byte) bool {
	if s == nil {
		return true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.revoked {
		return true
	}
	for _, variant := range s.variants {
		if bytes.Contains(value, variant) {
			return true
		}
	}
	return false
}

func (s *AdmissionScanner) Stream() *AdmissionStream {
	if s == nil {
		s = newAdmissionScanner(nil, nil, nil)
		s.revoke()
	}
	if s.onStreamOpen != nil {
		s.onStreamOpen()
	}
	return &AdmissionStream{scanner: s}
}

func (s *AdmissionScanner) revoke() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.revoked = true
	s.variants = nil
	s.longest = 0
	s.mu.Unlock()
}

func (s *AdmissionScanner) overlapSize() int {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.revoked {
		return 0
	}
	return s.longest - 1
}

func (s *AdmissionStream) Write(value []byte) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.detected {
		return s.detected
	}
	candidate := make([]byte, 0, len(s.overlap)+len(value))
	candidate = append(candidate, s.overlap...)
	candidate = append(candidate, value...)
	if s.scanner.Scan(candidate) {
		s.detected = true
		s.overlap = nil
		return true
	}
	keep := s.scanner.overlapSize()
	if keep <= 0 {
		s.overlap = nil
	} else if len(candidate) <= keep {
		s.overlap = append(s.overlap[:0], candidate...)
	} else {
		s.overlap = append(s.overlap[:0], candidate[len(candidate)-keep:]...)
	}
	return false
}

// Close releases the stream lifetime and reports whether any write contained a
// complete registered variant. It is safe to call more than once.
func (s *AdmissionStream) Close() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	if s.closed {
		detected := s.detected
		s.mu.Unlock()
		return detected
	}
	s.closed = true
	detected := s.detected || s.scanner.Scan(nil)
	onClose := s.scanner.onStreamClose
	s.overlap = nil
	s.mu.Unlock()
	if onClose != nil {
		onClose()
	}
	return detected
}

func expandVariants(values [][]byte) [][]byte {
	unique := make(map[string][]byte)
	for _, raw := range values {
		if len(raw) == 0 {
			continue
		}
		encoded := [][]byte{
			bytes.Clone(raw),
			[]byte(base64.StdEncoding.EncodeToString(raw)),
			[]byte(base64.RawStdEncoding.EncodeToString(raw)),
			[]byte(base64.URLEncoding.EncodeToString(raw)),
			[]byte(base64.RawURLEncoding.EncodeToString(raw)),
			[]byte(hex.EncodeToString(raw)),
			[]byte(strings.ToUpper(hex.EncodeToString(raw))),
		}
		for _, variant := range encoded {
			if len(variant) != 0 {
				unique[string(variant)] = variant
			}
		}
	}
	variants := make([][]byte, 0, len(unique))
	for _, variant := range unique {
		variants = append(variants, variant)
	}
	slices.SortFunc(variants, func(left, right []byte) int {
		if difference := len(right) - len(left); difference != 0 {
			return difference
		}
		return bytes.Compare(left, right)
	})
	return variants
}

func cloneByteSlices(values [][]byte) [][]byte {
	cloned := make([][]byte, len(values))
	for index, value := range values {
		cloned[index] = bytes.Clone(value)
	}
	return cloned
}
