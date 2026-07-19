package logging

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/muratmirgun/yordam/internal/secret"
)

type Logger struct {
	mu       sync.Mutex
	dest     io.Writer
	redactor secret.Redacting
	binding  *secret.Binding
}

// NewGenerationBound constructs a production logger that acquires an owned
// lease for every event and therefore follows reloads without an admission gap.
func NewGenerationBound(destination io.Writer, binding *secret.Binding) (*Logger, error) {
	if binding == nil {
		return nil, fmt.Errorf("secret generation binding is required")
	}
	lease, err := binding.AcquireLease()
	if err != nil {
		return nil, err
	}
	_ = lease.Close()
	return &Logger{dest: destination, binding: binding}, nil
}

func New(destination io.Writer, redactor secret.Redacting) *Logger {
	if redactor == nil {
		redactor = secret.New()
	}
	return &Logger{dest: destination, redactor: redactor}
}

// NewLeased constructs a logger whose redaction set is pinned to one runtime
// generation. The caller owns the producer lease and closes it after the logger
// can no longer emit events.
func NewLeased(destination io.Writer, lease *secret.Lease) (*Logger, error) {
	if lease == nil {
		return nil, fmt.Errorf("secret producer lease is required")
	}
	return New(destination, lease), nil
}

func (l *Logger) Event(name string, fields map[string]any) error {
	if l == nil || l.dest == nil {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	redactor := l.redactor
	var lease *secret.Lease
	if l.binding != nil {
		var err error
		lease, err = l.binding.AcquireLease()
		if err != nil {
			return err
		}
		defer lease.Close()
		redactor = lease
	}

	raw, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	var tree map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&tree); err != nil {
		return err
	}
	if tree == nil {
		tree = make(map[string]any)
	}
	tree = l.cleanMap(tree, redactor)
	tree["event"] = redactor.String(name)
	raw, err = json.Marshal(tree)
	if err != nil {
		return err
	}
	raw = append(redactor.Bytes(raw), '\n')
	written, err := l.dest.Write(raw)
	if err == nil && written != len(raw) {
		return io.ErrShortWrite
	}
	return err
}

func (l *Logger) cleanMap(value map[string]any, redactor secret.Redacting) map[string]any {
	for key, item := range value {
		if sensitiveKey(key) {
			delete(value, key)
			continue
		}
		value[key] = l.cleanValue(item, redactor)
	}
	return value
}

func (l *Logger) cleanValue(value any, redactor secret.Redacting) any {
	switch typed := value.(type) {
	case string:
		return redactor.String(typed)
	case []any:
		for index, item := range typed {
			typed[index] = l.cleanValue(item, redactor)
		}
		return typed
	case map[string]any:
		return l.cleanMap(typed, redactor)
	default:
		return value
	}
}

func sensitiveKey(key string) bool {
	switch strings.ToLower(key) {
	case "authorization", "proxy-authorization", "api_key", "api-key":
		return true
	default:
		return false
	}
}
