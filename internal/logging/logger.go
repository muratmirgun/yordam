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
	tree = l.cleanMap(tree)
	tree["event"] = l.redactor.String(name)
	raw, err = json.Marshal(tree)
	if err != nil {
		return err
	}
	raw = append(l.redactor.Bytes(raw), '\n')
	written, err := l.dest.Write(raw)
	if err == nil && written != len(raw) {
		return io.ErrShortWrite
	}
	return err
}

func (l *Logger) cleanMap(value map[string]any) map[string]any {
	for key, item := range value {
		if sensitiveKey(key) {
			delete(value, key)
			continue
		}
		value[key] = l.cleanValue(item)
	}
	return value
}

func (l *Logger) cleanValue(value any) any {
	switch typed := value.(type) {
	case string:
		return l.redactor.String(typed)
	case []any:
		for index, item := range typed {
			typed[index] = l.cleanValue(item)
		}
		return typed
	case map[string]any:
		return l.cleanMap(typed)
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
