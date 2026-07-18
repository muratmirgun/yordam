package secret

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
)

const replacement = "[REDACTED]"

type Redactor struct {
	values []string
}

type Stream struct {
	redactor Redactor
	pending  string
}

func New(values ...string) Redactor {
	filtered := slices.DeleteFunc(append([]string(nil), values...), func(value string) bool {
		return value == ""
	})
	slices.SortFunc(filtered, func(left, right string) int {
		if difference := len(right) - len(left); difference != 0 {
			return difference
		}
		return strings.Compare(left, right)
	})
	return Redactor{values: filtered}
}

func (r Redactor) String(value string) string {
	for _, configured := range r.values {
		value = strings.ReplaceAll(value, configured, replacement)
	}
	return value
}

func (r Redactor) Stream() *Stream {
	return &Stream{redactor: r}
}

func (s *Stream) Write(value string) string {
	s.pending += value
	return s.consume(false)
}

func (s *Stream) Close() string {
	return s.consume(true)
}

func (s *Stream) revoke() {
	if s == nil {
		return
	}
	s.redactor = New()
	s.pending = ""
}

func (s *Stream) consume(flush bool) string {
	var output strings.Builder
	for s.pending != "" {
		matched := false
		prefix := false
		for _, configured := range s.redactor.values {
			if strings.HasPrefix(s.pending, configured) {
				output.WriteString(replacement)
				s.pending = s.pending[len(configured):]
				matched = true
				break
			}
			if !flush && strings.HasPrefix(configured, s.pending) {
				prefix = true
			}
		}
		if matched {
			continue
		}
		if prefix {
			break
		}
		output.WriteByte(s.pending[0])
		s.pending = s.pending[1:]
	}
	return output.String()
}

func (r Redactor) Bytes(value []byte) []byte {
	return []byte(r.String(string(value)))
}

func (r Redactor) JSON(value any) (json.RawMessage, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var tree any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&tree); err != nil {
		return nil, err
	}
	return json.Marshal(r.redactTree(tree))
}

func (r Redactor) redactTree(value any) any {
	switch typed := value.(type) {
	case string:
		return r.String(typed)
	case []any:
		for index, item := range typed {
			typed[index] = r.redactTree(item)
		}
	case map[string]any:
		for key, item := range typed {
			typed[key] = r.redactTree(item)
		}
	}
	return value
}
