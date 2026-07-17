package output

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/secret"
)

const ModelExcerptBytes = 32 << 10
const RetainedBytes = 10 << 20

type Options struct {
	SessionID        string
	CurrentSessionID func() string
	Artifacts        ports.ArtifactStore
	Redact           secret.Redacting
}

type Buffer struct {
	mu        sync.Mutex
	opts      Options
	data      bytes.Buffer
	truncated bool
}

func New(opts Options) *Buffer {
	if opts.Redact == nil {
		opts.Redact = secret.New()
	}
	return &Buffer{opts: opts}
}

func (b *Buffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	original := len(value)
	remaining := RetainedBytes - b.data.Len()
	if remaining <= 0 {
		if original > 0 {
			b.truncated = true
		}
		return original, nil
	}
	if len(value) > remaining {
		value = value[:remaining]
		b.truncated = true
	}
	_, err := b.data.Write(value)
	return original, err
}

func (b *Buffer) Snapshot() (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	redacted := b.opts.Redact.Bytes(b.data.Bytes())
	return string(excerpt(redacted)), b.truncated
}

func (b *Buffer) Result(ctx context.Context) (domain.ToolResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	redacted := b.opts.Redact.Bytes(b.data.Bytes())
	content := excerpt(redacted)
	result := domain.ToolResult{
		Status:    domain.ToolSucceeded,
		Content:   string(content),
		Truncated: b.truncated,
	}
	if b.data.Len() <= ModelExcerptBytes && len(redacted) <= ModelExcerptBytes && !b.truncated {
		return result, nil
	}
	if b.opts.Artifacts == nil {
		return result, fmt.Errorf("artifact store is required for output exceeding %d bytes", ModelExcerptBytes)
	}
	sessionID := b.opts.SessionID
	if b.opts.CurrentSessionID != nil {
		sessionID = b.opts.CurrentSessionID()
	}
	artifact, err := b.opts.Artifacts.Put(ctx, sessionID, "text/plain", bytes.NewReader(redacted), RetainedBytes)
	if err != nil {
		return result, err
	}
	result.ArtifactIDs = []string{artifact.ID}
	result.Truncated = result.Truncated || artifact.Truncated
	return result, nil
}

func excerpt(value []byte) []byte {
	if len(value) <= ModelExcerptBytes {
		return value
	}
	return value[len(value)-ModelExcerptBytes:]
}
