package ports

import (
	"context"
	"io"

	"github.com/muratmirgun/yordam/internal/domain"
)

type ArtifactStore interface {
	Put(context.Context, string, string, io.Reader, int64) (domain.Artifact, error)
	Open(context.Context, domain.Artifact) (io.ReadCloser, error)
}
