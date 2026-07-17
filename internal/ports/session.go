package ports

import (
	"context"
	"io"

	"github.com/muratmirgun/yordam/internal/domain"
)

type SessionStore interface {
	Create(context.Context, domain.Workspace, domain.PermissionMode, domain.ModelSelection) (domain.Session, error)
	Append(context.Context, string, domain.EventKind, any) (domain.DurableEvent, error)
	Load(context.Context, string) (domain.SessionReplay, error)
	List(context.Context, domain.Workspace) ([]domain.SessionSummary, error)
}

type ArtifactStore interface {
	Put(context.Context, string, string, io.Reader, int64) (domain.Artifact, error)
	Open(context.Context, domain.Artifact) (io.ReadCloser, error)
}
