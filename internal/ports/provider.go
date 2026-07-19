package ports

import (
	"context"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/provider"
)

type ModelProvider interface {
	Stream(context.Context, domain.ModelRequest) (<-chan domain.ModelEvent, error)
}

// ProviderPreparer is the Gate 1 provider-neutral boundary. Its result is
// intentionally opaque; dispatch authority is added at the committed-token
// boundary rather than exposing an adapter or transport here.
type ProviderPreparer interface {
	Prepare(context.Context, protocol.ActivityID, string, protocol.ModelRequest, protocol.Digest) (provider.ProviderHandle, error)
}
