package openaicompat

import (
	"context"
	"fmt"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
	"github.com/muratmirgun/yordam/internal/protocol"
	"github.com/muratmirgun/yordam/internal/provider"
)

type Router struct {
	clients map[string]*Client
}

func NewRouter(clients map[string]*Client) (*Router, error) {
	if len(clients) == 0 {
		return nil, fmt.Errorf("provider router requires at least one profile")
	}
	copied := make(map[string]*Client, len(clients))
	for name, client := range clients {
		if name == "" || client == nil {
			return nil, fmt.Errorf("invalid provider profile %q", name)
		}
		copied[name] = client
	}
	return &Router{clients: copied}, nil
}

func (r *Router) Stream(ctx context.Context, input domain.ModelRequest) (<-chan domain.ModelEvent, error) {
	client, ok := r.clients[input.Selection.Profile]
	if !ok {
		return nil, fmt.Errorf("unknown provider profile %q", input.Selection.Profile)
	}
	return client.Stream(ctx, input)
}

func (*Router) Kind() string { return AdapterKind }

func (r *Router) Normalize(ctx context.Context, request protocol.ModelRequest) (provider.PreparedRequest, error) {
	if _, ok := r.clients[string(request.ProviderID)]; !ok {
		return provider.PreparedRequest{}, fmt.Errorf("unknown provider profile %q", request.ProviderID)
	}
	return NewAdapter(nil).Normalize(ctx, request)
}

func (r *Router) StartPrepared(ctx context.Context, prepared provider.PreparedRequest) (<-chan protocol.ModelEvent, error) {
	var normalized normalizedRequest
	if err := prepared.Decode(r.Kind(), &normalized); err != nil {
		return nil, err
	}
	client, ok := r.clients[string(normalized.ProviderID)]
	if !ok {
		return nil, fmt.Errorf("unknown provider profile %q", normalized.ProviderID)
	}
	return NewAdapter(client).StartPrepared(ctx, prepared)
}

var _ ports.ModelProvider = (*Router)(nil)
var _ provider.Adapter = (*Router)(nil)
