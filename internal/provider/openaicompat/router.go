package openaicompat

import (
	"context"
	"fmt"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/ports"
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

var _ ports.ModelProvider = (*Router)(nil)
