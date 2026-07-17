package ports

import (
	"context"

	"github.com/muratmirgun/yordam/internal/domain"
)

type ModelProvider interface {
	Stream(context.Context, domain.ModelRequest) (<-chan domain.ModelEvent, error)
}
