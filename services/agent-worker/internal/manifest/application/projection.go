package application

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/domain"
)

type Projection interface {
	Apply(context.Context, domain.Publication, int) error
	Read(context.Context, string, string) (domain.Publication, error)
}
