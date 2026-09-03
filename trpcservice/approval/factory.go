package approval

import (
	"database/sql"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

type sqlProvider interface{ SQLDB() *sql.DB }

func NewForControlPlane(repository controlplane.Repository) (Repository, error) {
	if repository == nil {
		return nil, fmt.Errorf("approval control-plane repository is required")
	}
	if provider, ok := repository.(sqlProvider); ok {
		return NewPostgresRepository(provider.SQLDB())
	}
	return NewMemoryRepository(), nil
}
