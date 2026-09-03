package background

import (
	"database/sql"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

func NewForControlPlane(repository controlplane.Repository) (Repository, error) {
	if repository == nil {
		return nil, fmt.Errorf("background control-plane repository is required")
	}
	if provider, ok := repository.(interface{ SQLDB() *sql.DB }); ok {
		return NewPostgresRepository(provider.SQLDB())
	}
	return NewMemoryRepository(), nil
}
