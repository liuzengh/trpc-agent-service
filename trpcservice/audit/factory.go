package audit

import (
	"database/sql"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

type sqlProvider interface{ SQLDB() *sql.DB }

func NewForControlPlane(repository controlplane.Repository) (Writer, error) {
	if repository == nil {
		return nil, fmt.Errorf("audit control-plane repository is required")
	}
	if provider, ok := repository.(sqlProvider); ok {
		return NewPostgresWriter(provider.SQLDB())
	}
	return NewMemoryWriter(), nil
}
