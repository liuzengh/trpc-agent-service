package toolexec

import (
	"database/sql"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

func NewForControlPlane(repository controlplane.Repository) (Journal, error) {
	if repository == nil {
		return nil, fmt.Errorf("tool execution control plane is required")
	}
	if provider, ok := repository.(interface{ SQLDB() *sql.DB }); ok {
		return NewPostgresJournal(provider.SQLDB())
	}
	return NewMemoryJournal(), nil
}
