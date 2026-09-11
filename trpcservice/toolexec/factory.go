package toolexec

import (
	"database/sql"
	"fmt"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"

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

func NewOperationsForControlPlane(repository controlplane.Repository, journal Journal, writer audit.Writer) (*Operations, error) {
	store, provider, err := NewOperationBackend(repository)
	if err != nil {
		return nil, err
	}
	return NewOperations(store, journal, writer, provider)
}
