package gateway

import (
	"database/sql"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

type sqlDBProvider interface {
	SQLDB() *sql.DB
}

// NewJournalForControlPlane uses the same PostgreSQL pool when available and
// otherwise creates an in-memory journal.
func NewJournalForControlPlane(repository controlplane.Repository) (Journal, error) {
	if repository == nil {
		return nil, fmt.Errorf("control-plane repository is required")
	}
	if provider, ok := repository.(sqlDBProvider); ok {
		return NewPostgresJournal(provider.SQLDB())
	}
	return NewMemoryJournal(), nil
}
