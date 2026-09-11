package postgresadapter

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

const rollbackTimeout = 5 * time.Second

// rollbackTransaction can still clean up after the operation context is
// cancelled, but a non-responsive database cannot extend workload shutdown
// indefinitely. It also bounds savepoint rollback before durable quarantine.
func rollbackTransaction(tx pgx.Tx) error {
	cleanup, cancel := context.WithTimeout(context.Background(), rollbackTimeout)
	defer cancel()
	return tx.Rollback(cleanup)
}
