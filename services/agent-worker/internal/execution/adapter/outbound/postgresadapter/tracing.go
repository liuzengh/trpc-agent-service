package postgresadapter

import (
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// A server rejection/rollback is definite; losing the commit response is not.
// This classification annotates observations only, never retries a transaction.
func completionOutcome(err error, commitAttempted bool) string {
	if err == nil {
		return "accepted"
	}
	var pgError *pgconn.PgError
	if !commitAttempted || errors.As(err, &pgError) || errors.Is(err, pgx.ErrTxCommitRollback) {
		return "failed"
	}
	return "UNKNOWN"
}
