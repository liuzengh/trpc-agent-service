package postgresadapter

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
)

// TransactionAuthorizer rechecks a trusted Session context under row locks in
// the caller's transaction. No session token or external header is accepted here.
type TransactionAuthorizer struct{}

func (TransactionAuthorizer) AuthorizeSession(ctx context.Context, tx pgx.Tx, user string) (bool, error) {
	identity, ok := application.IdentityFromContext(ctx)
	if !ok || identity.UserID != user || identity.SessionID == "" || identity.Restricted {
		return false, nil
	}
	var status string
	err := tx.QueryRow(ctx, `SELECT status FROM user_accounts WHERE id=$1 FOR SHARE`, user).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if status != "ACTIVE" {
		return false, nil
	}
	var active bool
	err = tx.QueryRow(ctx, `SELECT NOT restricted AND revoked_at IS NULL AND expires_at>clock_timestamp() FROM user_sessions WHERE id=$1 AND user_id=$2 FOR SHARE`, identity.SessionID, user).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return active, nil
}
