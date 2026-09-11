package postgresadapter

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// AuthorizeActiveUser checks the durable user status for a previously authorized
// background operation. It neither accepts nor synthesizes a Session and does
// not change AuthorizeSession's authentication requirements.
func (TransactionAuthorizer) AuthorizeActiveUser(ctx context.Context, tx pgx.Tx, user string) (bool, error) {
	var status string
	err := tx.QueryRow(ctx, `SELECT status FROM user_accounts WHERE id=$1 FOR SHARE`, user).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return status == "ACTIVE", nil
}
