package postgresadapter

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// TransactionAuthorizer is the Tenant owner's adapter-level authorization port.
// It joins no Channel state. The caller supplies the existing transaction so the
// membership/tenant row locks remain held until the owning business commit ends.
type TransactionAuthorizer struct{}

func (TransactionAuthorizer) AuthorizeOwner(ctx context.Context, tx pgx.Tx, tenant, user string) (bool, error) {
	if ok, err := (TransactionAuthorizer{}).AuthorizeActiveTenant(ctx, tx, tenant); err != nil || !ok {
		return ok, err
	}
	var role string
	err := tx.QueryRow(ctx, `SELECT role FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2 FOR SHARE`, tenant, user).Scan(&role)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return role == "OWNER", nil
}
func (TransactionAuthorizer) AuthorizeActiveTenant(ctx context.Context, tx pgx.Tx, tenant string) (bool, error) {
	var status string
	err := tx.QueryRow(ctx, `SELECT status FROM tenants WHERE id=$1 FOR SHARE`, tenant).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return status == "ACTIVE", nil
}
