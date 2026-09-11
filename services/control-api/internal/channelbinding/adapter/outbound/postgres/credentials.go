package postgresadapter

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

// WithCredentials authorizes and decrypts inside one locked transaction. It
// reads no Binding/Deployment/Profile table and never substitutes newer refs.
func (s *Store) WithCredentials(ctx context.Context, scope, tenant, account string, fn func(domain.Account, []domain.CredentialRecord, string) error) error {
	if scope != s.options.ScopeID || fn == nil {
		return application.ErrWorkloadDenied
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return dbError(err)
	}
	defer rollback(tx)
	c, err := s.catalog(ctx, tx, "FOR SHARE")
	if err != nil {
		return err
	}
	allowed, err := s.auth.AuthorizeActiveTenant(ctx, tx, tenant)
	if err != nil {
		return dbError(err)
	}
	if !allowed {
		return application.ErrAccountNotFound
	}
	a, err := scanAccount(tx.QueryRow(ctx, `SELECT `+accountColumns+` FROM channel_accounts WHERE tenant_id=$1 AND id=$2 AND scope_id=$3 FOR SHARE`, tenant, account, scope))
	if err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT tenant_id,account_id,provider,purpose,id,credential_version,configured,COALESCE(key_id,''),ciphertext FROM channel_account_credentials WHERE tenant_id=$1 AND account_id=$2 ORDER BY purpose`, tenant, account)
	if err != nil {
		return dbError(err)
	}
	records := make([]domain.CredentialRecord, 0, 2)
	for rows.Next() {
		var v domain.CredentialRecord
		if err = rows.Scan(&v.TenantID, &v.AccountID, &v.Provider, &v.Meta.Purpose, &v.Meta.ID, &v.Meta.Version, &v.Meta.Configured, &v.KeyID, &v.Ciphertext); err != nil {
			rows.Close()
			return dbError(err)
		}
		records = append(records, v)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return dbError(err)
	}
	if err = fn(a, records, c.epoch); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return dbError(err)
	}
	return nil
}
