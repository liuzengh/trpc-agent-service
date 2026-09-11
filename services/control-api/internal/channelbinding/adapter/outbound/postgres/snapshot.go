package postgresadapter

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

func (s *Store) ReadSnapshot(ctx context.Context, scope string) (domain.Snapshot, error) {
	if scope != s.options.ScopeID {
		return domain.Snapshot{}, application.ErrWorkloadDenied
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return domain.Snapshot{}, dbError(err)
	}
	defer rollback(tx)
	value, err := s.snapshot(ctx, tx)
	if err != nil {
		return value, err
	}
	if err = tx.Commit(ctx); err != nil {
		return value, dbError(err)
	}
	return value, nil
}
func (s *Store) snapshot(ctx context.Context, tx pgx.Tx) (domain.Snapshot, error) {
	c, err := s.catalog(ctx, tx, "")
	if err != nil {
		return domain.Snapshot{}, err
	}
	rows, err := tx.Query(ctx, `SELECT `+accountColumns+` FROM channel_accounts WHERE scope_id=$1 ORDER BY provider,id LIMIT 1001`, s.options.ScopeID)
	if err != nil {
		return domain.Snapshot{}, dbError(err)
	}
	accounts := make([]domain.Account, 0, c.count)
	for rows.Next() {
		a, err := scanAccount(rows)
		if err != nil {
			rows.Close()
			return domain.Snapshot{}, err
		}
		accounts = append(accounts, a)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return domain.Snapshot{}, dbError(err)
	}
	if len(accounts) != c.count || len(accounts) > domain.MaxAccounts {
		return domain.Snapshot{}, integrity()
	}
	credentials := map[string][]domain.CredentialMeta{}
	rows, err = tx.Query(ctx, `SELECT c.account_id,c.purpose,c.id,c.credential_version,c.configured FROM channel_account_credentials c JOIN channel_accounts a ON a.tenant_id=c.tenant_id AND a.id=c.account_id WHERE a.scope_id=$1 ORDER BY c.account_id,c.purpose`, s.options.ScopeID)
	if err != nil {
		return domain.Snapshot{}, dbError(err)
	}
	for rows.Next() {
		var id string
		var meta domain.CredentialMeta
		if err = rows.Scan(&id, &meta.Purpose, &meta.ID, &meta.Version, &meta.Configured); err != nil {
			rows.Close()
			return domain.Snapshot{}, dbError(err)
		}
		credentials[id] = append(credentials[id], meta)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return domain.Snapshot{}, dbError(err)
	}
	entries := make([]domain.SnapshotAccount, 0, len(accounts))
	for _, a := range accounts {
		entry, err := domain.ProjectSnapshotAccount(a, credentials[a.ID])
		if err != nil {
			return domain.Snapshot{}, integrity()
		}
		entries = append(entries, entry)
	}
	return domain.NewSnapshot(s.options.ScopeID, c.epoch, c.revision, entries)
}
