// Package postgresadapter persists only Channel-owned business records. Tenant
// and Identity authorization remain owner adapters injected at the transaction seam.
package postgresadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

type DB interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}
type TenantAuthorizer interface {
	AuthorizeOwner(context.Context, pgx.Tx, string, string) (bool, error)
	AuthorizeActiveTenant(context.Context, pgx.Tx, string) (bool, error)
}
type Options struct {
	ScopeID, SourceEpoch string
	MaxTenantAccounts    int
}
type Store struct {
	db      DB
	auth    TenantAuthorizer
	options Options
}

func NewStore(db DB, auth TenantAuthorizer, options Options) (*Store, error) {
	if db == nil || auth == nil || !domain.ValidID(options.ScopeID) || !domain.ValidEpoch(options.SourceEpoch) {
		return nil, application.ErrDependencyUnavailable
	}
	if options.MaxTenantAccounts == 0 {
		options.MaxTenantAccounts = domain.MaxAccounts
	}
	if options.MaxTenantAccounts < 1 || options.MaxTenantAccounts > domain.MaxAccounts {
		return nil, application.ErrDependencyUnavailable
	}
	return &Store{db, auth, options}, nil
}

// EnsureCatalog is bootstrap-only. A configured epoch never silently replaces
// an existing persisted epoch, including after restart or database restoration.
func (s *Store) EnsureCatalog(ctx context.Context) error {
	_, err := s.db.Exec(ctx, `INSERT INTO channel_account_catalog(scope_id,source_epoch) VALUES($1,$2) ON CONFLICT(scope_id) DO NOTHING`, s.options.ScopeID, s.options.SourceEpoch)
	if err != nil {
		return dbError(err)
	}
	var epoch string
	err = s.db.QueryRow(ctx, `SELECT source_epoch::text FROM channel_account_catalog WHERE scope_id=$1`, s.options.ScopeID).Scan(&epoch)
	if err != nil {
		return dbError(err)
	}
	if epoch != s.options.SourceEpoch {
		return application.ErrEpochMismatch
	}
	return nil
}

type catalog struct {
	epoch    string
	revision int64
	count    int
}

func (s *Store) catalog(ctx context.Context, tx pgx.Tx, lock string) (catalog, error) {
	var c catalog
	err := tx.QueryRow(ctx, `SELECT source_epoch::text,snapshot_revision,account_count FROM channel_account_catalog WHERE scope_id=$1 `+lock, s.options.ScopeID).Scan(&c.epoch, &c.revision, &c.count)
	if err != nil {
		return c, dbError(err)
	}
	if c.epoch != s.options.SourceEpoch {
		return c, application.ErrEpochMismatch
	}
	if !domain.ValidVersion(c.revision) || c.count < 0 || c.count > domain.MaxAccounts {
		return c, integrity()
	}
	return c, nil
}
func (s *Store) WithWrite(ctx context.Context, scope application.WriteScope, fn func(application.Transaction) error) error {
	if scope.ScopeID != s.options.ScopeID || !domain.ValidID(scope.Actor.TenantID) || !domain.ValidID(scope.Actor.UserID) || fn == nil {
		return application.ErrPermissionDenied
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return dbError(err)
	}
	defer rollback(tx)
	c, err := s.catalog(ctx, tx, "FOR UPDATE")
	if err != nil {
		return err
	}
	allowed, err := s.auth.AuthorizeOwner(ctx, tx, scope.Actor.TenantID, scope.Actor.UserID)
	if err != nil {
		return dbError(err)
	}
	if !allowed {
		return application.ErrPermissionDenied
	}
	t := &writeTx{store: s, tx: tx, scope: scope, catalog: c, loaded: map[string]application.Aggregate{}}
	if err = fn(t); err != nil {
		return err
	}
	if t.snapshotChanged {
		if c.revision == domain.MaxVersion {
			return &domain.Error{Code: domain.VersionExhausted}
		}
		_, err = tx.Exec(ctx, `UPDATE channel_account_catalog SET snapshot_revision=snapshot_revision+1,account_count=account_count+$2 WHERE scope_id=$1`, s.options.ScopeID, t.createdCount)
		if err != nil {
			return dbError(err)
		}
		// Size and identity checks are part of the writer transaction. A too-large
		// complete snapshot rolls back all rows, the Outbox and the success receipt.
		if _, err = s.snapshot(ctx, tx); err != nil {
			return err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return dbError(err)
	}
	return nil
}

type writeTx struct {
	store           *Store
	tx              pgx.Tx
	scope           application.WriteScope
	catalog         catalog
	loaded          map[string]application.Aggregate
	saved           bool
	snapshotChanged bool
	createdCount    int
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
func integrity() error { return &domain.Error{Code: domain.SourceIntegrity} }
func dbError(err error) error {
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) {
		switch pgerr.ConstraintName {
		case "channel_accounts_provider_identity", "channel_accounts_telegram_identity":
			return application.ErrIdentityConflict
		case "channel_bindings_one_account":
			return application.ErrAccountAlreadyBound
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("channel persistence: %w", application.ErrDependencyUnavailable)
}

var _ application.CommandStore = (*Store)(nil)
var _ application.QueryStore = (*Store)(nil)
