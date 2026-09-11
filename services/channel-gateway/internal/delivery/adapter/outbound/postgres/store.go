// Package postgresadapter owns the Delivery ledger and side-effect boundaries.
package postgresadapter

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"time"
)

// ConnectionGuard is a technical, caller-owned transaction seam. It must hold
// Connection's SHARE lock until this transaction commits; no Connection SQL is copied.
type ConnectionGuard interface {
	VerifyOwner(context.Context, pgx.Tx, string, string, int64, int64) error
}
type Options struct{ MaxIntents, MaxParts, MaxAttempts int }
type AccountUseGuard interface {
	AccountContext(context.Context) (context.Context, context.CancelFunc, error)
	VerifyAccount(context.Context, pgx.Tx, string, string, string, *int64) (string, error)
	RecheckAccount(context.Context, pgx.Tx) error
}
type Store struct {
	accountGuard AccountUseGuard
	pool         *pgxpool.Pool
	guard        ConnectionGuard
	options      Options
}

func NewStore(pool *pgxpool.Pool, guard ConnectionGuard, o Options) (*Store, error) {
	if o.MaxIntents == 0 {
		o.MaxIntents = 10000
	}
	if o.MaxParts == 0 {
		o.MaxParts = 100000
	}
	if o.MaxAttempts == 0 {
		o.MaxAttempts = 3
	}
	if pool == nil || o.MaxIntents < 1 || o.MaxIntents > 1000000 || o.MaxParts < 1 || o.MaxParts > 10000000 || o.MaxAttempts < 1 || o.MaxAttempts > 3 {
		return nil, domain.ErrInvalid
	}
	return &Store{pool: pool, guard: guard, options: o}, nil
}
func (s *Store) WithAccountUseGuard(g AccountUseGuard) *Store {
	cp := *s
	cp.accountGuard = g
	return &cp
}
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
func databaseError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return errors.Join(domain.ErrUnavailable, ctx.Err())
	}
	return domain.ErrUnavailable
}
