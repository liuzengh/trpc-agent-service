package postgres

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"time"
)

func (s *CoordinationStore) Acquire(ctx context.Context, tc tenant.TenantContext, resource, owner string, ttl time.Duration) (storage.Lease, error) {
	if err := ctx.Err(); err != nil {
		return storage.Lease{}, err
	}
	if tc.TenantID == "" || resource == "" || owner == "" || ttl <= 0 {
		return storage.Lease{}, storage.ErrInvalidArgument
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return storage.Lease{}, e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, `SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='15s'`); e != nil {
		return storage.Lease{}, e
	}
	authorityEpoch, e := lockEpochTx(ctx, tx, tc.TenantID, resource)
	if e != nil {
		return storage.Lease{}, e
	}
	var exists bool
	e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM session WHERE tenant_id=$1 AND session_id=$2)`, tc.TenantID, resource).Scan(&exists)
	if e != nil {
		return storage.Lease{}, e
	}
	if !exists {
		return storage.Lease{}, storage.ErrNotFound
	}
	now := time.Now().UTC()
	exp := now.Add(ttl)
	_, e = tx.Exec(ctx, `INSERT INTO session_lease(tenant_id,session_id,owner_id,epoch,fencing_token,leased_until) VALUES($1,$2,$3,$4,1,$5) ON CONFLICT (tenant_id,session_id) DO NOTHING`, tc.TenantID, resource, owner, authorityEpoch, exp)
	if e != nil {
		return storage.Lease{}, e
	}
	var l storage.Lease
	var leaseEpoch storage.Epoch
	e = tx.QueryRow(ctx, `SELECT owner_id,epoch,fencing_token,leased_until FROM session_lease WHERE tenant_id=$1 AND session_id=$2 FOR UPDATE`, tc.TenantID, resource).Scan(&l.OwnerID, &leaseEpoch, &l.FenceToken, &l.ExpiresAt)
	if e != nil {
		return storage.Lease{}, e
	}
	if (l.ExpiresAt.Before(now) || leaseEpoch != authorityEpoch) && l.OwnerID != owner {
		l.OwnerID = owner
		l.FenceToken++
		l.ExpiresAt = exp
		leaseEpoch = authorityEpoch
		_, e = tx.Exec(ctx, `UPDATE session_lease SET owner_id=$3,epoch=$4,fencing_token=$5,leased_until=$6,updated_at=now() WHERE tenant_id=$1 AND session_id=$2`, tc.TenantID, resource, owner, leaseEpoch, l.FenceToken, exp)
		if e != nil {
			return storage.Lease{}, e
		}
	} else if l.ExpiresAt.After(now) && leaseEpoch == authorityEpoch && l.OwnerID != owner {
		return storage.Lease{}, storage.ErrLeaseLost
	}
	l.TenantID = tc.TenantID
	l.SessionID = resource
	l.ResourceID = resource
	l.Backend = storage.BackendPostgres
	l.Epoch = authorityEpoch
	if e = tx.Commit(ctx); e != nil {
		return storage.Lease{}, e
	}
	return l, nil
}
func (s *CoordinationStore) Renew(ctx context.Context, tc tenant.TenantContext, l storage.Lease, ttl time.Duration) (storage.Lease, error) {
	if l.Backend != storage.BackendPostgres {
		return l, storage.ErrEpochRejected
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return l, e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, `SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='15s'`); e != nil {
		return l, e
	}
	current, e := lockEpochTx(ctx, tx, tc.TenantID, l.SessionID)
	if e != nil {
		return l, e
	}
	if l.Epoch != current {
		return l, storage.ErrEpochRejected
	}
	exp := time.Now().UTC().Add(ttl)
	r, e := tx.Exec(ctx, `UPDATE session_lease SET leased_until=$6,updated_at=now() WHERE tenant_id=$1 AND session_id=$2 AND owner_id=$3 AND fencing_token=$4 AND epoch=$5 AND leased_until>now()`, tc.TenantID, l.SessionID, l.OwnerID, l.FenceToken, current, exp)
	if e != nil {
		return l, e
	}
	if r.RowsAffected() != 1 {
		return l, storage.ErrLeaseLost
	}
	if e = tx.Commit(ctx); e != nil {
		return l, e
	}
	l.ExpiresAt = exp
	return l, nil
}
func (s *CoordinationStore) Release(ctx context.Context, tc tenant.TenantContext, l storage.Lease) error {
	if l.Backend != storage.BackendPostgres {
		return storage.ErrEpochRejected
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, `SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='15s'`); e != nil {
		return e
	}
	current, e := lockEpochTx(ctx, tx, tc.TenantID, l.SessionID)
	if e != nil {
		return e
	}
	if l.Epoch != current {
		return storage.ErrEpochRejected
	}
	r, e := tx.Exec(ctx, `DELETE FROM session_lease WHERE tenant_id=$1 AND session_id=$2 AND owner_id=$3 AND fencing_token=$4 AND epoch=$5`, tc.TenantID, l.SessionID, l.OwnerID, l.FenceToken, current)
	if e != nil {
		return e
	}
	if r.RowsAffected() != 1 {
		return storage.ErrFenceRejected
	}
	return tx.Commit(ctx)
}
func (s *CoordinationStore) Validate(ctx context.Context, tc tenant.TenantContext, l storage.Lease) error {
	if l.Backend != storage.BackendPostgres {
		return storage.ErrEpochRejected
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, `SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='15s'`); e != nil {
		return e
	}
	current, e := lockEpochTx(ctx, tx, tc.TenantID, l.SessionID)
	if e != nil {
		return e
	}
	if l.Epoch != current {
		return storage.ErrEpochRejected
	}
	var exp time.Time
	e = tx.QueryRow(ctx, `SELECT leased_until FROM session_lease WHERE tenant_id=$1 AND session_id=$2 AND owner_id=$3 AND fencing_token=$4 AND epoch=$5`, tc.TenantID, l.SessionID, l.OwnerID, l.FenceToken, current).Scan(&exp)
	if e == pgx.ErrNoRows {
		return storage.ErrLeaseLost
	}
	if e != nil {
		return e
	}
	if !exp.After(time.Now().UTC()) {
		return storage.ErrLeaseLost
	}
	return tx.Commit(ctx)
}

var _ storage.LeaseStore = (*CoordinationStore)(nil)
