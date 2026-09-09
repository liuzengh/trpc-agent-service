package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type CoordinationStore struct {
	pool  *pgxpool.Pool
	epoch storage.Epoch
}

func NewCoordinationStore(pool *pgxpool.Pool) (*CoordinationStore, error) {
	if pool == nil {
		return nil, fmt.Errorf("postgres: pool is required")
	}
	return &CoordinationStore{pool: pool, epoch: 1}, nil
}

func (s *CoordinationStore) GetEpoch(ctx context.Context, tenantID, resourceID string) (storage.Epoch, error) {
	if tenantID == "" || resourceID == "" {
		return 0, storage.ErrInvalidArgument
	}
	var epoch storage.Epoch
	err := WithTenantContext(ctx, s.pool, tenantID, "epoch read", func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT epoch FROM coordination_epoch WHERE tenant_id=$1 AND resource_id=$2`, tenantID, resourceID).Scan(&epoch)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	return epoch, nil
}

func (s *CoordinationStore) ValidateEpoch(ctx context.Context, tenantID, resourceID string, epoch storage.Epoch) error {
	current, err := s.GetEpoch(ctx, tenantID, resourceID)
	if err != nil {
		return err
	}
	if current != epoch {
		return storage.ErrEpochRejected
	}
	return nil
}

func (s *CoordinationStore) BumpEpoch(ctx context.Context, tenantID, resourceID string) (storage.Epoch, error) {
	if tenantID == "" || resourceID == "" {
		return 0, storage.ErrInvalidArgument
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if err = SetTenantContext(ctx, tx, tenantID); err != nil {
		return 0, err
	}
	if _, err = tx.Exec(ctx, `SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='15s'`); err != nil {
		return 0, err
	}
	if _, err = lockEpochTx(ctx, tx, tenantID, resourceID); err != nil {
		return 0, err
	}
	var next storage.Epoch
	if err = tx.QueryRow(ctx, `UPDATE coordination_epoch SET epoch=epoch+1, updated_at=now() WHERE tenant_id=$1 AND resource_id=$2 RETURNING epoch`, tenantID, resourceID).Scan(&next); err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return next, nil
}

func lockEpochTx(ctx context.Context, tx pgx.Tx, tenantID, resourceID string) (storage.Epoch, error) {
	if _, err := tx.Exec(ctx, `INSERT INTO coordination_epoch (tenant_id, resource_id) VALUES ($1,$2) ON CONFLICT (tenant_id, resource_id) DO NOTHING`, tenantID, resourceID); err != nil {
		return 0, err
	}
	var epoch storage.Epoch
	if err := tx.QueryRow(ctx, `SELECT epoch FROM coordination_epoch WHERE tenant_id=$1 AND resource_id=$2 FOR UPDATE`, tenantID, resourceID).Scan(&epoch); err != nil {
		return 0, err
	}
	return epoch, nil
}

func (s *CoordinationStore) Claim(ctx context.Context, tc tenant.TenantContext, key storage.DedupKey, ttl time.Duration, owner string) (storage.Claim, error) {
	if err := ctx.Err(); err != nil {
		return storage.Claim{}, err
	}
	if err := storage.ValidateDedupKey(key); err != nil || key.TenantID != tc.TenantID {
		if err != nil {
			return storage.Claim{}, err
		}
		return storage.Claim{}, storage.ErrTenantMismatch
	}
	if owner == "" || ttl <= 0 {
		return storage.Claim{}, storage.ErrInvalidArgument
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return storage.Claim{}, err
	}
	defer tx.Rollback(ctx)
	if err = SetTenantContext(ctx, tx, key.TenantID); err != nil {
		return storage.Claim{}, err
	}
	_, err = tx.Exec(ctx, `SET LOCAL lock_timeout = '5s'; SET LOCAL statement_timeout = '15s'`)
	if err != nil {
		return storage.Claim{}, err
	}
	authorityEpoch, err := lockEpochTx(ctx, tx, key.TenantID, storage.ClaimEpochResource(key))
	if err != nil {
		return storage.Claim{}, err
	}
	now := time.Now().UTC()
	expiry := now.Add(ttl)
	_, err = tx.Exec(ctx, `INSERT INTO message_dedup (tenant_id, channel, binding_id, external_message_id, status, owner_id, attempt, fence_token, epoch, claimed_at, expires_at) VALUES ($1,$2,$3,$4,'acquired',$5,1,1,$6,$7,$8) ON CONFLICT DO NOTHING`, key.TenantID, key.Channel, key.BindingID, key.ExternalMessageID, owner, authorityEpoch, now, expiry)
	if err != nil {
		return storage.Claim{}, err
	}
	var c storage.Claim
	var status string
	var responseRef *string
	var claimEpoch storage.Epoch
	err = tx.QueryRow(ctx, `SELECT status, owner_id, attempt, fence_token, epoch, response_ref, claimed_at, expires_at FROM message_dedup WHERE tenant_id=$1 AND channel=$2 AND binding_id=$3 AND external_message_id=$4 FOR UPDATE`, key.TenantID, key.Channel, key.BindingID, key.ExternalMessageID).Scan(&status, &c.OwnerID, &c.Attempt, &c.FenceToken, &claimEpoch, &responseRef, &c.ClaimedAt, &c.ExpiresAt)
	if responseRef != nil {
		c.ResponseRef = *responseRef
	}
	if err != nil {
		if err == pgx.ErrNoRows {
			return storage.Claim{}, storage.ErrNotFound
		}
		return storage.Claim{}, err
	}
	c.Key = key
	c.Backend = storage.BackendPostgres
	c.Epoch = claimEpoch
	c.Status = storage.ClaimStatus(status)
	if c.Status != storage.ClaimCompleted && (c.ExpiresAt.Before(now) || claimEpoch != authorityEpoch) && c.OwnerID != owner {
		c.Attempt++
		c.FenceToken++
		c.OwnerID = owner
		c.ClaimedAt = now
		c.ExpiresAt = expiry
		c.Status = storage.ClaimAcquired
		c.Epoch = authorityEpoch
		if _, err = tx.Exec(ctx, `UPDATE message_dedup SET status='acquired', owner_id=$5, attempt=$6, fence_token=$7, epoch=$8, claimed_at=$9, expires_at=$10, updated_at=now() WHERE tenant_id=$1 AND channel=$2 AND binding_id=$3 AND external_message_id=$4`, key.TenantID, key.Channel, key.BindingID, key.ExternalMessageID, owner, c.Attempt, c.FenceToken, authorityEpoch, now, expiry); err != nil {
			return storage.Claim{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return storage.Claim{}, err
	}
	return c, nil
}

func (s *CoordinationStore) Complete(ctx context.Context, tc tenant.TenantContext, key storage.DedupKey, owner, response string, g storage.OperationGuard) error {
	return s.write(ctx, tc, key, owner, g, response, "completed")
}
func (s *CoordinationStore) Fail(ctx context.Context, tc tenant.TenantContext, key storage.DedupKey, owner string, g storage.OperationGuard, retryable bool) error {
	status := "completed"
	if retryable {
		status = "acquired"
	}
	return s.write(ctx, tc, key, owner, g, "", status)
}
func (s *CoordinationStore) write(ctx context.Context, tc tenant.TenantContext, key storage.DedupKey, owner string, g storage.OperationGuard, response, status string) error {
	if g.Backend != storage.BackendPostgres || g.OwnerID != owner {
		return storage.ErrFenceRejected
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = SetTenantContext(ctx, tx, key.TenantID); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='15s'`); e != nil {
		return e
	}
	current, e := lockEpochTx(ctx, tx, key.TenantID, storage.ClaimEpochResource(key))
	if e != nil {
		return e
	}
	if g.Epoch != current {
		return storage.ErrEpochRejected
	}
	r, e := tx.Exec(ctx, `UPDATE message_dedup SET status=$5,response_ref=NULLIF($6,''),updated_at=now() WHERE tenant_id=$1 AND channel=$2 AND binding_id=$3 AND external_message_id=$4 AND owner_id=$7 AND fence_token=$8 AND epoch=$9 AND expires_at>now()`, key.TenantID, key.Channel, key.BindingID, key.ExternalMessageID, status, response, owner, g.FenceToken, current)
	if e != nil {
		return e
	}
	if r.RowsAffected() != 1 {
		return storage.ErrFenceRejected
	}
	return tx.Commit(ctx)
}

var _ storage.ClaimStore = (*CoordinationStore)(nil)
