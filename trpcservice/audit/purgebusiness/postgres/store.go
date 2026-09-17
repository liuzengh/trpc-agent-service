// Package postgres implements the business audit retention store over the
// durable schema, delegating the watermark gate and guarded deletion to the
// SECURITY DEFINER SQL functions.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit/purgebusiness"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Store struct{ db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db: db} }

func (s *Store) Plan(ctx context.Context, in purgebusiness.PlanInput) (string, error) {
	if s == nil || s.db == nil {
		return "", runtime.ErrBackendUnavailable
	}
	if in.TenantID == "" || in.CutoffAt.IsZero() || in.Actor == "" || in.Reason == "" || in.Now.IsZero() {
		return "", runtime.ErrInvariantViolation
	}
	var batchID string
	err := s.db.QueryRowContext(ctx, `SELECT public.plan_business_audit_purge($1,$2,$3,$4,$5)`,
		in.TenantID, in.CutoffAt.UTC().Truncate(time.Microsecond), in.Actor, in.Reason,
		in.Now.UTC().Truncate(time.Microsecond)).Scan(&batchID)
	return batchID, mapError(err)
}

func (s *Store) Execute(ctx context.Context, tenantID, batchID, owner string, maxBatchSize int64) (string, error) {
	if s == nil || s.db == nil {
		return "", runtime.ErrBackendUnavailable
	}
	if tenantID == "" || batchID == "" || owner == "" || maxBatchSize < 1 || maxBatchSize > 1_000_000 {
		return "", runtime.ErrInvariantViolation
	}
	var result string
	err := s.db.QueryRowContext(ctx, `SELECT public.execute_business_audit_purge($1,$2,$3,$4)`,
		tenantID, batchID, owner, maxBatchSize).Scan(&result)
	return result, mapError(err)
}

func (s *Store) Quarantine(ctx context.Context, tenantID, batchID, owner string) error {
	if s == nil || s.db == nil {
		return runtime.ErrBackendUnavailable
	}
	if tenantID == "" || batchID == "" || owner == "" {
		return runtime.ErrInvariantViolation
	}
	_, err := s.db.ExecContext(ctx, `SELECT public.quarantine_business_audit_purge($1,$2,$3)`, tenantID, batchID, owner)
	return mapError(err)
}

func (s *Store) Get(ctx context.Context, tenantID, batchID string) (purgebusiness.Batch, error) {
	if s == nil || s.db == nil {
		return purgebusiness.Batch{}, runtime.ErrBackendUnavailable
	}
	if tenantID == "" || batchID == "" {
		return purgebusiness.Batch{}, runtime.ErrInvariantViolation
	}
	var b purgebusiness.Batch
	var watermark, claim sql.NullTime
	err := s.db.QueryRowContext(ctx, `SELECT tenant_id,batch_id,state,cutoff_at,watermark_at,safe_cutoff_at,
planned_events,planned_outbox,planned_digest,deleted_events,deleted_outbox,delete_attempt,
COALESCE(last_error_class,''),COALESCE(claim_owner,''),claim_until,not_before,created_at,version
FROM public.business_audit_purge_batch WHERE tenant_id=$1 AND batch_id=$2`, tenantID, batchID).Scan(
		&b.TenantID, &b.BatchID, &b.State, &b.CutoffAt, &watermark, &b.SafeCutoffAt,
		&b.PlannedEvents, &b.PlannedOutbox, &b.PlannedDigest, &b.DeletedEvents, &b.DeletedOutbox,
		&b.DeleteAttempt, &b.LastError, &b.ClaimOwner, &claim, &b.NotBefore, &b.CreatedAt, &b.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return purgebusiness.Batch{}, runtime.ErrNotFound
	}
	if err != nil {
		return purgebusiness.Batch{}, mapError(err)
	}
	if watermark.Valid {
		b.WatermarkAt = watermark.Time.UTC()
	}
	if claim.Valid {
		b.ClaimUntil = claim.Time.UTC()
	}
	b.CutoffAt = b.CutoffAt.UTC()
	b.SafeCutoffAt = b.SafeCutoffAt.UTC()
	b.NotBefore = b.NotBefore.UTC()
	b.CreatedAt = b.CreatedAt.UTC()
	return b, nil
}

func (s *Store) ActiveBatches(ctx context.Context) ([]purgebusiness.Batch, error) {
	if s == nil || s.db == nil {
		return nil, runtime.ErrBackendUnavailable
	}
	rows, err := s.db.QueryContext(ctx, `SELECT tenant_id,batch_id FROM public.business_audit_purge_batch
WHERE state IN ('planned','executing','failed') ORDER BY created_at`)
	if err != nil {
		return nil, mapError(err)
	}
	defer rows.Close()
	var batches []purgebusiness.Batch
	for rows.Next() {
		var tenantID, batchID string
		if err := rows.Scan(&tenantID, &batchID); err != nil {
			return nil, err
		}
		batch, err := s.Get(ctx, tenantID, batchID)
		if err != nil {
			return nil, err
		}
		batches = append(batches, batch)
	}
	return batches, rows.Err()
}

func (s *Store) DueTenants(ctx context.Context, cutoff time.Time) ([]string, error) {
	if s == nil || s.db == nil {
		return nil, runtime.ErrBackendUnavailable
	}
	if cutoff.IsZero() {
		return nil, runtime.ErrInvariantViolation
	}
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT tenant_id FROM (
  SELECT tenant_id FROM public.audit_event WHERE occurred_at < $1
  UNION
  SELECT tenant_id FROM public.outbox WHERE kind='audit' AND state='published' AND created_at < $1
) t ORDER BY 1`, cutoff.UTC())
	if err != nil {
		return nil, mapError(err)
	}
	defer rows.Close()
	var tenants []string
	for rows.Next() {
		var tenant string
		if err := rows.Scan(&tenant); err != nil {
			return nil, err
		}
		tenants = append(tenants, tenant)
	}
	return tenants, rows.Err()
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42501":
			return purgebusiness.ErrNotAuthorized
		case "23505":
			return runtime.ErrIdempotencyCollision
		case "40001", "23514":
			return runtime.ErrVersionConflict
		case "22023", "23503", "55000", "23000":
			return runtime.ErrInvariantViolation
		case "P0002", "02000":
			return runtime.ErrNotFound
		}
	}
	return err
}

var _ purgebusiness.Store = (*Store)(nil)
