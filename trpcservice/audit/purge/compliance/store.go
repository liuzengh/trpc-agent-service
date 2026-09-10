package compliance

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit/purge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// Store implements purge.Store over the compliance database, delegating the
// guarded state machine and deletion to the SECURITY DEFINER SQL functions.
type Store struct{ DB *sql.DB }

func New(db *sql.DB) *Store { return &Store{DB: db} }

func (s *Store) EffectiveRetention(ctx context.Context, tenantID, class string) (time.Duration, error) {
	if s == nil || s.DB == nil {
		return 0, runtime.ErrInvariantViolation
	}
	var seconds int64
	if err := s.DB.QueryRowContext(ctx, `SELECT compliance.audit_effective_retention($1,$2)`, tenantID, class).Scan(&seconds); err != nil {
		return 0, err
	}
	return time.Duration(seconds) * time.Second, nil
}

func (s *Store) DueCandidates(ctx context.Context, now time.Time) ([]purge.Candidate, error) {
	if s == nil || s.DB == nil {
		return nil, runtime.ErrInvariantViolation
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT DISTINCT tenant_id, compliance.audit_retention_class(event_json->>'action')
FROM compliance.audit_event e
WHERE e.occurred_at < $1 - make_interval(secs => compliance.audit_effective_retention(
  e.tenant_id, compliance.audit_retention_class(e.event_json->>'action')))
  AND NOT compliance.audit_event_on_hold(e.tenant_id, e.occurred_at)
ORDER BY 1, 2`, now.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var candidates []purge.Candidate
	for rows.Next() {
		var candidate purge.Candidate
		if err := rows.Scan(&candidate.TenantID, &candidate.Class); err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	return candidates, rows.Err()
}

func (s *Store) ActiveBatches(ctx context.Context) ([]purge.Batch, error) {
	if s == nil || s.DB == nil {
		return nil, runtime.ErrInvariantViolation
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT tenant_id,batch_id,state,cutoff_at,class,planned_count,planned_digest,
deleted_count,alert_count,delete_attempt,COALESCE(last_error_class,''),COALESCE(policy_version,0),
COALESCE(floor_version,0),COALESCE(claim_owner,''),COALESCE(claim_until,'1970-01-01'::timestamptz),
COALESCE(ttl_until,'1970-01-01'::timestamptz),COALESCE(not_before,'1970-01-01'::timestamptz)
FROM compliance.audit_purge_batch WHERE state IN ('planned','approved','executing','failed')
ORDER BY created_at,tenant_id,batch_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var batches []purge.Batch
	for rows.Next() {
		var b purge.Batch
		if err := rows.Scan(&b.TenantID, &b.BatchID, &b.State, &b.CutoffAt, &b.Class,
			&b.PlannedCount, &b.PlannedDigest, &b.DeletedCount, &b.AlertCount, &b.DeleteAttempt,
			&b.LastError, &b.PolicyVersion, &b.FloorVersion, &b.ClaimOwner, &b.ClaimUntil,
			&b.TTLUntil, &b.NotBefore); err != nil {
			return nil, err
		}
		batches = append(batches, b)
	}
	return batches, rows.Err()
}

func (s *Store) Plan(ctx context.Context, input purge.PlanInput) (string, error) {
	if s == nil || s.DB == nil {
		return "", runtime.ErrInvariantViolation
	}
	maxBatch := input.MaxBatchSize
	if maxBatch <= 0 {
		maxBatch = 50000
	}
	var batchID string
	err := s.DB.QueryRowContext(ctx, `SELECT compliance.plan_audit_purge_batch($1,$2,$3,$4,$5,make_interval(secs => $6),$7)`,
		input.TenantID, input.Class, input.CutoffAt.UTC(), input.Actor, input.Reason, input.TTL.Seconds(), maxBatch).Scan(&batchID)
	return batchID, err
}

func (s *Store) Approve(ctx context.Context, tenantID, batchID, approver, reason string) error {
	if s == nil || s.DB == nil {
		return runtime.ErrInvariantViolation
	}
	_, err := s.DB.ExecContext(ctx, `SELECT compliance.approve_audit_purge_batch($1,$2,$3,$4)`, tenantID, batchID, approver, reason)
	return err
}

func (s *Store) Execute(ctx context.Context, tenantID, batchID, owner string) error {
	if s == nil || s.DB == nil {
		return runtime.ErrInvariantViolation
	}
	var code string
	if err := s.DB.QueryRowContext(ctx, `SELECT compliance.execute_audit_purge_batch($1,$2,$3)`, tenantID, batchID, owner).Scan(&code); err != nil {
		return err
	}
	switch code {
	case "completed", "claimed_by_another", "backoff":
		return nil
	default:
		return fmt.Errorf("purge execute: %s", code)
	}
}

func (s *Store) Quarantine(ctx context.Context, tenantID, batchID, owner, errClass string) error {
	if s == nil || s.DB == nil {
		return runtime.ErrInvariantViolation
	}
	_, err := s.DB.ExecContext(ctx, `SELECT compliance.quarantine_audit_purge_batch($1,$2,$3,$4)`, tenantID, batchID, owner, errClass)
	return err
}

func (s *Store) Get(ctx context.Context, tenantID, batchID string) (purge.Batch, error) {
	if s == nil || s.DB == nil {
		return purge.Batch{}, runtime.ErrInvariantViolation
	}
	var b purge.Batch
	err := s.DB.QueryRowContext(ctx, `SELECT tenant_id,batch_id,state,cutoff_at,class,planned_count,planned_digest,
deleted_count,alert_count,delete_attempt,COALESCE(last_error_class,''),COALESCE(policy_version,0),
COALESCE(floor_version,0),COALESCE(claim_owner,''),COALESCE(claim_until,'1970-01-01'::timestamptz),COALESCE(ttl_until,'1970-01-01'::timestamptz),COALESCE(not_before,'1970-01-01'::timestamptz)
FROM compliance.audit_purge_batch WHERE tenant_id=$1 AND batch_id=$2`, tenantID, batchID).Scan(
		&b.TenantID, &b.BatchID, &b.State, &b.CutoffAt, &b.Class, &b.PlannedCount, &b.PlannedDigest,
		&b.DeletedCount, &b.AlertCount, &b.DeleteAttempt, &b.LastError, &b.PolicyVersion,
		&b.FloorVersion, &b.ClaimOwner, &b.ClaimUntil, &b.TTLUntil, &b.NotBefore)
	if err != nil {
		return purge.Batch{}, err
	}
	return b, nil
}

func (s *Store) Gauge(ctx context.Context, now time.Time) (purge.Gauge, error) {
	if s == nil || s.DB == nil {
		return purge.Gauge{}, runtime.ErrInvariantViolation
	}
	var gauge purge.Gauge
	if err := s.DB.QueryRowContext(ctx, `SELECT count(DISTINCT e.tenant_id) FROM compliance.audit_event e
WHERE e.occurred_at < $1 - make_interval(secs => compliance.audit_effective_retention(e.tenant_id, compliance.audit_retention_class(e.event_json->>'action')))
  AND NOT compliance.audit_event_on_hold(e.tenant_id, e.occurred_at)`, now.UTC()).Scan(&gauge.OverdueTenants); err != nil {
		return purge.Gauge{}, err
	}
	if err := s.DB.QueryRowContext(ctx, `SELECT count(*) FROM compliance.audit_legal_hold h
WHERE h.event='placed' AND NOT EXISTS (SELECT 1 FROM compliance.audit_legal_hold r
  WHERE r.tenant_id=h.tenant_id AND r.hold_id=h.hold_id AND r.event='released')`).Scan(&gauge.LegalHolds); err != nil {
		return purge.Gauge{}, err
	}
	return gauge, nil
}

var _ purge.Store = (*Store)(nil)
