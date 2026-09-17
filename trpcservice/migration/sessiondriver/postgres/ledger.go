// Package postgres implements the durable session migration repair ledger.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/sessiondriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Ledger struct{ db *sql.DB }

func New(db *sql.DB) *Ledger { return &Ledger{db: db} }

func (l *Ledger) Record(ctx context.Context, in sessiondriver.RecordRequest) (sessiondriver.Mutation, error) {
	if l == nil || l.db == nil {
		return sessiondriver.Mutation{}, runtime.ErrBackendUnavailable
	}
	if in.Direction == "" {
		in.Direction = sessiondriver.DirectionForward
	}
	if in.TenantID == "" || in.MigrationID == "" || in.MutationID == "" || in.Epoch < 1 ||
		(in.Direction != sessiondriver.DirectionForward && in.Direction != sessiondriver.DirectionReverse) ||
		in.SessionKey.TenantID != in.TenantID || in.AgentAppID == "" || in.SessionID == "" ||
		in.SourceVersion < 1 || !validDigest(in.MutationDigest) || in.CreatedAt.IsZero() {
		return sessiondriver.Mutation{}, runtime.ErrInvariantViolation
	}
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return sessiondriver.Mutation{}, mapError(err)
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO public.session_migration_mutation(
tenant_id,migration_id,mutation_id,epoch,direction,agent_app_id,session_id,source_version,
mutation_digest,not_before,created_at,updated_at)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$10,$10)
ON CONFLICT (tenant_id,migration_id,agent_app_id,session_id,mutation_id) DO NOTHING`,
		in.TenantID, in.MigrationID, in.MutationID, in.Epoch, in.Direction,
		in.AgentAppID, in.SessionID, in.SourceVersion, in.MutationDigest, in.CreatedAt.UTC())
	if err != nil {
		return sessiondriver.Mutation{}, mapError(err)
	}
	value, err := scanMutation(tx.QueryRowContext(ctx, `SELECT tenant_id,migration_id,mutation_id,epoch,direction,
agent_app_id,session_id,source_version,mutation_digest,state,attempt,lease_owner,lease_until,not_before,
last_error_class,target_version,target_digest,created_at,applied_at,version
FROM public.session_migration_mutation WHERE tenant_id=$1 AND migration_id=$2 AND agent_app_id=$3 AND session_id=$4 AND mutation_id=$5`,
		in.TenantID, in.MigrationID, in.AgentAppID, in.SessionID, in.MutationID))
	if err != nil {
		return value, err
	}
	if value.Direction != in.Direction || value.Epoch != in.Epoch || value.SourceVersion != in.SourceVersion ||
		value.MutationDigest != in.MutationDigest || !value.CreatedAt.Equal(in.CreatedAt.UTC()) {
		return sessiondriver.Mutation{}, runtime.ErrIdempotencyCollision
	}
	if err := tx.Commit(); err != nil {
		return sessiondriver.Mutation{}, mapError(err)
	}
	return value, nil
}

func (l *Ledger) Claim(ctx context.Context, in sessiondriver.ClaimRequest) ([]sessiondriver.Mutation, error) {
	if l == nil || l.db == nil {
		return nil, runtime.ErrBackendUnavailable
	}
	if in.TenantID == "" || in.MigrationID == "" || in.WorkerID == "" || in.Limit < 1 || in.Now.IsZero() || in.Lease <= 0 {
		return nil, runtime.ErrInvariantViolation
	}
	rows, err := l.db.QueryContext(ctx, `WITH candidates AS (
  SELECT tenant_id,migration_id,agent_app_id,session_id,mutation_id
  FROM public.session_migration_mutation
  WHERE tenant_id=$1 AND migration_id=$2 AND state<>'applied' AND not_before<=$3
    AND (state='pending' OR lease_until<=$3)
  ORDER BY created_at,agent_app_id,session_id,mutation_id
  FOR UPDATE SKIP LOCKED LIMIT $4
)
UPDATE public.session_migration_mutation m SET state='applying',attempt=m.attempt+1,
  lease_owner=$5,lease_until=$3+($6 * interval '1 microsecond'),updated_at=$3,version=m.version+1
FROM candidates c WHERE (m.tenant_id,m.migration_id,m.agent_app_id,m.session_id,m.mutation_id)=
  (c.tenant_id,c.migration_id,c.agent_app_id,c.session_id,c.mutation_id)
RETURNING m.tenant_id,m.migration_id,m.mutation_id,m.epoch,m.direction,m.agent_app_id,m.session_id,
  m.source_version,m.mutation_digest,m.state,m.attempt,m.lease_owner,m.lease_until,m.not_before,
  m.last_error_class,m.target_version,m.target_digest,m.created_at,m.applied_at,m.version`,
		in.TenantID, in.MigrationID, in.Now.UTC(), in.Limit, in.WorkerID, in.Lease.Microseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []sessiondriver.Mutation
	for rows.Next() {
		value, err := scanMutation(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func (l *Ledger) MarkApplied(ctx context.Context, in sessiondriver.CompleteRequest) (sessiondriver.Mutation, error) {
	if l == nil || l.db == nil {
		return sessiondriver.Mutation{}, runtime.ErrBackendUnavailable
	}
	row := l.db.QueryRowContext(ctx, `UPDATE public.session_migration_mutation SET state='applied',
target_version=$8,target_digest=$9,applied_at=$10,lease_owner='',lease_until=NULL,last_error_class='',
updated_at=$10,version=version+1
WHERE tenant_id=$1 AND migration_id=$2 AND agent_app_id=$3 AND session_id=$4 AND mutation_id=$5
  AND state='applying' AND lease_owner=$6 AND version=$7 AND lease_until>=$10
RETURNING tenant_id,migration_id,mutation_id,epoch,direction,agent_app_id,session_id,source_version,mutation_digest,
state,attempt,lease_owner,lease_until,not_before,last_error_class,target_version,target_digest,created_at,applied_at,version`,
		in.TenantID, in.MigrationID, in.AgentAppID, in.SessionID, in.MutationID, in.WorkerID,
		in.ExpectedVersion, in.TargetVersion, in.TargetDigest, in.At.UTC())
	return scanMutation(row)
}

func (l *Ledger) MarkRetry(ctx context.Context, in sessiondriver.RetryRequest) (sessiondriver.Mutation, error) {
	if l == nil || l.db == nil {
		return sessiondriver.Mutation{}, runtime.ErrBackendUnavailable
	}
	row := l.db.QueryRowContext(ctx, `UPDATE public.session_migration_mutation SET state='pending',
not_before=$8,last_error_class=$9,lease_owner='',lease_until=NULL,updated_at=$10,version=version+1
WHERE tenant_id=$1 AND migration_id=$2 AND agent_app_id=$3 AND session_id=$4 AND mutation_id=$5
  AND state='applying' AND lease_owner=$6 AND version=$7 AND lease_until>=$10
RETURNING tenant_id,migration_id,mutation_id,epoch,direction,agent_app_id,session_id,source_version,mutation_digest,
state,attempt,lease_owner,lease_until,not_before,last_error_class,target_version,target_digest,created_at,applied_at,version`,
		in.TenantID, in.MigrationID, in.AgentAppID, in.SessionID, in.MutationID, in.WorkerID,
		in.ExpectedVersion, in.NotBefore.UTC(), in.ErrorClass, in.At.UTC())
	return scanMutation(row)
}

func (l *Ledger) Outstanding(ctx context.Context, tenantID, migrationID string) (int64, error) {
	if l == nil || l.db == nil {
		return 0, runtime.ErrBackendUnavailable
	}
	if tenantID == "" || migrationID == "" {
		return 0, runtime.ErrTenantScope
	}
	var count int64
	err := l.db.QueryRowContext(ctx, `SELECT count(*) FROM public.session_migration_mutation
WHERE tenant_id=$1 AND migration_id=$2 AND state<>'applied'`, tenantID, migrationID).Scan(&count)
	return count, err
}

type scanner interface{ Scan(...any) error }

func scanMutation(row scanner) (sessiondriver.Mutation, error) {
	var value sessiondriver.Mutation
	var leaseUntil, appliedAt sql.NullTime
	var targetVersion sql.NullInt64
	err := row.Scan(&value.TenantID, &value.MigrationID, &value.MutationID, &value.Epoch, &value.Direction,
		&value.AgentAppID, &value.SessionID, &value.SourceVersion, &value.MutationDigest,
		&value.State, &value.Attempt, &value.LeaseOwner, &leaseUntil, &value.NotBefore,
		&value.LastErrorClass, &targetVersion, &value.TargetDigest, &value.CreatedAt, &appliedAt, &value.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiondriver.Mutation{}, runtime.ErrVersionConflict
	}
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "23514") {
			return sessiondriver.Mutation{}, runtime.ErrVersionConflict
		}
		return sessiondriver.Mutation{}, err
	}
	value.SessionKey.TenantID = value.TenantID
	if leaseUntil.Valid {
		value.LeaseUntil = leaseUntil.Time.UTC()
	}
	if appliedAt.Valid {
		value.AppliedAt = appliedAt.Time.UTC()
	}
	if targetVersion.Valid {
		value.TargetVersion = targetVersion.Int64
	}
	value.NotBefore, value.CreatedAt = value.NotBefore.UTC(), value.CreatedAt.UTC()
	return value, nil
}

var _ sessiondriver.MutationLedger = (*Ledger)(nil)
var _ sessiondriver.Recorder = (*Ledger)(nil)

func validDigest(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}
