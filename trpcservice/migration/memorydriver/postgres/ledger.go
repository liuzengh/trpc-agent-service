// Package postgres implements the durable Memory migration repair ledger.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/memorydriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Ledger struct{ db *sql.DB }

func New(db *sql.DB) *Ledger { return &Ledger{db: db} }

func (l *Ledger) Record(ctx context.Context, in memorydriver.RecordRequest) (memorydriver.Mutation, error) {
	if l == nil || l.db == nil {
		return memorydriver.Mutation{}, runtime.ErrBackendUnavailable
	}
	if !validRecord(in) {
		return memorydriver.Mutation{}, runtime.ErrInvariantViolation
	}
	at := in.CreatedAt.UTC().Truncate(time.Microsecond)
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return memorydriver.Mutation{}, mapError(err)
	}
	defer tx.Rollback()
	// The database function locks and validates backend_migration together
	// with the intent.  A stale worker therefore cannot append a forward or
	// reverse repair after the authority epoch/configuration changed.
	_, err = tx.ExecContext(ctx, `SELECT public.record_memory_migration_mutation($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		in.TenantID, in.MigrationID, in.MutationID, in.Epoch, in.Key.AppName, in.Key.UserID, in.SourceDigest, in.ConfigVersion, at)
	if err != nil {
		return memorydriver.Mutation{}, mapError(err)
	}
	value, scanErr := scan(tx.QueryRowContext(ctx, selectSQL+` WHERE tenant_id=$1 AND migration_id=$2 AND mutation_id=$3`, in.TenantID, in.MigrationID, in.MutationID))
	if scanErr != nil {
		return value, scanErr
	}
	if value.Epoch != in.Epoch || value.Direction != in.Direction || value.SourceDigest != in.SourceDigest || !value.CreatedAt.Equal(at) {
		return memorydriver.Mutation{}, runtime.ErrIdempotencyCollision
	}
	if err := tx.Commit(); err != nil {
		return memorydriver.Mutation{}, mapError(err)
	}
	return value, nil
}

func (l *Ledger) Claim(ctx context.Context, in memorydriver.ClaimRequest) ([]memorydriver.Mutation, error) {
	if l == nil || l.db == nil {
		return nil, runtime.ErrBackendUnavailable
	}
	if in.TenantID == "" || in.MigrationID == "" || in.WorkerID == "" || in.Limit < 1 || in.Now.IsZero() || in.Lease <= 0 {
		return nil, runtime.ErrInvariantViolation
	}
	// The UPDATE has both m and candidates in scope.  Keep every candidate
	// reference explicit and qualify RETURNING with m: otherwise PostgreSQL 16
	// rejects columns such as tenant_id as ambiguous.
	rows, err := l.db.QueryContext(ctx, `WITH candidates AS (
	SELECT m.tenant_id,m.migration_id,m.app_name,m.user_id,m.mutation_id
	FROM public.memory_migration_mutation m
	WHERE m.tenant_id=$1 AND m.migration_id=$2 AND m.state<>'applied'
	  AND m.not_before<=$3 AND (m.state='pending' OR m.lease_until<=$3)
	ORDER BY m.created_at,m.app_name,m.user_id,m.mutation_id
	FOR UPDATE SKIP LOCKED
	LIMIT $4
)
UPDATE public.memory_migration_mutation m
SET state='applying',attempt=m.attempt+1,lease_owner=$5,
    lease_until=$3+($6 * interval '1 microsecond'),updated_at=$3,version=m.version+1
FROM candidates c
WHERE (m.tenant_id,m.migration_id,m.app_name,m.user_id,m.mutation_id)=
      (c.tenant_id,c.migration_id,c.app_name,c.user_id,c.mutation_id)
RETURNING `+qualifiedColumns("m"), in.TenantID, in.MigrationID, in.Now.UTC(), in.Limit, in.WorkerID, in.Lease.Microseconds())
	if err != nil {
		return nil, mapError(err)
	}
	defer rows.Close()
	var result []memorydriver.Mutation
	for rows.Next() {
		value, e := scan(rows)
		if e != nil {
			return nil, e
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func (l *Ledger) MarkApplied(ctx context.Context, in memorydriver.CompleteRequest) (memorydriver.Mutation, error) {
	if l == nil || l.db == nil {
		return memorydriver.Mutation{}, runtime.ErrBackendUnavailable
	}
	if !validComplete(in) {
		return memorydriver.Mutation{}, runtime.ErrInvariantViolation
	}
	row := l.db.QueryRowContext(ctx, `UPDATE public.memory_migration_mutation SET state='applied',target_digest=$8,applied_at=$9,lease_owner='',lease_until=NULL,last_error_class='',updated_at=$9,version=version+1 WHERE tenant_id=$1 AND migration_id=$2 AND app_name=$3 AND user_id=$4 AND mutation_id=$5 AND state='applying' AND lease_owner=$6 AND version=$7 AND lease_until>=$9 RETURNING `+columns, in.TenantID, in.MigrationID, in.Key.AppName, in.Key.UserID, in.MutationID, in.WorkerID, in.ExpectedVersion, in.TargetDigest, in.At.UTC())
	return scan(row)
}
func (l *Ledger) MarkRetry(ctx context.Context, in memorydriver.RetryRequest) (memorydriver.Mutation, error) {
	if l == nil || l.db == nil {
		return memorydriver.Mutation{}, runtime.ErrBackendUnavailable
	}
	if !validRetry(in) {
		return memorydriver.Mutation{}, runtime.ErrInvariantViolation
	}
	row := l.db.QueryRowContext(ctx, `UPDATE public.memory_migration_mutation SET state='pending',not_before=$8,last_error_class=$9,lease_owner='',lease_until=NULL,updated_at=$10,version=version+1 WHERE tenant_id=$1 AND migration_id=$2 AND app_name=$3 AND user_id=$4 AND mutation_id=$5 AND state='applying' AND lease_owner=$6 AND version=$7 AND lease_until>=$10 RETURNING `+columns, in.TenantID, in.MigrationID, in.Key.AppName, in.Key.UserID, in.MutationID, in.WorkerID, in.ExpectedVersion, in.NotBefore.UTC(), in.ErrorClass, in.At.UTC())
	return scan(row)
}
func (l *Ledger) Outstanding(ctx context.Context, tenantID, migrationID string) (int64, error) {
	if l == nil || l.db == nil {
		return 0, runtime.ErrBackendUnavailable
	}
	var count int64
	err := l.db.QueryRowContext(ctx, `SELECT count(*) FROM public.memory_migration_mutation WHERE tenant_id=$1 AND migration_id=$2 AND state<>'applied'`, tenantID, migrationID).Scan(&count)
	return count, mapError(err)
}

const columns = `tenant_id,migration_id,mutation_id,epoch,direction,app_name,user_id,source_digest,state,attempt,lease_owner,lease_until,not_before,last_error_class,target_digest,created_at,applied_at,version`
const selectSQL = `SELECT ` + columns + ` FROM public.memory_migration_mutation`

func qualifiedColumns(alias string) string {
	parts := strings.Split(columns, ",")
	for index := range parts {
		parts[index] = alias + "." + parts[index]
	}
	return strings.Join(parts, ",")
}

type scanner interface{ Scan(...any) error }

func scan(row scanner) (memorydriver.Mutation, error) {
	var v memorydriver.Mutation
	var lease, applied sql.NullTime
	err := row.Scan(&v.TenantID, &v.MigrationID, &v.MutationID, &v.Epoch, &v.Direction, &v.Key.AppName, &v.Key.UserID, &v.SourceDigest, &v.State, &v.Attempt, &v.LeaseOwner, &lease, &v.NotBefore, &v.LastErrorClass, &v.TargetDigest, &v.CreatedAt, &applied, &v.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return v, runtime.ErrVersionConflict
	}
	if err != nil {
		return v, mapError(err)
	}
	v.Key.TenantID = v.TenantID
	if lease.Valid {
		v.LeaseUntil = lease.Time.UTC()
	}
	if applied.Valid {
		v.AppliedAt = applied.Time.UTC()
	}
	v.NotBefore = v.NotBefore.UTC()
	v.CreatedAt = v.CreatedAt.UTC()
	return v, nil
}
func validRecord(in memorydriver.RecordRequest) bool {
	return in.TenantID != "" && in.MigrationID != "" && in.MutationID != "" && in.Epoch >= 1 && in.ConfigVersion >= 1 && (in.Direction == memorydriver.DirectionForward || in.Direction == memorydriver.DirectionReverse) && in.Key.TenantID == in.TenantID && strings.HasPrefix(in.Key.AppName, in.TenantID+"/") && in.Key.UserID != "" && digest(in.SourceDigest) && !in.CreatedAt.IsZero()
}
func validComplete(in memorydriver.CompleteRequest) bool {
	return in.TenantID != "" && in.MigrationID != "" && in.MutationID != "" && in.WorkerID != "" && in.Key.TenantID == in.TenantID && strings.HasPrefix(in.Key.AppName, in.TenantID+"/") && in.Key.UserID != "" && in.ExpectedVersion >= 1 && digest(in.TargetDigest) && !in.At.IsZero()
}
func validRetry(in memorydriver.RetryRequest) bool {
	return in.TenantID != "" && in.MigrationID != "" && in.MutationID != "" && in.WorkerID != "" && in.Key.TenantID == in.TenantID && strings.HasPrefix(in.Key.AppName, in.TenantID+"/") && in.Key.UserID != "" && in.ExpectedVersion >= 1 && in.ErrorClass != "" && len(in.ErrorClass) <= 64 && !in.At.IsZero() && !in.NotBefore.Before(in.At)
}
func digest(v string) bool {
	if len(v) != 64 || v != strings.ToLower(v) {
		return false
	}
	for _, c := range v {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
func mapError(err error) error {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		switch pg.Code {
		case "P0002":
			return runtime.ErrNotFound
		case "23505":
			return runtime.ErrIdempotencyCollision
		case "40001", "23514":
			return runtime.ErrVersionConflict
		case "22023", "23503", "55000":
			return runtime.ErrInvariantViolation
		}
	}
	return err
}

var _ memorydriver.MutationLedger = (*Ledger)(nil)
