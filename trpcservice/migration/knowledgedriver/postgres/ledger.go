// Package postgres implements the durable Knowledge migration repair ledger.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Ledger struct{ db *sql.DB }

func New(db *sql.DB) *Ledger { return &Ledger{db: db} }

func (l *Ledger) Record(ctx context.Context, in knowledgedriver.RecordRequest) (knowledgedriver.Mutation, error) {
	if l == nil || l.db == nil {
		return knowledgedriver.Mutation{}, runtime.ErrBackendUnavailable
	}
	if in.TenantID == "" || in.MigrationID == "" || in.MutationID == "" || in.Epoch < 1 || in.ConfigVersion < 1 || in.Key.TenantID != in.TenantID ||
		in.Key.KnowledgeID == "" || in.Key.KnowledgeVersion < 1 || in.Key.ChunkID == "" ||
		(in.Operation != knowledgedriver.OperationUpsert && in.Operation != knowledgedriver.OperationDelete) ||
		in.SourceRevision < 1 || !validDigest(in.MutationDigest) || in.CreatedAt.IsZero() {
		return knowledgedriver.Mutation{}, runtime.ErrInvariantViolation
	}
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return knowledgedriver.Mutation{}, mapError(err)
	}
	defer tx.Rollback()
	createdAt := in.CreatedAt.UTC().Truncate(time.Microsecond)
	_, err = tx.ExecContext(ctx, `SELECT public.record_knowledge_migration_mutation($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		in.TenantID, in.MigrationID, in.MutationID, in.Epoch, in.Key.KnowledgeID, in.Key.KnowledgeVersion,
		in.Key.ChunkID, in.Operation, in.SourceRevision, in.MutationDigest, in.ConfigVersion, createdAt)
	if err != nil {
		return knowledgedriver.Mutation{}, mapError(err)
	}
	value, err := scan(tx.QueryRowContext(ctx, selectSQL+` WHERE tenant_id=$1 AND migration_id=$2 AND knowledge_id=$3 AND knowledge_version=$4 AND chunk_id=$5 AND mutation_id=$6`,
		in.TenantID, in.MigrationID, in.Key.KnowledgeID, in.Key.KnowledgeVersion, in.Key.ChunkID, in.MutationID))
	if err != nil {
		return value, err
	}
	if value.Epoch != in.Epoch || (in.Direction != "" && value.Direction != in.Direction) || value.Operation != in.Operation || value.SourceRevision != in.SourceRevision ||
		value.MutationDigest != in.MutationDigest || !value.CreatedAt.Equal(createdAt) {
		return knowledgedriver.Mutation{}, runtime.ErrIdempotencyCollision
	}
	if err := tx.Commit(); err != nil {
		return knowledgedriver.Mutation{}, mapError(err)
	}
	return value, nil
}

func (l *Ledger) Claim(ctx context.Context, in knowledgedriver.ClaimRequest) ([]knowledgedriver.Mutation, error) {
	if l == nil || l.db == nil {
		return nil, runtime.ErrBackendUnavailable
	}
	if in.TenantID == "" || in.MigrationID == "" || in.WorkerID == "" || in.Limit < 1 || in.Now.IsZero() || in.Lease <= 0 {
		return nil, runtime.ErrInvariantViolation
	}
	rows, err := l.db.QueryContext(ctx, `WITH candidates AS (
 SELECT tenant_id,migration_id,knowledge_id,knowledge_version,chunk_id,mutation_id
 FROM public.knowledge_migration_mutation WHERE tenant_id=$1 AND migration_id=$2 AND state<>'applied' AND not_before<=$3
 AND (state='pending' OR lease_until<=$3) ORDER BY created_at,knowledge_id,knowledge_version,chunk_id,mutation_id
 FOR UPDATE SKIP LOCKED LIMIT $4)
UPDATE public.knowledge_migration_mutation m SET state='applying',attempt=m.attempt+1,lease_owner=$5,
 lease_until=$3+($6 * interval '1 microsecond'),updated_at=$3,version=m.version+1 FROM candidates c
WHERE (m.tenant_id,m.migration_id,m.knowledge_id,m.knowledge_version,m.chunk_id,m.mutation_id)=
 (c.tenant_id,c.migration_id,c.knowledge_id,c.knowledge_version,c.chunk_id,c.mutation_id)
RETURNING `+claimedColumns, in.TenantID, in.MigrationID, in.Now.UTC(), in.Limit, in.WorkerID, in.Lease.Microseconds())
	if err != nil {
		return nil, mapError(err)
	}
	defer rows.Close()
	var result []knowledgedriver.Mutation
	for rows.Next() {
		value, scanErr := scan(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func (l *Ledger) MarkApplied(ctx context.Context, in knowledgedriver.CompleteRequest) (knowledgedriver.Mutation, error) {
	if l == nil || l.db == nil {
		return knowledgedriver.Mutation{}, runtime.ErrBackendUnavailable
	}
	if in.TenantID == "" || in.MigrationID == "" || in.MutationID == "" || in.WorkerID == "" ||
		in.Key.TenantID != in.TenantID || in.Key.KnowledgeID == "" || in.Key.KnowledgeVersion < 1 || in.Key.ChunkID == "" ||
		in.ExpectedVersion < 1 || in.TargetRevision < 1 || !validDigest(in.TargetDigest) || in.At.IsZero() {
		return knowledgedriver.Mutation{}, runtime.ErrInvariantViolation
	}
	row := l.db.QueryRowContext(ctx, `UPDATE public.knowledge_migration_mutation SET state='applied',target_revision=$9,
 target_digest=$10,applied_at=$11,lease_owner='',lease_until=NULL,last_error_class='',updated_at=$11,version=version+1
WHERE tenant_id=$1 AND migration_id=$2 AND knowledge_id=$3 AND knowledge_version=$4 AND chunk_id=$5 AND mutation_id=$6
 AND state='applying' AND lease_owner=$7 AND version=$8 AND lease_until>=$11 RETURNING `+columns,
		in.TenantID, in.MigrationID, in.Key.KnowledgeID, in.Key.KnowledgeVersion, in.Key.ChunkID, in.MutationID,
		in.WorkerID, in.ExpectedVersion, in.TargetRevision, in.TargetDigest, in.At.UTC())
	return scan(row)
}

func (l *Ledger) MarkRetry(ctx context.Context, in knowledgedriver.RetryRequest) (knowledgedriver.Mutation, error) {
	if l == nil || l.db == nil {
		return knowledgedriver.Mutation{}, runtime.ErrBackendUnavailable
	}
	if in.TenantID == "" || in.MigrationID == "" || in.MutationID == "" || in.WorkerID == "" ||
		in.Key.TenantID != in.TenantID || in.Key.KnowledgeID == "" || in.Key.KnowledgeVersion < 1 || in.Key.ChunkID == "" ||
		in.ExpectedVersion < 1 || in.ErrorClass == "" || len(in.ErrorClass) > 64 || in.At.IsZero() || in.NotBefore.Before(in.At) {
		return knowledgedriver.Mutation{}, runtime.ErrInvariantViolation
	}
	row := l.db.QueryRowContext(ctx, `UPDATE public.knowledge_migration_mutation SET state='pending',not_before=$9,
 last_error_class=$10,lease_owner='',lease_until=NULL,updated_at=$11,version=version+1
WHERE tenant_id=$1 AND migration_id=$2 AND knowledge_id=$3 AND knowledge_version=$4 AND chunk_id=$5 AND mutation_id=$6
 AND state='applying' AND lease_owner=$7 AND version=$8 AND lease_until>=$11 RETURNING `+columns,
		in.TenantID, in.MigrationID, in.Key.KnowledgeID, in.Key.KnowledgeVersion, in.Key.ChunkID, in.MutationID,
		in.WorkerID, in.ExpectedVersion, in.NotBefore.UTC(), in.ErrorClass, in.At.UTC())
	return scan(row)
}

func (l *Ledger) Outstanding(ctx context.Context, tenantID, migrationID string) (int64, error) {
	if l == nil || l.db == nil {
		return 0, runtime.ErrBackendUnavailable
	}
	if tenantID == "" || migrationID == "" {
		return 0, runtime.ErrTenantScope
	}
	var count int64
	err := l.db.QueryRowContext(ctx, `SELECT count(*) FROM public.knowledge_migration_mutation WHERE tenant_id=$1 AND migration_id=$2 AND state<>'applied'`, tenantID, migrationID).Scan(&count)
	return count, mapError(err)
}

const columns = `tenant_id,migration_id,mutation_id,epoch,direction,knowledge_id,knowledge_version,chunk_id,operation,
source_revision,mutation_digest,state,attempt,lease_owner,lease_until,not_before,last_error_class,
target_revision,target_digest,created_at,applied_at,version`
const claimedColumns = `m.tenant_id,m.migration_id,m.mutation_id,m.epoch,m.direction,m.knowledge_id,m.knowledge_version,m.chunk_id,m.operation,
m.source_revision,m.mutation_digest,m.state,m.attempt,m.lease_owner,m.lease_until,m.not_before,m.last_error_class,
m.target_revision,m.target_digest,m.created_at,m.applied_at,m.version`
const selectSQL = `SELECT tenant_id,migration_id,mutation_id,epoch,direction,knowledge_id,knowledge_version,chunk_id,operation,
source_revision,mutation_digest,state,attempt,lease_owner,lease_until,not_before,last_error_class,target_revision,target_digest,
created_at,applied_at,version FROM public.knowledge_migration_mutation`

type scanner interface{ Scan(...any) error }

func scan(row scanner) (knowledgedriver.Mutation, error) {
	var value knowledgedriver.Mutation
	var lease, applied sql.NullTime
	var target sql.NullInt64
	err := row.Scan(&value.TenantID, &value.MigrationID, &value.MutationID, &value.Epoch, &value.Direction, &value.Key.KnowledgeID,
		&value.Key.KnowledgeVersion, &value.Key.ChunkID, &value.Operation, &value.SourceRevision, &value.MutationDigest,
		&value.State, &value.Attempt, &value.LeaseOwner, &lease, &value.NotBefore, &value.LastErrorClass, &target,
		&value.TargetDigest, &value.CreatedAt, &applied, &value.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return knowledgedriver.Mutation{}, runtime.ErrVersionConflict
	}
	if err != nil {
		return knowledgedriver.Mutation{}, mapError(err)
	}
	value.Key.TenantID = value.TenantID
	if lease.Valid {
		value.LeaseUntil = lease.Time.UTC()
	}
	if applied.Valid {
		value.AppliedAt = applied.Time.UTC()
	}
	if target.Valid {
		value.TargetRevision = target.Int64
	}
	value.NotBefore = value.NotBefore.UTC()
	value.CreatedAt = value.CreatedAt.UTC()
	return value, nil
}
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
func mapError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return runtime.ErrIdempotencyCollision
		case "40001", "23514":
			return runtime.ErrVersionConflict
		case "22023", "23503", "55000":
			return runtime.ErrInvariantViolation
		case "P0002":
			return runtime.ErrNotFound
		}
	}
	return err
}

var _ knowledgedriver.MutationLedger = (*Ledger)(nil)
var _ knowledgedriver.Recorder = (*Ledger)(nil)
