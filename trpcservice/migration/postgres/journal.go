package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

const selectPhaseIntent = `SELECT tenant_id,migration_id,intent_id,request,request_digest,status,result_version,created_at,completed_at
FROM public.backend_migration_phase_intent`

func (s *Store) BeginPhaseIntent(ctx context.Context, in migration.TransitionRequest) (migration.PhaseIntent, error) {
	if s == nil || s.db == nil {
		return migration.PhaseIntent{}, runtime.ErrBackendUnavailable
	}
	created, err := migration.NewPhaseIntent(in)
	if err != nil {
		return migration.PhaseIntent{}, err
	}
	request, err := json.Marshal(created.Request)
	if err != nil {
		return migration.PhaseIntent{}, runtime.ErrInvariantViolation
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO public.backend_migration_phase_intent(
tenant_id,migration_id,intent_id,request,request_digest,status,result_version,created_at)
VALUES($1,$2,$3,$4::jsonb,$5,'pending',0,$6)
ON CONFLICT (tenant_id,migration_id,intent_id) DO NOTHING`, created.TenantID, created.MigrationID,
		created.IntentID, request, created.RequestDigest, created.CreatedAt)
	if err != nil {
		return migration.PhaseIntent{}, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return migration.PhaseIntent{}, err
	}
	if rows == 1 {
		return created, nil
	}
	existing, err := scanPhaseIntent(s.db.QueryRowContext(ctx, selectPhaseIntent+` WHERE tenant_id=$1 AND migration_id=$2 AND intent_id=$3`,
		created.TenantID, created.MigrationID, created.IntentID))
	if err != nil {
		return migration.PhaseIntent{}, err
	}
	if existing.RequestDigest != created.RequestDigest {
		return migration.PhaseIntent{}, runtime.ErrIdempotencyCollision
	}
	return existing, nil
}

func (s *Store) CompletePhaseIntent(ctx context.Context, in migration.PhaseIntent, result migration.Migration, completedAt time.Time) (migration.PhaseIntent, error) {
	if s == nil || s.db == nil {
		return migration.PhaseIntent{}, runtime.ErrBackendUnavailable
	}
	if completedAt.IsZero() || result.TenantID != in.TenantID || result.MigrationID != in.MigrationID ||
		result.State != in.Request.To || result.Version != in.Request.ExpectedVersion+1 {
		return migration.PhaseIntent{}, runtime.ErrInvariantViolation
	}
	updated, err := s.db.ExecContext(ctx, `UPDATE public.backend_migration_phase_intent
SET status='completed',result_version=$4,completed_at=$5
WHERE tenant_id=$1 AND migration_id=$2 AND intent_id=$3 AND status='pending' AND request_digest=$6`,
		in.TenantID, in.MigrationID, in.IntentID, result.Version, completedAt.UTC(), in.RequestDigest)
	if err != nil {
		return migration.PhaseIntent{}, err
	}
	rows, err := updated.RowsAffected()
	if err != nil {
		return migration.PhaseIntent{}, err
	}
	stored, err := scanPhaseIntent(s.db.QueryRowContext(ctx, selectPhaseIntent+` WHERE tenant_id=$1 AND migration_id=$2 AND intent_id=$3`,
		in.TenantID, in.MigrationID, in.IntentID))
	if err != nil {
		return migration.PhaseIntent{}, err
	}
	if stored.RequestDigest != in.RequestDigest {
		return migration.PhaseIntent{}, runtime.ErrIdempotencyCollision
	}
	if rows == 0 && (stored.Status != migration.PhaseIntentCompleted || stored.ResultVersion != result.Version) {
		return migration.PhaseIntent{}, runtime.ErrVersionConflict
	}
	return stored, nil
}

func (s *Store) ListPendingPhaseIntents(ctx context.Context, tenantID, migrationID string) ([]migration.PhaseIntent, error) {
	if s == nil || s.db == nil {
		return nil, runtime.ErrBackendUnavailable
	}
	if tenantID == "" || migrationID == "" {
		return nil, runtime.ErrTenantScope
	}
	rows, err := s.db.QueryContext(ctx, selectPhaseIntent+` WHERE tenant_id=$1 AND migration_id=$2 AND status='pending' ORDER BY created_at,intent_id`, tenantID, migrationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]migration.PhaseIntent, 0)
	for rows.Next() {
		intent, scanErr := scanPhaseIntent(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, intent)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result, nil
}

func scanPhaseIntent(row scanner) (migration.PhaseIntent, error) {
	var value migration.PhaseIntent
	var rawRequest []byte
	var completedAt sql.NullTime
	err := row.Scan(&value.TenantID, &value.MigrationID, &value.IntentID, &rawRequest, &value.RequestDigest,
		&value.Status, &value.ResultVersion, &value.CreatedAt, &completedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return migration.PhaseIntent{}, runtime.ErrNotFound
	}
	if err != nil {
		return migration.PhaseIntent{}, err
	}
	if err := json.Unmarshal(rawRequest, &value.Request); err != nil {
		return migration.PhaseIntent{}, runtime.ErrInvariantViolation
	}
	expected, err := migration.NewPhaseIntent(value.Request)
	if err != nil || expected.TenantID != value.TenantID || expected.MigrationID != value.MigrationID ||
		expected.IntentID != value.IntentID || expected.RequestDigest != value.RequestDigest {
		return migration.PhaseIntent{}, runtime.ErrInvariantViolation
	}
	value.CreatedAt = value.CreatedAt.UTC()
	if completedAt.Valid {
		value.CompletedAt = completedAt.Time.UTC()
	}
	if value.Status == migration.PhaseIntentPending && value.ResultVersion == 0 && value.CompletedAt.IsZero() {
		return value, nil
	}
	if value.Status == migration.PhaseIntentCompleted && value.ResultVersion == value.Request.ExpectedVersion+1 && !value.CompletedAt.IsZero() {
		return value, nil
	}
	return migration.PhaseIntent{}, runtime.ErrInvariantViolation
}

var _ migration.PhaseIntentStore = (*Store)(nil)
