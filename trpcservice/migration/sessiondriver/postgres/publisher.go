package postgres

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"

	migrationpg "github.com/liuzengh/trpc-agent-service/trpcservice/migration/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/sessiondriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Publisher struct {
	db        *sql.DB
	authority *migrationpg.Store
}

func NewPublisher(db *sql.DB) *Publisher { return &Publisher{db: db, authority: migrationpg.New(db)} }

func (p *Publisher) Cutover(ctx context.Context, in sessiondriver.CutoverRequest) (sessiondriver.SwitchResult, error) {
	if p == nil || p.db == nil {
		return sessiondriver.SwitchResult{}, runtime.ErrBackendUnavailable
	}
	if in.At.IsZero() {
		return sessiondriver.SwitchResult{}, runtime.ErrInvariantViolation
	}
	var tenantVersion, active int64
	v := in.Verification
	err := p.db.QueryRowContext(ctx, `SELECT tenant_version,active_config_version FROM public.cutover_session_backend_migration(
$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`, in.TenantID, in.MigrationID,
		in.ExpectedTenantVersion, in.ExpectedVersion, v.SourceCount, v.TargetCount, v.SourceDigest, v.TargetDigest,
		v.SourceWatermark, v.TargetWatermark, v.SampleDigest, in.At.UTC(), in.Metadata.SwitchID, in.Metadata.ActorID,
		in.Metadata.ReasonCode, in.Metadata.CorrelationID, in.Metadata.TraceID, nullable(in.Metadata.Traceparent)).Scan(&tenantVersion, &active)
	if err != nil {
		return sessiondriver.SwitchResult{}, mapError(err)
	}
	return p.result(ctx, in.TenantID, in.MigrationID, tenantVersion, active, false)
}

func (p *Publisher) BeginObserve(ctx context.Context, in sessiondriver.ObserveRequest) (sessiondriver.SwitchResult, error) {
	if p == nil || p.db == nil {
		return sessiondriver.SwitchResult{}, runtime.ErrBackendUnavailable
	}
	if in.At.IsZero() || in.ObserveUntil.IsZero() {
		return sessiondriver.SwitchResult{}, runtime.ErrInvariantViolation
	}
	var tenantVersion, active int64
	err := p.db.QueryRowContext(ctx, `SELECT tenant_version,active_config_version FROM public.begin_session_backend_observation($1,$2,$3,$4,$5,$6)`,
		in.TenantID, in.MigrationID, in.ExpectedTenantVersion, in.ExpectedVersion, in.At.UTC(), in.ObserveUntil.UTC()).Scan(&tenantVersion, &active)
	if err != nil {
		return sessiondriver.SwitchResult{}, mapError(err)
	}
	return p.result(ctx, in.TenantID, in.MigrationID, tenantVersion, active, false)
}

func (p *Publisher) Rollback(ctx context.Context, in sessiondriver.RollbackRequest) (sessiondriver.SwitchResult, error) {
	if p == nil || p.db == nil {
		return sessiondriver.SwitchResult{}, runtime.ErrBackendUnavailable
	}
	if in.At.IsZero() {
		return sessiondriver.SwitchResult{}, runtime.ErrInvariantViolation
	}
	var tenantVersion, active int64
	err := p.db.QueryRowContext(ctx, `SELECT tenant_version,active_config_version FROM public.rollback_session_backend_migration(
$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, in.TenantID, in.MigrationID, in.ExpectedTenantVersion,
		in.ExpectedVersion, in.RollbackSyncWatermark, in.At.UTC(), in.Metadata.SwitchID, in.Metadata.ActorID,
		in.Metadata.ReasonCode, in.Metadata.CorrelationID, in.Metadata.TraceID, nullable(in.Metadata.Traceparent)).Scan(&tenantVersion, &active)
	if err != nil {
		return sessiondriver.SwitchResult{}, mapError(err)
	}
	return p.result(ctx, in.TenantID, in.MigrationID, tenantVersion, active, true)
}

func (p *Publisher) Cleanup(ctx context.Context, in sessiondriver.CleanupRequest) (sessiondriver.SwitchResult, error) {
	if p == nil || p.db == nil {
		return sessiondriver.SwitchResult{}, runtime.ErrBackendUnavailable
	}
	if in.At.IsZero() {
		return sessiondriver.SwitchResult{}, runtime.ErrInvariantViolation
	}
	var tenantVersion, active int64
	err := p.db.QueryRowContext(ctx, `SELECT tenant_version,active_config_version FROM public.cleanup_session_backend_migration($1,$2,$3,$4,$5,$6)`,
		in.TenantID, in.MigrationID, in.ExpectedTenantVersion, in.ExpectedVersion, in.RollbackSyncWatermark, in.At.UTC()).Scan(&tenantVersion, &active)
	if err != nil {
		return sessiondriver.SwitchResult{}, mapError(err)
	}
	status, err := p.DrainStatus(ctx, in.TenantID, in.MigrationID)
	if err != nil {
		return sessiondriver.SwitchResult{}, err
	}
	return p.result(ctx, in.TenantID, in.MigrationID, tenantVersion, active, status.RolledBack)
}

func (p *Publisher) DrainStatus(ctx context.Context, tenantID, migrationID string) (sessiondriver.DrainStatus, error) {
	if p == nil || p.db == nil || tenantID == "" || migrationID == "" {
		return sessiondriver.DrainStatus{}, runtime.ErrTenantScope
	}
	var value sessiondriver.DrainStatus
	err := p.db.QueryRowContext(ctx, `SELECT source_in_flight,target_in_flight,forward_outstanding,
reverse_outstanding,active_config_version,rolled_back FROM public.session_backend_migration_drain_status($1,$2)`,
		tenantID, migrationID).Scan(&value.SourceInFlight, &value.TargetInFlight, &value.ForwardOutstanding,
		&value.ReverseOutstanding, &value.ActiveConfigVersion, &value.RolledBack)
	return value, mapError(err)
}

func (p *Publisher) result(ctx context.Context, tenantID, migrationID string, tenantVersion, active int64, rolledBack bool) (sessiondriver.SwitchResult, error) {
	value, err := p.authority.Get(ctx, tenantID, migrationID)
	if err != nil {
		return sessiondriver.SwitchResult{}, err
	}
	return sessiondriver.SwitchResult{Migration: value, TenantVersion: tenantVersion, ActiveConfigVersion: active, RolledBack: rolledBack}, nil
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001":
			return runtime.ErrVersionConflict
		case "P0002":
			return runtime.ErrNotFound
		case "22023", "23514", "23503", "55000":
			return runtime.ErrInvariantViolation
		case "23505":
			return runtime.ErrIdempotencyCollision
		}
	}
	return err
}

var _ sessiondriver.CutoverPublisher = (*Publisher)(nil)
