package postgres

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	migrationpg "github.com/liuzengh/trpc-agent-service/trpcservice/migration/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// Publisher owns the PostgreSQL transaction that changes the tenant's active
// config binding. It deliberately does not write a vector backend itself.
type Publisher struct {
	db        *sql.DB
	authority *migrationpg.Store
}

func NewPublisher(db *sql.DB) *Publisher { return &Publisher{db: db, authority: migrationpg.New(db)} }

func (p *Publisher) Cutover(ctx context.Context, in knowledgedriver.CutoverRequest) (knowledgedriver.SwitchResult, error) {
	if p == nil || p.db == nil || in.At.IsZero() {
		return knowledgedriver.SwitchResult{}, runtime.ErrInvariantViolation
	}
	v := in.Verification
	var tenantVersion, active int64
	err := p.db.QueryRowContext(ctx, `SELECT tenant_version,active_config_version FROM public.cutover_knowledge_backend_migration(
$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`, in.TenantID, in.MigrationID,
		in.ExpectedTenantVersion, in.ExpectedVersion, v.SourceCount, v.TargetCount, v.SourceDigest, v.TargetDigest,
		v.SourceWatermark, v.TargetWatermark, v.SampleDigest, in.At.UTC(), in.Metadata.SwitchID, in.Metadata.ActorID,
		in.Metadata.ReasonCode, in.Metadata.CorrelationID, in.Metadata.TraceID, nullable(in.Metadata.Traceparent)).Scan(&tenantVersion, &active)
	if err != nil {
		return knowledgedriver.SwitchResult{}, mapPublisherError(err)
	}
	return p.result(ctx, in.TenantID, in.MigrationID, tenantVersion, active, false)
}

func (p *Publisher) BeginObserve(ctx context.Context, in knowledgedriver.ObserveRequest) (knowledgedriver.SwitchResult, error) {
	if p == nil || p.db == nil || in.At.IsZero() || in.ObserveUntil.IsZero() {
		return knowledgedriver.SwitchResult{}, runtime.ErrInvariantViolation
	}
	var tenantVersion, active int64
	err := p.db.QueryRowContext(ctx, `SELECT tenant_version,active_config_version FROM public.begin_knowledge_backend_observation($1,$2,$3,$4,$5,$6)`,
		in.TenantID, in.MigrationID, in.ExpectedTenantVersion, in.ExpectedVersion, in.At.UTC(), in.ObserveUntil.UTC()).Scan(&tenantVersion, &active)
	if err != nil {
		return knowledgedriver.SwitchResult{}, mapPublisherError(err)
	}
	return p.result(ctx, in.TenantID, in.MigrationID, tenantVersion, active, false)
}

func (p *Publisher) Rollback(ctx context.Context, in knowledgedriver.RollbackRequest) (knowledgedriver.SwitchResult, error) {
	if p == nil || p.db == nil || in.At.IsZero() {
		return knowledgedriver.SwitchResult{}, runtime.ErrInvariantViolation
	}
	var tenantVersion, active int64
	err := p.db.QueryRowContext(ctx, `SELECT tenant_version,active_config_version FROM public.rollback_knowledge_backend_migration(
$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, in.TenantID, in.MigrationID, in.ExpectedTenantVersion,
		in.ExpectedVersion, in.RollbackSyncWatermark, in.At.UTC(), in.Metadata.SwitchID, in.Metadata.ActorID,
		in.Metadata.ReasonCode, in.Metadata.CorrelationID, in.Metadata.TraceID, nullable(in.Metadata.Traceparent)).Scan(&tenantVersion, &active)
	if err != nil {
		return knowledgedriver.SwitchResult{}, mapPublisherError(err)
	}
	return p.result(ctx, in.TenantID, in.MigrationID, tenantVersion, active, true)
}

func (p *Publisher) Cleanup(ctx context.Context, in knowledgedriver.CleanupRequest) (knowledgedriver.SwitchResult, error) {
	if p == nil || p.db == nil || in.At.IsZero() {
		return knowledgedriver.SwitchResult{}, runtime.ErrInvariantViolation
	}
	var tenantVersion, active int64
	err := p.db.QueryRowContext(ctx, `SELECT tenant_version,active_config_version FROM public.cleanup_knowledge_backend_migration($1,$2,$3,$4,$5,$6)`,
		in.TenantID, in.MigrationID, in.ExpectedTenantVersion, in.ExpectedVersion, in.RollbackSyncWatermark, in.At.UTC()).Scan(&tenantVersion, &active)
	if err != nil {
		return knowledgedriver.SwitchResult{}, mapPublisherError(err)
	}
	status, err := p.DrainStatus(ctx, in.TenantID, in.MigrationID)
	if err != nil {
		return knowledgedriver.SwitchResult{}, err
	}
	return p.result(ctx, in.TenantID, in.MigrationID, tenantVersion, active, status.RolledBack)
}

func (p *Publisher) DrainStatus(ctx context.Context, tenantID, migrationID string) (knowledgedriver.DrainStatus, error) {
	if p == nil || p.db == nil || tenantID == "" || migrationID == "" {
		return knowledgedriver.DrainStatus{}, runtime.ErrTenantScope
	}
	var value knowledgedriver.DrainStatus
	err := p.db.QueryRowContext(ctx, `SELECT source_in_flight,target_in_flight,forward_outstanding,reverse_outstanding,
active_config_version,rolled_back FROM public.knowledge_backend_migration_drain_status($1,$2)`, tenantID, migrationID).Scan(
		&value.SourceInFlight, &value.TargetInFlight, &value.ForwardOutstanding, &value.ReverseOutstanding,
		&value.ActiveConfigVersion, &value.RolledBack)
	return value, mapPublisherError(err)
}

func (p *Publisher) result(ctx context.Context, tenantID, migrationID string, tenantVersion, active int64, rolledBack bool) (knowledgedriver.SwitchResult, error) {
	value, err := p.authority.Get(ctx, tenantID, migrationID)
	if err != nil {
		return knowledgedriver.SwitchResult{}, err
	}
	return knowledgedriver.SwitchResult{Migration: value, TenantVersion: tenantVersion, ActiveConfigVersion: active, RolledBack: rolledBack}, nil
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func mapPublisherError(err error) error {
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

var _ knowledgedriver.CutoverPublisher = (*Publisher)(nil)
