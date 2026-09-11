package budget

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres is the authoritative cross-node budget ledger. The caller owns the
// pool lifecycle because the Queue and budget ledger intentionally use the
// same configured PostgreSQL database, while keeping independent pools.
type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(pool *pgxpool.Pool) (*Postgres, error) {
	if pool == nil {
		return nil, errors.New("budget: postgres pool is nil")
	}
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) Reserve(ctx context.Context, req ReserveRequest) (Reservation, error) {
	if err := validateReserve(req); err != nil {
		return Reservation{}, err
	}
	period := PeriodStart(req.BillingPeriod)
	req.BillingPeriod = period
	if req.StartedAt.IsZero() {
		req.StartedAt = time.Now().UTC()
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return Reservation{}, invalidLedgerError(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
		INSERT INTO budget_periods (tenant_id, billing_period, budget_limit_units)
		VALUES ($1, $2, $3)
		ON CONFLICT (tenant_id, billing_period) DO UPDATE
		SET budget_limit_units = EXCLUDED.budget_limit_units,
			updated_at = clock_timestamp()`,
		req.TenantID, period, req.MonthlyLimitUnits); err != nil {
		return Reservation{}, invalidLedgerError(err)
	}
	var limit, reserved, settled, unknown int64
	if err := tx.QueryRow(ctx, `
		SELECT budget_limit_units, reserved_units, settled_units, unknown_units
		FROM budget_periods
		WHERE tenant_id = $1 AND billing_period = $2
		FOR UPDATE`, req.TenantID, period).Scan(&limit, &reserved, &settled, &unknown); err != nil {
		return Reservation{}, invalidLedgerError(err)
	}

	row, found, err := readCall(ctx, tx, req.CallID)
	if err != nil {
		return Reservation{}, invalidLedgerError(err)
	}
	if found {
		if !sameReserve(row.request, req) {
			return Reservation{}, ErrConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return Reservation{}, invalidLedgerError(err)
		}
		return Reservation{TenantID: req.TenantID, CallID: req.CallID, BillingPeriod: row.request.BillingPeriod,
			EstimatedUnits: row.request.EstimatedUnits, State: row.state, Existing: true}, nil
	}
	if limit > 0 && exceeds(settled+unknown+reserved, req.EstimatedUnits, limit) {
		return Reservation{}, ErrBudgetExceeded
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO model_usage_calls (
			tenant_id, call_id, app_namespace, session_id, dedup_key, request_id,
			run_id, call_no, model_name, billing_period, estimated_units,
			input_price_per_million_units, output_price_per_million_units,
			estimated_prompt_tokens, estimated_completion_tokens, state, started_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, 'reserved', $16)`,
		req.TenantID, req.CallID, req.AppNamespace, req.SessionID, req.DedupKey, req.RequestID,
		req.RunID, req.CallNo, req.ModelName, period, req.EstimatedUnits,
		req.InputPricePerMillionUnits, req.OutputPricePerMillionUnits,
		req.EstimatedPrompt, req.EstimatedOutput, req.StartedAt); err != nil {
		return Reservation{}, invalidLedgerError(err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE budget_periods
		SET reserved_units = reserved_units + $3, updated_at = clock_timestamp()
		WHERE tenant_id = $1 AND billing_period = $2`, req.TenantID, period, req.EstimatedUnits); err != nil {
		return Reservation{}, invalidLedgerError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Reservation{}, invalidLedgerError(err)
	}
	return Reservation{TenantID: req.TenantID, CallID: req.CallID, BillingPeriod: period,
		EstimatedUnits: req.EstimatedUnits, State: StateReserved}, nil
}

func (p *Postgres) Settle(ctx context.Context, req SettlementRequest) error {
	if req.TenantID == "" || req.CallID == "" || req.PromptTokens < 0 || req.CompletionTokens < 0 || req.ActualUnits < 0 {
		return ErrInvalidRequest
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return invalidLedgerError(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	row, found, err := readCall(ctx, tx, req.CallID)
	if err != nil {
		return invalidLedgerError(err)
	}
	if !found || row.request.TenantID != req.TenantID {
		return ErrNotFound
	}
	if row.state == StateSettled {
		if row.actual == req.ActualUnits && row.promptTokens == req.PromptTokens && row.completionTokens == req.CompletionTokens {
			return nil
		}
		return ErrConflict
	}
	if row.state != StateReserved {
		return ErrConflict
	}
	if err := lockPeriod(ctx, tx, row.request.TenantID, row.request.BillingPeriod); err != nil {
		return invalidLedgerError(err)
	}
	row, found, err = readCall(ctx, tx, req.CallID)
	if err != nil {
		return invalidLedgerError(err)
	}
	if !found || row.request.TenantID != req.TenantID {
		return ErrNotFound
	}
	if row.state == StateSettled {
		if row.actual == req.ActualUnits && row.promptTokens == req.PromptTokens && row.completionTokens == req.CompletionTokens {
			return nil
		}
		return ErrConflict
	}
	if row.state != StateReserved {
		return ErrConflict
	}
	if _, err := tx.Exec(ctx, `
		UPDATE model_usage_calls
		SET state = 'settled', actual_units = $3, prompt_tokens = $4,
			completion_tokens = $5, settled_at = clock_timestamp(), updated_at = clock_timestamp()
		WHERE tenant_id = $1 AND call_id = $2 AND state = 'reserved'`,
		row.request.TenantID, req.CallID, req.ActualUnits, req.PromptTokens, req.CompletionTokens); err != nil {
		return invalidLedgerError(err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE budget_periods
		SET reserved_units = GREATEST(0, reserved_units - $3),
			settled_units = settled_units + $4, updated_at = clock_timestamp()
		WHERE tenant_id = $1 AND billing_period = $2`,
		row.request.TenantID, row.request.BillingPeriod, row.request.EstimatedUnits, req.ActualUnits); err != nil {
		return invalidLedgerError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return invalidLedgerError(err)
	}
	return nil
}

func (p *Postgres) MarkUnknown(ctx context.Context, req UnknownRequest) error {
	if req.TenantID == "" || req.CallID == "" {
		return ErrInvalidRequest
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return invalidLedgerError(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	row, found, err := readCall(ctx, tx, req.CallID)
	if err != nil {
		return invalidLedgerError(err)
	}
	if !found || row.request.TenantID != req.TenantID {
		return ErrNotFound
	}
	if row.state == StateUnknown {
		return nil
	}
	if row.state != StateReserved {
		return ErrConflict
	}
	if err := lockPeriod(ctx, tx, row.request.TenantID, row.request.BillingPeriod); err != nil {
		return invalidLedgerError(err)
	}
	row, found, err = readCall(ctx, tx, req.CallID)
	if err != nil {
		return invalidLedgerError(err)
	}
	if !found || row.request.TenantID != req.TenantID {
		return ErrNotFound
	}
	if row.state == StateUnknown {
		return nil
	}
	if row.state != StateReserved {
		return ErrConflict
	}
	if _, err := tx.Exec(ctx, `
		UPDATE model_usage_calls
		SET state = 'unknown', unknown_reason = $3, settled_at = clock_timestamp(), updated_at = clock_timestamp()
		WHERE tenant_id = $1 AND call_id = $2 AND state = 'reserved'`,
		row.request.TenantID, req.CallID, boundedErrorType(req.ErrorType)); err != nil {
		return invalidLedgerError(err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE budget_periods
		SET reserved_units = GREATEST(0, reserved_units - $3),
			unknown_units = unknown_units + $3, updated_at = clock_timestamp()
		WHERE tenant_id = $1 AND billing_period = $2`,
		row.request.TenantID, row.request.BillingPeriod, row.request.EstimatedUnits); err != nil {
		return invalidLedgerError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return invalidLedgerError(err)
	}
	return nil
}

func (p *Postgres) Release(ctx context.Context, callID string) error {
	if callID == "" {
		return ErrInvalidRequest
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return invalidLedgerError(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	row, found, err := readCall(ctx, tx, callID)
	if err != nil {
		return invalidLedgerError(err)
	}
	if !found {
		return ErrNotFound
	}
	if row.state == StateReleased {
		return nil
	}
	if row.state != StateReserved {
		return ErrConflict
	}
	if err := lockPeriod(ctx, tx, row.request.TenantID, row.request.BillingPeriod); err != nil {
		return invalidLedgerError(err)
	}
	row, found, err = readCall(ctx, tx, callID)
	if err != nil {
		return invalidLedgerError(err)
	}
	if !found {
		return ErrNotFound
	}
	if row.state == StateReleased {
		return nil
	}
	if row.state != StateReserved {
		return ErrConflict
	}
	if _, err := tx.Exec(ctx, `
		UPDATE model_usage_calls
		SET state = 'released', settled_at = clock_timestamp(), updated_at = clock_timestamp()
		WHERE tenant_id = $1 AND call_id = $2 AND state = 'reserved'`, row.request.TenantID, callID); err != nil {
		return invalidLedgerError(err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE budget_periods
		SET reserved_units = GREATEST(0, reserved_units - $3), updated_at = clock_timestamp()
		WHERE tenant_id = $1 AND billing_period = $2`, row.request.TenantID, row.request.BillingPeriod, row.request.EstimatedUnits); err != nil {
		return invalidLedgerError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return invalidLedgerError(err)
	}
	return nil
}

type callRow struct {
	request                        ReserveRequest
	state                          State
	actual                         int64
	promptTokens, completionTokens int
}

func readCall(ctx context.Context, tx pgx.Tx, callID string) (callRow, bool, error) {
	var row callRow
	var period time.Time
	err := tx.QueryRow(ctx, `
		SELECT tenant_id, app_namespace, session_id, dedup_key, request_id, run_id,
			call_id, call_no, model_name, billing_period, estimated_units,
			input_price_per_million_units, output_price_per_million_units,
			estimated_prompt_tokens, estimated_completion_tokens, state,
			actual_units, prompt_tokens, completion_tokens
		FROM model_usage_calls
		WHERE call_id = $1`, callID).Scan(
		&row.request.TenantID, &row.request.AppNamespace, &row.request.SessionID,
		&row.request.DedupKey, &row.request.RequestID, &row.request.RunID,
		&row.request.CallID, &row.request.CallNo, &row.request.ModelName, &period,
		&row.request.EstimatedUnits, &row.request.InputPricePerMillionUnits,
		&row.request.OutputPricePerMillionUnits, &row.request.EstimatedPrompt, &row.request.EstimatedOutput,
		&row.state, &row.actual, &row.promptTokens, &row.completionTokens)
	if errors.Is(err, pgx.ErrNoRows) {
		return callRow{}, false, nil
	}
	if err != nil {
		return callRow{}, false, err
	}
	row.request.BillingPeriod = period
	return row, true, nil
}

func lockPeriod(ctx context.Context, tx pgx.Tx, tenantID string, period time.Time) error {
	var ignored int64
	return tx.QueryRow(ctx, `
		SELECT reserved_units FROM budget_periods
		WHERE tenant_id = $1 AND billing_period = $2
		FOR UPDATE`, tenantID, PeriodStart(period)).Scan(&ignored)
}

func boundedErrorType(value string) string {
	if len(value) > 128 {
		return value[:128]
	}
	return value
}

func (p *Postgres) Snapshot(ctx context.Context, tenantID string, period time.Time) (PeriodSnapshot, error) {
	var snapshot PeriodSnapshot
	period = PeriodStart(period)
	snapshot.TenantID, snapshot.BillingPeriod = tenantID, period
	err := p.pool.QueryRow(ctx, `
		SELECT budget_limit_units, reserved_units, settled_units, unknown_units
		FROM budget_periods WHERE tenant_id = $1 AND billing_period = $2`, tenantID, period).Scan(
		&snapshot.LimitUnits, &snapshot.ReservedUnits, &snapshot.SettledUnits, &snapshot.UnknownUnits)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return PeriodSnapshot{}, ErrNotFound
		}
		return PeriodSnapshot{}, invalidLedgerError(err)
	}
	return snapshot, nil
}

func (p *Postgres) Close() error { return nil }

var _ Ledger = (*Postgres)(nil)

func postgresConflict(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", ErrConflict, err)
}
