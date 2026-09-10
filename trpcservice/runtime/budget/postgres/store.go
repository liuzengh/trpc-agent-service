// Package postgres provides the transactional PostgreSQL budget ledger.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode"

	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
)

var (
	// ErrStorage is the stable category for database failures.
	ErrStorage = errors.New("budget storage failed")
)

// Store is a borrowed *sql.DB-backed budget ledger.
type Store struct {
	db *sql.DB
}

// New creates a PostgreSQL budget store. The caller owns the database pool.
func New(db *sql.DB) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: database is required", budget.ErrInvalid)
	}
	return &Store{db: db}, nil
}

// Reserve atomically admits an execution against the PostgreSQL monthly ledger.
func (store *Store) Reserve(ctx context.Context, input budget.ReserveInput) (budget.Reservation, error) {
	if err := validateContext(ctx); err != nil {
		return budget.Reservation{}, err
	}
	if err := validateInput(input); err != nil {
		return budget.Reservation{}, err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return budget.Reservation{}, wrapStorage(err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	periodStart := normalizePeriod(input.PeriodStart)
	existing, found, err := lockReservation(ctx, tx, input.TenantID, input.ReservationID)
	if err != nil {
		return budget.Reservation{}, err
	}
	if found {
		if existing.PeriodStart != periodStart || existing.EstimatedTokens != input.Estimate.Tokens() || existing.EstimatedSpendMinor != input.Estimate.SpendMinor {
			return budget.Reservation{}, budget.ErrConflict
		}
		if err := tx.Commit(); err != nil {
			return budget.Reservation{}, wrapStorage(err)
		}
		committed = true
		return existing, nil
	}
	// Settle and Release lock the reservation before the ledger. Keep the new
	// reservation path in the same order so a retry racing with settlement
	// cannot hold the ledger row while waiting on the reservation row.
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO public.runtime_budget_ledger (tenant_id, period_start)
		VALUES ($1, $2)
		ON CONFLICT (tenant_id, period_start) DO NOTHING`, input.TenantID, periodStart); err != nil {
		return budget.Reservation{}, wrapStorage(err)
	}
	ledgerValue, err := lockLedger(ctx, tx, input.TenantID, periodStart)
	if err != nil {
		return budget.Reservation{}, err
	}
	if exceeds(input.Limits.TokenBudget, ledgerValue.usedTokens, ledgerValue.reservedTokens, input.Estimate.Tokens()) || exceeds(input.Limits.SpendLimitMinor, ledgerValue.usedMinor, ledgerValue.reservedMinor, input.Estimate.SpendMinor) {
		return budget.Reservation{}, budget.ErrExceeded
	}
	if ledgerValue.reservedTokens > math.MaxInt64-input.Estimate.Tokens() || ledgerValue.reservedMinor > math.MaxInt64-input.Estimate.SpendMinor {
		return budget.Reservation{}, budget.ErrInvalid
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO public.runtime_budget_reservation (
			tenant_id, reservation_id, period_start, token_limit, spend_limit_minor,
			currency, estimated_tokens, estimated_minor, state
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'reserved')`,
		input.TenantID, input.ReservationID, periodStart, nullableInt(input.Limits.TokenBudget), nullableInt(input.Limits.SpendLimitMinor), input.Limits.Currency,
		input.Estimate.Tokens(), input.Estimate.SpendMinor); err != nil {
		return budget.Reservation{}, wrapStorage(err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE public.runtime_budget_ledger
		SET reserved_tokens = reserved_tokens + $3,
		    reserved_minor = reserved_minor + $4,
		    updated_at = now()
		WHERE tenant_id = $1 AND period_start = $2`, input.TenantID, periodStart, input.Estimate.Tokens(), input.Estimate.SpendMinor); err != nil {
		return budget.Reservation{}, wrapStorage(err)
	}
	reservation := budget.Reservation{TenantID: input.TenantID, ReservationID: input.ReservationID, PeriodStart: periodStart, Limits: input.Limits, EstimatedTokens: input.Estimate.Tokens(), EstimatedSpendMinor: input.Estimate.SpendMinor, State: budget.ReservationStateReserved}
	if err := tx.Commit(); err != nil {
		return budget.Reservation{}, wrapStorage(err)
	}
	committed = true
	return reservation, nil
}

// Settle commits actual usage and releases the reservation's held capacity.
//
//nolint:gocyclo // The transaction keeps the idempotent reservation state machine atomic.
func (store *Store) Settle(ctx context.Context, tenantID, reservationID string, usage budget.Usage) (budget.Reservation, error) {
	if err := validateContext(ctx); err != nil {
		return budget.Reservation{}, err
	}
	if err := validateUsage(usage); err != nil {
		return budget.Reservation{}, err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return budget.Reservation{}, wrapStorage(err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	reservation, found, err := lockReservation(ctx, tx, tenantID, reservationID)
	if err != nil {
		return budget.Reservation{}, err
	}
	if !found {
		return budget.Reservation{}, budget.ErrNotFound
	}
	if reservation.State == budget.ReservationStateSettled {
		if reservation.ActualTokens != usage.Tokens() || reservation.ActualSpendMinor != usage.SpendMinor {
			return budget.Reservation{}, budget.ErrConflict
		}
		if err := tx.Commit(); err != nil {
			return budget.Reservation{}, wrapStorage(err)
		}
		committed = true
		return reservation, nil
	}
	if reservation.State != budget.ReservationStateReserved {
		return budget.Reservation{}, budget.ErrConflict
	}
	ledgerValue, err := lockLedger(ctx, tx, tenantID, reservation.PeriodStart)
	if err != nil {
		return budget.Reservation{}, err
	}
	if ledgerValue.reservedTokens < reservation.EstimatedTokens || ledgerValue.reservedMinor < reservation.EstimatedSpendMinor || ledgerValue.usedTokens > math.MaxInt64-usage.Tokens() || ledgerValue.usedMinor > math.MaxInt64-usage.SpendMinor {
		return budget.Reservation{}, budget.ErrConflict
	}
	exceeded := exceedsAfterRelease(reservation, usage, ledgerValue)
	if _, err := tx.ExecContext(ctx, `
		UPDATE public.runtime_budget_ledger
		SET reserved_tokens = reserved_tokens - $3,
		    reserved_minor = reserved_minor - $4,
		    used_tokens = used_tokens + $5,
		    used_minor = used_minor + $6,
		    updated_at = now()
		WHERE tenant_id = $1 AND period_start = $2`, tenantID, reservation.PeriodStart, reservation.EstimatedTokens, reservation.EstimatedSpendMinor, usage.Tokens(), usage.SpendMinor); err != nil {
		return budget.Reservation{}, wrapStorage(err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE public.runtime_budget_reservation
		SET actual_tokens = $3, actual_minor = $4, state = 'settled', updated_at = now()
		WHERE tenant_id = $1 AND reservation_id = $2`, tenantID, reservationID, usage.Tokens(), usage.SpendMinor); err != nil {
		return budget.Reservation{}, wrapStorage(err)
	}
	reservation.ActualTokens, reservation.ActualSpendMinor, reservation.State = usage.Tokens(), usage.SpendMinor, budget.ReservationStateSettled
	if err := tx.Commit(); err != nil {
		return budget.Reservation{}, wrapStorage(err)
	}
	committed = true
	if exceeded {
		return reservation, budget.ErrExceeded
	}
	return reservation, nil
}

// Release returns a reserved execution's held capacity to the PostgreSQL ledger.
func (store *Store) Release(ctx context.Context, tenantID, reservationID string) (budget.Reservation, error) {
	if err := validateContext(ctx); err != nil {
		return budget.Reservation{}, err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return budget.Reservation{}, wrapStorage(err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	reservation, found, err := lockReservation(ctx, tx, tenantID, reservationID)
	if err != nil {
		return budget.Reservation{}, err
	}
	if !found {
		return budget.Reservation{}, budget.ErrNotFound
	}
	if reservation.State == budget.ReservationStateReleased || reservation.State == budget.ReservationStateSettled {
		if err := tx.Commit(); err != nil {
			return budget.Reservation{}, wrapStorage(err)
		}
		committed = true
		return reservation, nil
	}
	if reservation.State != budget.ReservationStateReserved {
		return budget.Reservation{}, budget.ErrConflict
	}
	ledgerValue, err := lockLedger(ctx, tx, tenantID, reservation.PeriodStart)
	if err != nil {
		return budget.Reservation{}, err
	}
	if ledgerValue.reservedTokens < reservation.EstimatedTokens || ledgerValue.reservedMinor < reservation.EstimatedSpendMinor {
		return budget.Reservation{}, budget.ErrConflict
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE public.runtime_budget_ledger
		SET reserved_tokens = reserved_tokens - $3,
		    reserved_minor = reserved_minor - $4,
		    updated_at = now()
		WHERE tenant_id = $1 AND period_start = $2`, tenantID, reservation.PeriodStart, reservation.EstimatedTokens, reservation.EstimatedSpendMinor); err != nil {
		return budget.Reservation{}, wrapStorage(err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE public.runtime_budget_reservation
		SET state = 'released', updated_at = now()
		WHERE tenant_id = $1 AND reservation_id = $2`, tenantID, reservationID); err != nil {
		return budget.Reservation{}, wrapStorage(err)
	}
	reservation.State = budget.ReservationStateReleased
	if err := tx.Commit(); err != nil {
		return budget.Reservation{}, wrapStorage(err)
	}
	committed = true
	return reservation, nil
}

type ledgerValue struct {
	usedTokens, reservedTokens int64
	usedMinor, reservedMinor   int64
}

func lockLedger(ctx context.Context, tx *sql.Tx, tenantID string, periodStart time.Time) (ledgerValue, error) {
	var value ledgerValue
	err := tx.QueryRowContext(ctx, `
		SELECT used_tokens, reserved_tokens, used_minor, reserved_minor
		FROM public.runtime_budget_ledger
		WHERE tenant_id = $1 AND period_start = $2
		FOR UPDATE`, tenantID, periodStart).Scan(&value.usedTokens, &value.reservedTokens, &value.usedMinor, &value.reservedMinor)
	if errors.Is(err, sql.ErrNoRows) {
		return ledgerValue{}, budget.ErrConflict
	}
	if err != nil {
		return ledgerValue{}, wrapStorage(err)
	}
	return value, nil
}

func lockReservation(ctx context.Context, tx *sql.Tx, tenantID, reservationID string) (budget.Reservation, bool, error) {
	var value budget.Reservation
	var periodStart time.Time
	var tokenLimit, spendLimit sql.NullInt64
	var actualTokens, actualMinor sql.NullInt64
	var currency, state string
	err := tx.QueryRowContext(ctx, `
		SELECT period_start, token_limit, spend_limit_minor, currency,
		       estimated_tokens, estimated_minor, actual_tokens, actual_minor, state
		FROM public.runtime_budget_reservation
		WHERE tenant_id = $1 AND reservation_id = $2
		FOR UPDATE`, tenantID, reservationID).Scan(&periodStart, &tokenLimit, &spendLimit, &currency, &value.EstimatedTokens, &value.EstimatedSpendMinor, &actualTokens, &actualMinor, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return budget.Reservation{}, false, nil
	}
	if err != nil {
		return budget.Reservation{}, false, wrapStorage(err)
	}
	value.TenantID, value.ReservationID, value.PeriodStart, value.State = tenantID, reservationID, periodStart.UTC(), budget.ReservationState(state)
	if tokenLimit.Valid {
		value.Limits.TokenBudget = &tokenLimit.Int64
	}
	if spendLimit.Valid {
		value.Limits.SpendLimitMinor = &spendLimit.Int64
	}
	if actualTokens.Valid {
		value.ActualTokens = actualTokens.Int64
	}
	if actualMinor.Valid {
		value.ActualSpendMinor = actualMinor.Int64
	}
	value.Limits.Currency = strings.TrimSpace(currency)
	return value, true, nil
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return budget.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func validateInput(input budget.ReserveInput) error {
	if input.TenantID == "" || input.ReservationID == "" || input.PeriodStart.IsZero() || input.Estimate.InputTokens < 0 || input.Estimate.OutputTokens < 0 || input.Estimate.SpendMinor < 0 || input.Estimate.InputTokens > math.MaxInt64-input.Estimate.OutputTokens || strings.IndexFunc(input.ReservationID, unicode.IsControl) >= 0 {
		return budget.ErrInvalid
	}
	if err := input.Limits.Validate(); err != nil {
		return err
	}
	return nil
}

func validateUsage(value budget.Usage) error {
	if value.InputTokens < 0 || value.OutputTokens < 0 || value.SpendMinor < 0 || value.InputTokens > math.MaxInt64-value.OutputTokens {
		return budget.ErrInvalid
	}
	return nil
}

func exceeds(limit *int64, used, reserved, requested int64) bool {
	if limit == nil {
		return false
	}
	return used > *limit || reserved > *limit-used || requested > *limit-used-reserved
}

func exceedsAfterRelease(reservation budget.Reservation, usage budget.Usage, value ledgerValue) bool {
	if reservation.Limits.TokenBudget != nil && (value.usedTokens > *reservation.Limits.TokenBudget || usage.Tokens() > *reservation.Limits.TokenBudget-value.usedTokens) {
		return true
	}
	if reservation.Limits.SpendLimitMinor != nil && (value.usedMinor > *reservation.Limits.SpendLimitMinor || usage.SpendMinor > *reservation.Limits.SpendLimitMinor-value.usedMinor) {
		return true
	}
	return false
}

func normalizePeriod(value time.Time) time.Time {
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), 1, 0, 0, 0, 0, time.UTC)
}

func nullableInt(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func wrapStorage(err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(ErrStorage, err)
}
