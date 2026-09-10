// Package inmemory provides a concurrency-safe budget ledger for tests and
// single-process deployments.
package inmemory

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
)

type ledgerKey struct {
	tenantID    string
	periodStart time.Time
}

type reservationKey struct {
	tenantID      string
	reservationID string
}

type ledger struct {
	usedTokens     int64
	reservedTokens int64
	usedMinor      int64
	reservedMinor  int64
}

// Store is an in-memory implementation of budget.Store.
type Store struct {
	mu           sync.Mutex
	ledgers      map[ledgerKey]*ledger
	reservations map[reservationKey]budget.Reservation
}

// New creates an empty in-memory ledger.
func New() *Store {
	return &Store{ledgers: make(map[ledgerKey]*ledger), reservations: make(map[reservationKey]budget.Reservation)}
}

// Ledger is a read-only usage snapshot useful for operational checks and
// deterministic tests.
type Ledger struct {
	TenantID       string
	PeriodStart    time.Time
	UsedTokens     int64
	ReservedTokens int64
	UsedMinor      int64
	ReservedMinor  int64
}

// Snapshot returns the current monthly counters.
func (store *Store) Snapshot(ctx context.Context, tenantID string, periodStart time.Time) (Ledger, error) {
	if ctx == nil {
		return Ledger{}, errors.New("budget snapshot context is required")
	}
	if err := ctx.Err(); err != nil {
		return Ledger{}, err
	}
	periodStart = normalizePeriod(periodStart)
	store.mu.Lock()
	defer store.mu.Unlock()
	value := store.ledgers[ledgerKey{tenantID: tenantID, periodStart: periodStart}]
	if value == nil {
		return Ledger{TenantID: tenantID, PeriodStart: periodStart}, nil
	}
	return Ledger{TenantID: tenantID, PeriodStart: periodStart, UsedTokens: value.usedTokens, ReservedTokens: value.reservedTokens, UsedMinor: value.usedMinor, ReservedMinor: value.reservedMinor}, nil
}

// Reserve atomically admits an execution against the in-memory monthly ledger.
func (store *Store) Reserve(ctx context.Context, input budget.ReserveInput) (budget.Reservation, error) {
	if err := validContext(ctx); err != nil {
		return budget.Reservation{}, err
	}
	if err := validateInput(input); err != nil {
		return budget.Reservation{}, err
	}
	periodStart := normalizePeriod(input.PeriodStart)
	key := reservationKey{tenantID: input.TenantID, reservationID: input.ReservationID}
	store.mu.Lock()
	defer store.mu.Unlock()
	if existing, ok := store.reservations[key]; ok {
		if existing.PeriodStart != periodStart || existing.EstimatedTokens != input.Estimate.Tokens() || existing.EstimatedSpendMinor != input.Estimate.SpendMinor {
			return budget.Reservation{}, budget.ErrConflict
		}
		return existing, nil
	}
	entry := store.ledgers[ledgerKey{tenantID: input.TenantID, periodStart: periodStart}]
	if entry == nil {
		entry = &ledger{}
		store.ledgers[ledgerKey{tenantID: input.TenantID, periodStart: periodStart}] = entry
	}
	if exceeds(input.Limits.TokenBudget, entry.usedTokens, entry.reservedTokens, input.Estimate.Tokens()) || exceeds(input.Limits.SpendLimitMinor, entry.usedMinor, entry.reservedMinor, input.Estimate.SpendMinor) {
		return budget.Reservation{}, budget.ErrExceeded
	}
	if entry.reservedTokens > math.MaxInt64-input.Estimate.Tokens() || entry.reservedMinor > math.MaxInt64-input.Estimate.SpendMinor {
		return budget.Reservation{}, budget.ErrInvalid
	}
	entry.reservedTokens += input.Estimate.Tokens()
	entry.reservedMinor += input.Estimate.SpendMinor
	reservation := budget.Reservation{
		TenantID: input.TenantID, ReservationID: input.ReservationID, PeriodStart: periodStart,
		Limits: input.Limits, EstimatedTokens: input.Estimate.Tokens(), EstimatedSpendMinor: input.Estimate.SpendMinor, State: budget.ReservationStateReserved,
	}
	store.reservations[key] = reservation
	return reservation, nil
}

// Settle commits actual usage and releases the reservation's held capacity.
func (store *Store) Settle(ctx context.Context, tenantID, reservationID string, usage budget.Usage) (budget.Reservation, error) {
	if err := validContext(ctx); err != nil {
		return budget.Reservation{}, err
	}
	if usage.InputTokens < 0 || usage.OutputTokens < 0 || usage.SpendMinor < 0 || usage.InputTokens > math.MaxInt64-usage.OutputTokens {
		return budget.Reservation{}, budget.ErrInvalid
	}
	key := reservationKey{tenantID: tenantID, reservationID: reservationID}
	store.mu.Lock()
	defer store.mu.Unlock()
	reservation, ok := store.reservations[key]
	if !ok {
		return budget.Reservation{}, budget.ErrNotFound
	}
	if reservation.State == budget.ReservationStateSettled {
		if reservation.ActualTokens != usage.Tokens() || reservation.ActualSpendMinor != usage.SpendMinor {
			return budget.Reservation{}, budget.ErrConflict
		}
		return reservation, nil
	}
	if reservation.State != budget.ReservationStateReserved {
		return budget.Reservation{}, budget.ErrConflict
	}
	entry := store.ledgers[ledgerKey{tenantID: tenantID, periodStart: reservation.PeriodStart}]
	if entry == nil || entry.reservedTokens < reservation.EstimatedTokens || entry.reservedMinor < reservation.EstimatedSpendMinor {
		return budget.Reservation{}, budget.ErrConflict
	}
	if entry.usedTokens > math.MaxInt64-usage.Tokens() || entry.usedMinor > math.MaxInt64-usage.SpendMinor {
		return budget.Reservation{}, budget.ErrInvalid
	}
	exceeded := exceedsAfterRelease(reservation, usage, entry)
	entry.reservedTokens -= reservation.EstimatedTokens
	entry.reservedMinor -= reservation.EstimatedSpendMinor
	entry.usedTokens += usage.Tokens()
	entry.usedMinor += usage.SpendMinor
	reservation.ActualTokens = usage.Tokens()
	reservation.ActualSpendMinor = usage.SpendMinor
	reservation.State = budget.ReservationStateSettled
	store.reservations[key] = reservation
	if exceeded {
		return reservation, budget.ErrExceeded
	}
	return reservation, nil
}

// Release returns a reserved execution's held capacity to the in-memory ledger.
func (store *Store) Release(ctx context.Context, tenantID, reservationID string) (budget.Reservation, error) {
	if err := validContext(ctx); err != nil {
		return budget.Reservation{}, err
	}
	key := reservationKey{tenantID: tenantID, reservationID: reservationID}
	store.mu.Lock()
	defer store.mu.Unlock()
	reservation, ok := store.reservations[key]
	if !ok {
		return budget.Reservation{}, budget.ErrNotFound
	}
	if reservation.State == budget.ReservationStateReleased || reservation.State == budget.ReservationStateSettled {
		return reservation, nil
	}
	if reservation.State != budget.ReservationStateReserved {
		return budget.Reservation{}, budget.ErrConflict
	}
	entry := store.ledgers[ledgerKey{tenantID: tenantID, periodStart: reservation.PeriodStart}]
	if entry == nil || entry.reservedTokens < reservation.EstimatedTokens || entry.reservedMinor < reservation.EstimatedSpendMinor {
		return budget.Reservation{}, budget.ErrConflict
	}
	entry.reservedTokens -= reservation.EstimatedTokens
	entry.reservedMinor -= reservation.EstimatedSpendMinor
	reservation.State = budget.ReservationStateReleased
	store.reservations[key] = reservation
	return reservation, nil
}

func validContext(ctx context.Context) error {
	if ctx == nil {
		return budget.ErrInvalid
	}
	return ctx.Err()
}

func validateInput(input budget.ReserveInput) error {
	if input.TenantID == "" || input.ReservationID == "" || input.PeriodStart.IsZero() || input.Estimate.InputTokens < 0 || input.Estimate.OutputTokens < 0 || input.Estimate.SpendMinor < 0 || input.Estimate.InputTokens > math.MaxInt64-input.Estimate.OutputTokens {
		return budget.ErrInvalid
	}
	if err := input.Limits.Validate(); err != nil {
		return err
	}
	return nil
}

func normalizePeriod(value time.Time) time.Time {
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), 1, 0, 0, 0, 0, time.UTC)
}

func exceeds(limit *int64, used, reserved, requested int64) bool {
	if limit == nil {
		return false
	}
	if used > *limit || reserved > *limit-used || requested > *limit-used-reserved {
		return true
	}
	return false
}

func exceedsAfterRelease(reservation budget.Reservation, usage budget.Usage, entry *ledger) bool {
	if reservation.Limits.TokenBudget != nil && (entry.usedTokens > *reservation.Limits.TokenBudget || usage.Tokens() > *reservation.Limits.TokenBudget-entry.usedTokens) {
		return true
	}
	if reservation.Limits.SpendLimitMinor != nil && (entry.usedMinor > *reservation.Limits.SpendLimitMinor || usage.SpendMinor > *reservation.Limits.SpendLimitMinor-entry.usedMinor) {
		return true
	}
	return false
}
