package inmemory

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
)

func TestStoreReserveIsAtomicAcrossConcurrentRequests(t *testing.T) {
	store := New()
	limit := int64(100)
	period := time.Date(2026, 9, 7, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	input := func(id string) budget.ReserveInput {
		return budget.ReserveInput{
			TenantID: "tenant", ReservationID: id, PeriodStart: period,
			Limits: budget.Limits{TokenBudget: &limit}, Estimate: budget.Estimate{InputTokens: 60},
		}
	}

	const attempts = 8
	results := make(chan error, attempts)
	var wait sync.WaitGroup
	for index := 0; index < attempts; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			_, err := store.Reserve(context.Background(), input(fmt.Sprintf("request-%d", index)))
			results <- err
		}(index)
	}
	wait.Wait()
	close(results)

	var admitted int
	for err := range results {
		switch {
		case err == nil:
			admitted++
		case errors.Is(err, budget.ErrExceeded):
		default:
			t.Fatalf("concurrent reserve error = %v", err)
		}
	}
	if admitted != 1 {
		t.Fatalf("admitted reservations = %d, want 1", admitted)
	}
	snapshot, err := store.Snapshot(context.Background(), "tenant", period)
	if err != nil || snapshot.ReservedTokens != 60 {
		t.Fatalf("atomic ledger = %+v, err = %v", snapshot, err)
	}
}

//nolint:gocyclo // Lifecycle test intentionally exercises reserve, settle, and release transitions.
func TestStoreLifecycleIsIdempotentAndReleasesCapacity(t *testing.T) {
	store := New()
	ctx := context.Background()
	period := time.Date(2026, 9, 7, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	tokenLimit, spendLimit := int64(100), int64(50)
	input := budget.ReserveInput{
		TenantID: "tenant", ReservationID: "settle", PeriodStart: period,
		Limits:   budget.Limits{TokenBudget: &tokenLimit, SpendLimitMinor: &spendLimit, Currency: "USD"},
		Estimate: budget.Estimate{InputTokens: 10, OutputTokens: 20, SpendMinor: 5},
	}

	reservation, err := store.Reserve(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if !reservation.PeriodStart.Equal(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)) || reservation.EstimatedTokens != 30 || reservation.State != budget.ReservationStateReserved {
		t.Fatalf("reservation = %+v", reservation)
	}
	duplicate, err := store.Reserve(ctx, input)
	if err != nil || duplicate.ReservationID != reservation.ReservationID {
		t.Fatalf("duplicate reservation = %+v, err = %v", duplicate, err)
	}
	input.Estimate.OutputTokens++
	if _, err := store.Reserve(ctx, input); !errors.Is(err, budget.ErrConflict) {
		t.Fatalf("idempotency conflict = %v", err)
	}

	settled, err := store.Settle(ctx, "tenant", "settle", budget.Usage{InputTokens: 4, OutputTokens: 6, SpendMinor: 7})
	if err != nil || settled.State != budget.ReservationStateSettled || settled.ActualTokens != 10 || settled.ActualSpendMinor != 7 {
		t.Fatalf("settled reservation = %+v, err = %v", settled, err)
	}
	if again, err := store.Settle(ctx, "tenant", "settle", budget.Usage{InputTokens: 4, OutputTokens: 6, SpendMinor: 7}); err != nil || again.State != budget.ReservationStateSettled {
		t.Fatalf("idempotent settlement = %+v, err = %v", again, err)
	}
	if _, err := store.Settle(ctx, "tenant", "settle", budget.Usage{InputTokens: 4, OutputTokens: 7, SpendMinor: 7}); !errors.Is(err, budget.ErrConflict) {
		t.Fatalf("settlement conflict = %v", err)
	}
	ledger, err := store.Snapshot(ctx, "tenant", period)
	if err != nil || ledger.UsedTokens != 10 || ledger.UsedMinor != 7 || ledger.ReservedTokens != 0 || ledger.ReservedMinor != 0 {
		t.Fatalf("settled ledger = %+v, err = %v", ledger, err)
	}
	if released, err := store.Release(ctx, "tenant", "settle"); err != nil || released.State != budget.ReservationStateSettled {
		t.Fatalf("release after settlement = %+v, err = %v", released, err)
	}

	releasedInput := input
	releasedInput.ReservationID = "release"
	releasedInput.Estimate = budget.Estimate{InputTokens: 8, SpendMinor: 3}
	released, err := store.Reserve(ctx, releasedInput)
	if err != nil {
		t.Fatal(err)
	}
	if released, err = store.Release(ctx, "tenant", released.ReservationID); err != nil || released.State != budget.ReservationStateReleased {
		t.Fatalf("release = %+v, err = %v", released, err)
	}
	if again, err := store.Release(ctx, "tenant", "release"); err != nil || again.State != budget.ReservationStateReleased {
		t.Fatalf("idempotent release = %+v, err = %v", again, err)
	}
	if _, err := store.Settle(ctx, "tenant", "release", budget.Usage{}); !errors.Is(err, budget.ErrConflict) {
		t.Fatalf("settlement after release = %v", err)
	}
	ledger, err = store.Snapshot(ctx, "tenant", period)
	if err != nil || ledger.ReservedTokens != 0 || ledger.ReservedMinor != 0 {
		t.Fatalf("released ledger = %+v, err = %v", ledger, err)
	}
}

func TestStoreRejectsInvalidInputsAndProtectsOverflow(t *testing.T) {
	store := New()
	period := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	var nilContext context.Context
	if _, err := store.Reserve(nilContext, budget.ReserveInput{}); !errors.Is(err, budget.ErrInvalid) {
		t.Fatalf("nil reserve context = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Reserve(canceled, budget.ReserveInput{TenantID: "tenant", ReservationID: "cancel", PeriodStart: period}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled reserve context = %v", err)
	}
	if _, err := store.Snapshot(nilContext, "tenant", period); err == nil {
		t.Fatal("nil snapshot context unexpectedly succeeded")
	}
	if _, err := store.Snapshot(canceled, "tenant", period); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled snapshot context = %v", err)
	}
	for _, input := range []budget.ReserveInput{
		{TenantID: "", ReservationID: "id", PeriodStart: period},
		{TenantID: "tenant", ReservationID: "", PeriodStart: period},
		{TenantID: "tenant", ReservationID: "id"},
		{TenantID: "tenant", ReservationID: "negative", PeriodStart: period, Estimate: budget.Estimate{InputTokens: -1}},
		{TenantID: "tenant", ReservationID: "currency", PeriodStart: period, Limits: budget.Limits{SpendLimitMinor: int64Pointer(1), Currency: "usd"}},
	} {
		if _, err := store.Reserve(context.Background(), input); !errors.Is(err, budget.ErrInvalid) {
			t.Fatalf("invalid reserve input %+v error = %v", input, err)
		}
	}
	tokenLimit := int64(5)
	if _, err := store.Reserve(context.Background(), budget.ReserveInput{TenantID: "tenant", ReservationID: "token-limit", PeriodStart: period, Limits: budget.Limits{TokenBudget: &tokenLimit}, Estimate: budget.Estimate{InputTokens: 6}}); !errors.Is(err, budget.ErrExceeded) {
		t.Fatalf("token limit error = %v", err)
	}
	spendLimit := int64(5)
	if _, err := store.Reserve(context.Background(), budget.ReserveInput{TenantID: "tenant", ReservationID: "spend-limit", PeriodStart: period, Limits: budget.Limits{SpendLimitMinor: &spendLimit, Currency: "USD"}, Estimate: budget.Estimate{SpendMinor: 6}}); !errors.Is(err, budget.ErrExceeded) {
		t.Fatalf("spend limit error = %v", err)
	}

	max := int64(math.MaxInt64)
	if _, err := store.Reserve(context.Background(), budget.ReserveInput{TenantID: "overflow", ReservationID: "max", PeriodStart: period, Estimate: budget.Estimate{InputTokens: max}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Reserve(context.Background(), budget.ReserveInput{TenantID: "overflow", ReservationID: "next", PeriodStart: period, Estimate: budget.Estimate{InputTokens: 1}}); !errors.Is(err, budget.ErrInvalid) {
		t.Fatalf("reservation overflow error = %v", err)
	}
	if _, err := store.Settle(context.Background(), "missing", "id", budget.Usage{}); !errors.Is(err, budget.ErrNotFound) {
		t.Fatalf("missing settlement error = %v", err)
	}
	if _, err := store.Release(context.Background(), "missing", "id"); !errors.Is(err, budget.ErrNotFound) {
		t.Fatalf("missing release error = %v", err)
	}
}

func int64Pointer(value int64) *int64 { return &value }
