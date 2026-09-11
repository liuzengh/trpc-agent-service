package postgresadapter_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	postgresadapter "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/domain"
)

// This regression deliberately crosses the real PostgreSQL-clock grace period.
// It uses only the pre-existing Store seam, so the baseline fails on admitting a
// stale route, not on an absent field, new method, or migration fixture.
func TestIntegrationApplyLagRejectsWhileSourceObservationIsFresh(t *testing.T) {
	store, pool := setup(t)
	ctx := context.Background()
	mustBegin(t, store, 1)
	mustApply(t, store, 1, event("initial-route", 1))
	if err := store.ObserveSource(ctx, source(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Resolve(ctx, "telegram", "account-1"); err != nil {
		t.Fatalf("a short normal apply lag must retain its grace period: %v", err)
	}
	time.Sleep(61 * time.Second)
	if err := store.ObserveSource(ctx, source(2)); err != nil {
		t.Fatal(err)
	}
	h := health(t, store)
	if h.Stale {
		t.Fatal("the trusted source observation is fresh; this is not source expiry")
	}
	if h.Initialized {
		t.Error("continuous unapplied source watermark remained admissible after 60 seconds")
	}
	if _, err := store.Resolve(ctx, "telegram", "account-1"); !errors.Is(err, domain.ErrProjectionApplyLag) {
		t.Errorf("Resolve must classify continuous apply lag: %v", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = store.VerifyGeneration(ctx, tx, "telegram", "account-1", 1); !errors.Is(err, domain.ErrProjectionApplyLag) {
		t.Errorf("the admission transaction guard must classify continuous apply lag: %v", err)
	}
}

// Direct SQL only arranges elapsed database time for the remaining regression
// cases. Their decisions and persisted episode are observed through the Store.
// The test above independently verifies the real 60-second boundary.
func expireApplyLag(t *testing.T, pool *pgxpool.Pool) time.Time {
	t.Helper()
	var since time.Time
	err := pool.QueryRow(context.Background(), `UPDATE gateway_route_replay_state
 SET apply_lag_since=clock_timestamp()-interval '61 seconds'
 WHERE apply_lag_since IS NOT NULL RETURNING apply_lag_since`).Scan(&since)
	if err != nil {
		t.Fatal(err)
	}
	return since
}

func requireExpiredEpisode(t *testing.T, store *postgresadapter.Store, since time.Time, contiguous, highest uint64) {
	t.Helper()
	h := health(t, store)
	if h.ApplyLagSince == nil || !h.ApplyLagSince.Equal(since) || !h.ApplyLagExceeded || h.Initialized || h.Stale || h.ContiguousSequence != contiguous || h.HighestSequence != highest {
		t.Fatalf("continuous episode was cleared, renewed or conflated with source expiry: %+v", h)
	}
	if err := h.RequireInitialized(); !errors.Is(err, domain.ErrProjectionApplyLag) {
		t.Fatalf("health did not expose typed apply-lag rejection: %v", err)
	}
}

func TestIntegrationApplyLagEpisodeSurvivesPartialProgressAndRestart(t *testing.T) {
	store, pool := setup(t)
	ctx := context.Background()
	mustBegin(t, store, 0)
	mustApply(t, store, 1, event("route-1", 1))
	if err := store.ObserveSource(ctx, source(4)); err != nil {
		t.Fatal(err)
	}
	since := expireApplyLag(t, pool)
	// New source messages and delayed old observations cannot renew the episode.
	for _, last := range []uint64{6, 2} {
		if err := store.ObserveSource(ctx, source(last)); err != nil {
			t.Fatal(err)
		}
	}
	restarted := postgresadapter.NewStore(pool)
	mustBegin(t, restarted, 4)
	requireExpiredEpisode(t, restarted, since, 1, 6)
	mustApply(t, restarted, 1, event("route-1", 1)) // duplicate receipt
	mustApply(t, restarted, 2, event("route-2", 2))
	requireExpiredEpisode(t, restarted, since, 2, 6)
	mustApply(t, restarted, 4, event("route-4", 4)) // a hole remains
	requireExpiredEpisode(t, restarted, since, 2, 6)
	mustApply(t, restarted, 3, event("route-3", 3))
	// Catching the startup target is insufficient: the known tail is now 6.
	requireExpiredEpisode(t, restarted, since, 4, 6)
	mustApply(t, restarted, 5, event("route-5", 5))
	requireExpiredEpisode(t, restarted, since, 5, 6)
	mustApply(t, restarted, 6, event("route-6", 6))
	h := health(t, restarted)
	if h.ApplyLagSince != nil || h.ApplyLagExceeded || !h.Initialized || h.TargetSequence != 4 || h.ContiguousSequence != 6 {
		t.Fatalf("complete catch-up failed to recover: %+v", h)
	}
	if route, err := restarted.Resolve(ctx, "telegram", "account-1"); err != nil || route.Generation != 6 {
		t.Fatalf("caught-up route remains rejected: %+v %v", route, err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = restarted.VerifyGeneration(ctx, tx, "telegram", "account-1", 6); err != nil {
		t.Fatalf("caught-up admission guard remains rejected: %v", err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// A late observation/restart cannot resurrect the old episode.
	mustBegin(t, restarted, 2)
	if err = restarted.ObserveSource(ctx, source(3)); err != nil {
		t.Fatal(err)
	}
	if h = health(t, restarted); h.ApplyLagSince != nil || !h.Initialized {
		t.Fatalf("delayed source snapshot resurrected an episode: %+v", h)
	}
	if err = restarted.ObserveSource(ctx, source(7)); err != nil {
		t.Fatal(err)
	}
	if h = health(t, restarted); h.ApplyLagSince == nil || !h.ApplyLagSince.After(since) || h.ApplyLagExceeded || !h.Initialized {
		t.Fatalf("a genuinely new episode did not receive a fresh grace period: %+v", h)
	}
}

func TestIntegrationApplyLagStartsFromOutOfOrderApplyUsingDatabaseClock(t *testing.T) {
	store, pool := setup(t)
	ctx := context.Background()
	mustBegin(t, store, 0)
	var before, after time.Time
	if err := pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	mustApply(t, store, 3, event("route-3", 3))
	if err := pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	h := health(t, store)
	if h.ApplyLagSince == nil || h.ApplyLagSince.Before(before) || h.ApplyLagSince.After(after) || h.ApplyLagExceeded || !h.Initialized || h.HighestSequence != 3 || h.ContiguousSequence != 0 {
		t.Fatalf("out-of-order Apply failed to start a PG-clock episode: %+v", h)
	}
	started := *h.ApplyLagSince
	observation := source(3)
	observation.ObservedAt = time.Now().Add(24 * time.Hour)
	if err := store.ObserveSource(ctx, observation); err != nil {
		t.Fatal(err)
	}
	if h = health(t, store); h.ApplyLagSince == nil || !h.ApplyLagSince.Equal(started) {
		t.Fatalf("a future source timestamp changed the PG episode clock: %+v", h)
	}
	since := expireApplyLag(t, pool)
	mustApply(t, store, 3, event("route-3", 3))
	requireExpiredEpisode(t, store, since, 0, 3)
	mustApply(t, store, 1, event("route-1", 1))
	requireExpiredEpisode(t, store, since, 1, 3)
	mustApply(t, store, 2, event("route-2", 2))
	if h = health(t, store); h.ApplyLagSince != nil || !h.Initialized || h.ContiguousSequence != 3 {
		t.Fatalf("closing the receipt hole did not clear the episode: %+v", h)
	}
	if got := count(t, pool, "gateway_route_stream_receipts"); got != 3 {
		t.Fatalf("duplicate receipt changed the replay ledger: %d", got)
	}
}

func TestIntegrationApplyLagGuardRechecksHealthAfterRouteLookup(t *testing.T) {
	store, pool := setup(t)
	ctx := context.Background()
	mustBegin(t, store, 0)
	mustApply(t, store, 1, event("route-1", 1))
	if err := store.ObserveSource(ctx, source(2)); err != nil {
		t.Fatal(err)
	}
	route, err := store.Resolve(ctx, "telegram", "account-1")
	if err != nil {
		t.Fatal(err)
	}
	expireApplyLag(t, pool)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = store.VerifyGeneration(ctx, tx, route.Provider, route.AccountID, route.Generation); !errors.Is(err, domain.ErrProjectionApplyLag) {
		t.Fatalf("an earlier successful lookup bypassed the transaction's health gate: %v", err)
	}
}

func TestIntegrationApplyLagConcurrentProgressPreservesOldestEpisode(t *testing.T) {
	store, pool := setup(t)
	ctx := context.Background()
	mustBegin(t, store, 0)
	mustApply(t, store, 1, event("route-1", 1))
	if err := store.ObserveSource(ctx, source(24)); err != nil {
		t.Fatal(err)
	}
	since := expireApplyLag(t, pool)
	var wg sync.WaitGroup
	errs := make(chan error, 26)
	for seq := uint64(3); seq <= 24; seq++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- store.ApplyFromStream(ctx, position(seq), event(fmt.Sprintf("route-%d", seq), int64(seq)))
		}()
	}
	for _, last := range []uint64{24, 12, 5, 1} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- store.ObserveSource(ctx, source(last))
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	requireExpiredEpisode(t, store, since, 1, 24)
	mustApply(t, store, 2, event("route-2", 2))
	if h := health(t, store); h.ApplyLagSince != nil || h.ApplyLagExceeded || !h.Initialized || h.ContiguousSequence != 24 {
		t.Fatalf("concurrent observations or progress lost the full catch-up: %+v", h)
	}
}
