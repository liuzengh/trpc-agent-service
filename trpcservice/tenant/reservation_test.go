package tenant

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func TestReservationsConcurrentAndIdempotent(t *testing.T) {
	for _, backend := range []string{"local", "redis"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			cfg := config.QuotaConfig{Backend: backend, KeyPrefix: "budget-test"}
			if backend == "redis" {
				cfg.RedisURL = "redis://" + miniredis.RunT(t).Addr() + "/0"
			}
			repo := quotaRepository(`{"daily_prompt_tokens":100,"daily_completion_tokens":100,"daily_cost_usd":1}`)
			g, err := NewGuard(ctx, repo, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func(closer interface{ Close() error }) { _ = closer.Close() }(g)
			other := g
			if backend == "redis" {
				other, err = NewGuard(ctx, repo, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer func(closer interface{ Close() error }) { _ = closer.Close() }(other)
			}
			var wg sync.WaitGroup
			var accepted atomic.Int32
			reservations := make(chan Reservation, 30)
			for i := 0; i < 30; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					guard := g
					if i%2 == 0 {
						guard = other
					}
					r, err := guard.ReserveModel(ctx, "tutorial-tenant", 10, 10, .1)
					if err == nil {
						accepted.Add(1)
						reservations <- r
					} else if !errors.Is(err, ErrBudgetExceeded) {
						t.Error(err)
					}
				}(i)
			}
			wg.Wait()
			close(reservations)
			if accepted.Load() != 10 {
				t.Fatalf("concurrent reservations=%d, want 10", accepted.Load())
			}
			r := <-reservations
			if first, err := other.SettleModel(ctx, r, 5, 5, .05); err != nil || !first {
				t.Fatalf("settle=%t %v", first, err)
			}
			if first, err := g.SettleModel(ctx, r, 5, 5, .05); err != nil || first {
				t.Fatalf("replay=%t %v", first, err)
			}
			if _, err := g.SettleModel(ctx, r, 0, 0, 0); err == nil {
				t.Fatal("conflicting refund accepted")
			}
			if _, err := g.ReserveModel(ctx, "tutorial-tenant", 5, 5, .05); err != nil {
				t.Fatal(err)
			}
			if _, err := g.ReserveModel(ctx, "tutorial-tenant", 1, 1, .01); !errors.Is(err, ErrBudgetExceeded) {
				t.Fatal("budget overspent")
			}
		})
	}
}

func TestReservationChargesActualOverrunAndRejectsInvalidUsage(t *testing.T) {
	g, _ := NewGuard(context.Background(), quotaRepository(`{"daily_prompt_tokens":100}`), config.QuotaConfig{Backend: "local"})
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(g)
	r, err := g.ReserveModel(context.Background(), "tutorial-tenant", 10, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = g.SettleModel(context.Background(), r, 150, 2, 0); err != nil {
		t.Fatal(err)
	}
	if _, err = g.ReserveModel(context.Background(), "tutorial-tenant", 1, 1, 0); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatal("overrun not charged")
	}
	if _, err = g.ReserveModel(context.Background(), "tutorial-tenant", -1, 0, 0); err == nil {
		t.Fatal("negative budget accepted")
	}
}

func TestMonetaryBudgetCannotBeBypassedWithUnknownPrice(t *testing.T) {
	g, _ := NewGuard(context.Background(), quotaRepository(`{"daily_cost_usd":1}`), config.QuotaConfig{Backend: "local"})
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(g)
	if _, err := g.ReserveModel(context.Background(), "tutorial-tenant", 10, 10, 0); !errors.Is(err, ErrPricingRequired) {
		t.Fatal("zero-price bypass of monetary quota")
	}
	for _, raw := range []string{`{"daily_cost":1}`, `null`, `{} {}`, `{"daily_prompt_tokens":-1}`} {
		if _, err := ParseQuotaPolicy([]byte(raw)); err == nil {
			t.Fatal("malformed or misspelled quota accepted")
		}
	}
}
