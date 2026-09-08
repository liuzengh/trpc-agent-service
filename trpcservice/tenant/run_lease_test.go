package tenant

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func TestRunLeasesRenewAndOldReleaseCannotFreeNewOwner(t *testing.T) {
	s := miniredis.RunT(t)
	cfg := config.QuotaConfig{Backend: "redis", RedisURL: "redis://" + s.Addr(), KeyPrefix: "lease-test"}
	repo := quotaRepository(`{"concurrent_runs":1}`)
	a, _ := NewGuard(context.Background(), repo, cfg)
	b, _ := NewGuard(context.Background(), repo, cfg)
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(a)
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(b)
	ctx, cancel := context.WithCancel(context.Background())
	old, err := a.acquireRunLease(ctx, "tutorial-tenant", 150*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	// Multiple TTL windows: a live attempt must keep its slot.
	timer := time.NewTimer(400 * time.Millisecond)
	defer timer.Stop()
	<-timer.C
	if _, err := b.AcquireRunLease(context.Background(), "tutorial-tenant"); !errors.Is(err, ErrConcurrencyLimited) {
		t.Fatal("live slot expired")
	}
	cancel()
	<-old.done
	s.SetTime(time.Now().Add(time.Minute))
	newer, err := b.AcquireRunLease(context.Background(), "tutorial-tenant")
	if err != nil {
		t.Fatal(err)
	}
	defer newer.Release()
	old.Release()
	old.Release()
	if _, err := a.AcquireRunLease(context.Background(), "tutorial-tenant"); !errors.Is(err, ErrConcurrencyLimited) {
		t.Fatal("stale release freed another run")
	}
}
func TestRunLeaseLossCancelsExecution(t *testing.T) {
	s := miniredis.RunT(t)
	g, _ := NewGuard(context.Background(), quotaRepository(`{"concurrent_runs":1}`), config.QuotaConfig{Backend: "redis", RedisURL: "redis://" + s.Addr()})
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(g)
	l, err := g.acquireRunLease(context.Background(), "tutorial-tenant", 90*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Release()
	if err := g.redis.ZRem(context.Background(), l.key, l.id).Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-l.Context().Done():
		if !errors.Is(context.Cause(l.Context()), ErrRunLeaseLost) {
			t.Fatal("missing loss cause")
		}
	case <-time.After(time.Second):
		t.Fatal("lost lease did not cancel execution")
	}
}
func TestRunLeaseCloseStopsAllRenewals(t *testing.T) {
	g, _ := NewGuard(context.Background(), quotaRepository(`{"concurrent_runs":2}`), config.QuotaConfig{Backend: "local"})
	a, _ := g.AcquireRunLease(context.Background(), "tutorial-tenant")
	b, _ := g.AcquireRunLease(context.Background(), "tutorial-tenant")
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	for _, l := range []*RunLease{a, b} {
		select {
		case <-l.done:
		default:
			t.Fatal("renewal leaked after close")
		}
		l.Release()
	}
	if _, err := g.AcquireRunLease(context.Background(), "tutorial-tenant"); err == nil {
		t.Fatal("closed guard admitted a run")
	}
}
