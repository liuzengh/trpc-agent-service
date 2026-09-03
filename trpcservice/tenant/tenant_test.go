package tenant

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

func quotaRepository(policy string) *controlplane.MemoryRepository {
	data := controlplane.DefaultBootstrapData()
	data.Tenants[0].QuotaConfig = json.RawMessage(policy)
	return controlplane.NewMemoryRepository(data)
}

func TestLocalGuardRateConcurrencyAndBudget(t *testing.T) {
	repository := quotaRepository(`{
        "requests_per_minute":1,"concurrent_runs":1,"daily_prompt_tokens":10
    }`)
	guard, err := NewGuard(context.Background(), repository, config.QuotaConfig{
		Backend: config.QuotaBackendLocal,
	})
	if err != nil {
		t.Fatalf("new guard: %v", err)
	}
	if err := guard.AllowInbound(context.Background(), "tutorial-tenant", "alice"); err != nil {
		t.Fatalf("first inbound: %v", err)
	}
	if err := guard.AllowInbound(context.Background(), "tutorial-tenant", "alice"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("rate error=%v", err)
	}
	release, err := guard.AcquireRun(context.Background(), "tutorial-tenant")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := guard.AcquireRun(context.Background(), "tutorial-tenant"); !errors.Is(err, ErrConcurrencyLimited) {
		t.Fatalf("concurrency error=%v", err)
	}
	release()
	if err := guard.RecordUsage(context.Background(), "tutorial-tenant", "request-1", 10, 0, 0); err != nil {
		t.Fatalf("usage: %v", err)
	}
	if _, err := guard.AcquireRun(context.Background(), "tutorial-tenant"); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("budget error=%v", err)
	}
}

func TestRedisGuardSharesRateLimit(t *testing.T) {
	server := miniredis.RunT(t)
	repository := quotaRepository(`{"requests_per_minute":1}`)
	config := config.QuotaConfig{
		Backend:  config.QuotaBackendRedis,
		RedisURL: "redis://" + server.Addr() + "/0", KeyPrefix: "quota-test",
	}
	first, _ := NewGuard(context.Background(), repository, config)
	second, _ := NewGuard(context.Background(), repository, config)
	t.Cleanup(func() {
		_ = first.Close()
		_ = second.Close()
	})
	if err := first.AllowInbound(context.Background(), "tutorial-tenant", "alice"); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := second.AllowInbound(context.Background(), "tutorial-tenant", "alice"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("shared rate error=%v", err)
	}
}
