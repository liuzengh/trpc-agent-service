package governance

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisTenantRateLimiterSharesOneTenantBucket(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	limiter, err := NewRedisTenantRateLimiter(client)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := limiter.Take(context.Background(), "tenant-a", "support", 2); err != nil {
			t.Fatal(err)
		}
	}
	if err := limiter.Take(context.Background(), "tenant-a", "support", 2); !errors.Is(err, ErrTenantRateLimited) {
		t.Fatalf("error = %v", err)
	}
	if err := limiter.Take(context.Background(), "tenant-a", "sales", 2); err != nil {
		t.Fatal(err)
	}
}
