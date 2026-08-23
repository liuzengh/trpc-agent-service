package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func redisRateLimitClient(t *testing.T) (*redis.Client, string) {
	t.Helper()
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("TEST_REDIS_URL is required; Redis rate-limit integration not verified")
	}
	options, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(options)
	if err := client.Ping(context.Background()).Err(); err != nil {
		client.Close()
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("p006-ratelimit-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var cursor uint64
		for {
			keys, next, err := client.Scan(ctx, cursor, prefix+"*", 100).Result()
			if err != nil {
				break
			}
			if len(keys) != 0 {
				_ = client.Del(ctx, keys...).Err()
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		_ = client.Close()
	})
	return client, prefix
}

func TestRedisThreeDimensionalRateLimit(t *testing.T) {
	client, prefix := redisRateLimitClient(t)
	limiter, err := New(client, prefix, LimitPolicy{TenantLimit: 40, BindingLimit: 100, ChatLimit: 100, Window: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	request := LimitRequest{TenantID: "tenant-real", Channel: "web", BindingID: "binding-real", ExternalChatID: "chat-real", Cost: 1}
	decision, err := limiter.Allow(context.Background(), request)
	if err != nil || !decision.Allowed {
		t.Fatalf("initial decision=%+v err=%v", decision, err)
	}

	bindingLimiter, err := New(client, prefix+"-binding", LimitPolicy{TenantLimit: 100, BindingLimit: 1, ChatLimit: 100, Window: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bindingLimiter.Allow(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	decision, err = bindingLimiter.Allow(context.Background(), request)
	if !errors.Is(err, ErrRateLimited) || decision.Scope != ScopeBinding {
		t.Fatalf("binding decision=%+v err=%v", decision, err)
	}

	var allowed atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if decision, err := limiter.Allow(context.Background(), request); err == nil && decision.Allowed {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if allowed.Load() > 39 {
		t.Fatalf("concurrent allowed after initial=%d", allowed.Load())
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("rate-limit window did not reset")
		}
		decision, err = limiter.Allow(context.Background(), request)
		if err == nil && decision.Allowed {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
}
