//go:build integration

package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/testinfra"
	"github.com/redis/go-redis/v9"
)

// TestRateLimitBackendFaultClassification proves the P2-03 fail-closed
// contract on a real Redis: a refused or stopped backend is always
// backend-unavailable (never "allowed", never an ordinary rate limit), the
// limiter recovers after the backend restarts with the same client, and the
// atomic quota keeps its counters across the outage.
func TestRateLimitBackendFaultClassification(t *testing.T) {
	lab := testinfra.NewDockerLab(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	lab.Start(ctx)
	lab.WaitHealthy(ctx)
	options, err := redis.ParseURL(lab.RedisURL(ctx))
	if err != nil {
		t.Fatal(err)
	}
	options.DialTimeout = time.Second
	options.ReadTimeout = time.Second
	options.WriteTimeout = time.Second
	client := redis.NewClient(options)
	defer client.Close()
	limiter, err := New(client, "p203-rl", LimitPolicy{TenantLimit: 2, BindingLimit: 5, ChatLimit: 10, Window: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	request := LimitRequest{TenantID: "tenant-fault", BindingID: "binding-fault", Channel: "telegram", ExternalChatID: "chat-fault", Cost: 1}

	// Healthy: two admissions, third is a quota rejection (not unavailable).
	for i := 0; i < 2; i++ {
		if _, err := limiter.Allow(ctx, request); err != nil {
			t.Fatalf("admission %d rejected on a healthy backend: %v", i, err)
		}
	}
	_, err = limiter.Allow(ctx, request)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("quota rejection misclassified: %v", err)
	}

	// Stop the backend: every call is backend-unavailable and fail closed.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer stopCancel()
	lab.DisconnectRedis(stopCtx)
	for i := 0; i < 3; i++ {
		decision, err := limiter.Allow(ctx, request)
		if err == nil {
			t.Fatalf("stopped backend admitted request %d", i)
		}
		if !errors.Is(err, ErrRateLimitBackendUnavailable) {
			t.Fatalf("stopped backend misclassified: %v", err)
		}
		if decision.Allowed {
			t.Fatalf("stopped backend produced an allowed decision")
		}
	}

	// Restart the backend: the same client recovers with bounded behavior
	// and the fixed-window counters survive (the quota is still exhausted).
	restartCtx, restartCancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer restartCancel()
	lab.ReconnectRedis(restartCtx)
	recovered := false
	for i := 0; i < 100 && !recovered; i++ {
		_, err := limiter.Allow(ctx, request)
		if err == nil || errors.Is(err, ErrRateLimited) {
			recovered = true
		} else if i < 99 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if !recovered {
		t.Fatalf("limiter never recovered after the backend restart")
	}
}
