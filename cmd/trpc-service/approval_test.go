package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
)

func TestBuildApprovalBackendSharesAcrossServiceInstances(t *testing.T) {
	server := miniredis.RunT(t)
	t.Setenv("TEST_APPROVAL_REDIS_URL", "redis://"+server.Addr())
	cfg := config.CoordinationConfig{Backend: "redis", RedisURLEnv: "TEST_APPROVAL_REDIS_URL", KeyPrefix: "test-platform"}
	ctx := context.Background()
	first, closeFirst, err := buildApprovalBackend(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeFirst)
	nonce, err := first.IssueApproval(ctx, "tenant/app/user/session/tool/args", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	closeFirst() // A stopped process must not destroy outstanding approvals.
	second, closeSecond, err := buildApprovalBackend(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeSecond)
	approved, err := second.ConsumeApproval(ctx, nonce, "tenant/app/user/session/tool/args")
	if err != nil || !approved {
		t.Fatalf("approval on replacement process=%v, err=%v", approved, err)
	}
	approved, err = second.ConsumeApproval(ctx, nonce, "tenant/app/user/session/tool/args")
	if err != nil || approved {
		t.Fatalf("approval replay=%v, err=%v", approved, err)
	}
}

func TestBuildApprovalBackendFailsClosedWithoutLeakingRedisURL(t *testing.T) {
	for _, secret := range []string{"", "redis://private-user:private-password@host:bad-port"} {
		t.Setenv("TEST_APPROVAL_REDIS_URL", secret)
		backend, closeBackend, err := buildApprovalBackend(context.Background(), config.CoordinationConfig{
			Backend: "redis", RedisURLEnv: "TEST_APPROVAL_REDIS_URL",
		})
		closeBackend()
		if err == nil || backend != nil {
			t.Fatal("invalid Redis configuration must not fall back to memory")
		}
		if strings.Contains(err.Error(), "private-") || strings.Contains(err.Error(), "bad-port") {
			t.Fatal("Redis configuration error disclosed connection details")
		}
	}
	server := miniredis.RunT(t)
	t.Setenv("TEST_APPROVAL_REDIS_URL", "redis://"+server.Addr())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	backend, closeBackend, err := buildApprovalBackend(ctx, config.CoordinationConfig{
		Backend: "redis", RedisURLEnv: "TEST_APPROVAL_REDIS_URL",
	})
	closeBackend()
	if err == nil || backend != nil {
		t.Fatal("canceled Redis startup must not create a local approval store")
	}
}
