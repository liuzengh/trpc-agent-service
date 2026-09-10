package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestAdmissionRateLimiterSharesTenantBudgetAcrossInstances(t *testing.T) {
	mini, err := miniredis.Run()
	if err != nil {
		t.Fatalf("run miniredis: %v", err)
	}
	defer mini.Close()
	client, err := NewClient(context.Background(), "redis://"+mini.Addr())
	if err != nil {
		t.Fatalf("new redis client: %v", err)
	}
	defer client.Close()
	first, err := NewAdmissionRateLimiter(client, 1, time.Minute)
	if err != nil {
		t.Fatalf("new first limiter: %v", err)
	}
	second, err := NewAdmissionRateLimiter(client, 1, time.Minute)
	if err != nil {
		t.Fatalf("new second limiter: %v", err)
	}
	identity := gateway.AdmissionIdentity{Tenant: tenant.RuntimeContext{TenantID: "tenant-a", AppID: "support"}}
	if err := first.Allow(context.Background(), identity); err != nil {
		t.Fatalf("first admission: %v", err)
	}
	err = second.Allow(context.Background(), identity)
	if !errors.Is(err, gateway.ErrAdmissionRateLimited) {
		t.Fatalf("second admission error = %v, want shared rate limit", err)
	}
	otherApp := identity
	otherApp.Tenant.AppID = "other"
	if err := second.Allow(context.Background(), otherApp); err != nil {
		t.Fatalf("other application admission: %v", err)
	}
}

func TestAdmissionRateLimiterRejectsInvalidConfiguration(t *testing.T) {
	if _, err := NewAdmissionRateLimiter(nil, 1, time.Second); err == nil {
		t.Fatal("nil redis client was accepted")
	}
	mini, err := miniredis.Run()
	if err != nil {
		t.Fatalf("run miniredis: %v", err)
	}
	defer mini.Close()
	client, err := NewClient(context.Background(), "redis://"+mini.Addr())
	if err != nil {
		t.Fatalf("new redis client: %v", err)
	}
	defer client.Close()
	if _, err := NewAdmissionRateLimiter(client, 0, time.Second); err == nil {
		t.Fatal("zero admission rate limit was accepted")
	}
	if _, err := NewAdmissionRateLimiter(client, 1, 0); err == nil {
		t.Fatal("zero admission rate window was accepted")
	}
}
