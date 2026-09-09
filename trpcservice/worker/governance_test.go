package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Violet2314/trpc-agent-service/trpcservice/storage"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

func TestPolicyGovernorPermissions(t *testing.T) {
	governor := NewPolicyGovernor(nil)
	snapshot := workerSnapshot()
	snapshot.Binding.Config = map[string]string{"allowed_users": "user-1, user-2"}

	if err := governor.Authorize(
		context.Background(),
		snapshot,
		[]storage.UserEvent{{SenderID: "user-1"}},
	); err != nil {
		t.Fatalf("Authorize(allowed) error = %v", err)
	}
	err := governor.Authorize(
		context.Background(),
		snapshot,
		[]storage.UserEvent{{SenderID: "user-3"}},
	)
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("Authorize(denied) error = %v, want ErrPermissionDenied", err)
	}
}

func TestRedisQuotaCounterRateAndDailyBudget(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	counter, err := NewRedisQuotaCounter(client)
	if err != nil {
		t.Fatalf("NewRedisQuotaCounter() error = %v", err)
	}
	now := time.Date(2026, 8, 26, 5, 0, 0, 0, time.UTC)
	counter.now = func() time.Time { return now }
	quota := tenant.Quota{RatePerMinute: 1, DailyTokenLimit: 10}

	allowed, err := counter.Authorize(context.Background(), "tenant-a", quota)
	if err != nil || !allowed {
		t.Fatalf("first Authorize() = %v, %v; want allowed", allowed, err)
	}
	allowed, err = counter.Authorize(context.Background(), "tenant-a", quota)
	if err != nil {
		t.Fatalf("second Authorize() error = %v", err)
	}
	if allowed {
		t.Fatal("second Authorize() exceeded minute rate but was allowed")
	}

	if err := counter.AddTokens(context.Background(), "tenant-a", 10); err != nil {
		t.Fatalf("AddTokens() error = %v", err)
	}
	now = now.Add(time.Minute)
	allowed, err = counter.Authorize(context.Background(), "tenant-a", quota)
	if err != nil {
		t.Fatalf("daily Authorize() error = %v", err)
	}
	if allowed {
		t.Fatal("Authorize() exceeded daily token budget but was allowed")
	}
}

func TestPolicyRedactorBuiltInAndTenantPatterns(t *testing.T) {
	redactor := NewPolicyRedactor()
	snapshot := workerSnapshot()
	snapshot.Tenant.Policy.RedactPatterns = []string{`account-[0-9]+`, `[invalid`}
	input := "account-123 token=abc Bearer xyz sk-12345678"
	got := redactor.Redact(snapshot, input)
	want := redactedValue + " " + redactedValue + " " + redactedValue + " " + redactedValue
	if got != want {
		t.Fatalf("Redact() = %q, want %q", got, want)
	}
}

func TestQuotaCounterValidation(t *testing.T) {
	if _, err := NewRedisQuotaCounter(nil); err == nil {
		t.Fatal("NewRedisQuotaCounter() accepted nil client")
	}
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	counter, err := NewRedisQuotaCounter(client)
	if err != nil {
		t.Fatalf("NewRedisQuotaCounter() error = %v", err)
	}
	if _, err := counter.Authorize(context.Background(), "", tenant.Quota{}); err == nil {
		t.Fatal("Authorize() accepted empty tenant ID")
	}
	if err := counter.AddTokens(context.Background(), "tenant-a", 0); err == nil {
		t.Fatal("AddTokens() accepted zero tokens")
	}
}
