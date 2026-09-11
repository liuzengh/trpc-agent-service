package tenant

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/redis/go-redis/v9"
)

func tenantCacheSnapshot(t *testing.T) Snapshot {
	t.Helper()
	snapshot, err := newSnapshot(config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 3,
		Tools: config.ToolPolicy{Allowed: []string{"query_order"}},
	}, time.Date(2026, 9, 11, 1, 2, 3, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestRedisCacheRoundTripTTLDeleteAndCloneIsolation(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	cache, err := NewRedisCache(client)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	snapshot := tenantCacheSnapshot(t)
	if err := cache.Set(ctx, "tenant-key", snapshot, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, found, err := cache.Get(ctx, "tenant-key")
	if err != nil || !found || got.Checksum != snapshot.Checksum || got.Config.ConfigVersion != 3 {
		t.Fatalf("Get() = %#v, found=%v err=%v", got, found, err)
	}
	got.Config.Tools.Allowed[0] = "mutated"
	again, found, err := cache.Get(ctx, "tenant-key")
	if err != nil || !found || again.Config.Tools.Allowed[0] != "query_order" {
		t.Fatalf("cached snapshot mutated through caller: %#v, found=%v err=%v", again, found, err)
	}

	server.FastForward(time.Minute + time.Second)
	if _, found, err := cache.Get(ctx, "tenant-key"); err != nil || found {
		t.Fatalf("expired Get() found=%v err=%v", found, err)
	}
	if err := cache.Set(ctx, "tenant-key", snapshot, 0); err != nil {
		t.Fatal(err)
	}
	if err := cache.Delete(ctx, "tenant-key"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := cache.Get(ctx, "tenant-key"); err != nil || found {
		t.Fatalf("deleted Get() found=%v err=%v", found, err)
	}
}

func TestRedisCacheRejectsInvalidDependencyCorruptEntryAndRedisFailure(t *testing.T) {
	if _, err := NewRedisCache(nil); err == nil {
		t.Fatal("NewRedisCache(nil) succeeded")
	}
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	cache, err := NewRedisCache(client)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := client.Set(ctx, "broken", "not-json", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := cache.Get(ctx, "broken"); err == nil || !strings.Contains(err.Error(), "decode tenant configuration cache") {
		t.Fatalf("corrupt cache error = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := cache.Get(ctx, "missing"); err == nil || !strings.Contains(err.Error(), "read tenant configuration cache") {
		t.Fatalf("closed Redis Get error = %v", err)
	}
	if err := cache.Set(ctx, "key", tenantCacheSnapshot(t), time.Minute); err == nil || !strings.Contains(err.Error(), "write tenant configuration cache") {
		t.Fatalf("closed Redis Set error = %v", err)
	}
	if err := cache.Delete(ctx, "key"); err == nil || !strings.Contains(err.Error(), "delete tenant configuration cache") {
		t.Fatalf("closed Redis Delete error = %v", err)
	}
}
