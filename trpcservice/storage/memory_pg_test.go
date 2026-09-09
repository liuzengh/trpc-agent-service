package storage_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"trpc.group/trpc-go/trpc-agent-go/memory"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

func pgMemoryService(t *testing.T) (*storage.PGMemoryService, *pgxpool.Pool) {
	t.Helper()
	_, pool := pgSessionService(t) // same PG + fixture tenant/app
	return storage.NewPGMemoryService(pool), pool
}

func memoryUserKey(suffix string) memory.UserKey {
	return memory.UserKey{AppName: testAppID, UserID: "u-mem-" + suffix}
}

func TestPGMemoryLifecycle(t *testing.T) {
	svc, pool := pgMemoryService(t)
	ctx := context.Background()
	key := memoryUserKey(t.Name())
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM memory_item WHERE user_id = $1`, key.UserID)
	})

	// Add two memories: one app-private, then verify reads.
	if err := svc.AddMemory(ctx, key, "用户偏好：喜欢简洁的回复", []string{"偏好"}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddMemory(ctx, key, "用户是 VIP 客户", []string{"身份"}); err != nil {
		t.Fatal(err)
	}
	entries, err := svc.ReadMemories(ctx, key, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("want 2 memories, got %d", len(entries))
	}

	// Idempotent add: same content again does not duplicate.
	if err := svc.AddMemory(ctx, key, "用户是 VIP 客户", nil); err != nil {
		t.Fatal(err)
	}
	entries, _ = svc.ReadMemories(ctx, key, 10)
	if len(entries) != 2 {
		t.Fatalf("duplicate add must not create a row, got %d", len(entries))
	}

	// Keyword search.
	found, err := svc.SearchMemories(ctx, key, "VIP")
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 || found[0].Memory.Memory != "用户是 VIP 客户" {
		t.Fatalf("search mismatch: %+v", found)
	}
	if found[0].Memory.Topics[0] != "身份" {
		t.Fatalf("topics lost: %+v", found[0].Memory)
	}

	// Update.
	memID := found[0].ID
	if err := svc.UpdateMemory(ctx, memory.Key{AppName: key.AppName, UserID: key.UserID, MemoryID: memID},
		"用户是铂金 VIP 客户", []string{"身份"}); err != nil {
		t.Fatal(err)
	}
	found, _ = svc.SearchMemories(ctx, key, "铂金")
	if len(found) != 1 {
		t.Fatalf("updated memory not found: %+v", found)
	}

	// Soft delete: invisible to reads; re-adding revives the tombstone.
	if err := svc.DeleteMemory(ctx, memory.Key{AppName: key.AppName, UserID: key.UserID, MemoryID: memID}); err != nil {
		t.Fatal(err)
	}
	entries, _ = svc.ReadMemories(ctx, key, 10)
	if len(entries) != 1 {
		t.Fatalf("soft-deleted memory must be invisible, got %d", len(entries))
	}
	if err := svc.AddMemory(ctx, key, "用户是铂金 VIP 客户", []string{"身份"}); err != nil {
		t.Fatal(err)
	}
	entries, _ = svc.ReadMemories(ctx, key, 10)
	if len(entries) != 2 {
		t.Fatalf("tombstone must be reactivated, got %d memories", len(entries))
	}
	var activeCount int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM memory_item WHERE user_id=$1 AND deleted_at IS NULL`, key.UserID).Scan(&activeCount); err != nil {
		t.Fatal(err)
	}
	if activeCount != 2 {
		t.Fatalf("want 2 active rows after revive, got %d", activeCount)
	}

	// Clear: everything soft-deleted.
	if err := svc.ClearMemories(ctx, key); err != nil {
		t.Fatal(err)
	}
	entries, _ = svc.ReadMemories(ctx, key, 10)
	if len(entries) != 0 {
		t.Fatalf("clear must empty the scope, got %d", len(entries))
	}
}

func TestPGMemoryTenantSharedLevel(t *testing.T) {
	svc, pool := pgMemoryService(t)
	ctx := context.Background()
	key := memoryUserKey(t.Name())
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM memory_item WHERE user_id = $1`, key.UserID)
	})

	// A tenant-shared row (app_id NULL) plus an app-private one: reads merge.
	if _, err := pool.Exec(ctx,
		`INSERT INTO memory_item (tenant_id, app_id, user_id, content) VALUES ($1, NULL, $2, '共享：公司年假 15 天')`,
		testTenantID, key.UserID); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddMemory(ctx, key, "私有：用户负责支付业务", nil); err != nil {
		t.Fatal(err)
	}
	entries, err := svc.ReadMemories(ctx, key, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("reads must merge private and shared levels, got %d", len(entries))
	}

	// Clear touches only the app-private row, not the tenant-shared one.
	if err := svc.ClearMemories(ctx, key); err != nil {
		t.Fatal(err)
	}
	entries, _ = svc.ReadMemories(ctx, key, 10)
	if len(entries) != 1 || entries[0].Memory.Memory != "共享：公司年假 15 天" {
		t.Fatalf("clear must keep tenant-shared memories: %+v", entries)
	}
}
