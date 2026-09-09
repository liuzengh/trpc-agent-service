package storage_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// Tools exposes the standard memory tool set, and EnqueueAutoMemoryJob is an
// accepted no-op.
func TestPGMemoryToolsAndAutoMemoryJob(t *testing.T) {
	svc, _ := pgMemoryService(t)

	tools := svc.Tools()
	if len(tools) != 6 {
		t.Fatalf("want the 6 standard memory tools, got %d", len(tools))
	}
	seen := map[string]bool{}
	for _, tool := range tools {
		name := tool.Declaration().Name
		if name == "" {
			t.Fatal("every memory tool must declare a name")
		}
		if seen[name] {
			t.Fatalf("duplicate memory tool name %q", name)
		}
		seen[name] = true
	}
	for _, want := range []string{"memory_add", "memory_search", "memory_load", "memory_update", "memory_delete", "memory_clear"} {
		if !seen[want] {
			t.Fatalf("memory tool %q missing, got %v", want, seen)
		}
	}

	if err := svc.EnqueueAutoMemoryJob(context.Background(), &session.Session{}); err != nil {
		t.Fatalf("auto-memory job must be an accepted no-op: %v", err)
	}
}

// UpdateMemory covers all three row states: an active update, a reactivating
// update over a tombstone, and a not-found error. With an embedder configured
// the write also re-enqueues the vector job.
func TestPGMemoryUpdatePaths(t *testing.T) {
	_, pool := pgSessionService(t)
	ensureMemoryEmbeddingTable(t, pool)
	svc := storage.NewPGMemoryService(pool,
		storage.WithMemoryEmbedder(memFakeEmbedder{dim: storage.MemoryEmbeddingDimension}))
	t.Cleanup(func() { _ = svc.Close() })

	ctx := context.Background()
	key := memoryUserKey(t.Name())
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM memory_item WHERE user_id = $1`, key.UserID)
	})

	if err := svc.AddMemory(ctx, key, "更新前：用户喜欢喝茶", []string{"偏好"}); err != nil {
		t.Fatal(err)
	}
	waitForEmbeddings(t, pool, key.UserID, 1)
	found, err := svc.SearchMemories(ctx, key, "喝茶")
	if err != nil || len(found) != 1 {
		t.Fatalf("seed memory not found: %+v err=%v", found, err)
	}
	memKey := memory.Key{AppName: key.AppName, UserID: key.UserID, MemoryID: found[0].ID}

	// Active row: content and topics update in place.
	if err := svc.UpdateMemory(ctx, memKey, "更新后：用户喜欢喝咖啡", []string{"饮品"}); err != nil {
		t.Fatal(err)
	}
	found, err = svc.SearchMemories(ctx, key, "咖啡")
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 || found[0].Memory.Memory != "更新后：用户喜欢喝咖啡" {
		t.Fatalf("active update mismatch: %+v", found)
	}
	if len(found[0].Memory.Topics) != 1 || found[0].Memory.Topics[0] != "饮品" {
		t.Fatalf("topics must update with the content: %+v", found[0].Memory.Topics)
	}
	// The vector row is upserted by memory_id, never duplicated; the async
	// re-embed of the new content lands on the same row.
	var vecRows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM memory_embedding WHERE memory_id = $1`, memKey.MemoryID).Scan(&vecRows); err != nil {
		t.Fatal(err)
	}
	if vecRows != 1 {
		t.Fatalf("re-embed must upsert a single vector row, got %d", vecRows)
	}

	// A missing row (valid uuid, never written) is an error.
	missing := fmt.Sprintf("00000000-0000-0000-0000-%012d", time.Now().UnixNano()%1e12)
	if err := svc.UpdateMemory(ctx,
		memory.Key{AppName: key.AppName, UserID: key.UserID, MemoryID: missing}, "x", nil); err == nil {
		t.Fatal("updating a missing memory must fail")
	}

	// An invalid memory key is rejected before the query.
	if err := svc.UpdateMemory(ctx, memory.Key{}, "x", nil); err == nil {
		t.Fatal("an empty memory key must be rejected")
	}

	// Tombstone: UpdateMemory over a soft-deleted row revives it.
	if err := svc.DeleteMemory(ctx, memKey); err != nil {
		t.Fatal(err)
	}
	if entries, _ := svc.ReadMemories(ctx, key, 10); len(entries) != 0 {
		t.Fatalf("soft-deleted memory must be invisible, got %d", len(entries))
	}
	if err := svc.UpdateMemory(ctx, memKey, "复活后的记忆", []string{"偏好"}); err != nil {
		t.Fatalf("updating a tombstone must revive it: %v", err)
	}
	entries, err := svc.ReadMemories(ctx, key, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Memory.Memory != "复活后的记忆" {
		t.Fatalf("revived memory mismatch: %+v", entries)
	}
}
