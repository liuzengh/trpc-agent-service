package storage_test

import (
	"context"
	"errors"
	"hash/fnv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/memory"
)

// memFakeEmbedder produces deterministic bag-of-runes vectors: texts sharing
// runes are close, so semantic recall is testable without an embeddings API.
type memFakeEmbedder struct{ dim int }

func (f memFakeEmbedder) GetEmbedding(_ context.Context, text string) ([]float64, error) {
	v := make([]float64, f.dim)
	for _, r := range text {
		h := fnv.New32a()
		_, _ = h.Write([]byte(string(r)))
		v[int(h.Sum32())%f.dim]++
	}
	return v, nil
}

func (f memFakeEmbedder) GetEmbeddingWithUsage(ctx context.Context, text string) ([]float64, map[string]any, error) {
	v, err := f.GetEmbedding(ctx, text)
	return v, nil, err
}

func (f memFakeEmbedder) GetDimensions() int { return f.dim }

// errEmbedder always fails: the vector path must degrade to keyword search.
type errEmbedder struct{}

func (errEmbedder) GetEmbedding(context.Context, string) ([]float64, error) {
	return nil, errors.New("embedder unavailable")
}
func (errEmbedder) GetEmbeddingWithUsage(context.Context, string) ([]float64, map[string]any, error) {
	return nil, nil, errors.New("embedder unavailable")
}
func (errEmbedder) GetDimensions() int { return 0 }

// ensureMemoryEmbeddingTable deploys the vector table for the tests, so they
// also run against a schema without it.
func ensureMemoryEmbeddingTable(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS vector`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS memory_embedding (
			id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			memory_id  uuid         NOT NULL UNIQUE REFERENCES memory_item (id) ON DELETE CASCADE,
			tenant_id  uuid         NOT NULL,
			embedding  vector(1536) NOT NULL,
			created_at timestamptz  NOT NULL DEFAULT now()
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`CREATE INDEX IF NOT EXISTS idx_memory_embedding_hnsw
		 ON memory_embedding USING hnsw (embedding vector_cosine_ops)`); err != nil {
		t.Fatal(err)
	}
}

// waitForEmbeddings polls until n memories of the user have their vector
// backfilled (embedding generation is asynchronous by design).
func waitForEmbeddings(t *testing.T, pool *pgxpool.Pool, userID string, n int) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var got int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM memory_item WHERE user_id=$1 AND embedding_id IS NOT NULL`,
			userID).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got == n {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("embeddings for user %s not backfilled within 5s (want %d)", userID, n)
}

// Semantic recall end to end: write → async embedding → vector recall finds a
// paraphrase the keyword matcher cannot → soft delete removes both the memory
// and its vector.
func TestPGMemoryVectorRecall(t *testing.T) {
	_, pool := pgSessionService(t) // same PG + fixture tenant/app
	ensureMemoryEmbeddingTable(t, pool)
	svc := storage.NewPGMemoryService(pool,
		storage.WithMemoryEmbedder(memFakeEmbedder{dim: storage.MemoryEmbeddingDimension}))
	t.Cleanup(func() { _ = svc.Close() })

	ctx := context.Background()
	key := memoryUserKey(t.Name())
	t.Cleanup(func() {
		// ON DELETE CASCADE removes the memory_embedding rows with the items.
		_, _ = pool.Exec(ctx, `DELETE FROM memory_item WHERE user_id = $1`, key.UserID)
	})

	if err := svc.AddMemory(ctx, key, "用户每天下午喜欢喝手冲咖啡", []string{"偏好"}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddMemory(ctx, key, "公司差旅报销需要提前审批", []string{"制度"}); err != nil {
		t.Fatal(err)
	}

	// The embedding worker runs in the background: poll for the backfill.
	waitForEmbeddings(t, pool, key.UserID, 2)
	var vecCount int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM memory_embedding e JOIN memory_item m ON m.id = e.memory_id
		 WHERE m.user_id = $1`, key.UserID).Scan(&vecCount); err != nil {
		t.Fatal(err)
	}
	if vecCount != 2 {
		t.Fatalf("want 2 embedding rows, got %d", vecCount)
	}

	// Paraphrase query: ILIKE would match nothing (the query is not a
	// substring of either content); cosine distance must rank the coffee
	// memory first.
	found, err := svc.SearchMemories(ctx, key, "用户喜欢什么口味的咖啡")
	if err != nil {
		t.Fatal(err)
	}
	if len(found) == 0 || found[0].Memory.Memory != "用户每天下午喜欢喝手冲咖啡" {
		t.Fatalf("vector recall must rank the coffee memory first: %+v", found)
	}

	// Soft delete: the memory leaves recall and its vector row is gone.
	coffeeID := found[0].ID
	if err := svc.DeleteMemory(ctx, memory.Key{AppName: key.AppName, UserID: key.UserID, MemoryID: coffeeID}); err != nil {
		t.Fatal(err)
	}
	found, err = svc.SearchMemories(ctx, key, "用户喜欢什么口味的咖啡")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range found {
		if e.ID == coffeeID {
			t.Fatalf("soft-deleted memory must not be recalled: %+v", e)
		}
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM memory_embedding WHERE memory_id = $1`, coffeeID).Scan(&vecCount); err != nil {
		t.Fatal(err)
	}
	if vecCount != 0 {
		t.Fatalf("delete must remove the embedding row, got %d", vecCount)
	}
}

// A failing embedder degrades search to keyword matching instead of erroring.
func TestPGMemorySearchFallbackOnEmbedderError(t *testing.T) {
	_, pool := pgSessionService(t)
	ensureMemoryEmbeddingTable(t, pool)
	svc := storage.NewPGMemoryService(pool, storage.WithMemoryEmbedder(errEmbedder{}))
	t.Cleanup(func() { _ = svc.Close() })

	ctx := context.Background()
	key := memoryUserKey(t.Name())
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM memory_item WHERE user_id = $1`, key.UserID)
	})

	if err := svc.AddMemory(ctx, key, "回退场景：用户偏好关键词甲", nil); err != nil {
		t.Fatal(err)
	}
	found, err := svc.SearchMemories(ctx, key, "关键词甲")
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 || found[0].Memory.Memory != "回退场景：用户偏好关键词甲" {
		t.Fatalf("embedder failure must fall back to keyword search: %+v", found)
	}
}

// Without an embedder the service stays on keyword search (no vector table
// needed at all).
func TestPGMemorySearchFallbackWithoutEmbedder(t *testing.T) {
	svc, pool := pgMemoryService(t)
	ctx := context.Background()
	key := memoryUserKey(t.Name())
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM memory_item WHERE user_id = $1`, key.UserID)
	})

	if err := svc.AddMemory(ctx, key, "无向量配置：用户偏好关键词乙", nil); err != nil {
		t.Fatal(err)
	}
	found, err := svc.SearchMemories(ctx, key, "关键词乙")
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 {
		t.Fatalf("keyword search without embedder must still work: %+v", found)
	}
}

// A memory id from another tenant must not be deletable. The soft delete
// carries the tenant predicate, so it matches nothing — and the vector row has
// to survive that, otherwise anyone holding the id could destroy a recall they
// were never authorized to read.
func TestPGMemoryDeleteGuardsForeignMemory(t *testing.T) {
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

	if err := svc.AddMemory(ctx, key, "隔离用例：用户偏好关键词丙", []string{"偏好"}); err != nil {
		t.Fatal(err)
	}
	waitForEmbeddings(t, pool, key.UserID, 1)
	found, err := svc.SearchMemories(ctx, key, "关键词丙")
	if err != nil || len(found) != 1 {
		t.Fatalf("seed memory not found: %+v err=%v", found, err)
	}
	memoryID := found[0].ID

	// The same id resolved through an app of a second tenant.
	if err := svc.DeleteMemory(ctx,
		memory.Key{AppName: foreignTenantApp(t, pool), UserID: key.UserID, MemoryID: memoryID}); err != nil {
		t.Fatal(err)
	}

	entries, err := svc.ReadMemories(ctx, key, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("a delete from another tenant must not touch the memory, got %d", len(entries))
	}
	var vecRows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM memory_embedding WHERE memory_id = $1`, memoryID).Scan(&vecRows); err != nil {
		t.Fatal(err)
	}
	if vecRows != 1 {
		t.Fatalf("a delete from another tenant must not remove the embedding row, got %d", vecRows)
	}
}

// foreignTenantApp inserts a second tenant with its own app and returns the
// app id: resolving through it yields a different tenant_id, which is what the
// isolation predicates compare against.
func foreignTenantApp(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	ctx := context.Background()
	const tenantID = "00000000-0000-0000-0000-0000000000bb"
	const appID = "00000000-0000-0000-0000-0000000001bb"
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenant (id, name, status) VALUES ($1, 'pg-memory-foreign', 'active')
		 ON CONFLICT (id) DO NOTHING`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO agent_app (id, tenant_id, name, agent_type, config, version, status)
		 VALUES ($1, $2, 'pg-memory-foreign', 'llm', '{}', 1, 'published') ON CONFLICT DO NOTHING`,
		appID, tenantID); err != nil {
		t.Fatal(err)
	}
	return appID
}
