package qdrant

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// The adapter is tested against a real Qdrant (the compose service or the
// drill container), for the same reason the MySQL tests use a real MySQL:
// the two things most likely to be wrong here — filter JSON and the
// collection's dimension contract — are exactly the things a stub would
// reproduce from the same misunderstanding. Start one with:
//
//	docker run -d --name tas-qdrant-test -p 6333:6333 qdrant/qdrant:v1.12.4
//	QDRANT_TEST_URL=http://127.0.0.1:6333 go test ./trpcservice/storage/qdrant/ -v
func newTestClient(t *testing.T) *Client {
	t.Helper()
	base := os.Getenv("QDRANT_TEST_URL")
	if base == "" {
		t.Skip("QDRANT_TEST_URL not set; skipping real-qdrant integration test")
	}
	name := fmt.Sprintf("tas_test_%d", time.Now().UnixNano())
	c, err := New(base, name)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	return c
}

func TestEnsureCollectionPinsDimension(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	if err := c.EnsureCollection(ctx, 8); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	// Idempotent: the second call must not try to recreate.
	if err := c.EnsureCollection(ctx, 8); err != nil {
		t.Fatalf("ensure again: %v", err)
	}
	// A different width is a loud refusal, not a silent re-creation.
	err := c.EnsureCollection(ctx, 9)
	if err == nil || !strings.Contains(err.Error(), "8-dimensional") {
		t.Fatalf("dimension mismatch error = %v", err)
	}
}

func TestSearchRequiresTenantAndFiltersByIt(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	if err := c.EnsureCollection(ctx, 4); err != nil {
		t.Fatal(err)
	}
	// Two tenants, same vector: only the filter decides who sees what.
	points := []Point{
		{ID: "00000000-0000-0000-0000-000000000001", Vector: []float32{1, 0, 0, 0},
			Payload: map[string]any{"tenant_id": "acme", "doc_id": 1, "chunk_ord": 0}},
		{ID: "00000000-0000-0000-0000-000000000002", Vector: []float32{0.9, 0.1, 0, 0},
			Payload: map[string]any{"tenant_id": "acme", "doc_id": 1, "chunk_ord": 1}},
		{ID: "00000000-0000-0000-0000-000000000003", Vector: []float32{1, 0, 0, 0},
			Payload: map[string]any{"tenant_id": "globex", "doc_id": 9, "chunk_ord": 0}},
	}
	if err := c.Upsert(ctx, points); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	// A filter without tenant_id is refused before it reaches the store.
	if _, err := c.Search(ctx, []float32{1, 0, 0, 0}, Filter{Must: []Match{{Key: "doc_id", Value: 1}}}, 10); err == nil {
		t.Fatal("search without a tenant filter was accepted")
	}

	hits, err := c.Search(ctx, []float32{1, 0, 0, 0},
		Filter{Must: []Match{{Key: "tenant_id", Value: "acme"}}}, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("acme hits = %d, want 2 (globex must be invisible)", len(hits))
	}
	for _, h := range hits {
		if h.Payload["tenant_id"] != "acme" {
			t.Fatalf("a cross-tenant hit leaked: %v", h.Payload)
		}
	}

	// A doc-level filter narrows within the tenant.
	hits, err = c.Search(ctx, []float32{1, 0, 0, 0}, Filter{Must: []Match{
		{Key: "tenant_id", Value: "acme"}, {Key: "chunk_ord", Value: 1},
	}}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Payload["chunk_ord"] != float64(1) {
		t.Fatalf("chunk filter hits = %+v", hits)
	}
}

func TestDeleteByFilterAndCount(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	if err := c.EnsureCollection(ctx, 2); err != nil {
		t.Fatal(err)
	}
	points := []Point{
		{ID: "10000000-0000-0000-0000-000000000001", Vector: []float32{1, 0},
			Payload: map[string]any{"tenant_id": "acme", "doc_id": 7}},
		{ID: "10000000-0000-0000-0000-000000000002", Vector: []float32{0, 1},
			Payload: map[string]any{"tenant_id": "globex", "doc_id": 7}},
	}
	if err := c.Upsert(ctx, points); err != nil {
		t.Fatal(err)
	}
	if n, err := c.Count(ctx, Filter{Must: []Match{{Key: "tenant_id", Value: "acme"}}}); err != nil || n != 1 {
		t.Fatalf("count acme = %d, %v", n, err)
	}
	if err := c.DeleteByFilter(ctx, Filter{Must: []Match{{Key: "tenant_id", Value: "acme"}, {Key: "doc_id", Value: 7}}}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if n, _ := c.Count(ctx, Filter{Must: []Match{{Key: "tenant_id", Value: "acme"}}}); n != 0 {
		t.Fatalf("acme count after delete = %d", n)
	}
	if n, _ := c.Count(ctx, Filter{Must: []Match{{Key: "tenant_id", Value: "globex"}}}); n != 1 {
		t.Fatalf("globex count changed by another tenant's delete: %d", n)
	}
	if err := c.DeleteByFilter(ctx, Filter{}); err == nil {
		t.Fatal("delete without a filter was accepted")
	}
	// Deleting again is idempotent.
	if err := c.DeleteByFilter(ctx, Filter{Must: []Match{{Key: "tenant_id", Value: "acme"}}}); err != nil {
		t.Fatalf("second delete: %v", err)
	}
}
