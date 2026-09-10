package inmemory_test

import (
	"context"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
)

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

func TestCapabilitiesAreTenantScopedAndDefensive(t *testing.T) {
	store := inmemory.New()
	defer store.Close()
	value, err := store.PutMemory(context.Background(), runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "m1", UserID: "u1", Content: "likes coffee", Metadata: map[string]any{"kind": "fact"}, Embedding: []float64{1, 0}})
	if err != nil || value.Version != 1 {
		t.Fatalf("PutMemory = %+v, %v", value, err)
	}
	value.Metadata["kind"] = "changed"
	got, err := store.GetMemory(context.Background(), "tenant-a", "m1")
	if err != nil || got.Metadata["kind"] != "fact" {
		t.Fatalf("memory copy = %+v, %v", got, err)
	}
	if _, err := store.GetMemory(context.Background(), "tenant-b", "m1"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("cross tenant memory = %v", err)
	}
	if _, err := store.PutKnowledge(context.Background(), runtimestorage.KnowledgeDocument{TenantID: "tenant-a", DocumentID: "doc", Content: "coffee", Embedding: []float64{1, 0}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutKnowledge(context.Background(), runtimestorage.KnowledgeDocument{TenantID: "tenant-b", DocumentID: "doc", Content: "tea", Embedding: []float64{0, 1}}); err != nil {
		t.Fatal(err)
	}
	results, err := store.SearchKnowledge(context.Background(), "tenant-a", []float64{1, 0}, 10)
	if err != nil || len(results) != 1 || results[0].Document.TenantID != "tenant-a" {
		t.Fatalf("tenant knowledge search = %+v, %v", results, err)
	}
}

func TestAsyncMemoryIndexAndObjects(t *testing.T) {
	store := inmemory.New()
	defer store.Close()
	value, err := store.PutMemory(context.Background(), runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "m-index", UserID: "u", Content: "indexed", Embedding: []float64{1, 0}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := store.WaitForMemoryIndex(ctx, "tenant-a", "m-index", value.Version); err != nil {
		t.Fatal(err)
	}
	indexed, err := store.SearchVectors(ctx, "tenant-a", []float64{1, 0}, 1)
	if err != nil || len(indexed) != 1 || indexed[0].Record.DocumentID != "m-index" {
		t.Fatalf("async vector index = %+v, %v", indexed, err)
	}
	info, err := store.PutObject(context.Background(), "tenant-a", "a/file.txt", strings.NewReader("hello"), "text/plain")
	if err != nil || info.Size != 5 {
		t.Fatalf("PutObject = %+v, %v", info, err)
	}
	body, gotInfo, err := store.GetObject(context.Background(), "tenant-a", "a/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	data, _ := io.ReadAll(body)
	if string(data) != "hello" || gotInfo.ETag != info.ETag {
		t.Fatalf("object = %q, %+v", data, gotInfo)
	}
	if _, _, err := store.GetObject(context.Background(), "tenant-b", "a/file.txt"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("cross tenant object = %v", err)
	}
}

func TestSummaryOrderingAndSharedBackendVisibility(t *testing.T) {
	backend := inmemory.NewBackend()
	nodeA, nodeB := inmemory.NewWithBackend(backend), inmemory.NewWithBackend(backend)
	defer nodeA.Close()
	defer nodeB.Close()
	defer backend.Close()
	_, err := nodeA.CreateSession(context.Background(), "tenant-a", "session-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nodeA.PutSummary(context.Background(), runtimestorage.SummaryRecord{TenantID: "tenant-a", SessionID: "session-1", FilterKey: "default", Text: "new", EventSeq: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := nodeB.PutSummary(context.Background(), runtimestorage.SummaryRecord{TenantID: "tenant-a", SessionID: "session-1", FilterKey: "default", Text: "stale", EventSeq: 1}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("stale summary = %v", err)
	}
	got, err := nodeB.GetSummary(context.Background(), "tenant-a", "session-1", "default")
	if err != nil || got.Text != "new" || got.EventSeq != 2 {
		t.Fatalf("shared summary = %+v, %v", got, err)
	}
	if _, err := nodeB.GetSummary(context.Background(), "tenant-b", "session-1", "default"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("cross tenant summary = %v", err)
	}
}

func TestSharedBackendViewCloseDoesNotStopOtherViews(t *testing.T) {
	backend := inmemory.NewBackend()
	nodeA := inmemory.NewWithBackend(backend)
	nodeB := inmemory.NewWithBackend(backend)
	if _, err := nodeA.PutMemory(context.Background(), runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "before-close", UserID: "user", Content: "durable", Embedding: []float64{1, 0}}); err != nil {
		t.Fatal(err)
	}
	if err := nodeA.Close(); err != nil {
		t.Fatal(err)
	}
	value, err := nodeB.PutMemory(context.Background(), runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "after-close", UserID: "user", Content: "still running", Embedding: []float64{1, 0}})
	if err != nil {
		t.Fatalf("PutMemory through remaining view = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := nodeB.WaitForMemoryIndex(ctx, "tenant-a", value.MemoryID, value.Version); err != nil {
		t.Fatalf("remaining view index = %v", err)
	}
	if err := nodeB.Close(); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := nodeB.PutMemory(context.Background(), runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "closed", UserID: "user", Content: "closed"}); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("PutMemory after final view close = %v", err)
	}
}

func TestCapabilitiesRejectCanceledContext(t *testing.T) {
	store := inmemory.New()
	defer store.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", UserID: "user", Content: "x"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("PutMemory = %v", err)
	}
	if _, err := store.SearchKnowledge(ctx, "tenant-a", []float64{1}, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("SearchKnowledge = %v", err)
	}
	if err := store.DeleteVector(ctx, "tenant-a", "doc"); !errors.Is(err, context.Canceled) {
		t.Fatalf("DeleteVector = %v", err)
	}
}

func TestMemoryIndexUsesDurableRecordAndSeparateVectorNamespaces(t *testing.T) {
	store := inmemory.New()
	defer store.Close()

	memory, err := store.PutMemory(context.Background(), runtimestorage.MemoryInput{
		TenantID: "tenant-a", MemoryID: "shared-id", UserID: "user", Content: "durable memory", Embedding: []float64{1, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := store.WaitForMemoryIndex(ctx, "tenant-a", memory.MemoryID, memory.Version); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteVector(context.Background(), "tenant-a", memory.MemoryID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutKnowledge(context.Background(), runtimestorage.KnowledgeDocument{
		TenantID: "tenant-a", DocumentID: "shared-id", Content: "knowledge document", Embedding: []float64{1, 0},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueMemoryIndex(context.Background(), runtimestorage.MemoryRecord{
		TenantID: "tenant-a", MemoryID: memory.MemoryID, Version: memory.Version, Content: "forged", Embedding: []float64{0, 1},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.WaitForMemoryIndex(ctx, "tenant-a", memory.MemoryID, memory.Version); err != nil {
		t.Fatal(err)
	}
	results, err := store.SearchVectors(ctx, "tenant-a", []float64{1, 0}, 10)
	if err != nil || len(results) != 2 {
		t.Fatalf("namespaced vectors = %+v, %v", results, err)
	}
	seen := map[runtimestorage.VectorSource]string{}
	for _, result := range results {
		seen[result.Record.Source] = result.Record.Content
	}
	if seen[runtimestorage.VectorSourceMemory] != "durable memory" || seen[runtimestorage.VectorSourceKnowledge] != "knowledge document" {
		t.Fatalf("vector contents = %+v", seen)
	}
}

func TestInMemoryVectorUpsertRejectsStaleVersion(t *testing.T) {
	store := inmemory.New()
	defer store.Close()
	value := runtimestorage.VectorRecord{TenantID: "tenant-a", DocumentID: "doc", Version: 2, Embedding: []float64{1, 0}}
	if err := store.UpsertVector(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	value.Version = 1
	value.Embedding = []float64{0, 1}
	if err := store.UpsertVector(context.Background(), value); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("stale vector upsert = %v", err)
	}
}

//nolint:gocyclo // one contract test intentionally exercises the complete capability surface.
func TestInMemoryRemainingCapabilityContracts(t *testing.T) {
	store := inmemory.New()
	defer store.Close()
	ctx := context.Background()

	first, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "m-a", UserID: "user", Content: "coffee tea"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "m-b", UserID: "user", Content: "coffee"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "m-other", UserID: "other", Content: "coffee"}); err != nil {
		t.Fatal(err)
	}
	memories, err := store.ListMemories(ctx, "tenant-a", "user", 1)
	if err != nil || len(memories) != 1 {
		t.Fatalf("ListMemories = %+v, %v", memories, err)
	}
	results, err := store.SearchMemories(ctx, "tenant-a", "user", "coffee tea", 10)
	if err != nil || len(results) != 2 || results[0].Memory.MemoryID != first.MemoryID || results[0].Score != 1 {
		t.Fatalf("SearchMemories = %+v, %v", results, err)
	}
	if err := store.DeleteMemory(ctx, "tenant-a", first.MemoryID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetMemory(ctx, "tenant-a", first.MemoryID); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("deleted memory = %v", err)
	}

	if _, err := store.CreateSession(ctx, "tenant-a", "session", nil); err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueSummary(ctx, runtimestorage.SummaryRecord{TenantID: "tenant-a", SessionID: "session", FilterKey: "default", Text: "summary", EventSeq: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetSummary(ctx, "tenant-a", "session", "default"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetKnowledge(ctx, "tenant-a", "missing"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing knowledge = %v", err)
	}
	if _, err := store.PutKnowledge(ctx, runtimestorage.KnowledgeDocument{TenantID: "tenant-a", DocumentID: "doc-a", Content: "knowledge", Metadata: map[string]any{"a": true}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetKnowledge(ctx, "tenant-a", "doc-a"); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteKnowledge(ctx, "tenant-a", "doc-a"); err != nil {
		t.Fatal(err)
	}

	artifact := runtimestorage.ArtifactRecord{TenantID: "tenant-a", ArtifactID: "artifact", Content: []byte("data")}
	if _, err := store.PutArtifact(ctx, artifact); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetArtifact(ctx, "tenant-a", artifact.ArtifactID); err != nil {
		t.Fatal(err)
	}
	if listed, err := store.ListArtifacts(ctx, "tenant-a", ""); err != nil || len(listed) != 1 {
		t.Fatalf("ListArtifacts = %+v, %v", listed, err)
	}
	if err := store.DeleteArtifact(ctx, "tenant-a", artifact.ArtifactID); err != nil {
		t.Fatal(err)
	}

	audit, err := store.AppendAudit(ctx, runtimestorage.AuditRecord{TenantID: "tenant-a", AuditID: "audit", EventType: "memory.updated", Payload: map[string]any{"ok": true}})
	if err != nil || audit.AuditID != "audit" {
		t.Fatalf("AppendAudit = %+v, %v", audit, err)
	}
	if rows, err := store.ListAudit(ctx, "tenant-a", time.Time{}, 10); err != nil || len(rows) != 1 {
		t.Fatalf("ListAudit = %+v, %v", rows, err)
	}

	if _, err := store.PutObject(ctx, "tenant-a", "key", strings.NewReader("object"), "text/plain"); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteObject(ctx, "tenant-a", "key"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.GetObject(ctx, "tenant-a", "key"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("deleted object = %v", err)
	}
}

//nolint:gocyclo // branch coverage for the tenant-scoped contract is table-shaped but explicit.
func TestInMemoryCapabilityValidationAndVersionBranches(t *testing.T) {
	store := inmemory.New()
	defer store.Close()
	ctx := context.Background()
	if _, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "m", UserID: "user", Content: "first"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "m", UserID: "user", Content: "second"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetMemory(ctx, "tenant-a", "missing"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing memory = %v", err)
	}
	if _, err := store.ListMemories(ctx, "", "user", 1); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid ListMemories = %v", err)
	}
	if _, err := store.SearchMemories(ctx, "tenant-a", "user", "missing", 1); err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueMemoryIndex(ctx, runtimestorage.MemoryRecord{TenantID: "tenant-a", MemoryID: "m", Version: 1}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("stale EnqueueMemoryIndex = %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()
	if err := store.WaitForMemoryIndex(waitCtx, "tenant-a", "missing", 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing index wait = %v", err)
	}
	if _, err := store.CreateSession(ctx, "tenant-a", "session", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutSummary(ctx, runtimestorage.SummaryRecord{TenantID: "tenant-a", SessionID: "session", Text: "new", EventSeq: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutSummary(ctx, runtimestorage.SummaryRecord{TenantID: "tenant-a", SessionID: "session", Text: "old", EventSeq: 1}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("stale summary = %v", err)
	}
	if _, err := store.GetSummary(ctx, "tenant-a", "session", "missing"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing summary = %v", err)
	}
	if _, err := store.PutKnowledge(ctx, runtimestorage.KnowledgeDocument{TenantID: "tenant-a", DocumentID: "doc", Content: "text"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutKnowledge(ctx, runtimestorage.KnowledgeDocument{TenantID: "tenant-a", DocumentID: "doc", Content: "updated", Embedding: []float64{1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SearchKnowledge(ctx, "tenant-a", []float64{1, 0}, 1); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteKnowledge(ctx, "tenant-a", "missing"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing knowledge delete = %v", err)
	}
	if _, err := store.PutArtifact(ctx, runtimestorage.ArtifactRecord{TenantID: "tenant-a", ArtifactID: "a", Content: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutArtifact(ctx, runtimestorage.ArtifactRecord{TenantID: "tenant-a", ArtifactID: "a", Content: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteArtifact(ctx, "tenant-a", "missing"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing artifact delete = %v", err)
	}
	if _, err := store.AppendAudit(ctx, runtimestorage.AuditRecord{TenantID: "tenant-a", AuditID: "audit", EventType: "event", Payload: map[string]any{"v": 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendAudit(ctx, runtimestorage.AuditRecord{TenantID: "tenant-a", AuditID: "audit", EventType: "different"}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("conflicting audit = %v", err)
	}
	if err := store.DeleteObject(ctx, "tenant-a", "missing"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing object delete = %v", err)
	}
}

func TestInMemoryPutMemoryReportsClosedIndexer(t *testing.T) {
	store := inmemory.New()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := store.PutMemory(context.Background(), runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "closed", UserID: "user", Content: "durable"})
	if !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("PutMemory after Close = %v", err)
	}
	if err := store.WaitForMemoryIndex(context.Background(), "tenant-a", "closed", 1); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("WaitForMemoryIndex after Close = %v", err)
	}
}

func TestInMemoryCapabilityErrorBranches(t *testing.T) {
	store := inmemory.New()
	ctx := context.Background()
	if _, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "bad", UserID: "user", Content: "x", Metadata: map[string]any{"function": func() {}}}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid memory metadata = %v", err)
	}
	if err := store.EnqueueMemoryIndex(ctx, runtimestorage.MemoryRecord{TenantID: "tenant-a", MemoryID: "missing", Version: 1}); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing memory index = %v", err)
	}
	if _, err := store.PutSummary(ctx, runtimestorage.SummaryRecord{TenantID: "tenant-a", SessionID: "missing", Text: "x"}); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing summary session = %v", err)
	}
	if _, err := store.SearchKnowledge(ctx, "tenant-a", nil, 1); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid knowledge search = %v", err)
	}
	if _, err := store.AppendAudit(ctx, runtimestorage.AuditRecord{TenantID: "tenant-a", AuditID: "audit", EventType: "event", Payload: map[string]any{"function": func() {}}}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid audit payload = %v", err)
	}
	if _, err := store.AppendAudit(ctx, runtimestorage.AuditRecord{TenantID: "tenant-a", AuditID: "audit", EventType: "event", Payload: map[string]any{"ok": true}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendAudit(ctx, runtimestorage.AuditRecord{TenantID: "tenant-a", AuditID: "audit", EventType: "event", Payload: map[string]any{"ok": true}}); err != nil {
		t.Fatalf("idempotent audit = %v", err)
	}
	if _, err := store.ListAudit(ctx, "tenant-a", time.Now().UTC().Add(time.Hour), 1); err != nil {
		t.Fatal(err)
	}
	value, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "closed", UserID: "user", Content: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueMemoryIndex(ctx, value); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("closed memory index = %v", err)
	}
}

func TestInMemoryCapabilityCanceledContextComplete(t *testing.T) {
	store := inmemory.New()
	defer store.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cases := []struct {
		name string
		call func() error
	}{
		{"GetMemory", func() error { _, err := store.GetMemory(ctx, "tenant-a", "memory"); return err }},
		{"ListMemories", func() error { _, err := store.ListMemories(ctx, "tenant-a", "user", 1); return err }},
		{"SearchMemories", func() error { _, err := store.SearchMemories(ctx, "tenant-a", "user", "query", 1); return err }},
		{"DeleteMemory", func() error { return store.DeleteMemory(ctx, "tenant-a", "memory") }},
		{"EnqueueMemoryIndex", func() error {
			return store.EnqueueMemoryIndex(ctx, runtimestorage.MemoryRecord{TenantID: "tenant-a", MemoryID: "memory", Version: 1})
		}},
		{"WaitForMemoryIndex", func() error { return store.WaitForMemoryIndex(ctx, "tenant-a", "memory", 1) }},
		{"PutSummary", func() error {
			_, err := store.PutSummary(ctx, runtimestorage.SummaryRecord{TenantID: "tenant-a", SessionID: "session", Text: "summary"})
			return err
		}},
		{"GetSummary", func() error { _, err := store.GetSummary(ctx, "tenant-a", "session", "default"); return err }},
		{"EnqueueSummary", func() error {
			return store.EnqueueSummary(ctx, runtimestorage.SummaryRecord{TenantID: "tenant-a", SessionID: "session", Text: "summary"})
		}},
		{"PutKnowledge", func() error {
			_, err := store.PutKnowledge(ctx, runtimestorage.KnowledgeDocument{TenantID: "tenant-a", DocumentID: "document", Content: "content"})
			return err
		}},
		{"GetKnowledge", func() error { _, err := store.GetKnowledge(ctx, "tenant-a", "document"); return err }},
		{"SearchKnowledge", func() error { _, err := store.SearchKnowledge(ctx, "tenant-a", []float64{1}, 1); return err }},
		{"DeleteKnowledge", func() error { return store.DeleteKnowledge(ctx, "tenant-a", "document") }},
		{"PutArtifact", func() error {
			_, err := store.PutArtifact(ctx, runtimestorage.ArtifactRecord{TenantID: "tenant-a", ArtifactID: "artifact", Content: []byte("x")})
			return err
		}},
		{"GetArtifact", func() error { _, err := store.GetArtifact(ctx, "tenant-a", "artifact"); return err }},
		{"ListArtifacts", func() error { _, err := store.ListArtifacts(ctx, "tenant-a", ""); return err }},
		{"DeleteArtifact", func() error { return store.DeleteArtifact(ctx, "tenant-a", "artifact") }},
		{"AppendAudit", func() error {
			_, err := store.AppendAudit(ctx, runtimestorage.AuditRecord{TenantID: "tenant-a", EventType: "event"})
			return err
		}},
		{"ListAudit", func() error { _, err := store.ListAudit(ctx, "tenant-a", time.Time{}, 1); return err }},
		{"UpsertVector", func() error {
			return store.UpsertVector(ctx, runtimestorage.VectorRecord{TenantID: "tenant-a", DocumentID: "document", Embedding: []float64{1}})
		}},
		{"SearchVectors", func() error { _, err := store.SearchVectors(ctx, "tenant-a", []float64{1}, 1); return err }},
		{"DeleteVector", func() error { return store.DeleteVector(ctx, "tenant-a", "document") }},
		{"PutObject", func() error {
			_, err := store.PutObject(ctx, "tenant-a", "object", strings.NewReader("x"), "text/plain")
			return err
		}},
		{"GetObject", func() error { _, _, err := store.GetObject(ctx, "tenant-a", "object"); return err }},
		{"DeleteObject", func() error { return store.DeleteObject(ctx, "tenant-a", "object") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.call(), context.Canceled) {
				t.Fatalf("%s did not preserve cancellation", tc.name)
			}
		})
	}
}

//nolint:gocyclo // table-driven contract coverage intentionally exercises validation and ordering branches.
func TestInMemoryCapabilityValidationAndOrderingBranches(t *testing.T) {
	store := inmemory.New()
	defer store.Close()
	ctx := context.Background()
	invalid := []struct {
		name string
		call func() error
	}{
		{"PutMemory", func() error {
			_, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "", UserID: "user", Content: "content"})
			return err
		}},
		{"GetMemory", func() error { _, err := store.GetMemory(ctx, "", "memory"); return err }},
		{"ListMemories", func() error { _, err := store.ListMemories(ctx, "", "user", 1); return err }},
		{"SearchMemories", func() error { _, err := store.SearchMemories(ctx, "", "user", "query", 1); return err }},
		{"DeleteMemory", func() error { return store.DeleteMemory(ctx, "", "memory") }},
		{"EnqueueMemoryIndex", func() error {
			return store.EnqueueMemoryIndex(ctx, runtimestorage.MemoryRecord{TenantID: "", MemoryID: "memory", Version: 1})
		}},
		{"WaitForMemoryIndex", func() error { return store.WaitForMemoryIndex(ctx, "", "memory", 1) }},
		{"PutSummary", func() error {
			_, err := store.PutSummary(ctx, runtimestorage.SummaryRecord{TenantID: "", SessionID: "session", Text: "summary"})
			return err
		}},
		{"GetSummary", func() error { _, err := store.GetSummary(ctx, "", "session", "default"); return err }},
		{"PutKnowledge", func() error {
			_, err := store.PutKnowledge(ctx, runtimestorage.KnowledgeDocument{TenantID: "", DocumentID: "document", Content: "content"})
			return err
		}},
		{"GetKnowledge", func() error { _, err := store.GetKnowledge(ctx, "", "document"); return err }},
		{"SearchKnowledge", func() error { _, err := store.SearchKnowledge(ctx, "", []float64{1}, 1); return err }},
		{"DeleteKnowledge", func() error { return store.DeleteKnowledge(ctx, "", "document") }},
		{"PutArtifact", func() error {
			_, err := store.PutArtifact(ctx, runtimestorage.ArtifactRecord{TenantID: "", ArtifactID: "artifact", Content: []byte("x")})
			return err
		}},
		{"GetArtifact", func() error { _, err := store.GetArtifact(ctx, "", "artifact"); return err }},
		{"ListArtifacts", func() error { _, err := store.ListArtifacts(ctx, "", ""); return err }},
		{"DeleteArtifact", func() error { return store.DeleteArtifact(ctx, "", "artifact") }},
		{"AppendAudit", func() error {
			_, err := store.AppendAudit(ctx, runtimestorage.AuditRecord{TenantID: "", EventType: "event"})
			return err
		}},
		{"ListAudit", func() error { _, err := store.ListAudit(ctx, "", time.Time{}, 1); return err }},
		{"UpsertVector", func() error {
			return store.UpsertVector(ctx, runtimestorage.VectorRecord{TenantID: "", DocumentID: "document", Embedding: []float64{1}})
		}},
		{"SearchVectors", func() error { _, err := store.SearchVectors(ctx, "", []float64{1}, 1); return err }},
		{"DeleteVector", func() error { return store.DeleteVector(ctx, "", "document") }},
		{"PutObject", func() error {
			_, err := store.PutObject(ctx, "", "object", strings.NewReader("x"), "text/plain")
			return err
		}},
		{"GetObject", func() error { _, _, err := store.GetObject(ctx, "", "object"); return err }},
		{"DeleteObject", func() error { return store.DeleteObject(ctx, "", "object") }},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.call(), runtimestorage.ErrInvalid) {
				t.Fatalf("%s accepted invalid input", tc.name)
			}
		})
	}
	if _, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", UserID: "user", Content: "generated id"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "tie-a", UserID: "user", Content: "same score"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "tie-b", UserID: "user", Content: "same score"}); err != nil {
		t.Fatal(err)
	}
	if values, err := store.SearchMemories(ctx, "tenant-a", "user", "same", 0); err != nil || len(values) != 2 || values[0].Memory.MemoryID != "tie-a" {
		t.Fatalf("memory tie ordering = %+v, %v", values, err)
	}
	if _, err := store.PutKnowledge(ctx, runtimestorage.KnowledgeDocument{TenantID: "tenant-a", DocumentID: "zero", Content: "zero", Embedding: []float64{0}}); err != nil {
		t.Fatal(err)
	}
	if values, err := store.SearchKnowledge(ctx, "tenant-a", []float64{1}, 0); err != nil || len(values) != 0 {
		t.Fatalf("zero-norm knowledge = %+v, %v", values, err)
	}
	if err := store.UpsertVector(ctx, runtimestorage.VectorRecord{TenantID: "tenant-a", Source: runtimestorage.VectorSourceKnowledge, DocumentID: "same", Embedding: []float64{1}}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertVector(ctx, runtimestorage.VectorRecord{TenantID: "tenant-a", Source: runtimestorage.VectorSourceMemory, DocumentID: "same", Embedding: []float64{1}}); err != nil {
		t.Fatal(err)
	}
	if values, err := store.SearchVectors(ctx, "tenant-a", []float64{1}, 1); err != nil || len(values) != 1 {
		t.Fatalf("vector limit = %+v, %v", values, err)
	}
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, id := range []string{"audit-b", "audit-a"} {
		if _, err := store.AppendAudit(ctx, runtimestorage.AuditRecord{TenantID: "tenant-a", AuditID: id, EventType: "event", OccurredAt: when}); err != nil {
			t.Fatal(err)
		}
	}
	if values, err := store.ListAudit(ctx, "tenant-a", time.Time{}, 1); err != nil || len(values) != 1 || values[0].AuditID != "audit-a" {
		t.Fatalf("audit ordering = %+v, %v", values, err)
	}
	if _, err := store.PutObject(ctx, "tenant-a", "broken", failingReader{}, ""); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("failing object reader = %v", err)
	}
}

func TestInMemoryRejectsNonFiniteAndOversizedFields(t *testing.T) {
	store := inmemory.New()
	defer store.Close()
	ctx := context.Background()
	long := strings.Repeat("x", 300)
	if _, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: long, UserID: "user", Content: "x"}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", UserID: "user", Content: "x", Embedding: []float64{math.NaN()}}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := store.PutKnowledge(ctx, runtimestorage.KnowledgeDocument{TenantID: "tenant-a", DocumentID: "doc", Content: "x", Digest: long}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatal(err)
	}
	if err := store.UpsertVector(ctx, runtimestorage.VectorRecord{TenantID: "tenant-a", DocumentID: "doc", Embedding: []float64{math.Inf(1)}}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := store.PutObject(ctx, "tenant-a", strings.Repeat("x", 1100), strings.NewReader("x"), ""); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatal(err)
	}
}
