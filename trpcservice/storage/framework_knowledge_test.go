package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/inmemory"
)

func TestScopedVectorStoreDelegatesWithStructuralTenantScope(t *testing.T) {
	delegate := inmemory.New()
	first, err := newScopedVectorStore(delegate, "tenant-a", "support", map[string]any{knowledgeParentMetadataKey: "faq"})
	if err != nil {
		t.Fatalf("newScopedVectorStore(first) error = %v", err)
	}
	second, err := newScopedVectorStore(delegate, "tenant-b", "support", map[string]any{knowledgeParentMetadataKey: "faq"})
	if err != nil {
		t.Fatalf("newScopedVectorStore(second) error = %v", err)
	}

	ctx := context.Background()
	if err := first.Add(ctx, &document.Document{ID: "chunk-1", Content: "tenant a refund"}, []float64{1, 0}); err != nil {
		t.Fatalf("first.Add() error = %v", err)
	}
	if err := second.Add(ctx, &document.Document{ID: "chunk-1", Content: "tenant b shipping"}, []float64{0, 1}); err != nil {
		t.Fatalf("second.Add() error = %v", err)
	}

	count, err := first.Count(ctx)
	if err != nil || count != 1 {
		t.Fatalf("first.Count() = %d, %v; want 1, nil", count, err)
	}
	doc, _, err := first.Get(ctx, "chunk-1")
	if err != nil {
		t.Fatalf("first.Get() error = %v", err)
	}
	if doc.ID != "chunk-1" || doc.Content != "tenant a refund" || doc.Metadata[knowledgeTenantMetadataKey] != "tenant-a" {
		t.Fatalf("first.Get() = %#v", doc)
	}
	result, err := second.Search(ctx, &vectorstore.SearchQuery{Vector: []float64{1, 0}, Limit: 10})
	if err != nil {
		t.Fatalf("second.Search() error = %v", err)
	}
	if len(result.Results) != 1 || result.Results[0].Document.Content != "tenant b shipping" || result.Results[0].Document.ID != "chunk-1" {
		t.Fatalf("second.Search() leaked tenant data: %#v", result)
	}
}

func TestScopedVectorStoreIsolatesApplicationsWithinTenant(t *testing.T) {
	delegate := inmemory.New()
	trailforge, err := newScopedVectorStore(delegate, "tenant-a", "trailforge", nil)
	if err != nil {
		t.Fatal(err)
	}
	otherBot, err := newScopedVectorStore(delegate, "tenant-a", "other-bot", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := trailforge.Add(ctx, &document.Document{ID: "policy", Content: "trailforge-only knowledge"}, []float64{1, 0}); err != nil {
		t.Fatal(err)
	}
	if err := otherBot.Add(ctx, &document.Document{ID: "policy", Content: "other-bot-only knowledge"}, []float64{0, 1}); err != nil {
		t.Fatal(err)
	}
	first, err := trailforge.Search(ctx, &vectorstore.SearchQuery{Vector: []float64{1, 0}, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	second, err := otherBot.Search(ctx, &vectorstore.SearchQuery{Vector: []float64{1, 0}, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Results) != 1 || first.Results[0].Document.Content != "trailforge-only knowledge" {
		t.Fatalf("trailforge results = %#v", first.Results)
	}
	if len(second.Results) != 1 || second.Results[0].Document.Content != "other-bot-only knowledge" {
		t.Fatalf("other-bot results = %#v", second.Results)
	}
}

func TestScopedKnowledgeDSNSetsRLSAndIngestFenceParameters(t *testing.T) {
	dsn, err := scopedKnowledgeDSN("postgres://user:pass@localhost/db", "tenant-a", "support", map[string]any{
		knowledgeJobMetadataKey: "job-1", knowledgeOwnerMetadataKey: "worker-1",
	})
	if err != nil {
		t.Fatalf("scopedKnowledgeDSN() error = %v", err)
	}
	if dsn == "" {
		t.Fatal("scopedKnowledgeDSN() returned an empty DSN")
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse scoped DSN: %v", err)
	}
	options := parsed.RuntimeParams["options"]
	for _, want := range []string{
		"-c role=trpc_tenant",
		"-c app.tenant_id=tenant-a",
		"-c app.app_code=support",
		"-c app.knowledge_ingest_job=job-1",
		"-c app.knowledge_ingest_owner=worker-1",
	} {
		if !strings.Contains(options, want) {
			t.Fatalf("startup options = %q, want %q", options, want)
		}
	}
}

func TestFrameworkBootstrapDDLOnlySkipsMigrationOwnedVectorSchema(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{"CREATE EXTENSION IF NOT EXISTS vector", true},
		{"CREATE TABLE IF NOT EXISTS knowledge_vectors (id TEXT PRIMARY KEY)", true},
		{"CREATE INDEX IF NOT EXISTS knowledge_vectors_embedding_idx ON knowledge_vectors USING hnsw (embedding vector_cosine_ops)", true},
		{"CREATE INDEX IF NOT EXISTS knowledge_vectors_content_fts_idx ON knowledge_vectors USING gin (to_tsvector('simple', content))", true},
		{"CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY)", false},
		{"DELETE FROM knowledge_vectors WHERE id=$1", false},
	}
	for _, test := range tests {
		t.Run(strings.Fields(test.query)[0]+"_"+strings.ReplaceAll(test.query, " ", "_"), func(t *testing.T) {
			if got := isFrameworkBootstrapDDL(test.query, knowledgeVectorTable); got != test.want {
				t.Fatalf("isFrameworkBootstrapDDL(%q) = %v, want %v", test.query, got, test.want)
			}
		})
	}
}
