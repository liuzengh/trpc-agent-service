package storage

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

func TestFrameworkPGVectorEnforcesRLSVisibilityAndIngestFence(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is required")
	}
	ctx := context.Background()
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	tenantID := "rag-" + uuid.NewString()
	otherTenantID := "rag-" + uuid.NewString()
	appCode := "support"
	documentID := "refund-policy"
	jobID := uuid.NewString()
	owner := uuid.NewString()
	for _, id := range []string{tenantID, otherTenantID} {
		if _, err := database.ExecContext(ctx, "INSERT INTO tenants (id) VALUES ($1)", id); err != nil {
			t.Fatalf("seed tenant %q: %v", id, err)
		}
		if _, err := database.ExecContext(ctx, "INSERT INTO applications (tenant_id,app_code,status) VALUES ($1,$2,'active')", id, appCode); err != nil {
			t.Fatalf("seed application %q: %v", id, err)
		}
	}
	t.Cleanup(func() {
		_, _ = database.ExecContext(context.Background(), "DELETE FROM knowledge_vectors WHERE metadata->>'tenant_id' IN ($1,$2)", tenantID, otherTenantID)
		_, _ = database.ExecContext(context.Background(), "DELETE FROM knowledge_ingest_jobs WHERE tenant_id IN ($1,$2)", tenantID, otherTenantID)
		_, _ = database.ExecContext(context.Background(), "DELETE FROM knowledge_documents WHERE tenant_id IN ($1,$2)", tenantID, otherTenantID)
		_, _ = database.ExecContext(context.Background(), "DELETE FROM applications WHERE tenant_id IN ($1,$2)", tenantID, otherTenantID)
		_, _ = database.ExecContext(context.Background(), "DELETE FROM tenants WHERE id IN ($1,$2)", tenantID, otherTenantID)
	})

	if _, err := database.ExecContext(ctx, `
INSERT INTO knowledge_documents (tenant_id,app_code,document_id,name,status,total_chunks,metadata)
VALUES ($1,$2,$3,'Refund policy','indexing',0,'{}'::jsonb)`, tenantID, appCode, documentID); err != nil {
		t.Fatalf("seed knowledge projection: %v", err)
	}
	if _, err := database.ExecContext(ctx, `
INSERT INTO knowledge_ingest_jobs
    (id,tenant_id,app_code,document_id,name,filename,source_data,backend_driver,status,attempts,lease_owner,lease_until)
VALUES ($1,$2,$3,$4,'Refund policy','refund.md','fixture'::bytea,'pgvector','running',1,$5,NOW()+INTERVAL '5 minutes')`,
		jobID, tenantID, appCode, documentID, owner); err != nil {
		t.Fatalf("seed running knowledge ingest: %v", err)
	}

	framework := frameworkKnowledgeConfig{databaseURL: dsn}
	writer, err := framework.newVectorStore(ctx, tenantID, appCode, config.BackendConfig{Driver: "pgvector"}, map[string]any{
		knowledgeParentMetadataKey: documentID,
		knowledgeJobMetadataKey:    jobID,
		knowledgeOwnerMetadataKey:  owner,
	})
	if err != nil {
		t.Fatalf("construct fenced framework VectorStore: %v", err)
	}
	defer writer.Close()
	embedding := make([]float64, knowledgeEmbeddingDimensions)
	embedding[0] = 1
	if err := writer.Add(ctx, &document.Document{ID: "chunk-0", Name: "Refund policy", Content: "refunds are available for seven days"}, embedding); err != nil {
		t.Fatalf("framework VectorStore Add() under active lease: %v", err)
	}
	stored, storedEmbedding, err := writer.Get(ctx, "chunk-0")
	if err != nil || stored == nil || stored.ID != "chunk-0" || stored.Content != "refunds are available for seven days" || len(storedEmbedding) != len(embedding) {
		t.Fatalf("framework VectorStore Get() = %#v embedding=%d err=%v", stored, len(storedEmbedding), err)
	}
	if err := writer.Update(ctx, &document.Document{ID: "chunk-0", Name: "Refund policy", Content: "refunds are available for fourteen days"}, embedding); err != nil {
		t.Fatalf("framework VectorStore Update() under active lease: %v", err)
	}
	stored, _, err = writer.Get(ctx, "chunk-0")
	if err != nil || stored == nil || stored.Content != "refunds are available for fourteen days" {
		t.Fatalf("framework VectorStore Get(after update) = %#v, %v", stored, err)
	}
	metadata, err := writer.GetMetadata(ctx, vectorstore.WithGetMetadataIDs([]string{"chunk-0"}), vectorstore.WithGetMetadataLimit(10))
	if err != nil {
		t.Fatalf("framework VectorStore GetMetadata() error = %v", err)
	}
	if _, ok := metadata["chunk-0"]; !ok {
		t.Fatalf("framework VectorStore GetMetadata() = %#v, want chunk-0", metadata)
	}
	if err := writer.Delete(ctx, "chunk-0"); err != nil {
		t.Fatalf("framework VectorStore Delete() error = %v", err)
	}
	if count, err := writer.Count(ctx, vectorstore.WithCountFilter(map[string]any{knowledgeParentMetadataKey: documentID})); err != nil || count != 0 {
		t.Fatalf("framework VectorStore Count(after delete) = %d, %v; want 0, nil", count, err)
	}
	if err := writer.Add(ctx, &document.Document{ID: "chunk-0", Name: "Refund policy", Content: "refunds are available for fourteen days"}, embedding); err != nil {
		t.Fatalf("framework VectorStore re-Add() error = %v", err)
	}
	if err := writer.DeleteByFilter(ctx, vectorstore.WithDeleteDocumentIDs([]string{"chunk-0"})); err != nil {
		t.Fatalf("framework VectorStore DeleteByFilter() error = %v", err)
	}
	if count, err := writer.Count(ctx, vectorstore.WithCountFilter(map[string]any{knowledgeParentMetadataKey: documentID})); err != nil || count != 0 {
		t.Fatalf("framework VectorStore Count(after filtered delete) = %d, %v; want 0, nil", count, err)
	}
	if err := writer.Add(ctx, &document.Document{ID: "chunk-0", Name: "Refund policy", Content: "refunds are available for fourteen days"}, embedding); err != nil {
		t.Fatalf("framework VectorStore final Add() error = %v", err)
	}
	if count, err := writer.Count(ctx, vectorstore.WithCountFilter(map[string]any{knowledgeParentMetadataKey: documentID})); err != nil || count != 1 {
		t.Fatalf("fenced writer Count() = %d, %v; want 1, nil", count, err)
	}

	reader, err := framework.newVectorStore(ctx, tenantID, appCode, config.BackendConfig{Driver: "pgvector"}, nil)
	if err != nil {
		t.Fatalf("construct framework retrieval VectorStore: %v", err)
	}
	defer reader.Close()
	if count, err := reader.Count(ctx, vectorstore.WithCountFilter(map[string]any{knowledgeParentMetadataKey: documentID})); err != nil || count != 0 {
		t.Fatalf("indexing document visible to normal retrieval: count=%d err=%v", count, err)
	}
	if _, err := database.ExecContext(ctx, `UPDATE knowledge_documents SET status='ready',total_chunks=1 WHERE tenant_id=$1 AND app_code=$2 AND document_id=$3`, tenantID, appCode, documentID); err != nil {
		t.Fatalf("publish ready knowledge projection: %v", err)
	}
	scopedDSN, err := scopedKnowledgeDSN(dsn, tenantID, appCode, nil)
	if err != nil {
		t.Fatalf("construct retrieval scope DSN: %v", err)
	}
	scopedDB, err := sql.Open("pgx", scopedDSN)
	if err != nil {
		t.Fatalf("open retrieval scope database: %v", err)
	}
	defer scopedDB.Close()
	var currentUser, scopedTenant, scopedApp string
	if err := scopedDB.QueryRowContext(ctx, `
SELECT current_user, current_setting('app.tenant_id', true), current_setting('app.app_code', true)`).Scan(&currentUser, &scopedTenant, &scopedApp); err != nil {
		t.Fatalf("read retrieval PostgreSQL scope: %v", err)
	}
	if currentUser != "trpc_tenant" || scopedTenant != tenantID || scopedApp != appCode {
		t.Fatalf("retrieval PostgreSQL scope = user:%q tenant:%q app:%q", currentUser, scopedTenant, scopedApp)
	}
	var visibleProjection, visibleVectors int
	if err := scopedDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM knowledge_documents WHERE tenant_id=$1 AND app_code=$2 AND document_id=$3", tenantID, appCode, documentID).Scan(&visibleProjection); err != nil {
		t.Fatalf("count ready management projection through RLS: %v", err)
	}
	if err := scopedDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM knowledge_vectors WHERE metadata->>'parent_document_id'=$1", documentID).Scan(&visibleVectors); err != nil {
		t.Fatalf("count ready vectors through RLS: %v", err)
	}
	if visibleProjection != 1 || visibleVectors != 1 {
		t.Fatalf("ready RLS visibility = projection:%d vectors:%d, want 1/1", visibleProjection, visibleVectors)
	}
	if count, err := reader.Count(ctx, vectorstore.WithCountFilter(map[string]any{knowledgeParentMetadataKey: documentID})); err != nil || count != 1 {
		t.Fatalf("ready framework vector visibility count = %d, %v; want 1, nil", count, err)
	}
	result, err := reader.Search(ctx, &vectorstore.SearchQuery{Vector: embedding, Limit: 10, SearchMode: vectorstore.SearchModeVector})
	if err != nil || len(result.Results) != 1 || result.Results[0].Document.Content != "refunds are available for fourteen days" {
		t.Fatalf("framework pgvector retrieval after ready = %#v, %v", result, err)
	}

	other, err := framework.newVectorStore(ctx, otherTenantID, appCode, config.BackendConfig{Driver: "pgvector"}, nil)
	if err != nil {
		t.Fatalf("construct other-tenant VectorStore: %v", err)
	}
	defer other.Close()
	if count, err := other.Count(ctx); err != nil || count != 0 {
		t.Fatalf("cross-tenant framework VectorStore count = %d, %v; want 0, nil", count, err)
	}

	if _, err := database.ExecContext(ctx, "UPDATE knowledge_ingest_jobs SET lease_until=$2 WHERE id=$1", jobID, time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("expire ingest lease: %v", err)
	}
	if err := writer.Add(ctx, &document.Document{ID: "stale-chunk", Content: "must not be committed"}, embedding); err == nil {
		t.Fatal("expired ingest worker wrote a framework vector after losing its lease")
	}
}
