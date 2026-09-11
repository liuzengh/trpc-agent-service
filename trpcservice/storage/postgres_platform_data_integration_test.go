package storage

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresPlatformStoreKnowledgeProjectionLifecycle(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	if _, err := NewPostgresPlatformStore(nil); err == nil {
		t.Fatal("NewPostgresPlatformStore(nil) error = nil")
	}
	store, err := NewPostgresPlatformStore(database)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	const tenantID = "support-platform-data"
	const otherTenantID = "support-platform-data-other"
	const appCode = "support"
	for _, id := range []string{tenantID, otherTenantID} {
		if _, err := database.ExecContext(ctx, "INSERT INTO tenants (id) VALUES ($1) ON CONFLICT (id) DO NOTHING", id); err != nil {
			t.Fatal(err)
		}
		if _, err := database.ExecContext(ctx, "INSERT INTO applications (tenant_id, app_code, status) VALUES ($1,$2,'active') ON CONFLICT (tenant_id, app_code) DO NOTHING", id, appCode); err != nil {
			t.Fatal(err)
		}
		if _, err := database.ExecContext(ctx, "DELETE FROM knowledge_documents WHERE tenant_id=$1 AND app_code=$2", id, appCode); err != nil {
			t.Fatal(err)
		}
	}

	older := time.Now().UTC().Add(-time.Hour)
	newer := time.Now().UTC()
	if _, err := database.ExecContext(ctx, `
INSERT INTO knowledge_documents (tenant_id,app_code,document_id,name,status,total_chunks,metadata,updated_at)
VALUES ($1,$2,'faq','FAQ','ready',2,'{"category":"support"}'::jsonb,$3),
       ($1,$2,'guide','Guide','indexing',4,'{"language":"zh"}'::jsonb,$4)`, tenantID, appCode, older, newer); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `
INSERT INTO knowledge_documents (tenant_id,app_code,document_id,name,status,total_chunks,metadata)
VALUES ($1,$2,'private','Other tenant','ready',1,'{}'::jsonb)`, otherTenantID, appCode); err != nil {
		t.Fatal(err)
	}

	for _, args := range [][2]string{{"", appCode}, {tenantID, ""}} {
		if _, err := store.ListKnowledgeDocuments(ctx, args[0], args[1]); err == nil {
			t.Fatalf("ListKnowledgeDocuments(%q,%q) error = nil", args[0], args[1])
		}
	}
	documents, err := store.ListKnowledgeDocuments(ctx, tenantID, appCode)
	if err != nil {
		t.Fatalf("ListKnowledgeDocuments() error = %v", err)
	}
	if len(documents) != 2 || documents[0].DocumentID != "guide" || documents[1].DocumentID != "faq" {
		t.Fatalf("documents = %+v", documents)
	}
	if documents[0].Metadata["language"] != "zh" || documents[1].Metadata["category"] != "support" {
		t.Fatalf("document metadata = %+v / %+v", documents[0].Metadata, documents[1].Metadata)
	}
	for _, document := range documents {
		if document.TenantID != tenantID {
			t.Fatalf("cross-tenant document leaked: %+v", document)
		}
	}

	if err := store.DeleteKnowledgeDocument(ctx, tenantID, appCode, ""); err == nil {
		t.Fatal("DeleteKnowledgeDocument() accepted empty document ID")
	}
	if err := store.DeleteKnowledgeDocument(ctx, tenantID, appCode, "faq"); err != nil {
		t.Fatalf("DeleteKnowledgeDocument() error = %v", err)
	}
	remaining, err := store.ListKnowledgeDocuments(ctx, tenantID, appCode)
	if err != nil || len(remaining) != 1 || remaining[0].DocumentID != "guide" {
		t.Fatalf("remaining documents = %+v, %v", remaining, err)
	}
	if err := store.DeleteKnowledgeDocument(ctx, tenantID, appCode, "missing"); err != nil {
		t.Fatalf("DeleteKnowledgeDocument(missing) error = %v", err)
	}
}
