package tenant

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/liuzengh/trpc-agent-service/trpcservice/dbscope"
)

// TestTenantRoleRLSRejectsCrossTenantPlatformDataWrites proves that the shared
// database connection switches to the tenant role inside tenant-scoped
// transactions, hiding cross-tenant rows and rejecting cross-tenant writes.
func TestTenantRoleRLSRejectsCrossTenantPlatformDataWrites(t *testing.T) {
	ownerDSN := os.Getenv("TEST_POSTGRES_DSN")
	if ownerDSN == "" {
		t.Skip("TEST_POSTGRES_DSN is required")
	}
	ctx := context.Background()
	owner, err := sql.Open("pgx", ownerDSN)
	if err != nil {
		t.Fatalf("open owner database: %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	for _, tenantID := range []string{"rls-platform-a", "rls-platform-b"} {
		if _, err := owner.ExecContext(ctx, "INSERT INTO tenants (id) VALUES ($1) ON CONFLICT (id) DO NOTHING", tenantID); err != nil {
			t.Fatalf("seed tenant %s: %v", tenantID, err)
		}
		if _, err := owner.ExecContext(ctx, "INSERT INTO applications (tenant_id,app_code,status) VALUES ($1,'rls-platform','active') ON CONFLICT (tenant_id,app_code) DO NOTHING", tenantID); err != nil {
			t.Fatalf("seed application %s: %v", tenantID, err)
		}
	}
	seed := []string{
		"INSERT INTO knowledge_documents (tenant_id,app_code,document_id,name,status,total_chunks,metadata) VALUES ('rls-platform-a','rls-platform','seed','seed','ready',1,'{}'), ('rls-platform-b','rls-platform','seed','seed','ready',1,'{}') ON CONFLICT DO NOTHING",
		"INSERT INTO knowledge_vectors (id,name,content,embedding,metadata,created_at,updated_at) VALUES ('rls-a-vector','seed','a',NULL,'{\"tenant_id\":\"rls-platform-a\",\"app_code\":\"rls-platform\",\"parent_document_id\":\"seed\"}',0,0), ('rls-b-vector','seed','b',NULL,'{\"tenant_id\":\"rls-platform-b\",\"app_code\":\"rls-platform\",\"parent_document_id\":\"seed\"}',0,0) ON CONFLICT DO NOTHING",
		"INSERT INTO knowledge_ingest_jobs (id,tenant_id,app_code,document_id,name,filename,source_data,backend_driver) VALUES ('ingest-a','rls-platform-a','rls-platform','ingest-seed','seed','seed.txt','a'::bytea,'pgvector'), ('ingest-b','rls-platform-b','rls-platform','ingest-seed','seed','seed.txt','b'::bytea,'pgvector') ON CONFLICT DO NOTHING",
		"INSERT INTO artifacts (tenant_id,app_code,user_id,session_id,filename,version,mime_type,content) VALUES ('rls-platform-a','rls-platform','subject','session','seed',0,'text/plain',decode('01','hex')), ('rls-platform-b','rls-platform','subject','session','seed',0,'text/plain',decode('01','hex')) ON CONFLICT DO NOTHING",
		"INSERT INTO execution_traces (tenant_id,app_code,channel_type,binding_id,message_id,trace_id,projection) VALUES ('rls-platform-a','rls-platform','telegram','support-bot','seed','trace-a','{}'), ('rls-platform-b','rls-platform','telegram','support-bot','seed','trace-b','{}') ON CONFLICT DO NOTHING",
	}
	for _, statement := range seed {
		if _, err := owner.ExecContext(ctx, statement); err != nil {
			t.Fatalf("seed platform data: %v", err)
		}
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, owner, "rls-platform-a")
	if err != nil {
		t.Fatalf("begin tenant transaction: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SELECT set_config('app.app_code', $1, true)", "rls-platform"); err != nil {
		t.Fatalf("set application scope: %v", err)
	}
	cases := []struct {
		table, ownInsert, crossInsert string
	}{
		{"knowledge_documents", "INSERT INTO knowledge_documents (tenant_id,app_code,document_id,name,status,total_chunks,metadata) VALUES ('rls-platform-a','rls-platform','own','own','ready',0,'{}')", "INSERT INTO knowledge_documents (tenant_id,app_code,document_id,name,status,total_chunks,metadata) VALUES ('rls-platform-b','rls-platform','blocked','blocked','ready',0,'{}')"},
		{"knowledge_ingest_jobs", "INSERT INTO knowledge_ingest_jobs (id,tenant_id,app_code,document_id,name,filename,source_data,backend_driver) VALUES ('ingest-own','rls-platform-a','rls-platform','ingest-own','own','own.txt','own'::bytea,'pgvector')", "INSERT INTO knowledge_ingest_jobs (id,tenant_id,app_code,document_id,name,filename,source_data,backend_driver) VALUES ('ingest-blocked','rls-platform-b','rls-platform','ingest-blocked','blocked','blocked.txt','blocked'::bytea,'pgvector')"},
		{"artifacts", "INSERT INTO artifacts (tenant_id,app_code,user_id,session_id,filename,version,mime_type,content) VALUES ('rls-platform-a','rls-platform','subject','session','own',0,'text/plain',decode('01','hex'))", "INSERT INTO artifacts (tenant_id,app_code,user_id,session_id,filename,version,mime_type,content) VALUES ('rls-platform-b','rls-platform','subject','session','blocked',0,'text/plain',decode('01','hex'))"},
		{"execution_traces", "INSERT INTO execution_traces (tenant_id,app_code,channel_type,binding_id,message_id,trace_id,projection) VALUES ('rls-platform-a','rls-platform','telegram','support-bot','own','trace-own','{}')", "INSERT INTO execution_traces (tenant_id,app_code,channel_type,binding_id,message_id,trace_id,projection) VALUES ('rls-platform-b','rls-platform','telegram','support-bot','blocked','trace-blocked','{}')"},
	}
	for _, testCase := range cases {
		var own, other int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+testCase.table+" WHERE tenant_id='rls-platform-a'").Scan(&own); err != nil {
			t.Fatalf("read own %s: %v", testCase.table, err)
		}
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+testCase.table+" WHERE tenant_id='rls-platform-b'").Scan(&other); err != nil {
			t.Fatalf("read other %s: %v", testCase.table, err)
		}
		if own != 1 || other != 0 {
			t.Fatalf("%s RLS counts own=%d other=%d, want 1/0", testCase.table, own, other)
		}
		if _, err := tx.ExecContext(ctx, "SAVEPOINT cross_tenant_write"); err != nil {
			t.Fatalf("create %s cross-tenant savepoint: %v", testCase.table, err)
		}
		if _, err := tx.ExecContext(ctx, testCase.crossInsert); err == nil {
			t.Fatalf("%s accepted a cross-tenant insert", testCase.table)
		}
		if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT cross_tenant_write"); err != nil {
			t.Fatalf("rollback %s cross-tenant write: %v", testCase.table, err)
		}
		if _, err := tx.ExecContext(ctx, testCase.ownInsert); err != nil {
			t.Fatalf("%s rejected a same-tenant insert: %v", testCase.table, err)
		}
	}

	var ownVectors, otherVectors int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM knowledge_vectors WHERE metadata->>'tenant_id'='rls-platform-a'").Scan(&ownVectors); err != nil {
		t.Fatalf("read own knowledge_vectors: %v", err)
	}
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM knowledge_vectors WHERE metadata->>'tenant_id'='rls-platform-b'").Scan(&otherVectors); err != nil {
		t.Fatalf("read other knowledge_vectors: %v", err)
	}
	if ownVectors != 1 || otherVectors != 0 {
		t.Fatalf("knowledge_vectors RLS counts own=%d other=%d, want 1/0", ownVectors, otherVectors)
	}
	if _, err := tx.ExecContext(ctx, "SAVEPOINT unfenced_vector_write"); err != nil {
		t.Fatalf("create unfenced vector savepoint: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO knowledge_vectors (id,name,content,embedding,metadata,created_at,updated_at)
VALUES ('rls-unfenced-vector','blocked','blocked',NULL,'{"tenant_id":"rls-platform-a","app_code":"rls-platform","parent_document_id":"seed"}',0,0)`); err == nil {
		t.Fatal("knowledge_vectors accepted an unfenced runtime insert")
	}
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT unfenced_vector_write"); err != nil {
		t.Fatalf("rollback unfenced vector write: %v", err)
	}
}
