package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/postgres"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestCapabilityMethodsRejectNilStore(t *testing.T) {
	var store *runtimepostgres.Store
	_, err := store.PutMemory(context.Background(), runtimestorage.MemoryInput{TenantID: "tenant-a", UserID: "user", Content: "content"})
	if !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("PutMemory on nil store = %v", err)
	}
}

func TestAllPostgresCapabilityMethodsRejectNilStore(t *testing.T) {
	var store *runtimepostgres.Store
	ctx := context.Background()
	when := time.Now()
	checks := []struct {
		name string
		call func() error
	}{
		{"GetMemory", func() error { _, err := store.GetMemory(ctx, "tenant-a", "m"); return err }},
		{"ListMemories", func() error { _, err := store.ListMemories(ctx, "tenant-a", "u", 1); return err }},
		{"SearchMemories", func() error { _, err := store.SearchMemories(ctx, "tenant-a", "u", "q", 1); return err }},
		{"DeleteMemory", func() error { return store.DeleteMemory(ctx, "tenant-a", "m") }},
		{"EnqueueMemoryIndex", func() error {
			return store.EnqueueMemoryIndex(ctx, runtimestorage.MemoryRecord{TenantID: "tenant-a", MemoryID: "m", Version: 1})
		}},
		{"PutSummary", func() error {
			_, err := store.PutSummary(ctx, runtimestorage.SummaryRecord{TenantID: "tenant-a", SessionID: "s", Text: "x"})
			return err
		}},
		{"GetSummary", func() error { _, err := store.GetSummary(ctx, "tenant-a", "s", ""); return err }},
		{"EnqueueSummary", func() error {
			return store.EnqueueSummary(ctx, runtimestorage.SummaryRecord{TenantID: "tenant-a", SessionID: "s", Text: "x"})
		}},
		{"PutKnowledge", func() error {
			_, err := store.PutKnowledge(ctx, runtimestorage.KnowledgeDocument{TenantID: "tenant-a", DocumentID: "d", Content: "x"})
			return err
		}},
		{"GetKnowledge", func() error { _, err := store.GetKnowledge(ctx, "tenant-a", "d"); return err }},
		{"SearchKnowledge", func() error { _, err := store.SearchKnowledge(ctx, "tenant-a", []float64{1}, 1); return err }},
		{"DeleteKnowledge", func() error { return store.DeleteKnowledge(ctx, "tenant-a", "d") }},
		{"PutArtifact", func() error {
			_, err := store.PutArtifact(ctx, runtimestorage.ArtifactRecord{TenantID: "tenant-a", ArtifactID: "a", Content: []byte("x")})
			return err
		}},
		{"GetArtifact", func() error { _, err := store.GetArtifact(ctx, "tenant-a", "a"); return err }},
		{"ListArtifacts", func() error { _, err := store.ListArtifacts(ctx, "tenant-a", ""); return err }},
		{"DeleteArtifact", func() error { return store.DeleteArtifact(ctx, "tenant-a", "a") }},
		{"AppendAudit", func() error {
			_, err := store.AppendAudit(ctx, runtimestorage.AuditRecord{TenantID: "tenant-a", EventType: "event"})
			return err
		}},
		{"ListAudit", func() error { _, err := store.ListAudit(ctx, "tenant-a", when, 1); return err }},
		{"UpsertVector", func() error {
			return store.UpsertVector(ctx, runtimestorage.VectorRecord{TenantID: "tenant-a", DocumentID: "d", Embedding: []float64{1}})
		}},
		{"SearchVectors", func() error { _, err := store.SearchVectors(ctx, "tenant-a", []float64{1}, 1); return err }},
		{"DeleteVector", func() error { return store.DeleteVector(ctx, "tenant-a", "d") }},
		{"PutObject", func() error {
			_, err := store.PutObject(ctx, "tenant-a", "k", strings.NewReader("x"), "text/plain")
			return err
		}},
		{"GetObject", func() error { _, _, err := store.GetObject(ctx, "tenant-a", "k"); return err }},
		{"DeleteObject", func() error { return store.DeleteObject(ctx, "tenant-a", "k") }},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if !errors.Is(check.call(), runtimestorage.ErrStorage) {
				t.Fatalf("%s on nil store did not return ErrStorage", check.name)
			}
		})
	}
}

func TestPostgresCapabilityValidationRejectsMalformedInput(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := runtimepostgres.New(db)
	ctx := context.Background()
	checks := []struct {
		name string
		call func() error
	}{
		{"PutMemory", func() error { _, err := store.PutMemory(ctx, runtimestorage.MemoryInput{}); return err }},
		{"GetMemory", func() error { _, err := store.GetMemory(ctx, "", ""); return err }},
		{"ListMemories", func() error { _, err := store.ListMemories(ctx, "", "", -1); return err }},
		{"SearchMemories", func() error { _, err := store.SearchMemories(ctx, "", "", "", -1); return err }},
		{"DeleteMemory", func() error { return store.DeleteMemory(ctx, "", "") }},
		{"EnqueueMemoryIndex", func() error { return store.EnqueueMemoryIndex(ctx, runtimestorage.MemoryRecord{}) }},
		{"PutSummary", func() error { _, err := store.PutSummary(ctx, runtimestorage.SummaryRecord{}); return err }},
		{"GetSummary", func() error { _, err := store.GetSummary(ctx, "", "", ""); return err }},
		{"PutKnowledge", func() error { _, err := store.PutKnowledge(ctx, runtimestorage.KnowledgeDocument{}); return err }},
		{"GetKnowledge", func() error { _, err := store.GetKnowledge(ctx, "", ""); return err }},
		{"SearchKnowledge", func() error { _, err := store.SearchKnowledge(ctx, "", nil, -1); return err }},
		{"DeleteKnowledge", func() error { return store.DeleteKnowledge(ctx, "", "") }},
		{"PutArtifact", func() error { _, err := store.PutArtifact(ctx, runtimestorage.ArtifactRecord{}); return err }},
		{"GetArtifact", func() error { _, err := store.GetArtifact(ctx, "", ""); return err }},
		{"ListArtifacts", func() error { _, err := store.ListArtifacts(ctx, "", ""); return err }},
		{"DeleteArtifact", func() error { return store.DeleteArtifact(ctx, "", "") }},
		{"AppendAudit", func() error { _, err := store.AppendAudit(ctx, runtimestorage.AuditRecord{}); return err }},
		{"ListAudit", func() error { _, err := store.ListAudit(ctx, "", time.Time{}, -1); return err }},
		{"UpsertVector", func() error { return store.UpsertVector(ctx, runtimestorage.VectorRecord{}) }},
		{"UpsertVectorWhitespaceSource", func() error {
			return store.UpsertVector(ctx, runtimestorage.VectorRecord{TenantID: "tenant-a", Source: " ", DocumentID: "d", Embedding: []float64{1}})
		}},
		{"SearchVectors", func() error { _, err := store.SearchVectors(ctx, "", nil, -1); return err }},
		{"DeleteVector", func() error { return store.DeleteVector(ctx, "", "") }},
		{"PutObject", func() error { _, err := store.PutObject(ctx, "", "", strings.NewReader("x"), ""); return err }},
		{"GetObject", func() error { _, _, err := store.GetObject(ctx, "", ""); return err }},
		{"DeleteObject", func() error { return store.DeleteObject(ctx, "", "") }},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if !errors.Is(check.call(), runtimestorage.ErrInvalid) {
				t.Fatalf("%s did not reject malformed input", check.name)
			}
		})
	}
}

// Exercise database-error branches for every capability method. sqlmock's
// default unexpected-call error is sufficient to drive the repository mapping
// paths without coupling this contract test to driver-specific messages.
func TestPostgresCapabilityDatabaseErrorBranches(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		call func(*runtimepostgres.Store) error
	}{
		{"PutMemory", func(s *runtimepostgres.Store) error {
			_, err := s.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", UserID: "user", Content: "content"})
			return err
		}},
		{"GetMemory", func(s *runtimepostgres.Store) error { _, err := s.GetMemory(ctx, "tenant-a", "memory"); return err }},
		{"ListMemories", func(s *runtimepostgres.Store) error { _, err := s.ListMemories(ctx, "tenant-a", "user", 1); return err }},
		{"SearchMemories", func(s *runtimepostgres.Store) error {
			_, err := s.SearchMemories(ctx, "tenant-a", "user", "query", 1)
			return err
		}},
		{"DeleteMemory", func(s *runtimepostgres.Store) error { return s.DeleteMemory(ctx, "tenant-a", "memory") }},
		{"EnqueueMemoryIndex", func(s *runtimepostgres.Store) error {
			return s.EnqueueMemoryIndex(ctx, runtimestorage.MemoryRecord{TenantID: "tenant-a", MemoryID: "memory", Version: 1})
		}},
		{"PutSummary", func(s *runtimepostgres.Store) error {
			_, err := s.PutSummary(ctx, runtimestorage.SummaryRecord{TenantID: "tenant-a", SessionID: "session", Text: "summary"})
			return err
		}},
		{"GetSummary", func(s *runtimepostgres.Store) error {
			_, err := s.GetSummary(ctx, "tenant-a", "session", "default")
			return err
		}},
		{"PutKnowledge", func(s *runtimepostgres.Store) error {
			_, err := s.PutKnowledge(ctx, runtimestorage.KnowledgeDocument{TenantID: "tenant-a", DocumentID: "document", Content: "content"})
			return err
		}},
		{"GetKnowledge", func(s *runtimepostgres.Store) error {
			_, err := s.GetKnowledge(ctx, "tenant-a", "document")
			return err
		}},
		{"SearchKnowledge", func(s *runtimepostgres.Store) error {
			_, err := s.SearchKnowledge(ctx, "tenant-a", []float64{1}, 1)
			return err
		}},
		{"DeleteKnowledge", func(s *runtimepostgres.Store) error { return s.DeleteKnowledge(ctx, "tenant-a", "document") }},
		{"PutArtifact", func(s *runtimepostgres.Store) error {
			_, err := s.PutArtifact(ctx, runtimestorage.ArtifactRecord{TenantID: "tenant-a", ArtifactID: "artifact", Content: []byte("x")})
			return err
		}},
		{"GetArtifact", func(s *runtimepostgres.Store) error { _, err := s.GetArtifact(ctx, "tenant-a", "artifact"); return err }},
		{"ListArtifacts", func(s *runtimepostgres.Store) error { _, err := s.ListArtifacts(ctx, "tenant-a", ""); return err }},
		{"DeleteArtifact", func(s *runtimepostgres.Store) error { return s.DeleteArtifact(ctx, "tenant-a", "artifact") }},
		{"AppendAudit", func(s *runtimepostgres.Store) error {
			_, err := s.AppendAudit(ctx, runtimestorage.AuditRecord{TenantID: "tenant-a", AuditID: "audit", EventType: "event"})
			return err
		}},
		{"ListAudit", func(s *runtimepostgres.Store) error {
			_, err := s.ListAudit(ctx, "tenant-a", time.Time{}, 1)
			return err
		}},
		{"UpsertVector", func(s *runtimepostgres.Store) error {
			return s.UpsertVector(ctx, runtimestorage.VectorRecord{TenantID: "tenant-a", DocumentID: "document", Embedding: []float64{1}})
		}},
		{"SearchVectors", func(s *runtimepostgres.Store) error {
			_, err := s.SearchVectors(ctx, "tenant-a", []float64{1}, 1)
			return err
		}},
		{"DeleteVector", func(s *runtimepostgres.Store) error { return s.DeleteVector(ctx, "tenant-a", "document") }},
		{"PutObject", func(s *runtimepostgres.Store) error {
			_, err := s.PutObject(ctx, "tenant-a", "object", strings.NewReader("x"), "text/plain")
			return err
		}},
		{"GetObject", func(s *runtimepostgres.Store) error { _, _, err := s.GetObject(ctx, "tenant-a", "object"); return err }},
		{"DeleteObject", func(s *runtimepostgres.Store) error { return s.DeleteObject(ctx, "tenant-a", "object") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, _, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := tc.call(runtimepostgres.New(db)); err == nil {
				t.Fatalf("%s unexpectedly succeeded against an unconfigured database", tc.name)
			}
		})
	}
}

func TestPutKnowledgeProjectionErrorIsReported(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.runtime_knowledge")).
		WithArgs("tenant-a", "doc", "content", []byte("{}"), []byte("[1,0]"), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "document_id", "content", "metadata", "embedding", "digest", "version", "created_at", "updated_at"}).
			AddRow("tenant-a", "doc", "content", []byte("{}"), []byte("[1,0]"), "digest", int64(1), when, when))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO public.runtime_vector_index")).
		WithArgs("tenant-a", "doc", int64(1)).WillReturnError(errors.New("projection failed"))
	if _, err := runtimepostgres.New(db).PutKnowledge(context.Background(), runtimestorage.KnowledgeDocument{TenantID: "tenant-a", DocumentID: "doc", Content: "content", Embedding: []float64{1, 0}}); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("projection error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresCapabilityMethodsHonorCanceledContext(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := runtimepostgres.New(db)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	checks := []func() error{
		func() error { _, err := store.GetMemory(ctx, "tenant-a", "m"); return err },
		func() error { _, err := store.SearchMemories(ctx, "tenant-a", "u", "q", 1); return err },
		func() error { return store.DeleteMemory(ctx, "tenant-a", "m") },
		func() error {
			_, err := store.PutSummary(ctx, runtimestorage.SummaryRecord{TenantID: "tenant-a", SessionID: "s", Text: "x"})
			return err
		},
		func() error { _, err := store.SearchKnowledge(ctx, "tenant-a", []float64{1}, 1); return err },
		func() error { return store.DeleteKnowledge(ctx, "tenant-a", "d") },
		func() error { return store.DeleteArtifact(ctx, "tenant-a", "a") },
		func() error { _, err := store.ListAudit(ctx, "tenant-a", time.Time{}, 1); return err },
		func() error {
			return store.UpsertVector(ctx, runtimestorage.VectorRecord{TenantID: "tenant-a", DocumentID: "d", Embedding: []float64{1}})
		},
		func() error { return store.DeleteObject(ctx, "tenant-a", "k") },
	}
	for _, check := range checks {
		if !errors.Is(check(), context.Canceled) {
			t.Fatal("capability did not preserve context cancellation")
		}
	}
}

func TestPostgresCapabilityConflictAndRollbackPaths(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO public.runtime_vector_index")).WithArgs("tenant-a", "m1", int64(1)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,memory_id,user_id,session_id,content,topics,metadata,embedding,version,deleted_at,created_at,updated_at FROM public.runtime_memory WHERE tenant_id=$1 AND memory_id=$2 AND deleted_at IS NULL")).WithArgs("tenant-a", "m1").WillReturnRows(memoryRows().AddRow("tenant-a", "m1", "user", "", "content", []byte("[]"), []byte("{}"), []byte("[1,0]"), int64(2), nil, when, when))
	if err := runtimepostgres.New(db).EnqueueMemoryIndex(context.Background(), runtimestorage.MemoryRecord{TenantID: "tenant-a", MemoryID: "m1", Version: 1}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("stale memory index = %v", err)
	}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE public.runtime_memory SET deleted_at=now(),version=version+1,updated_at=now() WHERE tenant_id=$1 AND memory_id=$2 AND deleted_at IS NULL")).WithArgs("tenant-a", "missing").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()
	if err := runtimepostgres.New(db).DeleteMemory(context.Background(), "tenant-a", "missing"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing memory delete = %v", err)
	}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM public.runtime_knowledge WHERE tenant_id=$1 AND document_id=$2")).WithArgs("tenant-a", "missing").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()
	if err := runtimepostgres.New(db).DeleteKnowledge(context.Background(), "tenant-a", "missing"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing knowledge delete = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.runtime_audit_log")).WithArgs("tenant-a", "audit", "event", []byte("{}"), sqlmock.AnyArg()).WillReturnError(sql.ErrNoRows)
	if _, err := runtimepostgres.New(db).AppendAudit(context.Background(), runtimestorage.AuditRecord{TenantID: "tenant-a", AuditID: "audit", EventType: "event"}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("conflicting audit = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEnqueueMemoryIndexPreservesExistingProjection(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO public.runtime_vector_index")).
		WithArgs("tenant-a", "m1", int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,memory_id,user_id,session_id,content,topics,metadata,embedding,version,deleted_at,created_at,updated_at FROM public.runtime_memory WHERE tenant_id=$1 AND memory_id=$2 AND deleted_at IS NULL")).
		WithArgs("tenant-a", "m1").
		WillReturnRows(memoryRows().AddRow("tenant-a", "m1", "user", "", "content", []byte("[]"), []byte("{}"), []byte("[1,0]"), int64(1), nil, when, when))
	if err := runtimepostgres.New(db).EnqueueMemoryIndex(context.Background(), runtimestorage.MemoryRecord{TenantID: "tenant-a", MemoryID: "m1", Version: 1}); err != nil {
		t.Fatalf("EnqueueMemoryIndex = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSearchVectorsComputesCosineAndScopesTenant(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,source,document_id,content,metadata,embedding,version,updated_at FROM public.runtime_vector_index WHERE tenant_id=$1")).
		WithArgs("tenant-a").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "source", "document_id", "content", "metadata", "embedding", "version", "updated_at"}).
			AddRow("tenant-a", "generic", "doc-1", "coffee", []byte("{\"kind\":\"fact\"}"), []byte("[1,0]"), int64(3), when).
			AddRow("tenant-a", "generic", "doc-2", "tea", []byte("{}"), []byte("[0,1]"), int64(1), when))
	results, err := runtimepostgres.New(db).SearchVectors(context.Background(), "tenant-a", []float64{1, 0}, 10)
	if err != nil || len(results) != 2 {
		t.Fatalf("SearchVectors = %+v, %v", results, err)
	}
	if results[0].Record.DocumentID != "doc-1" || results[0].Score != 1 {
		t.Fatalf("top vector = %+v", results[0])
	}
	if results[1].Score != 0 {
		t.Fatalf("orthogonal score = %v", results[1].Score)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPutMemoryNormalizesNilJSONValues(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.runtime_memory")).
		WithArgs("tenant-a", sqlmock.AnyArg(), "user", "", "content", []byte("[]"), []byte("{}"), []byte("[]")).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "memory_id", "user_id", "session_id", "content", "topics", "metadata", "embedding", "version", "deleted_at", "created_at", "updated_at"}).
			AddRow("tenant-a", "mem-generated", "user", "", "content", []byte("[]"), []byte("{}"), []byte("[]"), int64(1), nil, when, when))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO public.runtime_vector_index")).
		WithArgs("tenant-a", "mem-generated", int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,memory_id,user_id,session_id,content,topics,metadata,embedding,version,deleted_at,created_at,updated_at FROM public.runtime_memory WHERE tenant_id=$1 AND memory_id=$2 AND deleted_at IS NULL")).
		WithArgs("tenant-a", "mem-generated").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "memory_id", "user_id", "session_id", "content", "topics", "metadata", "embedding", "version", "deleted_at", "created_at", "updated_at"}).
			AddRow("tenant-a", "mem-generated", "user", "", "content", []byte("[]"), []byte("{}"), []byte("[]"), int64(1), nil, when, when))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM public.runtime_vector_index WHERE tenant_id=$1 AND source=$2 AND document_id=$3")).
		WithArgs("tenant-a", runtimestorage.VectorSourceMemory, "mem-generated").
		WillReturnResult(sqlmock.NewResult(0, 0))
	value, err := runtimepostgres.New(db).PutMemory(context.Background(), runtimestorage.MemoryInput{TenantID: "tenant-a", UserID: "user", Content: "content"})
	if err != nil || value.MemoryID != "mem-generated" {
		t.Fatalf("PutMemory = %+v, %v", value, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPutMemoryEnqueuesEmbeddingProjection(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.runtime_memory")).
		WithArgs("tenant-a", "m1", "user", "", "content", []byte("[]"), []byte("{}"), []byte("[1,0]")).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "memory_id", "user_id", "session_id", "content", "topics", "metadata", "embedding", "version", "deleted_at", "created_at", "updated_at"}).
			AddRow("tenant-a", "m1", "user", "", "content", []byte("[]"), []byte("{}"), []byte("[1,0]"), int64(1), nil, when, when))
	// The memory INSERT returns the durable record that the index operation reads.
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO public.runtime_vector_index")).
		WithArgs("tenant-a", "m1", int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	value, err := runtimepostgres.New(db).PutMemory(context.Background(), runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "m1", UserID: "user", Content: "content", Embedding: []float64{1, 0}})
	if err != nil || value.MemoryID != "m1" {
		t.Fatalf("PutMemory = %+v, %v", value, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPutSummaryMapsMissingSessionToNotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.runtime_summary")).
		WithArgs("tenant-a", "missing", "", "summary", int64(0)).
		WillReturnError(&pgconn.PgError{Code: "23503"})
	if _, err := runtimepostgres.New(db).PutSummary(context.Background(), runtimestorage.SummaryRecord{TenantID: "tenant-a", SessionID: "missing", Text: "summary"}); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("PutSummary missing session = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestListArtifactsMapsRowsErrorToStorage(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	rows := sqlmock.NewRows([]string{"tenant_id", "artifact_id", "session_id", "name", "mime_type", "content", "version", "created_at", "updated_at"})
	rows.AddRow("tenant-a", "a", "", "", "", []byte("x"), int64(1), time.Now(), time.Now())
	rows.AddRow("tenant-a", "b", "", "", "", []byte("x"), int64(1), time.Now(), time.Now())
	rows.RowError(1, errors.New("rows failed"))
	/*
		mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,artifact_id,session_id,name,mime_type,content,version,created_at,updated_at FROM public.runtime_artifact WHERE tenant_id=$1 AND ($2='' OR session_id=$2) ORDER BY artifact_id")).
			WithArgs("tenant-a", "").
			WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "artifact_id", "session_id", "name", "mime_type", "content", "version", "created_at", "updated_at"]).
				RowError(1, errors.New("rows failed")))
	*/
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,artifact_id,session_id,name,mime_type,content,version,created_at,updated_at FROM public.runtime_artifact WHERE tenant_id=$1 AND ($2='' OR session_id=$2) ORDER BY artifact_id")).WithArgs("tenant-a", "").WillReturnRows(rows)
	if _, err := runtimepostgres.New(db).ListArtifacts(context.Background(), "tenant-a", ""); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("ListArtifacts rows error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSearchMemoriesMatchesTokenRelevance(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,memory_id,user_id,session_id,content,topics,metadata,embedding,version,deleted_at,created_at,updated_at FROM public.runtime_memory WHERE tenant_id=$1 AND user_id=$2 AND deleted_at IS NULL")).
		WithArgs("tenant-a", "user").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "memory_id", "user_id", "session_id", "content", "topics", "metadata", "embedding", "version", "deleted_at", "created_at", "updated_at"}).
			AddRow("tenant-a", "both", "user", "", "coffee and tea", []byte("[]"), []byte("{}"), []byte("[]"), int64(1), nil, when, when).
			AddRow("tenant-a", "one", "user", "", "coffee", []byte("[]"), []byte("{}"), []byte("[]"), int64(1), nil, when, when).
			AddRow("tenant-a", "none", "user", "", "water", []byte("[]"), []byte("{}"), []byte("[]"), int64(1), nil, when, when))
	results, err := runtimepostgres.New(db).SearchMemories(context.Background(), "tenant-a", "user", "coffee tea", 10)
	if err != nil || len(results) != 2 {
		t.Fatalf("SearchMemories = %+v, %v", results, err)
	}
	if results[0].Memory.MemoryID != "both" || results[0].Score != 1 || results[1].Memory.MemoryID != "one" || results[1].Score != 0.5 {
		t.Fatalf("memory relevance = %+v", results)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteKnowledgeRemovesVectorProjection(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM public.runtime_knowledge WHERE tenant_id=$1 AND document_id=$2")).
		WithArgs("tenant-a", "doc-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM public.runtime_vector_index WHERE tenant_id=$1 AND source=$2 AND document_id=$3")).
		WithArgs("tenant-a", runtimestorage.VectorSourceKnowledge, "doc-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := runtimepostgres.New(db).DeleteKnowledge(context.Background(), "tenant-a", "doc-1"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresVectorUpsertRejectsStaleVersion(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO public.runtime_vector_index")).
		WithArgs("tenant-a", runtimestorage.VectorSourceGeneric, "doc-1", "new", []byte("{}"), []byte("[1,0]"), int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	err = runtimepostgres.New(db).UpsertVector(context.Background(), runtimestorage.VectorRecord{TenantID: "tenant-a", DocumentID: "doc-1", Content: "new", Embedding: []float64{1, 0}, Version: 1})
	if !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("stale vector upsert = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func memoryRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"tenant_id", "memory_id", "user_id", "session_id", "content", "topics", "metadata", "embedding", "version", "deleted_at", "created_at", "updated_at"})
}

func TestPostgresMemoryReadAndDeleteContracts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,memory_id,user_id,session_id,content,topics,metadata,embedding,version,deleted_at,created_at,updated_at FROM public.runtime_memory WHERE tenant_id=$1 AND memory_id=$2 AND deleted_at IS NULL")).
		WithArgs("tenant-a", "m1").WillReturnRows(memoryRows().AddRow("tenant-a", "m1", "user", "", "content", []byte("[]"), []byte("{}"), []byte("[1,0]"), int64(2), nil, when, when))
	value, err := runtimepostgres.New(db).GetMemory(context.Background(), "tenant-a", "m1")
	if err != nil || value.Version != 2 {
		t.Fatalf("GetMemory = %+v, %v", value, err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,memory_id,user_id,session_id,content,topics,metadata,embedding,version,deleted_at,created_at,updated_at FROM public.runtime_memory WHERE tenant_id=$1 AND user_id=$2 AND deleted_at IS NULL ORDER BY updated_at DESC,memory_id LIMIT NULLIF($3,0)")).
		WithArgs("tenant-a", "user", 10).WillReturnRows(memoryRows().AddRow("tenant-a", "m1", "user", "", "content", []byte("[]"), []byte("{}"), []byte("[1,0]"), int64(2), nil, when, when))
	if values, err := runtimepostgres.New(db).ListMemories(context.Background(), "tenant-a", "user", 10); err != nil || len(values) != 1 {
		t.Fatalf("ListMemories = %+v, %v", values, err)
	}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE public.runtime_memory SET deleted_at=now(),version=version+1,updated_at=now() WHERE tenant_id=$1 AND memory_id=$2 AND deleted_at IS NULL")).
		WithArgs("tenant-a", "m1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM public.runtime_vector_index WHERE tenant_id=$1 AND source=$2 AND document_id=$3")).
		WithArgs("tenant-a", runtimestorage.VectorSourceMemory, "m1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := runtimepostgres.New(db).DeleteMemory(context.Background(), "tenant-a", "m1"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresMemoryIndexAndSummaryContracts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	value := runtimestorage.MemoryRecord{TenantID: "tenant-a", MemoryID: "m1", Version: 2}
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO public.runtime_vector_index")).
		WithArgs("tenant-a", "m1", int64(2)).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := runtimepostgres.New(db).EnqueueMemoryIndex(context.Background(), value); err != nil {
		t.Fatalf("EnqueueMemoryIndex = %v", err)
	}
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO public.runtime_vector_index")).
		WithArgs("tenant-a", "m1", int64(2)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,memory_id,user_id,session_id,content,topics,metadata,embedding,version,deleted_at,created_at,updated_at FROM public.runtime_memory WHERE tenant_id=$1 AND memory_id=$2 AND deleted_at IS NULL")).
		WithArgs("tenant-a", "m1").WillReturnRows(memoryRows().AddRow("tenant-a", "m1", "user", "", "content", []byte("[]"), []byte("{}"), []byte("[]"), int64(2), nil, time.Now(), time.Now()))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM public.runtime_vector_index WHERE tenant_id=$1 AND source=$2 AND document_id=$3")).
		WithArgs("tenant-a", runtimestorage.VectorSourceMemory, "m1").WillReturnResult(sqlmock.NewResult(0, 0))
	if err := runtimepostgres.New(db).EnqueueMemoryIndex(context.Background(), runtimestorage.MemoryRecord{TenantID: "tenant-a", MemoryID: "m1", Version: 2}); err != nil {
		t.Fatalf("EnqueueMemoryIndex without embedding = %v", err)
	}
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.runtime_summary (tenant_id,session_id,filter_key,text,event_seq,version) VALUES ($1,$2,$3,$4,$5,1) ON CONFLICT (tenant_id,session_id,filter_key) DO UPDATE SET text=EXCLUDED.text,event_seq=EXCLUDED.event_seq,version=public.runtime_summary.version+1,updated_at=now() WHERE EXCLUDED.event_seq >= public.runtime_summary.event_seq RETURNING tenant_id,session_id,filter_key,text,event_seq,version,created_at,updated_at")).
		WithArgs("tenant-a", "session", "default", "summary", int64(2)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "session_id", "filter_key", "text", "event_seq", "version", "created_at", "updated_at"}).AddRow("tenant-a", "session", "default", "summary", int64(2), int64(1), when, when))
	if _, err := runtimepostgres.New(db).PutSummary(context.Background(), runtimestorage.SummaryRecord{TenantID: "tenant-a", SessionID: "session", FilterKey: "default", Text: "summary", EventSeq: 2}); err != nil {
		t.Fatalf("PutSummary = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,session_id,filter_key,text,event_seq,version,created_at,updated_at FROM public.runtime_summary WHERE tenant_id=$1 AND session_id=$2 AND filter_key=$3")).
		WithArgs("tenant-a", "session", "default").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "session_id", "filter_key", "text", "event_seq", "version", "created_at", "updated_at"}).AddRow("tenant-a", "session", "default", "summary", int64(2), int64(1), when, when))
	if _, err := runtimepostgres.New(db).GetSummary(context.Background(), "tenant-a", "session", "default"); err != nil {
		t.Fatalf("GetSummary = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.runtime_summary")).
		WithArgs("tenant-a", "session", "default", "summary", int64(3)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "session_id", "filter_key", "text", "event_seq", "version", "created_at", "updated_at"}).AddRow("tenant-a", "session", "default", "summary", int64(3), int64(2), when, when))
	if err := runtimepostgres.New(db).EnqueueSummary(context.Background(), runtimestorage.SummaryRecord{TenantID: "tenant-a", SessionID: "session", FilterKey: "default", Text: "summary", EventSeq: 3}); err != nil {
		t.Fatalf("EnqueueSummary = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresKnowledgeAndArtifactContracts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	knowledgeRows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"tenant_id", "document_id", "content", "metadata", "embedding", "digest", "version", "created_at", "updated_at"})
	}
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.runtime_knowledge")).
		WithArgs("tenant-a", "doc", "knowledge", []byte("{}"), []byte("[1,0]"), sqlmock.AnyArg()).
		WillReturnRows(knowledgeRows().AddRow("tenant-a", "doc", "knowledge", []byte("{}"), []byte("[1,0]"), "digest", int64(1), when, when))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO public.runtime_vector_index")).
		WithArgs("tenant-a", "doc", int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	if _, err := runtimepostgres.New(db).PutKnowledge(context.Background(), runtimestorage.KnowledgeDocument{TenantID: "tenant-a", DocumentID: "doc", Content: "knowledge", Embedding: []float64{1, 0}}); err != nil {
		t.Fatalf("PutKnowledge = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.runtime_knowledge")).
		WithArgs("tenant-a", "empty", "without embedding", []byte("{}"), []byte("[]"), sqlmock.AnyArg()).
		WillReturnRows(knowledgeRows().AddRow("tenant-a", "empty", "without embedding", []byte("{}"), []byte("[]"), "empty-digest", int64(1), when, when))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM public.runtime_vector_index WHERE tenant_id=$1 AND source=$2 AND document_id=$3")).
		WithArgs("tenant-a", runtimestorage.VectorSourceKnowledge, "empty").WillReturnResult(sqlmock.NewResult(0, 0))
	if _, err := runtimepostgres.New(db).PutKnowledge(context.Background(), runtimestorage.KnowledgeDocument{TenantID: "tenant-a", DocumentID: "empty", Content: "without embedding"}); err != nil {
		t.Fatalf("PutKnowledge without embedding = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,document_id,content,metadata,embedding,digest,version,created_at,updated_at FROM public.runtime_knowledge WHERE tenant_id=$1 AND document_id=$2")).
		WithArgs("tenant-a", "doc").WillReturnRows(knowledgeRows().AddRow("tenant-a", "doc", "knowledge", []byte("{}"), []byte("[1,0]"), "digest", int64(1), when, when))
	if _, err := runtimepostgres.New(db).GetKnowledge(context.Background(), "tenant-a", "doc"); err != nil {
		t.Fatalf("GetKnowledge = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,document_id,content,metadata,embedding,digest,version,created_at,updated_at FROM public.runtime_knowledge WHERE tenant_id=$1 AND embedding <> '[]'::jsonb")).
		WithArgs("tenant-a").WillReturnRows(knowledgeRows().AddRow("tenant-a", "doc", "knowledge", []byte("{}"), []byte("[1,0]"), "digest", int64(1), when, when))
	if values, err := runtimepostgres.New(db).SearchKnowledge(context.Background(), "tenant-a", []float64{1, 0}, 10); err != nil || len(values) != 1 {
		t.Fatalf("SearchKnowledge = %+v, %v", values, err)
	}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM public.runtime_knowledge WHERE tenant_id=$1 AND document_id=$2")).WithArgs("tenant-a", "doc").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM public.runtime_vector_index WHERE tenant_id=$1 AND source=$2 AND document_id=$3")).WithArgs("tenant-a", runtimestorage.VectorSourceKnowledge, "doc").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := runtimepostgres.New(db).DeleteKnowledge(context.Background(), "tenant-a", "doc"); err != nil {
		t.Fatalf("DeleteKnowledge = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.runtime_artifact")).
		WithArgs("tenant-a", "artifact", "", "", "", []byte("data")).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "artifact_id", "session_id", "name", "mime_type", "content", "version", "created_at", "updated_at"}).AddRow("tenant-a", "artifact", "", "", "", []byte("data"), int64(1), when, when))
	if _, err := runtimepostgres.New(db).PutArtifact(context.Background(), runtimestorage.ArtifactRecord{TenantID: "tenant-a", ArtifactID: "artifact", Content: []byte("data")}); err != nil {
		t.Fatalf("PutArtifact = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,artifact_id,session_id,name,mime_type,content,version,created_at,updated_at FROM public.runtime_artifact WHERE tenant_id=$1 AND artifact_id=$2")).WithArgs("tenant-a", "artifact").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "artifact_id", "session_id", "name", "mime_type", "content", "version", "created_at", "updated_at"}).AddRow("tenant-a", "artifact", "", "", "", []byte("data"), int64(1), when, when))
	if _, err := runtimepostgres.New(db).GetArtifact(context.Background(), "tenant-a", "artifact"); err != nil {
		t.Fatalf("GetArtifact = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,artifact_id,session_id,name,mime_type,content,version,created_at,updated_at FROM public.runtime_artifact WHERE tenant_id=$1 AND ($2='' OR session_id=$2) ORDER BY artifact_id")).WithArgs("tenant-a", "").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "artifact_id", "session_id", "name", "mime_type", "content", "version", "created_at", "updated_at"}).AddRow("tenant-a", "artifact", "", "", "", []byte("data"), int64(1), when, when))
	if values, err := runtimepostgres.New(db).ListArtifacts(context.Background(), "tenant-a", ""); err != nil || len(values) != 1 {
		t.Fatalf("ListArtifacts = %+v, %v", values, err)
	}
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM public.runtime_artifact WHERE tenant_id=$1 AND artifact_id=$2")).WithArgs("tenant-a", "artifact").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := runtimepostgres.New(db).DeleteArtifact(context.Background(), "tenant-a", "artifact"); err != nil {
		t.Fatalf("DeleteArtifact = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

const searchKnowledgeQuery = "SELECT tenant_id,document_id,content,metadata,embedding,digest,version,created_at,updated_at FROM public.runtime_knowledge WHERE tenant_id=$1 AND embedding <> '[]'::jsonb"

func knowledgeSearchRows(documentID string, metadata, embedding []byte) *sqlmock.Rows {
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return sqlmock.NewRows([]string{"tenant_id", "document_id", "content", "metadata", "embedding", "digest", "version", "created_at", "updated_at"}).
		AddRow("tenant-a", documentID, "content", metadata, embedding, "digest", int64(1), when, when)
}

func TestSearchKnowledgeValidationReturns(t *testing.T) {
	cases := []struct {
		name string
		call func(*runtimepostgres.Store) error
	}{
		{name: "nil context", call: func(store *runtimepostgres.Store) error {
			_, err := store.SearchKnowledge(nil, "tenant-a", []float64{1}, 1)
			return err
		}},
		{name: "invalid tenant", call: func(store *runtimepostgres.Store) error {
			_, err := store.SearchKnowledge(context.Background(), "", []float64{1}, 1)
			return err
		}},
		{name: "empty embedding", call: func(store *runtimepostgres.Store) error {
			_, err := store.SearchKnowledge(context.Background(), "tenant-a", nil, 1)
			return err
		}},
		{name: "negative limit", call: func(store *runtimepostgres.Store) error {
			_, err := store.SearchKnowledge(context.Background(), "tenant-a", []float64{1}, -1)
			return err
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			db, _, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			if err := test.call(runtimepostgres.New(db)); !errors.Is(err, runtimestorage.ErrInvalid) {
				t.Fatalf("SearchKnowledge error = %v", err)
			}
		})
	}
}

func TestSearchKnowledgeNilStoreAndQueryErrors(t *testing.T) {
	t.Run("nil store", func(t *testing.T) {
		var store *runtimepostgres.Store
		_, err := store.SearchKnowledge(context.Background(), "tenant-a", []float64{1}, 1)
		if !errors.Is(err, runtimestorage.ErrStorage) {
			t.Fatalf("SearchKnowledge nil store = %v", err)
		}
	})
	t.Run("query error mapping", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		mock.ExpectQuery(regexp.QuoteMeta(searchKnowledgeQuery)).WithArgs("tenant-a").WillReturnError(sql.ErrNoRows)
		_, err = runtimepostgres.New(db).SearchKnowledge(context.Background(), "tenant-a", []float64{1}, 1)
		if !errors.Is(err, runtimestorage.ErrNotFound) {
			t.Fatalf("SearchKnowledge query error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSearchKnowledgeScanAndRowsErrors(t *testing.T) {
	cases := []struct {
		name string
		rows *sqlmock.Rows
	}{
		{name: "scan error", rows: sqlmock.NewRows([]string{"tenant_id", "document_id"}).AddRow("tenant-a", "doc")},
		{name: "metadata decode error", rows: knowledgeSearchRows("doc", []byte("{"), []byte("[1]"))},
		{name: "embedding decode error", rows: knowledgeSearchRows("doc", []byte("{}"), []byte("["))},
		{name: "rows error", rows: knowledgeSearchRows("doc", []byte("{}"), []byte("[1]")).RowError(0, errors.New("row iteration failed"))},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			mock.ExpectQuery(regexp.QuoteMeta(searchKnowledgeQuery)).WithArgs("tenant-a").WillReturnRows(test.rows)
			_, err = runtimepostgres.New(db).SearchKnowledge(context.Background(), "tenant-a", []float64{1}, 1)
			if !errors.Is(err, runtimestorage.ErrStorage) {
				t.Fatalf("SearchKnowledge %s = %v", test.name, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSearchKnowledgeSortsFiltersAndLimits(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	rows := knowledgeSearchRows("doc-z", []byte("{\"z\":1}"), []byte("[0,1]"))
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows.AddRow("tenant-a", "doc-a", "content", []byte("{\"a\":1}"), []byte("[0,1]"), "digest", int64(1), when, when)
	rows.AddRow("tenant-a", "doc-high", "content", []byte("{\"high\":1}"), []byte("[1,0]"), "digest", int64(1), when, when)
	rows.AddRow("tenant-a", "doc-zero", "content", []byte("{\"zero\":1}"), []byte("[0,0]"), "digest", int64(1), when, when)
	rows.AddRow("tenant-a", "doc-mismatch", "content", []byte("{\"mismatch\":1}"), []byte("[1]"), "digest", int64(1), when, when)
	mock.ExpectQuery(regexp.QuoteMeta(searchKnowledgeQuery)).WithArgs("tenant-a").WillReturnRows(rows)
	values, err := runtimepostgres.New(db).SearchKnowledge(context.Background(), "tenant-a", []float64{1, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || values[0].Document.DocumentID != "doc-high" || values[0].Score != 1 || values[1].Document.DocumentID != "doc-a" || values[1].Score != 0 {
		t.Fatalf("SearchKnowledge ordering/limit = %+v", values)
	}
	if values[1].Document.Metadata["a"] != float64(1) {
		t.Fatalf("SearchKnowledge metadata = %#v", values[1].Document.Metadata)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresAuditVectorAndObjectContracts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.runtime_audit_log")).
		WithArgs("tenant-a", "audit", "event", []byte("{\"ok\":true}"), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "audit_id", "event_type", "payload", "occurred_at"}).AddRow("tenant-a", "audit", "event", []byte("{\"ok\":true}"), when))
	if _, err := runtimepostgres.New(db).AppendAudit(context.Background(), runtimestorage.AuditRecord{TenantID: "tenant-a", AuditID: "audit", EventType: "event", Payload: map[string]any{"ok": true}}); err != nil {
		t.Fatalf("AppendAudit = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,audit_id,event_type,payload,occurred_at FROM public.runtime_audit_log WHERE tenant_id=$1 AND ($2::timestamptz IS NULL OR occurred_at >= $2) ORDER BY occurred_at,audit_id LIMIT NULLIF($3,0)")).
		WithArgs("tenant-a", nil, 10).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "audit_id", "event_type", "payload", "occurred_at"}).AddRow("tenant-a", "audit", "event", []byte("{\"ok\":true}"), when))
	if values, err := runtimepostgres.New(db).ListAudit(context.Background(), "tenant-a", time.Time{}, 10); err != nil || len(values) != 1 {
		t.Fatalf("ListAudit = %+v, %v", values, err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,audit_id,event_type,payload,occurred_at FROM public.runtime_audit_log WHERE tenant_id=$1 AND ($2::timestamptz IS NULL OR occurred_at >= $2) ORDER BY occurred_at,audit_id LIMIT NULLIF($3,0)")).
		WithArgs("tenant-a", when, 10).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "audit_id", "event_type", "payload", "occurred_at"}))
	if values, err := runtimepostgres.New(db).ListAudit(context.Background(), "tenant-a", when, 10); err != nil || len(values) != 0 {
		t.Fatalf("filtered ListAudit = %+v, %v", values, err)
	}
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM public.runtime_vector_index WHERE tenant_id=$1 AND document_id=$2")).WithArgs("tenant-a", "doc").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := runtimepostgres.New(db).DeleteVector(context.Background(), "tenant-a", "doc"); err != nil {
		t.Fatalf("DeleteVector = %v", err)
	}
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO public.runtime_vector_index")).
		WithArgs("tenant-a", runtimestorage.VectorSourceGeneric, "doc", "content", []byte("{}"), []byte("[1,0]"), int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := runtimepostgres.New(db).UpsertVector(context.Background(), runtimestorage.VectorRecord{TenantID: "tenant-a", DocumentID: "doc", Content: "content", Embedding: []float64{1, 0}}); err != nil {
		t.Fatalf("UpsertVector = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.runtime_object")).
		WithArgs("tenant-a", "object", "text/plain", []byte("data"), 4, sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "object_key", "content_type", "size", "etag", "created_at"}).AddRow("tenant-a", "object", "text/plain", int64(4), "etag", when))
	if _, err := runtimepostgres.New(db).PutObject(context.Background(), "tenant-a", "object", strings.NewReader("data"), "text/plain"); err != nil {
		t.Fatalf("PutObject = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,object_key,content_type,content,size,etag,created_at FROM public.runtime_object WHERE tenant_id=$1 AND object_key=$2")).WithArgs("tenant-a", "object").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "object_key", "content_type", "content", "size", "etag", "created_at"}).AddRow("tenant-a", "object", "text/plain", []byte("data"), int64(4), "etag", when))
	if body, _, err := runtimepostgres.New(db).GetObject(context.Background(), "tenant-a", "object"); err != nil {
		t.Fatalf("GetObject = %v", err)
	} else {
		_ = body.Close()
	}
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM public.runtime_object WHERE tenant_id=$1 AND object_key=$2")).WithArgs("tenant-a", "object").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := runtimepostgres.New(db).DeleteObject(context.Background(), "tenant-a", "object"); err != nil {
		t.Fatalf("DeleteObject = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
