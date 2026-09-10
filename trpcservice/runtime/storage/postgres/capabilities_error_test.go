package postgres_test

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/postgres"
)

func TestDeleteMemoryMapsUpdateFailureAndRollsBack(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE public.runtime_memory SET deleted_at=now(),version=version+1,updated_at=now() WHERE tenant_id=$1 AND memory_id=$2 AND deleted_at IS NULL")).
		WithArgs("tenant-a", "memory").
		WillReturnError(errors.New("memory update failed"))
	mock.ExpectRollback()

	err = runtimepostgres.New(db).DeleteMemory(context.Background(), "tenant-a", "memory")
	if !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("DeleteMemory update error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteMemoryMapsProjectionFailureAndRollsBack(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE public.runtime_memory SET deleted_at=now(),version=version+1,updated_at=now() WHERE tenant_id=$1 AND memory_id=$2 AND deleted_at IS NULL")).
		WithArgs("tenant-a", "memory").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM public.runtime_vector_index WHERE tenant_id=$1 AND source=$2 AND document_id=$3")).
		WithArgs("tenant-a", runtimestorage.VectorSourceMemory, "memory").
		WillReturnError(errors.New("memory projection cleanup failed"))
	mock.ExpectRollback()

	err = runtimepostgres.New(db).DeleteMemory(context.Background(), "tenant-a", "memory")
	if !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("DeleteMemory projection error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEnqueueMemoryIndexMapsProjectionCleanupFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO public.runtime_vector_index")).
		WithArgs("tenant-a", "memory", int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,memory_id,user_id,session_id,content,topics,metadata,embedding,version,deleted_at,created_at,updated_at FROM public.runtime_memory WHERE tenant_id=$1 AND memory_id=$2 AND deleted_at IS NULL")).
		WithArgs("tenant-a", "memory").
		WillReturnRows(memoryRows().AddRow("tenant-a", "memory", "user", "", "content", []byte("[]"), []byte("{}"), []byte("[]"), int64(1), nil, when, when))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM public.runtime_vector_index WHERE tenant_id=$1 AND source=$2 AND document_id=$3")).
		WithArgs("tenant-a", runtimestorage.VectorSourceMemory, "memory").
		WillReturnError(errors.New("stale memory projection cleanup failed"))

	err = runtimepostgres.New(db).EnqueueMemoryIndex(context.Background(), runtimestorage.MemoryRecord{TenantID: "tenant-a", MemoryID: "memory", Version: 1})
	if !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("EnqueueMemoryIndex cleanup error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteKnowledgeMapsDeleteFailureAndRollsBack(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM public.runtime_knowledge WHERE tenant_id=$1 AND document_id=$2")).
		WithArgs("tenant-a", "document").
		WillReturnError(errors.New("knowledge delete failed"))
	mock.ExpectRollback()

	err = runtimepostgres.New(db).DeleteKnowledge(context.Background(), "tenant-a", "document")
	if !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("DeleteKnowledge delete error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteKnowledgeMapsProjectionFailureAndRollsBack(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM public.runtime_knowledge WHERE tenant_id=$1 AND document_id=$2")).
		WithArgs("tenant-a", "document").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM public.runtime_vector_index WHERE tenant_id=$1 AND source=$2 AND document_id=$3")).
		WithArgs("tenant-a", runtimestorage.VectorSourceKnowledge, "document").
		WillReturnError(errors.New("knowledge projection cleanup failed"))
	mock.ExpectRollback()

	err = runtimepostgres.New(db).DeleteKnowledge(context.Background(), "tenant-a", "document")
	if !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("DeleteKnowledge projection error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
