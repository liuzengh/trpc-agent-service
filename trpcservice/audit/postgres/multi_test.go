package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
)

func TestMultiTenantStoreRoutesAndCachesTenantStores(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	event := testEvent("tenant-a")
	digest, err := event.Digest()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		mock.ExpectBegin()
		mock.ExpectExec("SELECT set_config").WithArgs("tenant-a").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery("SELECT \\* FROM public\\.audit_append_event").WillReturnRows(sqlmock.NewRows([]string{"stored_digest", "duplicate", "conflict"}).AddRow(digest, i == 1, false))
		mock.ExpectCommit()
	}
	store := NewMultiTenant(db)
	first, err := store.Append(context.Background(), event)
	if err != nil || first.Digest != digest || first.Duplicate {
		t.Fatalf("first append = %+v, %v", first, err)
	}
	second, err := store.Append(context.Background(), event)
	if err != nil || !second.Duplicate {
		t.Fatalf("second append = %+v, %v", second, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMultiTenantStoreRejectsInvalidInputs(t *testing.T) {
	var nilStore *MultiTenantStore
	if _, err := nilStore.Append(context.Background(), testEvent("tenant-a")); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil store Append() = %v", err)
	}
	store := NewMultiTenant(nil)
	if _, err := store.Append(context.Background(), testEvent("tenant-a")); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil database Append() = %v", err)
	}
	if _, err := NewMultiTenant(nil).Append(context.Background(), audit.Event{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("empty tenant Append() = %v", err)
	}
}
