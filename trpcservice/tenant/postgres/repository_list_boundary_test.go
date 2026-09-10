package postgres

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

//nolint:gocyclo // Exercises every list failure and pagination branch.
func TestTenantRepositoryListFailureAndPaginationBranches(t *testing.T) {
	value, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "list-boundary", DisplayName: "List Boundary", Status: tenant.StatusActive,
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		setup   func(sqlmock.Sqlmock)
		cursor  string
		wantErr error
	}{
		{name: "invalid cursor", cursor: "bad"},
		{name: "negative cursor", cursor: "-1"},
		{name: "trailing data", cursor: "1junk"},
		{name: "overflow", cursor: "999999999999999999999999999"},
		{name: "query error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT .* FROM public\.tenant`).WithArgs(value.TenantID, 2, 0).WillReturnError(errors.New("query"))
		}, wantErr: ErrStorage},
		{name: "scan error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT .* FROM public\.tenant`).WithArgs(value.TenantID, 2, 0).
				WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow(value.TenantID)).RowsWillBeClosed()
		}, wantErr: ErrStorage},
		{name: "iteration error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT .* FROM public\.tenant`).WithArgs(value.TenantID, 2, 0).
				WillReturnRows(testTenantRows(value).RowError(0, errors.New("iteration"))).RowsWillBeClosed()
		}, wantErr: ErrStorage},
		{name: "close error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT .* FROM public\.tenant`).WithArgs(value.TenantID, 2, 0).
				WillReturnRows(testTenantRows(value).CloseError(errors.New("close"))).RowsWillBeClosed()
		}, wantErr: ErrStorage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			if tc.setup != nil {
				tc.setup(mock)
			}
			_, _, callErr := NewRepository(db).List(ctx, []string{value.TenantID}, "", "", tc.cursor, 1)
			if tc.wantErr == nil {
				if callErr == nil {
					t.Fatal("invalid cursor was accepted")
				}
			} else if !errors.Is(callErr, tc.wantErr) {
				t.Fatalf("error = %v, want %v", callErr, tc.wantErr)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}

	second, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "list-boundary-secondary", DisplayName: "List Boundary Secondary", Status: tenant.StatusSuspended,
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery(`SELECT .* FROM public\.tenant`).WithArgs(value.TenantID, 2, 0).
		WillReturnRows(testTenantRows(value, second)).RowsWillBeClosed()
	items, next, err := NewRepository(db).List(ctx, []string{value.TenantID}, "", "", "", 1)
	if err != nil || len(items) != 1 || next != "1" {
		t.Fatalf("paginated tenant list = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery(`SELECT .* FROM public\.tenant`).WithArgs(value.TenantID, 2, 1).
		WillReturnRows(testTenantRows()).RowsWillBeClosed()
	items, next, err = NewRepository(db).List(ctx, []string{value.TenantID}, "", "", "1", 1)
	if err != nil || items == nil || len(items) != 0 || next != "" {
		t.Fatalf("past-end tenant list = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := NewRepository(nil).List(ctx, []string{value.TenantID}, "", "", "", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil-storage List error = %v", err)
	}
}

func TestTenantListAppliesFiltersBeforeSQLPagination(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	query := tenantSelect + " WHERE tenant_id IN ($1, $2)  AND status = $3 AND STRPOS(LOWER(tenant_id || ' ' || tenant_key || ' ' || display_name), $4) > 0 ORDER BY tenant_id LIMIT $5 OFFSET $6"
	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("tenant-a", "tenant-b", "active", "name%_'", 201, 2).
		WillReturnRows(testTenantRows()).RowsWillBeClosed()
	items, next, err := NewRepository(db).List(context.Background(), []string{"tenant-b", " tenant-a ", "tenant-a"}, " NAME%_' ", "active", "2", 1000)
	if err != nil || items == nil || len(items) != 0 || next != "" {
		t.Fatalf("filtered page = %v, %q, %v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTenantListRejectsInvalidContextBeforeScopeOrStorage(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, scopes := range [][]string{nil, {"tenant-a"}, {"*"}} {
		if _, _, err := NewRepository(nil).List(canceled, scopes, "", "", "", 1); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled context with scopes %v: %v", scopes, err)
		}
		var missing context.Context
		if _, _, err := NewRepository(nil).List(missing, scopes, "", "", "", 1); !errors.Is(err, ErrStorage) {
			t.Fatalf("nil context with scopes %v: %v", scopes, err)
		}
	}
}
