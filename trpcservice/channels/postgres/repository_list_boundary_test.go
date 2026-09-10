package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestChannelRepositoryListFailureAndPaginationBranches(t *testing.T) {
	binding := newStoredChannelBinding(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		setup   func(sqlmock.Sqlmock)
		cursor  string
		wantErr error
	}{
		{name: "invalid cursor", cursor: "bad"},
		{name: "negative cursor", cursor: "-1"},
		{name: "query error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT binding_id FROM public\.channel_binding`).WithArgs(binding.TenantID).WillReturnError(errors.New("query"))
		}, wantErr: ErrStorage},
		{name: "scan error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT binding_id FROM public\.channel_binding`).WithArgs(binding.TenantID).
				WillReturnRows(sqlmock.NewRows([]string{"binding_id"}).AddRow(nil)).RowsWillBeClosed()
		}, wantErr: ErrStorage},
		{name: "iteration error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT binding_id FROM public\.channel_binding`).WithArgs(binding.TenantID).
				WillReturnRows(sqlmock.NewRows([]string{"binding_id"}).AddRow(binding.BindingID).RowError(0, errors.New("iteration"))).RowsWillBeClosed()
		}, wantErr: ErrStorage},
		{name: "close error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT binding_id FROM public\.channel_binding`).WithArgs(binding.TenantID).
				WillReturnRows(sqlmock.NewRows([]string{"binding_id"}).AddRow(binding.BindingID).CloseError(errors.New("close"))).RowsWillBeClosed()
		}, wantErr: ErrStorage},
		{name: "load error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT binding_id FROM public\.channel_binding`).WithArgs(binding.TenantID).
				WillReturnRows(sqlmock.NewRows([]string{"binding_id"}).AddRow(binding.BindingID)).RowsWillBeClosed()
			mock.ExpectQuery(".*").WithArgs(binding.TenantID, binding.BindingID).WillReturnError(errors.New("load"))
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
			_, _, callErr := NewRepository(db).List(ctx, binding.TenantID, "", "", tc.cursor, 1)
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

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery(`SELECT binding_id FROM public\.channel_binding`).WithArgs(binding.TenantID).
		WillReturnRows(sqlmock.NewRows([]string{"binding_id"}).AddRow(binding.BindingID)).RowsWillBeClosed()
	mock.ExpectQuery(".*").WithArgs(binding.TenantID, binding.BindingID).WillReturnRows(testChannelBindingRows(t, binding))
	items, next, err := NewRepository(db).List(ctx, binding.TenantID, "account", string(binding.Status), "", 0)
	if err != nil || len(items) != 1 || next != "" {
		t.Fatalf("default page = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestScanBindingIDsBranches(t *testing.T) {
	for _, tc := range []struct {
		name    string
		rows    *sqlmock.Rows
		wantErr bool
	}{
		{name: "values", rows: sqlmock.NewRows([]string{"binding_id"}).AddRow("one").AddRow("two")},
		{name: "scan", rows: sqlmock.NewRows([]string{"binding_id"}).AddRow(nil), wantErr: true},
		{name: "iteration", rows: sqlmock.NewRows([]string{"binding_id"}).AddRow("one").RowError(0, errors.New("iteration")), wantErr: true},
		{name: "close", rows: sqlmock.NewRows([]string{"binding_id"}).AddRow("one").CloseError(errors.New("close")), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			mock.ExpectQuery("SELECT binding_id").WillReturnRows(tc.rows).RowsWillBeClosed()
			rows, err := db.QueryContext(context.Background(), "SELECT binding_id")
			if err != nil {
				t.Fatal(err)
			}
			ids, err := scanBindingIDs(rows)
			if tc.wantErr && err == nil {
				t.Fatalf("ids=%v, want error", ids)
			}
			if !tc.wantErr && (err != nil || len(ids) != 2) {
				t.Fatalf("ids=%v err=%v", ids, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
	if _, _, err := NewRepository(nil).List(context.Background(), "tenant", "", "", "", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil-storage List error = %v", err)
	}
	if _, err := NewRepository(nil).Get(context.Background(), "tenant", "binding"); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil-storage Get error = %v", err)
	}
}
