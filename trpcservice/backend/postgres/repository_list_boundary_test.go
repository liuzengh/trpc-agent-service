package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
)

//nolint:gocyclo // Exercises every list failure and pagination branch.
func TestBackendRepositoryListFailureAndPaginationBranches(t *testing.T) {
	catalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession},
		EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden,
		Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := backend.NewProfile(backend.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "primary", DisplayName: "Primary", Status: backend.StatusActive,
		SchemaVersion: 1, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "safe"}}},
	}, catalog)
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
		{name: "query error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT profile_id FROM public\.backend_profile`).WithArgs(profile.TenantID).WillReturnError(errors.New("query"))
		}, wantErr: ErrStorage},
		{name: "scan error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT profile_id FROM public\.backend_profile`).WithArgs(profile.TenantID).
				WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow(12)).RowsWillBeClosed()
		}, wantErr: ErrStorage},
		{name: "iteration error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT profile_id FROM public\.backend_profile`).WithArgs(profile.TenantID).
				WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow(profile.ProfileID).RowError(0, errors.New("iteration"))).RowsWillBeClosed()
		}, wantErr: ErrStorage},
		{name: "close error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT profile_id FROM public\.backend_profile`).WithArgs(profile.TenantID).
				WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow(profile.ProfileID).CloseError(errors.New("close"))).RowsWillBeClosed()
		}, wantErr: ErrStorage},
		{name: "load error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT profile_id FROM public\.backend_profile`).WithArgs(profile.TenantID).
				WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow(profile.ProfileID)).RowsWillBeClosed()
			mock.ExpectQuery(".*").WithArgs(profile.TenantID, profile.ProfileID).WillReturnError(errors.New("load"))
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
			_, _, callErr := NewRepository(db, catalog).List(ctx, profile.TenantID, "", "", tc.cursor, 1)
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
	mock.ExpectQuery(`SELECT profile_id FROM public\.backend_profile`).WithArgs(profile.TenantID).
		WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow(profile.ProfileID)).RowsWillBeClosed()
	mock.ExpectQuery(".*").WithArgs(profile.TenantID, profile.ProfileID).WillReturnRows(testBackendRootRows(t, profile))
	mock.ExpectQuery(".*").WithArgs(profile.TenantID, profile.ProfileID).WillReturnRows(testBackendBindingRows(t, profile))
	items, next, err := NewRepository(db, catalog).List(ctx, profile.TenantID, "primary", string(profile.Status), "", 0)
	if err != nil || len(items) != 1 || next != "" {
		t.Fatalf("default page = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery(`SELECT profile_id FROM public\.backend_profile`).WithArgs(profile.TenantID).
		WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow(profile.ProfileID)).RowsWillBeClosed()
	mock.ExpectQuery(".*").WithArgs(profile.TenantID, profile.ProfileID).WillReturnRows(testBackendRootRows(t, profile))
	mock.ExpectQuery(".*").WithArgs(profile.TenantID, profile.ProfileID).WillReturnRows(testBackendBindingRows(t, profile))
	items, next, err = NewRepository(db, catalog).List(ctx, profile.TenantID, "", "", "1", 1)
	if err != nil || items == nil || len(items) != 0 || next != "" {
		t.Fatalf("past-end page = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestScanProfileIDsBranches(t *testing.T) {
	for _, tc := range []struct {
		name    string
		rows    *sqlmock.Rows
		wantErr bool
	}{
		{name: "values", rows: sqlmock.NewRows([]string{"profile_id"}).AddRow("one").AddRow("two")},
		{name: "scan", rows: sqlmock.NewRows([]string{"profile_id"}).AddRow(nil), wantErr: true},
		{name: "iteration", rows: sqlmock.NewRows([]string{"profile_id"}).AddRow("one").RowError(0, errors.New("iteration")), wantErr: true},
		{name: "close", rows: sqlmock.NewRows([]string{"profile_id"}).AddRow("one").CloseError(errors.New("close")), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			mock.ExpectQuery("SELECT profile_id").WillReturnRows(tc.rows).RowsWillBeClosed()
			rows, err := db.QueryContext(context.Background(), "SELECT profile_id")
			if err != nil {
				t.Fatal(err)
			}
			ids, err := scanProfileIDs(rows)
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
	if _, _, err := NewRepository(nil, nil).List(context.Background(), "tenant", "", "", "", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil-storage List error = %v", err)
	}
}
