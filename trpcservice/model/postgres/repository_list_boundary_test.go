package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
)

func TestModelRepositoryListFailureAndPaginationBranches(t *testing.T) {
	profile, catalog := newPostgresListModelProfile(t)
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
			mock.ExpectQuery(`FROM public\.model_profile WHERE tenant_id=\$1`).WithArgs(profile.TenantID).WillReturnError(errors.New("query"))
		}, wantErr: ErrStorage},
		{name: "scan error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`FROM public\.model_profile WHERE tenant_id=\$1`).WithArgs(profile.TenantID).
				WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow(profile.TenantID)).RowsWillBeClosed()
		}, wantErr: ErrStorage},
		{name: "iteration error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`FROM public\.model_profile WHERE tenant_id=\$1`).WithArgs(profile.TenantID).
				WillReturnRows(testModelProfileRows(t, profile).RowError(0, errors.New("iteration"))).RowsWillBeClosed()
		}, wantErr: ErrStorage},
		{name: "invalid stored profile", setup: func(mock sqlmock.Sqlmock) {
			invalid := profile.Clone()
			invalid.Status = model.Status("unknown")
			mock.ExpectQuery(`FROM public\.model_profile WHERE tenant_id=\$1`).WithArgs(profile.TenantID).
				WillReturnRows(testModelProfileRows(t, &invalid)).RowsWillBeClosed()
		}, wantErr: ErrStorage},
		{name: "close error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`FROM public\.model_profile WHERE tenant_id=\$1`).WithArgs(profile.TenantID).
				WillReturnRows(testModelProfileRows(t, profile).CloseError(errors.New("close"))).RowsWillBeClosed()
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

	second := profile.Clone()
	second.ProfileKey = "secondary"
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery(`FROM public\.model_profile WHERE tenant_id=\$1`).WithArgs(profile.TenantID).
		WillReturnRows(testModelProfileRows(t, profile, &second)).RowsWillBeClosed()
	items, next, err := NewRepository(db, catalog).List(ctx, profile.TenantID, "", "", "", 1)
	if err != nil || len(items) != 1 || next != "1" {
		t.Fatalf("paginated page = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery(`FROM public\.model_profile WHERE tenant_id=\$1`).WithArgs(profile.TenantID).
		WillReturnRows(testModelProfileRows(t, profile)).RowsWillBeClosed()
	items, next, err = NewRepository(db, catalog).List(ctx, profile.TenantID, "", "", "1", 1)
	if err != nil || items == nil || len(items) != 0 || next != "" {
		t.Fatalf("past-end page = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := NewRepository(nil, nil).List(ctx, profile.TenantID, "", "", "", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil-storage List error = %v", err)
	}
}

func newPostgresListModelProfile(t *testing.T) (*model.Profile, *model.ProviderCatalog) {
	t.Helper()
	catalog, err := model.NewProviderCatalog(model.ProviderSpec{
		Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional,
		EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"}, SecretRefPolicy: model.FieldOptional,
		Options: map[string]model.OptionSpec{"mode": {Kind: model.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := model.NewProfile(model.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "primary", DisplayName: "Primary", Status: model.StatusActive,
		SchemaVersion: model.SchemaVersionV1, Configuration: model.Configuration{Provider: "public", Model: "chat", Options: map[string]string{"mode": "safe"}},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	return profile, catalog
}
