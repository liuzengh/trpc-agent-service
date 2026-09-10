package mysql

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
)

func TestBackendRepositoryListCoversFilteringPagingAndLoadReturns(t *testing.T) {
	catalog, err := backend.NewProviderCatalog(backend.ProviderSpec{Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}}})
	if err != nil {
		t.Fatal(err)
	}
	first, err := backend.NewProfile(backend.CreateInput{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "primary", DisplayName: "Primary", Status: backend.StatusActive, SchemaVersion: 1, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "safe"}}}}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	second := first.Clone()
	second.ProfileID = "bp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	second.ProfileKey, second.DisplayName = "secondary", "Secondary"
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(`SELECT profile_id FROM backend_profile WHERE tenant_id = \? ORDER BY profile_id`).WithArgs(first.TenantID).WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow(first.ProfileID).AddRow(second.ProfileID))
	mock.ExpectQuery(".*").WithArgs(first.TenantID, first.ProfileID).WillReturnRows(testBackendRootRows(t, first))
	mock.ExpectQuery(".*").WithArgs(first.TenantID, first.ProfileID).WillReturnRows(testBackendBindingRows(t, first))
	mock.ExpectQuery(".*").WithArgs(first.TenantID, second.ProfileID).WillReturnRows(testBackendRootRows(t, &second))
	mock.ExpectQuery(".*").WithArgs(first.TenantID, second.ProfileID).WillReturnRows(testBackendBindingRows(t, &second))
	items, next, err := NewRepository(db, catalog).List(ctx, first.TenantID, "", "", "", 1)
	if err != nil || len(items) != 1 || next == "" {
		t.Fatalf("first page = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(`SELECT profile_id FROM backend_profile WHERE tenant_id = \? ORDER BY profile_id`).WithArgs(first.TenantID).WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow(first.ProfileID))
	mock.ExpectQuery(".*").WithArgs(first.TenantID, first.ProfileID).WillReturnRows(testBackendRootRows(t, first))
	mock.ExpectQuery(".*").WithArgs(first.TenantID, first.ProfileID).WillReturnRows(testBackendBindingRows(t, first))
	items, next, err = NewRepository(db, catalog).List(ctx, first.TenantID, "secondary", "", "", 0)
	if err != nil || len(items) != 0 || next != "" {
		t.Fatalf("filtered page = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(`SELECT profile_id FROM backend_profile WHERE tenant_id = \? ORDER BY profile_id`).WithArgs(first.TenantID).WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow(first.ProfileID))
	mock.ExpectQuery(".*").WithArgs(first.TenantID, first.ProfileID).WillReturnError(errors.New("load"))
	if _, _, err := NewRepository(db, catalog).List(ctx, first.TenantID, "", "", "", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("load error = %v", err)
	}
}
