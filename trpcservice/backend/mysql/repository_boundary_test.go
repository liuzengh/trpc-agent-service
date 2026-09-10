package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
)

func TestBackendRepositoryRejectsCancelledContextsBeforeStorage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := NewRepository(nil, nil)
	cases := []struct {
		name string
		call func() error
	}{
		{"create", func() error { _, _, err := r.Create(ctx, backend.CreateInput{}); return err }},
		{"get", func() error { _, err := r.Get(ctx, "tenant", "profile"); return err }},
		{"list", func() error { _, _, err := r.List(ctx, "tenant", "", "", "", 1); return err }},
		{"update", func() error { _, _, err := r.UpdateConfiguration(ctx, backend.UpdateConfigurationInput{}); return err }},
		{"transition", func() error { _, _, err := r.TransitionStatus(ctx, backend.TransitionStatusInput{}); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestBackendRepositoryListBoundaryBranches(t *testing.T) {
	ctx := context.Background()
	if _, _, err := NewRepository(nil, nil).List(ctx, "tenant", "", "", "", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil-storage List error = %v", err)
	}
	catalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden,
		SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := backend.NewProfile(backend.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "primary", DisplayName: "Primary", Status: backend.StatusActive,
		SchemaVersion: 1,
		Bindings:      []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "safe"}}},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name        string
		setup       func(sqlmock.Sqlmock)
		wantStorage bool
		wantNoMatch bool
	}{
		{"invalid cursor", func(sqlmock.Sqlmock) {}, false, false},
		{"query error", func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery("SELECT profile_id FROM backend_profile").WithArgs(profile.TenantID).WillReturnError(errors.New("list query"))
		}, true, false},
		{"rows error", func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery("SELECT profile_id FROM backend_profile").WithArgs(profile.TenantID).WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow(profile.ProfileID).RowError(0, errors.New("rows")))
		}, true, false},
		{"filter no-match", func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery("SELECT profile_id FROM backend_profile").WithArgs(profile.TenantID).WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow(profile.ProfileID))
			mock.ExpectQuery(".*").WithArgs(profile.TenantID, profile.ProfileID).WillReturnRows(testBackendRootRows(t, profile))
			mock.ExpectQuery(".*").WithArgs(profile.TenantID, profile.ProfileID).WillReturnRows(testBackendBindingRows(t, profile))
		}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			tc.setup(mock)
			cursor, query := "", ""
			if tc.name == "invalid cursor" {
				cursor = "bad"
			}
			if tc.wantNoMatch {
				query = "missing"
			}
			items, _, callErr := NewRepository(db, catalog).List(ctx, profile.TenantID, query, "", cursor, 1)
			if tc.wantStorage && !errors.Is(callErr, ErrStorage) {
				t.Fatalf("error = %v", callErr)
			}
			if !tc.wantStorage && tc.name == "invalid cursor" && callErr == nil {
				t.Fatal("invalid cursor was accepted")
			}
			if tc.wantNoMatch && (callErr != nil || len(items) != 0) {
				t.Fatalf("no-match result = items=%+v err=%v", items, callErr)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReplaceBackendBindingsClearsEmptyBindingSet(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectExec("DELETE FROM backend_profile_binding WHERE tenant_id = \\? AND profile_id = \\?").
		WithArgs("tenant", "profile").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := replaceBackendBindings(context.Background(), db, backend.Profile{TenantID: "tenant", ProfileID: "profile"}); err != nil {
		t.Fatalf("replace empty bindings = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestBackendRepositoryBoundaryWriteFailures(t *testing.T) {
	catalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession},
		EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden,
		Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := backend.CreateInput{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "boundary", DisplayName: "Boundary", Status: backend.StatusActive, SchemaVersion: 1,
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "safe"}}},
		Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "boundary", CorrelationID: "backend-boundary"}}
	profile, err := backend.NewProfile(input, catalog)
	if err != nil {
		t.Fatal(err)
	}
	update := backend.UpdateConfigurationInput{TenantID: profile.TenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version, DisplayName: profile.DisplayName, SchemaVersion: profile.SchemaVersion, Bindings: profile.Bindings, Metadata: input.Metadata}
	newDB := func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db, mock
	}

	t.Run("create last insert id", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewErrorResult(errors.New("last insert id")))
		mock.ExpectRollback()
		if _, _, err := NewRepository(db, catalog).Create(context.Background(), input); !errors.Is(err, ErrStorage) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("update rows affected", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(testBackendRootRows(t, profile))
		mock.ExpectQuery(".*").WillReturnRows(testBackendBindingRows(t, profile))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected")))
		mock.ExpectRollback()
		if _, _, err := NewRepository(db, catalog).UpdateConfiguration(context.Background(), update); !errors.Is(err, backend.ErrConflict) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("update readback", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(testBackendRootRows(t, profile))
		mock.ExpectQuery(".*").WillReturnRows(testBackendBindingRows(t, profile))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(12, 1))
		mock.ExpectQuery(".*").WillReturnError(errors.New("readback"))
		mock.ExpectRollback()
		if _, _, err := NewRepository(db, catalog).UpdateConfiguration(context.Background(), update); !errors.Is(err, ErrStorage) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("update commit", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(testBackendRootRows(t, profile))
		mock.ExpectQuery(".*").WillReturnRows(testBackendBindingRows(t, profile))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(12, 1))
		mock.ExpectQuery(".*").WillReturnRows(testBackendRootRows(t, profile))
		mock.ExpectQuery(".*").WillReturnRows(testBackendBindingRows(t, profile))
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_type", "tenant_id", "profile_id", "previous_status", "current_status", "previous_digest", "current_digest", "actor_type", "actor_id", "reason", "correlation_id", "previous_version", "next_version", "occurred_at"}).AddRow("configuration_updated", profile.TenantID, profile.ProfileID, string(profile.Status), string(profile.Status), profile.ContentDigest, profile.ContentDigest, "test", "user", "boundary", "backend-boundary", profile.Version, profile.Version+1, profile.UpdatedAt))
		mock.ExpectCommit().WillReturnError(errors.New("commit"))
		if _, _, err := NewRepository(db, catalog).UpdateConfiguration(context.Background(), update); !errors.Is(err, ErrStorage) {
			t.Fatalf("error = %v", err)
		}
	})
}
