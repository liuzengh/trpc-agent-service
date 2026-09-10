package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
)

func TestModelRepositoryRejectsCancelledContextsBeforeStorage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := NewRepository(nil, nil)
	cases := []struct {
		name string
		call func() error
	}{
		{"create", func() error { _, _, err := r.Create(ctx, model.CreateInput{}); return err }},
		{"get", func() error { _, err := r.Get(ctx, "tenant", "profile"); return err }},
		{"list", func() error { _, _, err := r.List(ctx, "tenant", "", "", "", 1); return err }},
		{"update", func() error { _, _, err := r.UpdateConfiguration(ctx, model.UpdateConfigurationInput{}); return err }},
		{"transition", func() error { _, _, err := r.TransitionStatus(ctx, model.TransitionStatusInput{}); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestModelRepositoryListBoundaryBranches(t *testing.T) {
	ctx := context.Background()
	profile, catalog := newListModelProfile(t)
	if _, _, err := NewRepository(nil, nil).List(ctx, "tenant", "", "", "", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil-storage List error = %v", err)
	}
	for _, tc := range []struct {
		name        string
		setup       func(sqlmock.Sqlmock)
		wantStorage bool
	}{
		{"invalid cursor", func(sqlmock.Sqlmock) {}, false},
		{"query error", func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery("FROM model_profile WHERE tenant_id").WithArgs("tenant").WillReturnError(errors.New("list query"))
		}, true},
		{"rows error", func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery("FROM model_profile WHERE tenant_id").WithArgs("tenant").WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow("profile").RowError(0, errors.New("rows")))
		}, true},
		{"filter no-match", func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery("FROM model_profile WHERE tenant_id").WithArgs(profile.TenantID).WillReturnRows(testModelProfileRows(t, profile))
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			tc.setup(mock)
			cursor := ""
			if tc.name == "invalid cursor" {
				cursor = "bad"
			}
			query, tenantID := "", "tenant"
			if tc.name == "filter no-match" {
				query, tenantID = "missing", "t_01ARZ3NDEKTSV4RRFFQ69G5FAW"
			}
			items, _, callErr := NewRepository(db, catalog).List(ctx, tenantID, query, "", cursor, 1)
			if tc.wantStorage && !errors.Is(callErr, ErrStorage) {
				t.Fatalf("error = %v", callErr)
			}
			if !tc.wantStorage && tc.name == "invalid cursor" && callErr == nil {
				t.Fatal("invalid cursor was accepted")
			}
			if !tc.wantStorage && tc.name == "filter no-match" && (callErr != nil || len(items) != 0) {
				t.Fatalf("no-match result = items=%+v err=%v", items, callErr)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func newListModelProfile(t *testing.T) (*model.Profile, *model.ProviderCatalog) {
	t.Helper()
	catalog, err := model.NewProviderCatalog(model.ProviderSpec{Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional, EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"}, SecretRefPolicy: model.FieldOptional})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := model.NewProfile(model.CreateInput{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "primary", DisplayName: "Primary", Status: model.StatusActive, SchemaVersion: model.SchemaVersionV1, Configuration: model.Configuration{Provider: "public", Model: "chat"}}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	return profile, catalog
}

func TestModelRepositoryBoundaryWriteFailures(t *testing.T) {
	catalog, err := model.NewProviderCatalog(model.ProviderSpec{Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional, EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"}, SecretRefPolicy: model.FieldOptional, Options: map[string]model.OptionSpec{"mode": {Kind: model.OptionString}}})
	if err != nil {
		t.Fatal(err)
	}
	input := model.CreateInput{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "boundary", DisplayName: "Boundary", Status: model.StatusActive, SchemaVersion: model.SchemaVersionV1, Configuration: model.Configuration{Provider: "public", Model: "chat", Options: map[string]string{"mode": "safe"}}, Metadata: model.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "boundary", CorrelationID: "model-boundary"}}
	profile, err := model.NewProfile(input, catalog)
	if err != nil {
		t.Fatal(err)
	}
	update := model.UpdateConfigurationInput{TenantID: profile.TenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version, DisplayName: profile.DisplayName, SchemaVersion: profile.SchemaVersion, Configuration: profile.Configuration, Metadata: input.Metadata}
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
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewErrorResult(errors.New("last insert id")))
		mock.ExpectRollback()
		if _, _, err := NewRepository(db, catalog).Create(context.Background(), input); !errors.Is(err, ErrStorage) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("update rows affected", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(testModelProfileRows(t, profile))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected")))
		mock.ExpectRollback()
		if _, _, err := NewRepository(db, catalog).UpdateConfiguration(context.Background(), update); !errors.Is(err, model.ErrConflict) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("update readback", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(testModelProfileRows(t, profile))
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
		mock.ExpectQuery(".*").WillReturnRows(testModelProfileRows(t, profile))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(12, 1))
		mock.ExpectQuery(".*").WillReturnRows(testModelProfileRows(t, profile))
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_type", "tenant_id", "profile_id", "previous_status", "current_status", "previous_digest", "current_digest", "actor_type", "actor_id", "reason", "correlation_id", "previous_version", "next_version", "occurred_at"}).AddRow("configuration_updated", profile.TenantID, profile.ProfileID, string(profile.Status), string(profile.Status), profile.ContentDigest, profile.ContentDigest, "test", "user", "boundary", "model-boundary", profile.Version, profile.Version+1, profile.UpdatedAt))
		mock.ExpectCommit().WillReturnError(errors.New("commit"))
		if _, _, err := NewRepository(db, catalog).UpdateConfiguration(context.Background(), update); !errors.Is(err, ErrStorage) {
			t.Fatalf("error = %v", err)
		}
	})
}
