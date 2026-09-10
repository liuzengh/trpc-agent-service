package mysql

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestTenantRepositoryRejectsCancelledContextsBeforeStorage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := NewRepository(nil)
	cases := []struct {
		name string
		call func() error
	}{
		{"create", func() error { _, err := r.Create(ctx, tenant.CreateInput{}); return err }},
		{"create first", func() error { _, _, err := r.CreateFirst(ctx, tenant.CreateInput{}); return err }},
		{"get", func() error { _, err := r.Get(ctx, "tenant"); return err }},
		{"list", func() error { _, _, err := r.List(ctx, []string{"tenant"}, "", "", "", 1); return err }},
		{"count", func() error { _, err := r.Count(ctx); return err }},
		{"update", func() error { _, err := r.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{}); return err }},
		{"transition", func() error { _, _, err := r.TransitionStatus(ctx, tenant.TransitionStatusInput{}); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestTenantRepositoryListBoundaryBranches(t *testing.T) {
	ctx := context.Background()
	if _, _, err := NewRepository(nil).List(ctx, []string{"tenant"}, "", "", "", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil-storage List error = %v", err)
	}
	value, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "primary", DisplayName: "Primary", Status: tenant.StatusActive,
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
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
			mock.ExpectQuery("SELECT tenant_id FROM tenant").WithArgs(value.TenantID).WillReturnError(errors.New("list query"))
		}, true, false},
		{"rows error", func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery("SELECT tenant_id FROM tenant").WithArgs(value.TenantID).WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow(value.TenantID).RowError(0, errors.New("rows")))
		}, true, false},
		{"filter no-match", func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery("SELECT tenant_id FROM tenant").WithArgs(value.TenantID).WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow(value.TenantID))
			mock.ExpectQuery(".*").WithArgs(value.TenantID).WillReturnRows(testTenantRows(value))
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
			items, _, callErr := NewRepository(db).List(ctx, []string{value.TenantID}, query, "", cursor, 1)
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
