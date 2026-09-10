package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

func TestChannelRepositoryRejectsCancelledContextsBeforeStorage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := NewRepository(nil)
	cases := []struct {
		name string
		call func() error
	}{
		{"create", func() error { _, _, err := r.Create(ctx, channels.CreateInput{}); return err }},
		{"get", func() error { _, err := r.Get(ctx, "tenant", "binding"); return err }},
		{"list", func() error { _, _, err := r.List(ctx, "tenant", "", "", "", 1); return err }},
		{"update", func() error { _, _, err := r.UpdateConfiguration(ctx, channels.UpdateConfigurationInput{}); return err }},
		{"transition", func() error { _, _, err := r.TransitionStatus(ctx, channels.TransitionStatusInput{}); return err }},
		{"lookup candidates", func() error { _, err := r.LookupCandidates(ctx, "channel", "digest"); return err }},
		{"consume candidate", func() error { _, err := r.ConsumeCandidate(ctx, channels.CandidateBindingContext{}); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestChannelRepositoryListBoundaryBranches(t *testing.T) {
	ctx := context.Background()
	if _, _, err := NewRepository(nil).List(ctx, "tenant", "", "", "", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil-storage List error = %v", err)
	}
	binding := newStoredChannelBinding(t)
	for _, tc := range []struct {
		name        string
		setup       func(sqlmock.Sqlmock)
		wantStorage bool
		wantNoMatch bool
	}{
		{"invalid cursor", func(sqlmock.Sqlmock) {}, false, false},
		{"query error", func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery("SELECT binding_id FROM channel_binding").WithArgs(binding.TenantID).WillReturnError(errors.New("list query"))
		}, true, false},
		{"rows error", func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery("SELECT binding_id FROM channel_binding").WithArgs(binding.TenantID).WillReturnRows(sqlmock.NewRows([]string{"binding_id"}).AddRow(binding.BindingID).RowError(0, errors.New("rows")))
		}, true, false},
		{"filter no-match", func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery("SELECT binding_id FROM channel_binding").WithArgs(binding.TenantID).WillReturnRows(sqlmock.NewRows([]string{"binding_id"}).AddRow(binding.BindingID))
			mock.ExpectQuery(".*").WithArgs(binding.TenantID, binding.BindingID).WillReturnRows(testChannelBindingRows(t, binding))
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
			items, _, callErr := NewRepository(db).List(ctx, binding.TenantID, query, "", cursor, 1)
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

func TestChannelRepositoryBoundaryWriteFailures(t *testing.T) {
	binding := newStoredChannelBinding(t)
	create := channels.CreateInput{TenantID: binding.TenantID, BindingKey: "boundary", Channel: binding.Channel, ProviderAccountID: binding.ProviderAccountID, PublicRouteKeyDigest: binding.PublicRouteKeyDigest, AppID: binding.AppID, SecretRef: binding.SecretRef, Protocol: binding.Protocol, Status: channels.StatusActive, Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "boundary", CorrelationID: "channel-boundary"}}
	update := channels.UpdateConfigurationInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, ProviderAccountID: binding.ProviderAccountID, PublicRouteKeyDigest: binding.PublicRouteKeyDigest, AppID: binding.AppID, SecretRef: binding.SecretRef, Protocol: binding.Protocol, Metadata: create.Metadata}
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
		if _, _, err := NewRepository(db).Create(context.Background(), create); !errors.Is(err, ErrStorage) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("update rows affected", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(testChannelBindingRows(t, binding))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected")))
		mock.ExpectRollback()
		if _, _, err := NewRepository(db).UpdateConfiguration(context.Background(), update); !errors.Is(err, channels.ErrConflict) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("update readback", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(testChannelBindingRows(t, binding))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(12, 1))
		mock.ExpectQuery(".*").WillReturnError(errors.New("readback"))
		mock.ExpectRollback()
		if _, _, err := NewRepository(db).UpdateConfiguration(context.Background(), update); !errors.Is(err, ErrStorage) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("update commit", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(testChannelBindingRows(t, binding))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(12, 1))
		mock.ExpectQuery(".*").WillReturnRows(testChannelBindingRows(t, binding))
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_type", "tenant_id", "binding_id", "previous_status", "current_status", "previous_digest", "current_digest", "actor_type", "actor_id", "reason", "correlation_id", "previous_version", "next_version", "occurred_at"}).AddRow("configuration_updated", binding.TenantID, binding.BindingID, string(binding.Status), string(binding.Status), binding.ConfigDigest, binding.ConfigDigest, "test", "user", "boundary", "channel-boundary", binding.Version, binding.Version+1, binding.UpdatedAt))
		mock.ExpectCommit().WillReturnError(errors.New("commit"))
		if _, _, err := NewRepository(db).UpdateConfiguration(context.Background(), update); !errors.Is(err, ErrStorage) {
			t.Fatalf("error = %v", err)
		}
	})
}
