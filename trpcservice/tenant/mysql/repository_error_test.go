package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestTenantRepositoryCreateFirstSQLMockErrors(t *testing.T) {
	input := tenant.CreateInput{
		TenantKey: "create-first-errors", DisplayName: "Create First Errors", Status: tenant.StatusActive,
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	}
	stored, err := tenant.NewTenant(input)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("connection failure", func(t *testing.T) {
		db, _, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		_ = db.Close()
		if _, _, err := NewRepository(db).CreateFirst(context.Background(), input); !errors.Is(err, ErrStorage) {
			t.Fatalf("CreateFirst connection error = %v", err)
		}
	})

	t.Run("begin connection failure", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		mock.ExpectQuery("SELECT GET_LOCK").WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(1))
		mock.ExpectBegin().WillReturnError(errors.New("begin connection"))
		mock.ExpectQuery("SELECT RELEASE_LOCK").WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))

		if _, _, err := NewRepository(db).CreateFirst(context.Background(), input); !errors.Is(err, ErrStorage) {
			t.Fatalf("CreateFirst begin connection error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("insert failure", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		mock.ExpectQuery("SELECT GET_LOCK").WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(1))
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM tenant").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		mock.ExpectExec("INSERT INTO tenant").WillReturnError(errors.New("insert"))
		mock.ExpectRollback()
		mock.ExpectQuery("SELECT RELEASE_LOCK").WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))

		if _, _, err := NewRepository(db).CreateFirst(context.Background(), input); !errors.Is(err, ErrStorage) {
			t.Fatalf("CreateFirst insert error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("readback failure", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		mock.ExpectQuery("SELECT GET_LOCK").WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(1))
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM tenant").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		mock.ExpectExec("INSERT INTO tenant").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery("SELECT .*tenant_id =").WillReturnError(errors.New("readback"))
		mock.ExpectRollback()
		mock.ExpectQuery("SELECT RELEASE_LOCK").WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))

		if _, _, err := NewRepository(db).CreateFirst(context.Background(), input); !errors.Is(err, ErrStorage) {
			t.Fatalf("CreateFirst readback error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("commit failure", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		mock.ExpectQuery("SELECT GET_LOCK").WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(1))
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM tenant").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		mock.ExpectExec("INSERT INTO tenant").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery("SELECT .*tenant_id =").WillReturnRows(testTenantRows(stored))
		mock.ExpectCommit().WillReturnError(errors.New("commit"))
		mock.ExpectQuery("SELECT RELEASE_LOCK").WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))

		if _, _, err := NewRepository(db).CreateFirst(context.Background(), input); !errors.Is(err, ErrStorage) {
			t.Fatalf("CreateFirst commit error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestTenantRepositoryUpdateConfigurationSQLMockErrors(t *testing.T) {
	input := tenant.CreateInput{
		TenantKey: "update-errors", DisplayName: "Update Errors", Status: tenant.StatusActive,
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	}
	created, err := tenant.NewTenant(input)
	if err != nil {
		t.Fatal(err)
	}
	update := tenant.UpdateConfigurationInput{
		TenantID: created.TenantID, ExpectedVersion: created.Version, DisplayName: created.DisplayName,
		AuditRetentionDays: created.AuditRetentionDays, LogMaskingLevel: created.LogMaskingLevel,
		TraceSamplingRate: created.TraceSamplingRate,
	}
	newDB := func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("sqlmock expectations: %v", err)
			}
			_ = db.Close()
		})
		return db, mock
	}

	t.Run("current tenant query failure", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery("FOR UPDATE").WillReturnError(errors.New("current tenant"))
		mock.ExpectRollback()
		if _, err := NewRepository(db).UpdateConfiguration(context.Background(), update); !errors.Is(err, ErrStorage) {
			t.Fatalf("UpdateConfiguration current query error = %v", err)
		}
	})

	t.Run("default validation query failure", func(t *testing.T) {
		db, mock := newDB(t)
		appID := "app_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		withDefault := update
		withDefault.DefaultAgentAppID = &appID
		mock.ExpectBegin()
		mock.ExpectQuery("FOR UPDATE").WillReturnRows(testTenantRows(created))
		mock.ExpectQuery("SELECT status FROM agent_app").WillReturnError(errors.New("default app"))
		mock.ExpectRollback()
		if _, err := NewRepository(db).UpdateConfiguration(context.Background(), withDefault); !errors.Is(err, tenant.ErrInvalid) {
			t.Fatalf("UpdateConfiguration default query error = %v", err)
		}
	})

	t.Run("rows affected error", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery("FOR UPDATE").WillReturnRows(testTenantRows(created))
		mock.ExpectExec("UPDATE tenant SET").WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected")))
		mock.ExpectRollback()
		if _, err := NewRepository(db).UpdateConfiguration(context.Background(), update); !errors.Is(err, tenant.ErrConflict) {
			t.Fatalf("UpdateConfiguration RowsAffected error = %v", err)
		}
	})

	t.Run("zero rows affected", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery("FOR UPDATE").WillReturnRows(testTenantRows(created))
		mock.ExpectExec("UPDATE tenant SET").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectRollback()
		if _, err := NewRepository(db).UpdateConfiguration(context.Background(), update); !errors.Is(err, tenant.ErrConflict) {
			t.Fatalf("UpdateConfiguration zero rows = %v", err)
		}
	})

	t.Run("readback failure", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery("FOR UPDATE").WillReturnRows(testTenantRows(created))
		mock.ExpectExec("UPDATE tenant SET").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("tenant_configuration_outbox").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery("SELECT .*tenant_id =").WillReturnError(errors.New("updated readback"))
		mock.ExpectRollback()
		if _, err := NewRepository(db).UpdateConfiguration(context.Background(), update); !errors.Is(err, ErrStorage) {
			t.Fatalf("UpdateConfiguration readback error = %v", err)
		}
	})

	t.Run("commit failure", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery("FOR UPDATE").WillReturnRows(testTenantRows(created))
		mock.ExpectExec("UPDATE tenant SET").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("tenant_configuration_outbox").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery("SELECT .*tenant_id =").WillReturnRows(testTenantRows(created))
		mock.ExpectCommit().WillReturnError(errors.New("commit"))
		if _, err := NewRepository(db).UpdateConfiguration(context.Background(), update); !errors.Is(err, ErrStorage) {
			t.Fatalf("UpdateConfiguration commit error = %v", err)
		}
	})
}

func TestTenantRepositoryTransitionStatusSQLMockErrors(t *testing.T) {
	input := tenant.CreateInput{
		TenantKey: "transition-errors", DisplayName: "Transition Errors", Status: tenant.StatusActive,
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	}
	created, err := tenant.NewTenant(input)
	if err != nil {
		t.Fatal(err)
	}
	transition := tenant.TransitionStatusInput{
		TenantID: created.TenantID, ExpectedVersion: created.Version, NextStatus: tenant.StatusSuspended,
		Metadata: tenant.TransitionMetadata{ActorType: "test", ActorID: "user", Reason: "error", CorrelationID: "transition-errors"},
	}
	eventRows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{
			"tenant_id", "previous_status", "next_status", "actor_type", "actor_id", "reason", "previous_version", "next_version", "occurred_at",
		}).AddRow(created.TenantID, string(created.Status), string(transition.NextStatus), "test", "user", "error", created.Version, created.Version+1, created.UpdatedAt)
	}
	newDB := func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("sqlmock expectations: %v", err)
			}
			_ = db.Close()
		})
		return db, mock
	}

	t.Run("current tenant query failure", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery("FOR UPDATE").WillReturnError(errors.New("current tenant"))
		mock.ExpectRollback()
		if _, _, err := NewRepository(db).TransitionStatus(context.Background(), transition); !errors.Is(err, ErrStorage) {
			t.Fatalf("TransitionStatus current query error = %v", err)
		}
	})

	t.Run("rows affected error", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery("FOR UPDATE").WillReturnRows(testTenantRows(created))
		mock.ExpectExec("UPDATE tenant SET status").WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected")))
		mock.ExpectRollback()
		if _, _, err := NewRepository(db).TransitionStatus(context.Background(), transition); !errors.Is(err, tenant.ErrConflict) {
			t.Fatalf("TransitionStatus RowsAffected error = %v", err)
		}
	})

	t.Run("zero rows affected", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery("FOR UPDATE").WillReturnRows(testTenantRows(created))
		mock.ExpectExec("UPDATE tenant SET status").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectRollback()
		if _, _, err := NewRepository(db).TransitionStatus(context.Background(), transition); !errors.Is(err, tenant.ErrConflict) {
			t.Fatalf("TransitionStatus zero rows = %v", err)
		}
	})

	t.Run("last insert id failure", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery("FOR UPDATE").WillReturnRows(testTenantRows(created))
		mock.ExpectExec("UPDATE tenant SET status").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("tenant_status_change_outbox").WillReturnResult(sqlmock.NewErrorResult(errors.New("last insert id")))
		mock.ExpectRollback()
		if _, _, err := NewRepository(db).TransitionStatus(context.Background(), transition); !errors.Is(err, ErrStorage) {
			t.Fatalf("TransitionStatus LastInsertId error = %v", err)
		}
	})

	t.Run("updated tenant readback failure", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery("FOR UPDATE").WillReturnRows(testTenantRows(created))
		mock.ExpectExec("UPDATE tenant SET status").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("tenant_status_change_outbox").WillReturnResult(sqlmock.NewResult(7, 1))
		mock.ExpectQuery("SELECT .*tenant_id =").WillReturnError(errors.New("updated tenant"))
		mock.ExpectRollback()
		if _, _, err := NewRepository(db).TransitionStatus(context.Background(), transition); !errors.Is(err, ErrStorage) {
			t.Fatalf("TransitionStatus updated readback error = %v", err)
		}
	})

	t.Run("event readback failure", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery("FOR UPDATE").WillReturnRows(testTenantRows(created))
		mock.ExpectExec("UPDATE tenant SET status").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("tenant_status_change_outbox").WillReturnResult(sqlmock.NewResult(7, 1))
		mock.ExpectQuery("SELECT .*tenant_id =").WillReturnRows(testTenantRows(created))
		mock.ExpectQuery("FROM tenant_status_change_outbox").WillReturnError(errors.New("event"))
		mock.ExpectRollback()
		if _, _, err := NewRepository(db).TransitionStatus(context.Background(), transition); !errors.Is(err, ErrStorage) {
			t.Fatalf("TransitionStatus event readback error = %v", err)
		}
	})

	t.Run("commit failure", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery("FOR UPDATE").WillReturnRows(testTenantRows(created))
		mock.ExpectExec("UPDATE tenant SET status").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("tenant_status_change_outbox").WillReturnResult(sqlmock.NewResult(7, 1))
		mock.ExpectQuery("SELECT .*tenant_id =").WillReturnRows(testTenantRows(created))
		mock.ExpectQuery("FROM tenant_status_change_outbox").WillReturnRows(eventRows())
		mock.ExpectCommit().WillReturnError(errors.New("commit"))
		if _, _, err := NewRepository(db).TransitionStatus(context.Background(), transition); !errors.Is(err, ErrStorage) {
			t.Fatalf("TransitionStatus commit error = %v", err)
		}
	})
}
