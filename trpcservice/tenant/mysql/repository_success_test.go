package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestTenantRepositoryWritesCompleteReadback(t *testing.T) {
	ctx := context.Background()
	created, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "repository-success", DisplayName: "Repository Success", Status: tenant.StatusActive,
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("update configuration", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		updated := created.Clone()
		updated.DisplayName = "Updated Repository"
		updated.Version++
		updated.UpdatedAt = updated.UpdatedAt.Add(time.Second)

		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(testTenantRows(created))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery(".*").WillReturnRows(testTenantRows(&updated))
		mock.ExpectCommit()

		result, err := NewRepository(db).UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{
			TenantID: created.TenantID, ExpectedVersion: created.Version, DisplayName: updated.DisplayName,
			AuditRetentionDays: updated.AuditRetentionDays, LogMaskingLevel: updated.LogMaskingLevel, TraceSamplingRate: updated.TraceSamplingRate,
		})
		if err != nil {
			t.Fatal(err)
		}
		if result.Version != updated.Version || result.DisplayName != updated.DisplayName {
			t.Fatalf("updated tenant = %+v", result)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("transition status", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		updated := created.Clone()
		updated.Status = tenant.StatusSuspended
		updated.Version++
		updated.UpdatedAt = updated.UpdatedAt.Add(time.Second)

		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(testTenantRows(created))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(7, 1))
		mock.ExpectQuery(".*").WillReturnRows(testTenantRows(&updated))
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "previous_status", "next_status", "actor_type", "actor_id", "reason", "previous_version", "next_version", "occurred_at",
		}).AddRow(created.TenantID, string(created.Status), string(updated.Status), "test", "tenant", "coverage", created.Version, updated.Version, updated.UpdatedAt))
		mock.ExpectCommit()

		result, event, err := NewRepository(db).TransitionStatus(ctx, tenant.TransitionStatusInput{
			TenantID: created.TenantID, ExpectedVersion: created.Version, NextStatus: updated.Status,
			Metadata: tenant.TransitionMetadata{ActorType: "test", ActorID: "tenant", Reason: "coverage", CorrelationID: "tenant-success"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if result.Status != updated.Status || event.NextStatus != updated.Status || event.NextVersion != updated.Version {
			t.Fatalf("transition result = tenant=%+v event=%+v", result, event)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestTenantRepositoryCreatesAndGetsTenant(t *testing.T) {
	input := tenant.CreateInput{
		TenantKey: "create-and-get", DisplayName: "Create and Get", Status: tenant.StatusActive,
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	}
	stored, err := tenant.NewTenant(input)
	if err != nil {
		t.Fatal(err)
	}

	createDB, createMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = createDB.Close() })
	createMock.ExpectBegin()
	createMock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	createMock.ExpectQuery(".*").WithArgs(sqlmock.AnyArg()).WillReturnRows(testTenantRows(stored))
	createMock.ExpectCommit()

	created, err := NewRepository(createDB).Create(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if created.TenantKey != input.TenantKey || created.DisplayName != input.DisplayName || created.Status != tenant.StatusActive {
		t.Fatalf("created tenant = %+v", created)
	}
	if err := createMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	getDB, getMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = getDB.Close() })
	getMock.ExpectQuery(".*").WithArgs(stored.TenantID).WillReturnRows(testTenantRows(stored))

	loaded, err := NewRepository(getDB).Get(context.Background(), stored.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.TenantID != stored.TenantID || loaded.Version != stored.Version {
		t.Fatalf("loaded tenant = %+v", loaded)
	}
	if err := getMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTenantRepositoryListsScopedTenants(t *testing.T) {
	value, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "primary", DisplayName: "Primary", Status: tenant.StatusActive,
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(`SELECT tenant_id FROM tenant WHERE tenant_id IN \(\?\) ORDER BY tenant_id`).WithArgs(value.TenantID).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow(value.TenantID))
	mock.ExpectQuery(".*").WithArgs(value.TenantID).WillReturnRows(testTenantRows(value))

	items, next, err := NewRepository(db).List(context.Background(), []string{"", value.TenantID, value.TenantID}, "primary", string(value.Status), "", 50)
	if err != nil || len(items) != 1 || items[0].TenantID != value.TenantID || next != "" {
		t.Fatalf("listed tenants = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTenantRepositoryListBoundaries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := NewRepository(nil).List(ctx, []string{"tenant"}, "", "", "", 50); err == nil {
		t.Fatal("canceled tenant list was accepted")
	}
	if _, _, err := NewRepository(nil).List(context.Background(), []string{"tenant"}, "", "", "", 50); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil tenant list error = %v", err)
	}
	emptyScopeDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = emptyScopeDB.Close() })
	if items, next, err := NewRepository(emptyScopeDB).List(context.Background(), nil, "", "", "", 50); err != nil || len(items) != 0 || next != "" {
		t.Fatalf("empty-scope tenant list = items=%+v next=%q err=%v", items, next, err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	repository := NewRepository(db)
	if _, _, err := repository.List(context.Background(), []string{"tenant"}, "", "", "bad", 50); err == nil {
		t.Fatal("invalid tenant cursor was accepted")
	}
	mock.ExpectQuery(`SELECT tenant_id FROM tenant WHERE tenant_id IN \(\?\) ORDER BY tenant_id`).WithArgs("tenant").WillReturnError(errors.New("query down"))
	if _, _, err := repository.List(context.Background(), []string{"tenant"}, "", "", "", 50); !errors.Is(err, ErrStorage) {
		t.Fatalf("tenant query error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	scanDB, scanMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = scanDB.Close() })
	scanMock.ExpectQuery(`SELECT tenant_id FROM tenant WHERE tenant_id IN \(\?\) ORDER BY tenant_id`).WithArgs("tenant").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "extra"}).AddRow("tenant", "bad"))
	if _, _, err := NewRepository(scanDB).List(context.Background(), []string{"tenant"}, "", "", "", 50); !errors.Is(err, ErrStorage) {
		t.Fatalf("tenant scan error = %v", err)
	}
}

func TestTenantRepositoryListsAllTenantsForWildcardScope(t *testing.T) {
	value, err := tenant.NewTenant(tenant.CreateInput{TenantKey: "wildcard", DisplayName: "Wildcard", Status: tenant.StatusActive, AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1})
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(`SELECT tenant_id FROM tenant ORDER BY tenant_id`).WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow(value.TenantID))
	mock.ExpectQuery(".*").WithArgs(value.TenantID).WillReturnRows(testTenantRows(value))
	items, next, err := NewRepository(db).List(context.Background(), []string{" * "}, "", "", "", 50)
	if err != nil || len(items) != 1 || items[0].TenantID != value.TenantID || next != "" {
		t.Fatalf("wildcard tenant list = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTenantRepositoryValidatesDefaultsAndErrors(t *testing.T) {
	input := tenant.CreateInput{
		TenantKey: "validation-paths", DisplayName: "Validation Paths", Status: tenant.StatusActive,
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	}
	stored, err := tenant.NewTenant(input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRepository(nil).UpdateConfiguration(context.Background(), tenant.UpdateConfigurationInput{
		TenantID: stored.TenantID, ExpectedVersion: stored.Version, DisplayName: stored.DisplayName, TraceSamplingRate: stored.TraceSamplingRate,
	}); !errors.Is(err, ErrStorage) {
		t.Fatalf("defaulted configuration storage error = %v", err)
	}
	if _, _, err := NewRepository(nil).TransitionStatus(context.Background(), tenant.TransitionStatusInput{}); !errors.Is(err, tenant.ErrInvalid) {
		t.Fatalf("invalid transition metadata error = %v", err)
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(".*").WithArgs(stored.TenantID).WillReturnError(sql.ErrNoRows)
	if _, err := NewRepository(db).Get(context.Background(), stored.TenantID); !errors.Is(err, tenant.ErrNotFound) {
		t.Fatalf("missing tenant error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTenantRepositoryRequiresStorage(t *testing.T) {
	repository := NewRepository(nil)
	ctx := context.Background()
	input := tenant.CreateInput{
		TenantKey: "storage-guard", DisplayName: "Storage Guard", Status: tenant.StatusActive,
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	}
	created, err := tenant.NewTenant(input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Create(ctx, input); !errors.Is(err, ErrStorage) {
		t.Fatalf("Create nil-storage error = %v", err)
	}
	if _, err := repository.Get(ctx, "tenant"); !errors.Is(err, ErrStorage) {
		t.Fatalf("Get nil-storage error = %v", err)
	}
	if _, err := repository.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{
		TenantID: created.TenantID, ExpectedVersion: created.Version, DisplayName: created.DisplayName,
		AuditRetentionDays: created.AuditRetentionDays, LogMaskingLevel: created.LogMaskingLevel, TraceSamplingRate: created.TraceSamplingRate,
	}); !errors.Is(err, ErrStorage) {
		t.Fatalf("UpdateConfiguration nil-storage error = %v", err)
	}
	if _, _, err := repository.TransitionStatus(ctx, tenant.TransitionStatusInput{
		TenantID: created.TenantID, ExpectedVersion: created.Version, NextStatus: tenant.StatusSuspended,
		Metadata: tenant.TransitionMetadata{ActorType: "test", ActorID: "user", Reason: "storage", CorrelationID: "tenant-storage"},
	}); !errors.Is(err, ErrStorage) {
		t.Fatalf("TransitionStatus nil-storage error = %v", err)
	}
}

func TestTenantRepositoryCount(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM tenant`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	count, err := NewRepository(db).Count(context.Background())
	if err != nil || count != 2 {
		t.Fatalf("Count = %d, %v", count, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTenantRepositoryCreateFirstUsesAtomicAdvisoryGate(t *testing.T) {
	input := tenant.CreateInput{
		TenantKey: "create-first", DisplayName: "Create First", Status: tenant.StatusActive,
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	}
	stored, err := tenant.NewTenant(input)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("creates when empty", func(t *testing.T) {
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
		mock.ExpectCommit()
		mock.ExpectQuery("SELECT RELEASE_LOCK").WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))

		created, allowed, err := NewRepository(db).CreateFirst(context.Background(), input)
		if err != nil || !allowed || created == nil || created.TenantID != stored.TenantID {
			t.Fatalf("CreateFirst = tenant=%+v allowed=%v err=%v", created, allowed, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("rejects when already populated", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		mock.ExpectQuery("SELECT GET_LOCK").WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(1))
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM tenant").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
		mock.ExpectRollback()
		mock.ExpectQuery("SELECT RELEASE_LOCK").WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))

		created, allowed, err := NewRepository(db).CreateFirst(context.Background(), input)
		if err != nil || allowed || created != nil {
			t.Fatalf("CreateFirst populated = tenant=%+v allowed=%v err=%v", created, allowed, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestTenantRepositoryCreateFirstRejectsLockAndContextFailures(t *testing.T) {
	input := tenant.CreateInput{TenantKey: "create-first-errors", DisplayName: "Create First Errors", Status: tenant.StatusActive, AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery("SELECT GET_LOCK").WillReturnError(errors.New("lock unavailable"))
	if _, _, err := NewRepository(db).CreateFirst(context.Background(), input); !errors.Is(err, ErrStorage) {
		t.Fatalf("lock failure = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := NewRepository(db).CreateFirst(canceled, input); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled CreateFirst = %v", err)
	}
}

func TestTenantRepositoryDatabaseAndDefaultValidationPaths(t *testing.T) {
	input := tenant.CreateInput{TenantKey: "error-path", DisplayName: "Error Path", Status: tenant.StatusActive, AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1}
	created, err := tenant.NewTenant(input)
	if err != nil {
		t.Fatal(err)
	}
	newDB := func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db, mock
	}
	db, mock := newDB(t)
	mock.ExpectBegin().WillReturnError(errors.New("begin"))
	if _, err := NewRepository(db).Create(context.Background(), input); !errors.Is(err, ErrStorage) {
		t.Fatalf("Create begin error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectQuery(".*").WillReturnError(errors.New("read"))
	if _, err := NewRepository(db).Get(context.Background(), created.TenantID); !errors.Is(err, ErrStorage) {
		t.Fatalf("Get read error = %v", err)
	}
	if _, err := NewRepository(db).Count(nil); !errors.Is(err, ErrStorage) {
		t.Fatalf("Count nil context = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin().WillReturnError(errors.New("begin"))
	if _, err := NewRepository(db).UpdateConfiguration(context.Background(), tenant.UpdateConfigurationInput{TenantID: created.TenantID, ExpectedVersion: created.Version, DisplayName: created.DisplayName, AuditRetentionDays: created.AuditRetentionDays, LogMaskingLevel: created.LogMaskingLevel, TraceSamplingRate: created.TraceSamplingRate}); !errors.Is(err, ErrStorage) {
		t.Fatalf("Update begin error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin().WillReturnError(errors.New("begin"))
	if _, _, err := NewRepository(db).TransitionStatus(context.Background(), tenant.TransitionStatusInput{TenantID: created.TenantID, ExpectedVersion: created.Version, NextStatus: tenant.StatusSuspended, Metadata: tenant.TransitionMetadata{ActorType: "test", ActorID: "user", Reason: "begin", CorrelationID: "tenant-begin"}}); !errors.Is(err, ErrStorage) {
		t.Fatalf("Transition begin error = %v", err)
	}
	appID, backendID := "app_01ARZ3NDEKTSV4RRFFQ69G5FAW", "bp_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	db, mock = newDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testTenantRows(created))
	mock.ExpectQuery("SELECT status FROM agent_app").WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("active"))
	mock.ExpectQuery("SELECT status FROM backend_profile").WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("active"))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(".*").WillReturnRows(testTenantRows(created))
	mock.ExpectCommit()
	_, err = NewRepository(db).UpdateConfiguration(context.Background(), tenant.UpdateConfigurationInput{
		TenantID: created.TenantID, ExpectedVersion: created.Version, DisplayName: created.DisplayName,
		AuditRetentionDays: created.AuditRetentionDays, LogMaskingLevel: created.LogMaskingLevel, TraceSamplingRate: created.TraceSamplingRate,
		DefaultAgentAppID: &appID, DefaultBackendProfileID: &backendID,
	})
	if err != nil {
		t.Fatalf("default validation update = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTenantRepositoryWriteErrorBranches(t *testing.T) {
	input := tenant.CreateInput{TenantKey: "write-errors", DisplayName: "Write Errors", Status: tenant.StatusActive, AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1}
	created, err := tenant.NewTenant(input)
	if err != nil {
		t.Fatal(err)
	}
	update := tenant.UpdateConfigurationInput{TenantID: created.TenantID, ExpectedVersion: created.Version, DisplayName: created.DisplayName, AuditRetentionDays: created.AuditRetentionDays, LogMaskingLevel: created.LogMaskingLevel, TraceSamplingRate: created.TraceSamplingRate}
	transition := tenant.TransitionStatusInput{TenantID: created.TenantID, ExpectedVersion: created.Version, NextStatus: tenant.StatusSuspended, Metadata: tenant.TransitionMetadata{ActorType: "test", ActorID: "user", Reason: "transition", CorrelationID: "tenant-transition-errors"}}
	newDB := func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db, mock
	}
	db, mock := newDB(t)
	mock.ExpectBegin()
	mock.ExpectExec(".*").WillReturnError(errors.New("insert"))
	mock.ExpectRollback()
	if _, err := NewRepository(db).Create(context.Background(), input); !errors.Is(err, ErrStorage) {
		t.Fatalf("Create insert error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testTenantRows(created))
	mock.ExpectExec(".*").WillReturnError(errors.New("update"))
	mock.ExpectRollback()
	if _, err := NewRepository(db).UpdateConfiguration(context.Background(), update); !errors.Is(err, ErrStorage) {
		t.Fatalf("Update write error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testTenantRows(created))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnError(errors.New("outbox"))
	mock.ExpectRollback()
	if _, err := NewRepository(db).UpdateConfiguration(context.Background(), update); !errors.Is(err, ErrStorage) {
		t.Fatalf("Update outbox error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testTenantRows(created))
	mock.ExpectExec(".*").WillReturnError(errors.New("transition"))
	mock.ExpectRollback()
	if _, _, err := NewRepository(db).TransitionStatus(context.Background(), transition); !errors.Is(err, ErrStorage) {
		t.Fatalf("Transition write error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testTenantRows(created))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnError(errors.New("outbox"))
	mock.ExpectRollback()
	if _, _, err := NewRepository(db).TransitionStatus(context.Background(), transition); !errors.Is(err, ErrStorage) {
		t.Fatalf("Transition outbox error = %v", err)
	}
}

func TestTenantRepositoryCountAndTransitionGuardErrors(t *testing.T) {
	created, err := tenant.NewTenant(tenant.CreateInput{TenantKey: "guards", DisplayName: "Guards", Status: tenant.StatusActive, AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1})
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery("SELECT COUNT").WillReturnError(errors.New("count"))
	if _, err := NewRepository(db).Count(context.Background()); !errors.Is(err, ErrStorage) {
		t.Fatalf("Count query error = %v", err)
	}
	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery("SELECT GET_LOCK").WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(1))
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT COUNT").WillReturnError(errors.New("count"))
	mock.ExpectRollback()
	mock.ExpectQuery("SELECT RELEASE_LOCK").WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))
	if _, _, err := NewRepository(db).CreateFirst(context.Background(), tenant.CreateInput{TenantKey: "first-error", DisplayName: "First Error", Status: tenant.StatusActive, AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1}); !errors.Is(err, ErrStorage) {
		t.Fatalf("CreateFirst count error = %v", err)
	}
	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testTenantRows(created))
	mock.ExpectRollback()
	if _, _, err := NewRepository(db).TransitionStatus(context.Background(), tenant.TransitionStatusInput{TenantID: created.TenantID, ExpectedVersion: created.Version, NextStatus: tenant.StatusActive, Metadata: tenant.TransitionMetadata{ActorType: "test", ActorID: "user", Reason: "invalid", CorrelationID: "tenant-invalid"}}); !errors.Is(err, tenant.ErrInvalidTransition) {
		t.Fatalf("invalid transition = %v", err)
	}
	disabled := created.Clone()
	disabled.Status = tenant.StatusDisabled
	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testTenantRows(&disabled))
	mock.ExpectRollback()
	if _, _, err := NewRepository(db).TransitionStatus(context.Background(), tenant.TransitionStatusInput{TenantID: disabled.TenantID, ExpectedVersion: disabled.Version, NextStatus: tenant.StatusActive, Metadata: tenant.TransitionMetadata{ActorType: "test", ActorID: "user", Reason: "disabled", CorrelationID: "tenant-disabled"}}); !errors.Is(err, tenant.ErrDisabled) {
		t.Fatalf("disabled transition = %v", err)
	}
}

func TestTenantRepositoryScannersRejectShortRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("short"))
	if _, err := scanTenant(db.QueryRowContext(context.Background(), "SELECT 1")); err == nil {
		t.Fatal("short tenant row was accepted")
	}
	mock.ExpectQuery("SELECT 2").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("short"))
	if _, err := scanTenantStatusEvent(db.QueryRowContext(context.Background(), "SELECT 2")); err == nil {
		t.Fatal("short status event row was accepted")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func testTenantRows(value *tenant.Tenant) *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"tenant_id", "tenant_key", "display_name", "status", "rate_limit_rpm", "max_concurrent_executions", "monthly_token_budget",
		"monthly_spend_limit_minor", "billing_currency", "audit_retention_days", "log_masking_level", "trace_sampling_rate", "default_agent_app_id",
		"default_backend_profile_id", "version", "created_at", "updated_at",
	}).AddRow(
		value.TenantID, value.TenantKey, value.DisplayName, string(value.Status), nil, nil, nil, nil, nil,
		value.AuditRetentionDays, string(value.LogMaskingLevel), value.TraceSamplingRate, nil, nil, value.Version, value.CreatedAt, value.UpdatedAt,
	)
}
