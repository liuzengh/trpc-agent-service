package postgres

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
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"next_version"}).AddRow(updated.Version))
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
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(7)))
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

func TestTenantRepositoryListsOnlyRequestedTenantScopes(t *testing.T) {
	visible, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "visible-list", DisplayName: "Visible List", Status: tenant.StatusActive,
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
	mock.ExpectQuery(`tenant_id IN \(\$1\).*ORDER BY tenant_id LIMIT \$2 OFFSET \$3`).WithArgs(visible.TenantID, 51, 0).WillReturnRows(testTenantRows(visible))

	items, next, err := NewRepository(db).List(context.Background(), []string{visible.TenantID}, "", "", "", 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].TenantID != visible.TenantID || next != "" {
		t.Fatalf("scoped tenant list = items=%+v next=%q", items, next)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	empty, next, err := NewRepository(nil).List(context.Background(), nil, "", "", "", 50)
	if err != nil || len(empty) != 0 || next != "" {
		t.Fatalf("empty scope list = items=%+v next=%q err=%v", empty, next, err)
	}
}

func TestTenantScopeClauseAndListInputBoundaries(t *testing.T) {
	clause, arguments, allowed := tenantScopeClause([]string{"", "tenant-b", "tenant-a", "tenant-a"})
	if !allowed || clause != " WHERE tenant_id IN ($1, $2) " || len(arguments) != 2 || arguments[0] != "tenant-a" || arguments[1] != "tenant-b" {
		t.Fatalf("tenant scope clause = %q %#v", clause, arguments)
	}
	if clause, arguments, allowed := tenantScopeClause(nil); clause != "" || arguments != nil || allowed {
		t.Fatalf("empty tenant scope clause = %q %#v", clause, arguments)
	}
	if clause, arguments, allowed := tenantScopeClause([]string{"tenant-a", "*"}); clause != "" || arguments != nil || !allowed {
		t.Fatalf("wildcard tenant scope clause = %q %#v allowed=%v", clause, arguments, allowed)
	}
	visible, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "filter-list", DisplayName: "Filter List", Status: tenant.StatusActive,
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
	mock.ExpectQuery(`tenant_id IN \(\$1\).*STRPOS\(LOWER\(.*\), \$2\) > 0 ORDER BY tenant_id LIMIT \$3 OFFSET \$4`).WithArgs(visible.TenantID, "absent", 51, 0).WillReturnRows(testTenantRows())
	items, next, err := NewRepository(db).List(context.Background(), []string{visible.TenantID}, "absent", "", "", 0)
	if err != nil || len(items) != 0 || next != "" {
		t.Fatalf("filtered tenant list = items=%+v next=%q err=%v", items, next, err)
	}
	if _, _, err := NewRepository(db).List(context.Background(), []string{visible.TenantID}, "", "", "bad", 50); err == nil {
		t.Fatal("invalid tenant cursor was accepted")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
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
	mock.ExpectQuery(`.*`).WillReturnRows(testTenantRows(value))
	items, next, err := NewRepository(db).List(context.Background(), []string{"*"}, "", "", "", 50)
	if err != nil || len(items) != 1 || items[0].TenantID != value.TenantID || next != "" {
		t.Fatalf("wildcard tenant list = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
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
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM public\.tenant`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
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
		mock.ExpectBegin()
		mock.ExpectExec("SELECT pg_advisory_xact_lock").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM public\\.tenant").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		mock.ExpectExec("SELECT public\\.control_plane_create_tenant").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery("SELECT .*tenant_id =").WillReturnRows(testTenantRows(stored))
		mock.ExpectCommit()

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
		mock.ExpectBegin()
		mock.ExpectExec("SELECT pg_advisory_xact_lock").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM public\\.tenant").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
		mock.ExpectRollback()

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
	mock.ExpectBegin()
	mock.ExpectExec("SELECT pg_advisory_xact_lock").WillReturnError(errors.New("lock unavailable"))
	mock.ExpectRollback()
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

func testTenantRows(values ...*tenant.Tenant) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{
		"tenant_id", "tenant_key", "display_name", "status", "rate_limit_rpm", "max_concurrent_executions", "monthly_token_budget",
		"monthly_spend_limit_minor", "billing_currency", "audit_retention_days", "log_masking_level", "trace_sampling_rate", "default_agent_app_id",
		"default_backend_profile_id", "version", "created_at", "updated_at",
	})
	for _, value := range values {
		rows.AddRow(
			value.TenantID, value.TenantKey, value.DisplayName, string(value.Status), nil, nil, nil, nil, nil,
			value.AuditRetentionDays, string(value.LogMaskingLevel), value.TraceSamplingRate, nil, nil, value.Version, value.CreatedAt, value.UpdatedAt,
		)
	}
	return rows
}
