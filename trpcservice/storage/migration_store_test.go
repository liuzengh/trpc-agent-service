package storage

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestMySQLMigrationStoreRoundTrip(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()
	store, err := NewMySQLMigrationStore(db)
	if err != nil {
		t.Fatalf("NewMySQLMigrationStore() error = %v", err)
	}
	now := time.Unix(1_000, 0)
	status := MigrationStatus{
		ID: "migration-1", AppID: "app-a", FromBackend: "redis",
		ToBackend: "mysql", Phase: PhasePrepared,
		Detail: map[string]any{}, UpdatedAt: now,
	}
	mock.ExpectExec("INSERT INTO migration").
		WithArgs(
			status.ID, status.AppID, status.FromBackend, status.ToBackend,
			status.Phase, sqlmock.AnyArg(), now,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := store.Create(context.Background(), status); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	rows := sqlmock.NewRows([]string{
		"migration_id", "app_id", "from_backend", "to_backend",
		"phase", "detail_json", "updated_at",
	}).AddRow(
		status.ID, status.AppID, status.FromBackend, status.ToBackend,
		string(PhasePrepared), `{}`, now,
	)
	mock.ExpectQuery("SELECT migration_id, app_id, from_backend").
		WithArgs(status.ID).
		WillReturnRows(rows)
	got, err := store.Get(context.Background(), status.ID)
	if err != nil || got.Phase != PhasePrepared {
		t.Fatalf("Get() = %#v, %v", got, err)
	}
	status.Phase = PhaseDualWrite
	mock.ExpectExec("UPDATE migration SET phase").
		WithArgs(status.Phase, sqlmock.AnyArg(), now, status.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := store.Update(context.Background(), status); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	activeRows := sqlmock.NewRows([]string{
		"migration_id", "app_id", "from_backend", "to_backend",
		"phase", "detail_json", "updated_at",
	}).AddRow(
		status.ID, status.AppID, status.FromBackend, status.ToBackend,
		string(PhaseDualWrite), `{}`, now,
	)
	mock.ExpectQuery("SELECT migration_id, app_id, from_backend").
		WithArgs(PhaseRolledBack).
		WillReturnRows(activeRows)
	active, err := store.ListActive(context.Background())
	if err != nil || len(active) != 1 || active[0].Phase != PhaseDualWrite {
		t.Fatalf("ListActive() = %#v, %v", active, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestMySQLSessionCatalog(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()
	catalog, err := NewMySQLSessionCatalog(db)
	if err != nil {
		t.Fatalf("NewMySQLSessionCatalog() error = %v", err)
	}
	rows := sqlmock.NewRows([]string{"app_name", "user_ref", "session_id"}).
		AddRow("tenant-a-support", "group-1", "tenant-a:feishu:group:group-1").
		AddRow("tenant-a-support", "user-1", "tenant-a:webui:user-1")
	mock.ExpectQuery("SELECT a.app_name, s.user_ref, s.session_id").
		WithArgs("app-a").
		WillReturnRows(rows)
	keys, err := catalog.ListSessionKeys(context.Background(), "app-a")
	if err != nil {
		t.Fatalf("ListSessionKeys() error = %v", err)
	}
	if len(keys) != 2 || keys[0].UserID != "group:group-1" || keys[1].UserID != "user-1" {
		t.Fatalf("ListSessionKeys() = %#v", keys)
	}
}
