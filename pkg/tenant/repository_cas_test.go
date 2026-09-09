package tenant

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSQLRepositoryUpdateRejectsSnapshotAfterDelete(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := &SQLRepository{db: db}
	tenant := &Tenant{
		ID: "tenant-a", Name: "Acme", Status: TenantStatusActive, ConfigVersion: 1,
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT config FROM tenants")).
		WithArgs("tenant-a", int64(1), TenantStatusDeleted).
		WillReturnRows(sqlmock.NewRows([]string{"config"}))
	mock.ExpectRollback()

	err = repo.Update(ContextWithAuditActor(context.Background(), "operator"), tenant)
	if !errors.Is(err, ErrTenantConflict) {
		t.Fatalf("Update error = %v, want ErrTenantConflict", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLRepositoryUpdatePublishesVersionOnlyAfterCommit(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := &SQLRepository{db: db}
	value := &Tenant{
		ID: "tenant-a", Name: "Acme", Status: TenantStatusActive, ConfigVersion: 7,
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT config FROM tenants")).
		WithArgs("tenant-a", int64(7), TenantStatusDeleted).
		WillReturnRows(sqlmock.NewRows([]string{"config"}).AddRow(`{"storage":{}}`))
	mock.ExpectQuery(regexp.QuoteMeta("UPDATE tenants")).
		WithArgs("Acme", TenantStatusActive, sqlmock.AnyArg(), sqlmock.AnyArg(), "tenant-a", int64(7), TenantStatusDeleted).
		WillReturnRows(sqlmock.NewRows([]string{"config_version"}).AddRow(int64(8)))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM tenant_channels WHERE tenant_id=$1")).
		WithArgs("tenant-a").
		WillReturnError(errors.New("injected channel replacement failure"))
	mock.ExpectRollback()

	err = repo.Update(ContextWithAuditActor(context.Background(), "operator"), value)
	if err == nil {
		t.Fatal("Update unexpectedly succeeded")
	}
	if value.ConfigVersion != 7 {
		t.Fatalf("rolled-back update published config version %d, want 7", value.ConfigVersion)
	}
	if !value.UpdatedAt.IsZero() {
		t.Fatalf("rolled-back update published timestamp %s", value.UpdatedAt)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLRepositoryCreatePublishesMetadataOnlyAfterCommit(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := &SQLRepository{db: db}
	value := &Tenant{ID: "tenant-a", Name: "Acme", Status: TenantStatusActive}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO tenants")).
		WillReturnError(errors.New("injected create failure"))
	mock.ExpectRollback()

	err = repo.Create(ContextWithAuditActor(context.Background(), "operator"), value)
	if err == nil {
		t.Fatal("Create unexpectedly succeeded")
	}
	if value.ConfigVersion != 0 || !value.CreatedAt.Equal(time.Time{}) || !value.UpdatedAt.Equal(time.Time{}) {
		t.Fatalf("rolled-back create published metadata: version=%d created=%s updated=%s",
			value.ConfigVersion, value.CreatedAt, value.UpdatedAt)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLRepositoryDeleteBumpsVersionAndDoesNotRewriteDeletedTenant(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := &SQLRepository{db: db}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE tenants\n\t\tSET status = $1, updated_at = $2,\n\t\t    config_version = config_version + 1\n\t\tWHERE id = $3 AND status <> $4")).
		WithArgs(TenantStatusDeleted, sqlmock.AnyArg(), "tenant-a", TenantStatusDeleted).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO control_plane_audit")).
		WithArgs("tenant-a", "operator", "tenant.delete", "tenant-a", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := repo.Delete(ContextWithAuditActor(context.Background(), "operator"), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
