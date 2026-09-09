package storage

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestEmbeddedInitialMigration(t *testing.T) {
	files, err := upMigrationFiles()
	if err != nil {
		t.Fatalf("upMigrationFiles() error = %v", err)
	}
	if len(files) != 1 || files[0] != "migrations/0001_init.up.sql" {
		t.Fatalf("upMigrationFiles() = %v, want initial migration", files)
	}
	data, err := migrationFS.ReadFile(files[0])
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if statements := splitSQLStatements(string(data)); len(statements) != 9 {
		t.Fatalf("initial migration statement count = %d, want 9", len(statements))
	}
}

func TestApplyMigrations(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT GET_LOCK").
		WithArgs(migrationLockName).
		WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(1))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS schema_migration").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM schema_migration").
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	for range 9 {
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS").
			WillReturnResult(sqlmock.NewResult(0, 0))
	}
	mock.ExpectExec("INSERT INTO schema_migration").
		WithArgs(int64(1)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("SELECT RELEASE_LOCK").
		WithArgs(migrationLockName).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := ApplyMigrations(context.Background(), db); err != nil {
		t.Fatalf("ApplyMigrations() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}
