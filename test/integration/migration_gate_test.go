//go:build integration

package integration

import (
	"database/sql"
	"os"
	"testing"
)

func openMigrationGateDatabase(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = db.Close() })
	return db
}
