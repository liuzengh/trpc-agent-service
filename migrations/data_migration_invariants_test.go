package migrations

import (
	"io/fs"
	"strings"
	"testing"
)

func TestDataMigrationErrorInvariantMigrationIsAdditiveAndPaired(t *testing.T) {
	up, err := fs.ReadFile(files, "025_data_migration_error_invariants.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	down, err := fs.ReadFile(files, "025_data_migration_error_invariants.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"data_migrations", "last_error", "octet_length(last_error) <= 4096"} {
		if !strings.Contains(string(up), required) {
			t.Errorf("025 up migration missing %q", required)
		}
	}
	if !strings.Contains(string(down), "data_migrations_last_error_size") {
		t.Error("025 down migration does not remove the named constraint")
	}
}
