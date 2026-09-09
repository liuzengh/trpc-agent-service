package migrations

import (
	"os"
	"strings"
	"testing"
)

func TestDataMigrationRecordsDownRefusesDataLoss(t *testing.T) {
	data, err := os.ReadFile("037_data_migration_records.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, marker := range []string{"to_regclass('data_migration_records')", "EXISTS (SELECT 1 FROM data_migration_records", "RAISE EXCEPTION"} {
		if !strings.Contains(text, marker) {
			t.Fatalf("down migration missing non-empty data-loss guard %q", marker)
		}
	}
}
