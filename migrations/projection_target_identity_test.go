package migrations

import (
	"os"
	"strings"
	"testing"
)

func TestProjectionTargetIdentityMigrationKeepsMarkersPerMigration(t *testing.T) {
	up, err := os.ReadFile("043_projection_target_identity.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	upText := string(up)
	for _, marker := range []string{
		"ADD COLUMN IF NOT EXISTS migration_id TEXT",
		"SET migration_id = 'legacy'",
		"ALTER COLUMN migration_id SET NOT NULL",
		"PRIMARY KEY (tenant_id, domain, record_key, migration_id)",
	} {
		if !strings.Contains(upText, marker) {
			t.Fatalf("up migration missing target identity invariant %q", marker)
		}
	}

	down, err := os.ReadFile("043_projection_target_identity.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	downText := string(down)
	for _, marker := range []string{
		"COUNT(DISTINCT migration_id) > 1",
		"RAISE EXCEPTION",
		"DROP COLUMN IF EXISTS migration_id",
	} {
		if !strings.Contains(downText, marker) {
			t.Fatalf("down migration missing rollback guard %q", marker)
		}
	}
}
