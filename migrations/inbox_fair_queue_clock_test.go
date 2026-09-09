package migrations

import (
	"strings"
	"testing"
)

func TestInboxFairQueueClockMigrationIsSeededAndReversible(t *testing.T) {
	up, err := files.ReadFile("044_inbox_fair_queue_clock.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS inbox_fair_queue_clock",
		"singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton)",
		"virtual_time BIGINT NOT NULL DEFAULT 0 CHECK (virtual_time >= 0)",
		"COALESCE(MAX(virtual_runtime), 0)",
		"ON CONFLICT (singleton) DO NOTHING",
	} {
		if !strings.Contains(string(up), required) {
			t.Errorf("fair clock migration missing %q", required)
		}
	}
	down, err := files.ReadFile("044_inbox_fair_queue_clock.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(down), "DROP TABLE IF EXISTS inbox_fair_queue_clock") {
		t.Fatal("fair clock rollback does not remove its own table")
	}
	if strings.Contains(string(down), "DROP TABLE IF EXISTS tenant_queue_schedule") {
		t.Fatal("fair clock rollback removes tenant service debt")
	}
}
