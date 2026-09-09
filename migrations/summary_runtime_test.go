package migrations

import (
	"strings"
	"testing"
)

func TestSummaryRuntimeMigrationPinsVersionAndSupportsDeferredTarget(t *testing.T) {
	up, err := files.ReadFile("041_summary_runtime.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	text := string(up)
	for _, required := range []string{
		"DELETE FROM summary_jobs",
		"agent_version_id VARCHAR(64) NOT NULL",
		"FOREIGN KEY (agent_version_id, agent_app_id)",
		"REFERENCES agent_versions(id, agent_app_id)",
		"target_event_sequence >= 0",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("041 migration missing %q", required)
		}
	}
	down, err := files.ReadFile("041_summary_runtime.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(down), "DROP COLUMN agent_version_id") ||
		!strings.Contains(string(down), "target_event_sequence > 0") {
		t.Fatal("041 rollback does not restore the legacy summary contract")
	}
}
