package migrations

import (
	"strings"
	"testing"
)

func TestSummaryBoundaryMigrationPurgesUnsafeLegacyCheckpoints(t *testing.T) {
	data, err := files.ReadFile("042_summary_boundary.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	up := string(data)
	for _, required := range []string{
		"LOCK TABLE summary_checkpoints IN ACCESS EXCLUSIVE MODE",
		"DELETE FROM summary_checkpoints",
		"cutoff_at TIMESTAMPTZ NOT NULL",
		"last_event_id VARCHAR(512) NOT NULL",
	} {
		if !strings.Contains(up, required) {
			t.Fatalf("summary boundary migration missing %q", required)
		}
	}
}
