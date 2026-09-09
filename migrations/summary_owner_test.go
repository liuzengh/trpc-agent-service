package migrations

import (
	"strings"
	"testing"
)

func TestSummaryOwnerMigrationRekeysDerivedState(t *testing.T) {
	up, err := files.ReadFile("038_summary_session_owner.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	text := string(up)
	for _, required := range []string{
		"DELETE FROM summary_checkpoints",
		"DELETE FROM summary_jobs",
		"session_owner_id VARCHAR(255) NOT NULL",
		"UNIQUE (tenant_id, agent_app_id, session_owner_id, session_id, filter_key)",
		"PRIMARY KEY (tenant_id, agent_app_id, session_owner_id, session_id, filter_key)",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("038 migration missing %q", required)
		}
	}
	if strings.Index(text, "DELETE FROM summary_checkpoints") > strings.Index(text, "DELETE FROM summary_jobs") {
		t.Fatal("checkpoint rows must be removed before their coordinating jobs")
	}
	down, err := files.ReadFile("038_summary_session_owner.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(down), "DROP COLUMN session_owner_id") {
		t.Fatal("038 down migration does not remove session_owner_id")
	}
}
