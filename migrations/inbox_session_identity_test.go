package migrations

import (
	"strings"
	"testing"
)

func TestInboxSessionIdentityMigrationKeepsLegacyRowsExplicit(t *testing.T) {
	up, err := files.ReadFile("032_inbox_session_identity.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	script := string(up)
	for _, required := range []string{
		"is_group_chat BOOLEAN NOT NULL DEFAULT FALSE",
		"session_owner_id VARCHAR(255) NOT NULL DEFAULT ''",
		"routing_version INTEGER NOT NULL DEFAULT 0",
		"routing_version = 0 OR octet_length(session_owner_id) > 0",
	} {
		if !strings.Contains(script, required) {
			t.Errorf("032 up migration missing %q", required)
		}
	}

	down, err := files.ReadFile("032_inbox_session_identity.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(down), "DROP COLUMN IF EXISTS routing_version") {
		t.Fatal("032 down migration does not remove routing_version")
	}
}
