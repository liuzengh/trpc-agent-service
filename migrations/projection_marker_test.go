package migrations

import (
	"strings"
	"testing"
)

func TestProjectionMarkerMigrationDistinguishesCopiedFromAppliedRecords(t *testing.T) {
	up, err := files.ReadFile("040_data_projection_marker.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	text := string(up)
	for _, required := range []string{
		"ADD COLUMN IF NOT EXISTS projected_at TIMESTAMPTZ",
		"WHERE projected_at IS NULL",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("040 migration missing %q", required)
		}
	}
}
