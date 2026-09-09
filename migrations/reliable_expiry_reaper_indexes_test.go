package migrations

import (
	"io/fs"
	"strings"
	"testing"
)

func TestReliableExpiryReaperMigrationHasNarrowCandidateIndexes(t *testing.T) {
	up, err := fs.ReadFile(files, "031_reliable_expiry_reaper_indexes.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	script := string(up)
	for _, required := range []string{
		"idx_inbox_reap_final_attempt",
		"status='PROCESSING' AND attempt_count >= max_attempts",
		"idx_inbox_reap_approval",
		"approval_deadline NULLS FIRST",
		"idx_outbox_reap_final_attempt",
		"status='DELIVERING' AND attempt_count >= max_attempts",
	} {
		if !strings.Contains(script, required) {
			t.Errorf("031 up migration missing %q", required)
		}
	}
	down, err := fs.ReadFile(files, "031_reliable_expiry_reaper_indexes.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range []string{
		"idx_inbox_reap_final_attempt",
		"idx_inbox_reap_approval",
		"idx_outbox_reap_final_attempt",
	} {
		if !strings.Contains(string(down), "DROP INDEX IF EXISTS "+index) {
			t.Errorf("031 down migration does not remove %q", index)
		}
	}
}
