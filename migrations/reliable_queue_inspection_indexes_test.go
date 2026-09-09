package migrations

import (
	"os"
	"strings"
	"testing"
)

func TestReliableQueueInspectionIndexesCoverAutomaticStatesAndRollback(t *testing.T) {
	up, err := os.ReadFile("034_reliable_queue_inspection_indexes.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	down, err := os.ReadFile("034_reliable_queue_inspection_indexes.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"idx_inbox_automatic_queue_created",
		"idx_outbox_automatic_queue_created",
		"status IN ('RECEIVED','PROCESSING','RETRY_WAIT','WAITING_APPROVAL')",
		"status IN ('REPLY_PENDING','DELIVERING','DISPATCH_STARTED','RETRY_WAIT')",
		"created_at, id",
	} {
		if !strings.Contains(string(up), required) {
			t.Errorf("034 up migration missing %q", required)
		}
	}
	for _, required := range []string{
		"idx_inbox_automatic_queue_created",
		"idx_outbox_automatic_queue_created",
	} {
		if !strings.Contains(string(down), required) {
			t.Errorf("034 down migration missing %q", required)
		}
	}
}
