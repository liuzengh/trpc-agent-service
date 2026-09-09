package migrations

import (
	"io/fs"
	"regexp"
	"strings"
	"testing"
)

func TestOutboxReconciliationMigrationPreservesBlockedRows(t *testing.T) {
	up, err := fs.ReadFile(files, "027_outbox_reconciliation_wait.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	down, err := fs.ReadFile(files, "027_outbox_reconciliation_wait.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	upSQL := string(up)
	if !strings.Contains(upSQL, "WAITING_RECONCILIATION") {
		t.Error("027 up migration does not define the reconciliation state")
	}
	if !strings.Contains(upSQL, "DROP CONSTRAINT IF EXISTS outbox_messages_status_check") {
		t.Error("027 up migration must replace the previous status constraint")
	}
	// SQL formatting and the order of CHECK values are not behavioral
	// contracts. Extract the status literals and compare them as a set so a
	// harmless formatter or reordering does not break the migration gate.
	statusValues := map[string]bool{}
	for _, match := range regexp.MustCompile(`'([A-Z_]+)'`).FindAllStringSubmatch(upSQL, -1) {
		statusValues[match[1]] = true
	}
	for _, required := range []string{
		"REPLY_PENDING",
		"DELIVERING",
		"RETRY_WAIT",
		"WAITING_RECONCILIATION",
		"REPLIED",
		"DEAD_LETTERED",
	} {
		if !statusValues[required] {
			t.Errorf("027 up migration missing status %q", required)
		}
	}
	if !strings.Contains(string(down), "cannot roll back outbox reconciliation state") {
		t.Error("027 down migration must refuse to discard blocked messages")
	}
}
