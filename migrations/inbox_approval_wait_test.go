package migrations

import (
	"io/fs"
	"strings"
	"testing"
)

func TestInboxApprovalWaitMigrationDefinesBoundedState(t *testing.T) {
	up, err := fs.ReadFile(files, "030_inbox_approval_wait.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	script := string(up)
	for _, required := range []string{
		"approval_deadline TIMESTAMPTZ",
		"'WAITING_APPROVAL'",
		"inbox_messages_approval_wait_check",
		"approval_deadline IS NOT NULL",
		"WAITING_RECONCILIATION",
	} {
		if !strings.Contains(script, required) {
			t.Errorf("030 up migration missing %q", required)
		}
	}
	down, err := fs.ReadFile(files, "030_inbox_approval_wait.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(down), "cannot roll back inbox approval wait while waiting messages exist") {
		t.Error("030 down migration does not protect waiting approval messages")
	}
}
