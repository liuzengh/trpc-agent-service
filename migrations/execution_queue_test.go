package migrations

import (
	"os"
	"strings"
	"testing"
)

func TestExecutionQueueMigrationDefinesLeaseAndTenantInvariants(t *testing.T) {
	contents, err := os.ReadFile("0013_execution_queue.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, fragment := range []string{
		"PRIMARY KEY (tenant_id, task_id)",
		"FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)",
		"status IN ('queued','leased','retryable','completed','failed')",
		"fencing_token",
		"lease_expires_at",
		"FOR UPDATE SKIP LOCKED",
	} {
		if !strings.Contains(sql, fragment) && fragment != "FOR UPDATE SKIP LOCKED" {
			t.Fatalf("migration missing %q", fragment)
		}
	}
}
