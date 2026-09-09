package migrations

import (
	"os"
	"strings"
	"testing"
)

func TestTenantQueueScheduleMigrationIsOperatorOwnedAndReversible(t *testing.T) {
	up, err := os.ReadFile("035_tenant_queue_schedule.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	down, err := os.ReadFile("035_tenant_queue_schedule.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	upText, downText := string(up), string(down)
	for _, required := range []string{
		"tenant_queue_schedule",
		"REFERENCES tenants(id) ON DELETE CASCADE",
		"weight BIGINT NOT NULL DEFAULT 1",
		"max_queued BIGINT NOT NULL DEFAULT 0",
		"max_inflight BIGINT NOT NULL DEFAULT 0",
		"virtual_runtime BIGINT NOT NULL DEFAULT 0",
		"INSERT INTO tenant_queue_schedule (tenant_id)",
		"SELECT id",
		"FROM tenants",
		"idx_inbox_fair_tenant_head",
		"status IN ('RECEIVED','PROCESSING','RETRY_WAIT','WAITING_APPROVAL')",
	} {
		if !strings.Contains(upText, required) {
			t.Errorf("035 up migration missing %q", required)
		}
	}
	for _, required := range []string{
		"DROP INDEX IF EXISTS idx_inbox_fair_tenant_head",
		"DROP TABLE IF EXISTS tenant_queue_schedule",
	} {
		if !strings.Contains(downText, required) {
			t.Errorf("035 down migration missing %q", required)
		}
	}
}

func TestTenantQueueAdmissionIndexMigrationIsReversible(t *testing.T) {
	up, err := os.ReadFile("036_tenant_queue_admission_index.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	down, err := os.ReadFile("036_tenant_queue_admission_index.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"idx_inbox_queue_admission_tenant_status",
		"ON inbox_messages (tenant_id, status)",
		"status IN ('RECEIVED','PROCESSING','RETRY_WAIT','WAITING_APPROVAL')",
	} {
		if !strings.Contains(string(up), required) {
			t.Errorf("036 up migration missing %q", required)
		}
	}
	if !strings.Contains(string(down), "DROP INDEX IF EXISTS idx_inbox_queue_admission_tenant_status") {
		t.Error("036 down migration missing admission index drop")
	}
}
