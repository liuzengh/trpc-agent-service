package migrations

import (
	"strings"
	"testing"
)

func TestExecutionAttemptMigrationPreservesFencingInvariants(t *testing.T) {
	up, err := files.ReadFile("018_execution_attempts.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"invocation_binding_session_format",
		"invocation_binding_payload_hash_format",
		"execution_attempt_number_positive",
		"execution_token_byte_limit",
		"execution_lease_order",
		"uq_execution_request_attempt",
		"uq_execution_request_running",
		"uq_execution_token",
		"invocation_result_execution_tenant_fk",
		"uq_invocation_result_execution",
	} {
		if !strings.Contains(string(up), required) {
			t.Errorf("migration 018 is missing %s", required)
		}
	}
	if !strings.Contains(string(up), "legacy:") || !strings.Contains(string(up), "must be drained") {
		t.Error("migration 018 no longer documents and fences the legacy-worker drain boundary")
	}
}

func TestExecutionAttemptRollbackRefusesDestructiveHistoryCollapse(t *testing.T) {
	down, err := files.ReadFile("018_execution_attempts.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"HAVING COUNT(*) > 1",
		"RAISE EXCEPTION",
		"uq_execution_request",
	} {
		if !strings.Contains(string(down), required) {
			t.Errorf("migration 018 rollback is missing %s", required)
		}
	}
}
