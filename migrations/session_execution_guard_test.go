package migrations

import (
	"strings"
	"testing"
)

func TestSessionExecutionGuardMigrationKeepsAdmissionFailClosed(t *testing.T) {
	up, err := files.ReadFile("019_session_execution_guard.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	text := string(up)
	for _, required := range []string{
		"session_execution_guards",
		"PRIMARY KEY (tenant_id, agent_app_id, session_id)",
		"current_execution_id",
		"status IN ('READY','RUNNING','BLOCKED')",
		"execution_reconciliations",
		"decision='SAFE_TO_RETRY'",
		"legacy_unresolved_execution",
		"trpc_sync_session_execution_guard",
		"execution_record_sync_session_guard",
		"session execution guard mismatch",
	} {
		if !strings.Contains(text, required) {
			t.Errorf("guard migration is missing %q", required)
		}
	}
}

func TestSessionExecutionGuardRollbackPreservesActiveStateAndAudit(t *testing.T) {
	down, err := files.ReadFile("019_session_execution_guard.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	text := string(down)
	for _, required := range []string{
		"execution_reconciliations",
		"status <> 'READY'",
		"cannot roll back session guards",
		"DROP TRIGGER IF EXISTS execution_record_sync_session_guard",
	} {
		if !strings.Contains(text, required) {
			t.Errorf("guard rollback is missing %q", required)
		}
	}
	if strings.Index(text, "cannot roll back session guards") > strings.Index(text, "DROP TRIGGER IF EXISTS execution_record_sync_session_guard") {
		t.Fatal("rollback drops the trigger before evaluating safety checks")
	}
}

func TestSessionExecutionGuardReconcileMigrationIsIdempotentAndBounded(t *testing.T) {
	up, err := files.ReadFile("021_session_execution_guard_reconcile.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	text := string(up)
	for _, required := range []string{
		"CREATE OR REPLACE FUNCTION trpc_sync_session_execution_guard",
		"NEW.status = 'ABANDONED'",
		"NEW.error_message = 'expired_execution_lease'",
		"status='BLOCKED'",
		"session execution guard mismatch",
	} {
		if !strings.Contains(text, required) {
			t.Errorf("reconcile migration is missing %q", required)
		}
	}
	down, err := files.ReadFile("021_session_execution_guard_reconcile.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	downText := string(down)
	for _, required := range []string{
		"cannot roll back reconciliation trigger",
		"legacy expiry rows are not current guard owners",
		"CREATE OR REPLACE FUNCTION trpc_sync_session_execution_guard",
	} {
		if !strings.Contains(downText, required) {
			t.Errorf("reconcile rollback is missing %q", required)
		}
	}
}
