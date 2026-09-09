package migrations

import (
	"strings"
	"testing"
)

func TestToolApprovalMigrationHasTenantScopeAndAtomicConsumeFields(t *testing.T) {
	up, err := files.ReadFile("028_tool_approvals.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	down, err := files.ReadFile("028_tool_approvals.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	text := string(up)
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS tool_approvals",
		"tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id)",
		"session_owner_id VARCHAR(255) NOT NULL",
		"args_hash VARCHAR(71) NOT NULL",
		"token_hash BYTEA",
		"consumed_at TIMESTAMPTZ",
		"expires_at TIMESTAMPTZ NOT NULL",
		"granted_at TIMESTAMPTZ",
	} {
		if !strings.Contains(text, required) {
			t.Errorf("028 up migration missing %q", required)
		}
	}
	if !strings.Contains(string(down), "DROP TABLE IF EXISTS tool_approvals") {
		t.Error("028 down migration does not remove tool_approvals")
	}
}

func TestToolApprovalInvariantMigrationIsAdditive(t *testing.T) {
	up, err := files.ReadFile("029_tool_approvals_invariants.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	down, err := files.ReadFile("029_tool_approvals_invariants.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(up), "ADD CONSTRAINT tool_approvals_consumed_requires_grant") ||
		!strings.Contains(string(up), "consumed_at IS NULL OR granted_at IS NOT NULL") {
		t.Error("029 up migration is missing the consumed/granted invariant")
	}
	if !strings.Contains(string(down), "DROP CONSTRAINT IF EXISTS tool_approvals_consumed_requires_grant") {
		t.Error("029 down migration does not remove the named constraint")
	}
}
