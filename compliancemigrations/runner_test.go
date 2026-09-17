package compliancemigrations

import (
	"strings"
	"testing"
)

func complianceSchemaBaseline(t *testing.T) Migration {
	t.Helper()
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("compliance migrations=%d, want one acceptance baseline", len(all))
	}
	migration := all[0]
	if migration.Version != "000001" || migration.Name != "compliance_schema" {
		t.Fatalf("migration=%#v", migration)
	}
	return migration
}

func TestComplianceSchemaBaselineContainsImmutableAuditAndGuardedPurge(t *testing.T) {
	migration := complianceSchemaBaseline(t)
	for _, clause := range []string{
		"CREATE SCHEMA compliance", "CREATE TABLE compliance.audit_event", "CREATE TABLE compliance.quarantine_alert",
		"CREATE TABLE compliance.audit_retention_floor", "CREATE TABLE compliance.audit_retention_policy",
		"CREATE TABLE compliance.audit_legal_hold", "CREATE TABLE compliance.audit_quarantine_resolution",
		"CREATE TABLE compliance.audit_purge_batch", "CREATE TABLE compliance.audit_purge_certificate",
		"CREATE TABLE compliance.audit_query_record", "CREATE ROLE compliance_purger",
		"CREATE FUNCTION compliance.execute_audit_purge_batch", "CREATE FUNCTION compliance.plan_audit_purge_batch",
		"pg_has_role(session_user, 'compliance_purger', 'MEMBER')", "current_setting('compliance.purge_authorized', true)",
		"REVOKE ALL ON FUNCTION compliance.execute_audit_purge_batch", "GRANT ALL ON FUNCTION compliance.execute_audit_purge_batch",
		"INSERT INTO compliance.audit_retention_floor", "INSERT INTO compliance.audit_retention_class_rule",
	} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("baseline lacks %q", clause)
		}
	}
}

func TestComplianceSchemaBaselineIsTransactionWrappedAndRefusesDestructiveRollback(t *testing.T) {
	migration := complianceSchemaBaseline(t)
	if _, err := transactionBody(migration.Up); err != nil {
		t.Fatalf("baseline up: %v", err)
	}
	if _, err := transactionBody(migration.Down); err != nil {
		t.Fatalf("baseline down: %v", err)
	}
	if !strings.Contains(migration.Down, "cannot roll back compliance retention after purge execution") {
		t.Fatal("baseline down must refuse after purge execution")
	}
}
