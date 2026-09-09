package migrations

import "testing"

func TestWebhookRouteKeyMigrationPreflightLocksTenantTable(t *testing.T) {
	if got, want := migrationPreflightStatement(webhookRouteKeyBackfillVersion), "LOCK TABLE tenants IN SHARE ROW EXCLUSIVE MODE"; got != want {
		t.Fatalf("migration preflight=%q, want %q", got, want)
	}
	for _, version := range []string{queueInspectionIndexesVersion, tenantQueueScheduleVersion, tenantQueueAdmissionIndexVersion} {
		if got, want := migrationPreflightStatement(version), "SET LOCAL lock_timeout = '5s'; SET LOCAL statement_timeout = '30min'"; got != want {
			t.Fatalf("migration %s preflight=%q, want %q", version, got, want)
		}
	}
	if got := migrationPreflightStatement("027_outbox_reconciliation_wait"); got != "" {
		t.Fatalf("unrelated migration received preflight lock %q", got)
	}
}
