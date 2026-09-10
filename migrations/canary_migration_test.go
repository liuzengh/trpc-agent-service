package migrations

import (
	"io/fs"
	"strings"
	"testing"
)

func TestCanaryMigrationLocksTenantAndAppTogether(t *testing.T) {
	contents, err := fs.ReadFile(Files, "0010_agent_app_canary.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	tenantLock := strings.Index(sql, "SELECT status INTO v_tenant_status")
	appLock := strings.Index(sql, "SELECT current_revision, version, status")
	if tenantLock < 0 || appLock < 0 || tenantLock > appLock || !strings.Contains(sql[tenantLock:appLock], "FOR UPDATE") || !strings.Contains(sql[appLock:], "FOR UPDATE") {
		t.Fatal("canary mutation must lock tenant before app in an explicit order")
	}
	if strings.Contains(sql, "FOR UPDATE OF app, tenant") {
		t.Fatal("canary mutation must not rely on join lock order")
	}
	if !strings.Contains(sql, "FROM PUBLIC;") || !strings.Contains(sql, ") TO tenant_admin_writer;") || strings.Contains(sql, ") TO tenant_app_writer;") {
		t.Fatal("canary mutation must revoke public execution and grant the tenant admin writer only")
	}
}
