package admin

import "testing"

func TestPrincipalPermissions(t *testing.T) {
	tenantAdmin := Principal{Role: RoleTenantAdmin, TenantIDs: []string{"tenant-a"}}
	if !tenantAdmin.Allows(PermissionWrite, "tenant-a") ||
		tenantAdmin.Allows(PermissionWrite, "tenant-b") ||
		tenantAdmin.Allows(PermissionTenantCreate, "tenant-a") {
		t.Fatalf("unexpected tenant Admin permissions")
	}
	auditor := Principal{Role: RoleAuditor, TenantIDs: []string{"tenant-a"}}
	if !auditor.Allows(PermissionRead, "tenant-a") || auditor.Allows(PermissionOperate, "tenant-a") {
		t.Fatalf("unexpected auditor permissions")
	}
}
