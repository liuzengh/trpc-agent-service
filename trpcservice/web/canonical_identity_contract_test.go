package web

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
)

func seedLocalPlatformUser(t *testing.T, store *identity.MemoryIdentityStore, username, displayName string) string {
	t.Helper()
	hash, err := identity.HashLocalPassword("temporary-password-12345")
	if err != nil {
		t.Fatalf("HashLocalPassword() error = %v", err)
	}
	user, err := store.CreateLocalUser(context.Background(), username, displayName, username+"@example.test", hash, false)
	if err != nil {
		t.Fatalf("CreateLocalUser() error = %v", err)
	}
	return user.PlatformUserID
}

func TestConsoleSystemAdminManagesTenantsAndPlatformUsers(t *testing.T) {
	var (
		store        *identity.MemoryIdentityStore
		initialAdmin string
		targetUser   string
	)
	console := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		store = dependencies.Identities.(*identity.MemoryIdentityStore)
		initialAdmin = seedLocalPlatformUser(t, store, "support.admin", "客服管理员")
		targetUser = seedLocalPlatformUser(t, store, "support.member", "客服成员")
	})
	systemAdmin := identity.SessionUser{PlatformUserID: "system-admin", IsSystemAdmin: true}

	createTenant := console.requestAs(t, systemAdmin, http.MethodPost, "/api/v1/tenants", `{
		"tenant_id":"support-team",
		"display_name":"客服团队",
		"initial_admin_platform_user_id":"`+initialAdmin+`"
	}`)
	if createTenant.Code != http.StatusCreated {
		t.Fatalf("create tenant = %d %s", createTenant.Code, createTenant.Body.String())
	}

	listTenants := console.requestAs(t, systemAdmin, http.MethodGet, "/api/v1/tenants", "")
	if listTenants.Code != http.StatusOK || !strings.Contains(listTenants.Body.String(), "support-team") || !strings.Contains(listTenants.Body.String(), "客服团队") {
		t.Fatalf("list tenants = %d %s", listTenants.Code, listTenants.Body.String())
	}

	suspend := console.requestAs(t, systemAdmin, http.MethodPatch, "/api/v1/tenants/support-team", `{"status":"suspended"}`)
	if suspend.Code != http.StatusNoContent {
		t.Fatalf("suspend tenant = %d %s", suspend.Code, suspend.Body.String())
	}
	activate := console.requestAs(t, systemAdmin, http.MethodPatch, "/api/v1/tenants/support-team", `{"status":"active"}`)
	if activate.Code != http.StatusNoContent {
		t.Fatalf("activate tenant = %d %s", activate.Code, activate.Body.String())
	}

	users := console.requestAs(t, systemAdmin, http.MethodGet, "/api/v1/users?q=客服&limit=10", "")
	if users.Code != http.StatusOK || !strings.Contains(users.Body.String(), targetUser) {
		t.Fatalf("list users = %d %s", users.Code, users.Body.String())
	}

	update := console.requestAs(t, systemAdmin, http.MethodPut, "/api/v1/users", `{
		"platform_user_id":"`+targetUser+`",
		"status":"active",
		"is_system_admin":true
	}`)
	if update.Code != http.StatusNoContent {
		t.Fatalf("update platform user = %d %s", update.Code, update.Body.String())
	}
	isAdmin, err := store.IsSystemAdmin(context.Background(), targetUser)
	if err != nil || !isAdmin {
		t.Fatalf("IsSystemAdmin() = %v, %v", isAdmin, err)
	}

	selfUpdate := console.requestAs(t, systemAdmin, http.MethodPut, "/api/v1/users", `{
		"platform_user_id":"system-admin",
		"status":"suspended",
		"is_system_admin":false
	}`)
	if selfUpdate.Code != http.StatusForbidden {
		t.Fatalf("system admin self update = %d, want 403", selfUpdate.Code)
	}
}

func TestConsoleSystemAdminCreatesAndResetsLocalUser(t *testing.T) {
	console := testConsoleHandler(t)
	systemAdmin := identity.SessionUser{PlatformUserID: "system-admin", IsSystemAdmin: true}

	create := console.requestAs(t, systemAdmin, http.MethodPost, "/api/v1/users/local", `{
		"username":"new.support",
		"display_name":"新客服用户",
		"email":"new.support@example.test"
	}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("create local user = %d %s", create.Code, create.Body.String())
	}
	var created struct {
		PlatformUserID     string `json:"platform_user_id"`
		Username           string `json:"username"`
		TemporaryPassword  string `json:"temporary_password"`
		MustChangePassword bool   `json:"must_change_password"`
	}
	if err := json.Unmarshal(create.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.PlatformUserID == "" || created.Username != "new.support" || created.TemporaryPassword == "" || !created.MustChangePassword {
		t.Fatalf("created local user = %+v", created)
	}

	reset := console.requestAs(t, systemAdmin, http.MethodPost, "/api/v1/users/local/reset", `{"platform_user_id":"`+created.PlatformUserID+`"}`)
	if reset.Code != http.StatusOK || !strings.Contains(reset.Body.String(), `"must_change_password":true`) {
		t.Fatalf("reset local user = %d %s", reset.Code, reset.Body.String())
	}

	missing := console.requestAs(t, systemAdmin, http.MethodPost, "/api/v1/users/local/reset", `{"platform_user_id":"missing-user"}`)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("reset missing local user = %d, want 404", missing.Code)
	}

	bad := console.requestAs(t, systemAdmin, http.MethodPost, "/api/v1/users/local/reset", `{}`)
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("reset without user = %d, want 400", bad.Code)
	}
}

func TestConsoleTenantAdminManagesMembershipsAndCandidates(t *testing.T) {
	var (
		store     *identity.MemoryIdentityStore
		adminID   string
		candidate string
	)
	console := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		store = dependencies.Identities.(*identity.MemoryIdentityStore)
		adminID = seedLocalPlatformUser(t, store, "tenant.admin", "租户管理员")
		candidate = seedLocalPlatformUser(t, store, "candidate.user", "候选成员")
		if err := store.GrantMembership(context.Background(), "example", adminID, identity.RoleAdmin); err != nil {
			t.Fatalf("GrantMembership() error = %v", err)
		}
	})
	admin := identity.SessionUser{
		PlatformUserID: adminID,
		Tenants:        []identity.TenantRole{{TenantID: "example", Role: identity.RoleAdmin, Status: "active"}},
	}

	candidates := console.requestAs(t, admin, http.MethodGet, "/api/v1/tenant-members?tenant=example&view=candidates&q=候选", "")
	if candidates.Code != http.StatusOK || !strings.Contains(candidates.Body.String(), candidate) {
		t.Fatalf("member candidates = %d %s", candidates.Code, candidates.Body.String())
	}

	add := console.requestAs(t, admin, http.MethodPut, "/api/v1/tenant-members", `{
		"tenant_id":"example",
		"platform_user_id":"`+candidate+`",
		"role":"member",
		"status":"active"
	}`)
	if add.Code != http.StatusNoContent {
		t.Fatalf("add tenant member = %d %s", add.Code, add.Body.String())
	}

	members := console.requestAs(t, admin, http.MethodGet, "/api/v1/tenant-members?tenant=example&limit=20", "")
	if members.Code != http.StatusOK || !strings.Contains(members.Body.String(), candidate) {
		t.Fatalf("members = %d %s", members.Code, members.Body.String())
	}

	selfDemote := console.requestAs(t, admin, http.MethodPut, "/api/v1/tenant-members", `{
		"tenant_id":"example",
		"platform_user_id":"`+adminID+`",
		"role":"member",
		"status":"active"
	}`)
	if selfDemote.Code != http.StatusForbidden {
		t.Fatalf("tenant admin self demotion = %d, want 403", selfDemote.Code)
	}

	auditForMember := console.requestAs(t, admin, http.MethodPut, "/api/v1/tenant-members", `{
		"tenant_id":"example",
		"platform_user_id":"`+candidate+`",
		"role":"member",
		"status":"active",
		"conversation_content_audit":true
	}`)
	if auditForMember.Code != http.StatusBadRequest {
		t.Fatalf("content audit for member = %d, want 400", auditForMember.Code)
	}
}

func TestConsoleIdentityEndpointsRejectUnauthorizedMethodsAndParameters(t *testing.T) {
	console := testConsoleHandler(t)
	member := identity.SessionUser{PlatformUserID: "member", Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleMember, Status: "active"}}}
	systemAdmin := identity.SessionUser{PlatformUserID: "system-admin", IsSystemAdmin: true}

	for _, endpoint := range []string{"/api/v1/tenants", "/api/v1/users", "/api/v1/users/local"} {
		recorder := console.requestAs(t, systemAdmin, http.MethodDelete, endpoint, "")
		if recorder.Code != http.StatusMethodNotAllowed {
			t.Fatalf("DELETE %s = %d, want 405", endpoint, recorder.Code)
		}
	}
	if got := console.requestAs(t, member, http.MethodPost, "/api/v1/tenants", `{}`); got.Code != http.StatusForbidden {
		t.Fatalf("non-admin tenant create = %d, want 403", got.Code)
	}
	if got := console.requestAs(t, member, http.MethodGet, "/api/v1/users", ""); got.Code != http.StatusForbidden {
		t.Fatalf("non-admin users list = %d, want 403", got.Code)
	}
	if got := console.requestAs(t, systemAdmin, http.MethodPatch, "/api/v1/tenants/invalid/path", `{"status":"active"}`); got.Code != http.StatusBadRequest {
		t.Fatalf("invalid tenant path = %d, want 400", got.Code)
	}
	if got := console.requestAs(t, member, http.MethodGet, "/api/v1/tenant-members", ""); got.Code != http.StatusBadRequest {
		t.Fatalf("members without tenant = %d, want 400", got.Code)
	}
}
