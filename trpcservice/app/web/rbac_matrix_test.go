package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/member"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/auth"
)

// The role model, in one place: owner is platform-wide, admin manages exactly
// one tenant, member is an employee who creates and shares tenant assets
// (agents, knowledge, skills, IM bindings, endpoints), grants tools to the
// agents it may use, chats, and reads its own usage — but never manages
// tenants, members, credentials or audit.
func TestRolePermissionMatrix(t *testing.T) {
	cases := []struct {
		role string
		perm string
		want bool
	}{
		{member.RoleOwner, PermTenantManage, true},
		{member.RoleOwner, PermMemberManage, true},
		{member.RoleOwner, PermSecretManage, true},
		{member.RoleOwner, PermAuditRead, true},
		{member.RoleOwner, PermDlqManage, true},
		{member.RoleOwner, PermEndpointManage, true},

		{member.RoleAdmin, PermTenantManage, false}, // admins do not manage tenants
		{member.RoleAdmin, PermMemberManage, true},
		{member.RoleAdmin, PermSecretManage, false},
		{member.RoleAdmin, PermAuditRead, true},
		{member.RoleAdmin, PermDlqManage, true},
		{member.RoleAdmin, PermAgentCreate, true},
		{member.RoleAdmin, PermEndpointManage, true},

		// The member's contribution surface: create and share tenant assets,
		// including its own agents.
		{member.RoleMember, PermAgentCreate, true},
		{member.RoleMember, PermAgentUpdate, true},
		{member.RoleMember, PermAgentDelete, true},
		{member.RoleMember, PermKbManage, true},
		{member.RoleMember, PermSkillManage, true},
		{member.RoleMember, PermChannelManage, true},
		{member.RoleMember, PermEndpointManage, true},
		{member.RoleMember, PermToolManage, true}, // catalog + grants, no definition write
		{member.RoleMember, PermUsageRead, true},  // own usage only
		{member.RoleMember, PermAgentRead, true},
		{member.RoleMember, PermChat, true},
		{member.RoleMember, PermTenantRead, true},

		// What a member must never reach.
		{member.RoleMember, PermTenantManage, false},
		{member.RoleMember, PermMemberManage, false},
		{member.RoleMember, PermSecretManage, false},
		{member.RoleMember, PermAuditRead, false},
		{member.RoleMember, PermDlqManage, false},
	}
	for _, tc := range cases {
		if got := HasPermission(tc.role, tc.perm); got != tc.want {
			t.Errorf("HasPermission(%s, %s) = %v, want %v", tc.role, tc.perm, got, tc.want)
		}
	}
}

// A plain member must not reach the platform-management surfaces the
// requirement lists as invisible: tenant management, member management,
// credentials, audit and the DLQ.
func TestMemberCannotReachManagementRoutes(t *testing.T) {
	alice := &auth.Claims{TenantID: "acme", UserID: "alice", Role: member.RoleMember}
	for _, tc := range []struct {
		method, path string
	}{
		{http.MethodPost, "/tenants"},
		{http.MethodPut, "/tenants/acme"},
		{http.MethodDelete, "/tenants/acme"},
		{http.MethodGet, "/members"},
		{http.MethodGet, "/secrets"},
		{http.MethodGet, "/audit"},
		{http.MethodGet, "/dlq"},
	} {
		rec := routeGuardResult(t, alice, tc.method, tc.path)
		if rec.Code != http.StatusForbidden {
			t.Errorf("member %s %s = %d, want 403", tc.method, tc.path, rec.Code)
		}
	}

	// The member's contribution surfaces stay open. GET /tenants is included on
	// purpose: a member may read the directory, but the handler narrows it to
	// the caller's own tenant and the write routes above stay shut.
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/tenants"},
		{http.MethodGet, "/agents"},
		{http.MethodPost, "/agents"},
		{http.MethodPut, "/agents/a1"},
		{http.MethodDelete, "/agents/a1"},
		{http.MethodPost, "/agents/a1/publish"},
		{http.MethodGet, "/endpoints"},
		{http.MethodPost, "/endpoints"},
		{http.MethodGet, "/kbs"},
		{http.MethodPost, "/kbs"},
		{http.MethodGet, "/skills"},
		{http.MethodPost, "/skills"},
		{http.MethodGet, "/channels"},
		{http.MethodPost, "/channels"},
		{http.MethodGet, "/tools"},
		{http.MethodGet, "/usage"},
		{http.MethodGet, "/history"},
		{http.MethodPost, "/chat"},
	} {
		rec := routeGuardResult(t, alice, tc.method, tc.path)
		if rec.Code == http.StatusForbidden {
			t.Errorf("member %s %s should be reachable, got 403", tc.method, tc.path)
		}
	}
}

// routeGuardResult runs the coarse route gate and reports what it decided.
func routeGuardResult(t *testing.T, claims *auth.Claims, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	handler := RequireRoutePermission(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(method, path, nil)
	req = req.WithContext(context.WithValue(req.Context(), AuthUserKey, claims))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// TestScopeHelpersPinNonOwners covers the two scoping rules every list and
// history endpoint relies on.
func TestScopeHelpersPinNonOwners(t *testing.T) {
	admin := &auth.Claims{TenantID: "acme", UserID: "bob", Role: member.RoleAdmin}
	alice := &auth.Claims{TenantID: "acme", UserID: "alice", Role: member.RoleMember}
	owner := &auth.Claims{UserID: "root", Role: member.RoleOwner}

	// Tenant filter: non-owners are pinned even when the filter is omitted.
	if got := ScopeTenant(admin, ""); got != "acme" {
		t.Errorf("admin ScopeTenant(\"\") = %q, want acme", got)
	}
	if got := ScopeTenant(alice, "globex"); got != "acme" {
		t.Errorf("member ScopeTenant(globex) = %q, want acme", got)
	}
	if got := ScopeTenant(owner, "globex"); got != "globex" {
		t.Errorf("owner ScopeTenant(globex) = %q, want globex", got)
	}
	if got := ScopeTenant(owner, ""); got != "" {
		t.Errorf("owner ScopeTenant(\"\") = %q, want every tenant", got)
	}

	// Member filter: only a plain member is narrowed to its own sessions.
	if got := ScopeMember(alice, ""); got != "alice" {
		t.Errorf("member ScopeMember(\"\") = %q, want alice", got)
	}
	if got := ScopeMember(alice, "bob"); got != "alice" {
		t.Errorf("member ScopeMember(bob) = %q, want alice", got)
	}
	if got := ScopeMember(admin, "bob"); got != "bob" {
		t.Errorf("admin ScopeMember(bob) = %q, want bob (tenant-wide view)", got)
	}

	// Session read rule: tenant first, then producer.
	if !CanReadSession(owner, "anything", "anyone") {
		t.Error("owner must read every session")
	}
	if !CanReadSession(admin, "acme", "alice") {
		t.Error("admin must read its tenant's sessions")
	}
	if CanReadSession(admin, "globex", "alice") {
		t.Error("admin must not read another tenant's session")
	}
	if !CanReadSession(alice, "acme", "alice") {
		t.Error("member must read its own session")
	}
	if CanReadSession(alice, "acme", "bob") {
		t.Error("member must not read a colleague's session")
	}
}

// TestClaimTenantPinsWrites covers the write side of the same rule.
func TestClaimTenantPinsWrites(t *testing.T) {
	admin := &auth.Claims{TenantID: "acme", Role: member.RoleAdmin}
	owner := &auth.Claims{UserID: "root", Role: member.RoleOwner}

	if got := ClaimTenant(admin, "globex"); got != "acme" {
		t.Errorf("admin ClaimTenant(globex) = %q, want acme", got)
	}
	if got := ClaimTenant(admin, ""); got != "acme" {
		t.Errorf("admin ClaimTenant(\"\") = %q, want acme", got)
	}
	if got := ClaimTenant(owner, "globex"); got != "globex" {
		t.Errorf("owner ClaimTenant(globex) = %q, want globex", got)
	}
}

// TestTenantAccessibleSharedAssets documents the one deliberate exception to
// tenant isolation: platform-shared assets carry no tenant.
func TestTenantAccessibleSharedAssets(t *testing.T) {
	alice := &auth.Claims{TenantID: "acme", Role: member.RoleMember}
	owner := &auth.Claims{Role: member.RoleOwner}

	if !TenantAccessible(alice, "acme") {
		t.Error("member must reach its own tenant's assets")
	}
	if TenantAccessible(alice, "globex") {
		t.Error("member must not reach another tenant's assets")
	}
	if !TenantAccessible(alice, "") {
		t.Error("platform-shared (tenantless) assets stay reachable")
	}
	if !TenantAccessible(owner, "globex") {
		t.Error("owner is platform-wide")
	}
	if TenantAccessible(nil, "acme") {
		t.Error("an unauthenticated caller reaches nothing")
	}
}
