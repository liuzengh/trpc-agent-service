package web

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
)

func TestConsoleTenantMembersAreAdminOnlyAndRevokeStaleSessions(t *testing.T) {
	loginSessions := identity.NewMemorySessionStore()
	var target identity.PlatformUser
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		store := dependencies.Identities.(*identity.MemoryIdentityStore)
		ctx := context.Background()
		if err := store.UpsertLoginProvider(ctx, identity.ProviderDescriptor{
			ProviderID: "wecom-default", Type: identity.ProviderWeCom, DisplayName: "企业微信",
		}, "corp-default"); err != nil {
			t.Fatal(err)
		}
		var err error
		target, err = store.ResolveLoginIdentity(ctx, identity.Identity{
			ProviderID: "wecom-default", ProviderType: identity.ProviderWeCom,
			EnterpriseID: "corp-default", SubjectID: "lisi", DisplayName: "李四", Email: "lisi@example.com",
		})
		if err != nil {
			t.Fatal(err)
		}
		dependencies.LoginSessions = loginSessions
	})

	targetSession, err := loginSessions.Create(context.Background(), identity.SessionUser{PlatformUserID: target.PlatformUserID}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	admin := identity.SessionUser{PlatformUserID: "tenant-admin", Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleAdmin, Status: "active"}}}
	update := handler.requestAs(t, admin, http.MethodPut, "/api/v1/tenant-members", `{"tenant_id":"example","platform_user_id":"`+target.PlatformUserID+`","role":"member","status":"active"}`)
	if update.Code != http.StatusNoContent {
		t.Fatalf("update member = %d %s", update.Code, update.Body.String())
	}
	if _, err := loginSessions.Get(context.Background(), targetSession); !errors.Is(err, identity.ErrSessionNotFound) {
		t.Fatalf("target session after role change = %v, want revoked", err)
	}
	listed := handler.requestAs(t, admin, http.MethodGet, "/api/v1/tenant-members?tenant=example", "")
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), "李四") || !strings.Contains(listed.Body.String(), "企业微信") {
		t.Fatalf("list members = %d %s", listed.Code, listed.Body.String())
	}
	member := identity.SessionUser{PlatformUserID: "member", Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleMember, Status: "active"}}}
	denied := handler.requestAs(t, member, http.MethodGet, "/api/v1/tenant-members?tenant=example", "")
	if denied.Code != http.StatusForbidden {
		t.Fatalf("member members status = %d, want 403", denied.Code)
	}
}

func TestConsoleSystemAdminWithoutMembershipCannotAccessTenantBusiness(t *testing.T) {
	var target identity.PlatformUser
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		store := dependencies.Identities.(*identity.MemoryIdentityStore)
		ctx := context.Background()
		if err := store.UpsertLoginProvider(ctx, identity.ProviderDescriptor{
			ProviderID: "oidc-default", Type: identity.ProviderOIDC, DisplayName: "企业 SSO",
		}, "https://sso.example.com"); err != nil {
			t.Fatal(err)
		}
		var err error
		target, err = store.ResolveLoginIdentity(ctx, identity.Identity{
			ProviderID: "oidc-default", ProviderType: identity.ProviderOIDC,
			EnterpriseID: "https://sso.example.com", SubjectID: "employee-1", DisplayName: "王五",
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	systemAdmin := identity.SessionUser{PlatformUserID: "system-admin", IsSystemAdmin: true}
	assign := handler.requestAs(t, systemAdmin, http.MethodPut, "/api/v1/tenant-members", `{"tenant_id":"example","platform_user_id":"`+target.PlatformUserID+`","role":"admin","status":"active"}`)
	if assign.Code != http.StatusForbidden {
		t.Fatalf("system admin tenant membership mutation = %d, want 403", assign.Code)
	}
	apps := handler.requestAs(t, systemAdmin, http.MethodGet, "/api/v1/apps?tenant=example", "")
	if apps.Code != http.StatusForbidden {
		t.Fatalf("system admin tenant resource access = %d, want 403", apps.Code)
	}
}

func TestConsoleUsersAreSystemAdminOnlyAndAccessChangesRevokeSessions(t *testing.T) {
	loginSessions := identity.NewMemorySessionStore()
	var target identity.PlatformUser
	var systemAdminUser identity.PlatformUser
	var identities *identity.MemoryIdentityStore
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		store := dependencies.Identities.(*identity.MemoryIdentityStore)
		identities = store
		ctx := context.Background()
		if err := store.UpsertLoginProvider(ctx, identity.ProviderDescriptor{
			ProviderID: "feishu-default", Type: identity.ProviderFeishu, DisplayName: "飞书",
		}, "tenant-key"); err != nil {
			t.Fatal(err)
		}
		var err error
		target, err = store.ResolveLoginIdentity(ctx, identity.Identity{
			ProviderID: "feishu-default", ProviderType: identity.ProviderFeishu,
			EnterpriseID: "tenant-key", SubjectID: "ou_user", DisplayName: "赵六",
		})
		if err != nil {
			t.Fatal(err)
		}
		systemAdminUser, err = store.ResolveLoginIdentity(ctx, identity.Identity{
			ProviderID: "feishu-default", ProviderType: identity.ProviderFeishu,
			EnterpriseID: "tenant-key", SubjectID: "system-admin", DisplayName: "系统管理员",
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.SetSystemAdmin(ctx, systemAdminUser.PlatformUserID, true); err != nil {
			t.Fatal(err)
		}
		dependencies.LoginSessions = loginSessions
	})

	targetSession, err := loginSessions.Create(context.Background(), identity.SessionUser{PlatformUserID: target.PlatformUserID}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	tenantAdmin := identity.SessionUser{PlatformUserID: "tenant-admin", Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleAdmin, Status: "active"}}}
	if denied := handler.requestAs(t, tenantAdmin, http.MethodGet, "/api/v1/users", ""); denied.Code != http.StatusForbidden {
		t.Fatalf("tenant admin users status = %d, want 403", denied.Code)
	}

	systemAdmin := identity.SessionUser{PlatformUserID: systemAdminUser.PlatformUserID, IsSystemAdmin: true}
	listed := handler.requestAs(t, systemAdmin, http.MethodGet, "/api/v1/users", "")
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), "赵六") || !strings.Contains(listed.Body.String(), "飞书") {
		t.Fatalf("list users = %d %s", listed.Code, listed.Body.String())
	}
	updated := handler.requestAs(t, systemAdmin, http.MethodPut, "/api/v1/users", `{"platform_user_id":"`+target.PlatformUserID+`","status":"active","is_system_admin":true}`)
	if updated.Code != http.StatusNoContent {
		t.Fatalf("update user = %d %s", updated.Code, updated.Body.String())
	}
	if _, err := loginSessions.Get(context.Background(), targetSession); !errors.Is(err, identity.ErrSessionNotFound) {
		t.Fatalf("target session after access change = %v, want revoked", err)
	}
	isAdmin, err := identities.IsSystemAdmin(context.Background(), target.PlatformUserID)
	if err != nil || !isAdmin {
		t.Fatalf("target system admin = %v, %v", isAdmin, err)
	}

	targetSession, err = loginSessions.Create(context.Background(), identity.SessionUser{PlatformUserID: target.PlatformUserID, IsSystemAdmin: true}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	updated = handler.requestAs(t, systemAdmin, http.MethodPut, "/api/v1/users", `{"platform_user_id":"`+target.PlatformUserID+`","status":"suspended","is_system_admin":false}`)
	if updated.Code != http.StatusNoContent {
		t.Fatalf("suspend user = %d %s", updated.Code, updated.Body.String())
	}
	if _, err := loginSessions.Get(context.Background(), targetSession); !errors.Is(err, identity.ErrSessionNotFound) {
		t.Fatalf("target session after suspension = %v, want revoked", err)
	}
	isAdmin, err = identities.IsSystemAdmin(context.Background(), target.PlatformUserID)
	if err != nil || isAdmin {
		t.Fatalf("target system admin after suspension = %v, %v", isAdmin, err)
	}
}

func TestConsoleUsersAndTenantCandidatesUseCursorPages(t *testing.T) {
	var firstUser identity.PlatformUser
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		store := dependencies.Identities.(*identity.MemoryIdentityStore)
		ctx := context.Background()
		if err := store.UpsertLoginProvider(ctx, identity.ProviderDescriptor{
			ProviderID: "oidc-page", Type: identity.ProviderOIDC, DisplayName: "企业 SSO",
		}, "https://sso.example.com"); err != nil {
			t.Fatal(err)
		}
		for index, name := range []string{"Alice", "Bob", "Carol"} {
			user, err := store.ResolveLoginIdentity(ctx, identity.Identity{
				ProviderID: "oidc-page", ProviderType: identity.ProviderOIDC,
				EnterpriseID: "https://sso.example.com", SubjectID: strings.ToLower(name), DisplayName: name,
				Email: strings.ToLower(name) + "@example.com",
			})
			if err != nil {
				t.Fatal(err)
			}
			if index == 0 {
				firstUser = user
			}
		}
		if err := store.SetTenantMembership(ctx, "example", firstUser.PlatformUserID, identity.RoleMember, "active"); err != nil {
			t.Fatal(err)
		}
	})

	admin := identity.SessionUser{PlatformUserID: "system-admin", IsSystemAdmin: true, Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleAdmin, Status: "active"}}}
	first := handler.requestAs(t, admin, http.MethodGet, "/api/v1/users?limit=1", "")
	var firstPage struct {
		Users      []identity.MemberSummary `json:"users"`
		NextCursor string                   `json:"next_cursor"`
	}
	if first.Code != http.StatusOK || json.Unmarshal(first.Body.Bytes(), &firstPage) != nil || len(firstPage.Users) != 1 || firstPage.NextCursor == "" {
		t.Fatalf("first users page = %d %s", first.Code, first.Body.String())
	}
	second := handler.requestAs(t, admin, http.MethodGet, "/api/v1/users?limit=1&cursor="+firstPage.NextCursor, "")
	var secondPage struct {
		Users []identity.MemberSummary `json:"users"`
	}
	if second.Code != http.StatusOK || json.Unmarshal(second.Body.Bytes(), &secondPage) != nil || len(secondPage.Users) != 1 || secondPage.Users[0].PlatformUserID == firstPage.Users[0].PlatformUserID {
		t.Fatalf("second users page = %d %s", second.Code, second.Body.String())
	}

	candidates := handler.requestAs(t, admin, http.MethodGet, "/api/v1/tenant-members?tenant=example&view=candidates&q=alice", "")
	var candidatePage struct {
		Candidates []identity.MemberSummary `json:"candidates"`
	}
	if candidates.Code != http.StatusOK || json.Unmarshal(candidates.Body.Bytes(), &candidatePage) != nil || len(candidatePage.Candidates) != 0 {
		t.Fatalf("existing member candidate search = %d %s", candidates.Code, candidates.Body.String())
	}
	candidates = handler.requestAs(t, admin, http.MethodGet, "/api/v1/tenant-members?tenant=example&view=candidates&q=bob", "")
	if candidates.Code != http.StatusOK || json.Unmarshal(candidates.Body.Bytes(), &candidatePage) != nil || len(candidatePage.Candidates) != 1 || candidatePage.Candidates[0].DisplayName != "Bob" {
		t.Fatalf("candidate search = %d %s", candidates.Code, candidates.Body.String())
	}
}
